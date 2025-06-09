package impl

import (
	"fmt"
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/brick/smq"
	"git.pinquest.cn/qlb/brick/utils"
	"git.pinquest.cn/qlb/core"
	"git.pinquest.cn/qlb/featswitch"
	"git.pinquest.cn/qlb/qmsg"
	"git.pinquest.cn/qlb/qris"
	"time"
)

const defaultLimit uint32 = 1000

// 获取机器人关联的客服id列表 优先按会话维度，因为机器人a作为资产分配给A客服，A客服是可以将某个会话转给非分配资产模式的客服B的
func getRelatedCustomerServiceUidList(ctx *rpc.Context, corpId, appId uint32, robotUid, chatId uint64) ([]uint64, bool, error) {
	assignChat, err := AssignChat.getByCid(ctx, corpId, appId, chatId)
	if err != nil {
		if AssignChat.IsNotFoundErr(err) {
			userRobotList, err := UserRobot.getListByRobotUid(ctx, corpId, appId, robotUid)
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, false, err
			}
			if len(userRobotList) > 0 {
				return utils.PluckUint64(userRobotList, "Uid"), true, nil
			}
			return []uint64{}, false, nil
		}
		log.Errorf("err:%v", err)
		return nil, false, err
	}

	return []uint64{assignChat.Uid}, false, nil
}

func HandleSyncSingleChatStatMessageMq(ctx *rpc.Context, req *smq.ConsumeReq) (*smq.ConsumeRsp, error) {
	return MqSyncSingleChatStat.ProcessV2(ctx, req, func(ctx *rpc.Context, _ *smq.ConsumeReq, data interface{}) (*smq.ConsumeRsp, error) {
		var rsp smq.ConsumeRsp
		msgReq := data.(*qmsg.SyncSingleChatStatMessageMqReq)

		log.Debugf("HandleSyncSingleChatStatMessageMq msg: %v", msgReq)

		statRule := msgReq.Rule
		if statRule.Detail == nil {
			log.Warnf("empty statRule detail %d", statRule.Id)
			return &rsp, nil
		}

		date := getYesterday()

		//质检统计范围备份
		_, err := MsgStatTimeScope.FirstOrCreate(ctx, map[string]interface{}{
			DbCorpId: statRule.CorpId,
			DbAppId:  statRule.AppId,
			DbDate:   date,
		}, map[string]interface{}{
			DbConfig: &qmsg.ModelMsgStatTimeScope_Config{
				StatTimeScopeType: statRule.Detail.StatTimeScopeType,
				TimeScopeList:     statRule.Detail.TimeScopeList,
			},
		}, nil)

		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}

		err = NewSyncSingleStat(statRule, date).Run(ctx)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}

		return &rsp, nil
	})
}

func getStatRuleRelatedRobotUidList(ctx *rpc.Context, statRule *qmsg.ModelMsgQualityStatRule) ([]uint64, error) {
	var robotUidList []uint64
	if statRule.Detail.StatUserScopeType == uint32(qmsg.ModelMsgQualityStatRule_StatUserScopeTypeAll) {
		robotList, err := getAllRobotList(ctx, statRule.CorpId, statRule.AppId)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
		robotUidList = utils.PluckUint64(robotList, "Uid")
	} else {
		robotUidList = statRule.Detail.UidList
	}
	return robotUidList, nil
}

type SyncSingleStat struct {
	statRule     *qmsg.ModelMsgQualityStatRule
	date         uint32
	week         uint32
	robotUidList []uint64
}

func NewSyncSingleStat(statRule *qmsg.ModelMsgQualityStatRule, date uint32) *SyncSingleStat {
	week := uint32(getHumanWeekByTime(time.Unix(int64(utils.Day2Second(date)), 0)))
	return &SyncSingleStat{
		statRule: statRule,
		date:     date,
		week:     week,
	}
}

func (p *SyncSingleStat) Run(ctx *rpc.Context) (err error) {
	//机器人维度统计
	err = p.robotSingChatStat(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	//客服维度统计
	err = p.customerServiceSingleChatStat(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	return nil
}

func (p *SyncSingleStat) robotSingChatStat(ctx *rpc.Context) error {
	robotUidList, err := getStatRuleRelatedRobotUidList(ctx, p.statRule)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	if len(robotUidList) == 0 {
		log.Warnf("statRule not set robot %d", p.statRule.Id)
		return nil
	}
	p.robotUidList = robotUidList

	var list []*qmsg.ModelRobotSingleStat
	for _, robotUid := range robotUidList {
		if p.statRule.Detail.StatTimeScopeType == uint32(qmsg.StatTimeScopeType_StatTimeScopeTypeAll) {
			r, err := p.getRobotSingleChatStatItem(ctx, robotUid, 0, 0)
			if err != nil {
				log.Errorf("err:%v", err)
				return err
			}
			list = append(list, r)
		} else {
			for _, item := range p.statRule.Detail.TimeScopeList {
				if !utils.InSliceUint32(p.week, item.WeekList) {
					continue
				}
				r, err := p.getRobotSingleChatStatItem(ctx, robotUid, item.StartAt, item.EndAt)
				if err != nil {
					log.Errorf("err:%v", err)
					return err
				}
				list = append(list, r)
			}
		}
	}
	if len(list) > 0 {
		err = RobotSingleStat.BatchCreateIgnoreConflict(ctx, list)
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
	}
	return nil
}
func (p *SyncSingleStat) customerServiceSingleChatStat(ctx *rpc.Context) error {
	var list []*qmsg.ModelUser
	err := User.WhereCorpApp(p.statRule.CorpId, p.statRule.AppId).Find(ctx, &list)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	var statList []*qmsg.ModelCustomerServiceSingleStat
	for _, user := range list {
		if p.statRule.Detail.StatTimeScopeType == uint32(qmsg.StatTimeScopeType_StatTimeScopeTypeAll) {
			r, err := p.getCustomerServiceSingleChatStatItem(ctx, user.Id, 0, 0)
			if err != nil {
				log.Errorf("err:%v", err)
				return err
			}
			if r != nil {
				statList = append(statList, r)
			}
		} else {
			for _, item := range p.statRule.Detail.TimeScopeList {
				if !utils.InSliceUint32(p.week, item.WeekList) {
					continue
				}
				r, err := p.getCustomerServiceSingleChatStatItem(ctx, user.Id, item.StartAt, item.EndAt)
				if err != nil {
					log.Errorf("err:%v", err)
					return err
				}
				if r != nil {
					statList = append(statList, r)
				}
			}
		}
	}
	if len(statList) > 0 {
		err = CustomerServiceSingleStat.BatchCreateIgnoreConflict(ctx, statList)
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
	}
	return nil
}

func (p *SyncSingleStat) getRobotSingleChatStatItem(ctx *rpc.Context, robotUid uint64, startAt, endAt uint32) (*qmsg.ModelRobotSingleStat, error) {
	statRedisKey := &StatRedisKey{
		corpId:   p.statRule.CorpId,
		robotUid: robotUid,
		startAt:  startAt,
		endAt:    endAt,
		date:     p.date,
	}

	count, err := p.getSenOptRecordCount(ctx, []uint64{robotUid}, 0, startAt, endAt)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	r := &qmsg.ModelRobotSingleStat{
		CorpId:                p.statRule.CorpId,
		AppId:                 p.statRule.AppId,
		RobotUid:              robotUid,
		Date:                  p.date,
		StartAt:               startAt,
		EndAt:                 endAt,
		ChatCount:             getRedisSetCount(statRedisKey.getChatCountKey(0)),
		SendMsgCount:          getRedisIncVal(statRedisKey.getSendMsgCountKey(0)),
		ReceiveMsgCount:       getRedisIncVal(statRedisKey.getReceiveMsgCountKey(0)),
		ReplyChatCount:        getRedisSetCount(statRedisKey.getHasReplayChatKey(0)),
		FirstReplyTotalTime:   getRedisIncVal(statRedisKey.getFirstReplyTotalTimeKey(0)),
		TurnCount:             getRedisIncVal(statRedisKey.getTurnKey(0)),
		FirstInTimeReplyCount: getRedisIncVal(statRedisKey.getFirstInTimeReplyCountKey(0)),
		FirstTimeoutChatCount: getRedisIncVal(statRedisKey.getFirstTimeoutChatKey(0)),
		TimeoutChatCount:      getRedisIncVal(statRedisKey.getTimeoutChatKey(0)),
		SensitiveTriggerCount: count,
		FirstReplyTotalCount:  getRedisIncVal(statRedisKey.getFirstReplyTotalCountKey(0)),
		NoReplyCount:          getRedisIncVal(statRedisKey.getNoReplyMsgCountKey(0)),
	}
	return r, nil
}

func (p *SyncSingleStat) getCustomerServiceSingleChatStatItem(ctx *rpc.Context, customerServiceUid uint64, startAt, endAt uint32) (*qmsg.ModelCustomerServiceSingleStat, error) {
	statRedisKey := &StatRedisKey{
		corpId:   p.statRule.CorpId,
		robotUid: 0,
		startAt:  startAt,
		endAt:    endAt,
		date:     p.date,
	}

	var err error
	var count uint32
	if len(p.robotUidList) > 0 {
		count, err = p.getSenOptRecordCount(ctx, p.robotUidList, customerServiceUid, startAt, endAt)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
	}

	r := &qmsg.ModelCustomerServiceSingleStat{
		CorpId:                p.statRule.CorpId,
		AppId:                 p.statRule.AppId,
		RobotUid:              0,
		CustomerServiceId:     customerServiceUid,
		Date:                  p.date,
		StartAt:               startAt,
		EndAt:                 endAt,
		ChatCount:             getRedisSetCount(statRedisKey.getChatCountKey(customerServiceUid)),
		SendMsgCount:          getRedisIncVal(statRedisKey.getSendMsgCountKey(customerServiceUid)),
		ReceiveMsgCount:       getRedisIncVal(statRedisKey.getReceiveMsgCountKey(customerServiceUid)),
		ReplyChatCount:        getRedisSetCount(statRedisKey.getHasReplayChatKey(customerServiceUid)),
		FirstReplyTotalTime:   getRedisIncVal(statRedisKey.getFirstReplyTotalTimeKey(customerServiceUid)),
		TurnCount:             getRedisIncVal(statRedisKey.getTurnKey(customerServiceUid)),
		FirstInTimeReplyCount: getRedisIncVal(statRedisKey.getFirstInTimeReplyCountKey(customerServiceUid)),
		FirstTimeoutChatCount: getRedisIncVal(statRedisKey.getFirstTimeoutChatKey(customerServiceUid)),
		TimeoutChatCount:      getRedisIncVal(statRedisKey.getTimeoutChatKey(customerServiceUid)),
		SensitiveTriggerCount: count,
		FirstReplyTotalCount:  getRedisIncVal(statRedisKey.getFirstReplyTotalCountKey(customerServiceUid)),
		NoReplyCount:          getRedisIncVal(statRedisKey.getNoReplyMsgCountKey(customerServiceUid)),
	}
	//当所有统计维度数据为0时，不记录
	if r.ChatCount == 0 && r.SendMsgCount == 0 && r.ReceiveMsgCount == 0 && r.ReplyChatCount == 0 && r.FirstReplyTotalTime == 0 &&
		r.TurnCount == 0 && r.FirstInTimeReplyCount == 0 && r.FirstTimeoutChatCount == 0 && r.TimeoutChatCount == 0 &&
		r.SensitiveTriggerCount == 0 && r.FirstReplyTotalCount == 0 && r.NoReplyCount == 0 {
		return nil, nil
	}
	return r, nil
}

func (p *SyncSingleStat) getSenOptRecordCount(ctx *rpc.Context, robotUidList []uint64, customerServiceUid uint64, startAt, endAt uint32) (uint32, error) {
	if len(robotUidList) == 0 && customerServiceUid == 0 {
		return 0, nil
	}
	location, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		log.Errorf("err:%v", err)
		return 0, err
	}

	bt, err := time.ParseInLocation("20060102", fmt.Sprintf("%d", p.date), location)
	if err != nil {
		log.Errorf("err:%v", err)
		return 0, err
	}
	startAt = startAt * 60
	endAt = endAt * 60
	if endAt == 0 {
		endAt = 86400
	}
	timeRaw := fmt.Sprintf("%s >= %d and %s < %d", DbCreatedAt, bt.Add(time.Duration(startAt)*time.Second).Unix(), DbCreatedAt, bt.Add(time.Duration(endAt)*time.Second).Unix())

	scope := SenOptRecord.
		WhereCorpApp(p.statRule.CorpId, p.statRule.AppId).Where(DbChatType, qmsg.ChatType_ChatTypeSingle).WhereRaw(timeRaw)
	if len(robotUidList) != 0 {
		scope.WhereIn(DbRobotUid, robotUidList)
	}
	if customerServiceUid != 0 {
		scope.Where(DbUid, customerServiceUid)
	}
	count, err := scope.Count(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return 0, err
	}
	return count, nil
}

func getAllRobotList(ctx *rpc.Context, corpId, appId uint32) (list []*qris.Robot, err error) {
	limit := defaultLimit
	var offset uint32
	for {
		options := core.NewListOption().
			SetLimit(limit).SetOffset(offset)
		rsp, err := qris.GetRobotList(ctx, &qris.GetRobotListReq{
			ListOption: options,
			CorpId:     corpId,
			AppId:      appId,
		})
		offset += limit
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
		if len(rsp.List) == 0 {
			break
		}
		list = append(list, rsp.List...)
	}
	return list, nil
}

func pubToMsgBoxStatMq(ctx *rpc.Context, msg *qmsg.ModelMsgBox, chat *qmsg.ModelChat) {
	_, err := MqMsgBoxStat.PubV2(ctx, &smq.PubReq{
		CorpId: msg.CorpId,
		AppId:  msg.AppId,
		Hash:   utils.HashStr(fmt.Sprintf("%d_%d", msg.Uid, msg.ChatId)),
	}, &qmsg.MsgBoxStatMqReq{
		CorpId: msg.CorpId,
		AppId:  msg.AppId,
		MsgBox: msg,
		ChatId: chat.Id,
	})
	if err != nil {
		log.Errorf("err:%v", err)
	}
}

func pubToRobotToCustomerMsgStatMq(ctx *rpc.Context, msg *qmsg.RobotToCustomerMsgStatMqReq) {
	_, err := MqRobotToCustomerMsgStat.PubV2(ctx, &smq.PubReq{
		CorpId: msg.CorpId,
		AppId:  msg.AppId,
		Hash:   utils.HashStr(fmt.Sprintf("%d_%d", msg.RobotUid, msg.ReceiveAccountId)),
	}, msg)
	if err != nil {
		log.Errorf("err:%v", err)
	}
}

func pubToAssignChatChangedMq(ctx *rpc.Context, corpId, appId uint32, assignIdList []uint64) {
	_, err := MqAfterAssignChatChanged.PubV2(ctx, &smq.PubReq{
		CorpId: corpId,
		AppId:  appId,
		Hash:   utils.HashStr(fmt.Sprintf("%d", corpId)),
	}, &qmsg.AfterAssignChatChangedMqReq{
		CorpId:       corpId,
		AppId:        appId,
		AssignIdList: assignIdList,
	})
	if err != nil {
		log.Errorf("err:%v", err)
	}
}

func HandleMsgBoxStatMq(ctx *rpc.Context, req *smq.ConsumeReq) (*smq.ConsumeRsp, error) {
	return MqMsgBoxStat.ProcessV2(ctx, req, func(ctx *rpc.Context, _ *smq.ConsumeReq, data interface{}) (*smq.ConsumeRsp, error) {
		var rsp smq.ConsumeRsp
		msgReq := data.(*qmsg.MsgBoxStatMqReq)

		log.Debugf("HandleMsgBoxStatMq msg: %v", msgReq)

		handler := newReceiveStatHandler(msgReq.MsgBox, msgReq.ChatId)
		matchStatRule, err := handler.CheckIsMatchStatRule(ctx)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
		if !matchStatRule {
			log.Warnf("not match msg stat rule")
			return &rsp, nil
		}

		err = handler.Init(ctx)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}

		err = handler.Run(ctx)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}

		return &rsp, nil
	})
}

func HandleRobotToCustomerMsgStatMq(ctx *rpc.Context, req *smq.ConsumeReq) (*smq.ConsumeRsp, error) {
	return MqRobotToCustomerMsgStat.ProcessV2(ctx, req, func(ctx *rpc.Context, _ *smq.ConsumeReq, data interface{}) (*smq.ConsumeRsp, error) {
		var rsp smq.ConsumeRsp
		msgReq := data.(*qmsg.RobotToCustomerMsgStatMqReq)

		log.Debugf("MqRobotToCustomerMsgStat msg: %v", msgReq)

		chat, err := Chat.getByRobotAndExt(ctx, msgReq.CorpId, msgReq.AppId, msgReq.RobotUid, msgReq.ReceiveAccountId, false)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}

		handler := newSendStatHandler(chat, req.CreatedAt, msgReq.CustomerServiceUid)
		matchStateRule, err := handler.CheckIsMatchStatRule(ctx)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
		if !matchStateRule {
			log.Warnf("not match msg stat rule")
			return &rsp, nil
		}

		err = handler.Init(ctx)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}

		err = handler.Run(ctx, msgReq.IsSetNotReplyMsg)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}

		return &rsp, nil
	})
}

func AfterAssignChatChangedMq(ctx *rpc.Context, req *smq.ConsumeReq) (*smq.ConsumeRsp, error) {
	return MqAfterAssignChatChanged.ProcessV2(ctx, req, func(ctx *rpc.Context, _ *smq.ConsumeReq, data interface{}) (*smq.ConsumeRsp, error) {
		var rsp smq.ConsumeRsp
		msgReq := data.(*qmsg.AfterAssignChatChangedMqReq)

		log.Debugf("MqAfterAssignChatChanged msg: %v", msgReq)

		var assignChatList []*qmsg.ModelAssignChat
		err := AssignChat.WhereCorpApp(msgReq.CorpId, msgReq.AppId).WhereIn(DbId, msgReq.AssignIdList).Unscoped().Find(ctx, &assignChatList)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}

		const key = "QMSG_PUB_ASSIGN_CHAT_CHANGE_MQ_TO_QOPEN"
		featureFlagRsp, err := featswitch.GetFeatureFlagEnabled(ctx, &featswitch.GetFeatureFlagEnabledReq{Key: key, CorpId: msgReq.CorpId, AppId: msgReq.AppId})
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}

		for _, assignChat := range assignChatList {
			// 打开了特性开关才推送开平
			if featureFlagRsp.Enabled {
				// 推送开平
				err = pubAssignChatChangeMq(ctx, msgReq.CorpId, msgReq.AppId, assignChat)
				if err != nil {
					// 出现错误也不影响主流程
					log.Errorf("err:%v", err)
				}
			}
			//不处理群的
			if assignChat.IsGroup {
				continue
			}
			//待分配的
			if assignChat.Uid == 0 {
				continue
			}
			handler := &StatHandler{
				CorpId:            assignChat.CorpId,
				AppId:             assignChat.AppId,
				MsgTime:           assignChat.UpdatedAt,
				RobotUid:          assignChat.RobotUid,
				CustomerAccountId: assignChat.ChatExtId,
				ChatId:            assignChat.Cid,
			}
			matchStateRule, err := handler.CheckIsMatchStatRule(ctx)
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, err
			}
			//没有匹配质检规则
			if !matchStateRule {
				continue
			}
			err = handler.Init(ctx)
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, err
			}
			//会话被转走或关闭
			if assignChat.DeletedAt > 0 {
				//除了客服手动关闭的，其他的不计算超时
				if assignChat.EndScene != uint32(qmsg.ModelAssignChat_EndSceneClose) {
					err = handler.IgnoreTimeout(ctx, assignChat.Uid)
					if err != nil {
						log.Errorf("err:%v", err)
						return nil, err
					}
				}
			} else { //分配
				//找下分配前的消息有没有被别的客服回复，如果未回复则需要计算超时
				msgBoxId, err := handler.GetLastNotRepliedMsgBoxId()
				if err != nil {
					log.Errorf("err:%v", err)
					return nil, err
				}
				if msgBoxId > 0 {
					err = handler.CreateMsgCache(ctx, assignChat.Uid, msgBoxId, false)
					if err != nil {
						log.Errorf("err:%v", err)
						return nil, err
					}
				}
			}

		}

		return &rsp, nil
	})
}

func pubAssignChatChangeMq(ctx *rpc.Context, corpId, appId uint32, assignChat *qmsg.ModelAssignChat) error {

	// 获取会话信息
	chat, err := Chat.get(ctx, corpId, appId, assignChat.Cid)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	var receiverAccount string
	var referAccount string

	if assignChat.OriginUid != 0 {
		originUser, err2 := User.get(ctx, corpId, appId, assignChat.OriginUid)
		if err2 != nil {
			log.Errorf("err:%v", err2)
			return err2
		}
		if originUser != nil {
			referAccount = originUser.LoginId
		}
	}

	if assignChat.Uid != 0 {
		user, err2 := User.get(ctx, corpId, appId, assignChat.Uid)
		if err2 != nil {
			log.Errorf("err:%v", err2)
			return err2
		}
		if user != nil {
			receiverAccount = user.LoginId
		}
	}

	var state uint32
	if assignChat.DeletedAt > 0 {
		state = uint32(qmsg.AssignChatChangeMqReq_StateEnd)
	} else {
		state = uint32(qmsg.AssignChatChangeMqReq_StateProcess)
	}

	assignChatChangeMqReq := &qmsg.AssignChatChangeMqReq{
		CorpId:         corpId,
		AppId:          appId,
		AssignChatId:   assignChat.Id,
		ChatId:         chat.Id,
		RobotUid:       assignChat.RobotUid,
		ChatExtId:      chat.ChatExtId,
		IsGroup:        chat.IsGroup,
		ReceiveAccount: receiverAccount,
		ReferAccount:   referAccount,
		CreateAt:       assignChat.CreatedAt,
		DeletedAt:      assignChat.DeletedAt,
		StartScene:     assignChat.StartScene,
		EndScene:       assignChat.EndScene,
		State:          state,
	}
	// 推送消息
	_, err = qmsg.MqAssignChatChange.PubV2(ctx, &smq.PubReq{
		CorpId: corpId,
		AppId:  appId,
		Hash:   utils.HashStr(fmt.Sprintf("%d", corpId)),
	}, assignChatChangeMqReq)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	} else {
		log.Infof("pub_assignChat_change_msg %v", assignChatChangeMqReq)
	}

	return nil
}

func AssignChatChangeMq(ctx *rpc.Context, req *smq.ConsumeReq) (*smq.ConsumeRsp, error) {
	var rsp smq.ConsumeRsp
	return &rsp, nil
}
