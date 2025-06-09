// msg_ai_impl.go 是一个“混合”文件：它既有旧的 MQ 消费者（已清空），也有前端可调用的 AI 相关 RPC 接口。
// 未来的重构目标：一旦整个 AI Agent 模式稳定，且 qmsg 前端的所有 AI 相关接口都明确后，可以考虑将所有前端调用的 AI 相关接口（包括 GetAiEntertainConfig、SetAiEntertainConfig、AiEntertain2Manual 以及未来可能新增的“切换 AI 接待”接口）统一重构到如 impl/frontend_ai_api.go 或 impl/chat_settings_api.go 这样的新文件中，从而彻底废弃 msg_ai_impl.go。但这不属于当前阶段的核心任务。
package impl

import (
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/dbx"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/brick/smq"
	"git.pinquest.cn/qlb/brick/utils"
	"git.pinquest.cn/qlb/core"
	"git.pinquest.cn/qlb/iquan"
	"git.pinquest.cn/qlb/pindef"
	"git.pinquest.cn/qlb/qmsg"

	"git.pinquest.cn/qlb/qrobot"
	"git.pinquest.cn/qlb/quan"
)

var aiEntertainConfig = dbx.NewModel(&dbx.ModelConfig{
	Type:            &qmsg.ModelAiEntertainConfig{},
	NotFoundErrCode: qmsg.ErrAiEntertainConfigNotFound,
})

var aiConfigEditRecord = dbx.NewModel(&dbx.ModelConfig{
	Type:            &qmsg.ModelAiEntertainConfigEditRecord{},
	NotFoundErrCode: qmsg.ErrAiEntertainConfigNotFound,
})

var aiEntertainDetail = dbx.NewModel(&dbx.ModelConfig{
	Type:            &qmsg.ModelAiEntertainDetail{},
	NotFoundErrCode: qmsg.ErrAiEntertainDetailNotFound,
})

var aiEntertainRecord = dbx.NewModel(&dbx.ModelConfig{
	Type:            &qmsg.ModelAiEntertainRecord{},
	NotFoundErrCode: qmsg.ErrAiEntertainRecordNotFound,
})

// 智能助手发消息的 cliMsgId前缀
const aiMsgPrefix = "ai_send_msg"

func GetAiEntertainConfig(ctx *rpc.Context, req *qmsg.GetAiEntertainConfigReq) (*qmsg.GetAiEntertainConfigRsp, error) {
	var rsp qmsg.GetAiEntertainConfigRsp

	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)

	var config qmsg.ModelAiEntertainConfig

	err := aiEntertainConfig.WhereCorpApp(corpId, appId).First(ctx, &config)
	if err != nil {
		log.Errorf("err %v", err)
		return nil, err
	}

	rsp.Config = &config
	return &rsp, nil
}

func SetAiEntertainConfig(ctx *rpc.Context, req *qmsg.SetAiEntertainConfigReq) (*qmsg.SetAiEntertainConfigRsp, error) {
	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)

	uid, _ := core.GetPinHeaderUId2Int(ctx)
	if uid == 0 {
		return nil, pindef.ErrNotLogin
	}

	if req.Config == nil {
		return nil, rpc.InvalidArg("miss param 'config'")
	}
	req.Config.CorpId = corpId
	req.Config.AppId = appId

	// 注意：此接口用于配置旧 AI 自动回复，在新 Agent 模式下已废弃。
	// 使用新的 AI 配置管理工具。
	log.Warnf("SetAiEntertainConfig: 此接口已废弃，不再支持新 AI 配置。CorpId: %d, AppId: %d", corpId, appId)

	return nil, rpc.CreateErrorWithMsg(-9999, "此接口已废弃，请使用新的 AI 配置工具。")
}

func GetAiEntertainConfigEditRecord(ctx *rpc.Context, req *qmsg.GetAiEntertainConfigEditRecordReq) (*qmsg.GetAiEntertainConfigEditRecordRsp, error) {
	var rsp qmsg.GetAiEntertainConfigEditRecordRsp
	rsp.UserMap = make(map[uint64]*quan.ModelUser)

	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)

	var recordList []*qmsg.ModelAiEntertainConfigEditRecord
	err := aiConfigEditRecord.WhereCorpApp(corpId, appId).SetLimit(100).OrderDesc(DbCreatedAt).Find(ctx, &recordList)
	if err != nil {
		log.Errorf("err %v", err)
		return nil, err
	}

	uidList := utils.PluckUint64(recordList, "Uid").Unique()

	if len(uidList) > 0 {
		qRsp, err := iquan.GetUserList(ctx, &iquan.GetUserListReq{
			ListOption: core.NewListOption().AddOpt(iquan.GetUserListReq_ListOptionUidList, uidList),
			CorpId:     corpId,
			AppId:      appId,
		})
		if err != nil {
			log.Errorf("err %v", err)
			return nil, err
		}

		for _, v := range qRsp.List {
			rsp.UserMap[v.Id] = v
		}
	}

	rsp.List = recordList

	return &rsp, nil
}

func MsgAiEntertainMq(ctx *rpc.Context, req *smq.ConsumeReq) (*smq.ConsumeRsp, error) {
	return MqMsgAiEntertain.ProcessV2(ctx, req, func(ctx *rpc.Context, _ *smq.ConsumeReq, data interface{}) (*smq.ConsumeRsp, error) {
		var rsp smq.ConsumeRsp

		// 警告：此 MQ 消费者处理的旧 AI 逻辑已弃用，消息将不再被此函数处理。
		// 新的 AI 逻辑由 aiadmin 服务负责，并通过 MqAIWorkflowBufferEvent 触发。
		log.Warnf("MsgAiEntertainMq: 此 MQ 消费者处理的旧 AI 逻辑已弃用，消息将被忽略。请求ID: %s", req.MsgId)

		// 确保返回成功，避免消息重试，即使不再执行实际业务逻辑
		return &rsp, nil
	})
}

//		var rsp smq.ConsumeRsp
//
//		core.SetCorpAndApp(ctx, req.CorpId, req.AppId)
//		mqData := data.(*qmsg.MsgAiEntertainMqData)
//		log.Infof("ai_msg msg_id:%v", req.MsgId)
//
//		var chat qmsg.ModelChat
//		err := Chat.WhereCorpApp(req.CorpId, req.AppId).Where(DbId, mqData.ModelChatId).First(ctx, &chat)
//		if err != nil {
//			log.Errorf("err %v", err)
//			return nil, err
//		}
//
//		var msgBox qmsg.ModelMsgBox
//		err = ChoiceMsgBoxDb(MsgBox.W(req.CorpId, req.AppId, chat.RobotUid), req.CorpId).Where(DbId, mqData.MsgBoxId).First(ctx, &msgBox)
//		if err != nil {
//			log.Errorf("err:%v", err)
//			return nil, err
//		}
//
//		if chat.IsGroup {
//			//针对群消息避免重复发
//			isDup, err := checkAiMsgDeDupCache(msgBox.Msg)
//			if err != nil {
//				log.Errorf("err:%v", err)
//			}
//
//			if isDup {
//				log.Info("dup ai_msg")
//				return &rsp, nil
//			}
//		}
//
//		var aiConfig qmsg.ModelAiEntertainConfig
//
//		err = aiEntertainConfig.WhereCorpApp(req.CorpId, req.AppId).First(ctx, &aiConfig)
//		if err != nil {
//			log.Errorf("err %v", err)
//			return nil, err
//		}
//
//		aiReq := &qmsgai.GetAutoReplyMsgReq{
//			CorpId:        req.CorpId,
//			Keyword:       msgBox.Msg,
//			SendAccountId: msgBox.SenderAccountId,
//		}
//		if chat.IsGroup {
//			aiReq.ChId = aiConfig.GroupChatChId
//		} else {
//			aiReq.ChId = aiConfig.ChatChId
//		}
//
//		aiRsp, err := qmsgai.GetAutoReplyMsg(ctx, aiReq)
//		if err != nil {
//			log.Errorf("err %v", err)
//			return nil, err
//		}
//
//		log.Debugf("ai_msg auto reply:%v", aiRsp.MsgContent)
//
//		//智能助手返回空内容，就不发了
//		if aiRsp.MsgContent == "" {
//			log.Infof("ai response empty")
//			return &rsp, nil
//		}
//
//		uuid, _ := utils.GenUUID()
//		pubRsp := qmsg.MqSendChatMsg.PubOrExecInRoutine(
//			ctx, req.CorpId, req.AppId,
//			&qmsg.SendChatMsgMqReq{
//				SendReq: &qmsg.SendChatMsgReq{
//					RobotUid:    chat.RobotUid,
//					CliMsgId:    fmt.Sprintf("%s_%d_%s", aiMsgPrefix, chat.RobotUid, uuid),
//					ChatType:    msgBox.ChatType,
//					ChatId:      msgBox.ChatId,
//					ChatMsgType: uint32(qmsg.ChatMsgType_ChatMsgTypeText),
//					Msg:         aiRsp.MsgContent,
//				},
//				SendAt: utils.Now(),
//				User: &qmsg.ModelUser{
//					CorpId:   req.CorpId,
//					AppId:    req.AppId,
//					LoginId:  "ai_msg_fake_kefu",
//					Username: "ai_msg_fake_kefu",
//				},
//			}, processSendChatMsg)
//		if pubRsp != nil {
//			log.Infof("pub rsp %s", pubRsp.MsgId)
//		}
//
//		if aiConfig.ResetUnread == uint32(qmsg.ModelAiEntertainConfig_Set) {
//			_, err = Chat.WhereCorpApp(req.CorpId, req.AppId).Where(DbId, chat.Id).Update(ctx, map[string]interface{}{
//				DbUnreadCount: 0,
//				DbIsReply:     true,
//			})
//			if err != nil {
//				log.Errorf("err:%v", err)
//				return nil, err
//			}
//
//			qwhaleSrv := qwhaleService{}
//			hostAccount, err := qwhaleSrv.getHostAccountByUid(ctx, chat.CorpId, chat.AppId, chat.RobotUid)
//			if err == nil && hostAccount == nil {
//				log.Warnf("warn:由创已读接口只支持扫码号:%v", chat)
//				return &rsp, nil
//			}
//
//			_, err = qsm.MarkAsReadForIm(ctx, &qsm.MarkAsReadForImReq{
//				MsgSeq:   chat.MsgSeq,
//				RobotUid: chat.RobotUid,
//				ExtId:    chat.ChatExtId,
//				IsGroup:  chat.IsGroup,
//				Type:     uint32(qsm.MarkAsReadType_TypeRead),
//			})
//			if err != nil {
//				if rpc.GetErrCode(err) == qrobot.ErrGroupChatNotFound {
//					return &rsp, nil
//				}
//				log.Errorf("err:%v", err)
//				return nil, err
//			}
//		} else {
//			// 变更回复状态
//			_, err = Chat.WhereCorpApp(req.CorpId, req.AppId).
//				Where(DbId, chat.Id).
//				Update(ctx, map[string]interface{}{
//					// 已回复
//					DbIsReply: true,
//				})
//			if err != nil {
//				log.Errorf("err:%v", err)
//				return nil, err
//			}
//		}
//
//		return &rsp, nil
//	})
//}

func AiEntertain2Manual(ctx *rpc.Context, req *qmsg.AiEntertain2ManualReq) (*qmsg.AiEntertain2ManualRsp, error) {
	var rsp qmsg.AiEntertain2ManualRsp
	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)

	var chat qmsg.ModelChat
	err := Chat.WhereCorpApp(corpId, appId).Where(DbId, req.ModelChatId).First(ctx, &chat)
	if err != nil {
		log.Errorf("err %v", err)
		return nil, err
	}

	if chat.Detail != nil && chat.Detail.EntertainType != uint32(qmsg.ModelChat_Detail_Ai) {
		log.Info("not ai entertain")

		_, err = aiEntertainRecord.WhereCorpApp(corpId, appId).Where(DbModelChatId, chat.Id).Update(ctx, map[string]interface{}{
			DbEndAt: utils.Now(),
		})
		if err != nil {
			log.Errorf("err %v", err)
			return nil, err
		}

		return &rsp, nil
	}

	if req.Uid == 0 {
		var userRobot qmsg.ModelUserRobot
		err = UserRobot.WhereCorpApp(corpId, appId).Where(DbRobotUid, chat.RobotUid).First(ctx, &userRobot)
		if err != nil {
			log.Errorf("err %v", err)
			return nil, err
		}

		var msgBox qmsg.ModelMsgBox
		err := ChoiceMsgBoxDb(MsgBox.WhereCorpApp(corpId, appId).Where(map[string]interface{}{
			DbMsgSeq: chat.MsgSeq,
			DbChatId: chat.ChatExtId,
			DbUid:    chat.RobotUid,
		}), corpId).First(ctx, &msgBox)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}

		if chat.Detail == nil {
			chat.Detail = &qmsg.ModelChat_Detail{}
		}

		chat.Detail.EntertainType = uint32(qmsg.ModelChat_Detail_Manual)
		_, err = Chat.WhereCorpApp(corpId, appId).Where(DbId, req.ModelChatId).Update(ctx, map[string]interface{}{
			DbDetail: chat.Detail,
		})
		if err != nil {
			log.Errorf("err %v", err)
			return nil, err
		}

		//非智能助手处理
		wrapperList := []*qmsg.WsMsgWrapper{
			{
				MsgType: uint32(qmsg.WsMsgWrapper_MsgTypeMsgBox),
				MsgBox:  &msgBox,
				Chat:    &chat,
				MsgId:   msgBox.CliMsgId,
				LastSeq: chat.MsgSeq,
			},
			// 兼容 1.0 ws ，日后废弃
			{
				MsgType: uint32(qmsg.WsMsgWrapper_MsgTypeMsgSeqChanged),
				MsgSeqChanged: &qmsg.WsMsgWrapper_MsgSeqChanged{
					RobotUid:     msgBox.Uid,
					RemoteMsgSeq: msgBox.MsgSeq,
				},
			},
		}
		//特性开关控制，判断是否要等ws的ack，确保推送不丢失
		wrapperList, err = setOpenMsgAck(ctx, corpId, appId, wrapperList)
		if err != nil {
			log.Errorf("err:%v", err)
			// 出错无需中断业务流程，默认未开启
		}

		err = pushWsMsgByChatV2(ctx, &chat, nil, wrapperList)
		if err != nil {
			log.Errorf("err %v", err)
		}

	} else {
		transferUser, err := User.getUserById(ctx, corpId, appId, req.Uid)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}

		if transferUser.IsAssign {
			return nil, rpc.InvalidArg("不能转给分配资产的客服")
		}

		if !transferUser.IsOnline {
			return nil, rpc.InvalidArg("该客服已下班")
		}

		assignChat, err := AssignChat.getByCid(ctx, corpId, appId, chat.Id)
		if err != nil && !AssignChat.IsNotFoundErr(err) {
			log.Errorf("err:%v", err)
			return nil, err
		}

		if assignChat != nil {
			return nil, rpc.InvalidArg("该会话已经被分配")
		}

		var userRobot qmsg.ModelUserRobot
		err = UserRobot.WhereCorpApp(corpId, appId).Where(DbUid, req.Uid).Where(DbRobotUid, chat.RobotUid).First(ctx, &userRobot)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}

		if chat.Detail == nil {
			chat.Detail = &qmsg.ModelChat_Detail{}
		}

		chat.Detail.EntertainType = uint32(qmsg.ModelChat_Detail_Manual)
		_, err = Chat.WhereCorpApp(corpId, appId).Where(DbId, req.ModelChatId).Update(ctx, map[string]interface{}{
			DbDetail: chat.Detail,
		})
		if err != nil {
			log.Errorf("err %v", err)
			return nil, err
		}

		_, err = AssignChat.create(ctx, corpId, appId, req.Uid, 0, chat.RobotUid, chat.ChatExtId, chat.IsGroup, &chat, qmsg.ModelAssignChat_StartSceneAdmin)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}

	}

	_, err = aiEntertainRecord.WhereCorpApp(corpId, appId).Where(DbEndAt, 0).Where(DbModelChatId, chat.Id).Update(ctx, map[string]interface{}{
		DbEndAt: utils.Now(),
	})
	if err != nil {
		log.Errorf("err %v", err)
		return nil, err
	}

	return &rsp, nil
}

func GetAiEntertainRecordList(ctx *rpc.Context, req *qmsg.GetAiEntertainRecordListReq) (*qmsg.GetAiEntertainRecordListRsp, error) {
	var rsp qmsg.GetAiEntertainRecordListRsp
	var err error
	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)
	rsp.GroupMap = make(map[uint64]*qrobot.ModelGroupChat)
	rsp.AccountMap = make(map[uint64]*qrobot.ModelAccount)
	rsp.RobotAccountMap = make(map[uint64]*qrobot.ModelAccount)

	db := aiEntertainRecord.NewScopeWithListOption(req.ListOption).WhereCorpApp(corpId, appId)
	err = core.NewListOptionProcessor(req.ListOption).
		AddBool(qmsg.GetAiEntertainRecordListReq_ListOptionAiEntertain,
			func(val bool) error {
				if val {
					db = db.Where(DbEndAt, 0)
				} else {
					db = db.Gt(DbEndAt, 0)
				}
				return nil
			}).
		AddTimeStampRange(qmsg.GetAiEntertainRecordListReq_ListOptionCreatedAtRange, func(beginAt, endAt uint32) error {
			db.WhereBetween(DbCreatedAt, beginAt, endAt)
			return nil
		}).
		Process()

	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	db.OrderDesc(DbCreatedAt)

	rsp.Paginate, err = db.FindPaginate(ctx, &rsp.List)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	var accountIdList []uint64
	var groupIdList []uint64
	var robotUidList []uint64

	followAccountMap := make(map[uint64][]uint64)
	for _, v := range rsp.List {
		if v.GroupId > 0 {
			groupIdList = append(groupIdList, v.GroupId)
		}

		if v.AccountId > 0 {
			accountIdList = append(accountIdList, v.AccountId)
			followAccountMap[v.RobotUid] = append(followAccountMap[v.RobotUid], v.AccountId)
		}

		robotUidList = append(robotUidList, v.RobotUid)
	}

	if len(accountIdList) > 0 {
		aRsp, err := qrobot.GetAccountListSys(ctx, &qrobot.GetAccountListSysReq{
			ListOption: core.NewListOption().SetSkipCount().SetLimit(uint32(len(accountIdList))).AddOpt(qrobot.GetAccountListSysReq_ListOptionIdList, accountIdList),
			CorpId:     corpId,
			AppId:      appId,
		})
		if err != nil {
			log.Errorf("err %v", err)
			return nil, err
		}
		for _, v := range aRsp.List {
			rsp.AccountMap[v.Id] = v
		}

		if len(aRsp.List) > 0 {
			extUidList := make([]uint64, 0, len(aRsp.List))
			ext2acc := make(map[uint64]uint64, len(aRsp.List))
			for _, a := range aRsp.List {
				extUidList = append(extUidList, a.ExtUid)
				ext2acc[a.ExtUid] = a.Id
			}
			elo := core.NewListOption()
			elo.AddOpt(iquan.GetExtContactListReq_ListOptionExtUidList, extUidList)
			extRsp, err := iquan.GetExtContactList(ctx, &iquan.GetExtContactListReq{
				ListOption: elo,
				AppId:      appId,
				CorpId:     corpId,
			})
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, err
			}

			// key:qrobot.ModelAccount的id	value:quan.ModelExtContact
			extMap := make(map[uint64]*quan.ModelExtContact, len(extRsp.List))
			for _, ext := range extRsp.List {
				if _, ok := ext2acc[ext.Id]; ok {
					extMap[ext2acc[ext.Id]] = ext
				}
			}
			rsp.ExtMap = extMap
		}
	}

	followMap := make(map[uint64][]uint64)
	for robotUid, list := range followAccountMap {
		for _, v := range list {
			if acc, ok := rsp.AccountMap[v]; ok {
				if acc.ExtUid == 0 {
					continue
				}
				followMap[robotUid] = append(followMap[robotUid], acc.ExtUid)
			}
		}
	}

	for robotUid, extUidList := range followMap {

		listRsp, err := iquan.GetExtContactFollowList(ctx, &iquan.GetExtContactFollowListReq{
			ListOption: core.NewListOption().
				AddOpt(iquan.GetExtContactFollowListReq_ListOptionExtUidList, extUidList).
				AddOpt(iquan.GetExtContactFollowListReq_ListOptionUidList, []uint64{robotUid}),
			CorpId: corpId,
			AppId:  appId,
		})
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}

		if len(listRsp.List) > 0 {
			rsp.FollowList = append(rsp.FollowList, listRsp.List...)
		}
		log.Debugf("%v", listRsp)
	}

	if len(groupIdList) > 0 {
		qRsp, err := qrobot.GetGroupChatListSys(ctx, &qrobot.GetGroupChatListSysReq{
			ListOption: core.NewListOption().AddOpt(qrobot.GetGroupChatListSysReq_ListOptionGroupIdList, groupIdList),
			CorpId:     corpId,
			AppId:      appId,
		})
		if err != nil {
			log.Errorf("err %v", err)
			return nil, err
		}
		for _, v := range qRsp.List {
			rsp.GroupMap[v.Id] = v
		}
	}

	if len(robotUidList) > 0 {
		qRsp, err := qrobot.GetAccountListSys(ctx, &qrobot.GetAccountListSysReq{
			ListOption: core.NewListOption(qrobot.GetAccountListSysReq_ListOptionUidList, robotUidList).SetSkipCount().SetLimit(uint32(len(robotUidList))),
			CorpId:     corpId,
			AppId:      appId,
		})
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}

		utils.KeyBy(qRsp.List, "Uid", &rsp.RobotAccountMap)
	}

	return &rsp, nil
}

// Manual2AiEntertain 允许客服将人工接待会话切换到 AI 接待
// 对应产品需求中“当前处于人工接待的时候，支持切换AI接待”
func Manual2AiEntertain(ctx *rpc.Context, req *qmsg.Manual2AiEntertainReq) (*qmsg.Manual2AiEntertainRsp, error) {
	var rsp qmsg.Manual2AiEntertainRsp
	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)
	operatorUid, _ := core.GetPinHeaderUId2Int(ctx)

	// 1. 获取会话信息
	var chat qmsg.ModelChat // 声明为 var，以便在函数作用域内使用
	err := Chat.WhereCorpApp(corpId, appId).Where(DbId, req.ModelChatId).First(ctx, &chat)
	if err != nil {
		log.Errorf("Manual2AiEntertain: err %v, model_chat_id: %d", err, req.ModelChatId)
		return nil, err
	}

	// 2. 已经是 AI 接待则直接返回
	if chat.Detail != nil && chat.Detail.EntertainType == uint32(qmsg.ModelChat_Detail_Ai) {
		log.Warnf("Manual2AiEntertain: 会话 %d 已经处于 AI 接待状态，无需切换。CorpId: %d", req.ModelChatId, corpId)
		return nil, rpc.InvalidArg("会话已处于 AI 接待状态")
	}

	// 3. 更新会话接待类型为 AI
	// 先确保 chat.Detail 存在
	if chat.Detail == nil {
		chat.Detail = &qmsg.ModelChat_Detail{}
	}
	chat.Detail.EntertainType = uint32(qmsg.ModelChat_Detail_Ai) // 切换为 AI 接待
	_, err = Chat.WhereCorpApp(corpId, appId).Where(DbId, req.ModelChatId).Update(ctx, map[string]interface{}{
		DbDetail: chat.Detail,
	})
	if err != nil {
		log.Errorf("Manual2AiEntertain: err %v, updating chat detail", err)
		return nil, err
	}

	// 4. 创建或更新 AI 接待记录
	// 检查是否已有未结束的 AI 接待记录
	var existingAiRecord qmsg.ModelAiEntertainRecord
	recordFound := true
	err = aiEntertainRecord.WhereCorpApp(corpId, appId).
		Where(DbRobotUid, chat.RobotUid).
		Where(DbModelChatId, chat.Id).
		Where(DbEndAt, 0). // 查找未结束的记录 (EndAt=0 表示进行中)
		First(ctx, &existingAiRecord)
	if err != nil {
		if aiEntertainRecord.IsNotFoundErr(err) {
			recordFound = false // 没有找到未结束的 AI 接待记录
		} else {
			log.Errorf("Manual2AiEntertain: err %v, querying aiEntertainRecord", err)
			return nil, err
		}
	}

	if !recordFound {
		// 提前计算 groupId 和 accountId
		var groupId, accountId uint64
		if chat.IsGroup {
			groupId = chat.ChatExtId
		} else {
			accountId = chat.ChatExtId
		}
		// 如果没有找到活跃的 AI 接待记录，则创建一条新的
		err = aiEntertainRecord.Create(ctx, &qmsg.ModelAiEntertainRecord{
			CorpId:      corpId,
			AppId:       appId,
			RobotUid:    chat.RobotUid,
			ModelChatId: chat.Id,
			GroupId:     groupId,
			AccountId:   accountId,
			EndAt:       0, // 默认为 0，表示正在进行中
		})
		if err != nil {
			log.Errorf("Manual2AiEntertain: err %v, creating aiEntertainRecord", err)
			return nil, err
		}
	} else {
		// 如果找到活跃的 AI 接待记录，更新其 updated_at 和 EndAt=0 表示持续进行
		// 这可能发生在从人工模式短暂切换到 AI 模式后又切回来，或者 AI 记录因异常未结束。
		_, err = aiEntertainRecord.WhereCorpApp(corpId, appId).Where(DbId, existingAiRecord.Id).Update(ctx, map[string]interface{}{
			DbUpdatedAt: utils.Now(),
			DbEndAt:     0, // 确保 EndAt 重新设置为 0
		})
		if err != nil {
			log.Errorf("Manual2AiEntertain: err %v, updating existing aiEntertainRecord", err)
			return nil, err
		}
	}

	// 5. 触发新 Agent 模式工作流（模拟消息触发）

	// 6. 推送 WS 消息通知前端会话状态变更
	// 修正：初始化 updatedChat 变量
	var updatedChat = qmsg.ModelChat{}
	chatErr := Chat.WhereCorpApp(corpId, appId).Where(DbId, req.ModelChatId).First(ctx, &updatedChat)
	if chatErr != nil {
		log.Errorf("Manual2AiEntertain: err getting updated chat: %v", chatErr)
		// 不阻断，但记录错误
	}

	// 推送给当前操作的客服，更新其 IM 工作台 UI
	err = pushWsMsg(ctx, corpId, appId, operatorUid, []*qmsg.WsMsgWrapper{
		{
			MsgType: uint32(qmsg.WsMsgWrapper_MsgTypeAssignChat), // 使用 MsgTypeAssignChat 来通知前端更新会话状态
			Chat:    &updatedChat,                                // 传递更新后的 chat 对象
			AssignChat: &qmsg.ModelAssignChat{ // 构造一个简单的 AssignChat 结构，表示当前会话归属
				Cid:       chat.Id,
				RobotUid:  chat.RobotUid,
				IsGroup:   chat.IsGroup,
				ChatExtId: chat.ChatExtId,
				Uid:       0, // AI 模式下没有明确的客服 UID
			},
		},
	})
	if err != nil {
		log.Errorf("Manual2AiEntertain: err pushing WS msg to operator: %v", err)
		// 不阻断，但记录错误
	}

	// 也推送到机器人权限下的所有客服，确保多端一致性
	err = pushWsMsgByRobotUid(ctx, corpId, appId, chat.RobotUid, []*qmsg.WsMsgWrapper{
		{
			MsgType: uint32(qmsg.WsMsgWrapper_MsgTypeAssignChat),
			Chat:    &updatedChat,
			AssignChat: &qmsg.ModelAssignChat{
				Cid: chat.Id, RobotUid: chat.RobotUid, IsGroup: chat.IsGroup, ChatExtId: chat.ChatExtId,
				Uid: 0, // AI 模式下没有明确的客服 UID
			},
		},
	})
	if err != nil {
		log.Errorf("Manual2AiEntertain: err pushing WS msg by robot uid: %v", err)
		// 不阻断，但记录错误
	}

	return &rsp, nil
}
