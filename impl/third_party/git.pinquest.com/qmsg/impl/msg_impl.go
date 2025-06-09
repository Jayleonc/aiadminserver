package impl

import (
	"encoding/csv"
	"fmt"
	"git.pinquest.cn/qlb/brick/dbx"
	"git.pinquest.cn/qlb/brick/json"
	"git.pinquest.cn/qlb/commonv2"
	"git.pinquest.cn/qlb/extrpkg/github.com/elliotchance/pie/pie"
	"git.pinquest.cn/qlb/qris"
	"strconv"
	"strings"
	"time"

	"git.pinquest.cn/qlb/featswitch"

	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/ext"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/brick/smq"
	"git.pinquest.cn/qlb/brick/utils"
	"git.pinquest.cn/qlb/core"
	"git.pinquest.cn/qlb/extrpkg/github.com/golang/protobuf/proto"
	"git.pinquest.cn/qlb/iquan"
	"git.pinquest.cn/qlb/qmsg"
	"git.pinquest.cn/qlb/qrobot"
	"git.pinquest.cn/qlb/quan"
	"git.pinquest.cn/qlb/qwhale"
)

func SyncMsg(ctx *rpc.Context, req *qmsg.SyncMsgReq) (*qmsg.SyncMsgRsp, error) {
	var rsp qmsg.SyncMsgRsp
	// GET请求仅用于开发调试，因为转换这部分的数据会有一些性能损耗
	if !rpc.Meta.IsReleaseRole() {
		var err error
		switch ctx.Ctx.GetHttpMethod() {
		case "GET":
			log.Debug("get request")
			req.RobotUid, err = strconv.ParseUint(ctx.Ctx.GetQueryArg("robot_uid"), 10, 64)
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, rpc.InvalidArg(err.Error())
			}
			limit, err := strconv.ParseUint(ctx.Ctx.GetQueryArg("limit"), 10, 64)
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, rpc.InvalidArg(err.Error())
			}
			req.Limit = uint32(limit)
			req.LocalMsgSeq, err = strconv.ParseUint(ctx.Ctx.GetQueryArg("local_msg_seq"), 10, 64)
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, rpc.InvalidArg(err.Error())
			}
		}
	}
	if req.RobotUid == 0 || req.Limit == 0 {
		return nil, rpc.InvalidArg("missed robot uid or limit")
	}
	user, err := getUserCheck(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	// 拉下我有没有这个平台号的管理权限
	err = ensureHasRobotPerm(ctx, user, req.RobotUid)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	robotUid := req.RobotUid
	// 1. 取一下最大的 msg_seq
	top1, err := getTop1Msg(ctx, user.CorpId, user.AppId, robotUid)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	if top1 == nil {
		log.Infof("not found any msg")
		return &rsp, nil
	}
	// 2. 比对下消息差，如果过大，则只取最新的 N 条
	diff := int64(top1.MsgSeq) - int64(req.LocalMsgSeq)
	if diff <= 0 {
		// 没有新消息了
		log.Warnf("not found new msg, local seq %d, remote seq %d", req.LocalMsgSeq, top1.MsgSeq)
		return &rsp, nil
	}
	syncFrom := req.LocalMsgSeq
	if diff > maxSyncMsgCount {
		syncFrom = top1.MsgSeq - maxSyncMsgCount
		log.Warnf("diff %d too large, set sync from %d, local seq %d, remote seq %d",
			diff, syncFrom, req.LocalMsgSeq, top1.MsgSeq)
	}
	// 3. 拉新消息
	limit := req.Limit
	if limit > maxSyncMsgLimit {
		limit = maxSyncMsgLimit
	}
	err = ChoiceMsgBoxDb(MsgBox.W(user.CorpId, user.AppId, req.RobotUid).
		Where(DbMsgSeq, ">", syncFrom), user.CorpId).
		SetLimit(limit).OrderAsc(DbMsgSeq).Find(ctx, &rsp.MsgList)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	rsp.HasMore = uint32(len(rsp.MsgList)) >= limit
	log.Infof("sync return %d msg", len(rsp.MsgList))
	return &rsp, nil
}

func GetNewestMsgSeq(ctx *rpc.Context, req *qmsg.GetNewestMsgSeqReq) (*qmsg.GetNewestMsgSeqRsp, error) {
	var rsp qmsg.GetNewestMsgSeqRsp
	user, err := getUserCheck(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	// 拉下我有没有这个平台号的管理权限
	err = ensureHasRobotPerm(ctx, user, req.RobotUid)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	t, err := getTop1Msg(ctx, user.CorpId, user.AppId, req.RobotUid)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	if t != nil {
		rsp.Seq = t.MsgSeq
	}
	return &rsp, nil
}
func SendChatMsgMq(ctx *rpc.Context, req *smq.ConsumeReq) (*smq.ConsumeRsp, error) {
	return qmsg.MqSendChatMsg.Process(ctx, req, processSendChatMsg)
}

func RecvYcChatMsgMq(ctx *rpc.Context, req *smq.ConsumeReq) (*smq.ConsumeRsp, error) {
	return qrobot.MqRecvChat.Process(ctx, req, processRecvYcChatMsg)
}

func SendMsg(ctx *rpc.Context, req *qmsg.SendMsgReq) (*qmsg.SendMsgRsp, error) {
	var rsp qmsg.SendMsgRsp
	msg := req.Msg

	block, err := needBlock(ctx, msg)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	if block {
		//warning.ReportMsg(ctx, "block msg")
		log.Warnf("block msg")
		return &rsp, nil
	}

	if req.Msg.MsgType == uint32(qmsg.MsgType_MsgTypeExtContactAdded) {
		featureFlagRsp, err := featswitch.GetFeatureFlagEnabled(ctx, &featswitch.GetFeatureFlagEnabledReq{Key: "QMSG_BECOME_FRIEND_FAKE_MSG", CorpId: req.Msg.CorpId, AppId: req.Msg.AppId})
		if err != nil {
			log.Errorf("err:%v", err)
		} else if featureFlagRsp.Enabled {
			err = sendFakeMsg(ctx, req.Msg)
			if err != nil {
				log.Errorf("err %v", err)
			}
		}
	}

	chat, lastSeq, err := addMsg(ctx, msg, false)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	if chat == nil {
		return &rsp, nil
	}

	wrapperList := []*qmsg.WsMsgWrapper{
		{
			MsgType: uint32(qmsg.WsMsgWrapper_MsgTypeMsgBox),
			MsgBox:  msg,
			Chat:    chat,
			LastSeq: lastSeq,
		},
		// 兼容 1.0 ws ，日后废弃
		{
			MsgType: uint32(qmsg.WsMsgWrapper_MsgTypeMsgSeqChanged),
			MsgSeqChanged: &qmsg.WsMsgWrapper_MsgSeqChanged{
				RobotUid:     msg.Uid,
				RemoteMsgSeq: msg.MsgSeq,
			},
		},
	}

	assignChat, err := AssignChat.getByCid(ctx, chat.CorpId, chat.AppId, chat.Id)
	if err != nil && !AssignChat.IsNotFoundErr(err) {
		log.Errorf("err:%v", err)
		return nil, err
	}

	// 新增好友,修改会话分配类型为新好友会话
	if req.Msg.MsgType == uint32(qmsg.MsgType_MsgTypeExtContactAdded) &&
		assignChat != nil &&
		assignChat.AssignChatType != uint32(qmsg.ModelAssignChat_AssignChatTypeNewFriend) {
		_, err = AssignChat.WhereCorpApp(chat.CorpId, chat.AppId).
			Where(DbId, assignChat.Id).
			Update(ctx, map[string]interface{}{
				DbAssignChatType: uint32(qmsg.ModelAssignChat_AssignChatTypeNewFriend),
			})
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
	}

	err = pushWsMsgByChat(ctx, chat, assignChat, wrapperList)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	return &rsp, nil
}

func RecvUserDeleteMsgMq(ctx *rpc.Context, req *smq.ConsumeReq) (*smq.ConsumeRsp, error) {
	return quan.MqUserDeleted.Process(ctx, req, func(ctx *rpc.Context, req *smq.ConsumeReq, data interface{}) error {
		msg := data.(*quan.UserDeletedMqMsg)
		var urList []*qmsg.ModelUserRobot
		err := UserRobot.WhereCorpApp(msg.CorpId, msg.AppId).Where(DbRobotUid, msg.Uid).Find(ctx, &urList)
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}

		if len(urList) > 0 {
			for _, ur := range urList {
				if ur.Uid == 0 {
					continue
				}
				wrapper := &qmsg.WsMsgWrapper{
					MsgType: uint32(qmsg.WsMsgWrapper_MsgTypeRobotExited),
				}
				err = pushWsMsg(ctx, msg.CorpId, msg.AppId, ur.Uid, []*qmsg.WsMsgWrapper{wrapper})
				if err != nil {
					log.Errorf("err:%v", err)
					return err
				}
			}

			_, err = UserRobot.WhereCorpApp(msg.CorpId, msg.AppId).Where(DbRobotUid, msg.Uid).Delete(ctx)
			if err != nil {
				log.Errorf("err:%v", err)
				return err
			}
		}

		err = deleteAssignChatByRobotUid(ctx, msg.CorpId, msg.AppId, []uint64{msg.Uid}, qmsg.ModelAssignChat_EndSceneUserExit)
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
		return nil
	})
}

func RecvGroupChatMq(ctx *rpc.Context, req *smq.ConsumeReq) (*smq.ConsumeRsp, error) {
	return qwhale.MqGroupRecvChat.ProcessV2(ctx, req, func(ctx *rpc.Context, _ *smq.ConsumeReq, data interface{}) (*smq.ConsumeRsp, error) {
		var rsp smq.ConsumeRsp
		msg := data.(*qwhale.RecvGroupChatMsgReq)
		ycReq := msg.YcReq
		if ycReq == nil {
			return nil, rpc.InvalidArg("missed yc req")
		}
		recvData := ycReq.Data
		if recvData == nil {
			return nil, rpc.InvalidArg("missed yc req")
		}
		group := msg.Group
		if group == nil {
			// TODO 自己获取一下group
			return nil, rpc.InvalidArg("missed member")
		}
		member := msg.GroupMember
		if member == nil {
			// TODO 自己获取一下member
			return nil, rpc.InvalidArg("missed member")
		}
		log.Infof("get msg from %v to %v", recvData.SenderSerialNo, recvData.ReceiverSerialNo)

		aRsp, err := qrobot.GetAccountSys(ctx, &qrobot.GetAccountSysReq{
			CorpId: msg.CorpId,
			AppId:  msg.AppId,
			Id:     member.AccountId,
		})
		if err != nil {
			log.Errorf("err %v", err)
			return nil, err
		}

		mentionName := fmt.Sprintf("[%s]/[%s]", aRsp.Account.Name, group.Name)

		// 新逻辑！！！！
		err = handleReceiveMsg(ctx, msg.CorpId, msg.AppId, msg.RobotUid, group.Id, aRsp.Account, msg.RobotSn, msg.MsgDate, recvData, msg.SentAt, qmsg.ChatType_ChatTypeGroup, mentionName, true)
		if err != nil {
			if rpc.GetErrCode(err) == rpc.InvalidArgErrCode {
				return &rsp, nil
			}
			log.Errorf("err:%v", err)

			if rpc.GetErrCode(err) < 0 && req.CreatedAt+300 > utils.Now() {
				rsp.Retry = true
				rsp.SkipIncRetryCount = true
			}

			return &rsp, err
		}

		return &rsp, nil
	})
}
func RecvSingleChatMq(ctx *rpc.Context, req *smq.ConsumeReq) (*smq.ConsumeRsp, error) {
	return qwhale.MqSingleRecvChat.ProcessV2(ctx, req, func(ctx *rpc.Context, conReq *smq.ConsumeReq, data interface{}) (*smq.ConsumeRsp, error) {
		var rsp smq.ConsumeRsp
		msg := data.(*qwhale.RecvSingleChatMsgReq)
		ycReq := msg.YcReq
		if ycReq == nil {
			return nil, rpc.InvalidArg("missed yc req")
		}
		recvData := ycReq.Data
		if recvData == nil {
			return nil, rpc.InvalidArg("missed yc req")
		}
		receiver := msg.ReceiverAccount
		if receiver == nil {
			return nil, rpc.InvalidArg("missed receiver")
		}
		sender := msg.SenderAccount
		if sender == nil {
			return nil, rpc.InvalidArg("missed sender")
		}

		// 扔掉机器人发送的消息
		if msg.SenderAccount.YcSerialNo == msg.RobotSn {
			flagEnabled, err := featswitch.GetFeatureFlagEnabled(ctx, &featswitch.GetFeatureFlagEnabledReq{
				Key: "QMSG_NO_RECORD_MSG", CorpId: msg.CorpId, AppId: msg.AppId,
			})
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, err
			}
			if flagEnabled.Enabled {
				//// 放到慢速队列
				//pubRsp, err := MqSingleRecvChatSlow.PubV2(ctx, &smq.PubReq{
				//	Hash: utils.HashStr(msg.RobotSn),
				//}, &msg)
				//if err != nil {
				//	log.Errorf("err:%v", err)
				//	return nil, err
				//} else if pubRsp != nil {
				//	log.Infof("pub mq ret %s", pubRsp.MsgId)
				//}
				return &rsp, nil
			}
		}
		return processRecvSingleChatMq(ctx, conReq, data)
	})
}

func SlowRecvSingleChatMq(ctx *rpc.Context, req *smq.ConsumeReq) (*smq.ConsumeRsp, error) {
	return MqSingleRecvChatSlow.ProcessV2(ctx, req, processRecvSingleChatMq)
}

func processRecvSingleChatMq(ctx *rpc.Context, req *smq.ConsumeReq, data interface{}) (*smq.ConsumeRsp, error) {
	var rsp smq.ConsumeRsp
	msg := data.(*qwhale.RecvSingleChatMsgReq)
	ycReq := msg.YcReq
	if ycReq == nil {
		return nil, rpc.InvalidArg("missed yc req")
	}
	recvData := ycReq.Data
	if recvData == nil {
		return nil, rpc.InvalidArg("missed yc req")
	}
	receiver := msg.ReceiverAccount
	if receiver == nil {
		return nil, rpc.InvalidArg("missed receiver")
	}
	sender := msg.SenderAccount
	if sender == nil {
		return nil, rpc.InvalidArg("missed sender")
	}

	chatId := sender.Id
	// 扫码号手机端发送消息 由创会推送事件告知 但这里推送type错误了 变更一下sender
	if ycReq.RobotSerialNo == recvData.SenderSerialNo {
		chatId = receiver.Id
	}

	mentionName := fmt.Sprintf("[%s]", sender.Name)
	// 新逻辑！！！！
	err := handleReceiveMsg(ctx, msg.CorpId, msg.AppId, msg.RobotUid, chatId, sender, msg.RobotSn, msg.MsgDate, recvData, msg.SentAt, qmsg.ChatType_ChatTypeSingle, mentionName, msg.RobotSn != sender.YcSerialNo)
	if err != nil {
		if rpc.GetErrCode(err) == rpc.InvalidArgErrCode {
			return &rsp, nil
		}
		log.Errorf("err:%v", err)

		if rpc.GetErrCode(err) < 0 && req.CreatedAt+300 > utils.Now() {
			rsp.Retry = true
			rsp.SkipIncRetryCount = true
		}

		return &rsp, err
	}

	return &rsp, nil
}

func GetNewestMsgSeqBatch(ctx *rpc.Context, req *qmsg.GetNewestMsgSeqBatchReq) (*qmsg.GetNewestMsgSeqBatchRsp, error) {
	var rsp qmsg.GetNewestMsgSeqBatchRsp

	user, err := getUserCheck(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	// 拉下我有没有这个平台号的管理权限
	err = ensureHasRobotPerm(ctx, user, req.RobotUidList...)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	seqMap := make(map[uint64]uint64)
	for _, uid := range req.RobotUidList {
		seqMap[uid] = 0
	}
	for uid := range seqMap {
		t, err := getTop1Msg(ctx, user.CorpId, user.AppId, uid)
		if err != nil {
			log.Errorf("err:%v", err)
			continue
		}
		if t != nil {
			seqMap[uid] = t.MsgSeq
		}
	}
	rsp.SeqMap = seqMap

	return &rsp, nil
}

func AccountChangedNotifySys(ctx *rpc.Context, req *qmsg.AccountChangedNotifySysReq) (*qmsg.AccountChangedNotifySysRsp, error) {
	var rsp qmsg.AccountChangedNotifySysRsp
	return &rsp, nil
}

func ExtContactDeleteMq(ctx *rpc.Context, req *smq.ConsumeReq) (*smq.ConsumeRsp, error) {
	return quan.MqExtContactDelete.ProcessV2(ctx, req, func(ctx *rpc.Context, _ *smq.ConsumeReq, data interface{}) (*smq.ConsumeRsp, error) {
		var rsp smq.ConsumeRsp

		deleteMsg := data.(*quan.ExtContactDeleteMqMsg)

		follow, err := qrobot.GetAccountByExtUid(ctx, &qrobot.GetAccountByExtUidReq{
			CorpId: req.CorpId,
			AppId:  req.AppId,
			ExtUid: fmt.Sprintf("%v", deleteMsg.ExtUid),
		})
		if err != nil {
			if rpc.GetErrCode(err) == qrobot.ErrAccountNotFound {
				return &rsp, nil
			}
			log.Errorf("err:%v", err)
			return nil, err
		}
		msgType := uint32(qmsg.MsgType_MsgTypeExtContactDeletedByExtContact)
		if deleteMsg.DeleteByStaff {
			msgType = uint32(qmsg.MsgType_MsgTypeExtContactDeletedByStaff)
		}

		// 发送消息
		chatId := follow.Account.GetId()
		msg := qmsg.ModelMsgBox{
			CorpId:   req.CorpId,
			AppId:    req.AppId,
			Uid:      deleteMsg.Uid,
			CliMsgId: "qrobot_ext_contact_delete_" + utils.GenRandomStr(),
			MsgType:  msgType,
			ChatType: uint32(qmsg.ChatType_ChatTypeSingle),
			ChatId:   chatId,
		}
		chat, lastSeq, err := addMsg(ctx, &msg, false)
		if err != nil {
			log.Errorf("err:%v", err)

			if rpc.GetErrCode(err) < 0 && req.CreatedAt+300 > utils.Now() {
				rsp.Retry = true
				rsp.SkipIncRetryCount = true
			}

			return &rsp, err
		}

		if chat == nil {
			return &rsp, nil
		}

		updateChatFormFollow(chat, deleteMsg.Follow)
		if deleteMsg.DeleteByStaff {
			chat.Detail.DeletedByStaffAt = deleteMsg.CreateTime
		} else {
			chat.Detail.DeletedByExtContactAt = deleteMsg.CreateTime
		}
		_, err = Chat.updateMap(ctx, req.CorpId, req.AppId, chat.Id, map[string]interface{}{
			DbDetail: chat.Detail,
		})
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
		wrapperList := []*qmsg.WsMsgWrapper{
			// 兼容 1.0 ws ，日后废弃
			{
				MsgType: uint32(qmsg.WsMsgWrapper_MsgTypeMsgSeqChanged),
				MsgSeqChanged: &qmsg.WsMsgWrapper_MsgSeqChanged{
					RobotUid:     msg.Uid,
					RemoteMsgSeq: msg.MsgSeq,
				},
			},
			{
				MsgType: uint32(qmsg.WsMsgWrapper_MsgTypeExtContactChange),
				Chat:    chat,
				LastSeq: lastSeq,
			},
		}
		err = pushWsMsgByChat(ctx, chat, nil, wrapperList)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
		return &rsp, nil
	})
}

func GetChatMsgBoxList(ctx *rpc.Context, req *qmsg.GetChatMsgBoxListReq) (*qmsg.GetChatMsgBoxListRsp, error) {
	var rsp qmsg.GetChatMsgBoxListRsp

	db := MsgBox.NewList(req.ListOption).Where(DbUid, req.Uid).Where(DbChatId, req.ChatId).OrderDesc(DbMsgSeq)

	err := core.NewListOptionProcessor(req.ListOption).
		Process()

	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	rsp.Paginate, err = db.FindPaginate(ctx, &rsp.List)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	return &rsp, nil
}

func GetSendInterval(ctx *rpc.Context, req *ext.RawReq) (*ext.RawRsp, error) {
	now := time.Now().UnixNano() / 1e6

	intervalMap, err := s.RedisGroup.HGetAll(RobotUidMsgId)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	timeMap := map[int]int{
		1: 0,
		2: 0,
		3: 0,
		4: 0,
		5: 0,
	}

	for _, value := range intervalMap {
		var analyze qmsg.UidMsgIdAnalyze
		err = proto.Unmarshal([]byte(value), &analyze)
		if err != nil {
			log.Errorf("err:%s", err)
			return nil, err
		}

		// 大于1小时
		if now-analyze.CreatedAt > 1000*60*60 {
			// 大于1小时
			timeMap[1]++
		} else if now-analyze.CreatedAt > 1000*60*30 {
			// 大于半个钟
			timeMap[2]++
		} else if now-analyze.CreatedAt > 1000*60*10 {
			// 大于10分钟
			timeMap[3]++
		} else if now-analyze.CreatedAt > 1000*60 {
			// 大于1分钟
			timeMap[4]++
		} else if now-analyze.CreatedAt > 1000*10 {
			// 大于10秒
			timeMap[5]++
		}
	}

	log.Infof("larger than 1 hour cnt:%d", timeMap[1])
	log.Infof("larger than half hour cnt:%d", timeMap[2])
	log.Infof("larger than 10 minutes cnt:%d", timeMap[3])
	log.Infof("larger than 1 minute cnt:%d", timeMap[4])
	log.Infof("larger than 10 seconds cnt:%d", timeMap[5])

	return nil, nil
}

func CheckRobotHasSendMsgToAccountSys(ctx *rpc.Context, req *qmsg.CheckRobotHasSendMsgToAccountSysReq) (*qmsg.CheckRobotHasSendMsgToAccountSysRsp, error) {
	var rsp qmsg.CheckRobotHasSendMsgToAccountSysRsp
	db := ChoiceMsgBoxDb(MsgBox.W(req.CorpId, req.AppId, req.RobotUid), req.CorpId).
		Where(DbChatType, qmsg.ChatType_ChatTypeSingle).
		Where(DbChatId, req.ExtAccountId).
		Where(DbSenderAccountId, "!=", req.ExtAccountId)
	if req.AfterTime > 0 {
		db.Where(DbSentAt, ">", req.AfterTime)
	}
	res, err := db.IsEmpty(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	rsp.HasSend = !res
	return &rsp, nil
}

func GetChatSessionByIframe(ctx *rpc.Context, req *qmsg.GetChatSessionByIframeReq) (*qmsg.GetChatSessionByIframeRsp, error) {

	var (
		rsp         qmsg.GetChatSessionByIframeRsp
		robotUid    = req.RobotUid
		strOpenId   = strings.Trim(req.StrOpenId, " ")
		extUserId   = strings.Trim(req.ExtUserId, " ")
		groupId     = req.GroupId
		groupOpenId = strings.Trim(req.GroupStrOpenId, " ")
		// 默认等于群聊 ID
		chatExtId = req.GroupId
		isGroup   = true
	)

	currentUser, err := GetCurrentUser(ctx, &qmsg.GetCurrentUserReq{})
	if err != nil {
		log.Warnf("err:%v , customer service have err", err)
		return nil, rpc.CreateErrorWithMsg(int32(rpc.RecordNotFound), fmt.Sprintf("err:%v, customer service have err", err))
	}

	//// 目前只支持分配模式
	//if !currentUser.User.IsAssign {
	//	return nil, rpc.CreateErrorWithMsg(int32(qmsg.ErrChatNotFound), fmt.Sprintf("this customer service not have assign chat"))
	//}

	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)

	// 因为是对外接口，所以第三方传入 robotUid 或 strOpenId 都可以，如果都不传就不行。
	if robotUid == 0 && len(strOpenId) == 0 {
		return nil, rpc.CreateErrorWithMsg(int32(qmsg.ErrChatNotFound), fmt.Sprintf("not have robotUid and strOpenId"))
	}

	if robotUid == 0 {
		idRsp, err := qrobot.GetAccountByStrOpenId(ctx, &qrobot.GetAccountByStrOpenIdReq{AppId: appId, CorpId: corpId, StrOpenId: strOpenId})
		if err != nil {
			log.Warnf("err:%v", err)
			return nil, rpc.CreateErrorWithMsg(int32(qmsg.ErrChatNotFound), fmt.Sprintf("not find robot by str_open_id"))
		}
		robotUid = idRsp.Account.Uid
	}

	// 确认一下这个机器人是否属于当前客服。
	uidList := currentUser.User.ContactFilter.FilterUser.UidList
	uint64Map := utils.SliceToUint64Map(uidList)
	if currentUser.User.IsAssign && !uint64Map[robotUid] {
		return nil, rpc.CreateErrorWithMsg(int32(qmsg.ErrChatNotFound), fmt.Sprintf("this robot not belong to current customer service"))
	}

	if groupId == 0 && groupOpenId == "" {
		extContactRsp, err := iquan.GetExtContact(ctx, &iquan.GetExtContactReq{ExtUserId: extUserId, CorpId: corpId, AppId: appId})
		if err != nil {
			log.Warnf("err:%v , ext_user_id : %v", err, extUserId)
			return nil, rpc.CreateErrorWithMsg(int32(qmsg.ErrChatNotFound), fmt.Sprintf("not find this ext_user"))
		}

		extUid := fmt.Sprintf("%d", extContactRsp.ExtContact.Id)
		accountByExtUid, err := qrobot.GetAccountByExtUid(ctx, &qrobot.GetAccountByExtUidReq{CorpId: corpId, AppId: appId, ExtUid: extUid})
		if err != nil {
			log.Warnf("err:%v ", err)
			return nil, rpc.CreateErrorWithMsg(int32(qmsg.ErrChatNotFound), fmt.Sprintf("not find this account"))
		}

		chatExtId = accountByExtUid.Account.Id
		isGroup = false
	} else if groupOpenId != "" {
		// 获取群信息
		getGroupChatListSysRsp, err := qrobot.GetGroupChatListSys(ctx, &qrobot.GetGroupChatListSysReq{
			CorpId: corpId,
			AppId:  appId,
			ListOption: core.NewListOption().SetLimit(1).SetSkipCount().
				AddOpt(qrobot.GetGroupChatListSysReq_ListOptionStrOpenId, groupOpenId),
		})
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, rpc.CreateErrorWithMsg(int32(qmsg.ErrChatNotFound), fmt.Sprintf("not find this group"))
		}
		if len(getGroupChatListSysRsp.List) == 0 {
			return nil, rpc.CreateErrorWithMsg(int32(qmsg.ErrChatNotFound), fmt.Sprintf("not find this group"))
		}
		groupId = getGroupChatListSysRsp.List[0].Id
		chatExtId = groupId
	}

	chat, assignChat, err := getChat(ctx, corpId, appId, robotUid, chatExtId, isGroup, true)
	if err != nil {
		log.Warnf("getChat have a err:%v , robotUid is %v , chatExtId is %v ,isGroup is %v", err, robotUid, chatExtId, isGroup)
		return nil, rpc.CreateErrorWithMsg(int32(qmsg.ErrChatNotFound), fmt.Sprintf("err:%v ,not find this chat", err))
	}

	cnt, err := UserRobot.WhereCorpApp(corpId, appId).Where(DbUid, currentUser.User.Id).Where(DbRobotUid, robotUid).Count(ctx)
	if err != nil {
		log.Errorf("err %v", err)
		return nil, err
	}

	//资产，没有机器人权限
	if cnt == 0 && currentUser.User.IsAssign {
		log.Warnf("getChat have a err:%v , robotUid is %v , chatExtId is %v ,isGroup is %v", err, robotUid, chatExtId, isGroup)
		return nil, rpc.CreateErrorWithMsg(int32(qmsg.ErrChatNotFound), fmt.Sprintf("err:%v , customer server not have robot", err))
	}

	//非资产，没有分配会话
	if !currentUser.User.IsAssign && assignChat == nil {
		log.Warnf("getChat have a err:%v , robotUid is %v , chatExtId is %v ,isGroup is %v", err, robotUid, chatExtId, isGroup)
		return nil, rpc.CreateErrorWithMsg(int32(qmsg.ErrChatNotFound), fmt.Sprintf("err:%v , chat not assign to customer server", err))
	}

	rsp.Chat = chat
	return &rsp, nil
}

func GetNextSeq(ctx *rpc.Context, req *qmsg.GetNextSeqReq) (*qmsg.GetNextSeqRsp, error) {
	var rsp qmsg.GetNextSeqRsp

	resp, err := s.SeqGen.SubmitAndWait(ctx, &ActionExec{DataIn: req.Uid})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	rsp.NextSeq = resp.DataOut.(uint64)

	return &rsp, nil
}

// SendSystemMsg 客服工作台发送系统消息
func SendSystemMsg(ctx *rpc.Context, req *qmsg.SendSystemMsgReq) (*qmsg.SendSystemMsgRsp, error) {
	var rsp qmsg.SendSystemMsgRsp

	if len(req.CustomerServiceAccountList) == 0 && len(req.RobotIdList) == 0 {
		// 客服列表和机器人列表长度都为0则返回失败
		return nil, rpc.CreateErrorWithMsg(qmsg.ErrCustomerServiceAccountListAndRobotIdListIsNull, fmt.Sprintf("客服账号列表和机器人列表为空"))
	}

	// 最终需要批量插入的数据列表
	var insertRecordList []*qmsg.ModelSystemMsgRecord

	// 机器人map
	robotMap := map[string]*qris.Robot{}
	// 机器人信息列表
	var getRobotList []*qris.Robot
	// 机器人sn
	var ycSnList []string
	if len(req.RobotIdList) != 0 {

		// 获取机器人信息
		getRobotListRsp, err := qris.GetRobotList(ctx, &qris.GetRobotListReq{
			CorpId:     req.CorpId,
			AppId:      req.AppId,
			ListOption: core.NewListOption().AddOpt(qris.GetRobotListReq_ListOptionRobotSnList, req.RobotIdList),
		})
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
		if len(getRobotListRsp.List) != 0 {
			getRobotList = getRobotListRsp.List
			ycSnList = utils.PluckString(getRobotListRsp.List, "YcSn")
			for _, v := range getRobotListRsp.List {
				robotMap[v.YcSn] = v
			}

		}

	}

	// 客服账号列表
	var userList []*qmsg.ModelUser
	// 客服账号map
	userMap := map[string]*qmsg.ModelUser{}
	// 定义客服账号id列表
	var uidList []uint64
	if len(req.CustomerServiceAccountList) != 0 {
		// 获取企业对应账号的账号列表
		err := User.WhereCorpApp(req.CorpId, req.AppId).WhereIn(DbLoginId, req.CustomerServiceAccountList).Find(ctx, &userList)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
		// 获取对应的userMap
		userMap = map[string]*qmsg.ModelUser{}
		for _, user := range userList {
			userMap[user.LoginId] = user
		}

	}

	if len(req.CustomerServiceAccountList) != 0 && len(req.RobotIdList) == 0 {
		// 则客服账号列表不为空，所填客服账号下都会增加一条消息记录
		userLoginIdList := utils.PluckString(userList, "LoginId")
		for _, customerServiceAccount := range req.CustomerServiceAccountList {
			// 判断请求的客服账号是否存在,不存在则添加到失败列表中
			if !utils.InSliceStr(customerServiceAccount, userLoginIdList) {
				rsp.FailList = append(rsp.FailList, &qmsg.SendSystemMsgRsp_FailItem{
					CustomerServiceAccount: customerServiceAccount,
					FailReason:             fmt.Sprintf("ErrUserNotFound %v", rpc.CreateError(qmsg.ErrUserNotFound)),
				})
				continue
			}

			// 存在,添加到插入列表
			insertRecordList = append(insertRecordList, &qmsg.ModelSystemMsgRecord{
				Uid:      userMap[customerServiceAccount].Id,
				Title:    req.MsgTitle,
				Content:  req.MsgContent,
				TextDesc: req.TextDesc,
				Status:   uint32(qmsg.ModelSystemMsgRecord_statusTypeNil),
				IsRead:   false,
			})
		}
	}

	if len(req.CustomerServiceAccountList) == 0 && len(req.RobotIdList) != 0 {
		// 则机器人编号列表不为空，所填机器人下绑定的所有客服账号都会增加一条聊天记录
		// 判断请求的机器人编号是否存在
		for _, robotId := range req.RobotIdList {
			// 不存在就加入到失败列表中
			if !utils.InSliceStr(robotId, ycSnList) {
				rsp.FailList = append(rsp.FailList, &qmsg.SendSystemMsgRsp_FailItem{
					RobotId:    robotId,
					FailReason: fmt.Sprintf("%v", rpc.CreateError(qris.ErrRobotNotFound)),
				})
				continue
			}

			var userRobotList []*qmsg.ModelUserRobot
			// 找出该机器人下的客服账号
			err := UserRobot.WhereCorpApp(req.CorpId, req.AppId).Where(DbRobotUid, robotMap[robotId].Uid).Find(ctx, &userRobotList)
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, err
			}
			// 获取客服账号id列表
			uidList = utils.PluckUint64(userRobotList, "Uid")

			var newUserList []*qmsg.ModelUser
			err = User.WhereCorpApp(req.CorpId, req.AppId).WhereIn(DbId, uidList).Find(ctx, &newUserList)
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, err
			}

			userIdMap := map[uint64]*qmsg.ModelUser{}
			for _, v := range newUserList {
				userIdMap[v.Id] = v
			}

			for _, uid := range uidList {
				// 判断该客服是否存在
				_, ok := userIdMap[uid]
				if !ok {
					rsp.FailList = append(rsp.FailList, &qmsg.SendSystemMsgRsp_FailItem{
						RobotId:    robotId,
						FailReason: fmt.Sprintf("%v", rpc.CreateError(qmsg.ErrUserNotFound)),
					})
					continue
				}
				// 存在添加到插入列表
				insertRecordList = append(insertRecordList, &qmsg.ModelSystemMsgRecord{
					Uid:      userIdMap[uid].Id,
					Title:    req.MsgTitle,
					Content:  req.MsgContent,
					TextDesc: req.TextDesc,
					Status:   uint32(qmsg.ModelSystemMsgRecord_statusTypeNil),
					IsRead:   false,
				})
			}
		}

	}

	if len(req.CustomerServiceAccountList) != 0 && len(req.RobotIdList) != 0 {
		// 说明RobotIdList和CustomerServiceAccountList 都不为空，则依次判断所填客服账号是否包含所填机器人

		for _, customerServiceAccount := range req.CustomerServiceAccountList {
			// 客服账号不存在则加入到失败列表
			_, ok := userMap[customerServiceAccount]
			if !ok {
				rsp.FailList = append(rsp.FailList, &qmsg.SendSystemMsgRsp_FailItem{
					CustomerServiceAccount: customerServiceAccount,
					FailReason:             fmt.Sprintf("%v", rpc.CreateError(qmsg.ErrUserNotFound)),
				})
				continue
			}

			// 存在则找出客服账号对应的机器人列表
			var robotUidList []*qmsg.ModelUserRobot
			err := UserRobot.WhereCorpApp(req.CorpId, req.AppId).Where(DbUid, userMap[customerServiceAccount].Id).Find(ctx, &robotUidList)
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, err
			}
			newRobotUidList := utils.PluckUint64(robotUidList, "RobotUid")
			// 获取请求的机器人列表
			oldRobotUidList := utils.PluckUint64(getRobotList, "Uid")

			// 求两个列表的交集
			insertRobotUidList := utils.IntersectUint64(newRobotUidList, oldRobotUidList)
			if len(insertRobotUidList) == 0 {
				rsp.FailList = append(rsp.FailList, &qmsg.SendSystemMsgRsp_FailItem{
					CustomerServiceAccount: customerServiceAccount,
					FailReason:             fmt.Sprintf("客服账号所绑定的机器人未在所填的机器人列表中找到"),
				})
				continue
			}

			// 交集不为空则插入到记录表中
			insertRecordList = append(insertRecordList, &qmsg.ModelSystemMsgRecord{
				Uid:      userMap[customerServiceAccount].Id,
				Title:    req.MsgTitle,
				Content:  req.MsgContent,
				TextDesc: req.TextDesc,
				Status:   uint32(qmsg.ModelSystemMsgRecord_statusTypeNil),
				IsRead:   false,
			})
		}

	}
	// 对插入的数据进行去重
	newInsertRecordList := utils.UniqueSlice(insertRecordList).([]*qmsg.ModelSystemMsgRecord)
	// 往SystemMsgRecord表中批量插入数据
	err := SystemMsgRecord.BatchCreate(ctx, newInsertRecordList)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	// 获取插入数据的客服账号列表
	newCustomerServiceAccountIdList := utils.PluckUint64(newInsertRecordList, "Uid")
	var insertUserList []*qmsg.ModelUser
	err = User.WhereCorpApp(req.CorpId, req.AppId).WhereIn(DbId, newCustomerServiceAccountIdList).Find(ctx, &insertUserList)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	insertUserMap := map[uint64]*qmsg.ModelUser{}
	for _, insertUser := range insertUserList {
		insertUserMap[insertUser.Id] = insertUser
	}

	newInsertRecordIdList := utils.PluckUint64(newInsertRecordList, "Id")
	// 获取插入的数据
	err = SystemMsgRecord.NewScope().WhereIn(DbId, newInsertRecordIdList).Find(ctx, &newInsertRecordList)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	for _, systemMsgRecord := range newInsertRecordList {
		// 这里做一下校验，防止错panic
		_, ok := insertUserMap[systemMsgRecord.Uid]
		if !ok {
			log.Warnf("systemMsgRecord is nil ,skip")
			continue
		}
		// 判断插入的数据客服账号是否在线，如果在线则往ws推数据
		if insertUserMap[systemMsgRecord.Uid].IsOnline {
			// 往ws中推数据
			err = pushWsMsg(ctx, req.CorpId, req.AppId, insertUserMap[systemMsgRecord.Uid].Id, []*qmsg.WsMsgWrapper{
				{
					MsgType:         uint32(qmsg.WsMsgWrapper_MsgTypeSystemMsg),
					SystemMsgRecord: systemMsgRecord,
				},
			})
			if err != nil {
				// 错误不为空则变更发送状态为发送失败、记录失败原因
				_, err := SystemMsgRecord.NewScope().Where(DbId, systemMsgRecord.Id).Update(ctx, map[string]interface{}{
					DbStatus:     qmsg.ModelSystemMsgRecord_statusTypeFailed,
					DbFailReason: fmt.Sprintf("%v", err),
				})
				if err != nil {
					log.Errorf("err:%v", err)
					return nil, err
				}
				log.Errorf("err:%v", err)
				return nil, err
			}
			// 错误为空则变更发送状态为发送成功
			_, err = SystemMsgRecord.NewScope().Where(DbId, systemMsgRecord.Id).Update(ctx, map[string]interface{}{
				DbStatus: qmsg.ModelSystemMsgRecord_statusTypeSuccess,
			})
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, err
			}
			// 添加到成功列表中
			rsp.SuccessList = append(rsp.SuccessList, &qmsg.SendSystemMsgRsp_SuccessItem{
				CustomerServiceAccount: insertUserMap[systemMsgRecord.Uid].LoginId,
			})
			continue
		}
		// 客服账号不在线，变更发送状态发送失败、记录失败原因
		_, err := SystemMsgRecord.NewScope().Where(DbId, systemMsgRecord.Id).Update(ctx, map[string]interface{}{
			DbStatus:     qmsg.ModelSystemMsgRecord_statusTypeFailed,
			DbFailReason: fmt.Sprintf("%v", rpc.CreateError(qmsg.ErrAccountIsNotOnline)),
		})
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
		rsp.FailList = append(rsp.FailList, &qmsg.SendSystemMsgRsp_FailItem{
			CustomerServiceAccount: insertUserMap[systemMsgRecord.Uid].LoginId,
			FailReason:             fmt.Sprintf("%v", rpc.CreateError(qmsg.ErrAccountIsNotOnline)),
		})

	}

	return &rsp, nil
}

func GetSystemMsgList(ctx *rpc.Context, req *qmsg.GetSystemMsgListReq) (*qmsg.GetSystemMsgListRsp, error) {
	var rsp qmsg.GetSystemMsgListRsp
	uid, err := core.GetPinHeaderUId2Int(ctx)
	if err != nil {
		log.Errorf("err %v", err)
		return nil, err
	}

	db := SystemMsgRecord.NewList(req.ListOption).Where(DbUid, uid)

	err = core.NewListOptionProcessor(req.ListOption).
		AddUint32(uint32(qmsg.GetSystemMsgListReq_ListOptionReadState), func(val uint32) error {

			//根据读取状态查询
			switch val {
			case uint32(qmsg.GetSystemMsgListReq_ReadStateTrue):
				db.Where(DbIsRead, true)
			case uint32(qmsg.GetSystemMsgListReq_ReadStateFalse):
				db.Where(DbIsRead, false)
			default:
				//默认全部
			}

			return nil
		}).AddBool(uint32(qmsg.GetSystemMsgListReq_ListOptionQueryId), func(val bool) error {
		//仅查id
		if val {
			db.Select("id")
		}
		return nil
	}).AddBool(uint32(qmsg.GetSystemMsgListReq_ListOptionCreatedAtDesc), func(val bool) error {
		if val {
			db.OrderDesc(DbCreatedAt)
		}
		return nil
	}).Process()

	if err != nil {
		log.Errorf("err %v", err)
		return nil, err
	}

	rsp.Paginate, err = db.FindPaginate(ctx, &rsp.List)
	return &rsp, nil
}

func BatchUpdateSystemMsg(ctx *rpc.Context, req *qmsg.BatchUpdateSystemMsgReq) (*qmsg.BatchUpdateSystemMsgRsp, error) {
	var rsp qmsg.BatchUpdateSystemMsgRsp

	uid, err := core.GetPinHeaderUId2Int(ctx)
	if err != nil {
		log.Errorf("err %v", err)
		return nil, err
	}

	db := SystemMsgRecord.Where(DbUid, uid)
	err = core.NewListOptionProcessor(req.ListOption).
		AddUint32(uint32(qmsg.GetSystemMsgListReq_ListOptionReadState), func(val uint32) error {

			//根据读取状态查询
			switch val {
			case uint32(qmsg.GetSystemMsgListReq_ReadStateTrue):
				db.Where(DbIsRead, true)
			case uint32(qmsg.GetSystemMsgListReq_ReadStateFalse):
				db.Where(DbIsRead, false)
			default:
				//默认全部
			}

			return nil
		}).Process()
	if err != nil {
		log.Errorf("err %v", err)
		return nil, err
	}

	updateMap := map[string]interface{}{}
	switch req.UpdateType {
	case uint32(qmsg.BatchUpdateSystemMsgReq_UpdateTypeRead), uint32(qmsg.BatchUpdateSystemMsgReq_UpdateTypeReadAll):
		//批量已读
		updateMap[DbIsRead] = true

	case uint32(qmsg.BatchUpdateSystemMsgReq_UpdateTypeDel), uint32(qmsg.BatchUpdateSystemMsgReq_UpdateTypeReadStateDel):
		//软删
		updateMap[DbDeletedAt] = uint32(time.Now().Unix())

	default:

		//需填更新类型
		return nil, rpc.CreateError(qmsg.ErrBatchUpdateSystemMsgTypeRequired)
	}

	//不是全选已读或者软删,需必填IdList
	if req.UpdateType == uint32(qmsg.BatchUpdateSystemMsgReq_UpdateTypeRead) ||
		req.UpdateType == uint32(qmsg.BatchUpdateSystemMsgReq_UpdateTypeDel) {
		if len(req.IdList) == 0 {
			return nil, rpc.CreateError(qmsg.ErrBatchUpdateSystemMsgTypeIdListIsNull)
		}

	} else {

		//获取所有需要更新的id列表
		var recordList []*qmsg.ModelSystemMsgRecord
		if err = db.
			Select(DbId).
			Find(ctx, &recordList); err != nil {
			log.Errorf("err %v", err)
			return nil, err
		}

		if len(recordList) == 0 {
			return nil, nil
		}
		req.IdList = utils.PluckUint64(recordList, "Id")
	}

	var chunkErr error
	pie.Uint64s(req.IdList).Chunk(2000, func(chunk pie.Uint64s) (stopped bool) {
		if chunkErr != nil {
			return
		}

		//避免量太大切片更新
		_, err = SystemMsgRecord.NewScope().Where(DbUid, uid).WhereIn(DbId, chunk).Update(ctx, updateMap)
		if err != nil {
			chunkErr = err
			log.Errorf("err %v", err)
			return
		}
		return
	})
	if chunkErr != nil {
		log.Errorf("err %v", err)
		return nil, chunkErr
	}

	return &rsp, nil
}

func GetSystemMsgUnReadCount(ctx *rpc.Context, req *qmsg.GetSystemMsgUnReadCountReq) (*qmsg.GetSystemMsgUnReadCountRsp, error) {
	var rsp qmsg.GetSystemMsgUnReadCountRsp
	uid, err := core.GetPinHeaderUId2Int(ctx)
	if err != nil {
		log.Errorf("err %v", err)
		return nil, err
	}

	rsp.UnReadCount, err = SystemMsgRecord.Where(DbUid, uid).Where(DbIsRead, false).Count(ctx)
	if err != nil {
		log.Errorf("err", err)
		return nil, err
	}

	return &rsp, nil
}

func ExportRecallMsgList(ctx *rpc.Context, req *qmsg.ExportRecallMsgListReq) (*qmsg.ExportRecallMsgListRsp, error) {
	var rsp qmsg.ExportRecallMsgListRsp
	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)

	// 开始异步导出任务
	asyncTaskReq := &commonv2.AsyncTaskReq{
		IsAddTaskList:      true,
		TaskKeyPre:         "export_recall_msg_list",
		MaxExecTimeSeconds: 0,
		AppType:            0,
		TaskType:           uint32(commonv2.TaskType_TaskTypeExport),
		TaskName:           "企业撤回消息列表导出" + utils.TimeStamp2DateYmdhi(utils.Now()),
	}

	taskKey, err := commonv2.ExportExcelV3(ctx, "quan_pub", "企业撤回消息列表.xlsx", asyncTaskReq, func(ctx *rpc.Context, w *csv.Writer) error {
		// 一次性最大限制导出两万条数据
		req.ListOption.SetLimit(20000)

		db := MsgBox.NewList(req.ListOption).WhereCorpApp(corpId, appId).Where(DbMsgType, qmsg.MsgType_MsgTypeRecall).OrderDesc(DbCreatedAt)
		var isGroup bool
		switch req.ChatType {
		case uint32(qmsg.ChatType_ChatTypeSingle):
			db.Where(DbChatType, qmsg.ChatType_ChatTypeSingle)
		case uint32(qmsg.ChatType_ChatTypeGroup):
			db.Where(DbChatType, qmsg.ChatType_ChatTypeGroup)
			isGroup = true
		}

		var beginTime uint32
		var endTime uint32
		db = ChoiceMsgBoxDb(db, corpId)
		err := core.NewListOptionProcessor(req.ListOption).
			AddTimeStampRange(
				qmsg.ExportRecallMsgListReq_ListOptionSentAt,
				func(beginAt, endAt uint32) error {
					//db.Where(fmt.Sprintf("? < %s AND %s < ?", DbCreatedAt, DbCreatedAt), beginAt, endAt)
					beginTime = beginAt
					endTime = endAt
					return nil
				}).
			AddUint64List(
				qmsg.ExportRecallMsgListReq_ListOptionRobotUidList,
				func(valList []uint64) error {
					db.WhereIn(DbUid, valList)
					return nil
				}).
			Process()
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}

		// 深拷贝函数，用于获取一个Scope的副本
		deepCopyScope := func(scope dbx.Scope) *dbx.Scope {
			copyScope := scope
			return &copyScope
		}

		// 在这里进行分段查询
		var msgBoxList []*qmsg.ModelMsgBox
		var endTimeTmp uint32
		beginTimeTmp := beginTime
		for {

			// 当开始时间大于或等于结束时间，则退出循环
			if beginTimeTmp >= endTime {
				break
			}

			// 如果开始时间 + 5天大于结束时间，则取结束时间，否则取开始时间 + 5天
			if beginTimeTmp+5*24*3600 > endTime {
				endTimeTmp = endTime
			} else {
				endTimeTmp = beginTimeTmp + 5*24*3600
			}

			tempDb := deepCopyScope(*db)

			log.Infof("开始查询 %v 到 %v 的撤回消息数据", beginTimeTmp, endTimeTmp)
			tempDb.Where(fmt.Sprintf("? <= %s AND %s <= ?", DbCreatedAt, DbCreatedAt), beginTimeTmp, endTimeTmp)

			var msgBoxListTmp []*qmsg.ModelMsgBox
			_, err = tempDb.FindPaginate(ctx, &msgBoxListTmp)
			if err != nil {
				log.Errorf("err:%v", err)
				return err
			}

			if len(msgBoxListTmp) != 0 {
				msgBoxList = append(msgBoxList, msgBoxListTmp...)
			}

			beginTimeTmp = endTimeTmp
		}

		// 为空则返回
		if len(msgBoxList) == 0 {
			log.Infof("msgBoxList 's length is zero")
			return nil
		}

		chatIdList := utils.PluckUint64(msgBoxList, "ChatId")

		// 获取机器人信息
		robotUidList := utils.PluckUint64(msgBoxList, "Uid")
		userRsp, err := quan.GetUserList(ctx, &quan.GetUserListReq{
			ListOption:   core.NewListOption().AddOpt(quan.GetUserListReq_ListOptionUidList, robotUidList),
			OnlyUserList: true,
		})
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
		robotMap := make(map[uint64]*quan.ModelUser)
		utils.KeyBy(userRsp.List, "Id", &robotMap)

		// 获取群聊/客户 信息 key:chatId
		groupChatMapByChatId := make(map[uint64]*qrobot.ModelGroupChat)
		accountMapKeyByChatId := make(map[uint64]*qrobot.ModelAccount)
		if isGroup {
			// 去查询群聊信息
			getGroupChatListSysRsp, err := qrobot.GetGroupChatListSys(ctx, &qrobot.GetGroupChatListSysReq{
				CorpId:     corpId,
				AppId:      appId,
				ListOption: core.NewListOption(qrobot.GetGroupChatListSysReq_ListOptionGroupIdList, chatIdList),
			})
			if err != nil {
				log.Errorf("err:%v", err)
				return err
			}
			if len(getGroupChatListSysRsp.List) == 0 {
				return rpc.CreateError(qmsg.ErrLenZero)
			}

			for _, groupChat := range getGroupChatListSysRsp.List {
				if _, ok := groupChatMapByChatId[groupChat.Id]; ok {
					continue
				}
				groupChatMapByChatId[groupChat.Id] = groupChat
			}

		} else {
			// 去查询客户信息
			getAccountListSysRsp, err := qrobot.GetAccountListSys(ctx, &qrobot.GetAccountListSysReq{
				CorpId:     corpId,
				AppId:      appId,
				ListOption: core.NewListOption().AddOpt(qrobot.GetAccountListSysReq_ListOptionIdList, chatIdList),
			})
			if err != nil {
				log.Errorf("err:%v", err)
				return err
			}
			if len(getAccountListSysRsp.List) == 0 {
				return rpc.CreateError(qmsg.ErrLenZero)
			}

			for _, account := range getAccountListSysRsp.List {
				if _, ok := accountMapKeyByChatId[account.Id]; ok {
					continue
				}
				accountMapKeyByChatId[account.Id] = account
			}
		}

		rows := [][]string{{}}
		rows = [][]string{{"撤回时间", "客户名称/群名称", "撤回成员", "撤回消息类型", "撤回内容"}}
		if isGroup {
			rows[0][1] = "群名称"
		} else {
			rows[0][1] = "客户名称"
		}

		for _, msgItem := range msgBoxList {
			var rowData []string
			// 处理客户名称/群名称
			var name string
			if isGroup {
				groupChat, ok := groupChatMapByChatId[msgItem.ChatId]
				if ok {
					name = groupChat.Name
				}
			} else {
				account, ok := accountMapKeyByChatId[msgItem.ChatId]
				if ok {
					name = account.Name
				}
			}

			var robotName string
			robot, ok := robotMap[msgItem.Uid]
			if ok {
				robotName = robot.WwUserName
			}

			var msgType string
			var content string
			// 根据消息类型做判断,转换消息类型和内容
			switch msgItem.ChatMsgType {
			case uint32(qmsg.ChatMsgType_ChatMsgTypeText):
				msgType = "文本"
				content = msgItem.Msg
			case uint32(qmsg.ChatMsgType_ChatMsgTypeImage):
				msgType = "图片"
				content = msgItem.Msg
			case uint32(qmsg.ChatMsgType_ChatMsgTypeWxApp):
				msgType = "小程序"
				var miniProgram qmsg.ChatWxAppMsg
				err := utils.Json2Pb(msgItem.Msg, &miniProgram)
				if err != nil {
					log.Errorf("err:%v", err)
					return err
				}
				content = miniProgram.Title
			case uint32(qmsg.ChatMsgType_ChatMsgTypeWeb):
				msgType = "链接"
				var webMsg qmsg.ChatWebMsg
				err := utils.Json2Pb(msgItem.Msg, &webMsg)
				if err != nil {
					log.Errorf("err:%v", err)
					return err
				}
				content = webMsg.Href
			case uint32(qmsg.ChatMsgType_ChatMsgTypeChannelMsg):
				msgType = "视频号"
				var chatChannel qmsg.ChatChannelMsg
				err := utils.Json2Pb(msgItem.Msg, &chatChannel)
				if err != nil {
					log.Errorf("err:%v", err)
					return err
				}
				content = chatChannel.Title
			case uint32(qmsg.ChatMsgType_ChatMsgTypeVideo):
				msgType = "视频"
				var video qmsg.ChatVideoMsg
				err := utils.Json2Pb(msgItem.Msg, &video)
				if err != nil {
					log.Errorf("err:%v", err)
					return err
				}
				content = video.VideoUrl
			default:
				msgType = "其他"
				content = msgItem.Msg
			}

			rowData = append(rowData,
				time.Unix(int64(msgItem.UpdatedAt), 0).Format("2006-01-02 15:04:05"),
				name,
				robotName,
				msgType,
				content,
			)

			rows = append(rows, rowData)
		}

		// 写数据
		for _, row := range rows {
			err := w.Write(row)
			if err != nil {
				log.Errorf("err:%v", err)
				continue
			}
		}
		return nil
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	rsp.TaskKey = taskKey
	return &rsp, nil
}

func DeleteChatMsgMq(ctx *rpc.Context, req *smq.ConsumeReq) (*smq.ConsumeRsp, error) {
	var processCallbackRetryMq = func(ctx *rpc.Context, _ *smq.ConsumeReq, data interface{}) (*smq.ConsumeRsp, error) {
		var rsp smq.ConsumeRsp
		//log.Infof("DeleteChatMsgMq ------------------------ data:%v", data)
		var mqMsg qmsg.DeleteChatMsgReq
		err := json.Unmarshal(req.Data, &mqMsg)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}

		for {
			// 清除之前旧成员的聊天记录
			var msgBox []*qmsg.ModelMsgBox
			err = MsgBox.NewScope().
				UseTable(fmt.Sprintf("%s_%d", (&qmsg.ModelMsgBox{}).TableName(), mqMsg.CorpId%20)).WhereCorpApp(mqMsg.CorpId, mqMsg.AppId).
				Select(DbId).
				Where(DbSentAt+" < ?", mqMsg.EndAt).
				Where(DbUid, mqMsg.RobotUid).
				SetLimit(2000).Find(ctx, &msgBox)
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, err
			}
			if len(msgBox) == 0 {
				log.Infof("%v 聊天记录清理完毕", mqMsg.RobotUid)
				break
			}
			msgIdList := utils.PluckUint64(msgBox, "Id")
			var chunkErr error
			msgIdList.Chunk(50, func(chunkMsgIdList pie.Uint64s) (stopped bool) {
				_, err = MsgBox.NewScope().
					UseTable(fmt.Sprintf("%s_%d", (&qmsg.ModelMsgBox{}).TableName(), mqMsg.CorpId%20)).WhereCorpApp(mqMsg.CorpId, mqMsg.AppId).
					WhereIn(DbId, chunkMsgIdList).Delete(ctx)
				if err != nil {
					log.Errorf("err:%v", err)
					chunkErr = err
					return true
				}
				time.Sleep(50 * time.Millisecond)
				return false
			})
			if chunkErr != nil {
				log.Errorf("err:%v", chunkErr)
				return nil, chunkErr
			}
		}

		// 清理一下工作台的会话
		for {
			var chatRecordList []*qmsg.ModelChat
			err = Chat.Select(DbId).
				Where(DbCorpId, mqMsg.CorpId).
				Where(DbAppId, mqMsg.AppId).
				Where(DbRobotUid, mqMsg.RobotUid).
				Where(DbCreatedAt+" < ?", mqMsg.EndAt).
				SetLimit(2000).
				Find(ctx, &chatRecordList)
			if err != nil {
				log.Errorf("err %v", err)
				return nil, err
			}

			if len(chatRecordList) == 0 {
				log.Infof("会话清理完毕")
				break
			}
			chatIdList := utils.PluckUint64(chatRecordList, "Id")
			var chunkErr error
			chatIdList.Chunk(50, func(chunkChatIdList pie.Uint64s) (stopped bool) {
				_, err = Chat.
					Where(DbCorpId, mqMsg.CorpId).
					Where(DbAppId, mqMsg.AppId).
					WhereIn(DbId, chunkChatIdList).
					Delete(ctx)
				if err != nil {
					log.Errorf("err %v", err)
					chunkErr = err
					return true
				}
				time.Sleep(50 * time.Millisecond)
				return false
			})
			if chunkErr != nil {
				log.Errorf("err %v", chunkErr)
				return nil, chunkErr
			}
		}

		return &rsp, nil
	}
	return MqdeleteChatMsg.ProcessV2(ctx, req, processCallbackRetryMq)
}
