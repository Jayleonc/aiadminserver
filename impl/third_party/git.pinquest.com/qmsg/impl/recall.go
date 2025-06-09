package impl

import (
	"fmt"
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/brick/smq"
	"git.pinquest.cn/qlb/brick/utils"
	"git.pinquest.cn/qlb/core"
	"git.pinquest.cn/qlb/extrpkg/github.com/go-redis/redis"
	"git.pinquest.cn/qlb/qmsg"
	"git.pinquest.cn/qlb/qrobot"
	"git.pinquest.cn/qlb/qsm"
	"git.pinquest.cn/qlb/qwhale"
	"git.pinquest.cn/qlb/yc"
	"time"
)

const (
	Qmsg_Recall = "qmsg_recall"
)

func RecallMsg(ctx *rpc.Context, req *qmsg.RecallMsgReq) (*qmsg.RecallMsgRsp, error) {
	var rsp qmsg.RecallMsgRsp

	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)

	u, err := getUserCheck(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	recallCallback := &qmsg.WsMsgWrapper_RecallCallback{
		MsgId:    req.MsgId,
		CliMsgId: req.CliMsgId,
		RobotUid: req.RobotUid,
		Uid:      u.Id,
	}

	relaNo := fmt.Sprintf("climsgId.%s.%d", req.MsgId, utils.Now())
	err = s.RedisGroup.HSetPb(Qmsg_Recall, fmt.Sprintf("%d_%s", req.RobotUid, req.MsgId), recallCallback, time.Minute*2)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	_, err = qsm.RecallMsgForQmsg(ctx, corpId, appId, req.RobotUid, req.MsgId, relaNo)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	return &rsp, nil
}

// 别人撤回
func handleRecallMsg(ctx *rpc.Context, corpId, appId uint32, uid uint64, cliMsgId, recallMsgId, appInfo string) error {
	//_, err := ChoiceMsgBoxDb(MsgBox.W(corpId, appId, uid), corpId).Where(map[string]interface{}{
	//	DbCliMsgId: cliMsgId,
	//	DbMsgType:  uint32(qmsg.MsgType_MsgTypeChat),
	//}).Update(ctx, map[string]interface{}{
	//	DbMsgType: uint32(qmsg.MsgType_MsgTypeRecall),
	//})
	//if err != nil {
	//	log.Errorf("err:%v", err)
	//	return err
	//}

	//
	if appInfo == "" {
		return nil
	}

	originCliMsgId, err := s.RedisGroup.Get(getRevokeAppInfoKey(appInfo))
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	var msgBox qmsg.ModelMsgBox
	err = ChoiceMsgBoxDb(MsgBox.W(corpId, appId, uid, DbCliMsgId, string(originCliMsgId)), corpId).First(ctx, &msgBox)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	_, err = ChoiceMsgBoxDb(MsgBox.W(corpId, appId, uid), corpId).Where(DbId, msgBox.Id).Update(ctx, map[string]interface{}{
		DbMsgType: uint32(qmsg.MsgType_MsgTypeRecall),
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	err = updateChatForRecall(ctx, corpId, appId, uid, &msgBox)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	var usrRots []*qmsg.ModelUserRobot
	err = UserRobot.WhereCorpApp(corpId, appId).Where(DbRobotUid, uid).Find(ctx, &usrRots)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	//wrapperList := []*qmsg.WsMsgWrapper{
	//	{
	//		MsgType: uint32(qmsg.WsMsgWrapper_MsgTypeMsgBox),
	//		MsgBox:  &msgBox,
	//		Chat:    &chat,
	//	},
	//	// 兼容 1.0 ws ，日后废弃
	//	{
	//		MsgType: uint32(qmsg.WsMsgWrapper_MsgTypeMsgSeqChanged),
	//		MsgSeqChanged: &qmsg.WsMsgWrapper_MsgSeqChanged{
	//			RobotUid:     msgBox.Uid,
	//			RemoteMsgSeq: msgBox.MsgSeq,
	//		},
	//	},
	//}

	//err = pushWsMsgByChat(ctx, &chat, nil, wrapperList)
	//if err != nil {
	//	log.Errorf("err:%v", err)
	//	return err
	//}

	// ------- 按发送消息流程处理 -------
	// 先发有资产的 后发无资产的

	// 有资产
	for _, rot := range usrRots {
		wrapperList := []*qmsg.WsMsgWrapper{
			{
				MsgType: uint32(qmsg.WsMsgWrapper_MsgTypeRecallCallback),
				RecallCallback: &qmsg.WsMsgWrapper_RecallCallback{
					MsgId:    string(originCliMsgId),
					RobotUid: uid,
					Uid:      rot.Uid,
					CliMsgId: string(originCliMsgId),
				},
			},
		}

		err = pushWsMsg(ctx, corpId, appId, rot.Uid, wrapperList)
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
	}

	// 无资产
	var chat qmsg.ModelChat
	err = Chat.WhereCorpApp(corpId, appId).Where(map[string]interface{}{
		DbRobotUid:  msgBox.Uid,
		DbIsGroup:   msgBox.ChatType == uint32(qmsg.ChatType_ChatTypeGroup),
		DbChatExtId: msgBox.ChatId,
	}).First(ctx, &chat)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	// 判断分配会话
	assignChat, _, err := receiveMsgAssign(ctx, &msgBox, &chat)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	if assignChat == nil {
		assignChat, err = AssignChat.getByCid(ctx, chat.CorpId, chat.AppId, chat.Id)
		if err != nil {
			if AssignChat.IsNotFoundErr(err) {
				return nil
			}
			log.Errorf("err:%v", err)
			return err
		}
	}

	// 未分配无资产的客服
	if assignChat.Uid == 0 {
		return nil
	}

	// 推送无资产分配模式下对应的客服
	err = pushWsMsg(ctx, chat.CorpId, chat.AppId, assignChat.Uid, []*qmsg.WsMsgWrapper{
		{
			MsgType: uint32(qmsg.WsMsgWrapper_MsgTypeRecallCallback),
			RecallCallback: &qmsg.WsMsgWrapper_RecallCallback{
				MsgId:    string(originCliMsgId),
				RobotUid: uid,
				Uid:      assignChat.Uid,
				CliMsgId: string(originCliMsgId),
			},
		},
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	return nil
}

// 撤回回调
func RecallMsgCallback(ctx *rpc.Context, req *qsm.RecallMessageCallbackReq) (*qsm.RecallMessageCallbackRsp, error) {
	var rsp qsm.RecallMessageCallbackRsp

	robotRsp, err := qrobot.GetAccountByYcSerialNo(ctx, &qrobot.GetAccountByYcSerialNoReq{
		CorpId:     req.BizContext.CorpId,
		AppId:      req.BizContext.CorpId,
		YcSerialNo: req.RobotSn,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	if robotRsp.Account == nil {
		log.Error("robot not found")
		return nil, rpc.InvalidArg("robot not found")
	}

	var recallCallback qmsg.WsMsgWrapper_RecallCallback
	err = s.RedisGroup.HGetPb(Qmsg_Recall, fmt.Sprintf("%d_%s", robotRsp.Account.Uid, req.MsgId), &recallCallback)
	if err != nil {
		if err == redis.Nil {
			return &rsp, nil
		}
		log.Errorf("err:%v", err)
		return nil, err
	}

	recallCallback.ErrCode = req.ErrCode
	recallCallback.ErrMsg = req.ErrMsg

	if recallCallback.ErrCode == 269136 {
		recallCallback.ErrMsg = "仅支持撤回2分钟内发送的消息"
	} else if recallCallback.ErrCode == 0 {
		// 这里兼容下，早期查询cliMsgId用的是MsgId，有坑
		cliMsgId := req.MsgId
		if len(recallCallback.CliMsgId) > 0 {
			cliMsgId = recallCallback.CliMsgId
		}
		msgBox := &qmsg.ModelMsgBox{}
		err := ChoiceMsgBoxDb(MsgBox.W(req.BizContext.CorpId, req.BizContext.AppId, recallCallback.RobotUid), req.BizContext.CorpId).Where(map[string]interface{}{
			DbCliMsgId: cliMsgId,
		}).First(ctx, &msgBox)
		if err != nil {
			if MsgBox.IsNotFoundErr(err) {
				msgBox, err = MsgBox.getBySendRecord(ctx, req.BizContext.CorpId, req.BizContext.AppId, recallCallback.RobotUid, req.MsgId)
				if err != nil {
					log.Errorf("err:%v", err)
					return nil, err
				}
			} else {
				log.Errorf("err:%v", err)
				return nil, err
			}
		}

		_, err = ChoiceMsgBoxDb(MsgBox.W(req.BizContext.CorpId, req.BizContext.AppId, recallCallback.RobotUid), req.BizContext.CorpId).Where(DbId, msgBox.Id).Update(ctx, map[string]interface{}{
			DbMsgType: uint32(qmsg.MsgType_MsgTypeRecall),
		})
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}

		err = updateChatForRecall(ctx, req.BizContext.CorpId, req.BizContext.AppId, recallCallback.RobotUid, msgBox)
		if err != nil {
			log.Errorf("err:%v", err)
		}
	}

	err = pushWsMsg(ctx, req.BizContext.CorpId, req.BizContext.AppId, recallCallback.Uid, []*qmsg.WsMsgWrapper{
		{
			MsgType:        uint32(qmsg.WsMsgWrapper_MsgTypeRecallCallback),
			RecallCallback: &recallCallback,
		},
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	return &rsp, nil
}

// 只有扫码号可能走这里的逻辑(手机上撤回)
func RecallMsgCallbackMq(ctx *rpc.Context, req *smq.ConsumeReq) (*smq.ConsumeRsp, error) {
	return MqRecallMessage.ProcessV2(ctx, req, func(ctx *rpc.Context, req *smq.ConsumeReq, data interface{}) (*smq.ConsumeRsp, error) {
		var rsp smq.ConsumeRsp

		recallData := data.(*yc.RecalledMsgRsp)

		hostAccountRsp, err := qwhale.GetHostAccountBySn(ctx, &qwhale.GetHostAccountBySnReq{
			StrRobotId: recallData.RobotSerialNo,
		})
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}

		acc := hostAccountRsp.HostAccount

		var recallCallback qmsg.WsMsgWrapper_RecallCallback

		err = s.RedisGroup.HGetPb(Qmsg_Recall, fmt.Sprintf("%d_%s", hostAccountRsp.HostAccount.Uid, recallData.Data.MsgId), &recallCallback)
		if err != nil {

			if err == redis.Nil {
				recallCallback = qmsg.WsMsgWrapper_RecallCallback{
					ErrCode:  0,
					MsgId:    recallData.Data.MsgId,
					RobotUid: acc.Uid,
				}
			} else {
				log.Errorf("err:%v", err)
				return nil, err
			}
		}

		log.Infof("recall data %+v", recallData)
		log.Infof("recall cbdata %+v", recallCallback)

		msgId := recallCallback.CliMsgId
		if msgId == "" {
			msgId = recallData.Data.MsgId
		}

		var msg qmsg.ModelMsgBox
		err = ChoiceMsgBoxDb(MsgBox.W(acc.CorpId, acc.AppId, acc.Uid), acc.CorpId).Where(DbCliMsgId, msgId).First(ctx, &msg)
		if err != nil {
			if MsgBox.IsNotFoundErr(err) {
				// 走这里可能是因为工作台发，手机端撤回
				var record qmsg.ModelSendRecord
				err = SendRecord.WhereCorpApp(acc.CorpId, acc.AppId).Where(map[string]interface{}{
					DbRobotUid: acc.Uid,
					DbYcMsgId:  recallData.Data.MsgId,
				}).First(ctx, &record)
				if err != nil {
					// 可能是API的发的消息尝试从缓存获取
					if SendRecord.IsNotFoundErr(err) {
						// 查询消息id映射
						key := fmt.Sprintf("msg_id_map_%s_%s", acc.StrOpenId, recallData.Data.MsgId)
						result, err := s.RedisGroup.Get(key)
						if err != nil {
							log.Errorf("err %v", err)
							return nil, err
						}
						record.CliMsgId = string(result)
					} else {
						log.Errorf("err:%v", err)
						return nil, err
					}
				}

				err = ChoiceMsgBoxDb(MsgBox.W(acc.CorpId, acc.AppId, acc.Uid), acc.CorpId).Where(DbCliMsgId, record.CliMsgId).First(ctx, &msg)
				if err != nil {
					log.Errorf("err:%v", err)
					return nil, err
				}
			} else {
				log.Errorf("err:%v", err)
				return nil, err
			}
		}

		_, err = ChoiceMsgBoxDb(MsgBox.W(acc.CorpId, acc.AppId, acc.Uid), acc.CorpId).Where(DbId, msg.Id).Update(ctx, map[string]interface{}{
			DbMsgType: uint32(qmsg.MsgType_MsgTypeRecall),
		})
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}

		err = updateChatForRecall(ctx, acc.CorpId, acc.AppId, acc.Uid, &msg)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}

		assignChat, err := AssignChat.getByFull(ctx, acc.CorpId, acc.AppId, acc.Uid, msg.ChatId, msg.ChatType == uint32(qmsg.ChatType_ChatTypeGroup))
		if err != nil && !AssignChat.IsNotFoundErr(err) {
			log.Errorf("err:%v", err)
			return nil, err
		}

		if assignChat != nil {
			err = pushWsMsg(ctx, acc.CorpId, acc.AppId, assignChat.Uid, []*qmsg.WsMsgWrapper{
				{
					MsgType:        uint32(qmsg.WsMsgWrapper_MsgTypeRecallCallback),
					RecallCallback: &recallCallback,
				},
			})
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, err
			}
		}

		userRobotList, err := UserRobot.getListByRobotUid(ctx, acc.CorpId, acc.AppId, acc.Uid)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}

		for _, userRobot := range userRobotList {
			err = pushWsMsg(ctx, acc.CorpId, acc.AppId, userRobot.Uid, []*qmsg.WsMsgWrapper{
				{
					MsgType:        uint32(qmsg.WsMsgWrapper_MsgTypeRecallCallback),
					RecallCallback: &recallCallback,
				},
			})
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, err
			}
		}

		return &rsp, nil
	})
}

func updateChatForRecall(ctx *rpc.Context, corpId, appId uint32, uid uint64, recallMsg *qmsg.ModelMsgBox) error {
	var chatMsg qmsg.ModelMsgBox
	err := ChoiceMsgBoxDb(MsgBox.W(corpId, appId, uid), corpId).Where(map[string]interface{}{
		DbMsgSeq + " >=": recallMsg.MsgSeq,
	}).WhereIn(DbMsgType, []uint32{
		uint32(qmsg.MsgType_MsgTypeChat), uint32(qmsg.MsgType_MsgTypeRecall),
	}).OrderDesc(DbMsgSeq).First(ctx, &chatMsg)

	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	chatMsgType := chatMsg.ChatMsgType

	if chatMsg.MsgType == uint32(qmsg.MsgType_MsgTypeRecall) {
		chatMsgType = uint32(qmsg.ChatMsgType_ChatMsgTypeRecall)
	}

	_, err = Chat.WhereCorpApp(corpId, appId).Where(map[string]interface{}{
		DbRobotUid:  chatMsg.Uid,
		DbIsGroup:   chatMsg.ChatType == uint32(qmsg.ChatType_ChatTypeGroup),
		DbChatExtId: chatMsg.ChatId,
	}).Update(ctx, map[string]interface{}{
		DbMsg:         utils.SubString(chatMsg.Msg, 100),
		DbChatMsgType: chatMsgType,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	_, err = AtRecord.WhereCorpApp(corpId, appId).Where(map[string]interface{}{
		DbRobotUid: uid,
		DbMsgSeq:   recallMsg.MsgSeq,
	}).Delete(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	return nil
}
