package impl

import (
	"fmt"
	"strings"
	"time"

	"git.pinquest.cn/base/aiadmin"
	"git.pinquest.cn/qlb/featswitch"
	"git.pinquest.cn/qlb/qrt"

	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/rpc"

	"git.pinquest.cn/qlb/brick/utils"
	"git.pinquest.cn/qlb/qmsg"
	"git.pinquest.cn/qlb/qris"
	"git.pinquest.cn/qlb/qrobot"
	"git.pinquest.cn/qlb/quan"
	"git.pinquest.cn/qlb/yc"
)

func handleReceiveMsg(ctx *rpc.Context, corpId, appId uint32, robotUid, chatId uint64, acc *qrobot.ModelAccount,
	robotSn string, extraMsg *quan.ExtraMsg, recvData *yc.RecvMsgData, sendAt uint32, chatType qmsg.ChatType, mentionName string, skipWait bool) error {

	senderAccountId := acc.Id
	msgBox := &qmsg.ModelMsgBox{
		CorpId:          corpId,
		AppId:           appId,
		Uid:             robotUid,
		CliMsgId:        recvData.MsgId,
		MsgType:         uint32(qmsg.MsgType_MsgTypeChat),
		ChatType:        uint32(chatType),
		ChatId:          chatId,
		SentAt:          sendAt,
		SenderAccountId: senderAccountId,
	}

	log.Debugf("get msg from %v to %v", recvData.SenderSerialNo, recvData.ReceiverSerialNo)

	// 消息撤回
	if extraMsg.ExtraMsgType == uint32(quan.ExtraMsgType_ExtraMsgTypeRecall) {
		if err := checkForCuzIMRevoke(ctx, robotSn); err != nil {
			return err
		}

		err := handleRecallMsg(ctx, corpId, appId, robotUid, recvData.MsgId, extraMsg.RecallMsgId, recvData.AppInfo)
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}

		return nil
	}

	if err := checkMsgDeDupCache(robotUid, recvData.MsgId); err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	// 结构转换
	err := s.CoverMsg.CoverExtrMsg2QmsgChatMsg(ctx, extraMsg, msgBox)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	// 数据落地
	chat, lastSeq, err := addMsg(ctx, msgBox, skipWait)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	//
	// 暂且先统一cache 3min先 预留缓冲1min
	// 消息唯一
	// AppInfo 不是每个底层都有的
	// 如果是机器人发送的基础群发消息 不缓存
	if recvData.AppInfo != "" && (recvData.SenderSerialNo != robotSn || !strings.HasPrefix(recvData.AppInfo, "KHGS_")) {
		if err = checkForCuzIMRevoke(ctx, robotSn); err == nil {
			_ = s.RedisGroup.Set(getRevokeAppInfoKey(recvData.AppInfo), []byte(recvData.MsgId), time.Minute*3)
		}
	}

	if msgBox.ChatMsgType == uint32(qmsg.ChatMsgType_ChatMsgTypeVoice) {
		// 默认存一下语音消息的id，如果有自动转文本，用这个去匹配
		err = s.RedisGroup.Set(autoAudio2TextCacheKey(fmt.Sprintf("%s_%s", robotSn, recvData.MsgId)), []byte(fmt.Sprintf("%d", msgBox.Id)), 3*time.Minute)
		if err != nil {
			log.Errorf("err %v", err)
		}

		featureFlagRsp, err := featswitch.GetFeatureFlagEnabled(ctx, &featswitch.GetFeatureFlagEnabledReq{
			Key: "QMSG_AUTO_AUDIO_TO_TEXT", CorpId: corpId, AppId: appId})
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
		if featureFlagRsp.Enabled {
			merchantId, err := qrt.GetMerchantId(ctx)
			if err != nil {
				log.Errorf("err %v", err)
				return err
			}

			relaSn := fmt.Sprintf("qmsg_%d_%d", msgBox.Id, msgBox.Uid)
			_, err = yc.Audio2TextAsync(ctx, merchantId, &yc.Audio2TextReq{
				MerchantNo:    merchantId,
				RobotSerialNo: robotSn,
				RelaSerialNo:  relaSn,
				MsgId:         recvData.MsgId,
			}, &yc.BizContext{
				CorpId:  corpId,
				AppId:   appId,
				Module:  "qmsg",
				Context: "auto_audio2text",
			})
			if err != nil {
				log.Errorf("err %v", err)
				//失败不影响流程
			}
		}
	}

	// 客户发给机器人的消息
	if msgBox.ChatType == uint32(qmsg.ChatType_ChatTypeSingle) &&
		msgBox.MsgType == uint32(qmsg.MsgType_MsgTypeChat) &&
		msgBox.ChatId == msgBox.SenderAccountId &&
		msgBox.ChatMsgType != uint32(qmsg.ChatMsgType_ChatMsgTypeCall) && msgBox.ChatMsgType != uint32(qmsg.ChatMsgType_ChatMsgTypeCallFinish) {
		pubToMsgBoxStatMq(ctx, msgBox, chat)

		if msgBox.Msg != "我通过了你的联系人验证请求，现在我们可以开始聊天了（sys）" && msgBox.Msg != "我通过了你的联系人验证请求，现在我们可以开始聊天了" {
			checkChatPermission4Receive(ctx, corpId, appId, robotUid, msgBox.ChatId)
		}
	}

	// 去重写入
	setMsgDeDupCache(robotUid, recvData.MsgId)

	// 处理at消息
	var beingAt bool
	if chatType == qmsg.ChatType_ChatTypeGroup && recvData.AtList != "" && (strings.Contains(recvData.AtList, "ALL") || strings.Contains(recvData.AtList, robotSn)) {
		beingAt = true
		err = AtRecord.Create(ctx, &qmsg.ModelAtRecord{
			CorpId:          chat.CorpId,
			AppId:           chat.AppId,
			Cid:             chat.Id,
			MsgSeq:          chat.MsgSeq,
			SenderAccountId: msgBox.SenderAccountId,
			RobotUid:        msgBox.Uid,
		})
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
	}

	// 判断分配会话，以及是否智能助手处理
	assignChat, isUseAi, err := receiveMsgAssign(ctx, msgBox, chat)
	if err != nil {
		log.Errorf("err:%v", err)
	}

	log.Debugf("[ai_msg] isUserAi:%v", isUseAi)

	// 识别是否机器人，不记录err
	var isRobotMsg bool
	robotRsp, err := qris.GetRobotItemBySn(ctx, &qris.GetRobotItemBySnReq{
		RobotSn: recvData.SenderSerialNo,
	})
	if err == nil && robotRsp.RobotItem.CorpId == chat.CorpId {
		isRobotMsg = true
	}

	robotAcc, err := qrobot.GetAccountByRobotUidSys(ctx, &qrobot.GetAccountByRobotUidSysReq{
		RobotUid: robotUid,
		CorpId:   chat.CorpId,
		AppId:    chat.AppId,
	})
	if err != nil {
		log.Errorf("err %v", err)
		return err
	}

	// 只有非机器人发送的消息（即客户消息）才需要进行敏感词检查、接待状态记录、推送到工作台并可能触发AI工作流
	if !isRobotMsg {
		// 客户敏感词处理：这里使用了 robotAcc.Account
		if innerErr := extSensitiveWordCheck(ctx, msgBox, chat, robotAcc.Account, mentionName); innerErr != nil {
			log.Errorf("err %v", innerErr)
			// 敏感词检查失败，可能需要根据业务逻辑决定是否阻断消息。
			// 当前不阻断，仅记录错误。
		}

		// 1. 获取新 AI 工作流配置
		activeWorkflowConfig, errCfg := GetRobotActiveWorkflowConfigFromAIAdmin(ctx, corpId, robotUid)
		if errCfg != nil {
			log.Errorf("qmsg.handleReceiveMsg: 获取机器人 %d 的AI工作流配置失败: %v。不触发新AI。", robotUid, errCfg)
			// 配置获取失败，消息已推送，不阻断流程
		}

		shouldTriggerNewAI := false
		if activeWorkflowConfig != nil && activeWorkflowConfig.WorkflowId != "" && activeWorkflowConfig.WorkflowStatus == uint32(aiadmin.WorkflowStatus_WorkflowStatusEnabled) {
			// 判断消息类型：仅对单聊文本或语音消息触发 AI
			if chatType == qmsg.ChatType_ChatTypeSingle && (msgBox.ChatMsgType == uint32(qmsg.ChatMsgType_ChatMsgTypeText) || msgBox.ChatMsgType == uint32(qmsg.ChatMsgType_ChatMsgTypeVoice)) {
				// TODO: 可以添加更复杂的判断逻辑
				shouldTriggerNewAI = true // 暂时默认满足条件就触发
			}
		}

		// 记录智能助手接待状态（参数改为 shouldTriggerNewAI）
		err = markEntertain(ctx, chat, shouldTriggerNewAI)
		if err != nil {
			log.Errorf("err %v", err)
		}

		// 1. 无条件将客户消息推送到工作台，确保可见性
		errWsPush := pushMessageToWorkstation(ctx, msgBox, chat, assignChat, recvData, beingAt, lastSeq)
		if errWsPush != nil {
			log.Errorf("qmsg.handleReceiveMsg: WS推送客户消息失败: %v", errWsPush)
			return errWsPush // WS推送失败是严重问题，应返回错误
		}
		log.Infof("qmsg.handleReceiveMsg: 客户消息已推送到工作台。CliMsgId: %s", msgBox.CliMsgId)

		// 2. 新 Agent 模式工作流触发
		if shouldTriggerNewAI {
			targetWorkflowId := activeWorkflowConfig.WorkflowId
			log.Infof("qmsg.handleReceiveMsg: 机器人 %d, 企业 %d 尝试触发新AI工作流。CliMsgId: %s, WorkflowId: %s", robotUid, corpId, msgBox.CliMsgId, targetWorkflowId)

			// 获取最近的聊天记录
			recentHistoryForAI, errHistory := getRecentHistoryForAI(ctx, corpId, appId, robotUid, chatId, chatType, 20)
			if errHistory != nil {
				log.Warnf("qmsg.handleReceiveMsg: 获取AI历史记录失败 for robotUid %d, chatId %d: %v。将使用空历史记录。", robotUid, chatId, errHistory.Error())
				recentHistoryForAI = []*aiadmin.ChatMessage{} // 出错时使用空历史
			}

			// 调用 TriggerNewAIWorkflow 发布 MQ 任务
			errTrig := TriggerNewAIWorkflow(
				ctx,
				corpId,
				appId,
				robotUid,
				chatId,
				chatType,
				senderAccountId,
				msgBox.Msg,
				msgBox.CliMsgId,
				recentHistoryForAI,
				targetWorkflowId,
			)
			if errTrig != nil {
				log.Errorf("qmsg.handleReceiveMsg: TriggerNewAIWorkflow 失败: %v。CliMsgId: %s", errTrig, msgBox.CliMsgId)
			} else {
				log.Infof("qmsg.handleReceiveMsg: 新AI工作流缓冲事件已通过 TriggerNewAIWorkflow 请求发送成功。CliMsgId: %s", msgBox.CliMsgId)
			}
		} else {
			log.Infof("qmsg.handleReceiveMsg: 不满足新AI工作流触发条件，不触发AI。CliMsgId: %s", msgBox.CliMsgId)
		}
	}
	// 如果是机器人发送的消息 (isRobotMsg = true)，则直接返回，不进行额外处理
	return nil
}

// 辅助函数：封装将消息推送到工作台的WebSocket逻辑
func pushMessageToWorkstation(
	ctx *rpc.Context,
	msgBox *qmsg.ModelMsgBox,
	chat *qmsg.ModelChat,
	assignChat *qmsg.ModelAssignChat,
	recvData *yc.RecvMsgData,
	beingAt bool,
	lastSeq uint64,
) error {
	wrapperList := []*qmsg.WsMsgWrapper{ //
		{
			MsgType: uint32(qmsg.WsMsgWrapper_MsgTypeMsgBox), //
			MsgBox:  msgBox,                                  //
			Chat:    chat,                                    //
			MsgId:   recvData.MsgId,                          //
			BeingAt: beingAt,                                 //
			LastSeq: lastSeq,                                 //
		},
		{
			MsgType: uint32(qmsg.WsMsgWrapper_MsgTypeMsgSeqChanged), //
			MsgSeqChanged: &qmsg.WsMsgWrapper_MsgSeqChanged{ //
				RobotUid:     msgBox.Uid,    //
				RemoteMsgSeq: msgBox.MsgSeq, //
			},
		},
	}
	log.Infof("qmsg.handleReceiveMsg: 执行原有非AI的WS推送逻辑。CliMsgId: %s", msgBox.CliMsgId) //

	var err error
	wrapperList, err = setOpenMsgAck(ctx, msgBox.CorpId, msgBox.AppId, wrapperList) //
	if err != nil {                                                                 //
		log.Errorf("err:%v", err) //
	}

	err = pushWsMsgByChatV2(ctx, chat, assignChat, wrapperList) //
	if err != nil {                                             //
		log.Errorf("err %v", err) //
	}
	return err // 返回推送结果
}

// 接到消息逻辑适配器
//func handleReceiveMsg(ctx *rpc.Context, corpId, appId uint32, robotUid, chatId uint64, acc *qrobot.ModelAccount,
//	robotSn string, extraMsg *quan.ExtraMsg, recvData *yc.RecvMsgData, sendAt uint32, chatType qmsg.ChatType, mentionName string, skipWait bool) error {
//
//	senderAccountId := acc.Id
//	msgBox := &qmsg.ModelMsgBox{
//		CorpId:          corpId,
//		AppId:           appId,
//		Uid:             robotUid,
//		CliMsgId:        recvData.MsgId,
//		MsgType:         uint32(qmsg.MsgType_MsgTypeChat),
//		ChatType:        uint32(chatType),
//		ChatId:          chatId,
//		SentAt:          sendAt,
//		SenderAccountId: senderAccountId,
//	}
//
//	log.Debugf("get msg from %v to %v", recvData.SenderSerialNo, recvData.ReceiverSerialNo)
//
//	// 消息撤回
//	if extraMsg.ExtraMsgType == uint32(quan.ExtraMsgType_ExtraMsgTypeRecall) {
//		if err := checkForCuzIMRevoke(ctx, robotSn); err != nil {
//			return err
//		}
//
//		err := handleRecallMsg(ctx, corpId, appId, robotUid, recvData.MsgId, extraMsg.RecallMsgId, recvData.AppInfo)
//		if err != nil {
//			log.Errorf("err:%v", err)
//			return err
//		}
//
//		return nil
//	}
//
//	if err := checkMsgDeDupCache(robotUid, recvData.MsgId); err != nil {
//		log.Errorf("err:%v", err)
//		return err
//	}
//
//	// 结构转换
//	err := s.CoverMsg.CoverExtrMsg2QmsgChatMsg(ctx, extraMsg, msgBox)
//	if err != nil {
//		log.Errorf("err:%v", err)
//		return err
//	}
//
//	// 数据落地
//	chat, lastSeq, err := addMsg(ctx, msgBox, skipWait)
//	if err != nil {
//		log.Errorf("err:%v", err)
//		return err
//	}
//
//	log.Infof("查看数据落地后的chat未读数 ：%v", chat.UnreadCount)
//
//	// 暂且先统一cache 3min先 预留缓冲1min
//	// 消息唯一
//	// AppInfo 不是每个底层都有的
//	// 如果是机器人发送的基础群发消息 不缓存
//	if recvData.AppInfo != "" && (recvData.SenderSerialNo != robotSn || !strings.HasPrefix(recvData.AppInfo, "KHGS_")) {
//		if err = checkForCuzIMRevoke(ctx, robotSn); err == nil {
//			_ = s.RedisGroup.Set(getRevokeAppInfoKey(recvData.AppInfo), []byte(recvData.MsgId), time.Minute*3)
//		}
//	}
//
//	if msgBox.ChatMsgType == uint32(qmsg.ChatMsgType_ChatMsgTypeVoice) {
//		// 默认存一下语音消息的id，如果有自动转文本，用这个去匹配
//		err = s.RedisGroup.Set(autoAudio2TextCacheKey(fmt.Sprintf("%s_%s", robotSn, recvData.MsgId)), []byte(fmt.Sprintf("%d", msgBox.Id)), 3*time.Minute)
//		if err != nil {
//			log.Errorf("err %v", err)
//		}
//
//		featureFlagRsp, err := featswitch.GetFeatureFlagEnabled(ctx, &featswitch.GetFeatureFlagEnabledReq{
//			Key: "QMSG_AUTO_AUDIO_TO_TEXT", CorpId: corpId, AppId: appId})
//		if err != nil {
//			log.Errorf("err:%v", err)
//			return err
//		}
//		if featureFlagRsp.Enabled {
//			merchantId, err := qrt.GetMerchantId(ctx)
//			if err != nil {
//				log.Errorf("err %v", err)
//				return err
//			}
//
//			relaSn := fmt.Sprintf("qmsg_%d_%d", msgBox.Id, msgBox.Uid)
//			_, err = yc.Audio2TextAsync(ctx, merchantId, &yc.Audio2TextReq{
//				MerchantNo:    merchantId,
//				RobotSerialNo: robotSn,
//				RelaSerialNo:  relaSn,
//				MsgId:         recvData.MsgId,
//			}, &yc.BizContext{
//				CorpId:  corpId,
//				AppId:   appId,
//				Module:  "qmsg",
//				Context: "auto_audio2text",
//			})
//			if err != nil {
//				log.Errorf("err %v", err)
//				//失败不影响流程
//			}
//		}
//	}
//
//	// 消息重复处理时，chat可能为nil
//	if chat == nil {
//		return nil
//	}
//
//	// 客户发给机器人的消息
//	if msgBox.ChatType == uint32(qmsg.ChatType_ChatTypeSingle) &&
//		msgBox.MsgType == uint32(qmsg.MsgType_MsgTypeChat) &&
//		msgBox.ChatId == msgBox.SenderAccountId &&
//		msgBox.ChatMsgType != uint32(qmsg.ChatMsgType_ChatMsgTypeCall) && msgBox.ChatMsgType != uint32(qmsg.ChatMsgType_ChatMsgTypeCallFinish) {
//		pubToMsgBoxStatMq(ctx, msgBox, chat)
//
//		if msgBox.Msg != "我通过了你的联系人验证请求，现在我们可以开始聊天了（sys）" && msgBox.Msg != "我通过了你的联系人验证请求，现在我们可以开始聊天了" {
//			checkChatPermission4Receive(ctx, corpId, appId, robotUid, msgBox.ChatId)
//		}
//	}
//
//	// 去重写入
//	setMsgDeDupCache(robotUid, recvData.MsgId)
//
//	// 处理at消息
//	var beingAt bool
//	if chatType == qmsg.ChatType_ChatTypeGroup && recvData.AtList != "" && (strings.Contains(recvData.AtList, "ALL") || strings.Contains(recvData.AtList, robotSn)) {
//		beingAt = true
//		err = AtRecord.Create(ctx, &qmsg.ModelAtRecord{
//			CorpId:          chat.CorpId,
//			AppId:           chat.AppId,
//			Cid:             chat.Id,
//			MsgSeq:          chat.MsgSeq,
//			SenderAccountId: msgBox.SenderAccountId,
//			RobotUid:        msgBox.Uid,
//		})
//		if err != nil {
//			log.Errorf("err:%v", err)
//			return err
//		}
//	}
//
//	// 判断分配会话，以及是否智能助手处理
//	assignChat, isUseAi, err := receiveMsgAssign(ctx, msgBox, chat)
//	if err != nil {
//		log.Errorf("err:%v", err)
//	}
//
//	log.Debugf("[ai_msg] isUserAi:%v", isUseAi)
//
//	// 识别是否机器人，不记录err
//	var isRobotMsg bool
//	robotRsp, err := qris.GetRobotItemBySn(ctx, &qris.GetRobotItemBySnReq{
//		RobotSn: recvData.SenderSerialNo,
//	})
//	if err == nil && robotRsp.RobotItem.CorpId == chat.CorpId {
//		isRobotMsg = true
//	}
//
//	robotAcc, err := qrobot.GetAccountByRobotUidSys(ctx, &qrobot.GetAccountByRobotUidSysReq{
//		RobotUid: robotUid,
//		CorpId:   chat.CorpId,
//		AppId:    chat.AppId,
//	})
//	if err != nil {
//		log.Errorf("err %v", err)
//		return err
//	}
//
//	// 记录智能助手接待状态
//	if !isRobotMsg {
//		// 客户敏感词处理
//		if innerErr := extSensitiveWordCheck(ctx, msgBox, chat, robotAcc.Account, mentionName); innerErr != nil {
//			log.Errorf("err %v", innerErr)
//		}
//
//		err = markEntertain(ctx, chat, isUseAi)
//		if err != nil {
//			log.Errorf("err %v", err)
//		}
//	}
//
//	if !isUseAi {
//		// 非智能助手处理
//		wrapperList := []*qmsg.WsMsgWrapper{
//			{
//				MsgType: uint32(qmsg.WsMsgWrapper_MsgTypeMsgBox),
//				MsgBox:  msgBox,
//				Chat:    chat,
//				MsgId:   recvData.MsgId,
//				BeingAt: beingAt,
//				LastSeq: lastSeq,
//			},
//			// 兼容 1.0 ws ，日后废弃
//			{
//				MsgType: uint32(qmsg.WsMsgWrapper_MsgTypeMsgSeqChanged),
//				MsgSeqChanged: &qmsg.WsMsgWrapper_MsgSeqChanged{
//					RobotUid:     msgBox.Uid,
//					RemoteMsgSeq: msgBox.MsgSeq,
//				},
//			},
//		}
//		log.Infof("=========================")
//		log.Infof("aiadmin_workflow: Here is correct.")
//		log.Infof("=========================")
//		// 特性开关控制，判断是否要等ws的ack，确保推送不丢失
//		wrapperList, err = setOpenMsgAck(ctx, corpId, appId, wrapperList)
//		if err != nil {
//			log.Errorf("err:%v", err)
//			// 出错无需中断业务流程，默认未开启
//		}
//
//		err = pushWsMsgByChatV2(ctx, chat, assignChat, wrapperList)
//		if err != nil {
//			log.Errorf("err %v", err)
//		}
//
//	} else {
//
//		// 非机器人消息
//		if !isRobotMsg {
//			_, err = MqMsgAiEntertain.PubV2(ctx, &smq.PubReq{
//				CorpId: chat.CorpId,
//				AppId:  chat.AppId,
//			}, &qmsg.MsgAiEntertainMqData{
//				ModelChatId: chat.Id,
//				MsgBoxId:    msgBox.Id,
//			})
//			if err != nil {
//				log.Errorf("err:%v", err)
//			}
//		}
//
//	}
//
//	return nil
//}

func markEntertain(ctx *rpc.Context, chat *qmsg.ModelChat, isUseAi bool) error {

	if chat.Detail == nil {
		chat.Detail = &qmsg.ModelChat_Detail{}
	}

	var needUpChat bool

	var groupId, accountId uint64
	if chat.IsGroup {
		groupId = chat.ChatExtId
	} else {
		accountId = chat.ChatExtId
	}

	log.Debugf("ai_msg EntertainType:%v  isUserAi:%v", chat.Detail.EntertainType, isUseAi)
	if chat.Detail.EntertainType == uint32(qmsg.ModelChat_Detail_Ai) {

		var aiRecord qmsg.ModelAiEntertainRecord

		// 智能助手转人工，关闭
		_, err := aiEntertainRecord.FirstOrCreate(ctx, map[string]interface{}{
			DbCorpId:      chat.CorpId,
			DbAppId:       chat.AppId,
			DbRobotUid:    chat.RobotUid,
			DbModelChatId: chat.Id,
			DbEndAt:       0,
		}, map[string]interface{}{
			DbGroupId:   groupId,
			DbAccountId: accountId,
		}, &aiRecord)
		if err != nil {
			log.Errorf("err %v", err)
			return err
		}

		if !isUseAi {

			needUpChat = true
			chat.Detail.EntertainType = uint32(qmsg.ModelChat_Detail_Manual)

			_, err = aiEntertainRecord.WhereCorpApp(chat.CorpId, chat.AppId).
				Where(DbRobotUid, chat.RobotUid).
				Where(DbModelChatId, chat.Id).
				Where(DbEndAt, 0).
				Update(ctx, map[string]interface{}{
					DbEndAt: utils.Now(),
				})
			if err != nil {
				log.Errorf("err %v", err)
				return err
			}
		}
	}

	if (chat.Detail.EntertainType == uint32(qmsg.ModelChat_Detail_Manual) || chat.Detail.EntertainType == uint32(qmsg.ModelChat_Detail_Nil)) && isUseAi {
		needUpChat = true
		chat.Detail.EntertainType = uint32(qmsg.ModelChat_Detail_Ai)

		// 人工转智能助手
		var aiRecord qmsg.ModelAiEntertainRecord
		err := aiEntertainRecord.WhereCorpApp(chat.CorpId, chat.AppId).
			Where(DbRobotUid, chat.RobotUid).
			Where(DbModelChatId, chat.Id).
			Where(DbEndAt, 0).
			OrderDesc(DbUpdatedAt).First(ctx, &aiRecord)
		if err != nil && !aiEntertainRecord.IsNotFoundErr(err) {
			log.Errorf("err %v", err)
			return err
		}

		if aiEntertainRecord.IsNotFoundErr(err) {
			err = aiEntertainRecord.Create(ctx, &qmsg.ModelAiEntertainRecord{
				CorpId:      chat.CorpId,
				AppId:       chat.AppId,
				RobotUid:    chat.RobotUid,
				ModelChatId: chat.Id,
				GroupId:     groupId,
				AccountId:   accountId,
			})
			if err != nil {
				log.Errorf("err %v", err)
				return err
			}
		}
	}

	if !needUpChat {
		return nil
	}

	// 修改会话状态 ai or 人工
	_, err := Chat.WhereCorpApp(chat.CorpId, chat.AppId).Where(map[string]interface{}{
		DbId: chat.Id,
	}).Update(ctx, map[string]interface{}{
		DbDetail: chat.Detail,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	return nil
}
