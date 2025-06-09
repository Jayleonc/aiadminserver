// impl/aiadmin_msg.go
package impl

import (
	"fmt"
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/json"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/brick/utils"
	"git.pinquest.cn/qlb/qmsg"
	"time"
)

// SendAiReply 是供 AI Admin 服务调用的 RPC 接口，用于发送 AI 生成的回复。
func SendAiReply(ctx *rpc.Context, req *qmsg.SendAiReplyReq) (*qmsg.SendAiReplyRsp, error) {
	var rsp qmsg.SendAiReplyRsp

	// --- 1. 参数校验 ---
	if req.RobotUid == 0 {
		return nil, rpc.InvalidArg("robot_uid (代表AI的机器人实体ID) 不能为空")
	}
	if req.ChatId == 0 {
		return nil, rpc.InvalidArg("chat_id (客户或群组ID) 不能为空")
	}
	if req.CliMsgId == "" {
		// 如果AI Admin没有生成唯一的CliMsgId，qmsg服务可以考虑生成一个
		// req.CliMsgId = fmt.Sprintf("ai_reply_%s", utils.GenRandomStr())
		// 或者要求 aiadmin 必须提供
		return nil, rpc.InvalidArg("cli_msg_id 不能为空")
	}
	if req.Msg == "" && req.ChatMsgType == uint32(qmsg.ChatMsgType_ChatMsgTypeText) { // 假设文本消息不能为空
		return nil, rpc.InvalidArg("文本消息内容不能为空")
	}

	// --- 2. 构造 User 对象 (代表 AI 发送者) ---
	// 这里的 User 对象用于消息队列和后续流程中的身份识别。
	// 你可能需要一个特定的 AI 用户配置，或者沿用 `MsgAiEntertainMq` 中 `ai_msg_fake_kefu` 的方式。
	// 为简化示例，我们创建一个临时的 User 对象。
	// **注意**: 这个 User 对象可能需要更完善的填充，特别是 CorpId 和 AppId。
	//          一种常见的做法是，aiadmin 调用时传递其自身的 CorpId 和 AppId，或者 qmsg 根据 RobotUid 能查到。
	//          此处假设 req 中已包含 CorpId 和 AppId。
	if req.CorpId == 0 || req.AppId == 0 {
		// 如果 RobotUid 可以唯一确定 CorpId 和 AppId，可以通过 RobotUid 查询得到
		// robotAccount, err := getRobotByUid(ctx, req.CorpId, req.AppId, req.RobotUid)
		// if err != nil {
		//    log.Errorf("SendAiReply: 查询 RobotUid %d 对应的 CorpId/AppId 失败: %v", req.RobotUid, err)
		//    return nil, rpc.InternalError("无法确定企业信息")
		// }
		// req.CorpId = robotAccount.CorpId
		// req.AppId = robotAccount.AppId
		// 为演示，先硬编码或要求传入
		return nil, rpc.InvalidArg("corp_id 和 app_id 不能为空，或需要通过 robot_uid 查询得到")
	}

	aiSenderUser := &qmsg.ModelUser{
		Id:       0, // AI 可能没有真实的 User ID，或者有一个特殊的 ID
		CorpId:   req.CorpId,
		AppId:    req.AppId,
		LoginId:  fmt.Sprintf("ai_agent_%d", req.RobotUid), // 确保 LoginId 对于 AI 是可识别的
		Username: "AI助手",                                   // 显示名称
		IsAssign: false,                                    // AI发送通常不涉及客服工作台的分配逻辑
	}

	// --- 3. 消息内容和类型处理 ---
	// 确保 ChatMsgType 是有效的
	if _, ok := qmsg.ChatMsgType_name[int32(req.ChatMsgType)]; !ok || req.ChatMsgType == uint32(qmsg.ChatMsgType_ChatMsgTypeNil) {
		return nil, rpc.InvalidArg(fmt.Sprintf("无效的消息类型: %d", req.ChatMsgType))
	}

	// --- 4. 敏感词校验 (可选，根据业务调整) ---
	// 对于AI生成的内容，敏感词校验策略可能不同。
	// 这里的实现参考了 `SendChatMsg` 中的校验，但你可能需要调整或跳过。
	// 注意：`validateChatMsg` 内部的 `NewSenWordChecker` 和 `FindSensitiveWordAll` 的上下文和参数需要适配
	// validateRsp, err := validateChatMsg(ctx, req.ChatMsgType, internalSendReq, aiSenderUser, true /*或者 false，取决于是否需要YC层面的敏感词*/ )
	// if err != nil {
	//     log.Errorf("SendAiReply: 消息内容校验失败 for CliMsgId %s: %v", req.CliMsgId, err)
	//     // 根据错误类型决定是否返回，例如特定敏感词错误
	//     // rsp.TriggerSysSensitiveWordList = validateRsp.TriggerSysSensitiveWordList
	//     return &rsp, err
	// }
	// log.Infof("SendAiReply: AI消息内容校验通过 for CliMsgId %s.", req.CliMsgId)

	// --- 5. 检查 CliMsgId 是否已处理 (复用 SendChatMsg 中的逻辑) ---
	// 这里假设 `checkCliMsgId` 依赖的 `MsgBox` 和 `User` 结构体中的 `Uid` 字段是发送者的 `RobotUid`
	err := checkCliMsgId(ctx, aiSenderUser.CorpId, aiSenderUser.AppId, req.RobotUid, req.CliMsgId) // 注意这里的 uid 参数
	if err != nil {
		if rpc.GetErrCode(err) == qmsg.ErrCliMsgIdExisted {
			log.Warnf("SendAiReply: CliMsgId %s 已存在，消息可能已发送。", req.CliMsgId)
			// 可以考虑查询已发送的消息并返回相关信息，或者直接返回成功表示幂等性处理
			// 此处简单返回错误，具体业务需确认如何处理幂等性
			return nil, err
		}
		log.Errorf("SendAiReply: 检查 CliMsgId %s 失败: %v", req.CliMsgId, err)
		return nil, err
	}

	// --- 6. 构造内部 SendChatMsgReq ---
	// 这是为了复用 `processSendChatMsg` 的逻辑
	internalSendReq := &qmsg.SendChatMsgReq{
		RobotUid:    req.RobotUid, // AI 使用这个 RobotUid 发送
		CliMsgId:    req.CliMsgId,
		ChatType:    req.ChatType,
		ChatId:      req.ChatId,
		ChatMsgType: req.ChatMsgType,
		Msg:         req.Msg,
		// 如果有其他消息字段 (Href, Title, Desc, AtList, MiniProgram 等)，从 req 中获取并填充
		// Href:                 req.Href,
		// Title:                req.Title,
		// Desc:                 req.Desc,
		// VoiceTime:            req.VoiceTime,
		// AtList:               req.AtList, // 需要转换 qmsg.SendAiReplyReq_At 到 qmsg.ChatMsgAt
		// MiniProgramCoverUrl:  req.MiniProgramCoverUrl,
		// MiniProgramName:      req.MiniProgramName,
		// MiniProgramPath:      req.MiniProgramPath,
		// MiniProgramTitle:     req.MiniProgramTitle,
		// SosObjectId:          req.SosObjectId,
		// MaterialId:           req.MaterialId,
		// ChannelMsgTitle:      req.ChannelMsgTitle,
		// ChannelMsgCoverUrl:   req.ChannelMsgCoverUrl,
		// ChannelMsgHref:       req.ChannelMsgHref,
		// MsgSerialNo:          req.MsgSerialNo,
		// ChannelMsgName:       req.ChannelMsgName,
		// ChannelMsgIcon:       req.ChannelMsgIcon,
	}

	// --- 7. 将消息发布到消息队列 (复用 SendChatMsg 中的逻辑) ---
	// `processSendChatMsg` 会处理后续的发送到IM平台、落库、WebSocket推送等。
	mqMsg := &qmsg.SendChatMsgMqReq{
		SendReq: internalSendReq,
		User:    aiSenderUser, // 使用上面构造的 AI 发送者信息
		SendAt:  utils.Now(),
	}

	pubRsp := qmsg.MqSendChatMsg.PubOrExecInRoutine(
		ctx,
		aiSenderUser.CorpId,
		aiSenderUser.AppId,
		mqMsg,
		processSendChatMsg, // 复用现有的消息处理函数
	)

	if pubRsp != nil {
		log.Infof("SendAiReply: AI回复消息已成功发布到MQ, MQ MsgId: %s, CliMsgId: %s", pubRsp.MsgId, req.CliMsgId)
		// 如果需要同步返回YC平台的MsgID，这里会比较复杂，因为发送是异步的。
		// 通常做法是返回一个任务ID或者接受成功的状态。
		// 如果 `processSendChatMsg` 被同步执行了（PubOrExecInRoutine的特性），
		// 并且其内部有机会获取并返回YcMsgId，那么可以在这里设置。
		// 但当前结构下，`processSendChatMsg` 是异步消费者，直接获取YcMsgId不现实。
		// rsp.YcMsgId = ... (通常为空，或者是一个追踪ID)
	}

	// 记录AI发送的动作到特定的日志或统计表 (可选)
	// 例如，记录到 `ModelAiEntertainDetail` 或一个新的表 `ModelAiReplyRecord`
	// 这部分逻辑需要根据你的业务需求添加。
	// 例如，在 `impl/msg_ai_impl.go` 中，`MsgAiEntertainMq` 成功发送后会记录 `ModelAiEntertainDetail`。
	// AI 主动发送的回复，也应该有类似的记录。
	// if strings.HasPrefix(req.CliMsgId, aiMsgPrefix) { // 可以用 CliMsgId 前缀区分
	//     err = aiEntertainDetail.Create(ctx, &qmsg.ModelAiEntertainDetail{
	//         CorpId:   req.CorpId,
	//         AppId:    req.AppId,
	//         RobotUid: req.RobotUid,
	//         CliMsgId: req.CliMsgId,
	//         // YcMsgId:  ycMsgId, // YcMsgId 在异步流程中较难获取
	//         SentAt:   utils.Now(),
	//         MsgBoxId: 0, // MsgBoxId 会在 addMsg 之后才有，这里直接调用 MQ，可能无法立即获取
	//     })
	//     if err != nil {
	//         log.Errorf("SendAiReply: 记录 AiEntertainDetail 失败 for CliMsgId %s: %v", req.CliMsgId, err)
	//         // 根据业务决定是否因为记录失败而返回错误
	//     }
	// }

	log.Infof("SendAiReply: AI回复请求处理完成 for CliMsgId %s.", req.CliMsgId)
	return &rsp, nil
}

func TransferChatToHuman(ctx *rpc.Context, req *qmsg.TransferChatToHumanReq) (*qmsg.TransferChatToHumanRsp, error) {
	var rsp qmsg.TransferChatToHumanRsp

	corpId, appId := req.CorpId, req.AppId
	robotUid := req.RobotUid
	chatExtId := req.ChatExtId
	isGroup := req.IsGroup
	targetUid := req.TargetUid
	mark := req.Mark

	// 1. 校验目标客服是否存在且有效
	transferUser, err := User.getUserById(ctx, corpId, appId, targetUid) // 调用 impl/model.go 中的 User.get 方法
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, rpc.InvalidArg("目标客服不存在或异常")
	}
	if transferUser.IsDisable {
		return nil, rpc.InvalidArg("目标客服已禁用")
	}
	// 业务逻辑上，AI转人工时，不强制要求客服在线（因为可能转到离线排队）
	// if !transferUser.IsOnline {
	//     return nil, rpc.InvalidArg("目标客服已下班")
	// }

	// 2. 获取会话信息 (qmsg_chat)
	chat, err := Chat.getByRobotAndExt(ctx, corpId, appId, robotUid, chatExtId, isGroup) // 调用 impl/model.go 中的 Chat.getByRobotAndExt
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, rpc.InvalidArg("会话不存在")
	}

	// --- 推导会话的实际 ChatType 枚举值 ---
	var deducedChatType qmsg.ChatType // 定义 deducedChatType
	if chat.IsGroup {
		deducedChatType = qmsg.ChatType_ChatTypeGroup
	} else {
		deducedChatType = qmsg.ChatType_ChatTypeSingle
	}
	// ------------------------------------

	// 3. 更新会话的接待类型为人工 (ModelChat.Detail.EntertainType)
	// 仅当当前是 AI 接待状态时才更新，避免覆盖人工客服的设置
	if chat.Detail == nil {
		chat.Detail = &qmsg.ModelChat_Detail{}
	}
	if chat.Detail.EntertainType == uint32(qmsg.ModelChat_Detail_Ai) || chat.Detail.EntertainType == uint32(qmsg.ModelChat_Detail_Nil) {
		chat.Detail.EntertainType = uint32(qmsg.ModelChat_Detail_Manual)
		_, err = Chat.WhereCorpApp(corpId, appId).Where(DbId, chat.Id).Update(ctx, map[string]interface{}{
			DbDetail: chat.Detail,
		})
		if err != nil {
			log.Errorf("err %v", err)
			// 不阻断主流程，但记录错误
		}
	}

	// 4. 关闭旧的 AI 接待记录 (qmsg_ai_entertain_record)
	// 仅当会话从 AI 模式转入人工时才需要关闭 AI 接待记录
	// 这个更新操作应该在更新 ModelChat_Detail_Manual 之后
	if chat.Detail.EntertainType == uint32(qmsg.ModelChat_Detail_Manual) { // 假设上述更新成功
		_, err = aiEntertainRecord.WhereCorpApp(corpId, appId).Where(DbEndAt, 0).Where(DbModelChatId, chat.Id).Update(ctx, map[string]interface{}{
			DbEndAt: utils.Now(),
		})
		if err != nil {
			log.Errorf("err %v", err)
			// 不阻断主流程，但记录错误
		}
	}

	// 5. 处理现有的分配记录 (qmsg_assign_chat)
	// 查找当前活跃的分配记录
	currentAssignChat, err := AssignChat.getByCid(ctx, corpId, appId, chat.Id) // 调用 impl/model.go 中的 AssignChat.getByCid
	if err != nil && !AssignChat.IsNotFoundErr(err) {
		log.Errorf("err:%v", err)
		return nil, err
	}

	var originalAssignUid uint64 // 记录原分配客服，用于后续 WS 推送
	if currentAssignChat != nil {
		// 如果当前会话已分配，且不是分配给目标客服，则先关闭现有分配
		if currentAssignChat.Uid != targetUid {
			originalAssignUid = currentAssignChat.Uid // 记录原分配客服
			err = AssignChat.setClosed(ctx, corpId, appId, map[string]interface{}{
				DbEndScene: uint32(qmsg.ModelAssignChat_EndSceneTransfer), // 标记为转接结束
			}, currentAssignChat.Id)
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, err
			}
			// 将原分配的会话加入最近联系人
			_, err = LastContactChat.createOrUpdateByAssignChatId(ctx, corpId, appId, currentAssignChat.Id) // 调用 impl/last_contact_chat.go 中的 LastContactChat.createOrUpdateByAssignChatId
			if err != nil {
				log.Errorf("err:%v", err)
				// 不阻断，记录错误
			}
			// 推送 WS 消息给原客服，移除会话
			if originalAssignUid != 0 {
				err = pushWsMsg(ctx, corpId, appId, originalAssignUid, []*qmsg.WsMsgWrapper{ // 调用 impl/ws.go 中的 pushWsMsg
					{
						MsgType:      uint32(qmsg.WsMsgWrapper_MsgTypeRemoveAssignChat),
						AssignChatId: currentAssignChat.Id,
						Chat:         chat, // 携带 chat 信息，前端可以更新本地状态
					},
				})
				if err != nil {
					log.Errorf("err:%v", err)
					// 不阻断，记录错误
				}
			}
		} else {
			// 已经分配给目标客服，无需重复分配
			rsp.NewAssignChatId = currentAssignChat.Id
			return &rsp, nil
		}
	}

	// 6. 创建新的分配记录 (qmsg_assign_chat)
	newAssignChat, err := AssignChat.create(ctx, corpId, appId, targetUid, originalAssignUid, robotUid, chatExtId, isGroup, chat, qmsg.ModelAssignChat_StartSceneSys) // AI 转人工可以视为系统自动分配
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	// 7. 添加转介备注消息 (系统消息)
	msgContent := &qmsg.ChatTransferChatRemarkMsg{
		Date:             time.Now().Format("2006年01月02日 15:04"),
		Mark:             mark,
		Username:         "AI助手", // 标记为 AI 助手转接
		TransferUsername: transferUser.Username,
		TransferUserId:   fmt.Sprintf("%d", transferUser.Id),
		TransferType:     uint32(qmsg.ChatTransferChatRemarkMsg_TransferTypeSystem), // 视为系统转接
	}

	msgContentBuffer, err := json.Marshal(msgContent)
	if err != nil {
		return nil, err
	}
	cliMsgId := "transfer_assign_chat_ai_" + utils.GenRandomStr() // 区分 AI 转接的系统消息
	msg := &qmsg.ModelMsgBox{
		CorpId:      corpId,
		AppId:       appId,
		Uid:         newAssignChat.RobotUid, // 系统消息的发送者是机器人
		CliMsgId:    cliMsgId,
		MsgType:     uint32(qmsg.MsgType_MsgTypeTransferChatRemark),
		ChatType:    uint32(deducedChatType), // 修正此处为 deducedChatType
		ChatId:      chat.ChatExtId,
		ChatMsgType: uint32(qmsg.ChatMsgType_ChatMsgTypeTransferChatRemark),
		Msg:         string(msgContentBuffer),
		SentAt:      uint32(time.Now().Unix()),
	}
	// 添加系统消息到 MsgBox
	_, _, err = addMsg(ctx, msg, true) // 调用 impl/msg.go 中的 addMsg
	if err != nil {
		log.Errorf("err:%v", err)
		// 不阻断主流程，但记录错误
	}

	// 记录转介日志
	transferLog := &qmsg.ModelTransferLog{
		CorpId:       corpId,
		AppId:        appId,
		CliMsgId:     cliMsgId,
		ChatId:       newAssignChat.ChatExtId,
		Uid:          newAssignChat.RobotUid,
		AssignChatId: newAssignChat.Id,
		Mark:         mark,
		ChatType:     uint32(deducedChatType), // 修正此处为 deducedChatType
	}
	_, err = TransferLog.updateOrCreate(ctx, transferLog) // 调用 impl/model.go 中的 TransferLog.updateOrCreate
	if err != nil {
		log.Errorf("err:%v", err)
		// 不阻断主流程，但记录错误
	}

	// 8. WS 推送通知
	// 通知新的目标客服
	err = pushWsMsg(ctx, corpId, appId, targetUid, []*qmsg.WsMsgWrapper{ // 调用 impl/ws.go 中的 pushWsMsg
		{
			MsgType:    uint32(qmsg.WsMsgWrapper_MsgTypeAssignChat),
			AssignChat: newAssignChat,
			Chat:       chat,
			MsgBox:     msg, // 携带系统消息，前端可以显示转接提示
		},
	})
	if err != nil {
		log.Errorf("err:%v", err)
		// 不阻断主流程，但记录错误
	}

	// 同时通知拥有该机器人权限的客服，确保会话状态一致性
	// 这里需要根据业务需求决定是否推送到所有相关客服，或者只推送到新的分配客服。
	// 如果 isGroup，可能需要通知群内所有关注的客服。
	err = pushWsMsgByRobotUid(ctx, corpId, appId, robotUid, []*qmsg.WsMsgWrapper{ // 调用 impl/ws.go 中的 pushWsMsgByRobotUid
		{
			MsgType:    uint32(qmsg.WsMsgWrapper_MsgTypeAssignChat),
			AssignChat: newAssignChat,
			Chat:       chat,
			MsgBox:     msg,
		},
	})
	if err != nil {
		log.Errorf("err:%v", err)
		// 不阻断主流程，但记录错误
	}

	rsp.NewAssignChatId = newAssignChat.Id
	return &rsp, nil
}
