// impl/aiadmin_helpers.go
package impl

import (
	"encoding/json"
	"git.pinquest.cn/base/aiadmin"
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/qmsg"
)

const defaultHistoryLimit uint32 = 10 // 获取最近消息的默认条数

// getRecentHistoryForAI 封装了获取和转换最近聊天记录的逻辑
func getRecentHistoryForAI(
	ctx *rpc.Context,
	corpId uint32,
	appId uint32,
	robotUid uint64, // 当前AI机器人（消息接收方或发送方）的UID
	chatId uint64, // 会话对方的ID（如果是单聊）或群聊ID
	chatType qmsg.ChatType,
	limit uint32,
) ([]*aiadmin.ChatMessage, error) {
	if limit == 0 {
		limit = defaultHistoryLimit
	}

	// 1. 从数据库获取原始消息列表 (复用 ListRecentCustomerTextMsgs 的核心逻辑)
	// 注意：ListRecentCustomerTextMsgs 原本是为特定AI回复场景写的，可能需要微调以适应通用历史记录获取
	// 这里我们直接调用并适配
	qmsgMessages, err := listRecentMessages(ctx, corpId, appId, robotUid, chatId, uint32(chatType), limit)
	if err != nil {
		log.Errorf("getRecentHistoryForAI: listRecentMessages 失败: %v", err)
		return nil, err
	}

	if len(qmsgMessages) == 0 {
		return []*aiadmin.ChatMessage{}, nil
	}

	// 旧代码中的 reverse 是将时间倒序的消息列表再反转成时间正序，如果 listRecentMessages 已经是正序，则不需要
	// qmsgMessages = reverse(qmsgMessages) // 假设 listRecentMessages 返回的是按时间倒序的，需要反转成正序

	// 2. 将 qmsg.ModelMsgBox 转换为 aiadmin.ChatMessage
	var aiAdminMessages []*aiadmin.ChatMessage
	for _, msgBox := range qmsgMessages {
		textContent := extractMsgContent(msgBox) // 复用或改写 extractMessageContent
		if textContent == "" {
			// 可以选择跳过非纯文本或无法提取内容的消息
			continue
		}

		role := aiadmin.RoleTypeUser
		// 判断消息发送者以确定角色
		// 在单聊中:
		//   - 如果 SenderAccountId == chatId (对方的ID), 则是对方（USER）发送的
		//   - 如果 SenderAccountId == robotUid (当前AI机器人的某个关联账户ID), 则是AI (ASSISTANT)发送的
		//   需要明确 robotUid 如何映射到 ModelMsgBox 中的 SenderAccountId
		//   通常 qrobot.ModelAccount 中会有 Uid 和 Id (作为 AccountId) 的映射
		//   这里我们假设 robotUid 就是 AI 在 qmsg.ModelMsgBox 中的 SenderAccountId
		if msgBox.SenderAccountId != chatId { // 这里的 robotUid 应该是 msgBox.Uid 代表的机器人的 qrobot.ModelAccount.Id
			role = aiadmin.RoleTypeAssistant
		}
		// 注意：对于群聊，SenderAccountId 是发言人的 AccountId。
		// 如果需要区分群聊中哪个是AI的发言，逻辑会更复杂，可能需要检查 SenderAccountId 是否属于某个已知的AI机器人账户。
		// 为简化，当前主要针对单聊场景的AI回复。

		aiAdminMessages = append(aiAdminMessages, &aiadmin.ChatMessage{
			Role:      string(role),
			Content:   textContent,
			Timestamp: int64(msgBox.SentAt),
		})
	}

	return aiAdminMessages, nil
}

// listRecentMessages 从数据库获取原始消息 (基于 ListRecentCustomerTextMsgs 修改)
func listRecentMessages(
	ctx *rpc.Context,
	corpId uint32,
	appId uint32,
	robotUid uint64, // 这里的 robotUid 是指 qmsg.ModelChat.RobotUid 或 qmsg.ModelMsgBox.Uid
	chatExtId uint64, // 对方的 AccountId 或 GroupId
	chatType uint32, // qmsg.ChatType
	limit uint32,
) ([]*qmsg.ModelMsgBox, error) {
	if limit == 0 {
		limit = 10
	}

	db := MsgBox.WhereCorpApp(corpId, appId).
		Where(DbUid, robotUid).     // 当前机器人账号的Uid
		Where(DbChatId, chatExtId). // 交互对象的Id（用户或群）
		Where(DbChatType, chatType).
		Where(DbMsgType, qmsg.MsgType_MsgTypeChat). // 只拉取聊天类型消息
		// WhereIn(DbChatMsgType, []uint32{ // 可以根据需要筛选消息类型，例如只取文本和语音转文本
		// 	uint32(qmsg.ChatMsgType_ChatMsgTypeText),
		// 	uint32(qmsg.ChatMsgType_ChatMsgTypeVoice),
		// }).
		OrderDesc(DbMsgSeq). // 按消息序号倒序，获取最新的
		SetLimit(limit)

	db = ChoiceMsgBoxDb(db, corpId) // 确保这个函数存在且正确工作
	var msgList []*qmsg.ModelMsgBox
	err := db.Find(ctx, &msgList)
	if err != nil {
		return nil, err
	}

	// 由于是 OrderDesc 获取的，对于历史记录来说，顺序应该是反的，需要再次反转成时间正序
	if len(msgList) > 0 {
		for i, j := 0, len(msgList)-1; i < j; i, j = i+1, j-1 {
			msgList[i], msgList[j] = msgList[j], msgList[i]
		}
	}
	return msgList, nil
}

// extractMsgContent 从 qmsg.ModelMsgBox 提取纯文本内容 (基于 extractMessageContent 修改)
func extractMsgContent(m *qmsg.ModelMsgBox) string {
	if m.Msg == "" {
		return ""
	}
	// 只处理纯文本和已转换为文本的语音消息
	if m.ChatMsgType == uint32(qmsg.ChatMsgType_ChatMsgTypeText) {
		return m.Msg
	}

	if m.ChatMsgType == uint32(qmsg.ChatMsgType_ChatMsgTypeVoice) {
		var voiceMsg qmsg.ChatVoiceMsg
		if err := json.Unmarshal([]byte(m.Msg), &voiceMsg); err == nil {
			if voiceMsg.Text != "" { // 如果语音已转文字
				return voiceMsg.Text
			}
		}
	}
	// 其他类型的消息暂不提取内容或返回特定标记
	return ""
}

// reverse 函数
// func reverse[T any](s []*T) []*T {
// 	n := len(s)
// 	for i := 0; i < n/2; i++ {
// 		s[i], s[n-1-i] = s[n-1-i], s[i]
// 	}
// 	return s
// }

// GetRobotActiveWorkflowConfigFromAIAdmin 调用 aiadmin RPC 获取机器人激活的工作流配置
func GetRobotActiveWorkflowConfigFromAIAdmin(ctx *rpc.Context, corpId uint32, robotUid uint64) (*aiadmin.GetRobotActiveWorkflowConfigRsp, error) {
	req := &aiadmin.GetRobotActiveWorkflowConfigReq{
		CorpId:   corpId,
		RobotUid: robotUid,
	}

	rsp, err := aiadmin.GetRobotActiveWorkflowConfig(ctx, req)
	if err != nil {
		log.Errorf("GetRobotActiveWorkflowConfigFromAIAdmin: 调用 aiadmin.GetRobotActiveWorkflowConfig 失败: %v", err)
		return nil, err
	}
	return rsp, nil
}
