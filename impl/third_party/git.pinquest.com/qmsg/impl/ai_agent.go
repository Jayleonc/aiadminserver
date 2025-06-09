package impl

import (
	"encoding/json"
	"git.pinquest.cn/qlb/core"
	"strconv"
	"strings"
	"time"

	"git.pinquest.cn/base/aiagent"
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/qmsg"
)

var limit uint32 = 10

func GenerateAiReply(ctx *rpc.Context, req *qmsg.GenerateAiReplyReq) (*qmsg.GenerateAiReplyRsp, error) {
	user, err := getUserCheck(ctx)
	if err != nil {
		return nil, err
	}

	log.Infof("user: %v, req: %v\n", user, req)

	// 权限校验
	err = checkChatPermission(ctx, user, req.RobotUid, req.ChatId)
	if err != nil {
		return nil, err
	}

	msgList, err := ListRecentCustomerTextMsgs(
		ctx,
		req.ChatId,
		req.ChatType,
		req.RobotUid,
		limit,
	)

	if err != nil || len(msgList) == 0 {
		log.Errorf("获取客户发言失败: %v", err)
		return nil, rpc.CreateError(qmsg.ErrAiReplyNoContext)
	}

	msgList = trimAfterNoReply(ctx, msgList, req.ChatId, req.RobotUid)
	msgList = reverse(msgList) // 翻转

	if len(msgList) == 0 {
		log.Infof("🧩 无需回复过滤后，无可用上下文消息，返回默认提示语")
		replies := []string{
			"目前暂时没有检测到用户的新问题，如有需要我随时为您解答哦~",
		}
		return &qmsg.GenerateAiReplyRsp{
			Reply:   replies[0],
			Replies: replies,
		}, nil
	}

	var chatMessages []*aiagent.ChatMessage
	for _, m := range msgList {
		text := extractMessageContent(m)
		if text == "" {
			continue
		}

		role := aiagent.ChatRoleUser
		if m.SenderAccountId != req.ChatId {
			role = aiagent.ChatRoleAgent
		}

		chatMessages = append(chatMessages, &aiagent.ChatMessage{
			Role:      role,
			Content:   text,
			Timestamp: m.SentAt,
		})
	}

	corpId, _ := core.GetPinHeaderCorpIdAndAppId2Int(ctx)

	// 调用 AI Agent
	aiReq := &aiagent.GetAIReplyReq{
		//CorpId: user.CorpId,
		CorpId: corpId,
		//UserId:   strconv.FormatUint(user.Id, 10),
		UserId:   strconv.FormatUint(req.RobotUid, 10),
		ChatId:   strconv.FormatUint(req.ChatId, 10),
		Query:    req.Query,
		Messages: chatMessages,
	}

	PrintAIReq(aiReq)
	start := time.Now()
	aiResp, err := aiagent.GetAIReply(ctx, aiReq)
	if err != nil {
		log.Errorf("GetAIReply error: %v", err)
		return nil, rpc.CreateError(qmsg.ErrAiReplyGenFailed)
	}

	elapsed := time.Since(start).Milliseconds()
	aiResp.GetResponseTimeMs()
	log.Infof("🧠 AI 回复完成，总耗时: %dms，模型耗时: %dms，差值: %dms\n",
		elapsed,
		aiResp.GetResponseTimeMs(),
		elapsed-aiResp.GetResponseTimeMs(),
	)

	// 推送 WebSocket 消息
	//err = pushAiReplyWS(ctx, user.CorpId, user.AppId, req.RobotUid, req.ChatId, false, req.CliMsgId, reply)
	//if err != nil {
	//	log.Errorf("push ws err: %v", err)
	//	// 不 return err，确保 AI 结果依然返回
	//}

	// 解析多段回复（AI 双风格）
	replies := SplitDualReply(aiResp.Message)
	if len(replies) == 0 {
		replies = []string{"（AI 没有返回任何内容）"}
	}

	return &qmsg.GenerateAiReplyRsp{
		// 单条兼容字段，也可以保留第一条
		Reply: replies[0],
		// 新增字段（前端可用来展示多种风格选择）
		Replies: replies,
	}, nil
}

func reverse[T any](s []*T) []*T {
	n := len(s)
	for i := 0; i < n/2; i++ {
		s[i], s[n-1-i] = s[n-1-i], s[i]
	}
	return s
}

func SplitDualReply(reply string) []string {
	parts := strings.Split(reply, "---")
	var result []string
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

func PrintAIReq(req *aiagent.GetAIReplyReq) {
	// 用结构体序列化成漂亮 JSON（默认带换行、中文）
	b, err := json.MarshalIndent(req, "", "  ")
	if err != nil {
		log.Errorf("failed to marshal aiReq: %v", err)
		return
	}
	log.Infof("🔍 AI 请求结构:\n%s", b)
}

func extractMessageContent(m *qmsg.ModelMsgBox) string {
	if m.Msg == "" {
		return ""
	}

	if m.ChatMsgType == uint32(qmsg.ChatMsgType_ChatMsgTypeText) {
		// 文本类型，直接返回
		return m.Msg
	}

	// 尝试解析 JSON 格式，提取 text 字段（语音、图片可能有）
	var data map[string]interface{}
	if err := json.Unmarshal([]byte(m.Msg), &data); err != nil {
		return ""
	}

	if text, ok := data["text"].(string); ok && text != "" {
		return text
	}
	return ""
}

func ListRecentCustomerTextMsgs(
	ctx *rpc.Context,
	chatId uint64,
	chatType uint32,
	robotUid uint64,
	limit uint32,
) ([]*qmsg.ModelMsgBox, error) {
	if limit <= 0 {
		limit = 10
	}

	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)

	db := MsgBox.WhereCorpApp(corpId, appId).
		Where(DbChatId, chatId).
		Where(DbChatType, chatType).
		Where(DbUid, robotUid).
		// 普通文本和语音消息
		WhereIn(DbMsgType, []int32{int32(qmsg.MsgType_MsgTypeChat)}).
		WhereIn(DbChatMsgType, []int32{
			int32(qmsg.ChatMsgType_ChatMsgTypeText),
			int32(qmsg.ChatMsgType_ChatMsgTypeVoice),
		}).
		OrderDesc(DbMsgSeq).
		SetLimit(limit)
	db = ChoiceMsgBoxDb(db, corpId)
	var msgList []*qmsg.ModelMsgBox
	err := db.Find(ctx, &msgList)
	if err != nil {
		return nil, err
	}
	return msgList, nil
}

func trimAfterNoReply(ctx *rpc.Context, msgs []*qmsg.ModelMsgBox, chatId uint64, robotUid uint64) []*qmsg.ModelMsgBox {
	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)
	if len(msgs) == 0 {
		return msgs
	}

	// 找到当前消息列表里最老一条的 MsgSeq
	minSeq := msgs[len(msgs)-1].MsgSeq

	var noReply *qmsg.ModelMsgBox
	db := MsgBox.WhereCorpApp(corpId, appId).
		Where(DbChatId, chatId).
		Where(DbUid, robotUid).
		Where(DbMsgType, qmsg.MsgType_MsgTypeSetNotReply).
		// 只查比当前最老一条消息更新的“无需回复”标记
		Where("msg_seq > ?", minSeq).
		OrderDesc(DbMsgSeq).
		SetLimit(1) // 只要最新一条

	db = ChoiceMsgBoxDb(db, corpId)
	if err := db.First(ctx, &noReply); err != nil {
		return msgs
	}

	if noReply == nil {
		return msgs // 没有“无需回复”
	}

	cutoff := noReply.MsgSeq
	result := make([]*qmsg.ModelMsgBox, 0, limit)
	for _, m := range msgs {
		if m.MsgSeq <= cutoff {
			break
		}
		result = append(result, m)
	}
	return result
}
