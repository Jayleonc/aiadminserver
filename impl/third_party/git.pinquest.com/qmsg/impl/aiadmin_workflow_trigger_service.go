// impl/aiadmin_workflow_trigger_service.go
package impl

import (
	"fmt"
	"git.pinquest.cn/base/aiadmin"
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/brick/smq"
	"git.pinquest.cn/qlb/qmsg"
	"time"
)

// TriggerNewAIWorkflow 将构造并发送 AIWorkflowTriggerEventWithHistory 消息给 aiadmin 服务的新缓冲 Topic。
func TriggerNewAIWorkflow(
	ctx *rpc.Context,
	corpId uint32,
	appIdFromMsg uint32, // 消息上下文中的 AppId
	robotUidFromMsg uint64, // 接收消息的机器人 UID
	chatId uint64, // 会话对方的ID (单聊) 或群ID
	chatType qmsg.ChatType, // 聊天类型
	sendAccountId uint64, // 当前原始消息的发送者 AccountId
	keyword string, // 当前原始消息的内容
	cliMsgId string, // 当前原始消息的客户端ID
	recentHistory []*aiadmin.ChatMessage, // 获取到的近期历史消息
	targetWorkflowId string, // 【新增】目标工作流ID，应由调用方 (handleReceiveMsg) 决定
) error {
	log.Infof("TriggerNewAIWorkflow: 开始准备发送缓冲事件. CorpId: %d, RobotUid: %d, CliMsgId: %s, TargetWorkflowID: %s",
		corpId, robotUidFromMsg, cliMsgId, targetWorkflowId)

	if targetWorkflowId == "" {
		log.Errorf("TriggerNewAIWorkflow: 目标工作流ID (targetWorkflowId) 不能为空。")
		return fmt.Errorf("目标工作流ID不能为空")
	}

	// 1. 构造 AIWorkflowTriggerEventWithHistory 消息体
	bufferEvent := &aiadmin.AIWorkflowTriggerEventWithHistory{
		WorkflowId:             targetWorkflowId, // 使用传入的目标工作流ID
		CorpId:                 corpId,
		AppId:                  appIdFromMsg,
		RobotUid:               robotUidFromMsg,
		ChatId:                 chatId,
		ChatType:               uint32(chatType),
		SendAccountId:          sendAccountId,
		SingleMessageKeyword:   keyword,
		SingleMessageCliMsgId:  cliMsgId,
		SingleMessageTimestamp: time.Now().UnixMilli(), // 使用当前时间作为消息时间戳
		RecentHistory:          recentHistory,
	}

	pubReq := &smq.PubReq{
		CorpId: bufferEvent.CorpId,
		AppId:  bufferEvent.AppId,
		// Hash: utils.HashStr(fmt.Sprintf("%d-%d", bufferEvent.RobotUid, bufferEvent.ChatId)), // 可选
	}

	pubRsp, pubErr := MqAIWorkflowBufferEvent.PubV2(ctx, pubReq, bufferEvent)

	if pubErr != nil {
		log.Errorf("TriggerNewAIWorkflow: 发布 AIWorkflowTriggerEventWithHistory 失败 for CliMsgId %s: %v", bufferEvent.SingleMessageCliMsgId, pubErr)
		return fmt.Errorf("发布缓冲事件到MQ失败: %w", pubErr)
	}

	if pubRsp != nil {
		log.Infof("TriggerNewAIWorkflow: AIWorkflowTriggerEventWithHistory 已成功发布到MQ, MQ MsgId: %s, CliMsgId: %s, WorkflowId: %s",
			pubRsp.MsgId, bufferEvent.SingleMessageCliMsgId, bufferEvent.WorkflowId)
	} else {
		// 如果是模拟或者 PubV2 不一定返回 Rsp
		log.Infof("TriggerNewAIWorkflow: AIWorkflowTriggerEventWithHistory 发布请求已提交, CliMsgId: %s, WorkflowId: %s",
			bufferEvent.SingleMessageCliMsgId, bufferEvent.WorkflowId)
	}
	return nil
}
