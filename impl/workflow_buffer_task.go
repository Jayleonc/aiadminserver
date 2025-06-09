package impl

import (
	"git.pinquest.cn/base/aiadmin"
	"git.pinquest.cn/base/aiadminserver/impl/internal/workflow/buffer"
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/redisgroup"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/brick/smq"
)

var workflowBufferService *buffer.Service

// InitWorkflowBufferService 在应用启动时调用
func InitWorkflowBufferService(rg *redisgroup.RedisGroup) {
	workflowBufferService = buffer.NewService(rg)
	log.Infof("WorkflowBufferService initialized.")
}

// HandleAIWorkflowBufferEvent 是 AIWorkflowTriggerEventWithHistory 消息的 MQ 消费者。
func HandleAIWorkflowBufferEvent(rpcCtx *rpc.Context, req *smq.ConsumeReq) (*smq.ConsumeRsp, error) {
	log.Infof("HandleAIWorkflowBufferEvent: 接收到原始MQ请求, MsgId: %s, CorpId: %d", req.MsgId, req.CorpId)
	var rsp smq.ConsumeRsp

	// MqAIWorkflowBufferEvent 在 impl/mq.go 中定义
	return MqAIWorkflowBufferEvent.ProcessV2(rpcCtx, req, func(innerRpcCtx *rpc.Context, consumerReq *smq.ConsumeReq, data interface{}) (*smq.ConsumeRsp, error) {
		event, ok := data.(*aiadmin.AIWorkflowTriggerEventWithHistory)
		if !ok {
			log.Errorf("HandleAIWorkflowBufferEvent: 消息体类型断言失败，期望 *aiadmin.AIWorkflowTriggerEventWithHistory，实际为 %T", data)
			// 对于类型断言错误，不应重试
			return &rsp, nil // 或者返回一个指示消息格式错误的 error
		}

		log.Debugf("HandleAIWorkflowBufferEvent: 开始处理事件, WorkflowID: %s, ChatID: %d", event.WorkflowId, event.ChatId)
		log.Debugf("HandleAIWorkflowBufferEvent: Event: single_message_keyword: %v", event.SingleMessageKeyword)
		for _, msg := range event.RecentHistory {
			log.Debugf("HandleAIWorkflowBufferEvent: Event: recent_history role: %s, content: %v", msg.Role, msg.Content)
		}

		if workflowBufferService == nil {
			log.Errorf("HandleAIWorkflowBufferEvent: WorkflowBufferService尚未初始化！")
			rsp.Retry = true
			return &rsp, rpc.CreateErrorWithMsg(rpc.KSystemError, "WorkflowBufferService尚未初始化")
		}

		if err := workflowBufferService.HandleIncomingEvent(innerRpcCtx, event); err != nil {
			log.Errorf("HandleAIWorkflowBufferEvent: 处理 WorkflowID %s, ChatID %d 的事件失败: %v", event.WorkflowId, event.ChatId, err)
			rsp.Retry = true // 处理错误时进行重试
			return &rsp, err
		}

		log.Infof("HandleAIWorkflowBufferEvent: 成功处理 WorkflowID %s, ChatID %d 的事件", event.WorkflowId, event.ChatId)
		return &rsp, nil
	})
}
