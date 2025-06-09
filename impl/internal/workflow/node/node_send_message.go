// file: impl/internal/workflow/node/node_send_message.go
package node

import (
	"context"
	"encoding/json"
	"fmt"
	"git.pinquest.cn/base/aiadmin"
	"git.pinquest.cn/base/aiadminserver/impl/internal/workflow/common"
	"git.pinquest.cn/base/aiadminserver/impl/internal/workflow/logger" // 引入 logger
	"git.pinquest.cn/base/aiadminserver/impl/internal/workflow/types"
	utils2 "git.pinquest.cn/base/aiadminserver/impl/pkg/utils"
	baselog "git.pinquest.cn/base/log" // 项目标准日志
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/core"
	"git.pinquest.cn/qlb/qmsg" // 引入qmsg服务的proto定义
)

type SendMessageNode struct {
	nodeDef *aiadmin.WorkflowNodeDef       // 存储完整的节点定义
	config  *aiadmin.SendMessageNodeConfig // 解析后的配置
}

// NewSendMessageNodeBuilder 【修改】接收 *aiadmin.WorkflowNodeDef
func NewSendMessageNodeBuilder(nodeDef *aiadmin.WorkflowNodeDef) (common.NodeBuilder, error) {
	cfg := &aiadmin.SendMessageNodeConfig{}
	if err := json.Unmarshal([]byte(nodeDef.NodeConfigJson), cfg); err != nil {
		return nil, fmt.Errorf("反序列化发送消息节点 '%s' 配置失败: %w", nodeDef.Id, err)
	}
	return &SendMessageNode{
		nodeDef: nodeDef,
		config:  cfg,
	}, nil
}

// Build 【修改】接收 LogContext，并在 Lambda 中使用
// Build 方法修改：现在返回一个 common.NodeLogicFunc，移除日志记录部分
func (n *SendMessageNode) Build(lc *logger.LogContext) (any, error) { // 修改返回类型
	// 返回节点的纯业务逻辑函数
	var nodeLogic common.NodeLogicFunc = func(stdLibCtx context.Context, p *types.WorkflowPayload) (*types.WorkflowPayload, error) {
		// ---- 实际节点逻辑 ----
		if p == nil {
			err := fmt.Errorf("发送消息节点 [%s] 接收到的载荷为 nil", n.nodeDef.Id)
			p = &types.WorkflowPayload{} // 创建一个空的、带错误的payload
			p.Error = err
			return p, nil
		}
		if p.Error != nil {
			baselog.Warnf("SendMessageNode [%s]: 上游节点已出错，跳过发送消息: %v", n.nodeDef.Id, p.Error)
			p.MessageSentByNode = false
			return p, nil
		}

		// --- send_mode 严格分支 ---
		var messageToSendText string
		var finalMaterials []*aiadmin.MaterialInfo
		sendMessageMode := aiadmin.SendMessageMode_SendMessageModeFromKnowledge // 默认为1
		if n.config != nil {
			sendMessageMode = aiadmin.SendMessageMode(n.config.GetSendMode())
		}

		switch sendMessageMode {
		case aiadmin.SendMessageMode_SendMessageModeCustom: // 2
			// 发送节点配置中预设的自定义内容
			if n.config != nil {
				messageToSendText = n.config.GetContent()
				finalMaterials = n.config.GetExtra()
			}
			baselog.Debugf("[SendMessageNode:%s] Mode: SendMessageModeCustom. Using configured content: '%s'", n.nodeDef.Id, messageToSendText)
		case aiadmin.SendMessageMode_SendMessageModeFromKnowledge: // 1
			fallthrough
		default: // 也处理 SendMessageModeNil (0) 的情况
			// 发送从 payload 中传递过来的 FinalAnswer 和 FinalExtra
			// 【关键修改】: 此处不再检查 n.config.GetContent() 或其是否包含 {input}
			// 严格使用 payload 中的内容。
			messageToSendText = p.FinalAnswer
			finalMaterials = p.FinalExtra
			baselog.Debugf("[SendMessageNode:%s] Mode: SendMessageModeFromKnowledge (or default). Using payload.FinalAnswer: '%s'", n.nodeDef.Id, messageToSendText)
		}

		if messageToSendText == "" {
			var cfgContentForLog string
			if n.config != nil {
				cfgContentForLog = n.config.GetContent()
			}
			baselog.Warnf("SendMessageNode [%s]: 最终待发送的文本内容为空。Mode: %s. (Relevant source: if Custom, Node Config Content was '%s'; if FromKnowledge, Payload FinalAnswer was '%s'). 不发送消息。",
				n.nodeDef.Id, sendMessageMode.String(), cfgContentForLog, p.FinalAnswer)
			p.MessageSentByNode = false
			return p, nil
		}

		// 将最终确定的消息和素材更新回 payload，以便日志记录和可能的后续内部使用
		p.FinalAnswer = messageToSendText
		p.FinalExtra = finalMaterials

		baselog.Debugf("[SendMessageNode:%s] 准备发送消息. 最终文本: '%s', 素材数量: %d", n.nodeDef.Id, p.FinalAnswer, len(p.FinalExtra))

		var wfExecCtx *types.WorkflowExecutionContext
		if p.InitialQuery != nil && p.InitialQuery.Extra != nil {
			if ctxVal, ok := p.InitialQuery.Extra[types.WorkflowExecutionCtxKey]; ok {
				wfExecCtx, _ = ctxVal.(*types.WorkflowExecutionContext)
			}
		}

		if wfExecCtx == nil || wfExecCtx.InitialTask == nil {
			err := fmt.Errorf("SendMessageNode [%s]: 无法从载荷元数据中获取有效的 WorkflowExecutionContext 或 InitialTask", n.nodeDef.Id)
			p.Error = err
			return p, nil
		}
		aiWorkflowTask := wfExecCtx.InitialTask

		if wfExecCtx.RunMode == types.RunModeTest {
			baselog.Debugf("SendMessageNode [%s]: [测试模式] 模拟发送消息。文本: '%s'", n.nodeDef.Id, p.FinalAnswer)
			p.MessageSentByNode = true
			if p.CustomData == nil {
				p.CustomData = make(map[string]any)
			}
			p.CustomData[fmt.Sprintf("send_message_node_%s_status", n.nodeDef.Id)] = "success (mocked)"
			return p, nil
		}

		// 创建 qmsg 服务的 rpc.Context
		var qmsgRpcCtx *rpc.Context
		if lc != nil && lc.RpcCtx != nil { // 这里的 lc 是 Build 方法的参数，在 nodeLogic 闭包中可用
			qmsgRpcCtx = &rpc.Context{}
			qmsgRpcCtx.SetReqId(wfExecCtx.RequestId)
		} else {
			qmsgRpcCtx = &rpc.Context{}
			baselog.Warnf("SendMessageNode [%s]: LogContext or Lc.RpcCtx is nil, creating new rpc.Context for qmsg.", n.nodeDef.Id)
		}

		core.SetCorpAndApp(qmsgRpcCtx, aiWorkflowTask.GetCorpId(), aiWorkflowTask.GetAppId())
		core.SetPinHeaderUId(qmsgRpcCtx, aiWorkflowTask.GetRobotUid())

		cliMsgId := utils2.NewCliMsgId()

		sendReq := &qmsg.SendAiReplyReq{
			CorpId:      aiWorkflowTask.GetCorpId(),
			AppId:       aiWorkflowTask.GetAppId(),
			RobotUid:    aiWorkflowTask.GetRobotUid(),
			ChatType:    aiWorkflowTask.GetChatType(),
			ChatId:      aiWorkflowTask.GetChatId(),
			ChatMsgType: uint32(qmsg.ChatMsgType_ChatMsgTypeText),
			Msg:         p.FinalAnswer,
			CliMsgId:    cliMsgId,
		}
		// TODO: 处理素材 p.FinalExtra -> sendReq

		baselog.Debugf("SendMessageNode [%s]: RPC 调用 qmsg.SendAiReply. CliMsgId: %s, ChatId: %d, Text: %s",
			n.nodeDef.Id, sendReq.CliMsgId, sendReq.ChatId, sendReq.Msg)

		sendRsp, sendErr := sendAiReplyToQmsg(qmsgRpcCtx, sendReq)

		if sendErr != nil {
			err := fmt.Errorf("调用 qmsg.SendAiReply 失败 for CliMsgId %s: %w", sendReq.CliMsgId, sendErr)
			p.Error = err
			p.MessageSentByNode = false
			if p.CustomData == nil {
				p.CustomData = make(map[string]any)
			}
			p.CustomData[fmt.Sprintf("send_message_node_%s_status", n.nodeDef.Id)] = "failed"
			p.CustomData[fmt.Sprintf("send_message_node_%s_error", n.nodeDef.Id)] = err.Error()
			p.CustomData[fmt.Sprintf("send_message_node_%s_request", n.nodeDef.Id)] = sendReq
			return p, nil
		}

		baselog.Debugf("SendMessageNode [%s]: qmsg.SendAiReply 调用成功 for CliMsgId %s. YcMsgId: %s", n.nodeDef.Id, sendReq.CliMsgId, sendRsp.GetYcMsgId())
		p.MessageSentByNode = true

		if p.CustomData == nil {
			p.CustomData = make(map[string]any)
		}
		p.CustomData[fmt.Sprintf("send_message_node_%s_status", n.nodeDef.Id)] = "success"
		p.CustomData[fmt.Sprintf("send_message_node_%s_sent_text", n.nodeDef.Id)] = p.FinalAnswer
		p.CustomData[fmt.Sprintf("send_message_node_%s_cli_msg_id", n.nodeDef.Id)] = sendReq.CliMsgId
		if sendRsp != nil {
			p.CustomData[fmt.Sprintf("send_message_node_%s_yc_msg_id", n.nodeDef.Id)] = sendRsp.GetYcMsgId()
		}

		return p, nil
	}
	return nodeLogic, nil // 返回 NodeLogicFunc
}

// 将原来的 sendAiReply 重命名以避免与可能存在的全局函数冲突
func sendAiReplyToQmsg(ctx *rpc.Context, req *qmsg.SendAiReplyReq) (*qmsg.SendAiReplyRsp, error) {
	baselog.Debugf("sendAiReplyToQmsg: CorpId=%d, AppId=%d, RobotUid=%d, ChatId=%d, MsgLen=%d",
		req.CorpId, req.AppId, req.RobotUid, req.ChatId, len(req.Msg))
	reply, err := qmsg.SendAiReply(ctx, req)
	if err != nil {
		baselog.Errorf("sendAiReplyToQmsg: qmsg.SendAiReply failed: %v. Request: %+v", err, req)
		return nil, err
	}
	return reply, nil
}
