// file: internal/workflow/node/node_llm.go
package node

import (
	"context"
	"encoding/json"
	"fmt"
	"git.pinquest.cn/base/aiadmin"
	"git.pinquest.cn/base/aiadminserver/impl/internal/workflow/common"
	"git.pinquest.cn/base/aiadminserver/impl/internal/workflow/logger"
	"git.pinquest.cn/base/aiadminserver/impl/internal/workflow/types"
	pkgllm "git.pinquest.cn/base/aiadminserver/impl/pkg/llm"
	baselog "git.pinquest.cn/base/log"
	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/components/prompt"
	"github.com/cloudwego/eino/schema"
	"strings"
)

// LLMModelNode 持有节点的状态
type LLMModelNode struct {
	nodeDef *aiadmin.WorkflowNodeDef
	config  *aiadmin.LLMModelNodeConfig
}

// NewLLMModelNodeBuilder 接收 WorkflowNodeDef
func NewLLMModelNodeBuilder(nodeDef *aiadmin.WorkflowNodeDef) (common.NodeBuilder, error) {
	cfg := &aiadmin.LLMModelNodeConfig{}
	if err := json.Unmarshal([]byte(nodeDef.NodeConfigJson), cfg); err != nil {
		return nil, fmt.Errorf("反序列化LLM节点'%s'配置失败: %w", nodeDef.Id, err)
	}

	if cfg.Model == nil {
		return nil, fmt.Errorf("LLM节点'%s'配置中缺少 'model' 配置块", nodeDef.Id)
	}
	// 校验 provider、model、type 字段
	if cfg.Model.Provider == "" {
		return nil, fmt.Errorf("LLM节点'%s'配置中必须指定 model.provider_id (即 provider 类型)", nodeDef.Id)
	}
	if cfg.Model.Model == "" {
		return nil, fmt.Errorf("LLM节点'%s'配置中必须指定 model.model", nodeDef.Id)
	}
	if cfg.Model.Type == "" {
		return nil, fmt.Errorf("LLM节点'%s'配置中必须指定 model.type (如 chat/embedding)", nodeDef.Id)
	}

	return &LLMModelNode{
		nodeDef: nodeDef,
		config:  cfg,
	}, nil
}

// Build 接收 LogContext，并在 Lambda 中使用它
func (n *LLMModelNode) Build(lc *logger.LogContext) (any, error) { // 修改返回类型
	if n.config == nil || n.config.Model == nil {
		return nil, fmt.Errorf("LLM节点'%s'的配置或模型配置为空", n.nodeDef.Id)
	}

	// 可根据 n.config.Model.Type 做不同模型类型的初始化（如 chat/embedding）
	// 目前仅支持 chat 类型，后续可扩展 embedding 等
	var chatModel model.BaseChatModel
	var err error
	if n.config.Model.Type == "chat" {
		chatModel, err = pkgllm.GetChatModel(context.Background(), n.config.Model.Provider)
		if err != nil {
			return nil, fmt.Errorf("为LLM节点'%s'创建ChatModel客户端失败(ProviderType: %s): %w", n.nodeDef.Id, n.config.Model.Provider, err)
		}
	} else {
		return nil, fmt.Errorf("LLM节点'%s'暂不支持模型类型: %s", n.nodeDef.Id, n.config.Model.Type)
	}

	systemInstruction := n.config.PromptText
	if systemInstruction == "" {
		systemInstruction = "你是一个乐于助人的AI助手。"
	}
	einoMessageTemplates := []schema.MessagesTemplate{
		schema.SystemMessage(systemInstruction),
		schema.MessagesPlaceholder("chat_history", true),
		schema.UserMessage("{user_query}"),
	}
	chatTemplate := prompt.FromMessages(schema.FString, einoMessageTemplates...)

	// 返回节点的纯业务逻辑函数
	var nodeLogic common.NodeLogicFunc = func(stdLibCtx context.Context, p *types.WorkflowPayload) (*types.WorkflowPayload, error) {
		// ---- 实际节点逻辑 ----
		if p == nil {
			err := fmt.Errorf("LLM节点 [%s] 接收到的载荷为 nil", n.nodeDef.Id)
			p = &types.WorkflowPayload{Error: err}
			return p, nil
		}

		currentUserQuery := p.CurrentUserQuery
		if currentUserQuery == "" && p.InitialQuery != nil {
			currentUserQuery = p.InitialQuery.Content
		}
		if currentUserQuery == "" {
			err := fmt.Errorf("LLM节点 [%s] 无法获取有效的用户输入", n.nodeDef.Id)
			p.Error = err
			return p, nil
		}

		baselog.Debugf("[LLMNode:%s] InputPayload: UserQuery='%s', HistoryCount=%d, RetrievedContextLen=%d, ClassificationIn='%s', DraftAnswerIn='%s'",
			n.nodeDef.NodeName, p.CurrentUserQuery, len(p.RecentHistory), len(p.RetrievedContext), p.Classification, p.DraftAnswer)

		historyForEinoPrompt := common.ToEinoMessages(p.RecentHistory)
		templateVars := make(map[string]any)
		templateVars["user_query"] = currentUserQuery
		if len(historyForEinoPrompt) > 0 {
			templateVars["chat_history"] = historyForEinoPrompt
		} else {
			templateVars["chat_history"] = []*schema.Message{}
		}
		if strings.Contains(systemInstruction, "{retrieved_context}") {
			if p.RetrievedContext != "" {
				templateVars["retrieved_context"] = p.RetrievedContext
			} else {
				templateVars["retrieved_context"] = "没有额外的参考信息。"
			}
		}

		messages, formatErr := chatTemplate.Format(stdLibCtx, templateVars)
		if formatErr != nil {
			err := fmt.Errorf("LLM节点 [%s] 格式化提示词失败: %w", n.nodeDef.Id, formatErr)
			p.Error = err
			return p, nil
		}

		if p.CustomData == nil {
			p.CustomData = make(map[string]any)
		}
		nodeSpecificOutputLog := make(map[string]interface{})
		if len(messages) > 0 {
			formattedMessagesForLog := make([]map[string]string, len(messages))
			for i, msg := range messages {
				formattedMessagesForLog[i] = map[string]string{"role": string(msg.Role), "content": msg.Content}
			}
			nodeSpecificOutputLog["llm_prompt_messages"] = formattedMessagesForLog
		}
		p.CustomData[fmt.Sprintf("log_addon_output_for_node_%s", n.nodeDef.Id)] = nodeSpecificOutputLog

		baselog.Debugf("LLMNode [%s] - Formatted %d messages for Generate.", n.nodeDef.Id, len(messages))
		if len(messages) == 0 {
			err := fmt.Errorf("LLM节点 [%s] 格式化后的消息列表为空", n.nodeDef.Id)
			p.Error = err
			return p, nil
		}

		modelOpts := []model.Option{
			model.WithTemperature(n.config.Temperature),
			model.WithModel(n.config.Model.Model),
		}
		// 检查当前提供商类型是否为 Tongyi，并添加特定参数。
		// n.config.Model.Provider 此时已直接代表提供商类型（如 "tongyi"）。
		if n.config.Model.Provider == pkgllm.ProviderTongyi {
			customParams := model.WrapImplSpecificOptFn(func(o *map[string]any) {
				if *o == nil {
					*o = make(map[string]any)
				}
				(*o)["parameters"] = map[string]any{"enable_thinking": false}
			})
			modelOpts = append(modelOpts, customParams)
		}

		result, genErr := chatModel.Generate(stdLibCtx, messages, modelOpts...)
		if genErr != nil {
			err := fmt.Errorf("LLM节点 [%s] 模型生成失败 (Provider: %s, Model: %s): %w", n.nodeDef.Id, n.config.Model.Provider, n.config.Model.Model, genErr)
			p.Error = err
			if nodeSpecificOutputLog != nil {
				nodeSpecificOutputLog["llm_error_details"] = genErr.Error()
			}
			return p, nil
		}

		baselog.Debugf("LLMNode [%s] (Name: %s) LLM Raw Output: %s", n.nodeDef.Id, n.nodeDef.NodeName, result.Content)
		p.DraftAnswer = result.Content
		p.FinalAnswer = result.Content

		if nodeSpecificOutputLog != nil {
			nodeSpecificOutputLog["llm_raw_output_snippet"] = truncateString(result.Content, 200)
		}

		return p, nil
	}
	return nodeLogic, nil
}

// truncateString 辅助函数保持不变
func truncateString(s string, maxLen int) string {
	if len(s) == 0 {
		return ""
	}
	runes := []rune(s)
	if len(runes) > maxLen {
		return string(runes[:maxLen]) + "..."
	}
	return s
}
