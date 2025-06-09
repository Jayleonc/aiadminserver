// file: internal/workflow/node/node_classifier.go
package node

import (
	"context"
	"encoding/json"
	"fmt"
	"git.pinquest.cn/base/aiadmin"
	"git.pinquest.cn/base/aiadminserver/impl/internal/workflow/common"
	"git.pinquest.cn/base/aiadminserver/impl/internal/workflow/logger" // 引入 logger
	"git.pinquest.cn/base/aiadminserver/impl/internal/workflow/types"
	pkgllm "git.pinquest.cn/base/aiadminserver/impl/pkg/llm"
	baselog "git.pinquest.cn/base/log" // 项目标准日志
	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/schema"
	"strings"
)

// QuestionClassifierNode 持有节点的状态
type QuestionClassifierNode struct {
	nodeDef *aiadmin.WorkflowNodeDef              // 存储完整的节点定义
	config  *aiadmin.QuestionClassifierNodeConfig // 解析后的配置
}

// NewQuestionClassifierNodeBuilder 创建节点构建器实例
// 【修改】接收 *aiadmin.WorkflowNodeDef
func NewQuestionClassifierNodeBuilder(nodeDef *aiadmin.WorkflowNodeDef) (common.NodeBuilder, error) {
	cfg := &aiadmin.QuestionClassifierNodeConfig{}
	if err := json.Unmarshal([]byte(nodeDef.NodeConfigJson), cfg); err != nil {
		return nil, fmt.Errorf("反序列化问题分类器节点 '%s' 配置失败: %w", nodeDef.Id, err)
	}

	if cfg.Model == nil {
		return nil, fmt.Errorf("问题分类器节点 '%s' 配置中缺少 'model' 配置块", nodeDef.Id)
	}
	if cfg.Model.Provider == "" {
		return nil, fmt.Errorf("问题分类器节点 '%s' 配置中必须指定 model.provider_id", nodeDef.Id)
	}
	if cfg.Model.Model == "" {
		return nil, fmt.Errorf("问题分类器节点 '%s' 配置中必须指定 model.model", nodeDef.Id)
	}
	if len(cfg.Classifications) == 0 {
		return nil, fmt.Errorf("问题分类器节点 '%s' 配置中必须至少定义一个 classification item", nodeDef.Id)
	}

	return &QuestionClassifierNode{
		nodeDef: nodeDef,
		config:  cfg,
	}, nil
}

// Build 方法修改
// 【修改】接收 LogContext，并在 Lambda 中使用
// Build 方法修改：现在返回一个 common.NodeLogicFunc，移除日志记录部分
func (n *QuestionClassifierNode) Build(lc *logger.LogContext) (any, error) { // 修改返回类型
	chatModel, err := pkgllm.GetChatModel(context.Background(), n.config.Model.Provider)
	if err != nil {
		return nil, fmt.Errorf("为分类器节点 '%s' 创建ChatModel客户端失败 (Provider: %s): %w", n.nodeDef.Id, n.config.Model.Provider, err)
	}
	promptText := buildClassificationPrompt(n.config.Classifications)

	// 返回节点的纯业务逻辑函数
	var logic common.NodeLogicFunc = func(stdLibCtx context.Context, p *types.WorkflowPayload) (*types.WorkflowPayload, error) {
		// ---- 实际节点逻辑 ----
		if p == nil {
			err := fmt.Errorf("问题分类器节点 [%s] 接收到的载荷为 nil", n.nodeDef.Id)
			p = &types.WorkflowPayload{} // 创建一个空的、带错误的payload，以便后续处理
			p.Error = err
			return p, nil
		}
		if p.InitialQuery == nil && p.CurrentUserQuery == "" {
			err := fmt.Errorf("问题分类器节点 [%s] 接收到的载荷中 InitialQuery 和 CurrentUserQuery 均为空", n.nodeDef.Id)
			p.Error = err
			return p, nil
		}

		userInput := p.CurrentUserQuery
		if userInput == "" && p.InitialQuery != nil {
			userInput = p.InitialQuery.Content
		}
		if userInput == "" {
			err := fmt.Errorf("问题分类器节点 [%s] 无法获取有效的用户输入", n.nodeDef.Id)
			p.Error = err
			return p, nil
		}

		// baselog.Debugf("[ClassifierNode:%s][RunID:%s] 使用查询: '%s' 进行分类", n.nodeDef.Id, lc.WorkflowRunID, userInput) // lc 不再直接在业务逻辑中使用
		baselog.Debugf("[ClassifierNode:%s] 使用查询: '%s' 进行分类", n.nodeDef.Id, userInput)
		fullPrompt := fmt.Sprintf("%s\n用户问题：%s", promptText, userInput)
		messagesForLLM := []*schema.Message{schema.UserMessage(fullPrompt)}

		modelOpts := []model.Option{
			model.WithTemperature(0.2),
			model.WithModel(n.config.Model.Model),
		}
		if n.config.ImageRecognition {
			baselog.Debugf("[ClassifierNode:%s] 配置启用了图片识别，但当前执行逻辑主要处理文本。", n.nodeDef.Id)
		}
		_, accConfExists := pkgllm.GetProviderAccountConfig(n.config.Model.Provider)
		if accConfExists && n.config.Model.Provider == pkgllm.ProviderTongyi {
			customParams := model.WrapImplSpecificOptFn(func(o *map[string]any) {
				if *o == nil {
					*o = make(map[string]any)
				}
				(*o)["parameters"] = map[string]any{"enable_thinking": false}
			})
			modelOpts = append(modelOpts, customParams)
		}

		result, llmErr := chatModel.Generate(stdLibCtx, messagesForLLM, modelOpts...)
		if llmErr != nil {
			err := fmt.Errorf("分类器节点 [%s] 模型调用失败 (Provider: %s, Model: %s): %w", n.nodeDef.Id, n.config.Model.Provider, n.config.Model.Model, llmErr)
			p.Error = err
			return p, nil
		}

		cls := strings.TrimSpace(result.Content)
		isValidCls := false
		for _, c := range n.config.Classifications {
			if c.Id == cls {
				isValidCls = true
				break
			}
		}
		if !isValidCls {
			baselog.Debugf("[ClassifierNode:%s] 模型返回分类 '%s' 不在预设列表中，将使用默认路由 'else'", n.nodeDef.Id, cls)
			cls = "else"
		}
		if cls == "" {
			cls = "else"
		}

		baselog.Debugf("[ClassifierNode:%s] 分类结果: %s", n.nodeDef.Id, cls)
		p.Classification = cls
		if p.FlowControl == nil {
			p.FlowControl = make(map[types.PayloadKey]any)
		}
		p.FlowControl[types.FlowControlNextCondition] = cls

		return p, nil
	}
	return logic, nil
}

// buildClassificationPrompt 函数保持不变
func buildClassificationPrompt(classifications []*aiadmin.ClassificationItem) string {
	// ... (与之前相同)
	sb := strings.Builder{}
	sb.WriteString("你是一个严格的指令分类机器人。你的任务是根据用户提出的问题，将其精准地归类到以下几个类别中。")
	sb.WriteString("你必须选择一个最相关的类别ID作为回答，不能有任何多余的解释或文字。\n\n")
	sb.WriteString("可用类别列表：\n")
	for _, cls := range classifications {
		sb.WriteString(fmt.Sprintf("----------\n"))
		sb.WriteString(fmt.Sprintf("类别ID: %s\n", cls.Id))
		sb.WriteString(fmt.Sprintf("描述: %s\n", cls.Description))
	}
	sb.WriteString(fmt.Sprintf("----------\n\n"))
	sb.WriteString("现在，请分析下面的用户问题，并仅返回最匹配的类别ID。如果没有任何类别匹配，请返回 'else'。")
	return sb.String()
}
