// file: internal/workflow/node/node_transfer.go
package node

import (
	"context"
	"encoding/json"
	"fmt"
	"git.pinquest.cn/base/aiadmin"
	"git.pinquest.cn/base/aiadminserver/impl/internal/workflow/common"
	"git.pinquest.cn/base/aiadminserver/impl/internal/workflow/logger"
	"git.pinquest.cn/base/aiadminserver/impl/internal/workflow/types"
	// "github.com/cloudwego/eino/compose" // 不再直接在此文件使用 compose.InvokableLambda
	// 假设您有一个实际的人工客服系统服务
	// "your_project/services/customer_service"
)

// TransferToHumanNode 持有节点的状态
type TransferToHumanNode struct {
	nodeDef *aiadmin.WorkflowNodeDef // 新增：存储完整的节点定义
	config  *aiadmin.TransferToHumanNodeConfig
}

// NewTransferToHumanNodeBuilder 创建节点构建器实例
// 【修改】接收 *aiadmin.WorkflowNodeDef
func NewTransferToHumanNodeBuilder(nodeDef *aiadmin.WorkflowNodeDef) (common.NodeBuilder, error) { // 修改参数
	cfg := &aiadmin.TransferToHumanNodeConfig{}
	// rawConfig 从 nodeDef.NodeConfigJson 获取
	if err := json.Unmarshal([]byte(nodeDef.NodeConfigJson), cfg); err != nil {
		return nil, fmt.Errorf("反序列化转人工配置失败: %w", err)
	}
	if cfg.TransferMessage == "" {
		cfg.TransferMessage = "正在为您转接人工客服，请稍候..." // 提供默认值
	}
	return &TransferToHumanNode{nodeDef: nodeDef, config: cfg}, nil // 传入 nodeDef
}

// Build 方法修改：现在返回一个 common.NodeLogicFunc
func (n *TransferToHumanNode) Build(lc *logger.LogContext) (any, error) { // 修改返回类型
	// 返回节点的纯业务逻辑函数
	var nodeLogic common.NodeLogicFunc = func(ctx context.Context, p *types.WorkflowPayload) (*types.WorkflowPayload, error) {
		if p == nil {
			err := fmt.Errorf("转人工节点 [%s] 接收到的载荷为 nil", n.nodeDef.Id)
			p = &types.WorkflowPayload{}
			p.Error = err
			return p, nil
		}

		contextInfo := fmt.Sprintf("用户原始问题: %s. ", p.InitialQuery.Content)
		if p.Classification != "" {
			contextInfo += fmt.Sprintf("问题分类: %s. ", p.Classification)
		}
		if p.FinalAnswer != "" {
			contextInfo += fmt.Sprintf("系统尝试回复: %s. ", p.FinalAnswer)
		} else if p.RetrievedContext != "" {
			contextInfo += fmt.Sprintf("系统检索到的信息: %s. ", p.RetrievedContext)
		}

		fmt.Printf("[转人工 - %s] 触发转人工流程 (技能组: %s)\n", n.nodeDef.Id, n.config.TransferGroupId)
		fmt.Printf("    转接提示语: %s\n", n.config.TransferMessage)
		fmt.Printf("    附带上下文摘要: %s\n", contextInfo)

		// 在这里实现真实的转人工 API 调用逻辑
		// ...

		fmt.Println("    -> 已成功转接人工 (模拟)")

		p.FinalAnswer = n.config.TransferMessage
		p.FinalExtra = nil

		return p, nil
	}
	return nodeLogic, nil // 返回 NodeLogicFunc
}
