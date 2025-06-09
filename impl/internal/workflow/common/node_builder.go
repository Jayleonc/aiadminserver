package common

import (
	"context"
	"git.pinquest.cn/base/aiadminserver/impl/internal/workflow/logger"
	"git.pinquest.cn/base/aiadminserver/impl/internal/workflow/types"
)

// NodeBuilder 是构建可运行 Eino 组件的接口。
// 每种节点类型都将实现此接口，以生成其特定的可运行组件。
type NodeBuilder interface {
	// Build 方法现在返回一个可以被 Eino 图直接使用的组件类型 (例如 *compose.Lambda, retriever.Retriever, model.BaseChatModel)
	// 或者是一个可以被包装成这些组件的原始业务逻辑函数。
	Build(lc *logger.LogContext) (any, error) // <--- 保持返回 any
}

// NodeLogicFunc 是节点核心业务逻辑的函数签名
type NodeLogicFunc func(context.Context, *types.WorkflowPayload) (*types.WorkflowPayload, error)
