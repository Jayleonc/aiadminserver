package workflow

import (
	"fmt"
	"git.pinquest.cn/base/aiadmin"
	"git.pinquest.cn/base/aiadminserver/impl/internal/workflow/common"
	"git.pinquest.cn/base/aiadminserver/impl/internal/workflow/node"
)

// NodeFactoryFunc 【修改】定义了创建节点构建器的函数签名。
type NodeFactoryFunc func(nodeDef *aiadmin.WorkflowNodeDef) (common.NodeBuilder, error)

var NodeFactoryMap = map[string]NodeFactoryFunc{
	"trigger":             node.NewTriggerNodeBuilder,
	"question_classifier": node.NewQuestionClassifierNodeBuilder,
	"llm_model":           node.NewLLMModelNodeBuilder,
	"query_knowledge":     node.NewQueryKnowledgeNodeBuilder,
	"send_message":        node.NewSendMessageNodeBuilder,
	//"transfer_to_human":   node.NewTransferToHumanNodeBuilder,
	// "router": node.NewRouterNodeBuilder, // RouterNode 比较特殊，它不直接执行业务逻辑，而是路由
	// 如果 RouterNode 本身也需要记录启动/结束，它也需要遵循此模式
	// 但 router 节点的 "输入/输出" 概念与业务节点不同
}

// CreateNodeBuilderFromDefinition 【修改】调用新的工厂函数签名
func CreateNodeBuilderFromDefinition(nodeDef *aiadmin.WorkflowNodeDef) (common.NodeBuilder, error) {
	nodeType := nodeDef.NodeType

	factoryFunc, ok := NodeFactoryMap[nodeType]
	if !ok {
		// 处理兼容旧的类型名
		switch nodeType {
		case "llm":
			factoryFunc, ok = NodeFactoryMap["llm_model"]
		case "knowledge_query":
			factoryFunc, ok = NodeFactoryMap["query_knowledge"]
		case "classifier":
			factoryFunc, ok = NodeFactoryMap["question_classifier"]
		// ... 其他兼容映射
		default:
			return nil, fmt.Errorf("未知的节点类型: '%s' for node ID '%s'", nodeType, nodeDef.Id)
		}
		if !ok {
			return nil, fmt.Errorf("兼容旧节点类型 '%s' 失败，未找到对应的新构建器 for node ID '%s'", nodeType, nodeDef.Id)
		}
	}
	// 直接传递整个 nodeDef 给 NewXXXNodeBuilder
	return factoryFunc(nodeDef)
}
