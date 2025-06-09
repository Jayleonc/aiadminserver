// file: internal/workflow/compiler.go
package workflow

//
//import (
//	"context"
//	"fmt"
//
//	"git.pinquest.cn/base/aiadmin" // 您的 DB model 定义
//	"github.com/cloudwego/eino/components/model"
//	"github.com/cloudwego/eino/components/retriever"
//	"github.com/cloudwego/eino/compose"
//)
//
//// CompileWorkflow 将数据库中定义的工作流、节点和边，基于方案B编译成一个可执行的 Eino Graph。
//func CompileWorkflow(
//	workflow *aiadmin.ModelWorkflow,
//	nodes []*aiadmin.ModelWorkflowNode,
//	edges []*aiadmin.ModelWorkflowEdge,
//	ctx context.Context, // 传入上下文，用于编译
//) (compose.Runnable[any, any], error) {
//
//	// 1. 初始化一个泛型图，输入输出都为 any
//	graph := compose.NewGraph[any, any]()
//
//	// 用于存储已添加到图中的节点信息，方便后续连接边
//	addedNodes := make(map[string]any)
//
//	// 2. [方案B核心] 遍历所有数据库节点，创建并添加 Eino 组件到图中
//	for _, dbNode := range nodes {
//		component, err := CreateRunnableFromDBNode(dbNode)
//		if err != nil {
//			return nil, fmt.Errorf("为节点 %s (类型: %s) 创建组件失败: %w", dbNode.NodeName, dbNode.NodeType, err)
//		}
//
//		// 使用 switch 类型断言来判断组件的具体类型，并调用相应的 Add 方法
//		switch c := component.(type) {
//		case *compose.Lambda:
//			if err := graph.AddLambdaNode(dbNode.Id, c); err != nil { //
//				return nil, fmt.Errorf("添加 Lambda 节点 %s 到图失败: %w", dbNode.Id, err)
//			}
//		case retriever.Retriever:
//			// 注意：AddRetrieverNode 的输入是 retriever.Retriever 接口
//			if err := graph.AddRetrieverNode(dbNode.Id, c); err != nil { //
//				return nil, fmt.Errorf("添加 Retriever 节点 %s 到图失败: %w", dbNode.Id, err)
//			}
//		case model.BaseChatModel:
//			// 如果未来有节点直接返回一个 ChatModel
//			if err := graph.AddChatModelNode(dbNode.Id, c); err != nil { //
//				return nil, fmt.Errorf("添加 ChatModel 节点 %s 到图失败: %w", dbNode.Id, err)
//			}
//		default:
//			return nil, fmt.Errorf("节点 %s 构建了未知的组件类型: %T", dbNode.NodeName, c)
//		}
//		addedNodes[dbNode.Id] = component
//	}
//
//	// 3. 根据 Edge 信息，在图中建立节点间的连接
//	edgesBySource := make(map[string][]*aiadmin.ModelWorkflowEdge)
//	for i := range edges { // 使用索引以避免拷贝大切片成员
//		edge := edges[i]
//		edgesBySource[edge.SourceNodeId] = append(edgesBySource[edge.SourceNodeId], edge)
//	}
//
//	for sourceID, outgoingEdges := range edgesBySource {
//		if _, sourceExists := addedNodes[sourceID]; !sourceExists && sourceID != compose.START {
//			return nil, fmt.Errorf("边连接错误：源节点 %s 未在图中定义", sourceID)
//		}
//
//		isConditionalBranch := false
//		if len(outgoingEdges) > 1 {
//			isConditionalBranch = true
//		} else if len(outgoingEdges) == 1 {
//			edge := outgoingEdges[0]
//			// 您定义的 EdgeType 和 Condition 字段在这里很有用
//			// 假设: condition 不为空且不为 "default" 或 "else" 时，认为是条件性的一条边
//			if edge.Condition != "" && edge.Condition != "default" && edge.Condition != "else" {
//				isConditionalBranch = true
//			}
//		}
//
//		if isConditionalBranch {
//			// 处理条件分支
//			targets := make(map[string]string) // 条件 -> 目标节点ID
//			var defaultTarget string
//			endNodesForBranchValidation := make(map[string]bool) // 用于 GraphBranch 构造
//
//			for _, edge := range outgoingEdges {
//				if _, targetExists := addedNodes[edge.TargetNodeId]; !targetExists && edge.TargetNodeId != compose.END {
//					return nil, fmt.Errorf("分支目标节点 %s 未定义", edge.TargetNodeId)
//				}
//				endNodesForBranchValidation[edge.TargetNodeId] = true
//
//				if edge.Condition == "else" || edge.Condition == "default" || (edge.Condition == "" && len(outgoingEdges) > 1) { // 如果条件为空且有多条边，也视为默认
//					if defaultTarget != "" { // 确保只有一个默认分支
//						return nil, fmt.Errorf("节点 %s 定义了多个默认分支", sourceID)
//					}
//					defaultTarget = edge.TargetNodeId
//				} else if edge.Condition != "" {
//					targets[edge.Condition] = edge.TargetNodeId
//				} else if len(outgoingEdges) == 1 && edge.Condition == "" { // 单条无条件边，前面已处理，这里理论上不会进入
//					// 这种情况会在下面的 else if len(outgoingEdges) == 1 中处理
//				}
//			}
//			// 确保如果 targets 为空且 defaultTarget 不为空（例如只有一个 else 分支），也能正确创建
//			if len(targets) == 0 && defaultTarget == "" && len(outgoingEdges) > 0 {
//				// 如果没有明确的条件分支，也没有默认分支，但有多条边，这是不明确的
//				// 但如果只有一条边且其 condition 为空，则应视为直接边
//				if len(outgoingEdges) == 1 && outgoingEdges[0].Condition == "" {
//					edge := outgoingEdges[0]
//					if _, targetExists := addedNodes[edge.TargetNodeId]; !targetExists && edge.TargetNodeId != compose.END {
//						return nil, fmt.Errorf("目标节点 %s 未定义", edge.TargetNodeId)
//					}
//					if err := graph.AddEdge(sourceID, edge.TargetNodeId); err != nil {
//						return nil, fmt.Errorf("添加边从 %s 到 %s 失败: %w", sourceID, edge.TargetNodeId, err)
//					}
//					continue // 处理下一组边
//				}
//				return nil, fmt.Errorf("节点 %s 的分支逻辑不明确：存在多条边但无有效条件或默认分支", sourceID)
//			}
//
//			// 创建一个 GraphBranch 实例。
//			// 这个实例的条件判断逻辑会接收 sourceID 节点的输出作为输入。
//			// 假设我们的条件节点（如问题分类器）的输出类型是 string
//			conditionFunc := func(ctx context.Context, conditionValue string) (string, error) {
//				if targetNode, ok := targets[conditionValue]; ok {
//					return targetNode, nil
//				}
//				if defaultTarget != "" {
//					return defaultTarget, nil
//				}
//				return "", fmt.Errorf("节点 %s 的输出 '%s' 没有匹配的分支，且没有定义默认分支", sourceID, conditionValue)
//			}
//
//			branchInstance := compose.NewGraphBranch[string](conditionFunc, endNodesForBranchValidation)
//
//			if err := graph.AddBranch(sourceID, branchInstance); err != nil { //
//				return nil, fmt.Errorf("为节点 %s 添加条件分支失败: %w", sourceID, err)
//			}
//
//		} else if len(outgoingEdges) == 1 {
//			// 处理简单的直接连接
//			edge := outgoingEdges[0]
//			if _, targetExists := addedNodes[edge.TargetNodeId]; !targetExists && edge.TargetNodeId != compose.END {
//				return nil, fmt.Errorf("目标节点 %s 未定义", edge.TargetNodeId)
//			}
//			if err := graph.AddEdge(sourceID, edge.TargetNodeId); err != nil { //
//				return nil, fmt.Errorf("添加边从 %s 到 %s 失败: %w", sourceID, edge.TargetNodeId, err)
//			}
//		}
//	}
//
//	// 4. 设置工作流的入口节点
//	if workflow.EntryNodeId == "" {
//		return nil, fmt.Errorf("工作流 %d (%s) 没有定义入口节点ID", workflow.Id, workflow.Name)
//	}
//	if _, exists := addedNodes[workflow.EntryNodeId]; !exists {
//		// 确保入口节点ID是有效的，已添加到图中的
//		return nil, fmt.Errorf("定义的入口节点ID '%s' 不存在于图中", workflow.EntryNodeId)
//	}
//	// 将 Eino 的隐式 START 节点连接到我们定义的入口节点
//	if err := graph.AddEdge(compose.START, workflow.EntryNodeId); err != nil { //
//		return nil, fmt.Errorf("连接 START 到入口节点 %s 失败: %w", workflow.EntryNodeId, err)
//	}
//
//	// 5. 编译 Graph，生成最终的可执行对象
//	runnableGraph, err := graph.Compile(ctx, compose.WithGraphName(workflow.Name)) //
//	if err != nil {
//		return nil, fmt.Errorf("编译工作流 %s (%d) 失败: %w", workflow.Name, workflow.Id, err)
//	}
//
//	return runnableGraph, nil
//}
