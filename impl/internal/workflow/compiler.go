// file: impl/internal/workflow/compiler.go
package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"git.pinquest.cn/base/aiadmin"
	"git.pinquest.cn/base/aiadminserver/impl/internal/workflow/common"
	"git.pinquest.cn/base/aiadminserver/impl/internal/workflow/logger"
	"git.pinquest.cn/base/aiadminserver/impl/internal/workflow/types"
	baselog "git.pinquest.cn/base/log"
	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/components/retriever"
	"github.com/cloudwego/eino/compose"
	"github.com/cloudwego/eino/schema"
	"time"
)

// CompileWorkflow ...
func CompileWorkflow(
	workflowDef *aiadmin.ModelWorkflow,
	activeVersion *aiadmin.ModelWorkflowVersion,
	stdCtx context.Context,
	lc *logger.LogContext, // LogContext 仍然用于包装器
) (compose.Runnable[*schema.Message, *types.WorkflowPayload], error) { //

	if activeVersion == nil || activeVersion.GraphSnapshotJson == "" {
		return nil, fmt.Errorf("工作流版本或图快照为空")
	}
	var graphSnapshot aiadmin.WorkflowGraphSnapshot
	if err := json.Unmarshal([]byte(activeVersion.GraphSnapshotJson), &graphSnapshot); err != nil {
		return nil, fmt.Errorf("解析图快照失败: %w", err)
	}

	parsedNodeDefs := graphSnapshot.Nodes
	parsedEdges := graphSnapshot.Edges
	entryNodeIdFromVersion := activeVersion.EntryNodeId

	graph := compose.NewGraph[*schema.Message, *types.WorkflowPayload]()
	nodeComponentMap := make(map[string]any) // 用于存储最终的 Eino 组件

	for _, nodeDef := range parsedNodeDefs {
		builder, err := CreateNodeBuilderFromDefinition(nodeDef) //
		if err != nil {
			return nil, fmt.Errorf("创建节点构建器 '%s' (类型: %s) 失败: %w", nodeDef.Id, nodeDef.NodeType, err)
		}

		var einoComponentForGraph any

		// builder.Build(lc) 会返回一个 `any` 类型。
		// 对于大多数节点，它会返回一个 `common.NodeLogicFunc`。
		// 对于 TriggerNode，它会返回 `*compose.Lambda`。
		rawComponent, buildErr := builder.Build(lc) //
		if buildErr != nil {
			return nil, fmt.Errorf("构建节点 '%s' 的 Eino 组件失败: %w", nodeDef.Id, buildErr)
		}

		// 核心逻辑：在这里应用日志包装器
		// 注意：TriggerNode 不会被此通用日志包装器包装，因为它有不同的签名且内部已处理。
		if nodeDef.NodeType == "trigger" {
			// TriggerNode 已经是一个 *compose.Lambda 了，直接使用
			einoComponentForGraph = rawComponent
		} else if nodeLogicFunc, ok := rawComponent.(common.NodeLogicFunc); ok {
			// 只要是 NodeLogicFunc，统一日志包装后包成 Lambda
			currentNodeDef := nodeDef
			loggingWrappedLogic := func(lambdaCtx context.Context, pIn *types.WorkflowPayload) (*types.WorkflowPayload, error) {
				if lc == nil {
					baselog.Warnf("[CompilerWrapper][Node:%s] LogContext (lc) is NIL! DB日志将不会被记录。", currentNodeDef.Id)
					return nodeLogicFunc(lambdaCtx, pIn)
				}
				nodeStartTime := time.Now()
				var nodeInnerErr error
				var p_out *types.WorkflowPayload
				var nodeExecID string
				var startLogErr error
				inputAdditionalData := extractAdditionalLogData(pIn, "input", currentNodeDef.Id)
				nodeExecID, startLogErr = lc.StartNodeExecution(currentNodeDef, pIn, inputAdditionalData)
				if startLogErr != nil {
					baselog.Errorf("[WorkflowRun:%s][Node:%s] Wrapper: StartNodeExecution 失败: %v. 节点仍将尝试执行。", lc.WorkflowRunID, currentNodeDef.Id, startLogErr)
				}
				p_out, nodeInnerErr = nodeLogicFunc(lambdaCtx, pIn)
				elapsedTimeMs := time.Since(nodeStartTime).Milliseconds()
				outputAdditionalData := extractAdditionalLogData(p_out, "output", currentNodeDef.Id)
				if nodeExecID != "" || startLogErr != nil {
					if endLogErr := lc.EndNodeExecution(nodeExecID, currentNodeDef, p_out, nodeInnerErr, elapsedTimeMs, outputAdditionalData); endLogErr != nil {
						baselog.Errorf("[WorkflowRun:%s][Node:%s][ExecID:%s] Wrapper: EndNodeExecution 失败: %v.", lc.WorkflowRunID, currentNodeDef.Id, nodeExecID, endLogErr)
					}
				}
				return p_out, nodeInnerErr
			}
			einoComponentForGraph = compose.InvokableLambda(loggingWrappedLogic)
		} else {
			// 其他类型（如 Retriever/ChatModel），直接使用
			baselog.Debugf("节点 '%s' (类型: %s) 的 Build 方法未返回 NodeLogicFunc, 返回类型为 %T。将直接使用此组件。", nodeDef.Id, nodeDef.NodeType, rawComponent)
			einoComponentForGraph = rawComponent
		}

		nodeComponentMap[nodeDef.Id] = einoComponentForGraph
	}

	// 2. 将所有节点组件添加到图中
	for nodeId, component := range nodeComponentMap {
		switch c := component.(type) {
		case *compose.Lambda:
			if err := graph.AddLambdaNode(nodeId, c); err != nil {
				return nil, fmt.Errorf("添加 Lambda 节点 '%s' 到图失败: %w", nodeId, err)
			}
		case retriever.Retriever:
			if err := graph.AddRetrieverNode(nodeId, c); err != nil {
				return nil, fmt.Errorf("添加 Retriever 节点 '%s' 到图失败: %w", nodeId, err)
			}
		case model.BaseChatModel:
			if err := graph.AddChatModelNode(nodeId, c); err != nil {
				return nil, fmt.Errorf("添加 ChatModel 节点 '%s' 到图失败: %w", nodeId, err)
			}
		default:
			return nil, fmt.Errorf("节点 '%s' 构建了未知或不支持的组件类型: %T，无法添加到图", nodeId, c)
		}
	}

	// ... (CompileWorkflow 的其余部分：处理边、分支、START/END 节点连接、graph.Compile)
	edgesBySource := make(map[string][]*aiadmin.WorkflowEdge) //
	for i := range parsedEdges {                              //
		edge := parsedEdges[i]                                                            //
		edgesBySource[edge.SourceNodeId] = append(edgesBySource[edge.SourceNodeId], edge) //
	}

	for sourceID, outgoingEdges := range edgesBySource { //
		if _, ok := nodeComponentMap[sourceID]; !ok { // 检查 nodeComponentMap //
			if sourceID != compose.START { //
				return nil, fmt.Errorf("边的源节点 '%s' 未在节点列表中找到", sourceID) //
			}
		}
		if len(outgoingEdges) > 1 { // 这是一个分支点 //
			targets := make(map[string]string)         //
			var defaultTarget string                   //
			endNodesForBranch := make(map[string]bool) //

			for _, edge := range outgoingEdges { //
				if _, targetExists := nodeComponentMap[edge.TargetNodeId]; !targetExists && edge.TargetNodeId != compose.END { // 检查 nodeComponentMap //
					return nil, fmt.Errorf("分支的目标节点 '%s' (源自 '%s') 未在图中定义", edge.TargetNodeId, sourceID) //
				}
				endNodesForBranch[edge.TargetNodeId] = true                                          //
				isDefaultCondition := false                                                          //
				if edge.Condition == "" || edge.Condition == "default" || edge.Condition == "else" { //
					isDefaultCondition = true //
				}
				if isDefaultCondition { //
					if defaultTarget != "" && defaultTarget != edge.TargetNodeId { //
						return nil, fmt.Errorf("节点 '%s' 定义了多个不同的默认分支目标: '%s' 和 '%s'", sourceID, defaultTarget, edge.TargetNodeId) //
					}
					defaultTarget = edge.TargetNodeId //
				} else { //
					if _, exists := targets[edge.Condition]; exists { //
						return nil, fmt.Errorf("节点 '%s' 为条件 '%s' 定义了多个目标", sourceID, edge.Condition) //
					}
					targets[edge.Condition] = edge.TargetNodeId //
				}
			}
			if len(targets) == 0 && defaultTarget == "" && len(outgoingEdges) > 0 { //
				firstTarget := ""           //
				allSameTarget := true       //
				if len(outgoingEdges) > 0 { //
					firstTarget = outgoingEdges[0].TargetNodeId //
					for _, edge := range outgoingEdges {        //
						if edge.TargetNodeId != firstTarget { //
							allSameTarget = false //
							break                 //
						}
					}
				}
				if !allSameTarget { //
					return nil, fmt.Errorf("节点 '%s' 的分支逻辑不明确: 有多条出边但无有效条件或单一默认分支", sourceID) //
				}
			}
			conditionFunc := func(lambdaCtx context.Context, p *types.WorkflowPayload) (string, error) { // 注意这里改了 lambdaCtx //
				if p == nil || p.FlowControl == nil { //
					return "", fmt.Errorf("payload 或 FlowControl 为 nil") //
				}
				condVal, ok := p.FlowControl[types.FlowControlNextCondition] //
				if !ok {                                                     //
					return "", fmt.Errorf("FlowControl 中缺少 next_condition") //
				}
				cond, typeOk := condVal.(string) //
				if !typeOk {                     //
					return "", fmt.Errorf("next_condition 类型错误") //
				}
				if nextNodeID, found := targets[cond]; found { //
					return nextNodeID, nil //
				}
				if defaultTarget != "" { //
					return defaultTarget, nil //
				}
				return "", fmt.Errorf("节点 '%s' 的输出条件 '%s' 没有匹配的分支目标，且无默认分支", sourceID, cond) //
			}
			branch := compose.NewGraphBranch[*types.WorkflowPayload](conditionFunc, endNodesForBranch) //
			if err_b := graph.AddBranch(sourceID, branch); err_b != nil {                              //
				return nil, fmt.Errorf("为节点 '%s' 添加分支逻辑失败: %w", sourceID, err_b) //
			}
		} else if len(outgoingEdges) == 1 { //
			edge := outgoingEdges[0]                                                                                       //
			if _, targetExists := nodeComponentMap[edge.TargetNodeId]; !targetExists && edge.TargetNodeId != compose.END { // 检查 nodeComponentMap //
				return nil, fmt.Errorf("边的目标节点 '%s' (源自 '%s') 未在图中定义", edge.TargetNodeId, sourceID) //
			}
			if err_e := graph.AddEdge(sourceID, edge.TargetNodeId); err_e != nil { //
				return nil, fmt.Errorf("添加边从 '%s' 到 '%s' 失败: %w", sourceID, edge.TargetNodeId, err_e) //
			}
		}
	}

	// 连接叶子节点到 END
	for nodeID_k := range nodeComponentMap { // 遍历 nodeComponentMap //
		if _, hasOutgoing := edgesBySource[nodeID_k]; !hasOutgoing { //
			if nodeID_k != compose.END { //
				baselog.Debugf("调试: 节点 '%s' 没有出边，尝试连接到 END。", nodeID_k)          //
				if err_k := graph.AddEdge(nodeID_k, compose.END); err_k != nil { //
					baselog.Warnf("警告: 连接孤立节点 '%s' 到 END 时发生错误: %v", nodeID_k, err_k) //
				}
			}
		}
	}

	// 连接 START 到入口节点
	if entryNodeIdFromVersion == "" { //
		return nil, fmt.Errorf("工作流版本没有定义入口节点ID") //
	}
	if _, entryExists := nodeComponentMap[entryNodeIdFromVersion]; !entryExists { // 检查 nodeComponentMap //
		return nil, fmt.Errorf("工作流版本定义的入口节点 '%s' 未在节点列表中找到", entryNodeIdFromVersion) //
	}
	if err := graph.AddEdge(compose.START, entryNodeIdFromVersion); err != nil { //
		return nil, fmt.Errorf("连接 START 到入口节点 %s 失败: %w", entryNodeIdFromVersion, err) //
	}

	compiledRunnable, err := graph.Compile(stdCtx, compose.WithGraphName(workflowDef.Name)) //
	if err != nil {                                                                         //
		return nil, fmt.Errorf("最终编译工作流 '%s' (ID: %s) 版本 '%s' 失败: %w", workflowDef.Name, workflowDef.Id, activeVersion.Id, err) //
	}
	return compiledRunnable, nil //
}

// extractAdditionalLogData 函数不变
func extractAdditionalLogData(p *types.WorkflowPayload, stage, nodeId string) map[string]interface{} { //
	if p == nil || p.CustomData == nil { //
		return nil //
	}
	key := fmt.Sprintf("log_addon_%s_for_node_%s", stage, nodeId)   //
	if data, ok := p.CustomData[key].(map[string]interface{}); ok { //
		return data //
	}
	return nil //
}
