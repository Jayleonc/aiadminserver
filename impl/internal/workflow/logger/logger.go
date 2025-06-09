// file: impl/internal/workflow/logger/logger.go
package logger

import (
	"encoding/json"
	"fmt"
	"git.pinquest.cn/base/aiadmin"
	"git.pinquest.cn/base/aiadminserver/impl/global"
	"git.pinquest.cn/base/aiadminserver/impl/internal/model/modelworkflow"
	"git.pinquest.cn/base/aiadminserver/impl/internal/workflow/types"
	pkgutils "git.pinquest.cn/base/aiadminserver/impl/pkg/utils" // Renamed to avoid conflict
	baselog "git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/dbproxy"
	"git.pinquest.cn/qlb/brick/dbx"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/brick/utils/routine" // 引入 routine 包
	"strings"
	"time"
)

// LogContext 结构体不变
type LogContext struct {
	RpcCtx        *rpc.Context
	WorkflowRunID string
	ExecutionCtx  *types.WorkflowExecutionContext
	nodeExecIndex int32
}

// NewLogContext 构造函数不变
func NewLogContext(rpcCtx *rpc.Context, execCtx *types.WorkflowExecutionContext) *LogContext {
	return &LogContext{
		RpcCtx:        rpcCtx,
		WorkflowRunID: execCtx.RequestId,
		ExecutionCtx:  execCtx,
		nodeExecIndex: 0,
	}
}

// executeInTransaction 辅助方法不变
func (lc *LogContext) executeInTransaction(operationName string, fn func(txCtx *rpc.Context, tx *dbx.Tx) error) error {
	if lc.RpcCtx == nil {
		baselog.Errorf("LogContext.%s: RpcCtx is nil. Skipping DB operation.", operationName)
		return fmt.Errorf("RpcCtx is nil for %s", operationName)
	}

	scope := modelworkflow.ModelWorkflowRun.NewScope() // Or any other model, it's just for scope
	err := scope.OnTransaction(lc.RpcCtx, func(txCtx *rpc.Context, tx *dbx.Tx) error {
		return fn(txCtx, tx)
	}, dbproxy.OptionWithUseDb(global.GetDBConfigName()), dbproxy.OptionWithUseBiz(true)) // [cite: 69]

	if err != nil {
		baselog.Errorf("LogContext.%s: Transaction execution failed: %v", operationName, err)
	}
	return err
}

// StartWorkflowRun 异步记录工作流开始
func (lc *LogContext) StartWorkflowRun(workflowDef *aiadmin.ModelWorkflow, activeVersion *aiadmin.ModelWorkflowVersion, task *aiadmin.AIWorkflowMsqTask) error {
	operation := "StartWorkflowRunAsync" // 标记为异步

	// 复制需要在 goroutine 中使用的数据，避免闭包问题和并发修改问题
	// 对于指针类型，如果其指向的内容在 goroutine 执行前可能被修改，则需要深拷贝。
	// workflowDef, activeVersion, task 通常在 goroutine 执行时不会被修改，可以安全传递指针。
	// lc.WorkflowRunID 和 lc.ExecutionCtx 也是如此。

	// 确保 lc.RpcCtx 存在
	if lc.RpcCtx == nil {
		baselog.Errorf("LogContext.%s: RpcCtx is nil. Skipping async DB operation.", operation)
		return fmt.Errorf("RpcCtx is nil for %s", operation)
	}

	// 【重要】克隆 lc.RpcCtx 以传递给 routine.Go
	// routine.Go 内部会再次克隆，但我们在这里克隆是为了确保 lc.RpcCtx 本身在主线程中可以继续被安全使用，
	// 不受异步routine中可能对context的（不期望的）修改影响。
	// routine.Go 中的 cloneCtx 是为了创建一个隔离的、适合goroutine的上下文。
	// 如果 routine.Go 的克隆足够健壮，这里的克隆可以省略，直接传递 lc.RpcCtx。
	// 为安全起见，或如果 lc.RpcCtx 后续还有复杂操作，这里克隆一份是好的。
	// 但鉴于 routine.Go 已经 clone，这里可以直接传递 lc.RpcCtx。
	// ctxForRoutine := rpc.CloneContext(lc.RpcCtx)
	// if ctxForRoutine == nil {
	//    ctxForRoutine = new(rpc.Context) // Fallback, though CloneContext should handle this
	//    core.CopyContext(lc.RpcCtx, ctxForRoutine) // 复制必要头部
	// }
	// core.SetCmdPath(ctxForRoutine, lc.RpcCtx.GetCmdPath()+"."+operation) // 设置新的 CmdPath

	inputsData, _ := json.Marshal(map[string]interface{}{
		"keyword":              task.Keyword,            // [cite: 49]
		"recent_history_count": len(task.RecentHistory), // [cite: 49]
	})
	run := aiadmin.ModelWorkflowRun{ // [cite: 50]
		Id:                lc.WorkflowRunID,                                              // [cite: 50]
		WorkflowId:        workflowDef.Id,                                                // [cite: 50]
		WorkflowVersionId: activeVersion.Id,                                              // [cite: 50]
		CorpId:            lc.ExecutionCtx.CorpId,                                        // [cite: 50]
		AppId:             lc.ExecutionCtx.AppId,                                         // [cite: 50]
		RobotUid:          lc.ExecutionCtx.RobotUid,                                      // [cite: 50]
		ChatId:            lc.ExecutionCtx.ChatId,                                        // [cite: 50]
		TriggeredFrom:     "mq_task",                                                     // [cite: 50]
		Inputs:            string(inputsData),                                            // [cite: 50]
		Status:            uint32(aiadmin.WorkflowRunStatus_WORKFLOW_RUN_STATUS_RUNNING), // [cite: 50]
		CreatedByRole:     string(aiadmin.RoleTypeSystem),                                // [cite: 50]
		CreatedBy:         "system_mq",                                                   // [cite: 50]
		StartedAt:         time.Now().UnixMilli(),                                        // [cite: 50]
		CreatedAtTs:       uint32(time.Now().Unix()),                                     // [cite: 50]
		UpdatedAtTs:       uint32(time.Now().Unix()),                                     // [cite: 50]
	}

	routine.Go(lc.RpcCtx, func(asyncRpcCtx *rpc.Context) error { // 使用 lc.RpcCtx, routine.Go 内部会克隆
		// asyncRpcCtx 是 routine.Go 克隆和准备好的 context
		// 注意：LogContext 自身的 lc.executeInTransaction 接收的是 LogContext 的 RpcCtx
		// 为了在异步任务中使用正确的、新克隆的 asyncRpcCtx 进行DB操作，
		// 我们需要一种方式让 executeInTransaction 使用这个 asyncRpcCtx。
		// 方案1: 修改 executeInTransaction 接收 rpc.Context 参数。
		// 方案2: 创建一个新的临时 LogContext 实例，用 asyncRpcCtx 初始化它。
		// 这里采用方案2的思路，但不直接创建新 LogContext，而是直接调用 scope.OnTransaction

		tempLc := &LogContext{ // 创建一个临时的 LogContext 仅用于当前 goroutine 的事务
			RpcCtx:        asyncRpcCtx,     // 【关键】使用 routine.Go 提供的克隆上下文
			WorkflowRunID: run.Id,          // 从捕获的 run 变量中获取
			ExecutionCtx:  lc.ExecutionCtx, // 可以安全引用外部 lc 的 ExecutionCtx
		}

		err := tempLc.executeInTransaction(operation, func(txCtx *rpc.Context, tx *dbx.Tx) error {
			return modelworkflow.ModelWorkflowRun.CreateInTx(txCtx, tx, &run) // [cite: 50]
		})
		// routine.Go 会处理 err，如果非nil会打印错误日志
		if err != nil {
			baselog.Errorf("LogContext.%s (async): Failed for WorkflowRunID %s: %v", operation, run.Id, err)
		} else {
			baselog.Infof("[WorkflowRun:%s] (async) Started and logged to DB.", run.Id)
		}
		return err
	})
	return nil
}

// EndWorkflowRun 异步记录工作流结束
func (lc *LogContext) EndWorkflowRun(finalPayload *types.WorkflowPayload, runErr error) error {
	operation := "EndWorkflowRunAsync"
	if lc.RpcCtx == nil {
		baselog.Errorf("LogContext.%s: RpcCtx is nil. Skipping async DB operation.", operation)
		return fmt.Errorf("RpcCtx is nil for %s", operation)
	}

	// 复制需要传递给 goroutine 的数据
	currentWorkflowRunID := lc.WorkflowRunID
	currentNodeExecIndex := lc.nodeExecIndex
	// finalPayload 和 runErr 也需要注意并发安全
	// 如果 finalPayload 是指针，且可能在 goroutine 执行前被修改，需要深拷贝或只传递必要的值
	// 这里我们假设 finalPayload 在调用此方法后不会被外部修改，或者我们拷贝出需要的值

	var outputsDataStr string
	var errorMsgStr string
	status := aiadmin.WorkflowRunStatus_WORKFLOW_RUN_STATUS_SUCCEEDED // [cite: 16]

	if runErr != nil {
		status = aiadmin.WorkflowRunStatus_WORKFLOW_RUN_STATUS_FAILED // [cite: 16]
		errorMsgStr = runErr.Error()
	} else if finalPayload != nil && finalPayload.Error != nil {
		status = aiadmin.WorkflowRunStatus_WORKFLOW_RUN_STATUS_FAILED // [cite: 16, 53]
		errorMsgStr = finalPayload.Error.Error()                      // [cite: 53]
	}

	if finalPayload != nil {
		outputsBytes, _ := json.Marshal(getLoggablePayloadData(finalPayload, "output")) // [cite: 53]
		outputsDataStr = string(outputsBytes)                                           // [cite: 53]
	}

	// StartedAt 需要从数据库读取，这使得异步稍微复杂一点。
	// 简单起见，如果性能允许，我们可以在异步任务开始时再次查询。
	// 或者，如果 StartWorkflowRun 记录了 StartedAt，我们就不在这里计算 elapsedTime，
	// 而是让DB层面通过 StartedAt 和 FinishedAt (在本次更新时设置) 来计算，或者有一个单独的更新任务。
	// 这里我们选择在异步任务中读取 StartedAt。

	routine.Go(lc.RpcCtx, func(asyncRpcCtx *rpc.Context) error {
		tempLc := &LogContext{RpcCtx: asyncRpcCtx, WorkflowRunID: currentWorkflowRunID, ExecutionCtx: lc.ExecutionCtx}

		err := tempLc.executeInTransaction(operation, func(txCtx *rpc.Context, tx *dbx.Tx) error {
			runRecord, getErr := modelworkflow.ModelWorkflowRun.GetByIdInTx(txCtx, tx, currentWorkflowRunID) // [cite: 54]
			if getErr != nil {
				baselog.Errorf("[WorkflowRun:%s] %s (async): Failed to find record for elapsed time calculation: %v", currentWorkflowRunID, operation, getErr)
				return getErr
			}
			elapsedTime := time.Now().UnixMilli() - runRecord.StartedAt // [cite: 54]

			updates := map[string]interface{}{
				"status":          uint32(status),            // [cite: 55]
				"outputs":         outputsDataStr,            // [cite: 55]
				"error":           errorMsgStr,               // [cite: 55]
				"elapsed_time_ms": elapsedTime,               // [cite: 55]
				"finished_at":     time.Now().UnixMilli(),    // [cite: 55]
				"total_steps":     currentNodeExecIndex,      // [cite: 55]
				"updated_at_ts":   uint32(time.Now().Unix()), // [cite: 55]
			}
			return modelworkflow.ModelWorkflowRun.UpdateInTx(txCtx, tx, currentWorkflowRunID, updates) // [cite: 56]
		})

		if err != nil {
			baselog.Errorf("LogContext.%s (async): Failed for WorkflowRunID %s: %v", operation, currentWorkflowRunID, err)
		} else {
			baselog.Infof("[WorkflowRun:%s] (async) Ended and logged to DB. Status: %s", currentWorkflowRunID, status)
		}
		return err
	})
	return nil
}

// StartNodeExecution 异步记录节点开始
func (lc *LogContext) StartNodeExecution(
	nodeDef *aiadmin.WorkflowNodeDef,
	payloadIn *types.WorkflowPayload,
	additionalData ...map[string]interface{},
) (string, error) { // 返回的 nodeExecID 仍然同步生成，因为后续 EndNodeExecution 需要它

	lc.nodeExecIndex++
	nodeExecID := pkgutils.NewIdWithPrefix("wfn") // [cite: 57] 同步生成ID
	operation := fmt.Sprintf("StartNodeExecutionAsync(Node:%s, Exec:%s)", nodeDef.Id, nodeExecID)

	if lc.RpcCtx == nil {
		baselog.Errorf("LogContext.%s: RpcCtx is nil. Skipping async DB operation.", operation)
		// 即使异步，nodeExecID 也应该返回，即使日志可能失败
		return nodeExecID, fmt.Errorf("RpcCtx is nil for %s", operation)
	}

	// 复制数据
	currentWorkflowRunID := lc.WorkflowRunID
	currentNodeIndex := lc.nodeExecIndex
	// nodeDef 通常是安全的，payloadIn 也假设在调用后不会被外部修改关键部分，或只传递需要的值
	inputsDataStr, _ := json.Marshal(getLoggablePayloadData(payloadIn, "input")) // [cite: 57]

	var execMetaJSONStr string
	if len(additionalData) > 0 && additionalData[0] != nil {
		metaBytes, errMeta := json.Marshal(additionalData[0])
		if errMeta == nil {
			execMetaJSONStr = string(metaBytes)
		} else {
			baselog.Warnf("[WorkflowRun:%s][Node:%s] %s: Failed to marshal additionalData: %v", currentWorkflowRunID, nodeDef.Id, operation, errMeta)
		}
	}
	// 复制 nodeDef 的关键信息，而不是整个指针，以防 nodeDef 在外部被意外修改（虽然可能性小）
	clonedNodeDef := &aiadmin.WorkflowNodeDef{
		Id:       nodeDef.Id,
		NodeType: nodeDef.NodeType,
		NodeName: nodeDef.NodeName,
		//NodeConfigJson: nodeDef.NodeConfigJson, // 假设这个json内容不含指针且相对稳定
	}

	nodeExec := aiadmin.ModelWorkflowNodeExecution{ // [cite: 58]
		Id:                nodeExecID,
		WorkflowRunId:     currentWorkflowRunID,
		NodeId:            clonedNodeDef.Id,
		NodeType:          clonedNodeDef.NodeType,
		NodeName:          clonedNodeDef.NodeName,
		Index:             currentNodeIndex,
		Inputs:            string(inputsDataStr),
		Status:            uint32(aiadmin.WorkflowNodeExecutionStatus_WORKFLOW_NODE_EXECUTION_STATUS_RUNNING),
		StartedAt:         time.Now().UnixMilli(),
		ExecutionMetadata: execMetaJSONStr,           // [cite: 32]
		CreatedAtTs:       uint32(time.Now().Unix()), // [cite: 58]
		UpdatedAtTs:       uint32(time.Now().Unix()), // [cite: 58]
	}

	routine.Go(lc.RpcCtx, func(asyncRpcCtx *rpc.Context) error {
		tempLc := &LogContext{RpcCtx: asyncRpcCtx, WorkflowRunID: currentWorkflowRunID, ExecutionCtx: lc.ExecutionCtx}
		err := tempLc.executeInTransaction(operation, func(txCtx *rpc.Context, tx *dbx.Tx) error {
			return modelworkflow.ModelWorkflowNodeExecution.CreateInTx(txCtx, tx, &nodeExec) // [cite: 58]
		})
		if err != nil {
			baselog.Errorf("LogContext.%s (async): Failed: %v. Data: %+v", operation, err, nodeExec)
		} else {
			baselog.Debugf("[WorkflowRun:%s][Node:%s][Exec:%s] (async) Started and logged to DB. Index: %d", currentWorkflowRunID, clonedNodeDef.Id, nodeExecID, currentNodeIndex)
		}
		return err
	})
	return nodeExecID, nil // 同步返回ID，错误表示发起异步任务前的错误
}

// EndNodeExecution 异步记录节点结束
func (lc *LogContext) EndNodeExecution(
	nodeExecID string,
	nodeDef *aiadmin.WorkflowNodeDef,
	payloadOut *types.WorkflowPayload,
	nodeErr error,
	elapsedTimeMs int64,
	additionalData ...map[string]interface{},
) error {
	operation := fmt.Sprintf("EndNodeExecutionAsync(Node:%s, Exec:%s)", nodeDef.Id, nodeExecID)
	if nodeExecID == "" { // [cite: 62]
		baselog.Errorf("LogContext.%s: nodeExecID is empty. Cannot update record.", operation)
		return fmt.Errorf("nodeExecID is empty for %s", operation)
	}
	if lc.RpcCtx == nil {
		baselog.Errorf("LogContext.%s: RpcCtx is nil. Skipping async DB operation.", operation)
		return fmt.Errorf("RpcCtx is nil for %s", operation)
	}

	// 复制数据
	currentWorkflowRunID := lc.WorkflowRunID // 主要用于日志
	// nodeDef 和 payloadOut 的处理同 StartNodeExecution

	status := aiadmin.WorkflowNodeExecutionStatus_WORKFLOW_NODE_EXECUTION_STATUS_SUCCEEDED // [cite: 60]
	var errorMsgStr string
	var outputsDataStr string

	actualError := nodeErr
	if payloadOut != nil && payloadOut.Error != nil { // [cite: 61]
		actualError = payloadOut.Error // [cite: 61]
	}
	if actualError != nil {
		status = aiadmin.WorkflowNodeExecutionStatus_WORKFLOW_NODE_EXECUTION_STATUS_FAILED // [cite: 61]
		errorMsgStr = actualError.Error()                                                  // [cite: 61]
	}
	if payloadOut != nil {
		outputsBytes, _ := json.Marshal(getLoggablePayloadData(payloadOut, "output")) // [cite: 61]
		outputsDataStr = string(outputsBytes)                                         // [cite: 61]
	}

	var execMetaJSONStr string
	if len(additionalData) > 0 && additionalData[0] != nil {
		metaBytes, errMeta := json.Marshal(additionalData[0])
		if errMeta == nil {
			execMetaJSONStr = string(metaBytes)
		} else {
			baselog.Warnf("[WorkflowRun:%s][Node:%s][ExecID:%s] %s: Failed to marshal additionalData: %v", currentWorkflowRunID, nodeDef.Id, nodeExecID, operation, errMeta)
		}
	}
	// 这里不需要克隆 nodeDef，因为只用到了它的 ID 来构造 operation 字符串，并且它不会被传入 goroutine
	// elapsed_time_ms 是值类型，直接传递

	routine.Go(lc.RpcCtx, func(asyncRpcCtx *rpc.Context) error {
		updates := map[string]interface{}{ // [cite: 61]
			"status":          uint32(status),            // [cite: 61]
			"outputs":         outputsDataStr,            // [cite: 61]
			"error":           errorMsgStr,               // [cite: 61]
			"elapsed_time_ms": elapsedTimeMs,             // [cite: 61]
			"finished_at":     time.Now().UnixMilli(),    // [cite: 61]
			"updated_at_ts":   uint32(time.Now().Unix()), // [cite: 61]
		}
		if execMetaJSONStr != "" { // 只有当成功序列化时才添加
			updates["execution_metadata"] = execMetaJSONStr // [cite: 32]
		}

		tempLc := &LogContext{RpcCtx: asyncRpcCtx, WorkflowRunID: currentWorkflowRunID, ExecutionCtx: lc.ExecutionCtx}
		err := tempLc.executeInTransaction(operation, func(txCtx *rpc.Context, tx *dbx.Tx) error {
			return modelworkflow.ModelWorkflowNodeExecution.UpdateInTx(txCtx, tx, nodeExecID, updates) // [cite: 62]
		})

		if err != nil {
			baselog.Errorf("LogContext.%s (async): Failed: %v. Updates: %+v", operation, err, updates)
		} else {
			baselog.Debugf("[WorkflowRun:%s][Node:%s][Exec:%s] (async) Ended and logged to DB. Status: %s, Elapsed: %dms", currentWorkflowRunID, nodeDef.Id, nodeExecID, status, elapsedTimeMs)
		}
		return err
	})
	return nil
}

// getLoggablePayloadData 和 truncateString 辅助函数不变
func getLoggablePayloadData(p *types.WorkflowPayload, stage string) map[string]interface{} { // [cite: 63]
	if p == nil { // [cite: 63]
		return map[string]interface{}{"payload_status": "nil_at_" + stage} // [cite: 63]
	}

	data := map[string]interface{}{} // [cite: 63]

	// 通用基础信息，总是记录
	data["current_user_query"] = p.CurrentUserQuery // [cite: 64]
	if p.InitialQuery != nil {                      // [cite: 64]
		data["initial_query_content"] = p.InitialQuery.Content // [cite: 64]
	}
	if len(p.RecentHistory) > 0 { // [cite: 64]
		data["recent_history_count"] = len(p.RecentHistory) // [cite: 64]
		// 只记录最近3条历史 (如果需要更少或更多，调整这里的数字)
		if len(p.RecentHistory) > 0 {
			maxHistLog := 3
			startIdx := len(p.RecentHistory) - maxHistLog
			if startIdx < 0 {
				startIdx = 0
			}
			var loggedHistory []map[string]string
			for _, histMsg := range p.RecentHistory[startIdx:] {
				loggedHistory = append(loggedHistory, map[string]string{
					"role":    histMsg.Role,
					"content": truncateString(histMsg.Content, 150),
				})
			}
			data["recent_history_preview"] = loggedHistory
		}
	}

	// 根据阶段选择性记录，减少冗余
	if stage == "input" {
		data["classification_in"] = p.Classification
		data["draft_answer_in"] = truncateString(p.DraftAnswer, 100)
		data["final_answer_in"] = truncateString(p.FinalAnswer, 200)
		data["retrieved_context_in_length"] = len(p.RetrievedContext) // [cite: 64]
		if len(p.RetrievedContext) > 0 {                              // [cite: 65]
			data["retrieved_context_in_snippet"] = truncateString(p.RetrievedContext, 600) // [cite: 65]
		} else { // [cite: 65]
			data["retrieved_context_in_snippet"] = ""
		}
		if p.FlowControl != nil { // [cite: 66]
			data["flow_control_in"] = p.FlowControl // [cite: 66]
		}
	} else if stage == "output" || strings.HasPrefix(stage, "output_") {
		data["classification_out"] = p.Classification                 // [cite: 65]
		data["draft_answer_out"] = truncateString(p.DraftAnswer, 200) // [cite: 66]
		data["final_answer_out"] = truncateString(p.FinalAnswer, 200) // [cite: 66]
		data["retrieved_context_out_length"] = len(p.RetrievedContext)
		if len(p.RetrievedContext) > 0 {
			contextSnippetKey := "retrieved_context_out_snippet"
			data[contextSnippetKey] = truncateString(p.RetrievedContext, 600)
		} else {
			data["retrieved_context_out_snippet"] = ""
		}
		if p.FlowControl != nil { // [cite: 66]
			data["flow_control_out"] = p.FlowControl // [cite: 66]
		}
		data["message_sent_by_node_out"] = p.MessageSentByNode // [cite: 68]
		if len(p.FinalExtra) > 0 {                             // [cite: 67]
			data["final_extra_out_count"] = len(p.FinalExtra) // [cite: 67]
		}
	}

	if p.Error != nil { // [cite: 66]
		data["error"] = p.Error.Error() // [cite: 67]
	}

	return data // [cite: 68]
}

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
