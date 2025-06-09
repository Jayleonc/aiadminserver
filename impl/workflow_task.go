// file: impl/workflow_task.go
package impl

import (
	"context"
	"fmt"
	"git.pinquest.cn/base/aiadminserver/impl/internal/workflow/types"

	"git.pinquest.cn/base/aiadmin"
	"git.pinquest.cn/base/aiadminserver/impl/internal/model/modelworkflow"
	wf "git.pinquest.cn/base/aiadminserver/impl/internal/workflow"
	"git.pinquest.cn/base/aiadminserver/impl/internal/workflow/logger" // 引入 logger
	baselog "git.pinquest.cn/base/log"                                 // 项目标准日志
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/brick/smq"
	"git.pinquest.cn/qlb/core"
	"github.com/cloudwego/eino/schema"
)

// HandleAIWorkflowTask 调用 executeWorkflow
func HandleAIWorkflowTask(rpcCtx *rpc.Context, req *smq.ConsumeReq) (*smq.ConsumeRsp, error) {
	baselog.Infof("HandleAIWorkflowTask (gRPC/RPC) 接收到原始MQ请求, MsgId: %s, CorpId: %d", req.MsgId, req.CorpId)

	return MqAIWorkflowTrigger.ProcessV2(rpcCtx, req, func(innerRpcCtx *rpc.Context, consumerReq *smq.ConsumeReq, data interface{}) (*smq.ConsumeRsp, error) {
		baselog.Infof("HandleAIWorkflowTask Worker (ProcessV2) 进来了")
		var rsp smq.ConsumeRsp
		task, ok := data.(*aiadmin.AIWorkflowMsqTask)
		if !ok {
			baselog.Errorf("消息体类型断言失败，期望 *aiadmin.AIWorkflowMsqTask，实际为 %T", data)
			return &rsp, nil
		}

		baselog.Infof("HandleAIWorkflowTask Worker开始处理AI任务, WorkflowId: %s, CorpId: %d, Keyword: '%s', CliMsgId: %s",
			task.WorkflowId, task.GetCorpId(), task.GetKeyword(), task.GetCliMsgId())

		// 【修改】直接调用 executeWorkflow，不再有 internal 版本
		err := executeWorkflow(context.Background(), innerRpcCtx, task)
		if err != nil {
			baselog.Errorf("执行工作流失败 (WorkflowId: %s, CliMsgId: %s): %v", task.WorkflowId, task.GetCliMsgId(), err)
			rsp.Retry = true // 根据错误类型决定是否重试
			return &rsp, err
		}

		baselog.Infof("HandleAIWorkflowTask Worker成功处理AI任务, WorkflowId: %s, CliMsgId: %s", task.WorkflowId, task.GetCliMsgId())
		return &rsp, nil
	})
}

// executeWorkflow 接收标准 context.Context, rpc.Context (来自MQ消费者) 和从MQ解码后的task
func executeWorkflow(stdCtx context.Context, currentRpcCtx *rpc.Context, task *aiadmin.AIWorkflowMsqTask) error {
	baselog.Infof("----->executeWorkflow 开始 (WorkflowID: %s, RequestID: %s) <-----", task.WorkflowId, currentRpcCtx.GetReqId())
	baselog.Debugf("     executeWorkflow  任务详情: %+v", task)

	// 1. 加载元数据 (workflowDef, activeVersion)
	if task.GetCorpId() != 0 {
		core.SetCorpAndApp(currentRpcCtx, task.GetCorpId(), task.GetAppId())
	}
	if task.GetRobotUid() != 0 {
		core.SetPinHeaderUId(currentRpcCtx, task.GetRobotUid())
	}

	workflowDef, err := modelworkflow.ModelWorkflow.GetById(currentRpcCtx, task.WorkflowId)
	if err != nil {
		baselog.Errorf("executeWorkflow: 获取工作流定义失败 (ID: %s): %v", task.WorkflowId, err)
		return fmt.Errorf("获取工作流定义失败: %w", err)
	}
	if workflowDef == nil {
		baselog.Errorf("executeWorkflow: 未找到工作流定义 (ID: %s)", task.WorkflowId)
		return fmt.Errorf("未找到工作流定义 ID: %s", task.WorkflowId)
	}
	if workflowDef.Status != uint32(aiadmin.WorkflowStatus_WorkflowStatusEnabled) {
		baselog.Warnf("executeWorkflow: 工作流 '%s' (ID: %s) 未启用，跳过执行。", workflowDef.Name, workflowDef.Id)
		return nil // 不是错误，只是不执行
	}
	if workflowDef.ActiveVersionId == "" {
		baselog.Errorf("executeWorkflow: 工作流 '%s' (ID: %s) 没有活动的版本，无法执行", workflowDef.Name, workflowDef.Id)
		return fmt.Errorf("工作流 '%s' 没有活动的版本", workflowDef.Id)
	}
	activeVersion, err := modelworkflow.ModelWorkflowVersion.GetById(currentRpcCtx, workflowDef.ActiveVersionId)
	if err != nil {
		baselog.Errorf("executeWorkflow: 获取工作流 %s 的活跃版本 %s 失败: %v", task.WorkflowId, workflowDef.ActiveVersionId, err)
		return fmt.Errorf("获取工作流活跃版本失败: %w", err)
	}
	if activeVersion == nil {
		baselog.Errorf("executeWorkflow: 未找到工作流 %s 的活跃版本 %s", task.WorkflowId, workflowDef.ActiveVersionId)
		return fmt.Errorf("活跃的工作流版本 %s 未找到", workflowDef.ActiveVersionId)
	}
	baselog.Infof("executeWorkflow: 工作流 '%s' (ID: %s), 版本 '%s' (VersionID: %s) 定义已加载",
		workflowDef.Name, workflowDef.Id, activeVersion.VersionName, activeVersion.Id)

	// 2. 创建 WorkflowExecutionContext 和 LogContext
	wfExecCtx := &types.WorkflowExecutionContext{
		InitialTask:   task,
		RunMode:       types.RunModeProduction, // MQ 任务总是生产模式
		RequestId:     currentRpcCtx.GetReqId(),
		CorpId:        task.GetCorpId(),
		AppId:         task.GetAppId(),
		RobotUid:      task.GetRobotUid(),
		ChatId:        task.GetChatId(),
		ChatType:      task.GetChatType(),
		RecentHistory: task.GetRecentHistory(),
	}
	// 【修改】NewLogContext 不再接收 Tx
	logCtx := logger.NewLogContext(currentRpcCtx, wfExecCtx)

	// 3. 日志：工作流开始（内部会处理自己的短事务）
	var overallWorkflowError error // 用于捕获流程中的第一个重要错误

	if errStart := logCtx.StartWorkflowRun(workflowDef, activeVersion, task); errStart != nil {
		overallWorkflowError = fmt.Errorf("记录工作流开始日志失败: %w", errStart)
		baselog.Errorf("executeWorkflow: StartWorkflowRun failed for RequestId %s: %v", wfExecCtx.RequestId, overallWorkflowError)
		// 根据策略，如果开始日志都失败了，可能直接返回错误，不继续执行
		// return overallWorkflowError
	}

	// 使用 defer 确保 EndWorkflowRun 总能被调用以记录最终状态
	var finalPayloadFromInvoke *types.WorkflowPayload
	defer func() {
		// overallWorkflowError 包含了编译或执行期间的第一个错误
		// finalPayloadFromInvoke 是 Invoke 的结果
		// 注意：如果 StartWorkflowRun 成功，但 EndWorkflowRun 失败，这个错误会被记录到应用日志，但可能不会覆盖 overallWorkflowError
		if errEnd := logCtx.EndWorkflowRun(finalPayloadFromInvoke, overallWorkflowError); errEnd != nil {
			baselog.Errorf("executeWorkflow: EndWorkflowRun (deferred) failed for RequestId %s: %v. Original error: %v",
				wfExecCtx.RequestId, errEnd, overallWorkflowError)
			if overallWorkflowError == nil { // 如果主流程没问题，但结束日志记录失败了
				overallWorkflowError = fmt.Errorf("记录工作流结束日志失败: %w", errEnd)
			}
		}
	}()

	// 4. 编译工作流，传入 LogContext
	runnable, compileErr := wf.CompileWorkflow(workflowDef, activeVersion, stdCtx, logCtx)
	if compileErr != nil {
		overallWorkflowError = fmt.Errorf("工作流编译失败: %w", compileErr)
		baselog.Errorf("executeWorkflow: 工作流 '%s' 编译失败: %v", workflowDef.Name, overallWorkflowError)
		// defer 中的 EndWorkflowRun 会记录此错误
		return overallWorkflowError
	}
	baselog.Infof("executeWorkflow: 工作流编译成功 for WorkflowId: %s, VersionID: %s", task.WorkflowId, activeVersion.Id)

	// 5. 构造输入并执行
	userInput := schema.Message{Role: schema.User, Content: task.GetKeyword()}
	userInput.Extra = map[string]interface{}{
		types.WorkflowExecutionCtxKey: wfExecCtx,
	}
	baselog.Debugf("executeWorkflow: 工作流输入已构造 for WorkflowId: %s, Input Extra Key: %s", task.WorkflowId, types.WorkflowExecutionCtxKey)

	baselog.Infof("executeWorkflow: 即将执行 Invoke for WorkflowId: %s, VersionID: %s", task.WorkflowId, activeVersion.Id)

	// finalPayloadFromInvoke 和 invokeErr 已经在 defer 作用域之外声明
	var invokeErr error
	finalPayloadFromInvoke, invokeErr = runnable.Invoke(stdCtx, &userInput)

	// 6. 处理执行结果和错误
	if invokeErr != nil {
		overallWorkflowError = fmt.Errorf("工作流执行 Invoke 失败: %w", invokeErr)
		baselog.Errorf("executeWorkflow: 工作流 Invoke 执行失败 for RequestId %s: %v", wfExecCtx.RequestId, overallWorkflowError)
		// defer 中的 EndWorkflowRun 会记录此错误
		return overallWorkflowError
	}
	if finalPayloadFromInvoke == nil {
		baselog.Warnf("executeWorkflow: 工作流 Invoke 执行成功但返回的载荷 (WorkflowPayload) 为 nil for RequestId %s", wfExecCtx.RequestId)
		// 这种情况可能也算一种错误或异常，取决于业务逻辑。
		// overallWorkflowError 可以不设置，让 EndWorkflowRun 记录空输出。
		// 或者设置一个特定的错误：
		// overallWorkflowError = fmt.Errorf("工作流执行成功但返回空载荷")
		// defer 中的 EndWorkflowRun 会记录
		return nil // 或者返回一个代表“无结果”的特定错误
	}
	baselog.Infof("executeWorkflow: 工作流 Invoke 执行成功 for RequestId %s. FinalAnswer: '%s', MessageSentByNode: %t",
		wfExecCtx.RequestId, finalPayloadFromInvoke.FinalAnswer, finalPayloadFromInvoke.MessageSentByNode)

	if finalPayloadFromInvoke.Error != nil {
		overallWorkflowError = fmt.Errorf("工作流内部错误: %w", finalPayloadFromInvoke.Error)
		baselog.Errorf("executeWorkflow: 工作流执行过程中内部发生错误 for RequestId %s: %v", wfExecCtx.RequestId, overallWorkflowError)
		// defer 中的 EndWorkflowRun 会记录此错误
		return overallWorkflowError
	}

	baselog.Infof("----->executeWorkflow 工作流执行逻辑完成 (WorkflowID: %s, VersionID: %s, RequestID: %s) <-----",
		task.WorkflowId, activeVersion.Id, wfExecCtx.RequestId)
	return nil // overallWorkflowError 此时为 nil
}
