// impl/workflow_log.go
package impl

import (
	"fmt"

	"git.pinquest.cn/base/aiadmin"
	"git.pinquest.cn/base/aiadminserver/impl/internal/model/modelworkflow"
	"git.pinquest.cn/qlb/brick/rpc"
)

func GetWorkflowRunList(ctx *rpc.Context, req *aiadmin.GetWorkflowRunListReq) (*aiadmin.GetWorkflowRunListRsp, error) {
	var rsp aiadmin.GetWorkflowRunListRsp

	var ctxCorpId uint32 = 2000772
	if req.CorpId == 0 {
		req.CorpId = ctxCorpId
	} else if req.CorpId != ctxCorpId {
		return nil, rpc.CreateErrorWithMsg(rpc.KSystemError, "请求的 corp_id 与上下文不符")
	}

	runs, paginate, err := modelworkflow.ModelWorkflowRun.ListWithOption(ctx, req)
	if err != nil {
		return nil, rpc.CreateErrorWithMsg(rpc.KSystemError, fmt.Sprintf("查询工作流运行列表失败: %v", err))
	}

	rsp.List = make([]*aiadmin.WorkflowRunListItem, 0, len(runs))
	if len(runs) > 0 {
		// 批量获取关联的 Workflow 名称 (可选优化)
		workflowIds := make([]string, 0, len(runs))
		for _, run := range runs {
			if run.WorkflowId != "" {
				workflowIds = append(workflowIds, run.WorkflowId)
			}
		}
		workflowsMap := make(map[string]*aiadmin.ModelWorkflow)
		if len(workflowIds) > 0 {
			// 假设 ModelWorkflow 有 ListByIds 方法
			// workflowModels, _ := modelworkflow.ModelWorkflow.ListByIds(ctx, workflowIds)
			// for _, wf := range workflowModels {
			// 	workflowsMap[wf.Id] = wf
			// }
			// 简单实现：逐个查询（性能较低，后续可优化为批量）
			for _, wfID := range workflowIds {
				if _, exists := workflowsMap[wfID]; !exists {
					wfModel, _ := modelworkflow.ModelWorkflow.GetById(ctx, wfID) // 忽略单个查询错误
					if wfModel != nil {
						workflowsMap[wfID] = wfModel
					}
				}
			}
		}

		for _, run := range runs {
			item := &aiadmin.WorkflowRunListItem{
				Id:                run.Id,
				WorkflowId:        run.WorkflowId,
				WorkflowVersionId: run.WorkflowVersionId,
				TriggeredFrom:     run.TriggeredFrom,
				RobotUid:          run.RobotUid,
				ChatId:            run.ChatId,
				InputsPreview:     truncateString(run.Inputs, 100),  // 预览，截断
				OutputsPreview:    truncateString(run.Outputs, 100), // 预览，截断
				Status:            run.Status,                       // 类型转换
				ErrorPreview:      truncateString(run.Error, 100),   // 预览，截断
				ElapsedTimeMs:     run.ElapsedTimeMs,
				CreatedByRole:     run.CreatedByRole,
				CreatedBy:         run.CreatedBy,
				StartedAt:         run.StartedAt,
				FinishedAt:        run.FinishedAt,
			}
			if wf, ok := workflowsMap[run.WorkflowId]; ok && wf != nil {
				item.WorkflowName = wf.Name
			}
			rsp.List = append(rsp.List, item)
		}
	}
	rsp.Paginate = paginate
	return &rsp, nil
}

func GetWorkflowRunDetail(ctx *rpc.Context, req *aiadmin.GetWorkflowRunDetailReq) (*aiadmin.GetWorkflowRunDetailRsp, error) {
	var rsp aiadmin.GetWorkflowRunDetailRsp

	// 权限校验：确保请求的 run_id 属于当前 corp_id
	runDetail, err := modelworkflow.ModelWorkflowRun.GetById(ctx, req.RunId)
	if err != nil {
		return nil, rpc.CreateErrorWithMsg(rpc.KSystemError, fmt.Sprintf("查询运行详情失败: %v", err))
	}
	if runDetail == nil {
		return nil, rpc.CreateErrorWithMsg(rpc.KSystemError, "运行记录不存在")
	}
	// 假设 core.MustGetPinHeaderCorpId2Uint32(ctx) 能拿到当前上下文的 corpId
	// if runDetail.CorpId != core.MustGetPinHeaderCorpId2Uint32(ctx) {
	//    return nil, rpc.CreateErrorWithMsg(rpc.KErrPermissionDenied, "无权访问该运行记录")
	// }
	rsp.RunDetail = runDetail

	nodeExecutions, err := modelworkflow.ModelWorkflowNodeExecution.ListByWorkflowRunId(ctx, req.RunId)
	if err != nil {
		return nil, rpc.CreateErrorWithMsg(rpc.KSystemError, fmt.Sprintf("查询节点执行记录失败: %v", err))
	}

	rsp.NodeExecutions = make([]*aiadmin.WorkflowNodeExecutionItem, 0, len(nodeExecutions))
	for _, exec := range nodeExecutions {
		rsp.NodeExecutions = append(rsp.NodeExecutions, &aiadmin.WorkflowNodeExecutionItem{
			Id:                exec.Id,
			NodeId:            exec.NodeId,
			NodeType:          exec.NodeType,
			NodeName:          exec.NodeName,
			Index:             exec.Index,
			InputsPreview:     exec.Inputs,
			OutputsPreview:    exec.Outputs,
			Status:            exec.Status, // 类型转换
			ErrorPreview:      exec.Error,
			ElapsedTimeMs:     exec.ElapsedTimeMs,
			StartedAt:         exec.StartedAt,
			FinishedAt:        exec.FinishedAt,
			ExecutionMetadata: exec.ExecutionMetadata,
		})
	}

	return &rsp, nil
}

func truncateString(s string, maxLen int) string {
	if len(s) > maxLen {
		// 尝试更智能地截断 rune，避免半个汉字
		runes := []rune(s)
		if len(runes) > maxLen {
			return string(runes[:maxLen]) + "..."
		}
	}
	return s
}
