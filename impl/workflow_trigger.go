package impl

import (
	"encoding/json"
	"fmt"

	"git.pinquest.cn/base/aiadmin"
	"git.pinquest.cn/base/aiadminserver/impl/internal/model/modelworkflow"
	"git.pinquest.cn/qlb/brick/log"
	"git.pinquest.cn/qlb/brick/rpc"
)

func GetRobotActiveWorkflowConfig(ctx *rpc.Context, req *aiadmin.GetRobotActiveWorkflowConfigReq) (*aiadmin.GetRobotActiveWorkflowConfigRsp, error) {
	var rsp aiadmin.GetRobotActiveWorkflowConfigRsp

	// 1. 根据 robot_uid 和 corp_id 查询 ModelWorkflowMember 获取关联的 workflow_id 列表
	members, err := modelworkflow.ModelWorkflowMember.ListByRobotUid(ctx, req.CorpId, req.RobotUid)
	if err != nil {
		log.Errorf("GetRobotActiveWorkflowConfig: ListByRobotUid failed for robot_uid %d, corp_id %d: %v", req.RobotUid, req.CorpId, err)
		return nil, rpc.CreateErrorWithMsg(rpc.KSystemError, "查询成员工作流关联失败")
	}

	if len(members) == 0 {
		log.Warnf("GetRobotActiveWorkflowConfig: No workflow associated with robot_uid %d, corp_id %d", req.RobotUid, req.CorpId)
		return &rsp, nil // 没有关联的工作流
	}

	var activeWorkflowId string
	var activeWorkflowVersionId string
	var activeWorkflowStatus uint32

	for _, member := range members {
		wf, err := modelworkflow.ModelWorkflow.GetById(ctx, member.WorkflowId)
		if err != nil {
			log.Warnf("GetRobotActiveWorkflowConfig: Failed to get workflow %s details: %v", member.WorkflowId, err)
			continue
		}
		if wf.Status == uint32(aiadmin.WorkflowStatus_WorkflowStatusEnabled) {
			if activeWorkflowId != "" {
				// 严重错误：activeWorkflowId 已经被赋值过，说明一个 robot_uid 关联了多个启用的工作流，这违反了产品原则
				log.Errorf("CRITICAL: Robot_uid %d, corp_id %d is associated with multiple enabled workflows: %s and %s", req.RobotUid, req.CorpId, activeWorkflowId, wf.Id)
				// 或者直接报错，强制修正数据。
				return nil, rpc.CreateErrorWithMsg(rpc.KSystemError, fmt.Sprintf("机器人 %d 配置错误：关联了多个启用的工作流", req.RobotUid))
			}
			activeWorkflowId = wf.Id
			activeWorkflowVersionId = wf.ActiveVersionId
			activeWorkflowStatus = wf.Status
		}
	}

	if activeWorkflowId == "" || activeWorkflowVersionId == "" {
		log.Infof("GetRobotActiveWorkflowConfig: No ENABLED workflow found for robot_uid %d, corp_id %d", req.RobotUid, req.CorpId)
		return &rsp, nil // 没有启用的工作流
	}

	// 获取活跃版本的信息
	activeVersion, err := modelworkflow.ModelWorkflowVersion.GetById(ctx, activeWorkflowVersionId)
	if err != nil {
		log.Errorf("GetRobotActiveWorkflowConfig: Failed to get active version %s for workflow %s: %v", activeWorkflowVersionId, activeWorkflowId, err)
		return nil, rpc.CreateErrorWithMsg(rpc.KSystemError, "获取工作流版本信息失败")
	}
	if activeVersion == nil {
		log.Errorf("GetRobotActiveWorkflowConfig: Active version %s for workflow %s not found", activeWorkflowVersionId, activeWorkflowId)
		return nil, rpc.CreateErrorWithMsg(rpc.KSystemError, "活跃的工作流版本未找到")
	}

	var triggerConfig aiadmin.TriggerNodeConfig
	if activeVersion.TriggerConfig != "" {
		err = json.Unmarshal([]byte(activeVersion.TriggerConfig), &triggerConfig)
		if err != nil {
			log.Warnf("GetRobotActiveWorkflowConfig: Failed to unmarshal TriggerConfig for workflow %s, version %s: %v",
				activeWorkflowId, activeWorkflowVersionId, err)
		}
	}

	rsp.WorkflowId = activeWorkflowId
	rsp.ActiveVersionId = activeWorkflowVersionId
	rsp.TriggerType = triggerConfig.TriggerType
	rsp.TriggerConfig = activeVersion.TriggerConfig
	rsp.WorkflowStatus = activeWorkflowStatus

	log.Infof("GetRobotActiveWorkflowConfig: Found active workflow for robot_uid %d, corp_id %d. WorkflowID: %s, VersionID: %s",
		req.RobotUid, req.CorpId, rsp.WorkflowId, rsp.ActiveVersionId)

	return &rsp, nil
}
