package impl

import (
	"context"
	"encoding/json"
	"fmt"
	"git.pinquest.cn/base/aiadmin"
	"git.pinquest.cn/base/aiadminserver/impl/internal/model/modelworkflow"
	"git.pinquest.cn/base/aiadminserver/impl/internal/workflow"
	"git.pinquest.cn/base/aiadminserver/impl/internal/workflow/logger"
	"git.pinquest.cn/base/aiadminserver/impl/internal/workflow/types"
	"git.pinquest.cn/base/aiadminserver/impl/pkg/utils"
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/dbproxy"
	"git.pinquest.cn/qlb/brick/dbx"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/core"
	"git.pinquest.cn/qlb/qwhale"
	"github.com/cloudwego/eino/schema"
	"strings"
	"time"
)

func CreateWorkflow(ctx *rpc.Context, req *aiadmin.CreateWorkflowReq) (*aiadmin.CreateWorkflowRsp, error) {
	var rsp aiadmin.CreateWorkflowRsp
	ctx, corpId, _, uid, err := GetCorpAndAppIdAndUid(ctx)
	if err != nil {
		return nil, err
	}

	workflowUUID := utils.NewWorkflowId()
	versionUUID := utils.NewIdWithPrefix("wv") // 版本ID，例如 "wv_xxx"
	now := uint32(time.Now().Unix())

	// 准备主工作流记录
	wf := &aiadmin.ModelWorkflow{
		Id:              workflowUUID,
		CorpId:          corpId,
		Name:            req.Name,
		Description:     req.Description,
		Status:          uint32(aiadmin.WorkflowStatus_WorkflowStatusDisabled), // 默认禁用
		ActiveVersionId: versionUUID,                                           // 指向即将创建的第一个版本
		CreatedBy:       uid,
		CreatedAt:       now,
		UpdatedAt:       now,
		UpdatedBy:       uid,
	}

	// 准备第一个版本记录
	// 将 req.Nodes (类型 []*aiadmin.WorkflowNodeDef) 和 req.Edges (类型 []*aiadmin.ModelWorkflowEdge) 序列化到 graph_snapshot_json
	graphSnapshot := &aiadmin.WorkflowGraphSnapshot{
		Nodes: req.Nodes, // req.Nodes 已经是 []*aiadmin.WorkflowNodeDef
		Edges: req.Edges, // req.Edges 已经是 []*aiadmin.ModelWorkflowEdge
	}
	snapshotJsonBytes, jsonErr := json.Marshal(graphSnapshot)
	if jsonErr != nil {
		log.Errorf("创建工作流时序列化图快照失败: %v", jsonErr)
		return nil, rpc.CreateErrorWithMsg(rpc.KSystemError, "图结构序列化失败")
	}

	initialVersion := &aiadmin.ModelWorkflowVersion{
		Id:                versionUUID,
		WorkflowId:        workflowUUID,
		VersionNumber:     1,
		VersionName:       "初始版本",
		Description:       "系统自动创建的初始版本",
		EntryNodeId:       req.EntryNodeId, // 从请求中获取入口节点
		GraphSnapshotJson: string(snapshotJsonBytes),
		TriggerConfig:     req.TriggerConfig, // 从请求获取
		CreatedBy:         uid,
		CreatedAt:         now,
	}

	// 使用事务确保数据一致性
	scope := modelworkflow.ModelWorkflow.NewScope()
	err = scope.OnTransaction(ctx, func(txCtx *rpc.Context, tx *dbx.Tx) error {
		// 创建主工作流记录
		if err := modelworkflow.ModelWorkflow.CreateInTx(txCtx, tx, wf); err != nil {
			return fmt.Errorf("创建工作流主记录失败: %w", err)
		}

		// 创建第一个版本记录
		if err := modelworkflow.ModelWorkflowVersion.CreateInTx(txCtx, tx, initialVersion); err != nil {
			return fmt.Errorf("创建工作流初始版本失败: %w", err)
		}

		// 创建生效成员记录
		if len(req.Members) > 0 {
			membersToCreate := make([]*aiadmin.ModelWorkflowMember, 0, len(req.Members))
			for _, m := range req.Members {
				memberUUID := utils.NewIdWithPrefix("wfm") // 成员记录ID
				membersToCreate = append(membersToCreate, &aiadmin.ModelWorkflowMember{
					Id:         memberUUID,
					WorkflowId: wf.Id, // 关联主工作流ID
					CorpId:     corpId,
					RobotUid:   m.RobotUid, // RobotUid 来自请求
					// CreatedAt:  now, // 可选，如果 ModelWorkflowMember 有该字段
				})
			}
			if err := modelworkflow.ModelWorkflowMember.BatchCreateInTx(txCtx, tx, membersToCreate); err != nil {
				return fmt.Errorf("创建工作流成员失败: %w", err)
			}
		}

		// 处理工作流数据集使用关系
		datasetUsages := make([]*aiadmin.ModelWorkflowDatasetUsage, 0)

		// 遍历所有节点，查找 query_knowledge 类型的节点
		for _, node := range req.Nodes {
			if node.NodeType == "query_knowledge" && node.NodeConfigJson != "" {
				// 解析节点配置
				var queryKnowledgeConfig aiadmin.QueryKnowledgeNodeConfig
				if err := json.Unmarshal([]byte(node.NodeConfigJson), &queryKnowledgeConfig); err != nil {
					log.Warnf("解析节点 %s 的配置失败: %v", node.Id, err)
					continue
				}

				// 遍历数据集ID列表，为每个数据集创建使用关系记录
				for _, datasetId := range queryKnowledgeConfig.DatasetIds {
					// usageId := utils.NewIdWithPrefix("wdu") // 工作流数据集使用关系ID
					datasetUsages = append(datasetUsages, &aiadmin.ModelWorkflowDatasetUsage{
						WorkflowId:        workflowUUID,
						WorkflowVersionId: versionUUID,
						DatasetId:         datasetId,
						NodeId:            node.Id,
						NodeType:          node.NodeType,
						CorpId:            corpId,
						CreatedAt:         now,
					})
				}
			}
		}

		// 批量创建工作流数据集使用关系记录
		if len(datasetUsages) > 0 {
			if err := modelworkflow.ModelWorkflowDatasetUsage.BatchCreateInTx(txCtx, tx, datasetUsages); err != nil {
				return fmt.Errorf("创建工作流数据集使用关系失败: %w", err)
			}
		}

		return nil
	}, dbproxy.OptionWithUseDb(S.Conf.Db), dbproxy.OptionWithUseBiz(true))

	if err != nil {
		return nil, rpc.CreateErrorWithMsg(rpc.KSystemError, err.Error())
	}

	rsp.Id = wf.Id
	// rsp.InitialVersionId = initialVersion.Id // 可以考虑返回初始版本ID
	return &rsp, nil
}

func GetWorkflowList(ctx *rpc.Context, req *aiadmin.GetWorkflowListReq) (*aiadmin.GetWorkflowListRsp, error) {
	var rsp aiadmin.GetWorkflowListRsp
	ctx, corpId, appId, uid, err := GetCorpAndAppIdAndUid(ctx) // appId 在此处可用
	if err != nil {
		return nil, err
	}

	list, paginate, err := modelworkflow.ModelWorkflow.ListWithOption(ctx, corpId, uid, req)
	if err != nil {
		log.Errorf("GetWorkflowList: ListWithOption failed: %v", err)
		return nil, rpc.CreateErrorWithMsg(rpc.KSystemError, "查询工作流列表失败")
	}

	items := make([]*aiadmin.WorkflowListItem, 0, len(list))
	if len(list) == 0 {
		rsp.List = items
		rsp.Paginate = paginate
		return &rsp, nil
	}

	activeVersionIDs := make([]string, 0, len(list))
	workflowIDs := make([]string, 0, len(list))
	for _, wf := range list {
		if wf.ActiveVersionId != "" {
			activeVersionIDs = append(activeVersionIDs, wf.ActiveVersionId)
		}
		workflowIDs = append(workflowIDs, wf.Id)
	}

	versionsMap := make(map[string]*aiadmin.ModelWorkflowVersion)
	if len(activeVersionIDs) > 0 {
		versions, errVer := modelworkflow.ModelWorkflowVersion.ListByIds(ctx, activeVersionIDs)
		if errVer != nil {
			log.Warnf("GetWorkflowList: Failed to get active versions: %v", errVer)
			// 非致命错误，继续执行，但版本相关信息可能不完整
		}
		for _, v := range versions {
			versionsMap[v.Id] = v
		}
	}

	membersByWorkflowID := make(map[string][]*aiadmin.ModelWorkflowMember)
	var allRobotUids []uint64 // 收集所有的 RobotUid (uint64 类型)
	if len(workflowIDs) > 0 {
		allMembersModels, errMem := modelworkflow.ModelWorkflowMember.ListByWorkflowIds(ctx, workflowIDs)
		if errMem != nil {
			log.Warnf("GetWorkflowList: Failed to get members for workflows: %v", errMem)
			// 非致命错误，成员信息可能不完整
		}
		for _, memberModel := range allMembersModels { // memberModel 是 *aiadmin.ModelWorkflowMember
			membersByWorkflowID[memberModel.WorkflowId] = append(membersByWorkflowID[memberModel.WorkflowId], memberModel)
			// 假设 memberModel.RobotUid 是 uint64 类型
			if memberModel.RobotUid != 0 { // 检查是否为0 (uint64的零值)
				allRobotUids = append(allRobotUids, memberModel.RobotUid)
			}
		}
	}

	robotInfoMap := make(map[uint64]*qwhale.ModelHostAccount)
	if len(allRobotUids) > 0 { // 使用 allRobotUids (uint64 类型)
		uniqueRobotUids := make([]uint64, 0, len(allRobotUids))
		processedUids := make(map[uint64]bool)  // Key 是 uint64
		for _, robotUid := range allRobotUids { // robotUid 是 uint64
			if _, exists := processedUids[robotUid]; !exists {
				processedUids[robotUid] = true
				uniqueRobotUids = append(uniqueRobotUids, robotUid)
			}
		}

		if len(uniqueRobotUids) > 0 {
			option := core.ListOption{}
			lo := option.AddOpt(int32(qwhale.GetHostAccountListSysReq_ListOptionUidList), uniqueRobotUids)

			qwhaleReq := &qwhale.GetHostAccountListSysReq{
				CorpId:     corpId,
				AppId:      appId,
				ListOption: lo,
			}

			qwhaleRsp, qwhaleErr := qwhale.GetHostAccountListSys(ctx, qwhaleReq)
			if qwhaleErr != nil {
				log.Warnf("GetWorkflowList: Failed to get host accounts from qwhale: %v", qwhaleErr)
				// 继续执行而不包含 robot_info，或将错误处理为致命错误
			} else if qwhaleRsp != nil {
				for _, acc := range qwhaleRsp.List {
					if acc != nil { // 为 acc 添加 nil 检查
						robotInfoMap[acc.Uid] = acc // acc.Uid 是 uint64
					}
				}
			}
		}
	}

	for _, f := range list { // f 是 *aiadmin.ModelWorkflow
		item := &aiadmin.WorkflowListItem{
			Id:          f.Id,
			Name:        f.Name,
			Description: f.Description,
			Status:      f.Status,
			CreatedAt:   f.CreatedAt,
			UpdatedAt:   f.UpdatedAt,
			// Members 将在下方填充
		}
		if activeVersion, ok := versionsMap[f.ActiveVersionId]; ok {
			var triggerTypeInt32 int32
			if activeVersion.TriggerConfig != "" {
				var triggerConfig aiadmin.TriggerNodeConfig
				if errJson := json.Unmarshal([]byte(activeVersion.TriggerConfig), &triggerConfig); errJson != nil {
					log.Warnf("WorkflowList: 解析TriggerConfig失败 workflow_id=%s: %v", f.Id, errJson)
				} else {
					triggerTypeInt32 = triggerConfig.TriggerType
				}
			}
			item.TriggerType = triggerTypeInt32
			item.CurrentVersion = activeVersion.VersionNumber
			item.IsDraft = false // 或一些用于判断是否为草稿的逻辑
		}

		if memberModels, ok := membersByWorkflowID[f.Id]; ok { // memberModels 是 []*aiadmin.ModelWorkflowMember
			item.Members = make([]*aiadmin.WorkflowMemberDetail, 0, len(memberModels))
			for _, memberModel := range memberModels {
				// memberModel.RobotUid 已经是 uint64, 不需要再转换
				memberDetail := &aiadmin.WorkflowMemberDetail{
					Id:         memberModel.Id,
					WorkflowId: memberModel.WorkflowId,
					CorpId:     memberModel.CorpId,
					RobotUid:   memberModel.RobotUid, // 直接使用 uint64 类型
					CreatedAt:  memberModel.CreatedAt,
					UpdatedAt:  memberModel.UpdatedAt,
					DeletedAt:  memberModel.DeletedAt,
					RobotInfo:  robotInfoMap[memberModel.RobotUid], // 使用 uint64 作为 map 的 key
				}
				item.Members = append(item.Members, memberDetail)
			}
		}
		items = append(items, item)
	}

	rsp.List = items
	rsp.Paginate = paginate
	return &rsp, nil
}

// GetWorkflowDetail 函数 (调整为从 active_version 加载图快照)
func GetWorkflowDetail(ctx *rpc.Context, req *aiadmin.GetWorkflowDetailReq) (*aiadmin.GetWorkflowDetailRsp, error) {
	var rsp aiadmin.GetWorkflowDetailRsp

	workflowDef, err := modelworkflow.ModelWorkflow.GetById(ctx, req.Id)
	if err != nil {
		log.Errorf("GetWorkflowDetail: GetById failed for workflow %s: %v", req.Id, err)
		return nil, rpc.CreateErrorWithMsg(aiadmin.ErrWorkflowNotFound, "获取工作流主信息失败: "+err.Error())
	}
	if workflowDef == nil {
		return nil, rpc.CreateErrorWithMsg(aiadmin.ErrWorkflowNotFound, "工作流不存在")
	}

	rsp.Workflow = workflowDef

	if workflowDef.ActiveVersionId == "" {
		log.Warnf("GetWorkflowDetail: Workflow %s has no active version.", req.Id)
		// 根据业务需求，可以返回空图，或者报错
		return &rsp, nil // 或者返回错误，提示没有可展示的活跃版本
	}

	activeVersion, err := modelworkflow.ModelWorkflowVersion.GetById(ctx, workflowDef.ActiveVersionId)
	if err != nil {
		log.Errorf("GetWorkflowDetail: GetById failed for active version %s (workflow %s): %v", workflowDef.ActiveVersionId, req.Id, err)
		return nil, rpc.CreateErrorWithMsg(aiadmin.ErrWorkflowNotFound, "获取工作流活跃版本失败: "+err.Error())
	}
	if activeVersion == nil {
		return nil, rpc.CreateErrorWithMsg(aiadmin.ErrWorkflowNotFound, "活跃的工作流版本未找到")
	}

	rsp.ActiveVersionInfo = activeVersion // 返回活跃版本信息

	// 从快照解析节点和边
	if activeVersion.GraphSnapshotJson != "" {
		var graphSnapshot aiadmin.WorkflowGraphSnapshot
		if err := json.Unmarshal([]byte(activeVersion.GraphSnapshotJson), &graphSnapshot); err != nil {
			log.Errorf("GetWorkflowDetail: Failed to unmarshal graph snapshot for version %s (workflow %s): %v", activeVersion.Id, req.Id, err)
			return nil, rpc.CreateErrorWithMsg(rpc.KSystemError, "解析工作流图结构失败")
		}
		rsp.NodeDefs = graphSnapshot.Nodes // WorkflowNodeDef 列表
		rsp.Edges = graphSnapshot.Edges    // ModelWorkflowEdge 列表
	}

	// 获取成员列表
	members, err := modelworkflow.ModelWorkflowMember.ListByWorkflowId(ctx, req.Id) // 你需要实现 ListByWorkflowId 方法
	if err != nil {
		log.Warnf("GetWorkflowDetail: Failed to list members for workflow %s: %v", req.Id, err)
		// 非致命错误，可以继续返回其他信息
	}
	rsp.Members = members

	return &rsp, nil
}

func UpdateWorkflow(ctx *rpc.Context, req *aiadmin.UpdateWorkflowReq) (*aiadmin.UpdateWorkflowRsp, error) {
	var rsp aiadmin.UpdateWorkflowRsp
	ctx, corpId, _, uid, err := GetCorpAndAppIdAndUid(ctx)
	if err != nil {
		return nil, err
	}

	mainWf, err := modelworkflow.ModelWorkflow.GetById(ctx, req.Id)
	if err != nil {
		return nil, rpc.CreateErrorWithMsg(aiadmin.ErrWorkflowNotFound, "工作流不存在")
	}
	if mainWf.CorpId != corpId {
		return nil, rpc.CreateErrorWithMsg(rpc.KSystemError, "无权操作该工作流")
	}

	newVersionId := utils.NewIdWithPrefix("wv")
	now := uint32(time.Now().Unix())

	scope := modelworkflow.ModelWorkflow.NewScope()
	err = scope.OnTransaction(ctx, func(txCtx *rpc.Context, tx *dbx.Tx) error {
		latestVersionNum, errDb := modelworkflow.ModelWorkflowVersion.GetLatestVersionNumberByWorkflowId(txCtx, tx, req.Id)
		if errDb != nil && !modelworkflow.ModelWorkflowVersion.IsNotFoundErr(errDb) {
			return fmt.Errorf("获取最新版本号失败: %w", errDb)
		}
		newVersionNumber := latestVersionNum + 1

		// 构建图快照
		nodeDefsForSnapshot := make([]*aiadmin.WorkflowNodeDef, len(req.Nodes))
		for i, n := range req.Nodes {
			nodeDefsForSnapshot[i] = &aiadmin.WorkflowNodeDef{
				Id:             n.Id,
				NodeName:       n.NodeName,
				NodeType:       n.NodeType,
				NodeConfigJson: n.NodeConfigJson,
				PositionX:      n.PositionX,
				PositionY:      n.PositionY,
			}
		}
		graphSnapshot := &aiadmin.WorkflowGraphSnapshot{
			Nodes: nodeDefsForSnapshot,
			Edges: req.Edges,
		}
		snapshotJsonBytes, jsonErr := json.Marshal(graphSnapshot)
		if jsonErr != nil {
			return fmt.Errorf("序列化图快照失败: %w", jsonErr)
		}

		newVersion := &aiadmin.ModelWorkflowVersion{
			Id:                newVersionId,
			WorkflowId:        req.Id,
			VersionNumber:     newVersionNumber,
			VersionName:       fmt.Sprintf("版本 %d", newVersionNumber),
			Description:       "更新版本",
			EntryNodeId:       req.EntryNodeId,
			GraphSnapshotJson: string(snapshotJsonBytes),
			TriggerConfig:     req.TriggerConfig,
			CreatedBy:         uid,
			CreatedAt:         now,
		}

		if err := modelworkflow.ModelWorkflowVersion.CreateInTx(txCtx, tx, newVersion); err != nil {
			return fmt.Errorf("创建新工作流版本失败: %w", err)
		}

		// 删除旧的工作流数据集使用关系记录
		if err := modelworkflow.ModelWorkflowDatasetUsage.DeleteByWorkflowIdInTx(txCtx, tx, req.Id); err != nil {
			return fmt.Errorf("删除旧的工作流数据集使用关系记录失败: %w", err)
		}

		// 处理工作流数据集使用关系
		datasetUsages := make([]*aiadmin.ModelWorkflowDatasetUsage, 0)

		// 遍历所有节点，查找 query_knowledge 类型的节点
		for _, node := range req.Nodes {
			if node.NodeType == "query_knowledge" && node.NodeConfigJson != "" {
				// 解析节点配置
				var queryKnowledgeConfig aiadmin.QueryKnowledgeNodeConfig
				if err := json.Unmarshal([]byte(node.NodeConfigJson), &queryKnowledgeConfig); err != nil {
					log.Warnf("解析节点 %s 的配置失败: %v", node.Id, err)
					continue
				}

				// 遍历数据集ID列表，为每个数据集创建使用关系记录
				for _, datasetId := range queryKnowledgeConfig.DatasetIds {
					datasetUsages = append(datasetUsages, &aiadmin.ModelWorkflowDatasetUsage{
						WorkflowId:        req.Id,
						WorkflowVersionId: newVersionId,
						DatasetId:         datasetId,
						NodeId:            node.Id,
						NodeType:          node.NodeType,
						CorpId:            corpId,
						CreatedAt:         now,
					})
				}
			}
		}

		// 批量创建工作流数据集使用关系记录
		if len(datasetUsages) > 0 {
			if err := modelworkflow.ModelWorkflowDatasetUsage.BatchCreateInTx(txCtx, tx, datasetUsages); err != nil {
				return fmt.Errorf("创建工作流数据集使用关系失败: %w", err)
			}
		}

		updateMainWfData := map[string]interface{}{
			"name":              req.Name,
			"description":       req.Description,
			"active_version_id": newVersionId,
			"updated_at":        now,
			"updated_by":        uid,
		}

		if err := modelworkflow.ModelWorkflow.UpdateByIdInTx(txCtx, tx, req.Id, updateMainWfData); err != nil {
			return fmt.Errorf("更新主工作流记录失败: %w", err)
		}

		if err := modelworkflow.ModelWorkflowMember.DeleteByWorkflowIdInTx(txCtx, tx, req.Id); err != nil {
			return fmt.Errorf("删除旧工作流成员失败: %w", err)
		}

		if len(req.Members) > 0 && mainWf.Status == uint32(aiadmin.WorkflowStatus_WorkflowStatusEnabled) {
			for _, member := range req.Members {
				otherWorkflowIds, errGetOthers := modelworkflow.ModelWorkflowMember.GetOtherWorkflowIdsByRobotUid(txCtx, tx, mainWf.CorpId, member.RobotUid, req.Id)
				if errGetOthers != nil {
					return fmt.Errorf("成员冲突检查失败: %w", errGetOthers)
				}
				if len(otherWorkflowIds) > 0 {
					enabledCount, errCount := modelworkflow.ModelWorkflow.CountEnabledWorkflowsByIds(txCtx, tx, otherWorkflowIds)
					if errCount != nil {
						return fmt.Errorf("成员状态检查失败: %w", errCount)
					}
					if enabledCount > 0 {
						return fmt.Errorf("成员 %d 已在其他启用的工作流中", member.RobotUid)
					}
				}
			}
		}

		membersToCreate := make([]*aiadmin.ModelWorkflowMember, 0, len(req.Members))
		for _, m := range req.Members {
			memberUUID := utils.NewIdWithPrefix("wfm")
			membersToCreate = append(membersToCreate, &aiadmin.ModelWorkflowMember{
				Id:         memberUUID,
				WorkflowId: req.Id,
				CorpId:     corpId,
				RobotUid:   m.RobotUid,
			})
		}
		if err := modelworkflow.ModelWorkflowMember.BatchCreateInTx(txCtx, tx, membersToCreate); err != nil {
			return fmt.Errorf("创建新工作流成员失败: %w", err)
		}

		return nil
	}, dbproxy.OptionWithUseDb(S.Conf.Db), dbproxy.OptionWithUseBiz(true))

	if err != nil {
		return nil, rpc.CreateErrorWithMsg(rpc.KSystemError, err.Error())
	}

	rsp.Id = req.Id
	rsp.NewVersionId = newVersionId
	return &rsp, nil
}

// DeleteWorkflow 函数 (调整为删除主记录和所有版本记录、成员记录)
func DeleteWorkflow(ctx *rpc.Context, req *aiadmin.DeleteWorkflowReq) (*aiadmin.DeleteWorkflowRsp, error) {
	var rsp aiadmin.DeleteWorkflowRsp
	ctx, corpId, _, _, err := GetCorpAndAppIdAndUid(ctx)
	if err != nil {
		return nil, err
	}

	// 校验工作流是否存在且属于当前公司
	wf, err := modelworkflow.ModelWorkflow.GetById(ctx, req.Id)
	if err != nil {
		return nil, rpc.CreateErrorWithMsg(aiadmin.ErrWorkflowNotFound, "工作流不存在")
	}
	if wf.CorpId != corpId {
		return nil, rpc.CreateErrorWithMsg(rpc.KSystemError, "无权操作该工作流")
	}
	// todo: 检查工作流是否正在被引用（例如，是否有正在运行的实例），如果有，可能不允许删除或需要特殊处理

	scope := modelworkflow.ModelWorkflow.NewScope()
	err = scope.OnTransaction(ctx, func(txCtx *rpc.Context, tx *dbx.Tx) error {
		// 1. 删除所有版本记录
		if err := modelworkflow.ModelWorkflowVersion.DeleteByWorkflowIdInTx(txCtx, tx, req.Id); err != nil {
			return fmt.Errorf("删除工作流版本记录失败: %w", err)
		}
		// 2. 删除所有成员记录
		if err := modelworkflow.ModelWorkflowMember.DeleteByWorkflowIdInTx(txCtx, tx, req.Id); err != nil {
			return fmt.Errorf("删除工作流成员记录失败: %w", err)
		}
		// 3. 删除所有数据集使用关系记录
		if err := modelworkflow.ModelWorkflowDatasetUsage.DeleteByWorkflowIdInTx(txCtx, tx, req.Id); err != nil {
			return fmt.Errorf("删除工作流数据集使用关系记录失败: %w", err)
		}
		// 4. 删除主工作流记录
		// 你需要为 ModelWorkflow 实现 DeleteByIdInTx
		if err := modelworkflow.ModelWorkflow.DeleteByIdInTx(txCtx, tx, req.Id); err != nil {
			return fmt.Errorf("删除工作流主记录失败: %w", err)
		}
		return nil
	}, dbproxy.OptionWithUseDb(S.Conf.Db), dbproxy.OptionWithUseBiz(true))

	if err != nil {
		return nil, rpc.CreateErrorWithMsg(rpc.KSystemError, "删除工作流失败: "+err.Error())
	}

	rsp.Id = req.Id
	return &rsp, nil
}

func ToggleWorkflowStatus(ctx *rpc.Context, req *aiadmin.ToggleWorkflowStatusReq) (*aiadmin.ToggleWorkflowStatusRsp, error) {
	var rsp aiadmin.ToggleWorkflowStatusRsp
	ctx, corpId, appId, uid, err := GetCorpAndAppIdAndUid(ctx) // 获取 appId
	if err != nil {
		return nil, err
	}

	wf, err := modelworkflow.ModelWorkflow.GetById(ctx, req.Id)
	if err != nil {
		return nil, rpc.CreateErrorWithMsg(aiadmin.ErrWorkflowNotFound, "工作流不存在")
	}
	if wf.CorpId != corpId {
		return nil, rpc.CreateErrorWithMsg(rpc.KSystemError, "无权操作该工作流")
	}

	scope := modelworkflow.ModelWorkflow.NewScope()
	err = scope.OnTransaction(ctx, func(txCtx *rpc.Context, tx *dbx.Tx) error {
		if req.Status == uint32(aiadmin.WorkflowStatus_WorkflowStatusEnabled) {
			members, errMemberTx := modelworkflow.ModelWorkflowMember.ListByWorkflowId(txCtx, req.Id)
			if errMemberTx != nil {
				return fmt.Errorf("获取成员失败: %w", errMemberTx)
			}

			var conflictingRobotUids []uint64
			for _, member := range members {
				otherWorkflowIds, errGetOthersTx := modelworkflow.ModelWorkflowMember.GetOtherWorkflowIdsByRobotUid(txCtx, tx, wf.CorpId, member.RobotUid, req.Id)
				if errGetOthersTx != nil {
					return fmt.Errorf("检查成员关联失败: %w", errGetOthersTx)
				}
				if len(otherWorkflowIds) > 0 {
					enabledCount, errCountTx := modelworkflow.ModelWorkflow.CountEnabledWorkflowsByIds(txCtx, tx, otherWorkflowIds)
					if errCountTx != nil {
						return fmt.Errorf("检查其他工作流状态失败: %w", errCountTx)
					}
					if enabledCount > 0 {
						conflictingRobotUids = append(conflictingRobotUids, member.RobotUid)
					}
				}
			}

			if len(conflictingRobotUids) > 0 {
				// 获取冲突成员的名称
				uniqueConflictingUids := make([]uint64, 0, len(conflictingRobotUids))
				processedUids := make(map[uint64]bool)
				for _, ruid := range conflictingRobotUids {
					if _, exists := processedUids[ruid]; !exists {
						processedUids[ruid] = true
						uniqueConflictingUids = append(uniqueConflictingUids, ruid)
					}
				}

				var conflictingRobotNames []string
				if len(uniqueConflictingUids) > 0 {
					option := core.ListOption{}
					lo := option.AddOpt(int32(qwhale.GetHostAccountListSysReq_ListOptionUidList), uniqueConflictingUids)

					qwhaleReq := &qwhale.GetHostAccountListSysReq{
						CorpId:     corpId,
						AppId:      appId,
						ListOption: lo,
					}
					qwhaleRsp, qwhaleErr := qwhale.GetHostAccountListSys(ctx, qwhaleReq)
					if qwhaleErr != nil {
						log.Warnf("ToggleWorkflowStatus: 获取冲突成员名称失败: %v", qwhaleErr)
						// 即使获取名称失败，也返回 UID 列表
						for _, ruid := range uniqueConflictingUids {
							conflictingRobotNames = append(conflictingRobotNames, fmt.Sprintf("UID %d", ruid))
						}
					} else if qwhaleRsp != nil {
						for _, acc := range qwhaleRsp.List {
							if acc != nil {
								conflictingRobotNames = append(conflictingRobotNames, acc.Name)
							}
						}
					}
				}
				if len(conflictingRobotNames) == 0 {
					for _, ruid := range uniqueConflictingUids {
						conflictingRobotNames = append(conflictingRobotNames, fmt.Sprintf("UID %d", ruid))
					}
				}
				return fmt.Errorf("更新失败，以下成员已在其他启用的工作流中: %s", strings.Join(conflictingRobotNames, ", "))
			}
		}

		updateData := map[string]interface{}{
			"status":     req.Status,
			"updated_at": uint32(time.Now().Unix()),
			"updated_by": uid,
		}

		return modelworkflow.ModelWorkflow.UpdateByIdInTx(txCtx, tx, req.Id, updateData)
	}, dbproxy.OptionWithUseDb(S.Conf.Db), dbproxy.OptionWithUseBiz(true))

	if err != nil {
		return nil, rpc.CreateErrorWithMsg(rpc.KSystemError, "更新工作流状态失败: "+err.Error())
	}

	rsp.Id = req.Id
	rsp.Status = req.Status
	return &rsp, nil
}

func TestWorkflow(rpcCtx *rpc.Context, req *aiadmin.TestWorkflowReq) (*aiadmin.TestWorkflowRsp, error) {
	ctxStandard := context.Background()

	_, corpId, _, uid, err := GetCorpAndAppIdAndUid(rpcCtx)
	if err != nil {
		return nil, err
	}

	var rsp aiadmin.TestWorkflowRsp

	// 1. 获取元数据
	workflowDef, err := modelworkflow.ModelWorkflow.GetById(rpcCtx, req.Id)
	if err != nil {
		return nil, rpc.CreateErrorWithMsg(aiadmin.ErrWorkflowNotFound, "获取工作流失败: "+err.Error())
	}
	if workflowDef.ActiveVersionId == "" {
		return nil, rpc.CreateErrorWithMsg(aiadmin.ErrWorkflowInvalid, fmt.Sprintf("工作流 '%s' 没有活动的版本", req.Id))
	}
	activeVersion, err := modelworkflow.ModelWorkflowVersion.GetById(rpcCtx, workflowDef.ActiveVersionId)
	if err != nil {
		log.Errorf("TestWorkflow: 获取工作流 %s 的活跃版本 %s 失败: %v", req.Id, workflowDef.ActiveVersionId, err)
		return nil, rpc.CreateErrorWithMsg(aiadmin.ErrWorkflowNotFound, "获取工作流活跃版本失败: "+err.Error())
	}
	if activeVersion == nil {
		log.Errorf("TestWorkflow: 未找到工作流 %s 的活跃版本 %s", req.Id, workflowDef.ActiveVersionId)
		return nil, rpc.CreateErrorWithMsg(aiadmin.ErrWorkflowNotFound, "活跃的工作流版本未找到")
	}

	// 2. 准备 wfExecCtx
	mockTask := &aiadmin.AIWorkflowMsqTask{
		WorkflowId: req.Id, CorpId: corpId, Keyword: req.InputMessage, SendAccountId: req.UserId, RobotUid: uid,
		RecentHistory: []*aiadmin.ChatMessage{
			{Role: string(aiadmin.RoleTypeUser), Content: "你好"},
			{Role: string(aiadmin.RoleTypeAssistant), Content: "你好！有什么可以帮助你的吗？"},
			{Role: string(aiadmin.RoleTypeUser), Content: "我想问问关于会员权益的问题"},
			{Role: string(aiadmin.RoleTypeAssistant), Content: "好的，请问具体是哪方面的会员权益呢？"},
		},
	}
	var triggerCfg aiadmin.TriggerNodeConfig
	if activeVersion.TriggerConfig != "" {
		_ = json.Unmarshal([]byte(activeVersion.TriggerConfig), &triggerCfg)
	}
	mockTask.AppId = 0
	mockTask.ChatId = req.UserId
	mockTask.ChatType = 1

	wfExecCtx := &types.WorkflowExecutionContext{
		RunMode:       types.RunModeTest,
		InitialTask:   mockTask,
		RequestId:     rpcCtx.GetReqId(),
		CorpId:        corpId,
		AppId:         mockTask.AppId,
		RobotUid:      mockTask.RobotUid,
		ChatId:        mockTask.ChatId,
		ChatType:      mockTask.ChatType,
		RecentHistory: mockTask.RecentHistory,
	}

	// 3. 【修改】创建 LogContext，不传递 Tx，因为日志方法内部会管理自己的短事务
	// 使用传入的 rpcCtx，它将用于 LogContext 内部启动事务
	logCtx := logger.NewLogContext(rpcCtx, wfExecCtx)

	// 4. 【修改】编译工作流，传入 LogContext
	runnable, compileErr := workflow.CompileWorkflow(workflowDef, activeVersion, ctxStandard, logCtx)

	// 为 Invoke 准备的变量
	var finalPayload *types.WorkflowPayload
	var invokeErr error

	// 【修改】在执行 Invoke 前后，调用 logCtx 的方法。这些方法内部会处理事务。
	if errStart := logCtx.StartWorkflowRun(workflowDef, activeVersion, mockTask); errStart != nil {
		log.Errorf("TestWorkflow: 记录工作流开始日志失败 for %s: %v", wfExecCtx.RequestId, errStart)
		// 即使开始日志失败，也可能希望继续执行测试，或者根据策略返回错误
	}

	defer func() {
		// execErr 包含 compileErr 或 invokeErr
		var finalExecError error
		if compileErr != nil {
			finalExecError = compileErr
		} else if invokeErr != nil {
			finalExecError = invokeErr
		} else if finalPayload != nil && finalPayload.Error != nil {
			finalExecError = finalPayload.Error
		}
		if errEnd := logCtx.EndWorkflowRun(finalPayload, finalExecError); errEnd != nil {
			log.Errorf("TestWorkflow: 记录工作流结束日志失败 for %s: %v", wfExecCtx.RequestId, errEnd)
		}
	}()

	if compileErr != nil { // 编译错误，直接处理并返回
		log.Errorf("TestWorkflow: 工作流 %s 版本 %s 编译失败: %v", req.Id, activeVersion.Id, compileErr)
		rsp.OutputMessage = "工作流编译失败: " + compileErr.Error()
		return &rsp, nil // 或者 return nil, compileErr
	}

	// 5. 准备输入并执行
	userInput := schema.Message{Role: schema.User, Content: req.InputMessage}
	userInput.Extra = map[string]interface{}{
		types.WorkflowExecutionCtxKey: wfExecCtx,
	}

	finalPayload, invokeErr = runnable.Invoke(ctxStandard, &userInput)

	// 6. 处理结果
	if invokeErr != nil {
		log.Errorf("TestWorkflow: 工作流 %s 版本 %s 执行失败: %v", req.Id, activeVersion.Id, invokeErr)
		rsp.OutputMessage = "工作流执行Invoke失败: " + invokeErr.Error()
		return &rsp, nil
	}
	if finalPayload == nil {
		rsp.OutputMessage = "工作流执行成功但返回的载荷为nil"
		return &rsp, nil
	}
	if finalPayload.Error != nil {
		rsp.OutputMessage = "工作流执行出错: " + finalPayload.Error.Error()
		return &rsp, nil
	}

	rsp.OutputMessage = finalPayload.FinalAnswer
	return &rsp, nil
}
