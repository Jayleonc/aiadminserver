package impl

import (
	"fmt"

	"git.pinquest.cn/base/aiadmin"
	"git.pinquest.cn/base/aiadminserver/impl/internal/model/modelrag"
	"git.pinquest.cn/base/aiadminserver/impl/internal/model/modelworkflow"
	"git.pinquest.cn/base/aiadminserver/impl/internal/rag/config"
	task2 "git.pinquest.cn/base/aiadminserver/impl/internal/rag/task"
	"git.pinquest.cn/base/aiadminserver/impl/pkg/utils"
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/rpc"
	utils2 "git.pinquest.cn/qlb/brick/utils"
	"git.pinquest.cn/qlb/commonv2"
	"git.pinquest.cn/qlb/core"
)

func CreateDataset(ctx *rpc.Context, req *aiadmin.CreateDatasetReq) (*aiadmin.CreateDatasetRsp, error) {
	var rsp aiadmin.CreateDatasetRsp

	ctx, corpId, appId, uid, err := GetCorpAndAppIdAndUid(ctx)
	if err != nil {
		return nil, err
	}

	// 检查名称是否已存在
	existing, err := modelrag.ModelDataset.GetByCorpIdAndName(ctx, corpId, req.Name)
	if err != nil && !modelrag.ModelDataset.IsNotFoundErr(err) {
		log.Errorf("检查知识库名称失败: %v", err)
		return nil, rpc.CreateErrorWithMsg(rpc.KSystemError, "检查知识库名称失败")
	}

	if existing != nil && existing.Id != 0 {
		return nil, rpc.CreateErrorWithMsg(aiadmin.ErrDatasetNameExisted, "知识库名称已存在，请使用其他名称")
	}

	collectionName := utils.GenerateMilvusCollectionName(corpId)

	entity := &aiadmin.ModelDataset{
		CorpId:                 corpId,
		RobotUid:               uint32(uid),
		Name:                   req.Name,
		Description:            req.Description,
		IsPublic:               true,
		DataSourceType:         req.DataSourceType,
		EmbeddingModel:         config.DefaultDatasetConfig.EmbeddingModel,
		EmbeddingModelProvider: S.Conf.DefaultEmbeddingProviderID,
		IndexingTechnique:      config.DefaultDatasetConfig.IndexingTechnique,
		IndexStruct:            config.DefaultDatasetConfig.IndexStruct,
		RetrievalModel:         config.DefaultDatasetConfig.RetrievalModel,
		BuiltInFieldEnabled:    config.DefaultDatasetConfig.BuiltInFieldEnabled,
		CollectionName:         collectionName,
		IsDefault:              false,
	}

	result, err := modelrag.ModelDataset.Create(ctx, entity)
	if err != nil {
		return nil, rpc.CreateErrorWithMsg(rpc.KSystemError, "创建知识库失败")
	}

	rsp.Id = result.Id

	// === 新增内容：如果是“同步话术库”来源，启动异步同步任务 ===
	if req.DataSourceType == uint32(aiadmin.DataSourceType_DataSourceTypeSync) {
		taskKeyPrefix := "sync_speechcraft"
		taskKey, err := commonv2.GenAsyncTaskKey(taskKeyPrefix)
		if err != nil {
			log.Errorf("生成异步任务Key失败: %v，同步话术库失败", err)
			return &rsp, nil
		}

		syncStateKey := fmt.Sprintf("%s_%d_%d", taskKeyPrefix, corpId, result.Id)

		asyncTaskReq := &commonv2.AsyncTaskReq{
			IsAddTaskList:      true,
			TaskKeyPre:         taskKeyPrefix,
			TaskKey:            taskKey,
			TaskName:           "同步话术库到知识库",
			MaxExecTimeSeconds: 300,
			AppType:            0,
			TaskType:           uint32(commonv2.TaskType_TaskTypeImport),
			SyncStateKey:       syncStateKey,
			Redis:              S.RedisGroup,
		}

		_ = ctx.SetReqHeader("dataset_id", fmt.Sprintf("%d", result.Id))
		_ = ctx.SetReqHeader("corp_id", corpId)
		_ = ctx.SetReqHeader("app_id", appId)

		log.Infof("启动话术库同步任务，dataset_id: %d, corp_id: %d", result.Id, corpId)

		// todo 注意，是否需要事务。
		taskKey, err = commonv2.RunAsyncTaskV3(ctx, asyncTaskReq, task2.SyncSpeechcraftToDatasetTask)
		if err != nil {
			log.Errorf("run sync speechcraft to dataset task failed: %v", err)
			return nil, rpc.CreateErrorWithMsg(rpc.KSystemError, "同步话术库到知识库失败")
		}
		rsp.TaskKey = taskKey
	}

	return &rsp, nil
}

func GetDatasetList(ctx *rpc.Context, req *aiadmin.GetDatasetListReq) (*aiadmin.GetDatasetListRsp, error) {
	var rsp aiadmin.GetDatasetListRsp

	ctx, corpId, _, _, err := GetCorpAndAppIdAndUid(ctx)
	if err != nil {
		return nil, err
	}

	list, paginate, err := modelrag.ModelDataset.ListWithOption(ctx, corpId, req)
	if err != nil {
		return nil, rpc.CreateErrorWithMsg(rpc.KSystemError, "获取知识库列表失败")
	}

	items := make([]*aiadmin.DatasetItem, 0, len(list))
	for _, d := range list {
		items = append(items, &aiadmin.DatasetItem{
			Id:             d.Id,
			Name:           d.Name,
			Description:    d.Description,
			DataSourceType: d.DataSourceType,
			IsPublic:       d.IsPublic,
			IsDefault:      d.IsDefault,
		})
	}

	// 如果没有数据 && 没有筛选条件（说明是第一次访问），则创建默认知识库
	if len(items) == 0 && !hasEffectiveFilter(req.ListOption) {
		// 查询是否已经存在默认知识库
		defaultDatasetExists, err := modelrag.ModelDataset.CheckDefaultExists(ctx, corpId) // 需要实现该方法
		if err != nil {
			log.Errorf("Failed to check default dataset existence: %v", err)
			return nil, rpc.CreateErrorWithMsg(rpc.KSystemError, "检查默认知识库是否存在失败")
		}
		// 如果不存在默认知识库，则创建
		if !defaultDatasetExists {
			collectionName := utils.GenerateMilvusCollectionName(corpId)

			defaultDataset := &aiadmin.ModelDataset{
				CorpId:                 corpId,
				Name:                   "默认知识库",
				Description:            "系统自动创建的默认知识库",
				IsPublic:               true,
				DataSourceType:         uint32(aiadmin.DataSourceType_DataSourceTypeManual),
				EmbeddingModel:         config.DefaultDatasetConfig.EmbeddingModel,
				EmbeddingModelProvider: S.Conf.DefaultEmbeddingProviderID,
				IndexingTechnique:      config.DefaultDatasetConfig.IndexingTechnique,
				IndexStruct:            config.DefaultDatasetConfig.IndexStruct,
				RetrievalModel:         config.DefaultDatasetConfig.RetrievalModel,
				BuiltInFieldEnabled:    config.DefaultDatasetConfig.BuiltInFieldEnabled,
				CollectionName:         collectionName,
				IsDefault:              true,
			}

			result, err := modelrag.ModelDataset.Create(ctx, defaultDataset)
			if err != nil {
				log.Errorf("Failed to create default dataset: %v", err)
				return nil, rpc.CreateErrorWithMsg(rpc.KSystemError, "创建默认知识库失败")
			}

			// 直接把新创建的默认知识库追加到列表里，无需再次查询
			items = append(items, &aiadmin.DatasetItem{
				Id:             result.Id,
				Name:           result.Name,
				Description:    result.Description,
				IsPublic:       result.IsPublic,
				IsDefault:      result.IsDefault,
				DataSourceType: result.DataSourceType,
				CreatedAt:      result.CreatedAt,
			})

			// 分页信息更新
			paginate.Total += 1
		}
	}

	rsp.List = items
	rsp.Paginate = paginate

	return &rsp, nil
}

func UpdateDataset(ctx *rpc.Context, req *aiadmin.UpdateDatasetReq) (*aiadmin.UpdateDatasetRsp, error) {
	var rsp aiadmin.UpdateDatasetRsp

	// 从上下文中获取企业 ID 等信息
	ctx, corpId, _, _, err := GetCorpAndAppIdAndUid(ctx)
	if err != nil {
		return nil, err
	}

	// 检查要更新的知识库是否存在，并验证是否属于当前企业
	dataset, err := modelrag.ModelDataset.GetByCorpIdAndId(ctx, corpId, req.Id)
	if err != nil {
		if modelrag.ModelDataset.IsNotFoundErr(err) {
			return nil, rpc.CreateErrorWithMsg(aiadmin.ErrDatasetNotFound, "知识库不存在或无权限")
		}
		return nil, rpc.CreateErrorWithMsg(rpc.KSystemError, "查询知识库失败")
	}

	// 检查是否存在同名的 Dataset（排除自身）
	if req.Name != "" && req.Name != dataset.Name {
		existingDataset, err := modelrag.ModelDataset.GetByCorpIdAndName(ctx, corpId, req.Name)
		if err != nil && !modelrag.ModelDataset.IsNotFoundErr(err) {
			return nil, rpc.CreateErrorWithMsg(rpc.KSystemError, "查询同名知识库失败")
		}
		// 如果找到了同名的知识库，并且不是正在更新的知识库本身，则返回名称已存在的错误
		if existingDataset != nil && existingDataset.Id != req.Id {
			return nil, rpc.CreateErrorWithMsg(aiadmin.ErrDatasetNameExisted, "知识库名称已存在，请使用其他名称")
		}
	}

	// 构建需要更新的数据 map
	updateData := make(map[string]interface{})
	if req.Name != "" && req.Name != dataset.Name {
		updateData["name"] = req.Name
	}
	if req.Description != "" && req.Description != dataset.Description {
		updateData["description"] = req.Description
	}

	// 如果没有需要更新的数据，则直接返回
	if len(updateData) == 0 {
		return nil, rpc.CreateErrorWithMsg(aiadmin.ErrInvalidRequestData, "未检测到有效的变更数据，请修改后重试")
	}

	// 执行更新操作
	rowsAffected, err := modelrag.ModelDataset.UpdateById(ctx, req.Id, updateData)
	if err != nil {
		return nil, rpc.CreateErrorWithMsg(rpc.KSystemError, "更新知识库失败")
	}
	// 如果更新影响的行数为 0，说明可能没有找到对应的记录
	if rowsAffected == 0 {
		return nil, rpc.CreateErrorWithMsg(aiadmin.ErrInvalidRequestData, "更新成功，但数据未发生变化")
	}

	// 更新成功，返回被更新的知识库 ID
	rsp.Id = req.Id
	return &rsp, nil
}

func DeleteDataset(ctx *rpc.Context, req *aiadmin.DeleteDatasetReq) (*aiadmin.DeleteDatasetRsp, error) {
	var rsp aiadmin.DeleteDatasetRsp

	ctx, corpId, _, _, err := GetCorpAndAppIdAndUid(ctx)
	if err != nil {
		return nil, err
	}

	// todo 考虑被工作流引用问题。

	// 级联删除，这个知识库下的所有FAQ删除，包括向量数据库，用异步的方式
	taskKeyPrefix := "delete_dataset"
	taskKey, err := commonv2.GenAsyncTaskKey(taskKeyPrefix)

	if err != nil {
		log.Errorf("err:%v", err)
		return nil, rpc.CreateErrorWithMsg(rpc.KSystemError, "生成任务失败")
	}

	syncStateKey := fmt.Sprintf(taskKeyPrefix+"_%d_%d_%s", corpId, req.Id, utils2.GenRandomStr())
	asyncTaskReq := &commonv2.AsyncTaskReq{
		IsAddTaskList:      true,
		TaskKeyPre:         taskKeyPrefix,
		MaxExecTimeSeconds: 60,
		AppType:            0,
		TaskType:           uint32(commonv2.TaskType_TaskTypeImport),
		TaskName:           "删除知识库 FAQ 任务",
		Redis:              S.RedisGroup,
		SyncStateKey:       syncStateKey,
		TaskKey:            taskKey,
	}

	_ = ctx.SetReqHeader("dataset_id", fmt.Sprintf("%d", req.Id))
	_ = ctx.SetReqHeader("corp_id", corpId)

	_ = ctx.SetReqHeader("db", S.Conf.Db)

	log.Infof("delete dataset id: %d, corp_id: %d", req.Id, corpId)

	// 内部使用了事务
	taskKey, err = commonv2.RunAsyncTaskV3(ctx, asyncTaskReq, task2.DeleteDatasetFAQTask)
	if err != nil {
		log.Errorf("run delete dataset task failed: %v", err)
		return nil, rpc.CreateErrorWithMsg(rpc.KSystemError, "删除知识库向量失败")
	}

	rsp.Id = req.Id
	rsp.TaskKey = taskKey
	return &rsp, nil
}

func hasEffectiveFilter(opt *core.ListOption) bool {
	if opt == nil || len(opt.Options) == 0 {
		return false
	}
	for _, o := range opt.Options {
		switch o.Type {
		case int32(aiadmin.GetDatasetListReq_ListOptionOrderBy): // 排序字段，不算筛选
			continue
		default:
			if o.Value != "" {
				return true // 其他字段且有值，算有效筛选
			}
		}
	}
	return false
}

func CheckDatasetUsage(ctx *rpc.Context, req *aiadmin.CheckDatasetUsageReq) (*aiadmin.CheckDatasetUsageRsp, error) {
	var rsp aiadmin.CheckDatasetUsageRsp

	// 获取当前请求的 corpId
	ctx, corpId, _, _, err := GetCorpAndAppIdAndUid(ctx)
	if err != nil {
		return nil, err
	}

	// 调用 ModelWorkflowDatasetUsage.ListByDatasetId 查询该 datasetId 对应的所有使用记录
	usages, err := modelworkflow.ModelWorkflowDatasetUsage.ListByDatasetId(ctx, req.DatasetId)
	if err != nil {
		log.Errorf("CheckDatasetUsage: 查询数据集 %d 的使用情况失败: %v", req.DatasetId, err)
		return nil, rpc.CreateErrorWithMsg(rpc.KSystemError, "查询数据集使用情况失败")
	}

	// 设置 rsp.Used 为 len(usages) > 0
	rsp.Used = len(usages) > 0

	// 如果数据集被使用，收集使用详情
	if rsp.Used {
		// 收集所有引用到的 WorkflowId 并进行去重
		workflowIds := make(map[string]bool)
		for _, usage := range usages {
			workflowIds[usage.WorkflowId] = true
		}

		// 将去重后的 WorkflowId 转换为切片
		uniqueWorkflowIds := make([]string, 0, len(workflowIds))
		for id := range workflowIds {
			uniqueWorkflowIds = append(uniqueWorkflowIds, id)
		}

		// 存储工作流信息的映射
		workflowInfoMap := make(map[string]*aiadmin.ModelWorkflow)

		// 查询每个工作流的信息
		for _, workflowId := range uniqueWorkflowIds {
			workflow, err := modelworkflow.ModelWorkflow.GetById(ctx, workflowId)
			if err != nil {
				log.Warnf("CheckDatasetUsage: 获取工作流 %s 信息失败: %v", workflowId, err)
				continue
			}
			if workflow != nil && workflow.CorpId == corpId { // 确保只返回当前企业的工作流
				workflowInfoMap[workflowId] = workflow
			}
		}

		// 构造使用明细列表
		rsp.Usages = make([]*aiadmin.DatasetUsageItem, 0, len(usages))
		for _, usage := range usages {
			workflow, exists := workflowInfoMap[usage.WorkflowId]
			if !exists {
				continue // 跳过找不到工作流信息的记录
			}

			usageItem := &aiadmin.DatasetUsageItem{
				WorkflowId:        usage.WorkflowId,
				WorkflowVersionId: usage.WorkflowVersionId,
				NodeId:            usage.NodeId,
				NodeType:          usage.NodeType,
				WorkflowName:      workflow.Name,
				// 可以添加 WorkflowStatus 字段，如果 DatasetUsageItem 结构体中有该字段
			}
			rsp.Usages = append(rsp.Usages, usageItem)
		}
	}

	return &rsp, nil
}
