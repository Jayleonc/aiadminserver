package impl

import (
	"fmt"
	"git.pinquest.cn/base/aiadmin"
	"git.pinquest.cn/base/aiadminserver/impl/internal/model/modelrag"
	"git.pinquest.cn/base/aiadminserver/impl/internal/rag/task"
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/brick/utils"
	"git.pinquest.cn/qlb/commonv2"
)

func AddFAQCategory(ctx *rpc.Context, req *aiadmin.AddFAQCategoryReq) (*aiadmin.AddFAQCategoryRsp, error) {
	var rsp aiadmin.AddFAQCategoryRsp
	ctx, corpId, _, _, err := GetCorpAndAppIdAndUid(ctx)
	if err != nil {
		return nil, err
	}

	var id uint64
	if id, err = modelrag.ModelFAQCategory.Insert(ctx, &aiadmin.ModelFAQCategory{
		CorpId:      corpId,
		DatasetId:   req.DatasetId,
		Name:        req.Name,
		Description: req.Description,
	}); err != nil {
		return nil, rpc.CreateErrorWithMsg(rpc.KSystemError, "创建 FAQ 分类失败")
	}

	rsp.Id = id
	return &rsp, nil
}

func GetFAQCategoryList(ctx *rpc.Context, req *aiadmin.GetFAQCategoryListReq) (*aiadmin.GetFAQCategoryListRsp, error) {
	var rsp aiadmin.GetFAQCategoryListRsp
	ctx, corpId, _, _, err := GetCorpAndAppIdAndUid(ctx)
	if err != nil {
		return nil, err
	}

	list, err := modelrag.ModelFAQCategory.List(ctx, corpId, req.DatasetId)
	if err != nil {
		return nil, rpc.CreateErrorWithMsg(rpc.KSystemError, "查询 FAQ 分类失败")
	}

	rsp.List = list
	return &rsp, nil
}

func DeleteFAQCategory(ctx *rpc.Context, req *aiadmin.DeleteFAQCategoryReq) (*aiadmin.DeleteFAQCategoryRsp, error) {
	var rsp aiadmin.DeleteFAQCategoryRsp
	ctx, corpId, _, _, err := GetCorpAndAppIdAndUid(ctx)
	if err != nil {
		return nil, err
	}

	taskKeyPrefix := "delete_faq_category"
	taskKey, err := commonv2.GenAsyncTaskKey(taskKeyPrefix)

	if err != nil {
		log.Errorf("err:%v", err)
		return nil, rpc.CreateErrorWithMsg(rpc.KSystemError, "生成任务失败")
	}

	syncStateKey := fmt.Sprintf(taskKeyPrefix+"_%d_%d_%s", corpId, req.DatasetId, utils.GenRandomStr())
	asyncTaskReq := &commonv2.AsyncTaskReq{
		IsAddTaskList:      true,
		TaskKeyPre:         taskKeyPrefix,
		MaxExecTimeSeconds: 60,
		AppType:            0,
		TaskType:           uint32(commonv2.TaskType_TaskTypeImport),
		TaskName:           "删除 FAQ 分类向量任务",
		Redis:              S.RedisGroup,
		SyncStateKey:       syncStateKey,
		TaskKey:            taskKey,
	}

	_ = ctx.SetReqHeader("corp_id", corpId)
	_ = ctx.SetReqHeader("dataset_id", req.DatasetId)
	_ = ctx.SetReqHeader("category_id", req.Id)
	_ = ctx.SetReqHeader("db", S.Conf.Db)

	// 内部使用了事务
	taskKey, err = commonv2.RunAsyncTaskV3(ctx, asyncTaskReq, task.DeleteFAQCategoryTask)
	if err != nil {
		log.Errorf("run delete faq category task failed:%v", err)
		return nil, rpc.CreateErrorWithMsg(rpc.KSystemError, "删除 FAQ 分类向量失败")
	}

	rsp.TaskKey = taskKey

	return &rsp, nil
}

// UpdateFAQCategory 更新名称
func UpdateFAQCategory(ctx *rpc.Context, req *aiadmin.UpdateFAQCategoryReq) (*aiadmin.UpdateFAQCategoryRsp, error) {
	var rsp aiadmin.UpdateFAQCategoryRsp
	ctx, corpId, _, _, err := GetCorpAndAppIdAndUid(ctx)
	if err != nil {
		return nil, err
	}

	if err := modelrag.ModelFAQCategory.UpdateById(ctx, corpId, req.Id, map[string]interface{}{
		"name": req.Name,
	}); err != nil {
		return nil, rpc.CreateErrorWithMsg(rpc.KSystemError, "更新 FAQ 分类失败")
	}
	return &rsp, nil
}
