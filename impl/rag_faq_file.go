package impl

import (
	"fmt"
	"git.pinquest.cn/base/aiadminserver/impl/internal/model/modelrag"
	task2 "git.pinquest.cn/base/aiadminserver/impl/internal/rag/task"
	"net/url"
	"path/filepath"
	"strings"
	"time"

	"git.pinquest.cn/base/aiadmin"
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/brick/utils"
	"git.pinquest.cn/qlb/commonv2"
)

func FAQImportExcel(ctx *rpc.Context, req *aiadmin.FAQImportExcelReq) (*aiadmin.FAQImportExcelRsp, error) {
	var rsp aiadmin.FAQImportExcelRsp

	// 1. 获取 corp_id 和 robot_uid（从 ctx 中获取）
	ctx, corpId, appId, uid, err := GetCorpAndAppIdAndUid(ctx)
	if err != nil {
		return nil, err
	}
	if req.DatasetId == 0 {
		return nil, rpc.CreateErrorWithMsg(rpc.KErrRequestBodyReadFail, "dataset_id is empty")
	}

	var file aiadmin.ModelFile
	file.Id = 16

	req.FileExtension = filepath.Ext(req.FileName)

	_, err = url.Parse(req.FileUrl)
	if err != nil {
		return nil, rpc.CreateErrorWithMsg(rpc.KErrRequestBodyReadFail, "无效的 file_url")
	}

	// 3. 写入 rag_files 表
	file = aiadmin.ModelFile{
		CorpId:        corpId,
		RobotUid:      uint32(uid),
		FileName:      req.FileName,
		FileUrl:       req.FileUrl,
		FileSize:      req.FileSize,
		FileExtension: req.FileExtension,
		Status:        uint32(aiadmin.FAQStatus_FAQStatusPending),
		CreatedAt:     uint32(time.Now().Unix()),
		UpdatedAt:     uint32(time.Now().Unix()),
	}

	if err := modelrag.ModelFile.Insert(ctx, &file); err != nil {
		log.Errorf("insert file failed: %v, corpId: %d, fileName: %s, fileUrl: %s, fileSize: %d, fileExtension: %s", err, corpId, req.FileName, req.FileUrl, req.FileSize, req.FileExtension)
		return nil, err
	}

	taskKeyPrefix := "ai_rag_file_import"
	taskKey, err := commonv2.GenAsyncTaskKey(taskKeyPrefix)

	if err != nil {
		log.Errorf("gen async task key failed: %v", err)
		return nil, rpc.CreateErrorWithMsg(rpc.KSystemError, "生成异步任务失败")
	}

	syncStateKey := fmt.Sprintf(taskKeyPrefix+"_%d_%d_%s", corpId, appId, utils.GenRandomStr())
	asyncTaskReq := &commonv2.AsyncTaskReq{
		IsAddTaskList:      true,
		TaskKeyPre:         taskKeyPrefix,
		MaxExecTimeSeconds: 60,
		AppType:            0,
		TaskType:           uint32(commonv2.TaskType_TaskTypeImport),
		TaskName:           "FAQ 文件导入任务",
		Redis:              S.RedisGroup,
		SyncStateKey:       syncStateKey,
		TaskKey:            taskKey,
	}

	_ = ctx.SetReqHeader("file_id", file.Id)
	_ = ctx.SetReqHeader("dataset_id", req.DatasetId)
	_ = ctx.SetReqHeader("local_path", req.LocalPath)
	_ = ctx.SetReqHeader("corp_id", corpId)
	_ = ctx.SetReqHeader("db", S.Conf.Db)
	log.Infof("file_id: %d, dataset_id: %d, local_path: %s, corp_id: %d, db: %s", file.Id, req.DatasetId, req.LocalPath, corpId, S.Conf.Db)

	taskKey, err = commonv2.RunAsyncTaskV3(ctx, asyncTaskReq, task2.ExecFileFAQTask)
	if err != nil {
		log.Errorf("run exec file faq task failed: %v", err)
		return nil, rpc.CreateErrorWithMsg(rpc.KSystemError, "文件导入失败")
	}

	// 5. 返回结果
	rsp.Message = "文件已记录，FAQ 将自动导入"
	rsp.TaskKey = taskKey

	return &rsp, nil
}

func CheckFAQFileFormatTask(ctx *rpc.Context, req *aiadmin.CheckFAQFileFormatReq) (*aiadmin.CheckFAQFileFormatRsp, error) {
	ctx, corpId, appId, _, err := GetCorpAndAppIdAndUid(ctx)
	if err != nil {
		return nil, err
	}

	req.FileExtension = req.FileName[strings.LastIndex(req.FileName, "."):]

	taskKeyPrefix := "check_faq_file_format"
	taskKey, err := commonv2.GenAsyncTaskKey(taskKeyPrefix)
	if err != nil {
		return nil, err
	}

	syncStateKey := fmt.Sprintf("%s_%d_%d_%s", taskKeyPrefix, corpId, appId, utils.GenRandomStr())

	asyncTaskReq := &commonv2.AsyncTaskReq{
		IsAddTaskList:      true,
		TaskKeyPre:         taskKeyPrefix,
		MaxExecTimeSeconds: 60,
		AppType:            0,
		TaskType:           uint32(commonv2.TaskType_TaskTypeImport),
		TaskName:           "校验FAQ上传文件格式",
		Redis:              S.RedisGroup,
		SyncStateKey:       syncStateKey,
		TaskKey:            taskKey,
	}

	// 塞请求参数
	_ = ctx.SetReqHeader("file_url", req.FileUrl)
	_ = ctx.SetReqHeader("file_name", req.FileName)
	_ = ctx.SetReqHeader("file_extension", req.FileExtension)
	log.Infof("file_url: %s, file_name: %s, file_extension: %s", req.FileUrl, req.FileName, req.FileExtension)

	taskKey, err = commonv2.RunAsyncTaskV3(ctx, asyncTaskReq, task2.CheckFAQFormatTask)
	if err != nil {
		log.Errorf("run check faq format task failed: %v", err)
		return nil, rpc.CreateErrorWithMsg(rpc.KSystemError, fmt.Sprintf("文件格式校验失败, %v", err))
	}

	return &aiadmin.CheckFAQFileFormatRsp{
		TaskKey: taskKey,
	}, nil

}
