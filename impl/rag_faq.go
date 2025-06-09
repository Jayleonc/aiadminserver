package impl

import (
	"fmt"
	modelrag2 "git.pinquest.cn/base/aiadminserver/impl/internal/model/modelrag"
	task2 "git.pinquest.cn/base/aiadminserver/impl/internal/rag/task"
	"git.pinquest.cn/base/aiadminserver/impl/pkg/utils"
	"git.pinquest.cn/qlb/brick/dbproxy"
	"git.pinquest.cn/qlb/brick/dbx"
	"time"

	"git.pinquest.cn/base/aiadmin"
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/rpc"
	utils2 "git.pinquest.cn/qlb/brick/utils"
	"git.pinquest.cn/qlb/commonv2"
	"git.pinquest.cn/qlb/quan"
)

func DeleteFAQ(ctx *rpc.Context, req *aiadmin.DeleteFAQReq) (*aiadmin.DeleteFAQRsp, error) {
	var rsp aiadmin.DeleteFAQRsp
	ctx, corpId, _, _, err := GetCorpAndAppIdAndUid(ctx)
	if err != nil {
		return nil, err
	}

	// 查询 FAQ 信息
	faq, err := modelrag2.ModelFAQ.GetById(ctx, req.Id)
	if err != nil {
		return nil, rpc.CreateErrorWithMsg(rpc.KSystemError, "查询 FAQ 失败")
	}

	if faq == nil {
		log.Errorf("faq not found")
		return nil, rpc.CreateErrorWithMsg(rpc.KSystemError, "FAQ 不存在")
	}

	// 事务操作
	scope := modelrag2.ModelFAQ.NewScope()
	err = scope.OnTransaction(ctx, func(txCtx *rpc.Context, tx *dbx.Tx) error {
		if err := modelrag2.ModelFAQ.DeleteByIdInTx(txCtx, tx, corpId, req.Id); err != nil {
			return fmt.Errorf("删除 FAQ 失败: %w", err)
		}
		if err := modelrag2.ModelFaqManualMedia.DeleteByFaqIDInTx(txCtx, tx, req.Id); err != nil {
			return fmt.Errorf("删除 FAQ 关联素材失败: %w", err)
		}
		return nil
	}, dbproxy.OptionWithUseDb(S.Conf.Db), dbproxy.OptionWithUseBiz(true))

	taskKeyPrefix := "ai_rag_delete_faq"
	taskKey, err := commonv2.GenAsyncTaskKey(taskKeyPrefix)

	if err != nil {
		log.Errorf("gen async task key failed: %v", err)
		return nil, rpc.CreateErrorWithMsg(rpc.KSystemError, "生成任务失败")
	}

	syncStateKey := fmt.Sprintf(taskKeyPrefix+"_%d_%d_%s", corpId, req.Id, utils2.GenRandomStr())
	asyncTaskReq := &commonv2.AsyncTaskReq{
		IsAddTaskList:      true,
		TaskKeyPre:         taskKeyPrefix,
		MaxExecTimeSeconds: 60,
		AppType:            0,
		TaskType:           uint32(commonv2.TaskType_TaskTypeImport),
		TaskName:           "删除单个 FAQ 向量任务",
		Redis:              S.RedisGroup,
		SyncStateKey:       syncStateKey,
		TaskKey:            taskKey,
	}

	_ = ctx.SetReqHeader("embedding_id", faq.EmbeddingId)
	_ = ctx.SetReqHeader("dataset_id", faq.DatasetId)
	_ = ctx.SetReqHeader("corp_id", corpId)

	log.Infof("embedding_id: %s, dataset_id: %d, corp_id: %d", faq.EmbeddingId, faq.DatasetId, corpId)

	taskKey, err = commonv2.RunAsyncTaskV3(ctx, asyncTaskReq, task2.DeleteVectorTask)
	if err != nil {
		log.Errorf("run delete vector task failed: %v", err)
		return &rsp, rpc.CreateErrorWithMsg(rpc.KSystemError, fmt.Sprintf("删除 FAQ 向量失败: %v", err))
	}

	rsp.TaskKey = taskKey
	return &rsp, nil
}

func UpdateFAQ(ctx *rpc.Context, req *aiadmin.UpdateFAQReq) (*aiadmin.UpdateFAQRsp, error) {
	var rsp aiadmin.UpdateFAQRsp
	ctx, corpId, _, uid, err := GetCorpAndAppIdAndUid(ctx)
	if err != nil {
		return nil, err
	}

	faq, err := modelrag2.ModelFAQ.GetByDatasetIdAndId(ctx, corpId, req.DatasetId, req.Id)
	if err != nil {
		return nil, rpc.CreateErrorWithMsg(rpc.KSystemError, "查询 FAQ 失败")
	}
	if faq == nil {
		return nil, rpc.CreateErrorWithMsg(rpc.KSystemError, "FAQ 不存在")
	}

	q := req.Question
	a := req.Answer
	if q == "" && a == "" {
		return nil, rpc.CreateErrorWithMsg(rpc.KErrRequestBodyReadFail, "问题和答案不能都为空")
	}

	faqType := aiadmin.FAQType_FAQTypeQa
	if q == "" {
		faqType = aiadmin.FAQType_FAQTypeQuestionOnly
	} else if a == "" {
		faqType = aiadmin.FAQType_FAQTypeParagraph
	}

	contentHash := utils.CalcHash(fmt.Sprintf("%d_%s", req.CategoryId, q))
	hasManualMedia := len(req.ManualMedia) > 0

	// ✅ 使用事务封装所有 DB 操作
	scope := modelrag2.ModelFAQ.NewScope()
	err = scope.OnTransaction(ctx, func(txCtx *rpc.Context, tx *dbx.Tx) error {
		// 仅删除旧素材绑定，但不删除素材，因为素材有可能被其他 FAQ 引用（当前不可能，还没有做素材复用）
		if err := modelrag2.ModelFaqManualMedia.DeleteByFaqIDInTx(txCtx, tx, faq.Id); err != nil {
			return fmt.Errorf("delete faq_manual_media failed: %w", err)
		}

		// 创建新素材并绑定
		for _, media := range req.ManualMedia {
			if media.MsgFileUrl == "" || media.MsgFileName == "" {
				continue
			}
			if !isAllowedManualMediaType(media.Type) {
				continue
			}

			var materialId uint64
			if media.Id != 0 {
				m, err := modelrag2.ModelManualMaterial.GetByID(txCtx, media.Id)
				if err != nil {
					log.Warnf("material not found: %v", err)
					continue
				}
				materialId = m.Id
			} else {
				mat := &aiadmin.ModelManualMaterial{
					CorpId:    corpId,
					RobotUid:  uint32(uid),
					FileName:  media.MsgFileName,
					FileUrl:   media.MsgFileUrl,
					FileSize:  uint64(media.MsgFileSize),
					Type:      media.Type,
					CreatedAt: uint32(time.Now().Unix()),
					UpdatedAt: uint32(time.Now().Unix()),
				}
				if err := modelrag2.ModelManualMaterial.CreateInTx(txCtx, tx, mat); err != nil {
					log.Errorf("create manual material failed: %v, corpId: %d, faqId: %d, media: %v", err, corpId, faq.Id, media)
					continue
				}
				materialId = mat.Id
			}

			link := &aiadmin.ModelFAQManualMedia{
				FaqId:      faq.Id,
				MaterialId: materialId,
				CreatedAt:  uint32(time.Now().Unix()),
				UpdatedAt:  uint32(time.Now().Unix()),
			}
			if err := modelrag2.ModelFaqManualMedia.CreateInTx(txCtx, tx, link); err != nil {
				log.Errorf("create faq_manual_media failed: %v, corpId: %d, faqId: %d, materialId: %d", err, corpId, faq.Id, materialId)
				continue
			}
		}

		updateMap := map[string]interface{}{
			"question":         q,
			"answer":           a,
			"category_id":      req.CategoryId,
			"enabled":          req.Enabled,
			"faq_type":         faqType,
			"content_hash":     contentHash,
			"has_manual_media": hasManualMedia,
			"updated_at":       uint32(time.Now().Unix()),
			"status":           aiadmin.FAQStatus_FAQStatusPending, // 新更新的，标记为待处理
		}

		return modelrag2.ModelFAQ.UpdateByIdInTx(txCtx, tx, corpId, req.Id, updateMap)
	}, dbproxy.OptionWithUseDb(S.Conf.Db), dbproxy.OptionWithUseBiz(true))
	if err != nil {
		log.Errorf("update faq failed: %v, corpId: %d, faqId: %d", err, corpId, req.Id)
		return nil, rpc.CreateErrorWithMsg(rpc.KSystemError, fmt.Sprintf("FAQ 更新失败：%v", err))
	}

	// 异步向量任务：更新向量库
	taskKeyPrefix := "ai_rag_faq_update"
	taskKey, err := commonv2.GenAsyncTaskKey(taskKeyPrefix)
	if err != nil {
		log.Errorf("gen async task key failed: %v", err)
		return nil, rpc.CreateErrorWithMsg(rpc.KSystemError, "生成异步任务失败")
	}

	asyncTaskReq := &commonv2.AsyncTaskReq{
		IsAddTaskList:      true,
		TaskKeyPre:         taskKeyPrefix,
		MaxExecTimeSeconds: 60,
		AppType:            0,
		TaskType:           uint32(commonv2.TaskType_TaskTypeImport),
		TaskName:           "更新 FAQ 向量化任务",
		Redis:              S.RedisGroup,
		SyncStateKey:       fmt.Sprintf("%s_%d_%d_%s", taskKeyPrefix, corpId, req.Id, utils2.GenRandomStr()),
		TaskKey:            taskKey,
	}

	_ = ctx.SetReqHeader("faq_id", fmt.Sprintf("%d", faq.Id))
	_ = ctx.SetReqHeader("dataset_id", fmt.Sprintf("%d", faq.DatasetId))
	_ = ctx.SetReqHeader("corp_id", fmt.Sprintf("%d", corpId))
	log.Infof("faq_id: %d, dataset_id: %d, corp_id: %d", faq.Id, faq.DatasetId, corpId)

	taskKey, err = commonv2.RunAsyncTaskV3(ctx, asyncTaskReq, task2.EmbedFAQTask)
	if err != nil {
		log.Errorf("run embed faq task failed: %v", err)
		return nil, rpc.CreateErrorWithMsg(rpc.KSystemError, "向量更新任务执行失败")
	}

	rsp.TaskKey = taskKey
	return &rsp, nil
}

func isAllowedManualMediaType(t int32) bool {
	switch t {
	case int32(quan.ExtraMsgType_ExtraMsgTypeImage),
		int32(quan.ExtraMsgType_ExtraMsgTypeVideo),
		int32(quan.ExtraMsgType_ExtraMsgTypeFile),
		int32(quan.ExtraMsgType_ExtraMsgTypeVoice):
		return true
	default:
		return false
	}
}

func GetFAQList(ctx *rpc.Context, req *aiadmin.GetFAQListReq) (*aiadmin.GetFAQListRsp, error) {
	var rsp aiadmin.GetFAQListRsp

	ctx, corpId, _, _, err := GetCorpAndAppIdAndUid(ctx)
	if err != nil {
		return nil, err
	}

	list, paginate, err := modelrag2.ModelFAQ.ListWithOption(ctx, corpId, req)
	if err != nil {
		return nil, rpc.CreateErrorWithMsg(rpc.KSystemError, "查询 FAQ 列表失败")
	}

	items := make([]*aiadmin.FAQItem, 0, len(list))
	for _, f := range list {
		item := &aiadmin.FAQItem{
			Id:         f.Id,
			Question:   f.Question,
			Answer:     f.Answer,
			CategoryId: f.CategoryId,
			FaqType:    f.FaqType,
			Enabled:    f.Enabled,
			CreatedAt:  f.CreatedAt,
			UpdatedAt:  f.UpdatedAt,
		}

		if f.HasManualMedia {
			mediaList, err := modelrag2.ModelFaqManualMedia.ListByFaqID(ctx, f.Id)
			if err != nil {
				log.Errorf("get faq manual media list failed, faq_id=%d, err=%v", f.Id, err)
			}

			for _, m := range mediaList {
				material, err := modelrag2.ModelManualMaterial.GetByID(ctx, m.MaterialId)
				if err != nil {
					log.Errorf("get manual material failed, id=%d, err=%v", m.MaterialId, err)
					continue
				}
				item.ManualMedia = append(item.ManualMedia, &aiadmin.MaterialInfo{
					Type:        material.Type,
					MsgFileName: material.FileName,
					MsgFileUrl:  material.FileUrl,
					MsgFileSize: uint32(material.FileSize),
				})
			}
		}
		items = append(items, item)
	}

	rsp.List = items
	rsp.Paginate = paginate
	return &rsp, nil
}

func AddFAQ(ctx *rpc.Context, req *aiadmin.AddFAQReq) (*aiadmin.AddFAQRsp, error) {
	var rsp aiadmin.AddFAQRsp

	ctx, corpId, _, uid, err := GetCorpAndAppIdAndUid(ctx)
	if err != nil {
		return nil, err
	}

	q := req.Question
	a := req.Answer

	if q == "" && a == "" {
		return nil, rpc.CreateErrorWithMsg(rpc.KErrRequestBodyReadFail, "问题和答案不能都为空")
	}

	faqType := aiadmin.FAQType_FAQTypeQa
	if q != "" && a == "" {
		faqType = aiadmin.FAQType_FAQTypeQuestionOnly
	} else if q == "" && a != "" {
		faqType = aiadmin.FAQType_FAQTypeParagraph
	}

	contentHash := utils.CalcHash(fmt.Sprintf("%d_%s", req.CategoryId, q))
	hasManualMedia := len(req.ManualMedia) > 0

	faq := &aiadmin.ModelFAQ{
		CorpId:         corpId,
		RobotUid:       uint32(uid),
		DatasetId:      req.DatasetId,
		CategoryId:     req.CategoryId,
		Question:       q,
		Answer:         a,
		CreateMethod:   req.CreateMethod,
		Enabled:        true,
		HasManualMedia: hasManualMedia,
		FaqType:        uint32(faqType),
		ContentHash:    contentHash,
		Status:         uint32(aiadmin.FAQStatus_FAQStatusPending),
		CreatedAt:      uint32(time.Now().Unix()),
		UpdatedAt:      uint32(time.Now().Unix()),
	}

	scope := modelrag2.ModelFAQ.NewScope()
	err = scope.OnTransaction(ctx, func(txCtx *rpc.Context, tx *dbx.Tx) error {
		if err := modelrag2.ModelFAQ.CreateInTx(txCtx, tx, faq); err != nil {
			return fmt.Errorf("创建 FAQ 失败: %w", err)
		}

		for _, mediaInfo := range req.ManualMedia {
			if mediaInfo.MsgFileUrl == "" || mediaInfo.MsgFileName == "" {
				continue
			}
			if !isAllowedManualMediaType(mediaInfo.Type) {
				return fmt.Errorf("invalid manual media type: %v", mediaInfo.Type)
			}

			material := &aiadmin.ModelManualMaterial{
				CorpId:    corpId,
				RobotUid:  uint32(uid),
				FileName:  mediaInfo.MsgFileName,
				FileUrl:   mediaInfo.MsgFileUrl,
				Type:      mediaInfo.Type,
				FileSize:  uint64(mediaInfo.MsgFileSize),
				CreatedAt: uint32(time.Now().Unix()),
				UpdatedAt: uint32(time.Now().Unix()),
			}
			if err := modelrag2.ModelManualMaterial.CreateInTx(txCtx, tx, material); err != nil {
				log.Errorf("create manual material failed: %v, corpId: %d, faqId: %d, media: %v", err, corpId, faq.Id, mediaInfo)
				return fmt.Errorf("创建手动素材失败: %w", err)
			}

			link := &aiadmin.ModelFAQManualMedia{
				FaqId:      faq.Id,
				MaterialId: material.Id,
				CreatedAt:  uint32(time.Now().Unix()),
				UpdatedAt:  uint32(time.Now().Unix()),
			}
			if err := modelrag2.ModelFaqManualMedia.CreateInTx(txCtx, tx, link); err != nil {
				log.Errorf("create faq_manual_media failed: %v, corpId: %d, faqId: %d, materialId: %d", err, corpId, faq.Id, material.Id)
				return fmt.Errorf("创建素材关联失败: %w", err)
			}
		}
		return nil
	}, dbproxy.OptionWithUseDb(S.Conf.Db), dbproxy.OptionWithUseBiz(true))
	if err != nil {
		log.Errorf("AddFAQ 事务失败: %v", err)
		return nil, rpc.CreateErrorWithMsg(rpc.KSystemError, "新增 FAQ 失败")
	}

	rsp.FaqId = faq.Id

	// 异步进行向量化
	taskKeyPrefix := "ai_rag_faq_add"
	taskKey, err := commonv2.GenAsyncTaskKey(taskKeyPrefix)
	if err != nil {
		log.Errorf("生成异步任务失败: %v", err)
		return nil, err
	}

	syncStateKey := fmt.Sprintf(taskKeyPrefix+"_%d_%d_%s", corpId, uid, utils2.GenRandomStr())
	asyncTaskReq := &commonv2.AsyncTaskReq{
		IsAddTaskList:      true,
		TaskKeyPre:         taskKeyPrefix,
		MaxExecTimeSeconds: 60,
		AppType:            0,
		TaskType:           uint32(commonv2.TaskType_TaskTypeImport),
		TaskName:           "增加 FAQ 向量化任务",
		Redis:              S.RedisGroup,
		SyncStateKey:       syncStateKey,
		TaskKey:            taskKey,
	}

	_ = ctx.SetReqHeader("faq_id", fmt.Sprintf("%d", faq.Id))
	_ = ctx.SetReqHeader("dataset_id", fmt.Sprintf("%d", req.DatasetId))
	_ = ctx.SetReqHeader("corp_id", fmt.Sprintf("%d", corpId))
	log.Infof("faq_id: %d, dataset_id: %d, corp_id: %d", faq.Id, req.DatasetId, corpId)

	taskKey, err = commonv2.RunAsyncTaskV3(ctx, asyncTaskReq, task2.EmbedFAQTask)
	if err != nil {
		log.Errorf("run embed faq task failed: %v", err)
		return nil, rpc.CreateErrorWithMsg(rpc.KSystemError, "FAQ 向量化任务失败")
	}

	rsp.TaskKey = taskKey
	return &rsp, nil
}
