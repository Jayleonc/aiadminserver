package task

import (
	"context"
	"fmt"
	"git.pinquest.cn/base/aiadmin"
	modelrag2 "git.pinquest.cn/base/aiadminserver/impl/internal/model/modelrag"
	"git.pinquest.cn/base/aiadminserver/impl/internal/rag/indexer"
	"git.pinquest.cn/base/aiadminserver/impl/internal/rag/service"
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/commonv2"
	einoschema "github.com/cloudwego/eino/schema"
	"strconv"
)

// EmbedFAQTask 处理单个 FAQ 的向量化任务
func EmbedFAQTask(ctx *rpc.Context) (*commonv2.AsyncTaskResult, error) {
	result := &commonv2.AsyncTaskResult{}
	result.State = uint32(commonv2.AsyncTaskState_AsyncTaskStateFailure) // 默认失败

	faqIdStr := ctx.GetReqHeader("faq_id")
	datasetIdStr := ctx.GetReqHeader("dataset_id")
	corpIdStr := ctx.GetReqHeader("corp_id")

	faqId, err := strconv.ParseUint(faqIdStr, 10, 64)
	if err != nil {
		log.Errorf("parse faq_id failed: %v", err)
		result.Error = &commonv2.ErrMsg{ErrCode: rpc.KErrRequestBodyReadFail, ErrMsg: fmt.Sprintf("parse faq_id failed, %v", err)}
		return result, err
	}

	datasetId, err := strconv.ParseUint(datasetIdStr, 10, 64)
	if err != nil {
		log.Errorf("parse dataset_id failed: %v", err)
		result.Error = &commonv2.ErrMsg{ErrCode: rpc.KErrRequestBodyReadFail, ErrMsg: fmt.Sprintf("parse dataset_id failed, %v", err)}
		return result, err
	}

	corpId, err := strconv.ParseUint(corpIdStr, 10, 32)
	if err != nil {
		log.Errorf("parse corp_id failed: %v", err)
		result.Error = &commonv2.ErrMsg{ErrCode: rpc.KErrRequestBodyReadFail, ErrMsg: fmt.Sprintf("parse corp_id failed, %v", err)}
		return result, err
	}

	// 1. 获取 FAQ 记录
	faq, err := modelrag2.ModelFAQ.GetById(ctx, faqId)
	if err != nil {
		log.Errorf("get faq by id failed: %v", err)
		result.Error = &commonv2.ErrMsg{ErrCode: rpc.KSystemError, ErrMsg: fmt.Sprintf("get faq by id failed, %v", err)}
		return result, err
	}

	// 2. 获取 Dataset 信息以获取 Collection Name
	dataset, err := modelrag2.ModelDataset.GetByCorpIdAndId(ctx, uint32(corpId), datasetId)
	if err != nil {
		log.Errorf("get dataset by id failed: %v", err)
		result.Error = &commonv2.ErrMsg{ErrCode: rpc.KSystemError, ErrMsg: fmt.Sprintf("get dataset by id failed, %v", err)}
		return result, err
	}
	collectionName := dataset.CollectionName

	// 3. 将 FAQ 转换为 eino Document
	doc := indexer.ConvertFAQToEinoDoc(faq)

	ctx1 := context.Background()

	// 4. 删除旧向量 (如果存在)
	if err := service.DeleteMilvusVectors(ctx1, collectionName, []string{doc.ID}); err != nil {
		log.Warnf("delete old vector for faq %d failed: %v", faq.Id, err)
		// 不阻塞后续操作
	}

	// 5. 存储新的向量
	ids, err := service.StoreMilvusVectors(ctx1, collectionName, []*einoschema.Document{doc})
	if err != nil {
		log.Errorf("store faq to milvus failed: %v", err)
		_ = modelrag2.ModelFAQ.UpdateStatus(ctx, faq.Id, uint32(aiadmin.FAQStatus_FAQStatusFailed), err.Error())
		result.Error = &commonv2.ErrMsg{
			ErrCode: rpc.KSystemError,
			ErrMsg:  fmt.Sprintf("store to milvus failed, %v", err),
		}
		return result, err
	}

	// 6. 检查向量是否存储成功
	if len(ids) == 0 || ids[0] != doc.ID {
		msg := "milvus returned wrong or empty ID"
		log.Errorf(msg)
		_ = modelrag2.ModelFAQ.UpdateStatus(ctx, faq.Id, uint32(aiadmin.FAQStatus_FAQStatusFailed), msg)
		result.Error = &commonv2.ErrMsg{
			ErrCode: rpc.KSystemError,
			ErrMsg:  msg,
		}
		return result, fmt.Errorf(msg)
	}

	// 7. 成功，更新状态
	err = modelrag2.ModelFAQ.UpdateStatus(ctx, faq.Id, uint32(aiadmin.FAQStatus_FAQStatusCompleted), "")
	if err != nil {
		log.Errorf("update faq status failed: %v, faqId: %v", err, faq.Id)
		result.Error = &commonv2.ErrMsg{
			ErrCode: rpc.KSystemError,
			ErrMsg:  fmt.Sprintf("update faq status failed, %v", err),
		}
		return result, err
	}

	result.State = uint32(commonv2.AsyncTaskState_AsyncTaskStateSuccess)
	result.Buf = "success"
	return result, nil
}
