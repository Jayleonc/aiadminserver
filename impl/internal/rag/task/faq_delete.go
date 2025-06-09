package task

import (
	"context"
	"errors"
	"fmt"
	modelrag2 "git.pinquest.cn/base/aiadminserver/impl/internal/model/modelrag"
	"git.pinquest.cn/base/aiadminserver/impl/internal/rag/service"
	"git.pinquest.cn/qlb/brick/dbproxy"
	"strconv"

	"git.pinquest.cn/qlb/brick/dbx"

	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/commonv2"
)

func DeleteDatasetFAQTask(ctx *rpc.Context) (*commonv2.AsyncTaskResult, error) {
	result := &commonv2.AsyncTaskResult{}

	db := ctx.GetReqHeader("db")

	datasetIdStr := ctx.GetReqHeader("dataset_id")
	corpIdStr := ctx.GetReqHeader("corp_id")
	if datasetIdStr == "" || corpIdStr == "" {
		result.Error = &commonv2.ErrMsg{
			ErrCode: rpc.KErrRequestBodyReadFail,
			ErrMsg:  "dataset_id or corp_id is empty",
		}
		result.State = uint32(commonv2.AsyncTaskState_AsyncTaskStateFailure)
		return result, errors.New("dataset_id or corp_id is empty")
	}
	datasetId, _ := strconv.ParseUint(datasetIdStr, 10, 64)
	corpId, _ := strconv.ParseUint(corpIdStr, 10, 64)

	// 查询 dataset
	dataset, err := modelrag2.ModelDataset.GetByCorpIdAndId(ctx, uint32(corpId), datasetId)
	if err != nil {
		result.Error = &commonv2.ErrMsg{
			ErrCode: rpc.KErrRequestBodyReadFail,
			ErrMsg:  "query dataset failed",
		}
		result.State = uint32(commonv2.AsyncTaskState_AsyncTaskStateFailure)
		return result, rpc.CreateErrorWithMsg(rpc.KSystemError, "query dataset failed")
	}
	collectionName := dataset.CollectionName

	scope := modelrag2.ModelDataset.NewScope()

	err = scope.OnTransaction(ctx, func(txCtx *rpc.Context, tx *dbx.Tx) error {
		if err := modelrag2.ModelFAQ.DeleteByDatasetIdInTx(txCtx, tx, uint32(corpId), datasetId); err != nil {
			return fmt.Errorf("delete faqs failed: %w", err)
		}

		if err := modelrag2.ModelFile.DeleteByDatasetIdInTx(txCtx, tx, uint32(corpId), datasetId); err != nil {
			return fmt.Errorf("delete files failed: %w", err)
		}

		if err := modelrag2.ModelDataset.DeleteByIdInTx(txCtx, tx, uint32(corpId), datasetId); err != nil {
			return fmt.Errorf("delete dataset failed: %w", err)
		}
		return nil
	}, dbproxy.OptionWithUseDb(db), dbproxy.OptionWithUseBiz(true))
	if err != nil {
		result.Error = &commonv2.ErrMsg{
			ErrCode: rpc.KErrRequestBodyReadFail,
			ErrMsg:  fmt.Sprintf("事务删除失败, %v\n", err),
		}
		result.State = uint32(commonv2.AsyncTaskState_AsyncTaskStateFailure)
		return result, rpc.CreateErrorWithMsg(rpc.KSystemError, "事务删除失败")
	}

	// 直接删除整个集合
	fmt.Println("delete collection name: ", dataset.CollectionName)
	// 删除数据库数据后，再删除向量，就算向量删除失败，也会是无效数据，而不是先删除向量数据库，再删除DB，这样的话，如果DB删失败了，回滚了DB数据，但是向量没了，这不扯淡吗。
	if err := service.DeleteMilvusCollection(context.Background(), collectionName); err != nil {
		result.Error = &commonv2.ErrMsg{
			ErrCode: rpc.KErrRequestBodyReadFail,
			ErrMsg:  fmt.Sprintf("delete milvus collection failed: %v", err),
		}
		result.State = uint32(commonv2.AsyncTaskState_AsyncTaskStateFailure)
		return result, rpc.CreateErrorWithMsg(rpc.KSystemError, "delete milvus collection failed")
	}

	result.State = uint32(commonv2.AsyncTaskState_AsyncTaskStateSuccess)
	result.Buf = "success"
	return result, nil
}

//func DeleteDatasetFAQTask(ctx *rpc.Context) (*commonv2.AsyncTaskResult, error) {
//	result := &commonv2.AsyncTaskResult{}
//
//	datasetIdStr := ctx.GetReqHeader("dataset_id")
//	if datasetIdStr == "" {
//		result.Error.ErrMsg = "dataset_id is empty"
//		return result, errors.New("dataset_id is empty")
//	}
//	datasetId, err := strconv.ParseUint(datasetIdStr, 10, 64)
//	if err != nil {
//		result.Error.ErrMsg = "dataset_id is invalid"
//		return result, err
//	}
//	corpIdStr := ctx.GetReqHeader("corp_id")
//	if corpIdStr == "" {
//		result.Error.ErrMsg = "corp_id is empty"
//		return result, errors.New("corp_id is empty")
//	}
//	corpId, err := strconv.ParseUint(corpIdStr, 10, 64)
//	if err != nil {
//		result.Error.ErrMsg = "corp_id is invalid"
//		return result, err
//	}
//
//	dataset, err := model.ModelDataset.GetByCorpIdAndId(ctx, uint32(corpId), datasetId)
//	if err != nil {
//		result.Error.ErrMsg = "query dataset failed"
//		return result, err
//	}
//
//	faqList, err := model.ModelFAQ.ListByDatasetId(ctx, uint32(corpId), datasetId)
//	if err != nil {
//		result.Error.ErrMsg = "query faq failed"
//		return result, err
//	}
//
//	if faqList == nil {
//		result.Error.ErrMsg = "faq not found"
//		return result, nil
//	}
//
//	embeddingIds := make([]string, 0, len(faqList))
//	for _, faq := range faqList {
//		embeddingIds = append(embeddingIds, faq.EmbeddingId)
//	}
//	log.Infof("delete faq collection name: %s", dataset.CollectionName)
//	log.Infof("delete faq embedding ids: %v", embeddingIds)
//
//	// 删除向量
//	if err := service.DeleteMilvusVectors(context.Background(), dataset.CollectionName, embeddingIds); err != nil {
//		result.Error.ErrMsg = "delete faq failed"
//		return result, err
//	}
//
//	// 删除向量集合
//	if err := service.DeleteMilvusCollection(context.Background(), dataset.CollectionName); err != nil {
//		result.Error.ErrMsg = "delete collection failed"
//		return result, err
//	}
//
//	// 然后直接删除所有的 FAQ 记录
//	if err := model.ModelFAQ.DeleteByDatasetId(ctx, uint32(corpId), datasetId); err != nil {
//		result.Error.ErrMsg = "delete faq failed"
//		return result, err
//	}
//
//	// 删除知识库
//	err = model.ModelDataset.DeleteById(ctx, uint32(corpId), datasetId)
//	if err != nil {
//		result.Error.ErrMsg = "delete dataset failed"
//		return result, err
//	}
//
//	// 删除知识库文件
//	err = model.ModelFile.DeleteByDatasetId(ctx, uint64(datasetId))
//	if err != nil {
//		result.Error.ErrMsg = "delete dataset file failed"
//		return result, err
//	}
//
//	buf := []byte("success")
//	result.State = uint32(commonv2.AsyncTaskState_AsyncTaskStateSuccess)
//	result.Buf = string(buf)
//	return result, nil
//}

//func DeleteFAQCategoryTask(ctx *rpc.Context) (*commonv2.AsyncTaskResult, error) {
//	result := &commonv2.AsyncTaskResult{}
//
//	// dataset_id
//	datasetIdStr := ctx.GetReqHeader("dataset_id")
//	if datasetIdStr == "" {
//		result.Error.ErrMsg = "dataset_id is empty"
//		return result, errors.New("dataset_id is empty")
//	}
//	datasetId, err := strconv.ParseUint(datasetIdStr, 10, 64)
//	if err != nil {
//		result.Error.ErrMsg = "dataset_id is invalid"
//		return result, err
//	}
//
//	// category_id
//	categoryIdStr := ctx.GetReqHeader("category_id")
//	if categoryIdStr == "" {
//		result.Error.ErrMsg = "category_id is empty"
//		return result, errors.New("category_id is empty")
//	}
//	categoryId, err := strconv.ParseUint(categoryIdStr, 10, 64)
//	if err != nil {
//		result.Error.ErrMsg = "category_id is invalid"
//		return result, err
//	}
//
//	// corp_id
//	corpIdStr := ctx.GetReqHeader("corp_id")
//	if corpIdStr == "" {
//		result.Error.ErrMsg = "corp_id is empty"
//		return result, errors.New("corp_id is empty")
//	}
//	corpId, err := strconv.ParseUint(corpIdStr, 10, 64)
//	if err != nil {
//		result.Error.ErrMsg = "corp_id is invalid"
//		return result, err
//	}
//
//	dataset, err := model.ModelDataset.GetByCorpIdAndId(ctx, uint32(corpId), datasetId)
//	if err != nil {
//		result.Error.ErrMsg = "query dataset failed"
//		return result, err
//	}
//
//	// 找到所有的 分类下所有的FAQ，然后去删除向量，再删除 FAQ，最后删除 分类
//	faqList, err := model.ModelFAQ.ListByCategoryId(ctx, uint32(corpId), datasetId, categoryId)
//	if err != nil {
//		result.Error.ErrMsg = "query faq failed"
//		return result, err
//	}
//
//	if faqList == nil {
//		result.Error.ErrMsg = "faq not found"
//		return result, nil
//	}
//
//	embeddingIds := make([]string, 0, len(faqList))
//	for _, faq := range faqList {
//		embeddingIds = append(embeddingIds, faq.EmbeddingId)
//	}
//	log.Infof("delete faq collection name: %s", dataset.CollectionName)
//	log.Infof("delete faq embedding ids: %v", embeddingIds)
//
//	if err := service.DeleteMilvusVectors(context.Background(), dataset.CollectionName, embeddingIds); err != nil {
//		result.Error.ErrMsg = "delete faq failed"
//		return result, err
//	}
//
//	// 然后直接删除所有的 FAQ 记录
//	if err := model.ModelFAQ.DeleteByCategoryId(ctx, uint32(corpId), datasetId, categoryId); err != nil {
//		result.Error.ErrMsg = "delete faq failed"
//		return result, err
//	}
//
//	// 删除分类
//	if err := model.ModelFAQCategory.DeleteById(ctx, uint32(corpId), datasetId, categoryId); err != nil {
//		result.Error.ErrMsg = "delete faq category failed"
//		return result, err
//	}
//
//	buf := []byte("success")
//	result.State = uint32(commonv2.AsyncTaskState_AsyncTaskStateSuccess)
//	result.Buf = string(buf)
//	return result, nil
//}

func DeleteFAQCategoryTask(ctx *rpc.Context) (*commonv2.AsyncTaskResult, error) {
	result := &commonv2.AsyncTaskResult{}
	result.State = uint32(commonv2.AsyncTaskState_AsyncTaskStateFailure) // 默认失败

	db := ctx.GetReqHeader("db")

	// 参数解析
	datasetIdStr := ctx.GetReqHeader("dataset_id")
	categoryIdStr := ctx.GetReqHeader("category_id")
	corpIdStr := ctx.GetReqHeader("corp_id")

	if datasetIdStr == "" || categoryIdStr == "" || corpIdStr == "" {
		result.Error = &commonv2.ErrMsg{
			ErrCode: rpc.KErrRequestBodyReadFail,
			ErrMsg:  "dataset_id, category_id or corp_id is empty",
		}
		return result, errors.New("required parameter missing")
	}
	datasetId, _ := strconv.ParseUint(datasetIdStr, 10, 64)
	categoryId, _ := strconv.ParseUint(categoryIdStr, 10, 64)
	corpId, _ := strconv.ParseUint(corpIdStr, 10, 64)

	dataset, err := modelrag2.ModelDataset.GetByCorpIdAndId(ctx, uint32(corpId), datasetId)
	if err != nil {
		result.Error = &commonv2.ErrMsg{
			ErrCode: rpc.KSystemError,
			ErrMsg:  "query dataset failed",
		}
		return result, err
	}

	// 查询 FAQ
	faqList, err := modelrag2.ModelFAQ.ListByCategoryId(ctx, uint32(corpId), datasetId, categoryId)
	if err != nil {
		result.Error = &commonv2.ErrMsg{
			ErrCode: rpc.KSystemError,
			ErrMsg:  "query faq failed",
		}
		return result, err
	}

	embeddingIds := make([]string, 0, len(faqList))
	for _, faq := range faqList {
		embeddingIds = append(embeddingIds, faq.EmbeddingId)
	}

	// 删除数据库：用事务
	scope := modelrag2.ModelFAQ.NewScope()
	err = scope.OnTransaction(ctx, func(txCtx *rpc.Context, tx *dbx.Tx) error {
		// 如果有 FAQ 就删除
		if len(faqList) > 0 {
			if err := modelrag2.ModelFAQ.DeleteByCategoryIdInTx(txCtx, tx, uint32(corpId), datasetId, categoryId); err != nil {
				return fmt.Errorf("delete faqs failed: %w", err)
			}
		}
		// 不论有没有 FAQ，都应该删除分类
		if err := modelrag2.ModelFAQCategory.DeleteByIdInTx(txCtx, tx, uint32(corpId), datasetId, categoryId); err != nil {
			return fmt.Errorf("delete category failed: %w", err)
		}
		return nil
	}, dbproxy.OptionWithUseDb(db), dbproxy.OptionWithUseBiz(true))

	if err != nil {
		result.Error = &commonv2.ErrMsg{
			ErrCode: rpc.KSystemError,
			ErrMsg:  fmt.Sprintf("事务失败：%v", err),
		}
		return result, err
	}

	// 向量删除
	if len(embeddingIds) > 0 {
		if err := service.DeleteMilvusVectors(context.Background(), dataset.CollectionName, embeddingIds); err != nil {
			result.Error = &commonv2.ErrMsg{
				ErrCode: rpc.KSystemError,
				ErrMsg:  fmt.Sprintf("delete milvus vectors failed: %v", err),
			}
			return result, err
		}
	}

	result.State = uint32(commonv2.AsyncTaskState_AsyncTaskStateSuccess) // 成功
	result.Buf = "success"
	return result, nil
}
