package task

import (
	"context"
	"errors"
	"git.pinquest.cn/base/aiadminserver/impl/internal/model/modelrag"
	"git.pinquest.cn/base/aiadminserver/impl/internal/rag/service"
	"strconv"

	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/commonv2"
)

func DeleteVectorTask(ctx *rpc.Context) (*commonv2.AsyncTaskResult, error) {
	result := &commonv2.AsyncTaskResult{}
	result.State = uint32(commonv2.AsyncTaskState_AsyncTaskStateFailure) // 默认失败

	embeddingId := ctx.GetReqHeader("embedding_id")
	if embeddingId == "" {
		result.Error = &commonv2.ErrMsg{ErrCode: rpc.KErrRequestBodyReadFail, ErrMsg: "embedding_id is empty"}
		return result, errors.New("embedding_id is empty")
	}

	datasetIdStr := ctx.GetReqHeader("dataset_id")
	if datasetIdStr == "" {
		result.Error = &commonv2.ErrMsg{ErrCode: rpc.KErrRequestBodyReadFail, ErrMsg: "dataset_id is empty"}
		return result, errors.New("dataset_id is empty")
	}
	datasetId, err := strconv.ParseUint(datasetIdStr, 10, 64)
	if err != nil {
		result.Error = &commonv2.ErrMsg{ErrCode: rpc.KErrRequestBodyReadFail, ErrMsg: "dataset_id is invalid"}
		return result, err
	}

	// 担心 ctx 里的过程中可能会变，不过可能是多余的担心
	corpIdStr := ctx.GetReqHeader("corp_id")
	if corpIdStr == "" {
		result.Error = &commonv2.ErrMsg{ErrCode: rpc.KErrRequestBodyReadFail, ErrMsg: "corp_id is empty"}
		return result, errors.New("corp_id is empty")
	}
	corpId, err := strconv.ParseUint(corpIdStr, 10, 64)
	if err != nil {
		result.Error = &commonv2.ErrMsg{ErrCode: rpc.KErrRequestBodyReadFail, ErrMsg: "corp_id is invalid"}
		return result, err
	}

	dataset, err := modelrag.ModelDataset.GetByCorpIdAndId(ctx, uint32(corpId), datasetId)
	if err != nil {
		result.Error = &commonv2.ErrMsg{ErrCode: rpc.KSystemError, ErrMsg: "dataset not found"}
		return result, err
	}

	ctx1 := context.Background()
	if err := service.DeleteMilvusVectors(ctx1, dataset.CollectionName, []string{embeddingId}); err != nil {
		result.Error = &commonv2.ErrMsg{ErrCode: rpc.KSystemError, ErrMsg: "delete milvus vector failed"}
		return result, err
	}

	buf := []byte("success")
	result.State = uint32(commonv2.AsyncTaskState_AsyncTaskStateSuccess)
	result.Buf = string(buf)

	return result, nil
}
