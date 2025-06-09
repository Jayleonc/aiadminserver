package task

import (
	"context"
	"errors"
	"fmt"
	modelrag2 "git.pinquest.cn/base/aiadminserver/impl/internal/model/modelrag"
	docstore2 "git.pinquest.cn/base/aiadminserver/impl/internal/rag/docstore"
	"git.pinquest.cn/base/aiadminserver/impl/internal/rag/indexer"
	"git.pinquest.cn/base/aiadminserver/impl/internal/rag/service"
	utils2 "git.pinquest.cn/base/aiadminserver/impl/pkg/utils"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"git.pinquest.cn/base/aiadmin"
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/dbproxy"
	"git.pinquest.cn/qlb/brick/dbx"
	"git.pinquest.cn/qlb/brick/json"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/commonv2"
	"git.pinquest.cn/qlb/core"
	einoschema "github.com/cloudwego/eino/schema"
)

func ExecFileFAQTask(ctx *rpc.Context) (*commonv2.AsyncTaskResult, error) {
	result := &commonv2.AsyncTaskResult{}
	result.State = uint32(commonv2.AsyncTaskState_AsyncTaskStateFailure)

	corpIdStr := ctx.GetReqHeader("corp_id")
	corpId, err := strconv.ParseUint(corpIdStr, 10, 32)
	if err != nil {
		result.Error = &commonv2.ErrMsg{ErrCode: rpc.KErrRequestBodyReadFail, ErrMsg: "corp_id is invalid"}
		return result, err
	}
	datasetIdStr := ctx.GetReqHeader("dataset_id")
	datasetId, err := strconv.ParseUint(datasetIdStr, 10, 64)
	if err != nil {
		result.Error = &commonv2.ErrMsg{ErrCode: rpc.KErrRequestBodyReadFail, ErrMsg: "dataset_id is invalid"}
		return result, err
	}
	fileIdStr := ctx.GetReqHeader("file_id")
	fileId, err := strconv.ParseUint(fileIdStr, 10, 64)
	if err != nil {
		result.Error = &commonv2.ErrMsg{ErrCode: rpc.KErrRequestBodyReadFail, ErrMsg: "file_id is invalid"}
		return result, err
	}

	db := ctx.GetReqHeader("db")
	if db == "" {
		result.Error = &commonv2.ErrMsg{ErrCode: rpc.KErrRequestBodyReadFail, ErrMsg: "db is invalid"}
		return result, errors.New("db is invalid")
	}

	// 0. 下载文件到本地临时路径
	// 优先使用 local_path（校验任务结果）
	filePath := ctx.GetReqHeader("local_path")
	if filePath == "" || !fileExists(filePath) {
		log.Debugf("local_path 不存在或已过期，重新下载文件")
		filePath, err = downloadFile(ctx, uint32(corpId), fileId)
		if err != nil {
			log.Errorf("download file failed: %v", err)
			result.Error.ErrMsg = "download file failed"
			return result, err
		}
	} else {
		log.Debugf("本地文件存在, %v\n", filePath)
		// todo 这里可以记录文件大小
		// 如果 local_path 存在，也补充设置 file.status=处理中（与 downloadFile 中逻辑保持一致）
		_ = modelrag2.ModelFile.UpdateData(ctx, fileId, map[string]interface{}{
			"status":     uint32(aiadmin.FileStatus_FileStatusParsing),
			"updated_at": time.Now().Unix(),
		})
	}

	fmt.Println(filePath)

	// 解析文件内容（根据 file_extension 判断格式，如 xlsx）
	// 注意：需判断空行、标题行、合并单元格等异常结构，避免影响解析质量
	ext := filepath.Ext(filePath)
	docs, err := loadDocumentsFromFile(ctx, filePath, ext)
	if err != nil {
		log.Errorf("loadDocumentsFromFile failed: %v", err)
		// 更新 rag_files 为失败
		result.Error.ErrMsg = "loadDocumentsFromFile failed"
		return result, err
	}
	totalCount := len(docs)

	if totalCount == 0 {
		log.Warnf("no FAQ parsed from document, fileId=%d, datasetId=%d", fileId, datasetId)
		result.Buf = "no FAQ parsed from document"
		result.State = uint32(commonv2.AsyncTaskState_AsyncTaskStateSuccess)
		return result, nil
	}

	log.Debugf("loaded %d documents", len(docs))
	log.Debugf("docs:%v\n ", docs[0])

	// todo 可计算 word_count 和 tokens（用于后续分析/成本控制）
	successCount, err := processFileFAQEmbedding(ctx, datasetId, fileId, docs, db)
	if err != nil {
		log.Errorf("processFileFAQEmbedding failed: %v", err)
		result.Error.ErrMsg = "processFileFAQEmbedding failed"
		return result, err
	}

	// 7. 更新 rag_files 表中对应记录的 status 为 completed
	err = modelrag2.ModelFile.UpdateData(ctx, fileId, map[string]interface{}{
		"status":             aiadmin.FileStatus_FileStatusCompleted,
		"used":               1,
		"used_in_dataset_id": datasetId,
		"updated_at":         time.Now().Unix(),
	})
	if err != nil {
		log.Errorf("update file status to completed failed: %v", err)
		result.Error.ErrMsg = "update file status to completed failed"
		return result, err
	}

	// 8. 返回成功，附带异步任务标识和执行结果
	stats := map[string]interface{}{
		"total":   totalCount,
		"success": successCount,
		"failed":  totalCount - successCount,
	}

	statsJson, _ := json.Marshal(stats)

	result.State = uint32(commonv2.AsyncTaskState_AsyncTaskStateSuccess)
	result.Buf = string(statsJson)
	return result, nil
}

func downloadFile(ctx *rpc.Context, corpId uint32, fileId uint64) (string, error) {
	// 2. 查询 rag_files 表
	file, err := modelrag2.ModelFile.GetById(ctx, fileId)
	if err != nil {
		return "", err
	}
	if file.Status != uint32(aiadmin.FileStatus_FileStatusPending) {
		return "", errors.New("文件已经处理，非 pending")
	}

	// 4. 下载文件
	localPath, err := docstore2.DownloadToLocal(ctx, corpId, file.FileUrl, file.FileName)
	if err != nil {
		_ = modelrag2.ModelFile.UpdateStatus(ctx, fileId, uint32(aiadmin.FileStatus_FileStatusFailed), err.Error())
		return "", err
	}

	// 设置文件 hash 和 状态=处理中
	if err = modelrag2.ModelFile.UpdateData(ctx, fileId,
		map[string]interface{}{
			"status": uint32(aiadmin.FileStatus_FileStatusParsing),
		}); err != nil {
		return "", err
	}

	return localPath, nil
}

func loadDocumentsFromFile(ctx *rpc.Context, localPath string, ext string) ([]*einoschema.Document, error) {
	// 如果是 xlsx，需要先转换为 csv（判断是否已有、是否需更新）
	if ext == ".xlsx" {
		csvPath, alreadyExists, err := utils2.XlsxToCsvIfNeeded(localPath)
		if err != nil {
			log.Errorf("convert xlsx to csv failed: %v", err)
			return nil, err
		}
		if alreadyExists {
			log.Infof("csv already exists, skipping conversion: %s", csvPath)
		} else {
			log.Infof("xlsx converted to csv: %s", csvPath)
		}
		localPath = csvPath
		ext = ".csv"
	}

	// 打开本地文件
	f, err := os.Open(localPath)
	if err != nil {
		return nil, fmt.Errorf("open local file failed: %v", err)
	}
	defer f.Close()

	// 调用 docstore 中统一封装的加载器
	docs, err := docstore2.LoadEinoDocsFromFile(f, ext)
	if err != nil {
		return nil, fmt.Errorf("load and parse file failed: %v", err)
	}

	return docs, nil
}

func processFileFAQEmbedding(ctx *rpc.Context, datasetId, fileId uint64, docs []*einoschema.Document, db string) (int, error) {
	dataset, err := modelrag2.ModelDataset.GetById(ctx, datasetId)
	if err != nil {
		return 0, err
	}

	totalCount := len(docs)

	log.Infof("开始处理faq")
	newItems, updatedItems, err := PrepareFAQItemsFromDocs(ctx, docs, dataset, fileId)
	if err != nil {
		log.Errorf("prepare faq items failed: %v", err)
		return 0, err
	}

	// 事务处理
	scope := modelrag2.ModelFAQ.NewScope()
	err = scope.OnTransaction(ctx, func(txCtx *rpc.Context, tx *dbx.Tx) error {
		// 批量插入新的 FAQ
		if len(newItems) > 0 {
			if err := modelrag2.ModelFAQ.InsertMultiInTx(txCtx, tx, newItems); err != nil {
				return fmt.Errorf("insert new faq items failed: %w", err)
			}
		}
		// 批量更新已存在的 FAQ
		if len(updatedItems) > 0 {
			if err := modelrag2.ModelFAQ.BatchUpdateInTx(txCtx, tx, updatedItems); err != nil {
				log.Errorf("batch update failed: %v", err) // 视为部分成功
			}
		}
		// 更新文件状态
		return modelrag2.ModelFile.UpdateDataInTx(txCtx, tx, fileId, map[string]interface{}{
			"status":     aiadmin.FileStatus_FileStatusParsing,
			"updated_at": time.Now().Unix(),
		})
	}, dbproxy.OptionWithUseDb(db), dbproxy.OptionWithUseBiz(true))

	if err != nil {
		log.Errorf("transaction failed: %v", err)
		return 0, err
	}

	// 合并需要 embedding 的 FAQ 列表
	var allFaqItemsToEmbed = append(newItems, updatedItems...)
	log.Infof("开始 embed: %v, count: %d", dataset.CollectionName, len(allFaqItemsToEmbed))
	successCount, err := EmbedAndStoreFAQItems(ctx, dataset.CollectionName, allFaqItemsToEmbed)
	if err != nil {
		log.Errorf("embed failed: %v", err)
		// 不 return，继续更新状态
	}

	// 状态判断
	fileStatus := aiadmin.FileStatus_FileStatusCompleted // 默认成功
	if successCount == 0 {
		fileStatus = aiadmin.FileStatus_FileStatusFailed // 全部失败
	} else if successCount < totalCount {
		fileStatus = aiadmin.FileStatus_FileStatusPartialSuccess // 部分失败
	}

	err = modelrag2.ModelFile.UpdateData(ctx, fileId, map[string]interface{}{
		"status":     uint32(fileStatus),
		"updated_at": time.Now().Unix(),
	})

	if err != nil {
		log.Errorf("update file status failed: %v, fileId: %d", err, fileId)
	}

	return successCount, nil
}

func PrepareFAQItemsFromDocs(ctx *rpc.Context, docs []*einoschema.Document, dataset *aiadmin.ModelDataset, sourceId uint64) ([]*aiadmin.ModelFAQ, []*aiadmin.ModelFAQ, error) {
	corpId := core.MustGetPinHeaderCorpId2Uint32(ctx)
	var newFaqList = make([]*aiadmin.ModelFAQ, 0, len(docs))
	var updatedFaqList = make([]*aiadmin.ModelFAQ, 0, len(docs))

	for _, doc := range docs {
		if doc.Content == "" {
			continue
		}

		var q, a, c string // question, answer, category
		if v, ok := doc.MetaData["question"]; ok && v != nil {
			q = strings.TrimSpace(fmt.Sprintf("%v", v))
		}
		if v, ok := doc.MetaData["answer"]; ok && v != nil {
			a = strings.TrimSpace(fmt.Sprintf("%v", v))
		}
		if v, ok := doc.MetaData["category"]; ok && v != nil {
			c = strings.TrimSpace(fmt.Sprintf("%v", v))
		}

		// 默认设置为问答型
		faqType := aiadmin.FAQType_FAQTypeQa
		if q == "" && a != "" {
			faqType = aiadmin.FAQType_FAQTypeQuestionOnly
		} else if q != "" && a == "" {
			faqType = aiadmin.FAQType_FAQTypeParagraph
		}

		// 空内容跳过
		if q == "" && a == "" {
			continue
		}
		// 获取或创建分类（如果有）
		var categoryId uint64
		if c != "" {
			category, err := modelrag2.ModelFAQCategory.GetOrCreate(ctx, corpId, dataset.RobotUid, dataset.Id, c)
			if err != nil {
				log.Errorf("get or create category failed: %v", err)
				continue
			}
			categoryId = category.Id
		}

		// 使用 categoryId + question 计算 contentHash
		h := fmt.Sprintf("%d_%s", categoryId, q)
		contentHash := utils2.CalcHash(h)
		existingFaq, err := modelrag2.ModelFAQ.GetByContentHash(ctx, corpId, dataset.Id, contentHash)
		if err != nil && !modelrag2.ModelFAQ.IsNotFoundErr(err) {
			log.Errorf("check faq exists failed: %v", err)
			continue
		}

		newEmbeddingId := utils2.GetEmbeddingId(corpId)

		if existingFaq != nil {
			// 存在：更新答案、hash
			existingFaq.Answer = a
			existingFaq.UpdatedAt = uint32(time.Now().Unix())
			updatedFaqList = append(updatedFaqList, existingFaq)
			fmt.Println("existing faq: ", existingFaq)
		} else {
			// 不存在：新增
			faq := &aiadmin.ModelFAQ{
				CorpId:       corpId,
				RobotUid:     dataset.RobotUid,
				DatasetId:    dataset.Id,
				CategoryId:   categoryId,
				FaqType:      uint32(faqType),
				Question:     q,
				Answer:       a,
				EmbeddingId:  newEmbeddingId,
				ContentHash:  contentHash,
				SourceType:   "file",
				SourceId:     sourceId,
				Status:       uint32(aiadmin.FAQStatus_FAQStatusPending),
				Enabled:      true,
				CreateMethod: uint32(aiadmin.CreateMethod_CreateMethodExcel),
			}
			newFaqList = append(newFaqList, faq)
		}
	}
	return newFaqList, updatedFaqList, nil
}

func EmbedAndStoreFAQItems(ctx *rpc.Context, collectionName string, faqList []*aiadmin.ModelFAQ) (int, error) {
	ctx1 := context.Background()

	var docs = make([]*einoschema.Document, 0, len(faqList))
	var faqIdMap = make(map[string]uint64, len(faqList))
	var idsToDelete = make([]string, 0, len(faqList))

	for _, faq := range faqList {
		doc := indexer.ConvertFAQToEinoDoc(faq)
		docs = append(docs, doc)
		faqIdMap[doc.ID] = faq.Id
		idsToDelete = append(idsToDelete, doc.ID) // 批量删除所有 FAQ 的旧向量
	}

	// 删除旧向量
	if err := service.DeleteMilvusVectors(ctx1, collectionName, idsToDelete); err != nil {
		log.Errorf("delete faqs failed: %v", err)
		// 忽略删除错误，尽量保证插入
	}

	// 存储新的向量
	ids, err := service.StoreMilvusVectors(ctx1, collectionName, docs)
	if err != nil {
		log.Errorf("store to milvus failed: %v", err)
		for _, faq := range faqList {
			_ = modelrag2.ModelFAQ.UpdateStatus(ctx, faq.Id, uint32(aiadmin.FAQStatus_FAQStatusFailed), err.Error())
		}
		return 0, err
	}

	// 构建成功 map 并更新状态
	successCount := 0
	successMap := make(map[string]struct{})
	for _, id := range ids {
		successMap[id] = struct{}{}
		if faqId, ok := faqIdMap[id]; ok {
			_ = modelrag2.ModelFAQ.UpdateStatus(ctx, faqId, uint32(aiadmin.FAQStatus_FAQStatusCompleted), "")
			successCount++
		}
	}

	for _, doc := range docs {
		if _, ok := successMap[doc.ID]; !ok {
			if faqId, ok := faqIdMap[doc.ID]; ok {
				_ = modelrag2.ModelFAQ.UpdateStatus(ctx, faqId, uint32(aiadmin.FAQStatus_FAQStatusFailed), "success but not returned")
			}
		}
	}
	return len(successMap), nil
}

func fileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}
