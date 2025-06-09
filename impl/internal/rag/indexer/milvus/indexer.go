package milvus

import (
	"context"
	"errors"
	"fmt"
	"git.pinquest.cn/base/log"
	"github.com/cloudwego/eino/callbacks"
	"github.com/cloudwego/eino/components"
	"github.com/cloudwego/eino/components/indexer"
	"strings"
	"time"

	"github.com/bytedance/sonic"
	"github.com/cloudwego/eino/components/embedding"
	"github.com/cloudwego/eino/schema"
	"github.com/milvus-io/milvus-sdk-go/v2/client"
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
)

type IndexerConfig struct {
	// Client is the milvus client to be called
	// Required
	Client client.Client

	// Default Collection config
	// Collection is the collection name in milvus database
	// Optional, and the default value is "eino_collection"
	Collection string
	// Description is the description for collection
	// Optional, and the default value is "the collection for eino"
	Description string
	// PartitionNum is the collection partition number
	// Optional, and the default value is 1(disable)
	// If the partition number is larger than 1, it means use partition and must have a partition key in Fields
	PartitionNum int64
	// Fields is the collection fields
	// Optional, and the default value is the default fields
	Fields []*entity.Field
	// SharedNum is the milvus required param to create collection
	// Optional, and the default value is 1
	SharedNum int32
	// ConsistencyLevel is the milvus collection consistency tactics
	// Optional, and the default level is ClBounded(bounded consistency level with default tolerance of 5 seconds)
	ConsistencyLevel ConsistencyLevel
	// EnableDynamicSchema is means the collection is enabled to dynamic schema
	// Optional, and the default value is false
	// Enable to dynamic schema it could affect milvus performance
	EnableDynamicSchema bool

	// DocumentConverter is the function to convert the schema.Document to the row data
	// Optional, and the default value is defaultDocumentConverter
	DocumentConverter func(ctx context.Context, docs []*schema.Document, vectors [][]float64) ([]interface{}, error)

	// Index config to the vector column
	// MetricType the metric type for vector
	// Optional and default type is HAMMING
	MetricType MetricType

	// Embedding vectorization method for values needs to be embedded from schema.Document's content.
	// Required
	Embedding embedding.Embedder
}

type Indexer struct {
	config IndexerConfig
}

// NewIndexer creates a new indexer.
func NewIndexer(ctx context.Context, conf *IndexerConfig) (*Indexer, error) {
	// conf check
	if err := conf.check(); err != nil {
		return nil, err
	}

	// check the collection whether to be created
	ok, err := conf.Client.HasCollection(ctx, conf.Collection)
	if err != nil {
		if errors.Is(err, client.ErrClientNotReady) {
			return nil, fmt.Errorf("[NewIndexer] milvus client not ready: %w", err)
		}
		if errors.Is(err, client.ErrStatusNil) {
			return nil, fmt.Errorf("[NewIndexer] milvus client status is nil: %w", err)
		}
		return nil, fmt.Errorf("[NewIndexer] failed to check collection: %w", err)
	}
	if !ok {
		// create the collection
		if errToCreate := conf.Client.CreateCollection(
			ctx,
			conf.getSchema(conf.Collection, conf.Description, conf.Fields),
			conf.SharedNum,
			client.WithConsistencyLevel(
				conf.ConsistencyLevel.getConsistencyLevel(),
			),
			client.WithEnableDynamicSchema(conf.EnableDynamicSchema),
			client.WithPartitionNum(conf.PartitionNum),
		); errToCreate != nil {
			return nil, fmt.Errorf("[NewIndexer] failed to create collection: %w", errToCreate)
		}
	}

	// load collection info
	collection, err := conf.Client.DescribeCollection(ctx, conf.Collection)
	if err != nil {
		return nil, fmt.Errorf("[NewIndexer] failed to describe collection: %w", err)
	}
	// check collection schema
	if !conf.checkCollectionSchema(collection.Schema, conf.Fields) {
		return nil, fmt.Errorf("[NewIndexer] collection schema not match")
	}
	// check the collection load state
	if !collection.Loaded {
		// load collection
		if err := conf.loadCollection(ctx); err != nil {
			return nil, err
		}
	}

	// create indexer
	return &Indexer{
		config: *conf,
	}, nil
}

// Store stores the documents into the indexer.
func (i *Indexer) Store(ctx context.Context, docs []*schema.Document, opts ...indexer.Option) (ids []string, err error) {
	const batchSize = 20 // 每批处理的大小，根据API限制（25）设置为一个安全值
	var allStoredIds []string
	var accumulatedErrors []string // 用于收集所有批次的错误信息

	// 1. 获取通用选项和嵌入模型
	co := indexer.GetCommonOptions(&indexer.Options{
		SubIndexes: nil,
		Embedding:  i.config.Embedding,
	}, opts...)

	emb := co.Embedding
	if emb == nil {
		err = fmt.Errorf("[Indexer.Store] embedding not provided")
		callbacks.OnError(ctx, err) // 确保在出错时调用 OnError
		return nil, err
	}

	// 2. 初始化回调
	ctx = callbacks.EnsureRunInfo(ctx, i.GetType(), components.ComponentOfIndexer)
	ctx = callbacks.OnStart(ctx, &indexer.CallbackInput{
		Docs: docs, // 初始回调传入所有文档
	})

	// 在函数结束时，如果发生错误，则调用 OnError 回调
	// 注意：这里的 err 是函数签名中的 err，需要在最后将 finalError 赋给它
	defer func() {
		if err != nil {
			callbacks.OnError(ctx, err)
		}
	}()

	// 3. 分批处理文档
	for batchStart := 0; batchStart < len(docs); batchStart += batchSize {
		batchEnd := batchStart + batchSize
		if batchEnd > len(docs) {
			batchEnd = len(docs)
		}
		currentBatchDocs := docs[batchStart:batchEnd]

		if len(currentBatchDocs) == 0 {
			continue
		}

		log.Infof("[Indexer.Store] Processing batch: %d to %d (total %d)", batchStart, batchEnd-1, len(docs))

		// 3.1 为当前批次提取文本
		texts := make([]string, 0, len(currentBatchDocs))
		for _, doc := range currentBatchDocs {
			texts = append(texts, doc.Content)
		}

		// 3.2 向量化当前批次的文本
		// makeEmbeddingCtx 应该为每个批次的具体操作创建上下文，或确保其可重入
		embeddingCtx := makeEmbeddingCtx(ctx, emb) // 注意: 确保此函数创建的上下文适合循环内多次调用
		vectors, batchErr := emb.EmbedStrings(embeddingCtx, texts)
		if batchErr != nil {
			errMsg := fmt.Sprintf("batch %d (docs %d-%d) embedding failed: %v", batchStart/batchSize, batchStart, batchEnd-1, batchErr)
			log.Errorf("[Indexer.Store] %s", errMsg)
			accumulatedErrors = append(accumulatedErrors, errMsg)
			continue // 继续处理下一批
		}

		if len(vectors) != len(currentBatchDocs) {
			errMsg := fmt.Sprintf("batch %d (docs %d-%d) embedding result length mismatch: expected %d, got %d", batchStart/batchSize, batchStart, batchEnd-1, len(currentBatchDocs), len(vectors))
			log.Errorf("[Indexer.Store] %s", errMsg)
			accumulatedErrors = append(accumulatedErrors, errMsg)
			continue // 继续处理下一批
		}

		// 3.3 转换当前批次的文档和向量为Milvus行数据
		rows, batchErr := i.config.DocumentConverter(ctx, currentBatchDocs, vectors)
		if batchErr != nil {
			errMsg := fmt.Sprintf("batch %d (docs %d-%d) document conversion failed: %v", batchStart/batchSize, batchStart, batchEnd-1, batchErr)
			log.Errorf("[Indexer.Store] %s", errMsg)
			accumulatedErrors = append(accumulatedErrors, errMsg)
			continue // 继续处理下一批
		}

		// 3.4 将当前批次的行数据存入Milvus
		results, batchErr := i.config.Client.InsertRows(ctx, i.config.Collection, "" /* partition name, empty for default */, rows)
		if batchErr != nil {
			errMsg := fmt.Sprintf("batch %d (docs %d-%d) Milvus InsertRows failed: %v", batchStart/batchSize, batchStart, batchEnd-1, batchErr)
			log.Errorf("[Indexer.Store] %s", errMsg)
			accumulatedErrors = append(accumulatedErrors, errMsg)
			continue // 继续处理下一批
		}

		// 3.5 提取并存储成功插入的ID
		batchStoredIds := make([]string, results.Len())
		var idExtractionError bool
		for idx := 0; idx < results.Len(); idx++ {
			idStr, idErr := results.GetAsString(idx)
			if idErr != nil {
				errMsg := fmt.Sprintf("batch %d (docs %d-%d) failed to get ID from Milvus result at index %d: %v", batchStart/batchSize, batchStart, batchEnd-1, idx, idErr)
				log.Errorf("[Indexer.Store] %s", errMsg)
				accumulatedErrors = append(accumulatedErrors, errMsg)
				idExtractionError = true // 标记此批次ID提取失败
				break
			}
			batchStoredIds[idx] = idStr
		}
		if idExtractionError {
			continue // 如果ID提取失败，不将此批次的ID加入allStoredIds，并继续下一批
		}
		allStoredIds = append(allStoredIds, batchStoredIds...)
		log.Infof("[Indexer.Store] Successfully processed and inserted batch %d (docs %d-%d), %d IDs obtained.", batchStart/batchSize, batchStart, batchEnd-1, len(batchStoredIds))
	}

	// 4. 所有批次处理完毕后，刷新Milvus集合
	// 注意：Flush 操作本身也可能失败
	if len(allStoredIds) > 0 { // 只有在至少成功插入了一批数据后才尝试 Flush
		log.Infof("[Indexer.Store] All batches processed. Attempting to flush collection '%s'. Total successful IDs: %d", i.config.Collection, len(allStoredIds))
		flushErr := i.config.Client.Flush(ctx, i.config.Collection, false)
		if flushErr != nil {
			errMsg := fmt.Sprintf("failed to flush Milvus collection '%s' after all batches: %v", i.config.Collection, flushErr)
			log.Errorf("[Indexer.Store] %s", errMsg)
			accumulatedErrors = append(accumulatedErrors, errMsg)
		} else {
			log.Infof("[Indexer.Store] Successfully flushed collection '%s'.", i.config.Collection)
		}
	} else if len(docs) > 0 && len(accumulatedErrors) > 0 { // 如果有文档输入，但没有成功存储的ID，且有错误
		log.Warnf("[Indexer.Store] No documents were successfully stored, possibly due to errors in all batches.")
	}

	// 5. 处理最终结果和回调
	if len(accumulatedErrors) > 0 {
		joinedErr := strings.Join(accumulatedErrors, "; ")
		// 如果错误信息超过2000个字符，截断
		const maxErrLen = 2000
		if len(joinedErr) > maxErrLen {
			// 只截断字符串，防止半个 utf-8 字符出错（Go 的字符串是 utf-8，截断不会乱码）
			joinedErr = joinedErr[:maxErrLen] + "...(truncated)"
		}
		err = fmt.Errorf("[Indexer.Store] completed with errors: %s", joinedErr)
	}

	// 即使有错误，也调用 OnEnd，并传递成功存储的ID列表
	// OnEnd 的语义通常是“操作结束”，无论成功与否，而 OnError 用于专门报告错误
	callbacks.OnEnd(ctx, &indexer.CallbackOutput{
		IDs: allStoredIds, // 只返回成功存储的ID
	})

	// 返回所有成功存储的ID和遇到的（聚合的）错误
	return allStoredIds, err
}

func (i *Indexer) GetType() string {
	return typ
}

func (i *Indexer) IsCallbacksEnabled() bool {
	return true
}

func (i *Indexer) Delete(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}

	// 构造表达式
	quoted := make([]string, 0, len(ids))
	for _, id := range ids {
		quoted = append(quoted, fmt.Sprintf("\"%s\"", id))
	}
	expr := fmt.Sprintf("%s in [%s]", "id", strings.Join(quoted, ","))

	log.Infof("[Milvus Delete] expr: %s", expr)

	// 调用 Milvus Delete
	if err := i.config.Client.Delete(ctx, i.config.Collection, "", expr); err != nil {
		return fmt.Errorf("milvus delete failed: %w", err)
	}

	// Flush 确保删除完成
	if err := i.config.Client.Flush(ctx, i.config.Collection, false); err != nil {
		return fmt.Errorf("milvus flush failed after delete: %w", err)
	}

	return nil
}

func (i *Indexer) DeleteCollection(ctx context.Context, collectionName string) error {
	return i.config.Client.DropCollection(ctx, collectionName)
}

// getDefaultSchema returns the default schema
func (i *IndexerConfig) getSchema(collection, description string, fields []*entity.Field) *entity.Schema {
	s := entity.NewSchema().
		WithName(collection).
		WithDescription(description)
	for _, field := range fields {
		s.WithField(field)
	}
	return s
}

func (i *IndexerConfig) getDefaultDocumentConvert() func(ctx context.Context, docs []*schema.Document, vectors [][]float64) ([]interface{}, error) {
	return func(ctx context.Context, docs []*schema.Document, vectors [][]float64) ([]interface{}, error) {
		em := make([]defaultSchema, 0, len(docs))
		texts := make([]string, 0, len(docs))
		rows := make([]interface{}, 0, len(docs))

		for _, doc := range docs {
			metadata, err := sonic.Marshal(doc.MetaData)
			if err != nil {
				return nil, fmt.Errorf("failed to marshal metadata: %w", err)
			}
			em = append(em, defaultSchema{
				ID:       doc.ID,
				Content:  doc.Content,
				Vector:   nil,
				Metadata: metadata,
			})
			texts = append(texts, doc.Content)
		}

		// build embedding documents for storing
		for idx, vec := range vectors {
			em[idx].Vector = toFloat32(vec)
			rows = append(rows, &em[idx])
		}
		return rows, nil
	}
}

// createdDefaultIndex creates the default index
func (i *IndexerConfig) createdDefaultIndex(ctx context.Context, async bool) error {
	index, err := entity.NewIndexAUTOINDEX(i.MetricType.getMetricType())
	if err != nil {
		return fmt.Errorf("[NewIndexer] failed to create index: %w", err)
	}
	if err := i.Client.CreateIndex(ctx, i.Collection, defaultIndexField, index, async); err != nil {
		return fmt.Errorf("[NewIndexer] failed to create index: %w", err)
	}
	return nil
}

// checkCollectionSchema checks the collection schema
func (i *IndexerConfig) checkCollectionSchema(schema *entity.Schema, field []*entity.Field) bool {
	var count int
	if len(schema.Fields) != len(field) {
		return false
	}
	for _, f := range schema.Fields {
		for _, e := range field {
			if f.Name == e.Name && f.DataType == e.DataType {
				count++
			}
		}
	}
	if count != len(field) {
		return false
	}
	return true
}

// getCollectionDim gets the collection dimension
func (i *IndexerConfig) loadCollection(ctx context.Context) error {
	loadState, err := i.Client.GetLoadState(ctx, i.Collection, nil)
	if err != nil {
		return fmt.Errorf("[NewIndexer] failed to get load state: %w", err)
	}
	switch loadState {
	case entity.LoadStateNotExist:
		return fmt.Errorf("[NewIndexer] collection not exist")
	case entity.LoadStateNotLoad:
		index, err := i.Client.DescribeIndex(ctx, i.Collection, "vector")
		if errors.Is(err, client.ErrClientNotReady) {
			return fmt.Errorf("[NewIndexer] milvus client not ready: %w", err)
		}
		if len(index) == 0 {
			if err := i.createdDefaultIndex(ctx, false); err != nil {
				return err
			}
		}
		if err := i.Client.LoadCollection(ctx, i.Collection, true); err != nil {
			return err
		}
		return nil
	case entity.LoadStateLoading:
		ticker := time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ticker.C:
				loadingProgress, err := i.Client.GetLoadingProgress(ctx, i.Collection, nil)
				if err != nil {
					return err
				}
				if loadingProgress == 100 {
					return nil
				}
			}
		}
	default:
		return nil
	}
}

// check the indexer config
func (i *IndexerConfig) check() error {
	if i.Client == nil {
		return fmt.Errorf("[NewIndexer] milvus client not provided")
	}
	if i.Embedding == nil {
		return fmt.Errorf("[NewIndexer] embedding not provided")
	}
	if i.Collection == "" {
		i.Collection = defaultCollection
	}
	if i.Description == "" {
		i.Description = defaultDescription
	}
	if i.SharedNum <= 0 {
		i.SharedNum = 1
	}
	if i.ConsistencyLevel <= 0 || i.ConsistencyLevel > 5 {
		i.ConsistencyLevel = defaultConsistencyLevel
	}
	if i.MetricType == "" {
		i.MetricType = defaultMetricType
	}
	if i.PartitionNum <= 1 {
		i.PartitionNum = 0
	}
	if i.Fields == nil {
		i.Fields = getDefaultFields()
	}
	if i.DocumentConverter == nil {
		i.DocumentConverter = i.getDefaultDocumentConvert()
	}
	return nil
}
