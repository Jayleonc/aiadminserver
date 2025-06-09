package retriever

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/apex/log"
	"github.com/cloudwego/eino/components/embedding"
	einoRetriever "github.com/cloudwego/eino/components/retriever"
	"github.com/cloudwego/eino/schema"
	"github.com/milvus-io/milvus-sdk-go/v2/client"
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
)

// MilvusRetrieverConfig 用于封装创建 Retriever 所需的所有配置和依赖。
// 这种方式使得依赖关系非常清晰。
type MilvusRetrieverConfig struct {
	Client         client.Client
	Embedder       embedding.Embedder
	CollectionName string
}

// MilvusRetriever 是你的自定义检索器，它实现了 eino.retriever.Retriever 接口。
type MilvusRetriever struct {
	cli            client.Client
	emb            embedding.Embedder
	collectionName string
	// 定义 Milvus schema 中的字段名，方便维护
	idField       string
	vectorField   string
	textField     string
	metadataField string
}

// NewMilvusRetriever 创建一个新的自定义 MilvusRetriever 实例。
func NewMilvusRetriever(config *MilvusRetrieverConfig) (*MilvusRetriever, error) { // 添加 error 返回
	// ✅ 检查 Client 和 Embedder 是否为 nil
	if config.Client == nil {
		return nil, fmt.Errorf("Milvus Client 不能为空")
	}
	if config.Embedder == nil {
		return nil, fmt.Errorf("Embedder 不能为空")
	}
	if config.CollectionName == "" {
		return nil, fmt.Errorf("CollectionName 不能为空")
	}

	ctx := context.Background() // 或者从 config 传入

	// 检查 Collection 是否存在
	hasColl, err := config.Client.HasCollection(ctx, config.CollectionName)
	if err != nil {
		return nil, fmt.Errorf("检查 Collection '%s' 是否存在失败: %w", config.CollectionName, err)
	}
	if !hasColl {
		return nil, fmt.Errorf("Collection '%s' 不存在", config.CollectionName)
	}

	// 检查并加载 Collection
	// (这里的超时和 ticker 逻辑可以根据需要调整或简化)
	loadState, err := config.Client.GetLoadState(ctx, config.CollectionName, nil)
	if err != nil {
		return nil, fmt.Errorf("获取 Collection '%s' 的加载状态失败: %w", config.CollectionName, err)
	}

	if loadState != entity.LoadStateLoaded {
		fmt.Printf("Collection '%s' 当前状态: %s, 尝试加载...\n", config.CollectionName, loadState)
		// 如果你的 Milvus 版本支持 LoadCollection 第二个参数传 partitionNames，可以传 nil
		err = config.Client.LoadCollection(ctx, config.CollectionName, false)
		if err != nil {
			return nil, fmt.Errorf("加载 Collection '%s' 失败: %w", config.CollectionName, err)
		}
		// 等待加载完成 (这是一个简化的等待逻辑，实际中可能需要更完善的带超时的轮询)
		for i := 0; i < 60; i++ { // 最多等待约 30 秒
			time.Sleep(500 * time.Millisecond)
			loadState, err = config.Client.GetLoadState(ctx, config.CollectionName, nil)
			if err != nil {
				log.Errorf("警告: 等待加载 Collection '%s' 时发生错误: %v\n", config.CollectionName, err)
				break // 出错则不再等待
			}
			if loadState == entity.LoadStateLoaded {
				log.Debugf("Collection '%s' 加载成功!\n", config.CollectionName)
				break
			}
		}
		if loadState != entity.LoadStateLoaded {
			return nil, fmt.Errorf("等待 Collection '%s' 加载超时或失败，最终状态: %s", config.CollectionName, loadState)
		}
	} else {
		log.Debugf("Collection '%s' 已加载。\n", config.CollectionName)
	}

	return &MilvusRetriever{
		cli:            config.Client,
		emb:            config.Embedder,
		collectionName: config.CollectionName,
		idField:        "id",
		vectorField:    "vector",
		textField:      "content",
		metadataField:  "metadata",
	}, nil
}

// GetRelevantDocuments 是实现 retriever.Retriever 接口的核心方法。
func (r *MilvusRetriever) GetRelevantDocuments(ctx context.Context, query string, option ...einoRetriever.Option) ([]*schema.Document, error) {
	// 1. 应用运行时选项 (逻辑不变)
	opts := einoRetriever.GetCommonOptions(&einoRetriever.Options{}, option...)
	if opts.TopK == nil {
		defaultTopK := 10
		opts.TopK = &defaultTopK
	}

	// 2. 将文本查询转换为向量
	queryEmbeddings, err := r.emb.EmbedStrings(ctx, []string{query})
	if err != nil || len(queryEmbeddings) == 0 {
		return nil, fmt.Errorf("查询向量化失败: %w", err)
	}
	// queryEmbeddings[0] 通常是 []float64
	queryVector64 := queryEmbeddings[0]

	// 将 []float64 转换为 []float32
	queryVector32 := make([]float32, len(queryVector64))
	for i, v := range queryVector64 {
		queryVector32[i] = float32(v)
	}

	// 3. 创建与 Milvus 索引匹配的 SearchParam 实例
	// nprobe 设置为 8192，这是一个在召回率和搜索速度之间取得平衡的常用值
	searchParams, err := entity.NewIndexBinIvfFlatSearchParam(8192)
	if err != nil {
		return nil, fmt.Errorf("创建 Milvus 搜索参数失败: %w", err)
	}

	// 定义需要从 Milvus 返回的字段
	outputFields := []string{r.textField, r.metadataField}

	// 4. 执行 Milvus 搜索
	searchResult, err := r.cli.Search(
		ctx,
		r.collectionName,
		nil,
		"", // filter expression
		outputFields,
		[]entity.Vector{entity.FloatVector(queryVector32)},
		r.vectorField,
		entity.COSINE,
		*opts.TopK,
		searchParams,
	)
	if err != nil {
		return nil, fmt.Errorf("milvus 搜索失败: %w", err)
	}

	// 5. 将 Milvus 搜索结果转换为 Eino 的 schema.Document 格式
	var documents []*schema.Document
	for _, result := range searchResult {
		contentColumn := result.Fields.GetColumn(r.textField)
		metadataColumn := result.Fields.GetColumn(r.metadataField)

		if contentColumn == nil || metadataColumn == nil {
			return nil, fmt.Errorf("milvus 返回的结果中缺少 'content' 或 'metadata' 字段")
		}

		for i := 0; i < result.ResultCount; i++ {
			idVal, errId := result.IDs.Get(i)
			if errId != nil {
				log.Errorf("get relevant documents: 无法获取第 %d 个结果的 ID: %v\n", i, errId)
				continue
			}
			docID, ok := idVal.(string)
			if !ok {
				log.Errorf("get relevant documents: ID 字段类型不是 string (实际类型: %T)\n", idVal)
				continue
			}

			textContentVal, _ := contentColumn.Get(i)
			textContent, ok := textContentVal.(string)
			if !ok {
				log.Errorf("get relevant documents: content 字段类型不是 string (实际类型: %T) for ID %s\n", textContentVal, docID)
				continue
			}

			metadataBytesVal, _ := metadataColumn.Get(i)
			metadataBytes, ok := metadataBytesVal.([]byte)
			if !ok {
				log.Errorf("get relevant documents: metadata 字段类型不是 []byte (实际类型: %T) for ID %s\n", metadataBytesVal, docID)
				continue
			}

			var metadataMap map[string]any
			_ = json.Unmarshal(metadataBytes, &metadataMap)

			doc := &schema.Document{
				ID:       docID,
				Content:  textContent,
				MetaData: metadataMap,
			}
			doc.WithScore(float64(result.Scores[i]))
			documents = append(documents, doc)
		}
	}

	return documents, nil
}
