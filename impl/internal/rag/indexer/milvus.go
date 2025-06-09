// internal/modelrag/indexer/milvus.go
package indexer

import (
	"context"
	"fmt"
	milvus2 "git.pinquest.cn/base/aiadminserver/impl/internal/rag/indexer/milvus"
	"github.com/cloudwego/eino/components/embedding"
	"github.com/milvus-io/milvus-sdk-go/v2/client"
)

// InitMilvusIndexer 用于初始化 eino-ext 的 Milvus Indexer 实例
func InitMilvusIndexer(ctx context.Context, cli client.Client, emb embedding.Embedder, collectionName string) (*milvus2.Indexer, error) {
	if cli == nil {
		return nil, fmt.Errorf("milvus client not provided")
	}
	if emb == nil {
		return nil, fmt.Errorf("embedding model not provided")
	}

	cfg := buildFAQMilvusConfig(cli, emb, collectionName)
	return milvus2.NewIndexer(ctx, cfg)
}

func buildFAQMilvusConfig(cli client.Client, emb embedding.Embedder, collectionName string) *milvus2.IndexerConfig {
	return &milvus2.IndexerConfig{
		Client:              cli,
		Collection:          collectionName,
		Description:         "RAG dataset collection", // 描述时，加上企业信息吧
		Fields:              BuildMilvusFAQSchema(),   // 统一字段结构
		Embedding:           emb,
		MetricType:          milvus2.COSINE,
		SharedNum:           1,     // 一般固定
		PartitionNum:        0,     // 不做分区时设为 0
		EnableDynamicSchema: false, // 禁止动态 schema
	}
}
