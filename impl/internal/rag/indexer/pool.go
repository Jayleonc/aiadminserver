package indexer

import (
	"context"
	"git.pinquest.cn/base/aiadminserver/impl/internal/rag/indexer/milvus"
	"github.com/cloudwego/eino/components/embedding"
	"github.com/milvus-io/milvus-sdk-go/v2/client"
	"sync"
)

type IndexerManager struct {
	mu       sync.RWMutex
	indexers map[string]*milvus.Indexer // key: collectionName
	cli      client.Client              // 复用 Milvus client
	emb      embedding.Embedder
}

var (
	globalOnce    sync.Once
	globalIndexer *IndexerManager
	globalErr     error
)

func InitGlobalIndexer(ctx context.Context, cli client.Client, embedder embedding.Embedder) error {
	globalOnce.Do(func() {
		globalIndexer = NewIndexerManager(cli, embedder)
	})
	return globalErr
}

func GetIndexer() *IndexerManager {
	return globalIndexer
}

func NewIndexerManager(cli client.Client, emb embedding.Embedder) *IndexerManager {
	return &IndexerManager{
		indexers: make(map[string]*milvus.Indexer, 5),
		cli:      cli,
		emb:      emb,
	}
}

func (m *IndexerManager) GetOrInitIndexer(ctx context.Context, collectionName string) (*milvus.Indexer, error) {
	m.mu.RLock()
	indexer, ok := m.indexers[collectionName]
	m.mu.RUnlock()
	if ok {
		return indexer, nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if indexer, ok := m.indexers[collectionName]; ok {
		return indexer, nil
	}

	newIndexer, err := InitMilvusIndexer(ctx, m.cli, m.emb, collectionName)
	if err != nil {
		return nil, err
	}
	m.indexers[collectionName] = newIndexer
	return newIndexer, nil
}
