package retriever

import (
	"fmt"
	"sync"

	"github.com/cloudwego/eino/components/embedding"
	"github.com/milvus-io/milvus-sdk-go/v2/client"
)

// RetrieverManager 用于管理和复用共享资源，并按需创建 Retriever 实例。
type RetrieverManager struct {
	mu         sync.RWMutex
	retrievers map[string]*MilvusRetriever // 缓存已经创建的 retriever 实例
	cli        client.Client               // 【共享资源】复用的 Milvus client
	emb        embedding.Embedder          // 【共享资源】复用的 Embedding model
}

var (
	globalOnce    sync.Once
	globalManager *RetrieverManager
)

// InitGlobalRetrieverManager 在服务启动时初始化全局的管理器。
// cli 和 embedder 应该在更高层被创建并传入，这是一种标准的依赖注入模式。
func InitGlobalRetrieverManager(cli client.Client, embedder embedding.Embedder) error {
	var initErr error
	globalOnce.Do(func() {
		if cli == nil || embedder == nil {
			initErr = fmt.Errorf("初始化 RetrieverManager 失败: Milvus client 或 embedder 为 nil")
			return
		}
		globalManager = &RetrieverManager{
			retrievers: make(map[string]*MilvusRetriever),
			cli:        cli,
			emb:        embedder,
		}
	})
	return initErr
}

// GetRetrieverManager 获取全局的 RetrieverManager 实例。
func GetRetrieverManager() *RetrieverManager {
	return globalManager
}

// GetRetrieverOptions 封装了获取 Retriever 时的可选参数
type GetRetrieverOptions struct {
	Embedder embedding.Embedder
}

// GetRetrieverOption 是一个函数类型，用于修改 GetRetrieverOptions
type GetRetrieverOption func(*GetRetrieverOptions)

// WithEmbedder 是一个选项函数，用于指定一个特定的 Embedder
func WithEmbedder(emb embedding.Embedder) GetRetrieverOption {
	return func(o *GetRetrieverOptions) {
		o.Embedder = emb
	}
}

// GetRetriever 根据 collectionName 获取一个配置好的 MilvusRetriever 实例。
// 这个方法现在是 MilvusRetriever 的工厂。
func (m *RetrieverManager) GetRetriever(collectionName string, opts ...GetRetrieverOption) (*MilvusRetriever, error) {
	if m == nil {
		return nil, fmt.Errorf("RetrieverManager 尚未初始化")
	}

	// 1. 解析选项
	options := &GetRetrieverOptions{}
	for _, opt := range opts {
		opt(options)
	}

	// 2. 如果指定了特定的 Embedder，则不使用缓存，直接创建新实例
	if options.Embedder != nil {
		config := &MilvusRetrieverConfig{
			Client:         m.cli,
			Embedder:       options.Embedder, // 使用传入的 Embedder
			CollectionName: collectionName,
		}
		return NewMilvusRetriever(config)
	}

	// 3. 默认行为：使用缓存或创建并缓存使用【默认 Embedder】的实例
	m.mu.RLock()
	retriever, ok := m.retrievers[collectionName]
	m.mu.RUnlock()
	if ok {
		return retriever, nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if retriever, ok := m.retrievers[collectionName]; ok {
		return retriever, nil
	}

	// 使用新的构造函数和配置对象来创建实例
	config := &MilvusRetrieverConfig{
		Client:         m.cli,
		Embedder:       m.emb,
		CollectionName: collectionName,
	}
	newRetriever, err := NewMilvusRetriever(config)
	if err != nil {
		return nil, fmt.Errorf("创建默认 Retriever 失败 for collection '%s': %w", collectionName, err)
	}

	m.retrievers[collectionName] = newRetriever
	return newRetriever, nil
}
