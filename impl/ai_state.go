package impl

import (
	"context"
	"fmt"
	indexer2 "git.pinquest.cn/base/aiadminserver/impl/internal/rag/indexer"
	"git.pinquest.cn/base/aiadminserver/impl/internal/rag/retriever"
	"git.pinquest.cn/base/aiadminserver/impl/pkg/llm"
	"github.com/milvus-io/milvus-sdk-go/v2/client"
)

type StateMilvusConf struct {
	MilvusAddr string `validate:"required"`
}

func InitStateMilvusIndexer() error {
	ctx := context.Background()
	cli, err := client.NewClient(ctx, client.Config{Address: S.Conf.MilvusAddr})
	if err != nil {
		return fmt.Errorf("Milvus 客户端(Indexer)连接失败: %w", err)
	}

	providerType := S.Conf.DefaultEmbeddingProviderID
	modelName := S.Conf.DefaultEmbeddingModelName

	if providerType == "" || modelName == "" {
		return fmt.Errorf("aiadmin.toml 中未配置 DefaultEmbeddingProviderID 或 DefaultEmbeddingModelName")
	}
	embedder, err := llm.GetEmbedder(ctx, providerType, modelName)
	if err != nil {
		return fmt.Errorf("为 Indexer 初始化默认 Embedder (ProviderType: %s, Model: %s) 失败: %w", providerType, modelName, err)
	}

	_, err = embedder.EmbedStrings(ctx, []string{"hello"}) // 测试
	if err != nil {
		return fmt.Errorf("默认 embedder (for Indexer) 测试失败: %w", err)
	}
	fmt.Println("默认 Embedder (for Indexer) 初始化并测试成功。")
	return indexer2.InitGlobalIndexer(ctx, cli, embedder)
}

func InitStateMilvusRetriever() error {
	ctx := context.Background()
	cli, err := client.NewClient(ctx, client.Config{Address: S.Conf.MilvusAddr})
	if err != nil {
		return fmt.Errorf("Milvus 客户端(Retriever)连接失败: %w", err)
	}

	providerType := S.Conf.DefaultEmbeddingProviderID
	modelName := S.Conf.DefaultEmbeddingModelName

	if providerType == "" || modelName == "" {
		return fmt.Errorf("aiadmin.toml 中未配置 DefaultEmbeddingProviderID 或 DefaultEmbeddingModelName")
	}
	embedder, err := llm.GetEmbedder(ctx, providerType, modelName)
	if err != nil {
		return fmt.Errorf("为 Retriever 初始化默认 Embedder (ProviderType: %s, Model: %s) 失败: %w", providerType, modelName, err)
	}

	_, err = embedder.EmbedStrings(ctx, []string{"world"}) // 测试
	if err != nil {
		return fmt.Errorf("默认 embedder (for Retriever) 测试失败: %w", err)
	}
	fmt.Println("默认 Embedder (for Retriever) 初始化并测试成功。")
	return retriever.InitGlobalRetrieverManager(cli, embedder)
}

// InitStateLLM 负责将加载好的配置注入到 llm 工厂中
func InitStateLLM() error {
	if S.Conf != nil && S.Conf.LLMProviders != nil {
		llm.InitFactory(S.Conf.LLMProviders)
		// 增加日志来确认
		fmt.Printf("LLM Providers Config Loaded: %+v\n", S.Conf.LLMProviders)
		for key, conf := range S.Conf.LLMProviders {
			fmt.Printf("  Provider ID: %s, Config: %+v\n", key, conf)
		}
	}
	return nil
}
