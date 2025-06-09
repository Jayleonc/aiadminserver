// file: aiadminserver/impl/pkg/llm/factory.go
package llm

import (
	"context"
	"fmt"
	openaiembed "github.com/cloudwego/eino-ext/components/embedding/openai"
	openaichat "github.com/cloudwego/eino-ext/components/model/openai"
	"github.com/cloudwego/eino/components/embedding"
	"github.com/cloudwego/eino/components/model"
	"sync"
)

var (
	// providerConfigs 存储注入的账号凭证配置
	// Key: "tongyi-main-account" (来自 toml 的逻辑名称)
	// Value: LLMProviderConfig { Provider: "tongyi", APIKey: "...", BaseURL: "..." }
	providerConfigs map[string]LLMProviderConfig

	// chatModels 缓存的是针对某个账号凭证(providerId)的通用聊天客户端
	chatModels = make(map[string]model.BaseChatModel)
	// embedders 缓存的是针对某个账号凭证(providerId)和具体模型(modelName)的Embedding客户端
	embedders = make(map[string]embedding.Embedder)
	mu        sync.RWMutex
)

// InitFactory 依赖注入的入口, 由 impl 包在启动时调用
func InitFactory(pConfs map[string]LLMProviderConfig) {
	providerConfigs = pConfs
}

// GetProviderAccountConfig 允许外部查询已注入的账号配置信息
// GetProviderAccountConfig 允许外部查询已注入的账号配置信息，现在直接通过 provider 类型获取。
func GetProviderAccountConfig(providerType string) (LLMProviderConfig, bool) {
	if providerConfigs == nil {
		return LLMProviderConfig{}, false
	}
	conf, ok := providerConfigs[providerType] // 使用 providerType 作为 key
	return conf, ok
}

// GetChatModel 根据 providerType (提供商类型，如 "tongyi") 获取一个聊天模型客户端。
// 具体模型名称将通过 model.WithModel 选项传递给 Generate 方法。
func GetChatModel(ctx context.Context, providerType string) (model.BaseChatModel, error) {
	mu.RLock()
	m, ok := chatModels[providerType]
	mu.RUnlock()
	if ok {
		return m, nil
	}

	mu.Lock()
	defer mu.Unlock()
	if m, ok := chatModels[providerType]; ok { // Double check
		return m, nil
	}

	if providerConfigs == nil {
		return nil, fmt.Errorf("LLM 工厂尚未初始化，请先调用 InitFactory")
	}
	accountConf, ok := providerConfigs[providerType] // 使用 providerType 作为 key
	if !ok {
		return nil, fmt.Errorf("LLM提供商类型 '%s' 未在 aiadmin.toml 中配置", providerType)
	}

	var chatModel model.BaseChatModel
	var err error

	// 根据传入的 providerType 来创建客户端
	switch providerType { // 直接使用传入的 providerType
	case ProviderTongyi, ProviderOpenAI:
		chatModel, err = openaichat.NewChatModel(ctx, &openaichat.ChatModelConfig{
			APIKey:  accountConf.APIKey,
			BaseURL: accountConf.BaseURL,
		})
	default:
		return nil, fmt.Errorf("不支持的聊天模型提供商类型: %s", providerType)
	}

	if err != nil {
		return nil, fmt.Errorf("为 LLM 提供商 '%s' 创建聊天模型客户端失败: %w", providerType, err)
	}

	chatModels[providerType] = chatModel
	return chatModel, nil
}

// GetEmbedder 根据 providerType (提供商类型，如 "tongyi") 和具体的 modelName 获取一个 Embedding 模型客户端
func GetEmbedder(ctx context.Context, providerType string, modelName string) (embedding.Embedder, error) {
	// Embedding 客户端通常与具体模型绑定，所以缓存键包含两者
	cacheKey := providerType + ":" + modelName
	mu.RLock()
	em, ok := embedders[cacheKey]
	mu.RUnlock()
	if ok {
		return em, nil
	}

	mu.Lock()
	defer mu.Unlock()
	if em, ok := embedders[cacheKey]; ok { // Double check
		return em, nil
	}

	if providerConfigs == nil {
		return nil, fmt.Errorf("LLM 工厂尚未初始化，请先调用 InitFactory")
	}
	accountConf, ok := providerConfigs[providerType] // 使用 providerType 作为 key
	if !ok {
		return nil, fmt.Errorf("LLM提供商类型 '%s' 未在 aiadmin.toml 中配置", providerType)
	}

	var embedderClient embedding.Embedder
	var err error

	// 根据传入的 providerType 来创建客户端
	switch providerType { // 直接使用传入的 providerType
	case ProviderTongyi, ProviderOpenAI:
		embedderClient, err = openaiembed.NewEmbedder(ctx, &openaiembed.EmbeddingConfig{
			APIKey:  accountConf.APIKey,
			BaseURL: accountConf.BaseURL,
			Model:   modelName, // 【重要】Embedding客户端创建时需要指定具体模型
		})
	default:
		return nil, fmt.Errorf("不支持的 Embedding 模型提供商类型: %s", providerType)
	}

	if err != nil {
		return nil, fmt.Errorf("为 LLM 提供商 '%s' 的模型 '%s' 创建 Embedding 模型客户端失败: %w", providerType, modelName, err)
	}

	embedders[cacheKey] = embedderClient
	return embedderClient, nil
}
