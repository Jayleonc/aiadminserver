package indexer

import (
	"context"
	openaiembed "github.com/cloudwego/eino-ext/components/embedding/openai"
	"github.com/cloudwego/eino/components/embedding"

	"time"
)

func NewEmbedderFromConf(model string) (embedding.Embedder, error) {
	//apiKey := os.Getenv("OPENAI_API_KEY")
	//
	//if apiKey == "" {
	//	return nil, fmt.Errorf("OPENAI_API_KEY is not set in environment")
	//}

	cfg := &openaiembed.EmbeddingConfig{
		APIKey:  "sk-49e83005efca4e51b85d5225d7feee0e",
		Model:   model,
		Timeout: 10 * time.Second,
		BaseURL: "https://dashscope.aliyuncs.com/compatible-mode/v1",
	}

	return openaiembed.NewEmbedder(context.Background(), cfg)
}
