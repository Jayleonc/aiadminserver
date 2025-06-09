// impl/pkg/llm/provider_tongyi.go
package llm

func init() {
	tongyiModels := []*Model{
		// --- 商业版 Chat Models ---
		{
			// Id:          "qwen-plus",
			Model:       "qwen-plus",
			Type:        ModelTypeChat,
			Provider:    ProviderTongyi,
			Description: "能力均衡，适合中等复杂任务，性价比高。",
		},
		{
			// Id:          "qwen-max",
			Model:       "qwen-max",
			Type:        ModelTypeChat,
			Provider:    ProviderTongyi,
			Description: "系列效果最好的模型，适合复杂的创作和多步骤任务。",
		},
		{
			// Id:          "qwen-turbo",
			Model:       "qwen-turbo",
			Type:        ModelTypeChat,
			Provider:    ProviderTongyi,
			Description: "响应速度最快，成本极低，适合需要快速响应的简单任务。",
		},
		{
			// Id:          "qwen-long",
			Model:       "qwen-long",
			Type:        ModelTypeChat,
			Provider:    ProviderTongyi,
			Description: "超长上下文窗口，适合处理长文档的分析、摘要和问答。",
		},
		// --- 多模态 Models ---
		{
			// Id:          "qwen-vl-plus",
			Model:       "qwen-vl-plus",
			Type:        ModelTypeChat,
			Provider:    ProviderTongyi,
			Description: "具备强大的视觉（图像）理解能力。",
		},
		// --- Embedding Models ---
		{
			// Id:          "text-embedding-v1",
			Model:       "text-embedding-v1",
			Type:        ModelTypeEmbedding,
			Provider:    ProviderTongyi,
			Description: "通用文本向量模型-v1。",
		},
		{
			// Id:          "text-embedding-v2",
			Model:       "text-embedding-v2",
			Type:        ModelTypeEmbedding,
			Provider:    ProviderTongyi,
			Description: "通用文本向量模型-v2。",
		},
	}

	RegisterModels(ProviderTongyi, tongyiModels)
}
