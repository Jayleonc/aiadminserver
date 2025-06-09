package llm

// LLMProviderConfig 现在只存储账号凭证信息
type LLMProviderConfig struct {
	APIKey  string `json:"APIKey" validate:"required"`
	BaseURL string `json:"BaseURL"`
}
