package impl

import (
	"fmt"
	"git.pinquest.cn/base/aiadminserver/impl/pkg/llm"
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/brick/utils/dynamicconfiguration"
)

var dynamicConfig *dynamicconfiguration.DynamicConfiguration

// StateLLMConf 对应 TOML 中的 [LLMProviders] 表
type StateLLMConf struct {
	// 【关键】引用的类型是 llm.LLMProviderConfig
	LLMProviders map[string]llm.LLMProviderConfig `json:"LLMProviders"`
}

type Config struct {
	rpc.ServerConfig
	StateDbConf
	StateRedisConf
	StateMilvusConf
	StateLLMConf

	// add fields below
	Db string

	DefaultEmbeddingProviderID string `json:"DefaultEmbeddingProviderID"`
	DefaultEmbeddingModelName  string `json:"DefaultEmbeddingModelName"`
	// DefaultEmbeddingProviderAccountID 字段已移除，统一用 providerType
}

func InitConfig() error {
	cfgPath := rpc.GetConfigFileOrDie()
	fmt.Println(cfgPath)
	dj, err := dynamicconfiguration.NewDynamicConfiguration(cfgPath, &Config{})
	if err != nil {
		return err
	}
	dynamicConfig = dj
	return nil
}

func GetConfig() *Config {
	if dynamicConfig != nil {
		i := dynamicConfig.Get()
		return i.(*Config)
	}
	log.Panic("dynamicConfig is nil")
	return nil
}
