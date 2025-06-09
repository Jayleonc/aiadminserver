package impl

import (
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/brick/utils/dynamicconfiguration"
)

var dynamicConfig *dynamicconfiguration.DynamicConfiguration

type Config struct {
	rpc.ServerConfig
	StateDbConf
	StateRedisConf
	// add fields below
	Db              string
	DbMsgBox        string
	MerchantKey     string
	ResourceType    string
	ResourceType4CI string
}

func InitConfig() error {
	cfgPath := rpc.GetConfigFileOrDie()
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
