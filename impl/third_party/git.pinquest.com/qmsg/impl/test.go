package impl

import (
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/redisgroup"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/core"
	"git.pinquest.cn/qlb/envconfig"
	"os"
)

func InitTestEnv() {
	var err error
	envCfg, err := envconfig.LoadConfig()
	if err != nil {
		log.Error(err)
		os.Exit(-1)
	}
	s = &State{
		Conf: &Config{
			StateRedisConf: StateRedisConf{
				RedisSvrAutoGen: "redis4session",
			},
			Db:       "$dispatch.mysql.biz",
			DbMsgBox: "$dispatch.mysql.msg",
		},
	}
	s.RedisGroup, err = redisgroup.New(s.Conf.RedisSvrAutoGen, envCfg.RedisPassword)
	if err != nil {
		log.Errorf("connect redis %s err %s", s.Conf.RedisSvrAutoGen, err)
		return
	}
}

func InitTestEnvWithCorp(corpId, appId uint32, uid uint64) *rpc.Context {
	InitTestEnv()

	ctx := &rpc.Context{}
	core.SetCorpAndApp(ctx, corpId, appId)

	ctx.SetSpecHttpReqHeader(core.HeaderUid, uid)
	ctx.SetSpecHttpReqHeader(core.PinHeaderUid, uid)
	ctx.SetSpecHttpReqHeader(core.PinHeaderAccountId, uid)

	return ctx
}
