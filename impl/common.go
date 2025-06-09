package impl

import (
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/core"
	"git.pinquest.cn/qlb/quan"
)

func GetCorpAndAppIdAndUid(ctx *rpc.Context) (*rpc.Context, uint32, uint32, uint64, error) {
	if isLocalEnv(ctx) {
		// 本地开发环境直接返回固定 corpId 和 appId
		ctx = InitTestEnvWithCorp(2000772, 2000273, 1699)
		return ctx, 2000772, 2000273, 1699, nil
	}

	// 正式环境走鉴权逻辑
	user, err := quan.GetUserCheckSys(ctx, &quan.GetUserCheckSysReq{})
	if err != nil {
		return nil, 0, 0, 0, err
	}
	return ctx, user.User.CorpId, user.User.AppId, user.User.Id, nil
}

func isLocalEnv(ctx *rpc.Context) bool {
	// 可根据环境变量或 ctx 中标记判断是否为本地环境，仅我本地开发使用
	id := core.GetHeaderUId(ctx)
	if id == "dev" {
		return true
	}
	return false
}
