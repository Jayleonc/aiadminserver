package impl

import (
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/qmsg"
	"git.pinquest.cn/qlb/quan"
)

func GetCurrentUser(ctx *rpc.Context, req *qmsg.GetCurrentUserReq) (*qmsg.GetCurrentUserRsp, error) {
	var rsp qmsg.GetCurrentUserRsp

	u, err := getUserCheck(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	// 获取企业信息
	corpRsp, err := quan.GetCorpByCorpId(ctx, &quan.GetCorpByCorpIdReq{
		CorpId: u.CorpId,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	// 客服账号信息
	rsp.User = u
	// 企业信息
	if corpRsp.Corp != nil {
		rsp.Corp = &qmsg.GetCurrentUserRsp_CorpItem{
			CorpId:        corpRsp.Corp.CorpId,
			AppId:         corpRsp.Corp.AppId,
			CorpName:      corpRsp.Corp.Name,
			CorpAliasName: corpRsp.Corp.AliasName,
		}
	}

	rsp.UserCategory, err = getUserCategory(ctx, u)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	return &rsp, nil
}
