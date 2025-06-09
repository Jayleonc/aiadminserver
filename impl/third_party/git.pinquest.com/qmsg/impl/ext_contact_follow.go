package impl

import (
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/iquan"
	"git.pinquest.cn/qlb/qmsg"
)

func GetExtContactFollow(ctx *rpc.Context, req *qmsg.GetExtContactFollowReq) (*qmsg.GetExtContactFollowRsp, error) {
	var rsp qmsg.GetExtContactFollowRsp
	user, err := getUserCheck(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	iquanRsp, err := iquan.GetExtContactFollow(ctx, &iquan.GetExtContactFollowReq{
		CorpId:    user.CorpId,
		AppId:     user.AppId,
		ExtUid:    req.ExtUid,
		Uid:       req.RobotUid,
		IsNeedTag: true,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	rsp.Follow = iquanRsp.Follow
	return &rsp, nil
}
