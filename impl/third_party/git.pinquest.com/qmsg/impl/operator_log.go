package impl

import (
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/qmsg"
)

// AddLogoutOperatorLog 退出登录操作日志
func AddLogoutOperatorLog(ctx *rpc.Context, _ *qmsg.AddLogoutOperatorLogReq) (*qmsg.AddLogoutOperatorLogRsp, error) {
	var rsp qmsg.AddLogoutOperatorLogRsp

	u, err := getUserCheck(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	_, err = OperatorLog.create(ctx, u.CorpId, u.AppId, u.Id, qmsg.UserLog_LogTypeLogout)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	return &rsp, nil
}
