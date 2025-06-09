package impl

import (
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/qmsg"
	"git.pinquest.cn/qlb/qycfile"
)

func GetYcFile(ctx *rpc.Context, req *qmsg.GetYcFileReq) (*qmsg.GetYcFileRsp, error) {
	var rsp qmsg.GetYcFileRsp

	user, err := getUserCheck(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	if user.IsAssign {
		err = ensureHasRobotPerm(ctx, user, req.RobotUid)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
	}

	fileRsp, err := qycfile.GetYcFileWithUid(ctx, &qycfile.GetYcFileWithUidReq{
		Id:       req.YcFileId,
		RobotUid: req.RobotUid,
		CorpId:   user.CorpId,
		AppId:    user.AppId,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	rsp.YcFile = fileRsp.YcFile
	return &rsp, nil
}

func RetryDownloadYcFile(ctx *rpc.Context, req *qmsg.RetryDownloadYcFileReq) (*qmsg.RetryDownloadYcFileRsp, error) {
	var rsp qmsg.RetryDownloadYcFileRsp

	user, err := getUserCheck(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	err = ensureHasRobotPerm(ctx, user, req.RobotUid)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	ycRsp, err := qycfile.RetryWithUid(ctx, &qycfile.RetryWithUidReq{
		Id:       req.YcFileId,
		RobotUid: req.RobotUid,
		CorpId:   user.CorpId,
		AppId:    user.AppId,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	rsp.YcFile = ycRsp.YcFile
	return &rsp, nil
}
