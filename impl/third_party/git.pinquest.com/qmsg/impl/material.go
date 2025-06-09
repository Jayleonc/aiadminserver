package impl

import (
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/core"
	"git.pinquest.cn/qlb/material"
	"git.pinquest.cn/qlb/qmsg"
	"git.pinquest.cn/qlb/quan"
)

// 检查小程序收录结果
func CheckMiniProgramRecruit(ctx *rpc.Context, req *qmsg.CheckMiniProgramRecruitReq) (*qmsg.CheckMiniProgramRecruitRsp, error) {
	var rsp qmsg.CheckMiniProgramRecruitRsp

	if req.MiniProgramAppId == "" || req.MiniProgramPath == "" {
		return nil, rpc.CreateErrorWithMsg(qmsg.ErrLenZero, "小程序APPID与路径不可为空！")
	}

	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)

	miniProgramRsp, err := quan.GetMiniProgramByAppIdAndPathSys(ctx, &quan.GetMiniProgramByAppIdAndPathSysReq{
		CorpId:           corpId,
		AppId:            appId,
		MiniProgramAppId: req.MiniProgramAppId,
		MiniProgramPath:  req.MiniProgramPath,
	})
	if err != nil {
		if rpc.GetErrCode(err) != quan.ErrMiniProgramPagePathNotFound &&
			rpc.GetErrCode(err) != quan.ErrMaterialNotFound &&
			rpc.GetErrCode(err) != quan.ErrMaterialYcPagePathNotFound &&
			rpc.GetErrCode(err) != quan.ErrModelMiniProgramNotFound {
			log.Errorf("err:%v", err)
			return nil, err
		}
	}
	if miniProgramRsp != nil &&
		miniProgramRsp.MaterialYcPagePath != nil &&
		miniProgramRsp.MaterialYcPagePath.SosObjectId != "" {
		log.Infof("SosObjectId=%s", miniProgramRsp.MaterialYcPagePath.SosObjectId)

		rsp.SosObjectId = miniProgramRsp.MaterialYcPagePath.SosObjectId
	} else {
		log.Infof("Not Found miniProgram SosObjectId!")
	}

	return &rsp, nil
}

// 检查视频号收录结果
func CheckChannelMsgRecruit(ctx *rpc.Context, req *qmsg.CheckChannelMsgRecruitReq) (*qmsg.CheckChannelMsgRecruitRsp, error) {
	var rsp qmsg.CheckChannelMsgRecruitRsp

	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)

	// 调用素材库查询，此方法会自动同步不同扫码号
	materialCheckRsp, err := material.CheckChannelMsgRecruitSys(ctx, &material.CheckChannelMsgRecruitSysReq{
		CorpId:        corpId,
		AppId:         appId,
		RobotSerialNo: req.RobotSerialNo,
		MsgSerialNo:   req.MsgSerialNo,
	})
	if err != nil {
		if rpc.GetErrCode(err) != material.ErrChannelMsgContentNotFound &&
			rpc.GetErrCode(err) != material.ErrChannelMsgContentSyncHistoryNotFound {
			log.Errorf("err:%v", err)
			return nil, err
		}
	}
	if materialCheckRsp != nil {
		rsp.MsgSerialNo = materialCheckRsp.MsgSerialNo
	}

	return &rsp, nil
}
