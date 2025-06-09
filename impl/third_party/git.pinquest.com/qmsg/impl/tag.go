package impl

import (
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/qcorptagmgr"
	"git.pinquest.cn/qlb/qmsg"
	"git.pinquest.cn/qlb/quan"
)

func GetCorpTagList(ctx *rpc.Context, req *qmsg.GetCorpTagListReq) (*qmsg.GetCorpTagListRsp, error) {
	var rsp qmsg.GetCorpTagListRsp
	user, err := getUserCheck(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	tagRsp, err := qcorptagmgr.GetCorpTagListSys(ctx, &qcorptagmgr.GetCorpTagListSysReq{
		CorpId: user.CorpId,
		AppId:  user.AppId,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	rsp.TagGroupList = tagMgr2QuanCorpTagGroupList(tagRsp.TagGroupList)
	return &rsp, nil
}

func BatchSetCorpTag2Follow(ctx *rpc.Context, req *qmsg.BatchSetCorpTag2FollowReq) (*qmsg.BatchSetCorpTag2FollowRsp, error) {
	var rsp qmsg.BatchSetCorpTag2FollowRsp
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
	quanRsp, err := quan.BatchSetCorpTag2FollowSys(ctx, &quan.BatchSetCorpTag2FollowSysReq{
		CorpId:       user.CorpId,
		AppId:        user.AppId,
		FollowIdList: req.FollowIdList,
		TagIdList:    req.TagIdList,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	rsp.TaskKey = quanRsp.TaskKey
	return &rsp, nil
}
func SetCorpTag(ctx *rpc.Context, req *qmsg.SetCorpTagReq) (*qmsg.SetCorpTagRsp, error) {
	var rsp qmsg.SetCorpTagRsp
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
	tagRsp, err := qcorptagmgr.SetCorpTagSys(ctx, &qcorptagmgr.SetCorpTagSysReq{
		TagGroup: quan2TagMgrCorpTagGroup(req.TagGroup),
		CorpId:   user.CorpId,
		AppId:    user.AppId,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	rsp.TagGroup = TagMgr2QuanCorpTagGroup(tagRsp.TagGroup)
	return &rsp, nil
}
func AddCorpTag(ctx *rpc.Context, req *qmsg.AddCorpTagReq) (*qmsg.AddCorpTagRsp, error) {
	var rsp qmsg.AddCorpTagRsp
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
	tagRsp, err := qcorptagmgr.AddCorpTagSys(ctx, &qcorptagmgr.AddCorpTagSysReq{
		TagGroup: quan2TagMgrCorpTagGroup(req.TagGroup),
		CorpId:   user.CorpId,
		AppId:    user.AppId,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	rsp.TagGroup = TagMgr2QuanCorpTagGroup(tagRsp.TagGroup)
	return &rsp, nil
}
