package impl

import (
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/core"
	"git.pinquest.cn/qlb/qrobot"
	"git.pinquest.cn/qlb/qwhale"
)

type StructRobot struct{}

var Robot = &StructRobot{}

func (robot *StructRobot) GetRobotListByRobotUid(ctx *rpc.Context, corpId, appId uint32, robotUid []uint64) (qrobot.RobotList, error) {
	if len(robotUid) == 0 {
		return qrobot.RobotList{}, nil
	}
	rsp, err := qrobot.GetQRobotListSys(ctx, &qrobot.GetQRobotListSysReq{
		ListOption: core.NewListOption().SetSkipCount().
			AddOpt(uint32(qrobot.GetQRobotListSysReq_ListOptionUidList), robotUid),
		CorpId: corpId,
		AppId:  appId,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	return rsp.List, nil
}
func (robot *StructRobot) GetHostAccountListByAccUid(ctx *rpc.Context, corpId, appId uint32, robotUid []uint64) (qwhale.HostAccountList, error) {
	if len(robotUid) == 0 {
		return qwhale.HostAccountList{}, nil
	}
	rsp, err := qwhale.GetHostAccountListSys(ctx, &qwhale.GetHostAccountListSysReq{
		ListOption: core.NewListOption().SetSkipCount().
			AddOpt(uint32(qwhale.GetHostAccountListSysReq_ListOptionUidList), robotUid),
		CorpId: corpId,
		AppId:  appId,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	return rsp.List, nil
}
func (robot *StructRobot) GetRobotListByUid(ctx *rpc.Context, corpId, appId uint32, uid uint64) (qrobot.RobotList, error) {
	robotUid, err := UserRobot.GetRobotUidList(ctx, corpId, appId, uid)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	if len(robotUid) == 0 {
		return nil, err
	}
	list, err := robot.GetRobotListByRobotUid(ctx, corpId, appId, robotUid)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	return list, nil
}
func (robot *StructRobot) GetHostAccountListByUid(ctx *rpc.Context, corpId, appId uint32, uid uint64) ([]*qwhale.ModelHostAccount, error) {
	robotUid, err := UserRobot.GetHostAccountUidList(ctx, corpId, appId, uid)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	if len(robotUid) == 0 {
		return nil, err
	}
	list, err := robot.GetHostAccountListByAccUid(ctx, corpId, appId, robotUid)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	return list, nil
}
