package impl

import (
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/brick/utils"
	"git.pinquest.cn/qlb/core"
	"git.pinquest.cn/qlb/iquan"
	"git.pinquest.cn/qlb/qcustomer"
	"git.pinquest.cn/qlb/qrobot"
	"git.pinquest.cn/qlb/quan"
	"git.pinquest.cn/qlb/qwhale"
)

type qrobotService struct {
}

func (_ *qrobotService) getAccountByRobotUid(ctx *rpc.Context, corpId, appId uint32, robotUid uint64) (*qrobot.ModelAccount, error) {
	robotRsp, err := qrobot.GetAccountByRobotUidSys(ctx, &qrobot.GetAccountByRobotUidSysReq{
		RobotUid: robotUid,
		CorpId:   corpId,
		AppId:    appId,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	return robotRsp.Account, nil
}

func (_ *qrobotService) getAccountByStrOpenId(ctx *rpc.Context, corpId, appId uint32, robotSerialNo string) (*qrobot.ModelAccount, error) {
	robotRsp, err := qrobot.GetAccountByStrOpenId(ctx, &qrobot.GetAccountByStrOpenIdReq{
		StrOpenId: robotSerialNo,
		CorpId:    corpId,
		AppId:     appId,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	return robotRsp.Account, nil
}

func (_ *qrobotService) getAccount(ctx *rpc.Context, corpId, appId uint32, chatExtId uint64) (*qrobot.ModelAccount, error) {
	accountRsp, err := qrobot.GetAccountSys(ctx, &qrobot.GetAccountSysReq{
		CorpId: corpId,
		AppId:  appId,
		Id:     chatExtId,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	return accountRsp.Account, nil
}

func (_ *qrobotService) getQRobotByStrOpenId(ctx *rpc.Context, robotSerialNo string) (*qrobot.ModelQRobot, error) {
	rsp, err := qrobot.GetQRobotListSys(ctx, &qrobot.GetQRobotListSysReq{
		ListOption: &core.ListOption{
			Options: []*core.ListOption_Option{
				{
					Type:  int32(qrobot.GetQRobotListSysReq_ListOptionStrRobotId),
					Value: robotSerialNo,
				},
			},
		},
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	if len(rsp.List) == 0 {
		return nil, nil
	}
	var robotMap map[string]*qrobot.ModelQRobot
	utils.KeyBy(rsp.List, "StrRobotId", &robotMap)

	robot := robotMap[robotSerialNo]
	return robot, nil
}

func (_ *qrobotService) getQRobotByUid(ctx *rpc.Context, corpId, appId uint32, uid uint64) (*qrobot.ModelQRobot, error) {
	uidList := []uint64{uid}
	rsp, err := qrobot.GetQRobotListSys(ctx, &qrobot.GetQRobotListSysReq{
		CorpId:     corpId,
		AppId:      appId,
		ListOption: core.NewListOption().AddOpt(qrobot.GetQRobotListSysReq_ListOptionUidList, uidList),
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	if len(rsp.List) == 0 {
		return nil, nil
	}
	var robotMap map[uint64]*qrobot.ModelQRobot
	utils.KeyBy(rsp.List, "Uid", &robotMap)
	robot := robotMap[uid]
	return robot, nil
}

func (_ qrobotService) getAccountListByNameOrRemark(ctx *rpc.Context, corpId, appId uint32, name string, robotUidList []uint64) ([]*qrobot.ModelAccount, []uint64, error) {
	listoption := core.NewListOption().SetLimit(2000).AddOpt(qcustomer.FollowListOption_NameOrRemarkLike, name)
	if len(robotUidList) > 0 {
		listoption.AddOpt(qcustomer.FollowListOption_UidList, robotUidList)
	}
	extContactFollowRsp, err := qcustomer.GetExtContactFollowList(ctx, &qcustomer.GetExtContactFollowListReq{
		CorpId:     corpId,
		AppId:      appId,
		ListOption: listoption,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, nil, err
	}
	if extContactFollowRsp == nil || len(extContactFollowRsp.List) == 0 {
		return nil, nil, nil
	}
	uidList := utils.PluckUint64(extContactFollowRsp.List, "Uid")
	extUidList := utils.PluckUint64(extContactFollowRsp.List, "ExtUid")
	accountListRsp, err := qrobot.GetAccountListSys(ctx, &qrobot.GetAccountListSysReq{
		CorpId:     corpId,
		AppId:      appId,
		ListOption: core.NewListOption().SetSkipCount().SetLimit(uint32(len(extUidList))).AddOpt(qrobot.GetAccountListSysReq_ListOptionExtUidList, extUidList),
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, uidList, err
	}
	if accountListRsp == nil || len(accountListRsp.List) == 0 {
		return nil, uidList, nil
	}
	return accountListRsp.List, uidList, nil

}

type qwhaleService struct {
}

func (_ *qwhaleService) getHostAccountBySn(ctx *rpc.Context, robotSerialNo string) (*qwhale.ModelHostAccount, error) {
	hostAccountRsp, err := qwhale.GetHostAccountBySn(ctx, &qwhale.GetHostAccountBySnReq{
		StrRobotId: robotSerialNo,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	return hostAccountRsp.HostAccount, nil
}

func (_ *qwhaleService) getHostAccountByUid(ctx *rpc.Context, corpId, appId uint32, uid uint64) (*qwhale.ModelHostAccount, error) {
	hostAccountRsp, err := qwhale.GetHostAccountListSys(ctx, &qwhale.GetHostAccountListSysReq{
		CorpId:     corpId,
		AppId:      appId,
		ListOption: core.NewListOption().SetSkipCount().AddOpt(qwhale.GetHostAccountListSysReq_ListOptionUidList, []uint64{uid}),
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	if len(hostAccountRsp.List) == 0 {
		return nil, nil
	}

	return hostAccountRsp.List[0], nil
}

type quanService struct {
}

func (t quanService) getAccountFollowList(ctx *rpc.Context, corpId, appId uint32, robotUidList []uint64,
	extUidList []uint64) ([]*quan.ModelExtContactFollow, error) {
	followListRsp, err := iquan.GetExtContactFollowList(ctx, &iquan.GetExtContactFollowListReq{
		ListOption: core.NewListOption().
			AddOpt(iquan.GetExtContactFollowListReq_ListOptionExtUidList, extUidList).
			AddOpt(iquan.GetExtContactFollowListReq_ListOptionUidList, robotUidList),
		CorpId:             corpId,
		AppId:              appId,
		CancelOrderByAddAt: true,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	if followListRsp == nil || len(followListRsp.List) == 0 {
		log.Warnf("warn: followList is empty: %v, params: %v, %v", followListRsp, robotUidList, extUidList)
		return nil, nil
	}
	return followListRsp.List, nil
}

func (t quanService) getAccountFollowMap(ctx *rpc.Context, corpId, appId uint32, robotUidList []uint64,
	extUidList []uint64) (map[string]*quan.ModelExtContactFollow, error) {
	followList, err := t.getAccountFollowList(ctx, corpId, appId, robotUidList, extUidList)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	var extContactMap = make(map[string]*quan.ModelExtContactFollow)
	if len(followList) > 0 {
		for _, follow := range followList {
			key := utils.JoinUint64([]uint64{follow.Uid, follow.ExtUid}, "_")
			extContactMap[key] = follow
		}
	}
	return extContactMap, nil
}
