package impl

import (
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/brick/utils"
	"git.pinquest.cn/qlb/core"
	"git.pinquest.cn/qlb/iquan"
	"git.pinquest.cn/qlb/qmsg"
	"git.pinquest.cn/qlb/qrobot"
	"git.pinquest.cn/qlb/quan"
	"git.pinquest.cn/qlb/qwhale"
)

func GetStaffList(ctx *rpc.Context, req *qmsg.GetStaffListReq) (*qmsg.GetStaffListRsp, error) {
	var rsp qmsg.GetStaffListRsp
	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)
	listOption := req.ListOption.CloneSkipOpts()
	err := core.NewListOptionProcessor(req.ListOption).
		AddBool(
			qmsg.GetStaffListReq_ListOptionIsRobot,
			func(val bool) error {
				if val {
					listOption.AddOpt(iquan.GetUserListReq_ListOptionRobotType, iquan.GetUserListReq_RobotTypeRobot)
				} else {
					listOption.AddOpt(iquan.GetUserListReq_ListOptionRobotType, iquan.GetUserListReq_RobotTypeNormal)
				}
				return nil
			},
		).
		AddString(
			qmsg.GetStaffListReq_ListOptionName,
			func(val string) error {
				listOption.AddOpt(iquan.GetUserListReq_ListOptionName, val)
				return nil
			},
		).
		Process()
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	iRsp, err := iquan.GetUserList(ctx, &iquan.GetUserListReq{
		ListOption: listOption,
		CorpId:     corpId,
		AppId:      appId,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	rsp.Paginate = iRsp.Paginate
	if len(iRsp.List) > 0 {
		rsp.List, err = GetStaffListByUserList(ctx, corpId, appId, iRsp.List)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
	}
	return &rsp, nil
}
func GetStaffListByUidList(ctx *rpc.Context, corpId, appId uint32, uidList []uint64) (list []*qmsg.Staff, err error) {
	if len(uidList) == 0 {
		return
	}
	iRsp, err := iquan.GetUserList(ctx, &iquan.GetUserListReq{
		ListOption: core.NewListOption().SetSkipCount().AddOpt(iquan.GetUserListReq_ListOptionUidList, uidList),
		CorpId:     corpId,
		AppId:      appId,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	list, err = GetStaffListByUserList(ctx, corpId, appId, iRsp.List)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	return
}
func GetStaffListByUserList(ctx *rpc.Context, corpId, appId uint32, userList []*quan.ModelUser) (list []*qmsg.Staff, err error) {
	if len(userList) == 0 {
		return
	}
	// 扫码号的处理
	hostAccMap := map[uint64]*qwhale.ModelHostAccount{}
	{
		uidList := utils.PluckUint64(quan.UserList(userList).
			FilterNot(func(user *quan.ModelUser) bool {
				return user.IsRobot
			}), "Id")
		if len(uidList) > 0 {
			qRsp, err := qwhale.GetHostAccountListSys(ctx, &qwhale.GetHostAccountListSysReq{
				ListOption: core.NewListOption().SetSkipCount().
					AddOpt(qwhale.GetHostAccountListSysReq_ListOptionUidList, uidList),
				CorpId: corpId,
				AppId:  appId,
			})
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, err
			}
			utils.KeyBy(qRsp.List, "Uid", &hostAccMap)
		}
	}
	// 平台号的处理
	robotMap := map[uint64]*qrobot.ModelQRobot{}
	{
		uidList := utils.PluckUint64(quan.UserList(userList).
			Filter(func(user *quan.ModelUser) bool {
				return user.IsRobot
			}), "Id")
		if len(uidList) > 0 {
			qRsp, err := qrobot.GetQRobotListSys(ctx, &qrobot.GetQRobotListSysReq{
				ListOption: core.NewListOption().SetSkipCount().
					AddOpt(qrobot.GetQRobotListSysReq_ListOptionUidList, uidList),
				CorpId: corpId,
				AppId:  appId,
			})
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, err
			}
			utils.KeyBy(qRsp.List, "Uid", &robotMap)
		}
	}
	distributedMap := map[uint64]bool{}
	{
		uidList := utils.PluckUint64(userList, "Id")
		if len(uidList) > 0 {
			var list []*qmsg.ModelUserRobot
			err = UserRobot.Where(map[string]interface{}{
				DbCorpId:           corpId,
				DbAppId:            appId,
				DbRobotUid + " IN": uidList,
			}).Find(ctx, &list)
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, err
			}
			distributedMap = utils.PluckUint64Map(list, "RobotUid")
		}
	}
	quan.UserList(userList).Each(func(user *quan.ModelUser) {
		staff := &qmsg.Staff{
			Uid:           user.Id,
			Profile:       mergeProfile4User(nil, user),
			IsDistributed: distributedMap[user.Id],
		}
		robot := robotMap[user.Id]
		if robot != nil {
			staff.IsRobot = true
			switch qrobot.ModelQRobot_RobotState(robot.RobotState) {
			case qrobot.ModelQRobot_RobotStateNil, qrobot.ModelQRobot_RobotStateNormal:
				staff.IsOnline = true
				staff.State = uint32(qmsg.Staff_StateNormal)
			case qrobot.ModelQRobot_RobotStateBaned:
				staff.State = uint32(qmsg.Staff_StateBaned)
			}
		}
		hostAcc := hostAccMap[user.Id]
		if hostAcc != nil {
			staff.IsOnline = hostAcc.IsOnline
			staff.State = uint32(qmsg.Staff_StateNormal)
		}
		list = append(list, staff)
	})
	return
}
