package impl

import (
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/core"
	"git.pinquest.cn/qlb/extrpkg/github.com/elliotchance/pie/pie"
	"git.pinquest.cn/qlb/iquan"
	"git.pinquest.cn/qlb/qmsg"
	"git.pinquest.cn/qlb/qris"
	"git.pinquest.cn/qlb/quan"
)

// getCustomerDataStrategy Customer获取客服的数据,封装相同的代码
func getCustomerDataStrategy(ctx *rpc.Context, corpId, appId uint32, customerServiceTriggerMap map[uint64]uint32, uidList []uint64) ([]*qmsg.GetMsgNotifyDataListRsp_Data, error) {
	var (
		customerServiceList []*qmsg.ModelUser
		dataList            []*qmsg.GetMsgNotifyDataListRsp_Data
	)
	// 得到客服信息
	if len(uidList) == 0 {
		return dataList, nil
	}
	err := User.WhereCorpApp(corpId, appId).WhereIn(DbId, uidList).Find(ctx, &customerServiceList)
	if err != nil {
		return nil, err
	}

	// 组合数据
	for _, customer := range customerServiceList {
		// 客服触发次数
		totalTrigger, ok := customerServiceTriggerMap[customer.Id]
		if !ok {
			log.Errorf("%v :totalTrigger not exist", customer.Id)
			totalTrigger = 0
		}
		dataList = append(dataList, &qmsg.GetMsgNotifyDataListRsp_Data{
			Name:         customer.Username,
			TotalTrigger: totalTrigger,
		})
	}
	return dataList, nil
}

// getRobotDataStrategy Robot获取成员的数据,封装相同的代码
func getRobotDataStrategy(ctx *rpc.Context, corpId, appId uint32, robotTriggerMap map[uint64]uint32, robotUid []uint64) ([]*qmsg.GetMsgNotifyDataListRsp_Data, error) {
	var dataList []*qmsg.GetMsgNotifyDataListRsp_Data

	// 获取机器人信息
	robotMap, err := getRobotInfoList(ctx, corpId, appId, robotUid)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	// 组合数据
	for _, uid := range robotUid {
		// 成员昵称
		robot, ok := robotMap[uid]
		if !ok {
			robot = &qris.Robot{
				AvatarUrl: "",
				Name:      "",
			}
		}

		// 成员触发次数
		totalTrigger, ok := robotTriggerMap[uid]
		if !ok {
			log.Errorf("%v :totalTrigger not exist", uid)
			totalTrigger = 0
		}
		dataList = append(dataList, &qmsg.GetMsgNotifyDataListRsp_Data{
			Avatar:       robot.AvatarUrl,
			Name:         robot.Name,
			TotalTrigger: totalTrigger,
		})
	}
	return dataList, nil
}

// getRobotInfoList 得到扫码号和平台号的汇总信息
func getRobotInfoList(ctx *rpc.Context, corpId, appId uint32, robotUidList pie.Uint64s) (map[uint64]*qris.Robot, error) {
	var robotMap = make(map[uint64]*qris.Robot)
	var robotListRsp = &qris.GetRobotListRsp{}
	var err error
	// 得到机器人的信息
	robotUidList.Chunk(200, func(chunk pie.Uint64s) bool {
		robotListRsp, err = qris.GetRobotList(ctx, &qris.GetRobotListReq{
			ListOption: core.NewListOption().AddOpt(qris.GetRobotListReq_ListOptionRobotUidList, chunk),
			CorpId:     corpId,
			AppId:      appId,
		})
		if err != nil {
			log.Errorf("err:%v", err)
			return true
		}
		// insert to map
		if len(robotListRsp.List) != 0 {
			for _, robot := range robotListRsp.List {
				robotMap[robot.Uid] = robot
			}
		}
		return false
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	return robotMap, nil
}

// getStaffUserList 得到企业成员的汇总信息
func getStaffUserList(ctx *rpc.Context, corpId, appId uint32, uidList pie.Uint64s) (map[uint64]*quan.ModelUser, error) {
	var userMap = make(map[uint64]*quan.ModelUser)
	var userListRsp = &iquan.GetUserListRsp{}
	var err error
	// 得到企业成员的信息
	uidList.Chunk(500, func(chunk pie.Uint64s) bool {
		userListRsp, err = iquan.GetUserList(ctx, &iquan.GetUserListReq{
			ListOption: core.NewListOption().AddOpt(iquan.GetUserListReq_ListOptionUidList, chunk),
			CorpId:     corpId,
			AppId:      appId,
		})
		if err != nil {
			log.Errorf("err:%v", err)
			return true
		}
		// insert to map
		if len(userListRsp.List) != 0 {
			for _, user := range userListRsp.List {
				userMap[user.Id] = user
			}
		}
		return false
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	return userMap, nil
}

// getTimeoutStat 获取机器人和成员的超时次数
func getTimeoutStat(ctx *rpc.Context, corpId, appId, date uint32, timeScope *qmsg.ModelMsgStatTimeScope) (uint32, uint32, uint32, error) {

	// 统计成员触发次数
	timeoutCnt, err := convertTimeScopeToDb(RobotSingleStat.WhereCorpApp(corpId, appId), date, timeScope).Sum(ctx, DbTimeoutChatCount)
	if err != nil && !RobotSingleStat.IsNotFoundErr(err) {
		log.Errorf("err:%v", err)
		return 0, 0, 0, err
	}

	// 统计客服触发超时人数
	customerCount, err := convertTimeScopeToDb(CustomerServiceSingleStat.WhereCorpApp(corpId, appId), date, timeScope).Gt(DbTimeoutChatCount, 0).Count(ctx)
	if err != nil && !RobotSingleStat.IsNotFoundErr(err) {
		log.Errorf("err:%v", err)
		return 0, 0, 0, err
	}
	// 统计成员触发超时人数
	totalRobotCount, err := convertTimeScopeToDb(RobotSingleStat.WhereCorpApp(corpId, appId), date, timeScope).Gt(DbTimeoutChatCount, 0).Count(ctx)
	if err != nil && !RobotSingleStat.IsNotFoundErr(err) {
		log.Errorf("err:%v", err)
		return 0, 0, 0, err
	}
	return timeoutCnt, customerCount, totalRobotCount, nil
}
