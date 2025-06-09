package impl

import (
	"fmt"

	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/brick/utils"
	"git.pinquest.cn/qlb/qmsg"
)

var (
	strategyMap = map[string]MsgStatDataStrategy{
		fmt.Sprintf("%d_%d", qmsg.GetMsgNotifyDataListReq_NotifyTypeTimeOut, qmsg.GetLineChatDataListReq_PersonTypeCustomer):   new(CustomerTimeoutDataStrategy),
		fmt.Sprintf("%d_%d", qmsg.GetMsgNotifyDataListReq_NotifyTypeSensitive, qmsg.GetLineChatDataListReq_PersonTypeCustomer): new(CustomerSensitiveStrategy),
		fmt.Sprintf("%d_%d", qmsg.GetMsgNotifyDataListReq_NotifyTypeTimeOut, qmsg.GetLineChatDataListReq_PersonTypeRobot):      new(RobotTimeoutDataStrategy),
		fmt.Sprintf("%d_%d", qmsg.GetMsgNotifyDataListReq_NotifyTypeSensitive, qmsg.GetLineChatDataListReq_PersonTypeRobot):    new(RobotSensitiveStrategy),
	}
)

// MsgStatDataStrategy 按方法的策略,根据对应的策略来获取对应的执行方法
type MsgStatDataStrategy interface {
	getMsgStatDataFunc(notifyType, personType uint32) (MsgStatDataFunc, error)
}

// MsgStatDataFunc 需要实现的策略方法
type MsgStatDataFunc func(ctx *rpc.Context, corpId, appId, date uint32, timeScope *qmsg.ModelMsgStatTimeScope) (*qmsg.GetMsgNotifyDataListRsp, error)

// FactoryMsgStatDataFunc 策略工厂
func FactoryMsgStatDataFunc(notifyType, personType uint32) (MsgStatDataFunc, error) {
	return getMsgStatDataFunc(notifyType, personType)
}

// getMsgStatDataFunc 获取策略
func getMsgStatDataFunc(notifyType uint32, personType uint32) (MsgStatDataFunc, error) {
	strategy, ok := strategyMap[fmt.Sprintf("%d_%d", notifyType, personType)]
	if !ok {
		return nil, rpc.InvalidArg("notifyType is %d and personType is %d", notifyType, personType)
	}
	return strategy.getMsgStatDataFunc(notifyType, personType)
}

// CustomerTimeoutDataStrategy 查询客服超时数据
type CustomerTimeoutDataStrategy struct{}

// CustomerSensitiveStrategy 查询客服敏感数据
type CustomerSensitiveStrategy struct{}

// RobotTimeoutDataStrategy 查询成员超时数据
type RobotTimeoutDataStrategy struct{}

// RobotSensitiveStrategy 查询成员敏感数据
type RobotSensitiveStrategy struct{}

func (s *CustomerTimeoutDataStrategy) getMsgStatDataFunc(notifyType, personType uint32) (MsgStatDataFunc, error) {
	return func(ctx *rpc.Context, corpId, appId, date uint32, timeScope *qmsg.ModelMsgStatTimeScope) (*qmsg.GetMsgNotifyDataListRsp, error) {
		var (
			rsp                       qmsg.GetMsgNotifyDataListRsp
			customerServiceTriggerMap = make(map[uint64]uint32)
			customerServiceList       []*qmsg.ModelCustomerServiceSingleStat
		)
		log.Infof("corpId %d appId %d ,start get customer timeout data", corpId, appId)
		// check method can use
		if notifyType != uint32(qmsg.GetMsgNotifyDataListReq_NotifyTypeTimeOut) || personType != uint32(qmsg.GetLineChatDataListReq_PersonTypeCustomer) {
			return nil, rpc.InvalidArg("notifyType is %d and personType is %d , not belong function", notifyType, personType)
		}
		timeoutCnt, customerCnt, robotCnt, err := getTimeoutStat(ctx, corpId, appId, date, timeScope)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
		// 获取客服触发统计数据
		err = convertTimeScopeToDb(CustomerServiceSingleStat.WhereCorpApp(corpId, appId), date, timeScope).Find(ctx, &customerServiceList)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
		// 获取客服触发次数
		for _, stat := range customerServiceList {
			count, ok := customerServiceTriggerMap[stat.CustomerServiceId]
			if !ok {
				customerServiceTriggerMap[stat.CustomerServiceId] = stat.TimeoutChatCount
				continue
			}
			customerServiceTriggerMap[stat.CustomerServiceId] = count + stat.TimeoutChatCount
		}
		// 获取超时客服的UID
		uidList := utils.PluckUint64(customerServiceList, "CustomerServiceId")
		// 获取数据信息
		dataList, err := getCustomerDataStrategy(ctx, corpId, appId, customerServiceTriggerMap, uidList.Unique())
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}

		rsp.List = dataList
		rsp.TotalCount = timeoutCnt
		rsp.TotalRobot = robotCnt
		rsp.TotalCustomer = customerCnt
		return &rsp, nil
	}, nil
}

func (s *RobotTimeoutDataStrategy) getMsgStatDataFunc(notifyType, personType uint32) (MsgStatDataFunc, error) {
	return func(ctx *rpc.Context, corpId, appId, date uint32, timeScope *qmsg.ModelMsgStatTimeScope) (*qmsg.GetMsgNotifyDataListRsp, error) {
		var (
			rsp                 qmsg.GetMsgNotifyDataListRsp
			robotTriggerMap     = make(map[uint64]uint32)
			robotSingleStatList []*qmsg.ModelCustomerServiceSingleStat
		)

		log.Infof("corpId %d appId %d ,start get robot timeout data", corpId, appId)
		// check method can use
		if notifyType != uint32(qmsg.GetMsgNotifyDataListReq_NotifyTypeTimeOut) || personType != uint32(qmsg.GetLineChatDataListReq_PersonTypeRobot) {
			return nil, rpc.InvalidArg("notifyType is %d and personType is %d , not belong function", notifyType, personType)
		}
		timeoutCnt, customerCnt, robotCnt, err := getTimeoutStat(ctx, corpId, appId, date, timeScope)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
		// 获取机器人触发统计数据
		err = convertTimeScopeToDb(RobotSingleStat.WhereCorpApp(corpId, appId), date, timeScope).Find(ctx, &robotSingleStatList)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
		// 得到成员ID信息
		robotUidList := utils.PluckUint64(robotSingleStatList, "RobotUid")
		// 获取成员触发次数
		for _, stat := range robotSingleStatList {
			count, ok := robotTriggerMap[stat.RobotUid]
			if !ok {
				robotTriggerMap[stat.RobotUid] = stat.TimeoutChatCount
				continue
			}
			robotTriggerMap[stat.RobotUid] = count + stat.TimeoutChatCount
		}
		// 得到封装的数据
		dataList, err := getRobotDataStrategy(ctx, corpId, appId, robotTriggerMap, robotUidList.Unique())
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}

		rsp.List = dataList
		rsp.TotalCount = timeoutCnt
		rsp.TotalRobot = robotCnt
		rsp.TotalCustomer = customerCnt

		return &rsp, nil
	}, nil
}

func (s *CustomerSensitiveStrategy) getMsgStatDataFunc(notifyType, personType uint32) (MsgStatDataFunc, error) {
	return func(ctx *rpc.Context, corpId, appId, date uint32, timeScope *qmsg.ModelMsgStatTimeScope) (*qmsg.GetMsgNotifyDataListRsp, error) {
		var rsp qmsg.GetMsgNotifyDataListRsp
		var customerServiceTriggerMap = make(map[uint64]uint32)
		log.Infof("corpId: %d, appId: %d, start get customer sensitive data", corpId, appId)
		// check method can use
		if notifyType != uint32(qmsg.GetMsgNotifyDataListReq_NotifyTypeSensitive) || personType != uint32(qmsg.GetLineChatDataListReq_PersonTypeCustomer) {
			return nil, rpc.InvalidArg("notifyType is %d and personType is %d , not belong function", notifyType, personType)
		}

		var list []*qmsg.ModelCustomerServiceSingleStat
		err := convertTimeScopeToDb(CustomerServiceSingleStat.WhereCorpApp(corpId, appId), date, timeScope).Gt(DbSensitiveTriggerCount, 0).Find(ctx, &list)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
		for _, item := range list {
			customerServiceTriggerMap[item.CustomerServiceId] = item.SensitiveTriggerCount
			rsp.TotalCount += item.SensitiveTriggerCount
		}
		// 获取数据信息
		customerServiceUidList := utils.PluckUint64(list, "CustomerServiceId")
		dataList, err := getCustomerDataStrategy(ctx, corpId, appId, customerServiceTriggerMap, customerServiceUidList.Unique())
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}

		rsp.TotalRobot, err = convertTimeScopeToDb(RobotSingleStat.WhereCorpApp(corpId, appId), date, timeScope).Gt(DbSensitiveTriggerCount, 0).Count(ctx)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
		rsp.List = dataList
		rsp.TotalCustomer = uint32(len(customerServiceUidList.Unique()))
		return &rsp, nil
	}, nil
}

func (s *RobotSensitiveStrategy) getMsgStatDataFunc(notifyType, personType uint32) (MsgStatDataFunc, error) {
	return func(ctx *rpc.Context, corpId, appId, date uint32, timeScope *qmsg.ModelMsgStatTimeScope) (*qmsg.GetMsgNotifyDataListRsp, error) {
		var rsp qmsg.GetMsgNotifyDataListRsp
		var robotTriggerMap = make(map[uint64]uint32)

		log.Infof("corpId: %d, appId: %d, start get robot sensitive data", corpId, appId)
		// check method can use
		if notifyType != uint32(qmsg.GetMsgNotifyDataListReq_NotifyTypeSensitive) || personType != uint32(qmsg.GetLineChatDataListReq_PersonTypeRobot) {
			return nil, rpc.InvalidArg("notifyType is %d and personType is %d , not belong function", notifyType, personType)
		}

		var list []*qmsg.ModelRobotSingleStat
		err := convertTimeScopeToDb(RobotSingleStat.WhereCorpApp(corpId, appId), date, timeScope).Gt(DbSensitiveTriggerCount, 0).Find(ctx, &list)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
		for _, item := range list {
			robotTriggerMap[item.RobotUid] = item.SensitiveTriggerCount
			rsp.TotalCount += item.SensitiveTriggerCount
		}

		// 成员的RobotUid
		robotUidList := utils.PluckUint64(list, "RobotUid")

		dataList, err := getRobotDataStrategy(ctx, corpId, appId, robotTriggerMap, robotUidList.Unique())
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}

		rsp.TotalCustomer, err = convertTimeScopeToDb(CustomerServiceSingleStat.WhereCorpApp(corpId, appId), date, timeScope).Gt(DbSensitiveTriggerCount, 0).Count(ctx)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}

		rsp.List = dataList
		rsp.TotalRobot = uint32(len(robotUidList.Unique()))
		return &rsp, nil
	}, nil
}
