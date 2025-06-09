package impl

import (
	"fmt"
	"sort"
	"time"

	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/brick/utils"
	"git.pinquest.cn/qlb/core"
	"git.pinquest.cn/qlb/qmsg"
	"git.pinquest.cn/qlb/qris"
	"git.pinquest.cn/qlb/quan"
)

// GetMsgNotifyDataList h5-会话质检-页面数据
func GetMsgNotifyDataList(ctx *rpc.Context, req *qmsg.GetMsgNotifyDataListReq) (*qmsg.GetMsgNotifyDataListRsp, error) {
	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)
	doFunc, err := FactoryMsgStatDataFunc(req.NotifyType, req.PersonType)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	rspData, err := doFunc(ctx, corpId, appId, req.Date, req.MsgStatTimeScope)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	// 进行排序
	var notifyDataList NotifyDataList
	notifyDataList = append(notifyDataList, rspData.List...)
	sort.Sort(notifyDataList)
	rspData.List = notifyDataList

	return rspData, nil
}

// GetTimeScopeList h5-会话质检-获取可选择时间范围
func GetTimeScopeList(ctx *rpc.Context, req *qmsg.GetTimeScopeListReq) (*qmsg.GetTimeScopeListRsp, error) {
	var rsp qmsg.GetTimeScopeListRsp
	var list []*qmsg.ModelMsgStatTimeScope
	var statList []*qmsg.ModelMsgStatTimeScope
	var model qmsg.ModelMsgStatTimeScope
	var config qmsg.ModelMsgStatTimeScope_Config

	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)

	// 查询数据库得到指定记录
	err := MsgStatTimeScope.WhereCorpApp(corpId, appId).Where(DbDate, req.Date).OrderDesc(DbCreatedAt).Find(ctx, &statList)
	if err != nil && !MsgStatTimeScope.IsNotFoundErr(err) {
		log.Errorf("err:%v", err)
		return nil, err
	}
	// 如果查询不到时间范围，则默认返回全天
	if len(statList) == 0 {
		log.Debugf("not found time scope , default All time")
		rsp.List = append(rsp.List, &qmsg.ModelMsgStatTimeScope{
			CorpId: corpId,
			AppId:  appId,
			Date:   req.Date,
			Config: &qmsg.ModelMsgStatTimeScope_Config{
				StatTimeScopeType: uint32(qmsg.StatTimeScopeType_StatTimeScopeTypeAll),
			},
		})
		return &rsp, nil
	}
	// 查询到时间范围，去最新的那个即生效的那个时间范围
	timeScope := statList[0]
	model.Id = timeScope.Id
	model.CorpId = timeScope.CorpId
	model.AppId = timeScope.AppId
	model.Date = timeScope.Date
	if timeScope.Config == nil {
		rsp.List = append(rsp.List, &qmsg.ModelMsgStatTimeScope{
			CorpId: corpId,
			AppId:  appId,
			Date:   req.Date,
			Config: &qmsg.ModelMsgStatTimeScope_Config{
				StatTimeScopeType: uint32(qmsg.StatTimeScopeType_StatTimeScopeTypeAll),
			},
		})
		return &rsp, nil
	}
	// 如果是全天 ， 那就直接返回
	if timeScope.Config.StatTimeScopeType == uint32(qmsg.StatTimeScopeType_StatTimeScopeTypeAll) {
		config.StatTimeScopeType = uint32(qmsg.StatTimeScopeType_StatTimeScopeTypeAll)
		model.Config = &config
		list = append(list, &model)
		rsp.List = list
		return &rsp, nil
	}
	// 得到今天星期几
	t, err := time.ParseInLocation("20060102", fmt.Sprintf("%d", req.Date), time.Local)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	// 处理一下周几问题
	config.StatTimeScopeType = uint32(qmsg.StatTimeScopeType_StatTimeScopeTypeSpecified)
	for _, scope := range timeScope.Config.TimeScopeList {
		if utils.InSliceUint32(uint32(t.Weekday()), scope.WeekList) {
			times := &qmsg.TimeScope{
				StartAt:  scope.StartAt,
				EndAt:    scope.EndAt,
				WeekList: []uint32{uint32(t.Weekday())},
			}
			config.TimeScopeList = append(config.TimeScopeList, times)
		}
	}
	// 进行时间段排序
	var timeScopeSlice TimeScopeSlice
	timeScopeSlice = append(timeScopeSlice, config.TimeScopeList...)
	sort.Sort(timeScopeSlice)
	config.TimeScopeList = timeScopeSlice
	model.Config = &config
	rsp.List = append(rsp.List, &model)
	return &rsp, nil
}

// GetMsgDataStatList pc-会话质检-数据概括 - 默认按成员维度
func GetMsgDataStatList(ctx *rpc.Context, req *qmsg.GetMsgDataStatListReq) (*qmsg.GetMsgDataStatListRsp, error) {
	var (
		rsp                 qmsg.GetMsgDataStatListRsp
		fieldList           []uint32
		robotSingleStatList []*qmsg.ModelRobotSingleStat
	)
	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)

	dbRobot := RobotSingleStat.WhereCorpApp(corpId, appId)
	err := core.NewListOptionProcessor(req.ListOption).
		AddUint32List(qmsg.GetLineChatDataListReq_ListFieldList, func(valList []uint32) error {
			fieldList = valList
			return nil
		}).
		AddTimeStampRange(qmsg.GetLineChatDataListReq_ListOptionTimeRange, func(beginAt, endAt uint32) error {
			beginAt, endAt, err := converTimeDate(beginAt, endAt)
			if err != nil {
				log.Errorf("err:%v", err)
				return err
			}
			dbRobot = dbRobot.WhereBetween(DbDate, beginAt, endAt)
			return nil
		}).
		Process()
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	err = dbRobot.Find(ctx, &robotSingleStatList)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	// 按成员维度 - 数据概览
	fieldList, err = getStatFieldList(ctx, fieldList)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	msgDataList, err := getMsgDataList(robotSingleStatList, fieldList)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	rsp.List = msgDataList
	return &rsp, nil
}

// GetLineChatDataList pc-会话质检-折线图
func GetLineChatDataList(ctx *rpc.Context, req *qmsg.GetLineChatDataListReq) (*qmsg.GetLineChatDataListRsp, error) {
	var rsp qmsg.GetLineChatDataListRsp
	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)
	log.Infof("GetLineChatDataList")
	if corpId == 0 || appId == 0 {
		log.Errorf("GetLineChatDataList: corpId=%d, appId=%d", corpId, appId)
		return &rsp, nil
	}
	// 区分账号类型
	msgDataModel, err := GetMsgDataModelByType(ctx, req.PersonType, corpId, appId)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	list, userNameMap, err := msgDataModel.GetLineInfoList(ctx, req.ListOption, req.MsgStatTimeScope)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	rsp.DataList = list
	rsp.UserNameMap = userNameMap
	return &rsp, nil
}

// GetPersonMsgList pc-会话质检-数据明细
func GetPersonMsgList(ctx *rpc.Context, req *qmsg.GetPersonMsgListReq) (*qmsg.GetPersonMsgListRsp, error) {
	var rsp qmsg.GetPersonMsgListRsp

	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)
	if corpId == 0 || appId == 0 {
		log.Errorf("GetPersonMsgList: corpId=%d, appId=%d", corpId, appId)
		return &rsp, nil
	}
	msgDataModel, err := GetMsgDataModelByType(ctx, req.PersonType, corpId, appId)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	list, personDataMap, err := msgDataModel.GetMsgInfoList(ctx, req.ListOption)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	rsp.List = list
	rsp.PersonDataMap = personDataMap

	return &rsp, nil
}

// SetStatFieldList pc-会话质检-设置指标字段排序
func SetStatFieldList(ctx *rpc.Context, req *qmsg.SetStatFieldListReq) (*qmsg.SetStatFieldListRsp, error) {
	var rsp qmsg.SetStatFieldListRsp
	// check field exist
	for _, val := range req.List {
		_, ok := qmsg.Field_name[int32(val)]
		if !ok {
			return nil, rpc.InvalidArg("field id %d not exist", val)
		}
	}
	user, err := quan.GetUserCheck(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	err = setMsgDataStatFieldList4Redis(user.CorpId, user.AppId, req.List, req.FieldType, user.Id)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	err = setMsgDataStatFieldMap4Redis(user.CorpId, user.AppId, req.FieldMap, req.FieldType, user.Id)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	rsp.List = req.List
	rsp.FieldMap = req.FieldMap
	return &rsp, nil
}

// GetStatFieldList pc-会话质检-获取指标字段排序
func GetStatFieldList(ctx *rpc.Context, req *qmsg.GetStatFieldListReq) (*qmsg.GetStatFieldListRsp, error) {
	var rsp qmsg.GetStatFieldListRsp
	user, err := quan.GetUserCheck(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	fieldList, err := getMsgDataStatFieldList4Redis(user.CorpId, user.AppId, req.FieldType, user.Id)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	fieldMap, err := getMsgDataStatFieldMap4Redis(user.CorpId, user.AppId, req.FieldType, user.Id)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	rsp.List = fieldList
	rsp.FieldMap = fieldMap
	return &rsp, nil
}

// GetPersonMsgInfoList pc-会话质检-数据明细-详情
func GetPersonMsgInfoList(ctx *rpc.Context, req *qmsg.GetPersonMsgInfoListReq) (*qmsg.GetPersonMsgInfoListRsp, error) {
	var rsp qmsg.GetPersonMsgInfoListRsp
	var valUidList []uint64
	var userList []*qmsg.ModelUser
	var beginDate, endDate uint32
	var fieldList []uint32

	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)
	if corpId == 0 || appId == 0 {
		log.Errorf("GetPersonMsgInfoList: corpId=%d, appId=%d", corpId, appId)
		return &rsp, nil
	}

	// 处理数据，得到成员或客服ID集合
	err := core.NewListOptionProcessor(req.ListOption).
		AddUint64List(qmsg.GetLineChatDataListReq_ListOptionUidList, func(valList []uint64) error {
			valUidList = valList
			return nil
		}).
		AddTimeStampRange(qmsg.GetLineChatDataListReq_ListOptionTimeRange, func(beginAt, endAt uint32) error {
			var err error
			beginDate, endDate, err = converTimeDate(beginAt, endAt)
			if err != nil {
				log.Errorf("err:%v", err)
				return err
			}
			return nil
		}).
		AddUint32List(qmsg.GetLineChatDataListReq_ListFieldList, func(valList []uint32) error {
			fieldList = valList
			return nil
		}).
		Process()
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	// 如果没传用户ID，那么就不往下走了
	if len(valUidList) == 0 {
		return &rsp, nil
	}

	// 通过区分是客服还是成员
	msgDataModel, err := GetMsgDataModelByType(ctx, req.PersonType, corpId, appId)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	// 具体执行逻辑
	personDataMap, robotUidList, customerUidList, robotList, customerList, err := msgDataModel.GetMsgDetailsList(ctx, beginDate, endDate, valUidList, fieldList)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	// 获取成员信息
	if len(robotUidList) != 0 {
		sysRep, err := qris.GetRobotList(ctx, &qris.GetRobotListReq{
			ListOption: core.NewListOption().AddOpt(qris.GetRobotListReq_ListOptionRobotUidList, robotUidList),
			CorpId:     corpId,
			AppId:      appId,
		})
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
		utils.KeyBy(sysRep.List, "Uid", &rsp.RobotMap)
	}
	// 获取客服信息
	if len(customerUidList) != 0 {
		err := User.WhereCorpApp(corpId, appId).WhereIn(DbId, customerUidList).Find(ctx, &userList)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
		utils.KeyBy(userList, "Id", &rsp.UserMap)
	}

	rsp.RobotList = robotList
	rsp.CustomerList = customerList
	rsp.PersonDataMap = personDataMap
	return &rsp, nil
}

// ====== start ======

// TimeScopeSlice 排序时间段
type TimeScopeSlice []*qmsg.TimeScope

// Len 重写 Len() 方法
func (a TimeScopeSlice) Len() int {
	return len(a)
}

// Swap 重写 Swap() 方法
func (a TimeScopeSlice) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

// Less 重写 Less() 方法， 从大到小排序
func (a TimeScopeSlice) Less(i, j int) bool {
	return a[j].StartAt > a[i].StartAt // 从小到大
	// return a[j].StartAt < a[i].StartAt // 从大到小
}

// NotifyDataList H5页面排序内容
type NotifyDataList []*qmsg.GetMsgNotifyDataListRsp_Data

// Len 重写 Len() 方法
func (a NotifyDataList) Len() int {
	return len(a)
}

// Swap 重写 Swap() 方法
func (a NotifyDataList) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

// Less 重写 Less() 方法， 从大到小排序
func (a NotifyDataList) Less(i, j int) bool {
	// return a[j].StartAt > a[i].StartAt // 从小到大
	return a[j].TotalTrigger < a[i].TotalTrigger // 从大到小
}

// ====== end ======
