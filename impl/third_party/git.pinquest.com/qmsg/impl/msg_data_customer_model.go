package impl

import (
	"fmt"

	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/brick/utils"
	"git.pinquest.cn/qlb/core"
	"git.pinquest.cn/qlb/qmsg"
	"git.pinquest.cn/qlb/qris"
	"github.com/360EntSecGroup-Skylar/excelize/v2"
)

type MsgCustomerModel struct {
	CorpId uint32
	AppId  uint32
}

func (m *MsgCustomerModel) ExportPersonMsg(ctx *rpc.Context, listOption *core.ListOption, uidList []uint64, f *excelize.File, titleList []interface{}) error {

	// 获取数据
	sysRsp, err := GetPersonMsgInfoList(ctx, &qmsg.GetPersonMsgInfoListReq{
		ListOption: listOption.AddOpt(qmsg.GetLineChatDataListReq_ListOptionUidList, uidList),
		PersonType: uint32(qmsg.GetLineChatDataListReq_PersonTypeCustomer),
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	// 去重一下人员ID
	unique := utils.PluckUint64(sysRsp.CustomerList, "CustomerServiceId").Unique()
	for i, v := range unique {
		// 得到每个人员的数据名称
		user, ok := sysRsp.UserMap[v]
		// 得到要写哪个sheet
		// 得到要写哪个sheet
		var values string
		if ok {
			values = fmt.Sprintf("%s(%s)", user.Username, user.LoginId)
		} else {
			values = fmt.Sprintf("成员%d", i)
		}
		_ = f.NewSheet(values)
		f.DeleteSheet("Sheet1")
		excelWriter, err := f.NewStreamWriter(values)
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
		// 第一行写表头
		err = excelWriter.SetRow("A1", titleList)
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
		// 第二行开始写数据
		rowNum := 2
		for _, val := range sysRsp.CustomerList {
			if val.CustomerServiceId == v {
				record, err := getExportDataList(val.Id, val.Date, val.StartAt, val.EndAt, sysRsp.PersonDataMap)
				if err != nil {
					log.Errorf("err:%v", err)
					return err
				}
				// 写入表格
				cell, _ := excelize.CoordinatesToCellName(1, rowNum)
				if err = excelWriter.SetRow(cell, record); err != nil {
					log.Errorf("err:%v", err)
					return err
				}
				rowNum++
			}
		}

		if err = excelWriter.Flush(); err != nil {
			log.Errorf("err:%v", err)
			return err
		}
	}
	return nil
}

func (m *MsgCustomerModel) GetLineInfoList(ctx *rpc.Context, listOption *core.ListOption, msgStatTimeScope *qmsg.ModelMsgStatTimeScope) ([]*qmsg.GetLineChatDataListRsp_DataList, map[uint64]string, error) {
	var customerSingleStatList []*qmsg.ModelCustomerServiceSingleStat
	var dataList []*qmsg.GetLineChatDataListRsp_DataList
	var fieldList []uint32
	var dateList []uint32
	var isNeedUid bool
	var userNameMap = make(map[uint64]string)
	var userList []*qmsg.ModelUser

	db := CustomerServiceSingleStat.WhereCorpApp(m.CorpId, m.AppId)
	err := core.NewListOptionProcessor(listOption).
		AddUint32List(qmsg.GetLineChatDataListReq_ListOptionUidList, func(valList []uint32) error {
			db.WhereIn(DbCustomerServiceId, valList)
			isNeedUid = true
			return nil
		}).
		AddUint32List(qmsg.GetLineChatDataListReq_ListFieldList, func(valList []uint32) error {
			if len(valList) != 1 {
				return rpc.InvalidArg("field required , size must equal 1 ,this field size is %d", len(valList))
			}
			fieldList = valList
			return nil
		}).
		AddTimeStampRange(qmsg.GetLineChatDataListReq_ListOptionTimeRange, func(beginAt, endAt uint32) error {
			beginDate, endDate, err := converTimeDate(beginAt, endAt)
			if err != nil {
				log.Errorf("err:%v", err)
				return err
			}
			dateList = GetDateList(beginDate, endDate)
			db.WhereBetween(DbDate, beginDate, endDate)
			return nil
		}).
		Process()
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, nil, err
	}
	// 拼接时间范围
	if msgStatTimeScope.Config.StatTimeScopeType == uint32(qmsg.StatTimeScopeType_StatTimeScopeTypeSpecified) {
		db = convertTimeScopeToDb(db, msgStatTimeScope.Date, msgStatTimeScope)
	}
	err = db.Find(ctx, &customerSingleStatList)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, nil, err
	}
	// 处理数据
	customerServiceIdList := utils.PluckUint64(customerSingleStatList, "CustomerServiceId").Unique()

	err = User.WhereCorpApp(m.CorpId, m.AppId).WhereIn(DbId, customerServiceIdList).Find(ctx, &userList)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, nil, err
	}
	for _, user := range userList {
		userNameMap[user.Id] = user.Username
	}

	// 如果是全部客服 ， 数据汇总成一条,按时间划分
	if !isNeedUid {
		var rspDataList = &qmsg.GetLineChatDataListRsp_DataList{
			List: []*qmsg.GetLineChatDataListRsp_ShowLineChatData{},
		}
		for _, dateVal := range dateList {
			var data = &qmsg.GetLineChatDataListRsp_ShowLineChatData{
				Date:  fmt.Sprintf("%d", dateVal),
				Count: 0,
			}
			msgDataMap, err := getCustomerMsgDataMap(customerSingleStatList, fieldList, dateVal, 0)
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, nil, err
			}
			count, ok := msgDataMap[fieldList[0]]
			if ok {
				data.Count = count
			}
			rspDataList.List = append(rspDataList.List, data)
		}
		// 根据时间分类
		dataList = append(dataList, rspDataList)
		return dataList, nil, nil
	}
	for _, val := range customerServiceIdList {
		var rspDataList = &qmsg.GetLineChatDataListRsp_DataList{
			List: []*qmsg.GetLineChatDataListRsp_ShowLineChatData{},
		}
		// 分类时间范围
		for _, dateVal := range dateList {
			var data = &qmsg.GetLineChatDataListRsp_ShowLineChatData{
				Date:  fmt.Sprintf("%d", dateVal),
				Count: 0,
				Uid:   val,
			}
			msgDataMap, err := getCustomerMsgDataMap(customerSingleStatList, fieldList, dateVal, val)
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, nil, err
			}
			count, ok := msgDataMap[fieldList[0]]
			if ok {
				data.Count = count
			}
			rspDataList.List = append(rspDataList.List, data)
		}
		// 根据时间分类
		dataList = append(dataList, rspDataList)
	}
	return dataList, userNameMap, nil
}

func (m *MsgCustomerModel) GetMsgDetailsList(ctx *rpc.Context, beginDate, endDate uint32, valUidList []uint64, fieldList []uint32) (map[uint64]*qmsg.GetPersonMsgListRsp_PersonData, []uint64, []uint64, []*qmsg.ModelRobotSingleStat, []*qmsg.ModelCustomerServiceSingleStat, error) {
	var customerList []*qmsg.ModelCustomerServiceSingleStat
	var personDataMap = make(map[uint64]*qmsg.GetPersonMsgListRsp_PersonData)
	// 得到客服维度的单聊统计
	db := CustomerServiceSingleStat.WhereCorpApp(m.CorpId, m.AppId)
	if beginDate != 0 && endDate != 0 {
		db.WhereBetween(DbDate, beginDate, endDate)
	}
	err := db.WhereIn(DbCustomerServiceId, valUidList).
		OrderAsc(DbDate, DbStartAt).
		Find(ctx, &customerList)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, nil, nil, nil, nil, err
	}

	// 按客服维度获取字段 - 数据明细
	fieldList, err = getCustomerStatFieldList(ctx, fieldList)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, nil, nil, nil, nil, err
	}
	// 获取数据明细
	for _, stat := range customerList {
		list, err := getCustomerMsgDataList([]*qmsg.ModelCustomerServiceSingleStat{stat}, fieldList)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, nil, nil, nil, nil, err
		}
		personDataMap[stat.Id] = &qmsg.GetPersonMsgListRsp_PersonData{
			List: list,
		}
	}
	robotUidList := utils.PluckUint64(customerList, "RobotUid")
	userIdList := utils.PluckUint64(customerList, "CustomerServiceId")
	return personDataMap, robotUidList, userIdList, []*qmsg.ModelRobotSingleStat{}, customerList, nil
}

func (m *MsgCustomerModel) GetMsgInfoList(ctx *rpc.Context, listOption *core.ListOption) ([]*qmsg.GetPersonMsgListRsp_PersonData, map[uint64]*qmsg.GetPersonMsgListRsp_PersonData, error) {
	var (
		list                      []*qmsg.GetPersonMsgListRsp_PersonData
		personDataMap             = make(map[uint64]*qmsg.GetPersonMsgListRsp_PersonData)
		defaultHaveNameListOption bool
		userList                  []*qmsg.ModelUser
		user4DbList               []*qmsg.ModelUser
		user4DbMap                = make(map[uint64]*qmsg.ModelUser)
		customerSingleStatList    []*qmsg.ModelCustomerServiceSingleStat
		customerSingleStatMap     = make(map[uint64][]*qmsg.ModelCustomerServiceSingleStat)
		userRobotRef              = make(map[uint64][]uint64)
		userRobotList             []*qmsg.ModelUserRobot
		fieldList                 []uint32
	)

	// 取出成员的记录
	db := CustomerServiceSingleStat.WhereCorpApp(m.CorpId, m.AppId)
	err := core.NewListOptionProcessor(listOption).
		AddString(qmsg.GetLineChatDataListReq_ListOptionName, func(val string) error {
			defaultHaveNameListOption = true
			err := User.WhereCorpApp(m.CorpId, m.AppId).WhereLike(DbUsername, fmt.Sprintf("%%%s%%", val)).Find(ctx, &userList)
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
		AddTimeStampRange(qmsg.GetLineChatDataListReq_ListOptionTimeRange, func(beginAt, endAt uint32) error {
			beginDate, endDate, err := converTimeDate(beginAt, endAt)
			if err != nil {
				log.Errorf("err:%v", err)
				return err
			}
			db.WhereBetween(DbDate, beginDate, endDate)
			return nil
		}).
		Process()
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, nil, err
	}
	// 搜索名字得到的UidList
	if defaultHaveNameListOption {
		if len(userList) == 0 {
			return list, personDataMap, nil
		}
		db.WhereIn(DbCustomerServiceId, utils.PluckUint64(userList, "Id"))
	}
	// 得到相关的数据
	err = db.Find(ctx, &customerSingleStatList)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, nil, err
	}
	// 数据库找不到数据
	if len(customerSingleStatList) == 0 {
		return nil, nil, nil
	}
	// 得到客服的信息
	err = User.WhereCorpApp(m.CorpId, m.AppId).
		WhereIn(DbId, utils.PluckUint64(customerSingleStatList, "CustomerServiceId")).
		Find(ctx, &user4DbList)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, nil, err
	}
	utils.KeyBy(user4DbList, "Id", &user4DbMap)
	// 得到客服关联机器人的信息
	err = UserRobot.WhereCorpApp(m.CorpId, m.AppId).WhereIn(DbUid, utils.PluckUint64(customerSingleStatList, "CustomerServiceId")).Find(ctx, &userRobotList)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, nil, err
	}
	for _, userRobot := range userRobotList {
		userIds, ok := userRobotRef[userRobot.Uid]
		if !ok {
			userRobotRef[userRobot.Uid] = []uint64{userRobot.RobotUid}
			continue
		}
		userRobotRef[userRobot.Uid] = append(userIds, userRobot.RobotUid)
	}
	// 得到机器人信息
	sysRep, err := qris.GetRobotList(ctx, &qris.GetRobotListReq{
		ListOption: core.NewListOption().AddOpt(qris.GetRobotListReq_ListOptionRobotUidList, utils.PluckUint64(userRobotList, "RobotUid")),
		CorpId:     m.CorpId,
		AppId:      m.AppId,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, nil, err
	}
	// 记录按客服分类
	for _, stat := range customerSingleStatList {
		singleStatList, ok := customerSingleStatMap[stat.CustomerServiceId]
		if !ok {
			customerSingleStatMap[stat.CustomerServiceId] = []*qmsg.ModelCustomerServiceSingleStat{stat}
			continue
		}
		customerSingleStatMap[stat.CustomerServiceId] = append(singleStatList, stat)
	}

	// 处理不传字段的情况
	fieldList, err = getCustomerStatFieldList(ctx, fieldList)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, nil, err
	}

	// 处理每个成员单独的记录
	for customerId, statList := range customerSingleStatMap {
		msgDataList, err := getCustomerMsgDataList(statList, fieldList)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, nil, err
		}
		var obj = &qmsg.GetPersonMsgListRsp_PersonData{
			Id:   customerId,
			List: msgDataList,
		}
		modelUser, ok := user4DbMap[customerId]
		if ok {
			obj.Name = modelUser.Username
		}
		robotIds, ok := userRobotRef[customerId]
		if ok {
			obj.BelongUserIdList = robotIds
		}
		list = append(list, obj)
	}
	for _, robot := range sysRep.List {
		personDataMap[robot.Uid] = &qmsg.GetPersonMsgListRsp_PersonData{
			Id:     robot.Uid,
			Name:   robot.Name,
			Avatar: robot.AvatarUrl,
		}
	}
	return list, personDataMap, nil
}
