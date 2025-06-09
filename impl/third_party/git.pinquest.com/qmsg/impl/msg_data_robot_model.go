package impl

import (
	"fmt"

	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/brick/utils"
	"git.pinquest.cn/qlb/core"
	"git.pinquest.cn/qlb/qmsg"
	"git.pinquest.cn/qlb/qris"
	"git.pinquest.cn/qlb/quan"
	"github.com/360EntSecGroup-Skylar/excelize/v2"
)

type MsgRobotModel struct {
	CorpId uint32
	AppId  uint32
}

func (m *MsgRobotModel) ExportPersonMsg(ctx *rpc.Context, listOption *core.ListOption, uidList []uint64, f *excelize.File, titleList []interface{}) error {
	// 获取数据
	sysRsp, err := GetPersonMsgInfoList(ctx, &qmsg.GetPersonMsgInfoListReq{
		ListOption: listOption.AddOpt(qmsg.GetLineChatDataListReq_ListOptionUidList, uidList),
		PersonType: uint32(qmsg.GetLineChatDataListReq_PersonTypeRobot),
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	// 去重一下人员ID
	unique := utils.PluckUint64(sysRsp.RobotList, "RobotUid").Unique()
	for i, v := range unique {
		// 得到每个人员的数据名称
		user, ok := sysRsp.RobotMap[v]
		// 得到要写哪个sheet
		var values string
		if ok && user.Name != "" {
			values = fmt.Sprintf("%s(%d)", user.Name, user.Uid)
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
		for _, val := range sysRsp.RobotList {
			if val.RobotUid == v {
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

func (m *MsgRobotModel) GetLineInfoList(ctx *rpc.Context, listOption *core.ListOption, msgStatTimeScope *qmsg.ModelMsgStatTimeScope) ([]*qmsg.GetLineChatDataListRsp_DataList, map[uint64]string, error) {
	var fieldList []uint32
	var dateList []uint32
	var isNeedUid bool

	db := RobotSingleStat.WhereCorpApp(m.CorpId, m.AppId)
	err := core.NewListOptionProcessor(listOption).
		AddUint64List(qmsg.GetLineChatDataListReq_ListOptionUidList, func(valList []uint64) error {
			db.WhereIn(DbRobotUid, valList)
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

	var robotSingleStatList []*qmsg.ModelRobotSingleStat
	err = db.Find(ctx, &robotSingleStatList)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, nil, err
	}
	if len(robotSingleStatList) == 0 {
		return []*qmsg.GetLineChatDataListRsp_DataList{}, map[uint64]string{}, nil
	}
	// 处理数据
	robotUidList := utils.PluckUint64(robotSingleStatList, "RobotUid").Unique()
	list, err := quan.GetUserList(ctx, &quan.GetUserListReq{
		ListOption: core.NewListOption().
			AddOpt(quan.GetUserListReq_ListOptionUidList, robotUidList).
			AddOpt(quan.GetUserListReq_ListOptionNewAccountTypeList, "3,4"),
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, nil, err
	}

	// 得到机器人名字
	userNameMap := map[uint64]string{}
	for _, user := range list.List {
		userNameMap[user.Id] = user.WwUserName
	}

	// 如果是全部客服 ， 数据汇总成一条,按时间划分
	var dataList []*qmsg.GetLineChatDataListRsp_DataList
	if !isNeedUid {
		var rspDataList = &qmsg.GetLineChatDataListRsp_DataList{
			List: []*qmsg.GetLineChatDataListRsp_ShowLineChatData{},
		}
		for _, dateVal := range dateList {
			var data = &qmsg.GetLineChatDataListRsp_ShowLineChatData{
				Date:  fmt.Sprintf("%d", dateVal),
				Count: 0,
			}
			msgDataMap, err := getRobotMsgDataMap(robotSingleStatList, fieldList, dateVal, 0)
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
		return dataList, userNameMap, nil
	}
	for _, val := range robotUidList {
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
			msgDataMap, err := getRobotMsgDataMap(robotSingleStatList, fieldList, dateVal, val)
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

func (m *MsgRobotModel) GetMsgDetailsList(ctx *rpc.Context, beginDate, endDate uint32, valUidList []uint64, fieldList []uint32) (map[uint64]*qmsg.GetPersonMsgListRsp_PersonData, []uint64, []uint64, []*qmsg.ModelRobotSingleStat, []*qmsg.ModelCustomerServiceSingleStat, error) {
	// 得到成员维度的单聊统计
	db := RobotSingleStat.WhereCorpApp(m.CorpId, m.AppId)
	if beginDate != 0 && endDate != 0 {
		db.WhereBetween(DbDate, beginDate, endDate)
	}

	var robotList []*qmsg.ModelRobotSingleStat
	err := db.WhereIn(DbRobotUid, valUidList).
		OrderAsc(DbDate, DbStartAt).
		Find(ctx, &robotList)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, nil, nil, nil, nil, err
	}

	robotUidList := utils.PluckUint64(robotList, "RobotUid")

	// 按成员维度获取字段 - 数据明细
	fieldList, err = getRobotStatFieldList(ctx, fieldList)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, nil, nil, nil, nil, err
	}
	// 获取数据明细
	personDataMap := make(map[uint64]*qmsg.GetPersonMsgListRsp_PersonData, len(robotList))
	for _, stat := range robotList {
		list, err := getMsgDataList([]*qmsg.ModelRobotSingleStat{stat}, fieldList)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, nil, nil, nil, nil, err
		}
		personDataMap[stat.Id] = &qmsg.GetPersonMsgListRsp_PersonData{
			List: list,
		}
	}
	return personDataMap, robotUidList, []uint64{}, robotList, []*qmsg.ModelCustomerServiceSingleStat{}, nil
}

func (m *MsgRobotModel) GetMsgInfoList(ctx *rpc.Context, listOption *core.ListOption) ([]*qmsg.GetPersonMsgListRsp_PersonData, map[uint64]*qmsg.GetPersonMsgListRsp_PersonData, error) {
	var fieldList []uint32

	// 取出成员的记录
	db := RobotSingleStat.WhereCorpApp(m.CorpId, m.AppId)
	err := core.NewListOptionProcessor(listOption).
		AddString(qmsg.GetLineChatDataListReq_ListOptionName, func(val string) error {
			sysRep, err := quan.GetUserList(ctx, &quan.GetUserListReq{
				ListOption: core.NewListOption().
					AddOpt(quan.GetUserListReq_ListOptionName, val),
			})
			if err != nil {
				log.Errorf("err:%v", err)
				return err
			}
			// 搜索名字得到的UidList
			db.WhereIn(DbRobotUid, utils.PluckUint64(sysRep.List, "Id").Unique())
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
		AddUint32List(qmsg.GetLineChatDataListReq_ListFieldList, func(valList []uint32) error {
			fieldList = valList
			return nil
		}).
		Process()
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, nil, err
	}

	// 得到相关的数据
	var robotSingleStatList []*qmsg.ModelRobotSingleStat
	err = db.Find(ctx, &robotSingleStatList)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, nil, err
	}

	// 数据库找不到数据
	if len(robotSingleStatList) == 0 {
		return []*qmsg.GetPersonMsgListRsp_PersonData{}, map[uint64]*qmsg.GetPersonMsgListRsp_PersonData{}, nil
	}

	// 处理数据
	robotUidList := utils.PluckUint64(robotSingleStatList, "RobotUid").Unique()
	// 得到机器人信息
	sysRep, err := qris.GetRobotList(ctx, &qris.GetRobotListReq{
		ListOption: core.NewListOption().AddOpt(qris.GetRobotListReq_ListOptionRobotUidList, robotUidList),
		CorpId:     m.CorpId,
		AppId:      m.AppId,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, nil, err
	}

	var robotInfoMap map[uint64]*qris.Robot
	utils.KeyBy(sysRep.List, "Uid", &robotInfoMap)

	// 得到机器人关联客服的信息
	var userRobotList []*qmsg.ModelUserRobot
	err = UserRobot.WhereCorpApp(m.CorpId, m.AppId).WhereIn(DbRobotUid, robotUidList).Find(ctx, &userRobotList)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, nil, err
	}

	userIdList := utils.PluckUint64(userRobotList, "Uid")
	robotUserRef := map[uint64][]uint64{}
	for _, userRobot := range userRobotList {
		robotUserRef[userRobot.RobotUid] = append(robotUserRef[userRobot.RobotUid], userRobot.Uid)
	}

	// 得到客服信息
	var userMap map[uint64]*qmsg.ModelUser
	{
		var userList []*qmsg.ModelUser
		err = User.WhereCorpApp(m.CorpId, m.AppId).WhereIn(DbId, userIdList).Find(ctx, &userList)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, nil, err
		}

		utils.KeyBy(userList, "Id", &userMap)
	}

	// 记录按成员分类
	robotSingleStatMap := map[uint64][]*qmsg.ModelRobotSingleStat{}
	for _, stat := range robotSingleStatList {
		robotSingleStatMap[stat.RobotUid] = append(robotSingleStatMap[stat.RobotUid], stat)
	}

	// 按成员维度 - 数据明细
	fieldList, err = getRobotStatFieldList(ctx, fieldList)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, nil, err
	}

	// 处理每个成员单独的记录
	var list []*qmsg.GetPersonMsgListRsp_PersonData
	for robotUid, statList := range robotSingleStatMap {
		msgDataList, err := getMsgDataList(statList, fieldList)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, nil, err
		}
		var obj = &qmsg.GetPersonMsgListRsp_PersonData{
			Id:   robotUid,
			List: msgDataList,
		}

		robot, ok := robotInfoMap[robotUid]
		if ok {
			obj.Avatar = robot.AvatarUrl
			obj.Name = robot.Name
		}
		userIds, ok := robotUserRef[robotUid]
		if ok {
			obj.BelongUserIdList = userIds
		}
		list = append(list, obj)
	}

	// 处理所属人员
	personDataMap := make(map[uint64]*qmsg.GetPersonMsgListRsp_PersonData, len(userMap))
	for id, user := range userMap {
		personDataMap[id] = &qmsg.GetPersonMsgListRsp_PersonData{
			Id:   id,
			Name: user.Username,
		}
	}

	return list, personDataMap, nil
}
