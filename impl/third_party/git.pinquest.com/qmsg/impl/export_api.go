package impl

import (
	"fmt"
	"time"

	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/brick/utils"
	"git.pinquest.cn/qlb/commonv2"
	"git.pinquest.cn/qlb/core"
	"git.pinquest.cn/qlb/qmsg"
	"github.com/360EntSecGroup-Skylar/excelize/v2"
)

// ExportPersonMsgList 导出会话质检数据明细 - 暂无大数据量分片操作
func ExportPersonMsgList(ctx *rpc.Context, req *qmsg.ExportPersonMsgListReq) (*qmsg.ExportPersonMsgListRsp, error) {
	var rsp qmsg.ExportPersonMsgListRsp
	var fieldList []uint32
	var title = "会话质检数据明细"

	taskKey, err := commonv2.GenAsyncTaskKey("export_msg_data_stat")
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)

	err = core.NewListOptionProcessor(req.ListOption).
		AddUint32List(qmsg.GetLineChatDataListReq_ListFieldList, func(valList []uint32) error {
			fieldList = valList
			return nil
		}).
		AddTimeStampRange(qmsg.GetLineChatDataListReq_ListOptionTimeRange, func(beginAt, endAt uint32) error {
			tmBegin := time.Unix(int64(beginAt), 0).Format("2006/01/02")
			tmEndAt := time.Unix(int64(endAt), 0).Format("2006/01/02")
			if err != nil {
				log.Errorf("err:%v", err)
				return err
			}
			title = fmt.Sprintf("%s-%s%s", tmBegin, tmEndAt, title)
			return nil
		}).
		Process()
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	// 执行异步任务
	rsp.TaskKey, err = commonv2.ExportExcelV4(ctx, s.Conf.ResourceType, fmt.Sprintf("%s.xlsx", title),
		&commonv2.AsyncTaskReq{
			IsAddTaskList:      true,
			TaskKey:            taskKey,
			MaxExecTimeSeconds: 0,
			AppType:            0,
			TaskType:           uint32(commonv2.TaskType_TaskTypeExport),
			TaskName:           fmt.Sprintf("%s %s", title, utils.TimeStamp2DateYmdhi(utils.Now())),
		},
		func(ctx *rpc.Context, f *excelize.File) error {
			var uidList []uint64

			// 区分用户类型获取指定的字段值
			fieldList, err = getExportFieldList(ctx, req.PersonType, fieldList)
			if err != nil {
				log.Errorf("err:%v", err)
				return err
			}

			// 得到需要导出哪些用户
			list, err := GetPersonMsgList(ctx, &qmsg.GetPersonMsgListReq{
				PersonType: req.PersonType,
				ListOption: req.ListOption,
			})
			if err != nil {
				log.Errorf("err:%v", err)
				return err
			}
			uidList = utils.PluckUint64(list.List, "Id")

			// 得到表头
			var titleList []interface{}
			titleList = append(titleList, "时间", "时间段")

			// 校验需要哪些字段,得到表头
			tableTitle := getExportTableTitle(fieldList)
			titleList = append(titleList, tableTitle...)

			// 根据人员类型分类写入表格
			dataModel, err := GetMsgDataModelByType(ctx, req.PersonType, corpId, appId)
			if err != nil {
				log.Errorf("err:%v", err)
				return err
			}

			err = dataModel.ExportPersonMsg(ctx, req.ListOption, uidList, f, titleList)
			if err != nil {
				log.Errorf("err:%v", err)
				return err
			}
			return nil
		})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	return &rsp, nil
}

// getExportTableTitle 得到导出表头
func getExportTableTitle(fieldList []uint32) []interface{} {
	var fieldNameList []interface{}
	for _, field := range fieldList {
		switch field {
		case uint32(qmsg.Field_FieldChatCountAll):
			fieldNameList = append(fieldNameList, "单聊数")
		case uint32(qmsg.Field_FieldSendMsgCountAll):
			fieldNameList = append(fieldNameList, "发送消息条数")
		case uint32(qmsg.Field_FieldReceiveMsgCountAll):
			fieldNameList = append(fieldNameList, "接收消息数")
		case uint32(qmsg.Field_FieldTurnCountAll):
			fieldNameList = append(fieldNameList, "对话轮次")
		case uint32(qmsg.Field_FieldTurnCountAver):
			fieldNameList = append(fieldNameList, "平均对话轮次")
		case uint32(qmsg.Field_FieldRelyChatCountProportion):
			fieldNameList = append(fieldNameList, "已回复单聊占比")
		case uint32(qmsg.Field_FieldFirstReplyCountProportion):
			fieldNameList = append(fieldNameList, "首次响应及时率")
		case uint32(qmsg.Field_FieldFirstReplyCountAll):
			fieldNameList = append(fieldNameList, "首次响应超时次数")
		case uint32(qmsg.Field_FieldTimeoutCountAll):
			fieldNameList = append(fieldNameList, "回复超时次数")
		case uint32(qmsg.Field_FieldFirstTimeoutCountAver):
			fieldNameList = append(fieldNameList, "平均首次响应时长")
		case uint32(qmsg.Field_FieldSensitiveCount):
			fieldNameList = append(fieldNameList, "敏感词触发次数")
		case uint32(qmsg.Field_FieldReplyChatCount):
			fieldNameList = append(fieldNameList, "总已回复单聊数")
		case uint32(qmsg.Field_FieldNoReplyCount):
			fieldNameList = append(fieldNameList, "总无需回复次数")
		default:
			log.Errorf("field:%v", field)
		}

	}
	return fieldNameList
}

// getExportFieldList 得到导出字段
func getExportFieldList(ctx *rpc.Context, personType uint32, fieldListFormReq []uint32) ([]uint32, error) {
	if personType == uint32(qmsg.GetLineChatDataListReq_PersonTypeCustomer) {
		fieldList, err := getCustomerStatFieldList(ctx, fieldListFormReq)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
		return fieldList, nil
	} else if personType == uint32(qmsg.GetLineChatDataListReq_PersonTypeRobot) {
		fieldList, err := getRobotStatFieldList(ctx, fieldListFormReq)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
		return fieldList, nil
	}
	return []uint32{}, nil
}

// getExportDataList 得到导出数据-行-明细
func getExportDataList(id uint64, date uint32, startAt uint32, endAt uint32, personDataMap map[uint64]*qmsg.GetPersonMsgListRsp_PersonData) ([]interface{}, error) {
	var record []interface{}
	// 得到指定时间的格式的数据
	getTimeScope2Hour := func(minutes uint32) string {
		minute := minutes % 60
		hour := minutes / 60
		return fmt.Sprintf("%02d:%02d", hour, minute)
	}

	// 处理时间
	location, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	// 时间
	valDate, err := time.ParseInLocation("20060102", fmt.Sprintf("%d", date), location)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	record = append(record, valDate.Format("2006/01/02"))
	// 时间段
	if startAt == 0 && endAt == 0 {
		record = append(record, "全天")
	} else {
		record = append(record, fmt.Sprintf("%s~%s", getTimeScope2Hour(startAt), getTimeScope2Hour(endAt)))
	}

	// 循环数据单体
	dataObj, ok := personDataMap[id]
	if !ok {
		return nil, rpc.InvalidArg("not dataObj")
	}
	// 写入该用户的数据
	for _, data := range dataObj.List {
		//  总平均对话轮次(次) 总已回复单聊占比 总首次响应及时率 总平均首次响应时长 - 特殊小数点后两位
		if data.FieldId == uint32(qmsg.Field_FieldTurnCountAver) {
			record = append(record, fmt.Sprintf("%.2f", float64(data.Count)/100))
		} else if data.FieldId == uint32(qmsg.Field_FieldRelyChatCountProportion) ||
			data.FieldId == uint32(qmsg.Field_FieldFirstReplyCountProportion) {
			record = append(record, fmt.Sprintf("%v%%", fmt.Sprintf("%.2f", float64(data.Count)/100)))
		} else if data.FieldId == uint32(qmsg.Field_FieldFirstTimeoutCountAver) {
			record = append(record, fmt.Sprintf("%.2fs", float64(data.Count)/100))
		} else {
			record = append(record, data.Count)
		}
	}
	return record, nil
}
