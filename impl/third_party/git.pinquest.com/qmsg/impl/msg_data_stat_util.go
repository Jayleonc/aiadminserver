package impl

import (
	"fmt"
	"sort"
	"strconv"
	"time"

	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/agent/opsagent"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/brick/utils"
	"git.pinquest.cn/qlb/core"
	"git.pinquest.cn/qlb/extrpkg/github.com/elliotchance/pie/pie"
	"git.pinquest.cn/qlb/extrpkg/github.com/go-redis/redis"
	"git.pinquest.cn/qlb/qmsg"
	"git.pinquest.cn/qlb/quan"
)

const MsgDataStatKey = "msg_data_stat"

// StatSingleCorpData H5-统计单个企业的昨日统计数据 并且 通知该企业成员
func StatSingleCorpData(ctx *rpc.Context, msgQualityStatRule *qmsg.ModelMsgQualityStatRule) error {
	var (
		msgContent         = "[质检日报]\n"
		detail             = msgQualityStatRule.Detail
		corpId             = msgQualityStatRule.CorpId
		appId              = msgQualityStatRule.AppId
		timeOutNotifyUid   []uint64 // 超时数据的通知员工
		sensitiveNotifyUid []uint64 // 敏感词数据的通知员工
		timeOutContent     string
		sensitiveContent   string
		err                error
	)
	h5Url, err := getStatSingleH5Link()
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	log.Debugf("the corp %d app %d start stat corp data", corpId, appId)
	if detail == nil {
		return rpc.InvalidArg("corp %d app %d not have detail", corpId, appId)
	}
	// 获取该企业的所有员工
	if detail.OverTimeNotify != nil && detail.OverTimeNotify.EnableNotify {
		timeOutNotifyUid = append(timeOutNotifyUid, detail.OverTimeNotify.UidList...)
		timeOutContent, err = statTimeOutData(ctx, corpId, appId)
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
		msgContent = fmt.Sprintf("%s%s\n", msgContent, timeOutContent)
	}

	if detail.SensitiveWordsNotify != nil && detail.SensitiveWordsNotify.EnableNotify {
		sensitiveNotifyUid = append(sensitiveNotifyUid, detail.SensitiveWordsNotify.UidList...)
		sensitiveContent, err = statSensitiveData(ctx, corpId, appId)
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
		msgContent = fmt.Sprintf("%s%s\n", msgContent, sensitiveContent)
	}

	if len(msgContent) == 0 {
		return rpc.InvalidArg("corp %d app %d not open notify config", corpId, appId)
	}

	// 需要发相同的人
	sameUid := utils.IntersectUint64(timeOutNotifyUid, sensitiveNotifyUid)
	if len(sameUid) != 0 {
		err = notifyStaff(ctx, corpId, appId, fmt.Sprintf("%s\n%s",
			msgContent,
			fmt.Sprintf("<a href=\"%v?tab_type=%d\">%v</a>", h5Url, qmsg.H5TabType_H5TabTypeAll, "查看详情")),
			sameUid)
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
	}

	// 只发超时
	timeOutSlice := utils.DiffSlice(timeOutNotifyUid, sameUid)
	timeOutNotifyUid, okT := timeOutSlice.([]uint64)
	if okT && (len(timeOutNotifyUid) != 0) {
		err = notifyStaff(ctx, corpId, appId, fmt.Sprintf("[质检日报]\n%s\n%s", timeOutContent,
			fmt.Sprintf("<a href=\"%v?tab_type=%d\">%v</a>", h5Url, qmsg.H5TabType_H5TabTypeTimeout, "查看详情"),
		), timeOutNotifyUid)
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
	}

	// 只发敏感词
	sensitiveSlice := utils.DiffSlice(sensitiveNotifyUid, sameUid)
	sensitiveNotifyUid, okS := sensitiveSlice.([]uint64)
	if okS && (len(sensitiveNotifyUid) != 0) {
		err = notifyStaff(ctx, corpId, appId, fmt.Sprintf("[质检日报]\n%s\n%s", sensitiveContent,
			fmt.Sprintf("<a href=\"%v?tab_type=%d\">%v</a>", h5Url, qmsg.H5TabType_H5TabTypeSensitive, "查看详情"),
		), sensitiveNotifyUid)
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
	}

	return nil
}

// getStatSingleH5Link 获取H5的链接
func getStatSingleH5Link() (string, error) {
	baseUrl, err := opsagent.GetClusterFrontBaseUrl(nil)
	if err != nil {
		log.Errorf("err:%v", err)
		return "", err
	}

	return fmt.Sprintf("%s/app/quan/#/qt-log-detail", baseUrl), nil
}

// statTimeOutData H5-统计超时数据
func statTimeOutData(ctx *rpc.Context, corpId, appId uint32) (string, error) {
	var (
		notifyTemp = "超时数据\n总回复超时次数: %d次\n总回复超时成员数: %d人\n总回复超时客服数: %d人\n"
	)
	date := getYesterday()
	timeoutCnt, customerCount, robotCount, err := getTimeoutStat(ctx, corpId, appId, date, &qmsg.ModelMsgStatTimeScope{
		Config: &qmsg.ModelMsgStatTimeScope_Config{
			StatTimeScopeType: uint32(qmsg.StatTimeScopeType_StatTimeScopeTypeAll),
		},
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return "", err
	}

	return fmt.Sprintf(notifyTemp, timeoutCnt, robotCount, customerCount), nil
}

// statSensitiveData H5-统计敏感词数据
func statSensitiveData(ctx *rpc.Context, corpId, appId uint32) (string, error) {
	var (
		notifyTemp             = "敏感词数据\n总敏感词触发次数: %d次\n总敏感词触发成员数: %d人\n总敏感词触发客服数: %d人\n"
		totalSensitive         uint32
		totalSensitiveRobot    uint32
		totalSensitiveCustomer uint32
		err                    error
	)

	date := getYesterday()
	totalSensitive, err = RobotSingleStat.WhereCorpApp(corpId, appId).Where(DbDate, date).Sum(ctx, DbSensitiveTriggerCount)
	if err != nil && !RobotSingleStat.IsNotFoundErr(err) {
		log.Errorf("err:%v", err)
		return "", err
	}

	totalSensitiveRobot, err = RobotSingleStat.WhereCorpApp(corpId, appId).Where(DbDate, date).Gt(DbSensitiveTriggerCount, 0).Count(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return "", err
	}

	totalSensitiveCustomer, err = CustomerServiceSingleStat.WhereCorpApp(corpId, appId).Where(DbDate, date).Gt(DbSensitiveTriggerCount, 0).Count(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return "", err
	}

	return fmt.Sprintf(notifyTemp, totalSensitive, totalSensitiveRobot, totalSensitiveCustomer), nil
}

// notifyStaff H5-圈量提醒，通知员工
func notifyStaff(ctx *rpc.Context, corpId, appId uint32, context string, uidList []uint64) error {

	if len(uidList) == 0 {
		log.Debug("uid list empty")
		return nil
	}

	sysRsp, err := quan.GetUserListSys(ctx, &quan.GetUserListSysReq{
		ListOption: core.NewListOption().AddOpt(quan.GetUserListSysReq_ListOptionUidList, uidList),
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	if len(sysRsp.List) == 0 {
		log.Warnf("corpId:%v, appId:%v ,uidList = %v, len(userList) is zero", corpId, appId, uidList)
		return nil
	}
	// 遍历 sysRsp.List
	for _, user := range sysRsp.List {
		sendNotifyWithMsgId(ctx, corpId, appId, context, fmt.Sprintf("%v_%v", notifyMsgIdPrefix, utils.GenRandomStr()), user)
	}
	return nil
}

// GetDateList 获取需要统计的日期
// case params startDate 20220402
// case params endDate 20220405
func GetDateList(startDate, endDate uint32) (out []uint32) {
	if startDate > endDate {
		return out
	}
	st := time.Unix(int64(utils.Day2Second(startDate)), 0)
	startDate = uint32(0)
	for startDate != endDate {
		startDate64, _ := strconv.ParseUint(st.Format("20060102"), 10, 64)
		startDate := uint32(startDate64)
		out = append(out, startDate)
		st = st.Add(time.Second * 3600 * 24)
		if startDate >= endDate {
			return
		}
	}
	return
}

// 时间戳转换为 20220402 的形式
func converTimeDate(beginAt, endAt uint32) (uint32, uint32, error) {
	tmBegin := time.Unix(int64(beginAt), 0).Format("20060102")
	tmEndAt := time.Unix(int64(endAt), 0).Format("20060102")
	begin, err := strconv.Atoi(tmBegin)
	if err != nil {
		log.Errorf("err:%v", err)
		return 0, 0, err
	}
	end, err := strconv.Atoi(tmEndAt)
	if err != nil {
		log.Errorf("err:%v", err)
		return 0, 0, err
	}
	return uint32(begin), uint32(end), nil

}

// genMsgDataStatKey 获取标识该企业的key
func genMsgDataStatKey(corpId, appId, fieldType uint32, uid uint64) string {
	return fmt.Sprintf("%s_%d_%d_%d_%d", MsgDataStatKey, fieldType, corpId, appId, uid)
}

// getMsgDataStatFieldList4Redis 获取该企业设置的字段值
func getMsgDataStatFieldList4Redis(corpId, appId, fieldType uint32, uid uint64) ([]uint32, error) {
	var msgDataFieldMap = make(map[string][]uint32)
	_, okF := qmsg.GetStatFieldListReq_FieldType_name[int32(fieldType)]
	if !okF || fieldType == uint32(qmsg.GetStatFieldListReq_FieldTypeNil) {
		return nil, rpc.InvalidArg("fieldType not null")
	}
	var fieldList []uint32
	err := s.RedisGroup.GetJson(fmt.Sprintf("%s_%d", MsgDataStatKey, fieldType), &msgDataFieldMap)
	if err != nil && err != redis.Nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	fieldList, ok := msgDataFieldMap[genMsgDataStatKey(corpId, appId, fieldType, uid)]
	// 初始化字段数据
	if !ok {
		fieldList = []uint32{}
		for _, val := range qmsg.Field_value {
			if val != 0 {
				fieldList = append(fieldList, uint32(val))
			}
		}
		sort.Slice(fieldList, func(i, j int) bool {
			return fieldList[i] < fieldList[j]
		})
		msgDataFieldMap[genMsgDataStatKey(corpId, appId, fieldType, uid)] = fieldList
		err = s.RedisGroup.SetJson(fmt.Sprintf("%s_%d", MsgDataStatKey, fieldType), msgDataFieldMap, 0)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
	}
	// 修正新加的字段
	if len(fieldList) != (len(qmsg.Field_value) - 1) {
		// 找到缺的字段
		var fieldIdList []uint32
		for _, val := range qmsg.Field_value {
			if val != 0 {
				fieldIdList = append(fieldIdList, uint32(val))
			}
		}
		add, remove := pie.Uint32s(fieldIdList).Diff(fieldList)
		log.Infof("add : %v, remove: %v", add, remove)
		fieldList = append(fieldList, remove...)
		msgDataFieldMap[genMsgDataStatKey(corpId, appId, fieldType, uid)] = fieldList
		err = s.RedisGroup.SetJson(fmt.Sprintf("%s_%d", MsgDataStatKey, fieldType), msgDataFieldMap, 0)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
	}
	return fieldList, err
}

// getMsgDataStatFieldMap4Redis 获取该企业设置的字段开关
func getMsgDataStatFieldMap4Redis(corpId, appId, fieldType uint32, uid uint64) (map[uint32]bool, error) {
	var msgDataFieldMap = make(map[string]map[uint32]bool)
	_, okF := qmsg.GetStatFieldListReq_FieldType_name[int32(fieldType)]
	if !okF || fieldType == uint32(qmsg.GetStatFieldListReq_FieldTypeNil) {
		return nil, rpc.InvalidArg("fieldType not null")
	}
	err := s.RedisGroup.GetJson(fmt.Sprintf("%s_%d_map", MsgDataStatKey, fieldType), &msgDataFieldMap)
	if err != nil && err != redis.Nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	fieldMap, ok := msgDataFieldMap[genMsgDataStatKey(corpId, appId, fieldType, uid)]
	// 初始化字段数据
	if !ok {
		fieldMap = make(map[uint32]bool)
		for _, val := range qmsg.Field_value {
			if val != 0 {
				fieldMap[uint32(val)] = true
			}
		}
		msgDataFieldMap[genMsgDataStatKey(corpId, appId, fieldType, uid)] = fieldMap
		err = s.RedisGroup.SetJson(fmt.Sprintf("%s_%d_map", MsgDataStatKey, fieldType), msgDataFieldMap, 0)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
	}
	// 修正新加的字段
	if len(fieldMap) != (len(qmsg.Field_value) - 1) {
		// 找到缺的字段
		for _, val := range qmsg.Field_value {
			if val == 0 {
				continue
			}
			if _, ok := fieldMap[uint32(val)]; !ok {
				fieldMap[uint32(val)] = false
			}
		}
		msgDataFieldMap[genMsgDataStatKey(corpId, appId, fieldType, uid)] = fieldMap
		err = s.RedisGroup.SetJson(fmt.Sprintf("%s_%d_map", MsgDataStatKey, fieldType), msgDataFieldMap, 0)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
	}
	return fieldMap, err
}

// setMsgDataStatFieldList4Redis 更新该企业的字段值
func setMsgDataStatFieldList4Redis(corpId, appId uint32, fieldList []uint32, fieldType uint32, uid uint64) error {
	var msgDataFieldMap = make(map[string][]uint32)
	_, okF := qmsg.GetStatFieldListReq_FieldType_name[int32(fieldType)]
	if !okF || fieldType == uint32(qmsg.GetStatFieldListReq_FieldTypeNil) {
		return rpc.InvalidArg("fieldType not null")
	}
	err := s.RedisGroup.GetJson(fmt.Sprintf("%s_%d", MsgDataStatKey, fieldType), &msgDataFieldMap)
	if err != nil && err != redis.Nil {
		log.Errorf("err:%v", err)
		return err
	}
	msgDataFieldMap[genMsgDataStatKey(corpId, appId, fieldType, uid)] = fieldList
	err = s.RedisGroup.SetJson(fmt.Sprintf("%s_%d", MsgDataStatKey, fieldType), msgDataFieldMap, 0)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	return nil
}

// setMsgDataStatFieldMap4Redis 更新该企业的字段开关
func setMsgDataStatFieldMap4Redis(corpId, appId uint32, fieldMap map[uint32]bool, fieldType uint32, uid uint64) error {
	var msgDataFieldMap = make(map[string]map[uint32]bool)
	_, okF := qmsg.GetStatFieldListReq_FieldType_name[int32(fieldType)]
	if !okF || fieldType == uint32(qmsg.GetStatFieldListReq_FieldTypeNil) {
		return rpc.InvalidArg("fieldType not null")
	}
	err := s.RedisGroup.GetJson(fmt.Sprintf("%s_%d_map", MsgDataStatKey, fieldType), &msgDataFieldMap)
	if err != nil && err != redis.Nil {
		log.Errorf("err:%v", err)
		return err
	}
	msgDataFieldMap[genMsgDataStatKey(corpId, appId, fieldType, uid)] = fieldMap
	err = s.RedisGroup.SetJson(fmt.Sprintf("%s_%d_map", MsgDataStatKey, fieldType), msgDataFieldMap, 0)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	return nil
}

type MsgDataStatModel struct {
	ChatCount             uint32 // 总单聊数
	SendMsgCount          uint32 // 总发送消息数
	ReceiveMsgCount       uint32 // 总接收消息数
	ReplyChatCount        uint32 // 已回复单聊数
	FirstReplyTotalTime   uint32 // 总首次响应时长
	TurnCount             uint32 // 轮次
	FirstInTimeReplyCount uint32 // 总首次及时响应数量
	FirstTimeoutChatCount uint32 // 总首次响应超时次数
	TimeoutChatCount      uint32 // 总回复超时次数
	SensitiveTriggerCount uint32 // 总敏感词触发次数
	FirstReplyTotalCount  uint32 // 总首次响应次数
	NoReplyCount          uint32 // 无需回复次数
	MsgDataList           []*qmsg.GetMsgDataStatListRsp_ShowMsgStatData
	DataMap               map[uint32]uint32
}

// GetMsgDataStatModel 计算数据的struct
func GetMsgDataStatModel() *MsgDataStatModel {
	return &MsgDataStatModel{
		MsgDataList: []*qmsg.GetMsgDataStatListRsp_ShowMsgStatData{},
		DataMap:     make(map[uint32]uint32),
	}
}

func (m *MsgDataStatModel) addVal(chatCount, sendMsgCount, receiveMsgCount, replyChatCount, firstReplyTotalTime, turnCount, firstInTimeReplyCount, firstTimeoutChatCount, timeoutChatCount, sensitiveTriggerCount, firstReplyTotalCount, noReplyCount uint32) {
	m.ChatCount += chatCount
	m.SendMsgCount += sendMsgCount
	m.ReceiveMsgCount += receiveMsgCount
	m.ReplyChatCount += replyChatCount
	m.FirstReplyTotalTime += firstReplyTotalTime
	m.TurnCount += turnCount
	m.FirstInTimeReplyCount += firstInTimeReplyCount
	m.FirstTimeoutChatCount += firstTimeoutChatCount
	m.TimeoutChatCount += timeoutChatCount
	m.SensitiveTriggerCount += sensitiveTriggerCount
	m.FirstReplyTotalCount += firstReplyTotalCount
	m.NoReplyCount += noReplyCount
}

func (m *MsgDataStatModel) calculateVal(fieldList []uint32) ([]*qmsg.GetMsgDataStatListRsp_ShowMsgStatData, map[uint32]uint32) {
	for _, field := range fieldList {
		var count = 0
		switch field {
		case uint32(qmsg.Field_FieldChatCountAll):
			// 总单聊数(次)
			m.MsgDataList = append(m.MsgDataList, &qmsg.GetMsgDataStatListRsp_ShowMsgStatData{
				FieldId: uint32(qmsg.Field_FieldChatCountAll),
				Count:   m.ChatCount,
			})
			m.DataMap[uint32(qmsg.Field_FieldChatCountAll)] = m.ChatCount
		case uint32(qmsg.Field_FieldSendMsgCountAll):
			// 总发送消息条数(条)
			m.MsgDataList = append(m.MsgDataList, &qmsg.GetMsgDataStatListRsp_ShowMsgStatData{
				FieldId: uint32(qmsg.Field_FieldSendMsgCountAll),
				Count:   m.SendMsgCount,
			})
			m.DataMap[uint32(qmsg.Field_FieldSendMsgCountAll)] = m.SendMsgCount
		case uint32(qmsg.Field_FieldReceiveMsgCountAll):
			// 总接收消息数(条)
			m.MsgDataList = append(m.MsgDataList, &qmsg.GetMsgDataStatListRsp_ShowMsgStatData{
				FieldId: uint32(qmsg.Field_FieldReceiveMsgCountAll),
				Count:   m.ReceiveMsgCount,
			})
			m.DataMap[uint32(qmsg.Field_FieldReceiveMsgCountAll)] = m.ReceiveMsgCount
		case uint32(qmsg.Field_FieldTurnCountAll):
			// 总对话轮次(次)
			m.MsgDataList = append(m.MsgDataList, &qmsg.GetMsgDataStatListRsp_ShowMsgStatData{
				FieldId: uint32(qmsg.Field_FieldTurnCountAll),
				Count:   m.TurnCount,
			})
			m.DataMap[uint32(qmsg.Field_FieldTurnCountAll)] = m.TurnCount
		case uint32(qmsg.Field_FieldTurnCountAver):
			// 总平均对话轮次(次)
			if m.ReplyChatCount != 0 {
				count = int((m.TurnCount * 100) / m.ReplyChatCount)
			}
			m.MsgDataList = append(m.MsgDataList, &qmsg.GetMsgDataStatListRsp_ShowMsgStatData{
				FieldId: uint32(qmsg.Field_FieldTurnCountAver),
				Count:   uint32(count),
			})
			m.DataMap[uint32(qmsg.Field_FieldTurnCountAver)] = uint32(count)
		case uint32(qmsg.Field_FieldRelyChatCountProportion):
			// 总已回复单聊占比
			if m.ChatCount != 0 {
				count = int((m.ReplyChatCount * 10000) / m.ChatCount)
			}
			m.MsgDataList = append(m.MsgDataList, &qmsg.GetMsgDataStatListRsp_ShowMsgStatData{
				FieldId: uint32(qmsg.Field_FieldRelyChatCountProportion),
				Count:   uint32(count),
			})
			m.DataMap[uint32(qmsg.Field_FieldRelyChatCountProportion)] = uint32(count)
		case uint32(qmsg.Field_FieldFirstReplyCountProportion):
			// 总首次响应及时率
			if m.ReplyChatCount != 0 {
				count = int((m.FirstInTimeReplyCount * 10000) / m.ReplyChatCount)
			}
			m.MsgDataList = append(m.MsgDataList, &qmsg.GetMsgDataStatListRsp_ShowMsgStatData{
				FieldId: uint32(qmsg.Field_FieldFirstReplyCountProportion),
				Count:   uint32(count),
			})
			m.DataMap[uint32(qmsg.Field_FieldFirstReplyCountProportion)] = uint32(count)
		case uint32(qmsg.Field_FieldFirstReplyCountAll):
			// 总首次响应超时次数
			m.MsgDataList = append(m.MsgDataList, &qmsg.GetMsgDataStatListRsp_ShowMsgStatData{
				FieldId: uint32(qmsg.Field_FieldFirstReplyCountAll),
				Count:   m.FirstTimeoutChatCount,
			})
			m.DataMap[uint32(qmsg.Field_FieldFirstReplyCountAll)] = m.FirstTimeoutChatCount
		case uint32(qmsg.Field_FieldTimeoutCountAll):
			// 总回复超时次数
			m.MsgDataList = append(m.MsgDataList, &qmsg.GetMsgDataStatListRsp_ShowMsgStatData{
				FieldId: uint32(qmsg.Field_FieldTimeoutCountAll),
				Count:   m.TimeoutChatCount,
			})
			m.DataMap[uint32(qmsg.Field_FieldTimeoutCountAll)] = m.TimeoutChatCount
		case uint32(qmsg.Field_FieldFirstTimeoutCountAver):
			// 总平均首次响应时长
			if m.ReplyChatCount != 0 {
				count = int((m.FirstReplyTotalTime * 100) / m.ReplyChatCount)
			}
			m.MsgDataList = append(m.MsgDataList, &qmsg.GetMsgDataStatListRsp_ShowMsgStatData{
				FieldId: uint32(qmsg.Field_FieldFirstTimeoutCountAver),
				Count:   uint32(count),
			})
			m.DataMap[uint32(qmsg.Field_FieldFirstTimeoutCountAver)] = uint32(count)
		case uint32(qmsg.Field_FieldSensitiveCount):
			// 总敏感词触发次数
			m.MsgDataList = append(m.MsgDataList, &qmsg.GetMsgDataStatListRsp_ShowMsgStatData{
				FieldId: uint32(qmsg.Field_FieldSensitiveCount),
				Count:   m.SensitiveTriggerCount,
			})
			m.DataMap[uint32(qmsg.Field_FieldSensitiveCount)] = m.SensitiveTriggerCount
		case uint32(qmsg.Field_FieldReplyChatCount):
			// 总已回复单聊数
			m.MsgDataList = append(m.MsgDataList, &qmsg.GetMsgDataStatListRsp_ShowMsgStatData{
				FieldId: uint32(qmsg.Field_FieldReplyChatCount),
				Count:   m.ReplyChatCount,
			})
			m.DataMap[uint32(qmsg.Field_FieldReplyChatCount)] = m.ReplyChatCount
		case uint32(qmsg.Field_FieldNoReplyCount):
			// 无需回复次数
			m.MsgDataList = append(m.MsgDataList, &qmsg.GetMsgDataStatListRsp_ShowMsgStatData{
				FieldId: uint32(qmsg.Field_FieldNoReplyCount),
				Count:   m.NoReplyCount,
			})
			m.DataMap[uint32(qmsg.Field_FieldNoReplyCount)] = m.NoReplyCount
		default:
			log.Errorf("not field %d", field)
		}
	}
	return m.MsgDataList, m.DataMap
}

// getMsgDataList 对成员数据进行计算统计
func getMsgDataList(robotSingleStatList []*qmsg.ModelRobotSingleStat, fieldList []uint32) ([]*qmsg.GetMsgDataStatListRsp_ShowMsgStatData, error) {
	if len(robotSingleStatList) == 0 {
		return []*qmsg.GetMsgDataStatListRsp_ShowMsgStatData{}, nil
	}
	msgDataStatModel := GetMsgDataStatModel()
	// 处理机器人单聊数据
	for _, stat := range robotSingleStatList {
		msgDataStatModel.addVal(
			stat.ChatCount,
			stat.SendMsgCount,
			stat.ReceiveMsgCount,
			stat.ReplyChatCount,
			stat.FirstReplyTotalTime,
			stat.TurnCount,
			stat.FirstInTimeReplyCount,
			stat.FirstTimeoutChatCount,
			stat.TimeoutChatCount,
			stat.SensitiveTriggerCount,
			stat.FirstReplyTotalCount,
			stat.NoReplyCount,
		)
	}
	// 得到计算数据
	msgStatDataList, _ := msgDataStatModel.calculateVal(fieldList)
	return msgStatDataList, nil
}

// getCustomerMsgDataList 对客服数据进行计算统计
func getCustomerMsgDataList(customerSingleStatList []*qmsg.ModelCustomerServiceSingleStat, fieldList []uint32) ([]*qmsg.GetMsgDataStatListRsp_ShowMsgStatData, error) {
	if len(customerSingleStatList) == 0 {
		return []*qmsg.GetMsgDataStatListRsp_ShowMsgStatData{}, nil
	}
	msgDataStatModel := GetMsgDataStatModel()
	// 处理机器人单聊数据
	for _, stat := range customerSingleStatList {
		msgDataStatModel.addVal(
			stat.ChatCount,
			stat.SendMsgCount,
			stat.ReceiveMsgCount,
			stat.ReplyChatCount,
			stat.FirstReplyTotalTime,
			stat.TurnCount,
			stat.FirstInTimeReplyCount,
			stat.FirstTimeoutChatCount,
			stat.TimeoutChatCount,
			stat.SensitiveTriggerCount,
			stat.FirstReplyTotalCount,
			stat.NoReplyCount,
		)
	}
	msgDataList, _ := msgDataStatModel.calculateVal(fieldList)
	return msgDataList, nil
}

// getCustomerMsgDataMap 得到客服统计map
func getCustomerMsgDataMap(customerSingleStatList []*qmsg.ModelCustomerServiceSingleStat, fieldList []uint32, date uint32, customerId uint64) (map[uint32]uint32, error) {
	msgDataStatModel := GetMsgDataStatModel()
	// 处理机器人单聊数据
	for _, stat := range customerSingleStatList {
		var canCount bool
		if date != 0 && customerId != 0 {
			canCount = stat.CustomerServiceId == customerId && stat.Date == date
		} else if date != 0 && customerId == 0 {
			canCount = stat.Date == date
		} else if date == 0 && customerId != 0 {
			canCount = stat.CustomerServiceId == customerId
		}
		if canCount {
			msgDataStatModel.addVal(
				stat.ChatCount,
				stat.SendMsgCount,
				stat.ReceiveMsgCount,
				stat.ReplyChatCount,
				stat.FirstReplyTotalTime,
				stat.TurnCount,
				stat.FirstInTimeReplyCount,
				stat.FirstTimeoutChatCount,
				stat.TimeoutChatCount,
				stat.SensitiveTriggerCount,
				stat.FirstReplyTotalCount,
				stat.NoReplyCount,
			)
		}
	}

	_, dataMap := msgDataStatModel.calculateVal(fieldList)
	return dataMap, nil
}

// getRobotMsgDataMap 得到成员统计map
func getRobotMsgDataMap(robotSingleStatList []*qmsg.ModelRobotSingleStat, fieldList []uint32, date uint32, robotId uint64) (map[uint32]uint32, error) {
	msgDataStatModel := GetMsgDataStatModel()
	// 处理机器人单聊数据
	for _, stat := range robotSingleStatList {
		// 可能会有数据错误
		var canCount bool
		if date != 0 && robotId != 0 {
			canCount = stat.RobotUid == robotId && stat.Date == date
		} else if date != 0 && robotId == 0 {
			canCount = stat.Date == date
		} else if date == 0 && robotId != 0 {
			canCount = stat.RobotUid == robotId
		}
		if canCount {
			msgDataStatModel.addVal(
				stat.ChatCount,
				stat.SendMsgCount,
				stat.ReceiveMsgCount,
				stat.ReplyChatCount,
				stat.FirstReplyTotalTime,
				stat.TurnCount,
				stat.FirstInTimeReplyCount,
				stat.FirstTimeoutChatCount,
				stat.TimeoutChatCount,
				stat.SensitiveTriggerCount,
				stat.FirstReplyTotalCount,
				stat.NoReplyCount,
			)
		}
	}

	_, dataMap := msgDataStatModel.calculateVal(fieldList)
	return dataMap, nil
}

// getStatFieldList 获取pc端上方数据概览
func getStatFieldList(ctx *rpc.Context, fieldList []uint32) ([]uint32, error) {
	if len(fieldList) == 0 {
		sysRep, err := GetStatFieldList(ctx, &qmsg.GetStatFieldListReq{
			FieldType: uint32(qmsg.GetStatFieldListReq_FieldTypeStat),
		})
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
		fieldList = sysRep.List
	}
	return fieldList, nil
}

// getRobotStatFieldList 获取下方成员维度数据字段
func getRobotStatFieldList(ctx *rpc.Context, fieldList []uint32) ([]uint32, error) {
	if len(fieldList) == 0 {
		sysRep, err := GetStatFieldList(ctx, &qmsg.GetStatFieldListReq{
			FieldType: uint32(qmsg.GetStatFieldListReq_FieldTypeRobotStat),
		})
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
		fieldList = sysRep.List
	}
	return fieldList, nil
}

// getCustomerStatFieldList 获取下方客服维度数据字段
func getCustomerStatFieldList(ctx *rpc.Context, fieldList []uint32) ([]uint32, error) {
	if len(fieldList) == 0 {
		sysRep, err := GetStatFieldList(ctx, &qmsg.GetStatFieldListReq{
			FieldType: uint32(qmsg.GetStatFieldListReq_FieldTypeCustomerStat),
		})
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
		fieldList = sysRep.List
	}
	return fieldList, nil
}
