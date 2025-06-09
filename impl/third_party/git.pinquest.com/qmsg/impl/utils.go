package impl

import (
	"fmt"
	"git.pinquest.cn/qlb/brick/dbx"
	"git.pinquest.cn/qlb/qmsg"
	"github.com/shopspring/decimal"
	"strconv"
	"time"
)

const (
	projectId = 108
)

func filterRobot(acc *qmsg.ModelUserRobot) bool {
	return acc.RobotType == 0 || acc.RobotType == uint32(qmsg.RobotType_RobotTypePlatform)
}
func filterHostAccount(acc *qmsg.ModelUserRobot) bool {
	return acc.RobotType == uint32(qmsg.RobotType_RobotTypeHostAccount)
}
func ChoiceMsgBoxDb(db *dbx.Scope, corpId uint32) *dbx.Scope {
	return db.UseTable(fmt.Sprintf("%s_%d", (&qmsg.ModelMsgBox{}).TableName(), corpId%20)).UseDb(s.Conf.DbMsgBox)
}

func parseTime(timeStr string) uint32 {
	parse, err := strconv.ParseUint(timeStr, 10, 32)
	if err != nil {
		return 0
	}
	return uint32(parse)
}

// 昨天
func getYesterday() uint32 {
	yesterdayTime := time.Now().AddDate(0, 0, -1)
	yesterday, _ := strconv.ParseUint(yesterdayTime.Format("20060102"), 10, 32)
	return uint32(yesterday)
}

// 四舍五入
func round(origin float32, places uint32) float32 {
	v, _ := decimal.NewFromFloat(float64(origin)).Round(int32(places)).Float64()
	return float32(v)
}

// 求和
func sum(s []uint32) uint32 {
	total := 0
	for item := range s {
		total = total + item
	}
	return uint32(total)
}

// 获取今天是周几 从1开始 1是周一
func getHumanWeek() int {
	return getHumanWeekByTime(time.Now())
}

func getHumanWeekByTime(t time.Time) int {
	weekDay := t.Weekday()
	if weekDay == time.Sunday {
		return 7
	}
	return int(weekDay)
}

func getRevokeAppInfoKey(appInfo string) string {
	return qmsg.ServiceName + "_revoke_" + appInfo
}
