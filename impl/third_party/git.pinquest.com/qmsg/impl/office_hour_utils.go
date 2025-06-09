package impl

import (
	"fmt"
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/qmsg"
	"time"
)

func getIsOnWork(ctx *rpc.Context, corpId, appId uint32) (bool, error) {
	var officeHourList []*qmsg.ModelOfficeHour
	err := OfficeHour.WhereCorpApp(corpId, appId).
		Where(fmt.Sprintf("JSON_CONTAINS(convert(week_list using utf8mb4), '[%d]')", time.Now().Weekday())).Find(ctx, &officeHourList)
	if err != nil {
		log.Errorf("err:%v", err)
		return false, err
	}

	now := getNowMinutes()

	for _, officeHour := range officeHourList {
		if int(officeHour.StartAt) <= now && int(officeHour.EndAt) >= now {
			return true, nil
		}
	}

	return false, nil
}

func getNowMinutes() int {
	hour := time.Now().Hour()
	minutes := time.Now().Minute()
	return hour*60 + minutes
}
