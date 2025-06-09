package impl

import (
	"fmt"
	"git.pinquest.cn/base/log"
	"testing"
	"time"
)

func Test_msgStatYesterdayReportSched(t *testing.T) {
}

func Test_converTimeScope(t *testing.T) {
	//ctx := core.NewInternalRpcCtx4Test(2000772, 2000273, 1257)
	//corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)
	//var timeScope = qmsg.ModelMsgStatTimeScope{
	//	Config: &qmsg.ModelMsgStatTimeScope_Config{
	//		TimeScopeList: []*qmsg.TimeScope{
	//			{
	//				StartAt: 360,
	//				EndAt:   420,
	//			},
	//			{
	//				StartAt: 430,
	//				EndAt:   480,
	//			},
	//		},
	//	},
	//}
	//
	//db := MsgStatTimeOut.WhereCorpApp(corpId, appId).Where(DbDate, 20220402)
	//
	//// 解析时间范围
	//db = converTimeScope(timeScope.Config.TimeScopeList, db)
	//
	//count, err := db.Count(ctx)
	//if err != nil {
	//	log.Errorf("err:%v", err)
	//	return
	//}
	//fmt.Printf("count:%v", count)
}

func Test_converTimeScopeParse(t *testing.T) {
	location, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		log.Errorf("err:%v", err)
		return
	}
	start, _ := time.ParseInLocation("20060102", fmt.Sprintf("%d", 20220402), location)
	fmt.Println(start.Unix())
	startAt, err := time.ParseDuration(fmt.Sprintf("%dm", 360))
	if err != nil {
		log.Errorf("err:%v", err)
		return
	}
	fmt.Println(start.Add(startAt).Unix())
}

func TestDemo1(t *testing.T) {
	h5Url, err := getStatSingleH5Link()
	if err != nil {
		log.Errorf("err:%v", err)
		return
	}
	log.Infof("h5Url:%v", h5Url)
}
