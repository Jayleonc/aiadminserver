package impl

import (
	"fmt"
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/commonv2"
	"git.pinquest.cn/qlb/core"
	"git.pinquest.cn/qlb/qmsg"
	"testing"
	"time"
)

func TestGetMsgNotifyDataList(t *testing.T) {
	ctx := core.NewInternalRpcCtx4Test(2000772, 2000273, 1257)
	sysRsp, err := GetMsgNotifyDataList(ctx, &qmsg.GetMsgNotifyDataListReq{
		//NotifyType: uint32(qmsg.GetMsgNotifyDataListReq_NotifyTypeTimeOut),
		NotifyType: uint32(qmsg.GetMsgNotifyDataListReq_NotifyTypeSensitive),
		//PersonType: uint32(qmsg.GetLineChatDataListReq_PersonTypeCustomer),
		PersonType: uint32(qmsg.GetLineChatDataListReq_PersonTypeRobot),
		Date:       20220406,
		//Date: 20220307,
		MsgStatTimeScope: &qmsg.ModelMsgStatTimeScope{
			Config: &qmsg.ModelMsgStatTimeScope_Config{
				StatTimeScopeType: uint32(qmsg.StatTimeScopeType_StatTimeScopeTypeAll),
				TimeScopeList:     []*qmsg.TimeScope{},
			},
		},
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return
	}
	for _, data := range sysRsp.List {
		log.Infof("data:%+v \n", data.TotalTrigger)
	}
}

func TestGetStatFieldList(t *testing.T) {
	ctx := InitTestEnvWithCorp(2000772, 2000273, 1257)
	sysRsp, err := GetStatFieldList(ctx, &qmsg.GetStatFieldListReq{
		FieldType: 3,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return
	}
	for _, val := range sysRsp.List {
		log.Infof("val:%+v \n", val)
	}
}

func TestSetStatFieldList(t *testing.T) {
	ctx := InitTestEnvWithCorp(2000772, 2000273, 1257)
	sysRsp, err := SetStatFieldList(ctx, &qmsg.SetStatFieldListReq{
		List: []uint32{1, 2, 3, 4},
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return
	}
	log.Infof("sysRsp:%+v \n", sysRsp.List)
}

func TestDemo(t *testing.T) {
	beginAt, endAt, err := converTimeDate(uint32(time.Now().Unix()), uint32(time.Now().Unix()))
	if err != nil {
		log.Errorf("err:%v", err)
		return
	}
	list := GetDateList(uint32(20220302), uint32(20220406))
	fmt.Printf("beginAt:%d, endAt:%d \n", beginAt, endAt)
	fmt.Printf("list:%+v \n", list)
}

func TestGetMsgDataStatList(t *testing.T) {
	ctx := InitTestEnvWithCorp(2000772, 2000273, 1257)
	sysRep, err := GetMsgDataStatList(ctx, &qmsg.GetMsgDataStatListReq{
		ListOption: core.NewListOption().
			AddOpt(qmsg.GetLineChatDataListReq_ListOptionTimeRange, fmt.Sprintf("%d,%d", time.Now().Unix()-86400*4, time.Now().Unix())),
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return
	}
	log.Infof("%+v", sysRep.List)
}

func TestGetLineChatDataList(t *testing.T) {
	ctx := InitTestEnvWithCorp(2000772, 2000273, 1257)
	sysRep, err := GetLineChatDataList(ctx, &qmsg.GetLineChatDataListReq{
		ListOption: core.NewListOption().
			AddOpt(qmsg.GetLineChatDataListReq_ListOptionTimeRange, fmt.Sprintf("%d,%d", 1648828800, 1649952000)).
			//AddOpt(qmsg.GetLineChatDataListReq_ListOptionUidList, []uint64{31220}).
			AddOpt(qmsg.GetLineChatDataListReq_ListFieldList, []uint32{uint32(qmsg.Field_FieldChatCountAll)}),

		//PersonType: uint32(qmsg.GetLineChatDataListReq_PersonTypeCustomer),
		PersonType: uint32(qmsg.GetLineChatDataListReq_PersonTypeRobot),
		MsgStatTimeScope: &qmsg.ModelMsgStatTimeScope{
			Config: &qmsg.ModelMsgStatTimeScope_Config{
				StatTimeScopeType: uint32(qmsg.StatTimeScopeType_StatTimeScopeTypeAll),
				TimeScopeList:     []*qmsg.TimeScope{},
			},
		},
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return
	}
	log.Infof("%+v", sysRep.DataList)
	log.Infof("%+v", sysRep.UserNameMap)
}

func TestGetPersonMsgList(t *testing.T) {
	ctx := InitTestEnvWithCorp(2000772, 2000273, 1257)
	list, err := GetPersonMsgList(ctx, &qmsg.GetPersonMsgListReq{
		ListOption: core.NewListOption().
			AddOpt(qmsg.GetLineChatDataListReq_ListOptionTimeRange, fmt.Sprintf("%d,%d", 1648828800, 1649952000)).
			AddOpt(qmsg.GetLineChatDataListReq_ListOptionName, "张"),
		//PersonType: uint32(qmsg.GetLineChatDataListReq_PersonTypeCustomer),
		PersonType: uint32(qmsg.GetLineChatDataListReq_PersonTypeRobot),
	})

	if err != nil {
		log.Errorf("err:%v", err)
		return
	}
	log.Infof("%v", list.List)
	log.Infof("%v", list.PersonDataMap)
}

func TestGetPersonMsgInfoList(t *testing.T) {
	ctx := InitTestEnvWithCorp(2000772, 2000273, 1257)
	list, err := GetPersonMsgInfoList(ctx, &qmsg.GetPersonMsgInfoListReq{
		PersonType: 1,
		ListOption: core.NewListOption().AddOpt(qmsg.GetLineChatDataListReq_ListOptionUidList, []uint64{5229680035}),
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return
	}
	log.Infof("%v", list)
}

func TestExportPersonMsgInfo(t *testing.T) {
	ctx := InitTestEnvWithCorp(2000772, 2000273, 1257)
	list, err := ExportPersonMsgList(ctx, &qmsg.ExportPersonMsgListReq{
		PersonType: 1,
		ListOption: core.NewListOption().AddOpt(qmsg.GetLineChatDataListReq_ListOptionUidList, []uint64{1257}).
			AddOpt(qmsg.GetLineChatDataListReq_ListOptionTimeRange, fmt.Sprintf("%d,%d", time.Now().Unix()-(86400*10), time.Now().Unix())),
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return
	}
	log.Infof("%v", list)
	for {
		rsp, err := commonv2.GetAsyncTaskState(nil, &commonv2.GetAsyncTaskStateReq{
			TaskKey: list.TaskKey,
		})
		if err != nil {
			log.Errorf("err:%v", err)
			return
		}

		if rsp.State == uint32(commonv2.AsyncTaskState_AsyncTaskStateSuccess) {
			log.Infof("export success <url:%v>", rsp.Url)
			return
		} else if rsp.State == uint32(commonv2.AsyncTaskState_AsyncTaskStateFailure) {
			log.Infof("export fail")
			return
		}

		time.Sleep(3 * time.Second)
	}
}

func TestGetTimeScopeList(t *testing.T) {
	ctx := InitTestEnvWithCorp(2000772, 2000273, 1257)
	list, err := GetTimeScopeList(ctx, &qmsg.GetTimeScopeListReq{
		Date: uint32(20220418),
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return
	}
	for _, scope := range list.List[0].Config.TimeScopeList {
		log.Infof("%v , %v , %v", scope.WeekList, scope.StartAt, scope.EndAt)
	}
}
