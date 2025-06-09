package impl

import (
	"testing"
	"time"

	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/commonv2"
	"git.pinquest.cn/qlb/core"
	"git.pinquest.cn/qlb/qmsg"
)

func TestExportPersonMsgList(t *testing.T) {
	ctx := InitTestEnvWithCorp(2000772, 2000273, 1257)
	list, err := ExportPersonMsgList(ctx, &qmsg.ExportPersonMsgListReq{
		ListOption: core.NewListOption().
			AddOpt(qmsg.GetLineChatDataListReq_ListOptionTimeRange, "1648828800,1649952000").
			AddOpt(qmsg.GetLineChatDataListReq_ListFieldList, "1,2,3,4,5,6,7,8,9,10,11"),
		PersonType: uint32(qmsg.GetLineChatDataListReq_PersonTypeCustomer),
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return
	}
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

func TestDemo2(t *testing.T) {
	ctx := InitTestEnvWithCorp(2000772, 2000273, 1257)
	v2, err := GetUserListV2(ctx, &qmsg.GetUserListV2Req{
		ListOption: core.NewListOption().AddOpt(qmsg.GetUserListV2Req_ListOptionName, "guo"),
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return
	}
	log.Infof("v2:%v", v2)
}
