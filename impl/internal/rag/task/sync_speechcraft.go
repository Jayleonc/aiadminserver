package task

import (
	"fmt"
	"git.pinquest.cn/qlb/quan"
	"strconv"

	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/commonv2"
)

func SyncSpeechcraftToDatasetTask(ctx *rpc.Context) (*commonv2.AsyncTaskResult, error) {
	result := &commonv2.AsyncTaskResult{}
	corpIdStr := ctx.GetReqHeader("corp_id")
	corpId, _ := strconv.ParseUint(corpIdStr, 10, 64)
	appIdStr := ctx.GetReqHeader("app_id")
	appId, _ := strconv.ParseUint(appIdStr, 10, 64)

	fmt.Println(corpId)
	sys, err := quan.GetSpeechcraftListSys(ctx, &quan.GetSpeechcraftListSysReq{
		AppId:  uint32(appId),
		CorpId: uint32(corpId),
	})
	if err != nil {
		return nil, err
	}
	fmt.Println("==========")
	fmt.Println(sys)
	fmt.Println("==========")
	return result, nil
}
