package impl

import (
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/core"
	"testing"
)

func Test_sendNotifyWithMsgId(t *testing.T) {
	//LaoBai
	ctx := core.NewInternalRpcCtx4Test(2000772, 2000273, 1257)
	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)
	err := notifyStaff(ctx, corpId, appId, "hello", []uint64{5229680879})
	if err != nil {
		log.Errorf("err:%v", err)
	}
}
