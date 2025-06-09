package impl

import (
	"git.pinquest.cn/qlb/core"
	"testing"
)

var testCtx = core.NewInternalRpcCtx(0, 0, 0)
var testCorpId = 2000772
var testAppId = 2000273
var testDay uint32 = 20220406

func Test_SyncSingChatStat(t *testing.T) {
	InitTestEnv()
	statRule, err := MsgQualityStatRule.GetCurrentInUseStatRule(testCtx, uint32(testCorpId), uint32(testAppId))
	if err != nil {
		t.Fatal(err)
		return
	}
	err = NewSyncSingleStat(statRule, testDay).Run(testCtx)
	if err != nil {
		t.Fatal(err)
		return
	}
}
