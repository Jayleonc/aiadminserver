package impl

import (
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/core"
	"testing"
)

func TestSync(t *testing.T) {
	ctx := core.NewInternalRpcCtx(0, 0, 0)
	err := SyncLastContact(ctx, 0, 100)
	if err != nil {
		log.Errorf("err:%v", err)
	}
}

func TestClearInvalidAssignChat(t *testing.T) {
	ctx := core.NewInternalRpcCtx(0, 0, 0)
	err := ClearInvalidAssignChat(ctx, 0, 0, 100)
	if err != nil {
		log.Errorf("err:%v", err)
	}
}
