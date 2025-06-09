package impl

import (
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/core"
	"git.pinquest.cn/qlb/qmsg"
	"testing"
)

func TestWs(t *testing.T) {
	ctx := core.NewInternalRpcCtx(2000772, 2000273, 0)
	log.Infof("reqid %s", ctx.GetReqId())
	for i := 0; i < 100; i++ {
		err := pushWsMsg(ctx, 2000772, 2000273, 11223, []*qmsg.WsMsgWrapper{
			{
				MsgType: uint32(100 + i),
			},
		})
		if err != nil {
			log.Errorf("err:%v", err)
			return
		}
	}
}
