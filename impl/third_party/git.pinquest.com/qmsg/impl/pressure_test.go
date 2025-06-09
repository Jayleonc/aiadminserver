package impl

import (
	"fmt"
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/redisgroup"
	"git.pinquest.cn/qlb/brick/smq"
	"git.pinquest.cn/qlb/brick/utils"
	"git.pinquest.cn/qlb/core"
	"git.pinquest.cn/qlb/qrobot"
	"git.pinquest.cn/qlb/quan"
	"git.pinquest.cn/qlb/yc"
	"sync"
	"testing"
	"time"
)

func TestProcessRecvYcChatMsg(t *testing.T) {
	ctx := core.NewInternalRpcCtx(2000835, 2000339, 0)
	//InitConfig()
	//InitState()
	s = &State{}
	s.RedisGroup, _ = redisgroup.New("redis4session", "pinquestdev")
	s.Conf = &Config{DbMsgBox: "$dispatch.mysql.msg"}
	now := uint32(time.Now().Unix())
	log.Infof("req id :%s", ctx.GetReqId())
	log.Infof("start:%d", now)
	ch := make(chan int, 100)

	var wg sync.WaitGroup

	go func() {
		for i := 0; i < 50000; i++ {
			ch <- i
			wg.Add(1)
			//go func(i int) {
			//	defer wg.Done()
			//}(i)
		}
	}()

	select {
	case i := <-ch:
		go func(i int) {
			defer wg.Done()
			err := processRecvYcChatMsg(ctx, &smq.ConsumeReq{
				CreatedAt:   now,
				Data:        nil,
				CorpId:      2000835,
				AppId:       2000339,
				MsgId:       utils.GetRandomString(16, 0),
				MaxRetryCnt: 5,
			}, &qrobot.RecvChatMsgReq{
				MsgData: &yc.RecvMsgData{
					MsgType:          2001,
					MsgId:            fmt.Sprintf("mock_TestYcPrivateSendCallbackV2_%s_%d", ctx.GetReqId(), i),
					SenderSerialNo:   fmt.Sprintf("mockFollowYcSerialNo_WGjMV0rQ6CVWOeVw_%d_%d", i%250, i%20000),
					ReceiverSerialNo: fmt.Sprintf("mockYcSerialNo_WGjMV0rQ6CVWOeVw_%d", i%250),
					MsgContent:       fmt.Sprintf("%d", i),
					VoiceTime:        0,
				},
				RobotSerialNo: fmt.Sprintf("mockYcSerialNo_WGjMV0rQ6CVWOeVw_%d", i%250),
				ChatType:      1,
				SentAt:        now,
				ExtraMsg: &quan.ExtraMsg{
					ExtraMsgType: 5,
					MsgText:      fmt.Sprintf("%d", i),
				},
			})
			if err != nil {
				log.Errorf("err:%v", err)
				return
			}
		}(i)
	}
	wg.Wait()
	log.Infof("end:%d", time.Now().Unix())

}
