package impl

import (
	"fmt"
	"git.pinquest.cn/qlb/core"
	"git.pinquest.cn/qlb/qmsg"
	"testing"
	"time"
)

func TestSelect(t *testing.T) {
	type result struct {
		Uid uint64
	}
	var assignChatList []*result
	_ = AssignChat.WhereCorpApp(2000772, 2000273).WhereIn(DbUid, []uint64{75, 94, 103, 106, 108, 109}).Select(DbUid, "count(*) as c").Group(fmt.Sprintf("%s HAVING count(*) < %d", DbUid, 10)).Find(nil, &assignChatList)
	fmt.Printf("%v", assignChatList)

}

func TestRoundRobin(t *testing.T) {
	corpId := uint32(2000772)
	appId := uint32(2000273)
	ctx := core.NewInternalRpcCtx(corpId, appId, 0)
	var mgr = NewRoundRobin()
	go func() {
		ringBuffer3, err := mgr.InitRingBuffer(ctx, corpId, appId, nil)
		if err != nil {
			t.Errorf("err: %v", err)
			return
		}
		t.Log("===> r3:", ringBuffer3.Len())
	}()
	ringBuffer, err := mgr.InitRingBuffer(ctx, corpId, appId, nil)
	if err != nil {
		t.Errorf("err: %v", err)
		return
	}
	ringBuffer2, err := mgr.GetRingBuffer(ctx, corpId, appId)
	if err != nil {
		t.Errorf("err: %v", err)
		return
	}
	time.Sleep(time.Second * 3)
	if ringBuffer != ringBuffer2 {
		t.Errorf("ringBuffer != ringBuffer2")
		return
	}
	_len1 := ringBuffer.Len()
	t.Log("===> len: ", _len1)
	r := ringBuffer.Pop()

	cur := ringBuffer.cursor
	if r == cur && ringBuffer.Len() != 1 {
		t.Errorf("r==cur")
		return
	}

	for p := cur.Prev(); p != cur; p = p.Prev() {
		if p == r {
			t.Log("pop is not really pop")
		}
	}
	//ringBuffer.Del(r)
	//_len2 := ringBuffer.Len()
	//if _len2 == _len1 {
	//	t.Errorf("_len1 == _len2")
	//	return
	//}
	//for p := cur.Prev(); p != cur; p = p.Prev() {
	//	if p == r {
	//		t.Errorf("p==r")
	//		return
	//	}
	//}

	mgr.AddRing(ctx, corpId, appId, 123456)

	for p := cur.Prev(); p != cur; p = p.Prev() {
		if p.Value.(uint64) == 123456 {
			t.Log("push 123456 successfully")
		}
	}
	mgr.RemoveRing(ctx, corpId, appId, 123456)

	mgr.DelRingBuffer(corpId)

	uid, scene, err := findAssignUserByRound(ctx, corpId, appId, 0, 0, false, 0, &qmsg.ModelSessionConfig{
		Id:        0,
		CreatedAt: 0,
		UpdatedAt: 0,
		CorpId:    0,
		AppId:     0,
		AssignSetting: &qmsg.ModelSessionConfig_AssignSetting{
			OnlyOnline:      true,
			MaxChatNum:      1000,
			IsBlockRobotMsg: false,
			AssignStrategy:  uint32(qmsg.ModelSessionConfig_AssignStrategyRound),
		},
		SessionSetting:    nil,
		OfficeHourSetting: nil,
	})
	if err != nil {
		t.Errorf("err: %v", err)
		return
	}
	t.Log(uid, scene)
}
