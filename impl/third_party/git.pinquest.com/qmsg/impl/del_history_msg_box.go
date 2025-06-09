package impl

import (
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/qmsg"
	"time"
)

type delCorpInfo struct {
	CorpId       uint32
	AppId        uint32
	DelBeforeDay int //删除几天前到以往的记录
}

func DelHistoryMsgBoxScheduler(ctx *rpc.Context, req *qmsg.DelHistoryMsgBoxSchedulerReq) (*qmsg.DelHistoryMsgBoxSchedulerRsp, error) {
	var rsp qmsg.DelHistoryMsgBoxSchedulerRsp

	key := "del_history_msg_box_scheduler"
	//避免抢节点
	ok, err := s.RedisGroup.SetNX(key, []byte{'0'}, time.Minute)
	if err != nil {
		log.Errorf("err %v", err)
		return nil, err
	}
	if !ok {
		log.Info("DelHistoryMsgBoxScheduler running")
		return &rsp, nil
	}

	log.Infof("del_history_msg_box_scheduler_start")
	log.Infof("del_history_msg_box_scheduler_reqId %v", ctx.GetReqId())

	var delList []*delCorpInfo
	delList = append(delList, &delCorpInfo{CorpId: uint32(2505), AppId: uint32(2505), DelBeforeDay: 3}) //福建中行要求删除3天前聊天记录,只保留三天内

	for _, del := range delList {
		err = del.delHistory(ctx)
		if err != nil {
			log.Errorf("err %v", err)
			return &rsp, err
		}
	}

	return &rsp, nil
}

func (d *delCorpInfo) delHistory(ctx *rpc.Context) error {
	limit := uint32(500)

	now := time.Now()
	delTimeAt := time.Date(now.Year(), now.Month(), now.Day()-d.DelBeforeDay, 0, 0, 0, 0, time.Local).Unix()

	for {

		row, err := ChoiceMsgBoxDb(MsgBox.WhereCorpApp(d.CorpId, d.AppId).
			SetLimit(limit).Lt(DbCreatedAt, delTimeAt),
			d.CorpId).ForceDelete(ctx)
		if err != nil {
			log.Errorf("err %v", err)
			log.Infof("corpId %v err", d.CorpId)
			return err
		}

		if row.RowsAffected < uint64(limit) {
			break
		}
	}
	log.Infof("corpId %v done", d.CorpId)
	return nil
}
