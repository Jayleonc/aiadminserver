package impl

import (
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/core"
	"git.pinquest.cn/qlb/qmsg"
)

func DelAtRecord(ctx *rpc.Context, req *qmsg.DelAtRecordReq) (*qmsg.DelAtRecordRsp, error) {
	var rsp qmsg.DelAtRecordRsp

	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)

	_, err := AtRecord.WhereCorpApp(corpId, appId).Where(DbRobotUid, req.RobotUid).Where(DbCid, req.Cid).Where("msg_seq <=", req.MsgSeq).Delete(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	return &rsp, nil
}

func GetAtRecordList(ctx *rpc.Context, req *qmsg.GetAtRecordListReq) (*qmsg.GetAtRecordListRsp, error) {
	var rsp qmsg.GetAtRecordListRsp

	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)

	atRecordMap := map[uint64]*qmsg.GetAtRecordListRsp_RecordList{}

	var recordList []*qmsg.ModelAtRecord
	err := AtRecord.Select(DbCid, DbMsgSeq, DbSenderAccountId).WhereCorpApp(corpId, appId).Where(DbRobotUid, req.RobotUid).WhereIn(DbCid, req.CidList).Find(ctx, &recordList)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	for _, record := range recordList {
		if list, ok := atRecordMap[record.Cid]; ok {
			atRecordMap[record.Cid].List = append(list.List, record)
		} else {
			atRecordMap[record.Cid] = &qmsg.GetAtRecordListRsp_RecordList{List: []*qmsg.ModelAtRecord{record}}
		}
	}

	rsp.AtRecordMap = atRecordMap

	return &rsp, nil
}
