package impl

import (
	"fmt"
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/logstream"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/extrpkg/github.com/go-redis/redis"
	"git.pinquest.cn/qlb/qmsg"
	"time"
)

var topicType = "logicReport"

var LoginSubType = "qmsg_login"
var SendMsgSubType = "qmsg_send_msg"
var MsgIntervalSubType = "qmsg_msg_interval"
var ExtUidZeroSubType = "qmsg_ext_uid_zero"
var SendFailSubType = "qmsg_send_fail"
var SendDelaySubType = "qmsg_send_delay"

var LogStream *logstream.LoggerStream

type LoginStream struct {
	CorpId   uint32 `json:"corp_id"`
	Uid      uint64 `json:"uid"`
	StaffId  uint64 `json:"staff_id"`
	Version  int    `json:"version"`
	IsAssign bool   `json:"is_assign"`
}

type SendMsgStream struct {
	CorpId   uint32 `json:"corp_id"`
	Uid      uint64 `json:"uid"`
	ChatType uint32 `json:"chat_type"`
}

type MsgIntervalStream struct {
	CorpId   uint32 `json:"corp_id"`
	RobotUid uint64 `json:"robot_uid"`
	Interval int64  `json:"interval"`
}

type ExtUidZeroStream struct {
	CorpId    uint32 `json:"corp_id"`
	AccountId uint64 `json:"account_id"`
}

type SendFailStream struct {
	CorpId    uint32 `json:"corp_id"`
	RobotUid  uint64 `json:"robot_uid"`
	ExtChatId uint64 `json:"ext_chat_id"`
	ErrCode   uint32 `json:"err_code"`
}

type SendDelayStream struct {
	CorpId   uint32 `json:"corp_id"`
	RobotUid uint64 `json:"robot_uid"`
	Delay    uint32 `json:"delay"`
}

func initLogStream() error {
	var err error
	LogStream, err = logstream.InitLogStream(topicType)
	if err != nil {
		log.Errorf("err:%+v", err)
		return err
	}

	return nil
}

const (
	RobotUidMsgId = "qmsg_robot_uid_msg_id_interval"
)

func logInterval(ctx *rpc.Context, corpId uint32, robotUid uint64, msgId, method string) error {
	return nil
	key := fmt.Sprintf("%d_%s", robotUid, msgId)
	now := time.Now().UnixNano() / 1e6

	var analyze qmsg.UidMsgIdAnalyze
	err := s.RedisGroup.HGetPb(RobotUidMsgId, key, &analyze)
	if err != nil {
		if err == redis.Nil {
			analyze.CreatedAt = now
			analyze.Method = method
			err = s.RedisGroup.HSetPb(RobotUidMsgId, key, &analyze, time.Hour*24)
			if err != nil {
				log.Errorf("err:%v", err)
				return err
			}

			return nil
		}
		log.Errorf("err:%v", err)
		return err
	}

	LogStream.LogStreamSubType(ctx, MsgIntervalSubType, &MsgIntervalStream{
		CorpId:   corpId,
		RobotUid: robotUid,
		Interval: now - analyze.CreatedAt,
	})

	_, err = s.RedisGroup.HDel(RobotUidMsgId, key)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	return nil
}
