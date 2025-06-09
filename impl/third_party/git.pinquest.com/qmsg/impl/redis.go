package impl

import (
	"fmt"
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/extrpkg/github.com/go-redis/redis"
	"strconv"
	"time"
)

func incAndSetExpire(key string, val int64) {
	_, err := s.RedisGroup.IncrBy(key, val)
	if err != nil {
		log.Errorf("err:%v", err)
		return
	}
	err = s.RedisGroup.Expire(key, time.Hour*48)
	if err != nil {
		log.Errorf("err:%v", err)
	}
}

func hSetAndExpire(key, subKey string, val interface{}) {
	err := s.RedisGroup.HSet(key, subKey, val)
	if err != nil {
		log.Errorf("err:%v", err)
		return
	}
	err = s.RedisGroup.Expire(key, time.Hour*48)
	if err != nil {
		log.Errorf("err:%v", err)
	}
}

func addRedisSetAndExpire(key string, val interface{}) {
	err := s.RedisGroup.SAdd(key, val)
	if err != nil {
		log.Errorf("err:%v", err)
		return
	}
	err = s.RedisGroup.Expire(key, time.Hour*48)
	if err != nil {
		log.Errorf("err:%v", err)
	}
}

func getRedisIncVal(key string) uint32 {
	incByte, err := s.RedisGroup.Get(key)
	if err != nil {
		if err != redis.Nil {
			log.Errorf("err:%v", err)
		}
		return 0
	}
	//_ = s.RedisGroup.Del(key)
	incVal, err := strconv.ParseUint(string(incByte), 10, 64)
	if err != nil {
		log.Errorf("err:%v", err)
		return 0
	}
	return uint32(incVal)
}

func getRedisSetCount(key string) uint32 {
	count, err := s.RedisGroup.SCard(key)
	if err != nil {
		log.Errorf("err:%v", err)
		return 0
	}
	//_ = s.RedisGroup.Del(key)
	return uint32(count)
}

type StatRedisKey struct {
	corpId               uint32
	robotUid             uint64
	startAt, endAt, date uint32
}

func (p *StatRedisKey) getChatCountKey(customerServiceUid uint64) string {
	if customerServiceUid != 0 {
		return fmt.Sprintf("qmsg_chat_count_%d_%d_%d_%d_%d", p.corpId, customerServiceUid, p.date, p.startAt, p.endAt)
	}
	return fmt.Sprintf("qmsg_robot_chat_count_%d_%d_%d_%d_%d", p.corpId, p.robotUid, p.date, p.startAt, p.endAt)
}

// 某个chat是否可以展示无需回复按钮
func (p *StatRedisKey) getEnableDoNoRequiredReplyKey(chatId uint64) string {
	return fmt.Sprintf("qmsg_enable_do_no_required_reply_%d_%d_%d_%d_%d", p.corpId, p.date, p.startAt, p.endAt, chatId)
}

func (p *StatRedisKey) getNoRequiredReplyIngCacheKey(chatId uint64, cache string) string {
	return fmt.Sprintf("qmsg_set_chat_not_reply_%d_%d_%d_%d_%d_%s", p.corpId, p.date, p.startAt, p.endAt, chatId, cache)
}

func (p *StatRedisKey) getReceiveMsgCountKey(customerServiceUid uint64) string {
	if customerServiceUid != 0 {
		return fmt.Sprintf("qmsg_receive_msg_count_%d_%d_%d_%d_%d", p.corpId, customerServiceUid, p.date, p.startAt, p.endAt)
	}
	return fmt.Sprintf("qmsg_robot_receive_msg_count_%d_%d_%d_%d_%d", p.corpId, p.robotUid, p.date, p.startAt, p.endAt)
}

func (p *StatRedisKey) getSendMsgCountKey(customerServiceUid uint64) string {
	if customerServiceUid != 0 {
		return fmt.Sprintf("qmsg_send_msg_count_%d_%d_%d_%d_%d", p.corpId, customerServiceUid, p.date, p.startAt, p.endAt)
	}
	return fmt.Sprintf("qmsg_robot_send_msg_count_%d_%d_%d_%d_%d", p.corpId, p.robotUid, p.date, p.startAt, p.endAt)
}

func (p *StatRedisKey) getNoReplyMsgCountKey(customerServiceUid uint64) string {
	if customerServiceUid != 0 {
		return fmt.Sprintf("qmsg_no_reply_msg_count_%d_%d_%d_%d_%d", p.corpId, customerServiceUid, p.date, p.startAt, p.endAt)
	}
	return fmt.Sprintf("qmsg_robot_no_reply_msg_count_%d_%d_%d_%d_%d", p.corpId, p.robotUid, p.date, p.startAt, p.endAt)
}

func (p *StatRedisKey) getHasReplayChatKey(customerServiceUid uint64) string {
	if customerServiceUid != 0 {
		return fmt.Sprintf("qmsg_has_reply_chat_%d_%d_%d_%d_%d", p.corpId, customerServiceUid, p.date, p.startAt, p.endAt)
	}
	return fmt.Sprintf("qmsg_robot_has_reply_chat_%d_%d_%d_%d_%d", p.corpId, p.robotUid, p.date, p.startAt, p.endAt)
}

func (p *StatRedisKey) getFirstReplyTotalTimeKey(customerServiceUid uint64) string {
	if customerServiceUid != 0 {
		return fmt.Sprintf("qmsg_frist_reply_total_time_%d_%d_%d_%d_%d", p.corpId, customerServiceUid, p.date, p.startAt, p.endAt)
	}
	return fmt.Sprintf("qmsg_robot_frist_reply_total_time_%d_%d_%d_%d_%d", p.corpId, p.robotUid, p.date, p.startAt, p.endAt)
}

func (p *StatRedisKey) getFirstReplyTotalCountKey(customerServiceUid uint64) string {
	if customerServiceUid != 0 {
		return fmt.Sprintf("qmsg_frist_reply_total_count_%d_%d_%d_%d_%d", p.corpId, customerServiceUid, p.date, p.startAt, p.endAt)
	}
	return fmt.Sprintf("qmsg_robot_frist_reply_total_count_%d_%d_%d_%d_%d", p.corpId, p.robotUid, p.date, p.startAt, p.endAt)
}

// 首次响应及时次数
func (p *StatRedisKey) getFirstInTimeReplyCountKey(customerServiceUid uint64) string {
	if customerServiceUid != 0 {
		return fmt.Sprintf("qmsg_first_in_time_reply_count_%d_%d_%d_%d_%d", p.corpId, customerServiceUid, p.date, p.startAt, p.endAt)
	}
	return fmt.Sprintf("qmsg_robot_first_in_time_reply_count_%d_%d_%d_%d_%d", p.corpId, p.robotUid, p.date, p.startAt, p.endAt)
}

// 超时次数
func (p *StatRedisKey) getTimeoutChatKey(customerServiceUid uint64) string {
	if customerServiceUid != 0 {
		return fmt.Sprintf("qmsg_timeout_chat_%d_%d_%d_%d_%d", p.corpId, customerServiceUid, p.date, p.startAt, p.endAt)
	}
	return fmt.Sprintf("qmsg_robot_timeout_chat_%d_%d_%d_%d_%d", p.corpId, p.robotUid, p.date, p.startAt, p.endAt)
}

// 首次响应超时次数
func (p *StatRedisKey) getFirstTimeoutChatKey(customerServiceUid uint64) string {
	if customerServiceUid != 0 {
		return fmt.Sprintf("qmsg_first_timeout_chat_%d_%d_%d_%d_%d", p.corpId, customerServiceUid, p.date, p.startAt, p.endAt)
	}
	return fmt.Sprintf("qmsg_robot_first_timeout_chat_%d_%d_%d_%d_%d", p.corpId, p.robotUid, p.date, p.startAt, p.endAt)
}

// 收到客户最后一条消息对应的qmsg.ModelMsgBox 的id
func (p *StatRedisKey) getLastMsgBoxKey() string {
	return fmt.Sprintf("qmsg_last_msg_box_%d_%d_%d_%d", p.corpId, p.date, p.startAt, p.endAt)
}

// 轮次
func (p *StatRedisKey) getTurnKey(customerServiceUid uint64) string {
	if customerServiceUid != 0 {
		return fmt.Sprintf("qmsg_turn_%d_%d_%d_%d_%d", p.corpId, customerServiceUid, p.date, p.startAt, p.endAt)
	}
	return fmt.Sprintf("qmsg_robot_turn_%d_%d_%d_%d_%d", p.corpId, p.robotUid, p.date, p.startAt, p.endAt)
}

// 对应msgbox已计算轮次的
func (p *StatRedisKey) getCalTurnedKey(msgBoxId string) string {
	return fmt.Sprintf("qmsg_cal_turn_%d_%s", p.corpId, msgBoxId)
}
