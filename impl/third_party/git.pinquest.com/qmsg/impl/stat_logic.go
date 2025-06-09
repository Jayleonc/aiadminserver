package impl

import (
	"errors"
	"fmt"
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/brick/utils"
	"git.pinquest.cn/qlb/extrpkg/github.com/go-redis/redis"
	"git.pinquest.cn/qlb/qmsg"
	"strconv"
	"time"
)

var NotFoundSuitableStartAtAndEndAt = errors.New("NotFoundSuitableStartAtAndEndAt")

type StatHandler struct {
	CorpId, AppId uint32
	MsgTime       uint32
	//机器人uid
	RobotUid uint64
	//客户的 qrobot_account.id
	CustomerAccountId uint64
	startAt, endAt    uint32
	//qmsg.ModelChat.id 会话id
	ChatId uint64
	*StatRedisKey
	StatRule *qmsg.ModelMsgQualityStatRule
}

// CheckIsMatchStatRule 是否符合统计规则 包括时间段、机器人的校验
func (p *StatHandler) CheckIsMatchStatRule(ctx *rpc.Context) (matchStatRule bool, err error) {
	p.StatRule, err = getMsgQualityStatRule(ctx, p.CorpId, p.AppId)
	if err != nil {
		if MsgQualityStatRule.IsNotFoundErr(err) {
			log.Warn("rule not found")
			err = nil
			return
		}
		log.Errorf("err:%v", err)
		return
	}
	if p.StatRule.EnableState != uint32(qmsg.ModelMsgQualityStatRule_EnableStateOpen) || p.StatRule.Detail == nil {
		return
	}
	if !p.checkRobotIsInRule(p.StatRule) {
		log.Warn("robot not match")
		return
	}

	p.startAt, p.endAt, err = p.getSuitableStatAtAndEndAt(p.StatRule)
	if err == NotFoundSuitableStartAtAndEndAt {
		log.Warn("time not match")
		err = nil
		return
	}
	matchStatRule = true
	return
}

// 是否开启无需回复设置
func (p *StatHandler) GetEnableNoRequiredReply() bool {
	if p.StatRule == nil || p.StatRule.Detail == nil {
		return false
	}
	return p.StatRule.Detail.NoRequiredReply
}

// 缓存无需回复最开始的msgBoxId
func (p *StatHandler) AddEnableDoNoRequiredReplyCache(msgBoxId uint64) {
	err := s.RedisGroup.SetEx(p.getEnableDoNoRequiredReplyKey(p.ChatId), msgBoxId, time.Hour*48)
	if err != nil {
		log.Errorf("err:%v", err)
	}
}

// 获取缓存的无需回复最开始的msgBoxId
func (p *StatHandler) GetEnableDoNoRequiredReplyCache() (string, error) {
	b, err := s.RedisGroup.Get(p.getEnableDoNoRequiredReplyKey(p.ChatId))
	if err != nil {
		if err != redis.Nil {
			log.Errorf("err:%v", err)
			return "", err
		}
		return "", nil
	}
	return string(b), nil
}

// 清理缓存的无需回复最开始的msgBoxId
func (p *StatHandler) CleanEnableDoNoRequiredReplyCache(msgBoxIdStr string) {
	cache, err := p.GetEnableDoNoRequiredReplyCache()
	if err != nil {
		log.Errorf("err:%v", err)
		return
	}
	if cache == msgBoxIdStr {
		err = s.RedisGroup.Del(p.getEnableDoNoRequiredReplyKey(p.ChatId))
		if err != nil {
			log.Errorf("err:%v", err)
		}
	}
}

// 是否有缓存的无需回复最开始的msgBoxId
func (p *StatHandler) GetEnableDoNoRequiredReply() bool {
	cache, err := p.GetEnableDoNoRequiredReplyCache()
	if err != nil {
		log.Errorf("err:%v", err)
		return false
	}
	return cache != ""
}

// 添加正在处理无需回复操作缓存
func (p *StatHandler) AddNoRequiredReplyIngCache() (bool, error) {
	cache, err := p.GetEnableDoNoRequiredReplyCache()
	if err != nil {
		log.Errorf("err:%v", err)
		return false, err
	}
	if cache == "" {
		return false, nil
	}
	ok, err := s.RedisGroup.SetNX(p.getNoRequiredReplyIngCacheKey(p.ChatId, cache), []byte(""), time.Second*10)
	if err != nil {
		log.Errorf("err:%v", err)
		return false, err
	}
	return ok, nil
}

func (p *StatHandler) Init(ctx *rpc.Context) error {
	p.StatRedisKey = &StatRedisKey{
		corpId:   p.CorpId,
		robotUid: p.RobotUid,
		startAt:  p.startAt,
		endAt:    p.endAt,
		date:     parseTime(utils.TimeNowCompactYmd()),
	}
	return nil
}

// 获取合适的统计时间段
func (p *StatHandler) getSuitableStatAtAndEndAt(statRule *qmsg.ModelMsgQualityStatRule) (startAt, endAt uint32, err error) {
	//全天都统计
	if statRule.Detail.StatTimeScopeType == uint32(qmsg.StatTimeScopeType_StatTimeScopeTypeAll) {
		return
	}
	week := uint32(getHumanWeek())
	var hasFound bool
	beginTimeStamp := utils.BeginTimeStampOfDate(time.Now())
	for _, item := range statRule.Detail.TimeScopeList {
		if !utils.InSliceUint32(week, item.WeekList) {
			continue
		}
		start := uint32(beginTimeStamp) + item.StartAt*60
		end := uint32(beginTimeStamp) + item.EndAt*60
		if p.MsgTime >= start && p.MsgTime <= end {
			hasFound = true
			startAt = item.StartAt
			endAt = item.EndAt
			break
		}
	}
	if !hasFound {
		err = NotFoundSuitableStartAtAndEndAt
		log.Warn("not match stat time")
	}
	return
}

// 获取一个单聊的唯一id
func (p *StatHandler) getChatKey() string {
	return fmt.Sprintf("%d", p.ChatId)
}

func (p *StatHandler) checkRobotIsInRule(statRule *qmsg.ModelMsgQualityStatRule) bool {
	if statRule.Detail.StatUserScopeType == uint32(qmsg.ModelMsgQualityStatRule_StatUserScopeTypeAll) {
		return true
	}
	return utils.InSliceUint64(p.RobotUid, statRule.Detail.UidList)
}

func (p *StatHandler) getMsgCacheAttrs(customerServiceId uint64) map[string]interface{} {
	return map[string]interface{}{
		DbCorpId:            p.CorpId,
		DbRobotUid:          p.RobotUid,
		DbCustomerServiceId: customerServiceId,
		DbDate:              p.date,
		DbStartAt:           p.startAt,
		DbEndAt:             p.endAt,
		DbCustomerAccountId: p.CustomerAccountId,
	}
}

// IgnoreTimeout 部分会话关闭时不应该计算超时
func (p *StatHandler) IgnoreTimeout(ctx *rpc.Context, customerServiceId uint64) (err error) {
	_, err = MsgStatMsgCache.Where(p.getMsgCacheAttrs(customerServiceId)).Where(DbHasCalTimeout, false).Update(ctx, map[string]interface{}{
		DbHasCalTimeout: true,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	return nil
}

// GetLastNotRepliedMsgBoxId 获取某个会话还没有被成员回复的客户发送的消息id
func (p *StatHandler) GetLastNotRepliedMsgBoxId() (uint64, error) {
	val, err := s.RedisGroup.HGet(p.getLastMsgBoxKey(), p.getChatKey())
	if err != nil {
		if err != redis.Nil {
			log.Errorf("err:%v", err)
			return 0, err
		}
		return 0, nil
	} else {
		msgBoxId, err := strconv.ParseUint(val, 10, 64)
		if err != nil {
			log.Errorf("err:%v", err)
			return 0, err
		}
		calTurnedKey := p.StatRedisKey.getCalTurnedKey(val)
		b, err := s.RedisGroup.SIsMember(calTurnedKey, fmt.Sprintf("%d", 0))
		if err != nil {
			log.Errorf("err:%v", err)
			return 0, err
		}
		//已回复过了
		if b {
			return 0, nil
		}
		return msgBoxId, nil
	}
}

func (p *StatHandler) CreateMsgCache(ctx *rpc.Context, customerServiceId, msgBoxId uint64, isAssignMode bool) (err error) {
	attrs := p.getMsgCacheAttrs(customerServiceId)
	var cache *qmsg.ModelMsgStatMsgCache
	err = MsgStatMsgCache.Where(attrs).First(ctx, &cache)
	if err != nil {
		//首次
		if MsgStatMsgCache.IsNotFoundErr(err) {
			attrs[DbMsgBoxId] = msgBoxId
			attrs[DbMsgTime] = p.MsgTime
			attrs[DbAtLatestReplyTime] = p.MsgTime + getSecondsFromResponseTime(p.StatRule.Detail.ReplyTime)
			attrs[DbTimeoutTime] = p.MsgTime + getSecondsFromResponseTime(p.StatRule.Detail.FirstReplyOverTime)
			attrs[DbIsAssignMode] = isAssignMode
			err = MsgStatMsgCache.Create(ctx, attrs)
			if err != nil {
				log.Errorf("err:%v", err)
				return err
			}
		} else {
			log.Errorf("err:%v", err)
			return err
		}
	} else {
		//非首次
		if cache.HasCalTimeout { //已经计算了超时，则证明进入了下一轮
			_, err = MsgStatMsgCache.WhereCorp(p.CorpId).Where(DbId, cache.Id).Update(ctx, map[string]interface{}{
				DbMsgTime:       p.MsgTime,
				DbTimeoutTime:   p.MsgTime + getSecondsFromResponseTime(p.StatRule.Detail.LateReplyOverTime),
				DbHasCalTimeout: false,
				DbMsgBoxId:      msgBoxId,
				DbIsAssignMode:  isAssignMode,
			})
			if err != nil {
				log.Errorf("err:%v", err)
				return err
			}
		}
	}
	return nil
}

func (p *StatHandler) getMsgCache(ctx *rpc.Context, customerServiceId uint64) (*qmsg.ModelMsgStatMsgCache, error) {
	attrs := p.getMsgCacheAttrs(customerServiceId)
	var cache *qmsg.ModelMsgStatMsgCache
	err := MsgStatMsgCache.NewScope().Where(attrs).First(ctx, &cache)
	if err != nil {
		if !MsgStatMsgCache.IsNotFoundErr(err) {
			log.Errorf("err:%v", err)
		}
		return nil, err
	}
	return cache, nil
}

// ReceiveStatHandler 机器人接收到客户消息后处理器
type ReceiveStatHandler struct {
	customerServiceUidList []uint64
	// 客服账号是否是分配模式
	isAssignMode bool
	msgBoxId     uint64
	*StatHandler
}

func newReceiveStatHandler(msgBox *qmsg.ModelMsgBox, chatId uint64) *ReceiveStatHandler {
	return &ReceiveStatHandler{
		msgBoxId: msgBox.Id,
		StatHandler: &StatHandler{
			CorpId:            msgBox.CorpId,
			AppId:             msgBox.AppId,
			MsgTime:           msgBox.SentAt,
			RobotUid:          msgBox.Uid,
			CustomerAccountId: msgBox.ChatId,
			ChatId:            chatId,
		},
	}
}

func (p *ReceiveStatHandler) Init(ctx *rpc.Context) (err error) {
	//客服列表
	p.customerServiceUidList, p.isAssignMode, err = getRelatedCustomerServiceUidList(ctx, p.CorpId, p.AppId, p.RobotUid, p.ChatId)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	err = p.StatHandler.Init(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	return nil
}

func (p *ReceiveStatHandler) Run(ctx *rpc.Context) (err error) {
	//msgbox标记
	hSetAndExpire(p.getLastMsgBoxKey(), p.getChatKey(), p.msgBoxId)

	//设置客户未被回复的首条消息发送时间用于计算首响
	//只有分配模式下才用收到客户的消息时间，非分配模式使用客服收到会话的时间
	err = p.CreateMsgCache(ctx, 0, p.msgBoxId, p.isAssignMode)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	for _, customerServiceUid := range p.customerServiceUidList {
		err = p.CreateMsgCache(ctx, customerServiceUid, p.msgBoxId, p.isAssignMode)
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
	}

	p.chatCount()
	p.receiveMsgCount()
	// 开启了无需回复设置
	if p.GetEnableNoRequiredReply() {
		p.AddEnableDoNoRequiredReplyCache(p.msgBoxId)
	}

	return nil
}

// 总单聊数
func (p *ReceiveStatHandler) chatCount() {
	//机器人维度
	addRedisSetAndExpire(p.StatRedisKey.getChatCountKey(0), p.getChatKey())

	//客服维度
	for _, customerServiceUid := range p.customerServiceUidList {
		addRedisSetAndExpire(p.StatRedisKey.getChatCountKey(customerServiceUid), p.getChatKey())
	}
}

// 总接收消息数
func (p *ReceiveStatHandler) receiveMsgCount() {
	//机器人维度
	incAndSetExpire(p.StatRedisKey.getReceiveMsgCountKey(0), 1)

	//客服维度
	for _, customerServiceUid := range p.customerServiceUidList {
		incAndSetExpire(p.StatRedisKey.getReceiveMsgCountKey(customerServiceUid), 1)
	}
}

// SendStatHandler 客服操作机器人发送消息给客户后处理器
type SendStatHandler struct {
	customerServiceUid uint64
	*StatHandler
}

func newSendStatHandler(chat *qmsg.ModelChat, msgTime uint32, customerServiceUid uint64) *SendStatHandler {
	return &SendStatHandler{
		customerServiceUid: customerServiceUid,
		StatHandler: &StatHandler{
			CorpId:            chat.CorpId,
			AppId:             chat.AppId,
			MsgTime:           msgTime,
			RobotUid:          chat.RobotUid,
			CustomerAccountId: chat.ChatExtId,
			ChatId:            chat.Id,
		},
	}
}

func (p *SendStatHandler) Run(ctx *rpc.Context, isSetNotReplyMsg bool) (err error) {
	// 无需回复不算发送消息
	if isSetNotReplyMsg {
		err = p.setNoReplyMsgCount()
	} else {
		err = p.sendMsgCount()
	}
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	err = p.calReply(ctx, isSetNotReplyMsg)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	return nil
}

// 无需回复次数
func (p *SendStatHandler) setNoReplyMsgCount() error {
	//机器人维度
	incAndSetExpire(p.StatRedisKey.getNoReplyMsgCountKey(0), 1)
	//客服维度
	incAndSetExpire(p.StatRedisKey.getNoReplyMsgCountKey(p.customerServiceUid), 1)
	return nil
}

// 总发送消息数
func (p *SendStatHandler) sendMsgCount() error {
	//机器人维度
	incAndSetExpire(p.StatRedisKey.getSendMsgCountKey(0), 1)
	//客服维度
	incAndSetExpire(p.StatRedisKey.getSendMsgCountKey(p.customerServiceUid), 1)
	return nil
}

// 响应相关
func (p *SendStatHandler) calReply(ctx *rpc.Context, isSetNotReplyMsg bool) error {
	//轮次逻辑
	msgBoxId, err := s.RedisGroup.HGet(p.getLastMsgBoxKey(), p.getChatKey())
	if err != nil {
		if err != redis.Nil {
			log.Errorf("err:%v", err)
			return err
		}
	} else {
		// 无需回复不算入已回复
		if !isSetNotReplyMsg {
			//单聊数
			addRedisSetAndExpire(p.StatRedisKey.getChatCountKey(0), p.getChatKey())
			addRedisSetAndExpire(p.StatRedisKey.getChatCountKey(p.customerServiceUid), p.getChatKey())
			//已回复单聊数机器人维度
			addRedisSetAndExpire(p.StatRedisKey.getHasReplayChatKey(0), p.getChatKey())
			//已回复单聊数客服维度
			addRedisSetAndExpire(p.StatRedisKey.getHasReplayChatKey(p.customerServiceUid), p.getChatKey())
		}
		// 开启了无需回复设置，回复时需要清掉缓存
		if p.GetEnableNoRequiredReply() {
			p.CleanEnableDoNoRequiredReplyCache(msgBoxId)
		}

		calTurnedKey := p.StatRedisKey.getCalTurnedKey(msgBoxId)
		calTurnedArr, err := s.RedisGroup.SMembers(calTurnedKey)
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
		for _, sender := range []uint64{0, p.customerServiceUid} {
			senderStr := fmt.Sprintf("%d", sender)
			//没有计算过
			if !utils.InSliceStr(senderStr, calTurnedArr) {
				//标记已计算轮次
				addRedisSetAndExpire(calTurnedKey, senderStr)
				//新增轮次
				incAndSetExpire(p.StatRedisKey.getTurnKey(sender), 1)
			}
		}
	}

	//
	var msgCacheList []*qmsg.ModelMsgStatMsgCache
	for _, sender := range []uint64{0, p.customerServiceUid} {
		msgCache, err := p.getMsgCache(ctx, sender)
		if err != nil {
			if !MsgStatMsgCache.IsNotFoundErr(err) {
				log.Errorf("err:%v", err)
				return err
			}
		} else {
			msgCacheList = append(msgCacheList, msgCache)
		}
	}

	var calFirstReplyCacheIdList []uint64
	var calTimeoutCacheIdList []uint64
	var assignModeMsgBoxId uint64
	for _, item := range msgCacheList {
		//这种是客服先发起聊天
		if p.MsgTime < item.MsgTime {
			continue
		}
		//响应时间
		replyTime := p.MsgTime - item.MsgTime
		//没有计算过首响
		if !item.HasCalFirstReply {
			//首响时长
			incAndSetExpire(p.StatRedisKey.getFirstReplyTotalTimeKey(item.CustomerServiceId), int64(replyTime))
			incAndSetExpire(p.StatRedisKey.getFirstReplyTotalCountKey(item.CustomerServiceId), 1)
			//及时响应
			if item.AtLatestReplyTime >= p.MsgTime {
				//首次响应及时次数
				incAndSetExpire(p.StatRedisKey.getFirstInTimeReplyCountKey(item.CustomerServiceId), 1)
			}
			calFirstReplyCacheIdList = append(calFirstReplyCacheIdList, item.Id)
		}
		//超时了
		if item.TimeoutTime < p.MsgTime && !item.HasCalTimeout {
			err = afterTimeout(ctx, item, p.StatRedisKey)
			if err != nil {
				log.Errorf("err:%v", err)
				return err
			}
		} else {
			// 将有资产模式的标记为未超时
			if item.CustomerServiceId == 0 && item.IsAssignMode {
				assignModeMsgBoxId = item.MsgBoxId
			}
		}
		if !item.HasCalTimeout {
			calTimeoutCacheIdList = append(calTimeoutCacheIdList, item.Id)
		}
	}

	if assignModeMsgBoxId > 0 {
		err = MsgStatMsgCache.setAssignModelCalTimeout(ctx, p.CorpId, p.RobotUid, p.CustomerAccountId, assignModeMsgBoxId)
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
	}

	if len(calFirstReplyCacheIdList) > 0 {
		_, err = MsgStatMsgCache.WhereCorp(p.CorpId).WhereIn(DbId, calFirstReplyCacheIdList).Where(DbHasCalFirstReply, false).Update(ctx, map[string]interface{}{
			DbHasCalFirstReply: true,
		})
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
	}
	if len(calTimeoutCacheIdList) > 0 {
		err = MsgStatMsgCache.setCalTimeout(ctx, p.CorpId, calTimeoutCacheIdList)
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
	}
	return nil
}

func afterTimeout(ctx *rpc.Context, item *qmsg.ModelMsgStatMsgCache, statRedisKey *StatRedisKey) (err error) {
	//没有计算过首次超时
	if !item.HasCalFirstTimeout {
		//首次响应超时次数
		incAndSetExpire(statRedisKey.getFirstTimeoutChatKey(item.CustomerServiceId), 1)
	}
	//超时次数
	if !item.HasCalTimeout {
		incAndSetExpire(statRedisKey.getTimeoutChatKey(item.CustomerServiceId), 1)
	}
	//只有成员维度的超时才记录到超时消息中
	if item.CustomerServiceId == 0 {
		_, err = MsgStatTimeOutDetail.FirstOrCreate(ctx, map[string]interface{}{
			DbCorpId:            item.CorpId,
			DbMsgBoxId:          item.MsgBoxId,
			DbRobotUid:          item.RobotUid,
			DbCustomerAccountId: item.CustomerAccountId,
		}, nil, nil)
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
	}
	return nil
}

func HandleTimeoutByStatRule(ctx *rpc.Context, statRule *qmsg.ModelMsgQualityStatRule, date uint32) error {
	for {
		var list []qmsg.ModelMsgStatMsgCache
		err := MsgStatMsgCache.WhereCorp(statRule.CorpId).Where(DbDate, date).Where(DbHasCalTimeout, false).SetLimit(500).Find(ctx, &list)
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
		if len(list) == 0 {
			break
		}

		var calTimeoutCacheIdList []uint64
		for _, item := range list {
			statRedisKey := &StatRedisKey{
				corpId:   item.CorpId,
				robotUid: item.RobotUid,
				startAt:  item.StartAt,
				endAt:    item.EndAt,
				date:     item.Date,
			}
			err = afterTimeout(ctx, &item, statRedisKey)
			if err != nil {
				log.Errorf("err:%v", err)
				return err
			}
			calTimeoutCacheIdList = append(calTimeoutCacheIdList, item.Id)
		}
		if len(calTimeoutCacheIdList) > 0 {
			err = MsgStatMsgCache.setCalTimeout(ctx, statRule.CorpId, calTimeoutCacheIdList)
			if err != nil {
				log.Errorf("err:%v", err)
				return err
			}
		}
	}
	return nil
}
