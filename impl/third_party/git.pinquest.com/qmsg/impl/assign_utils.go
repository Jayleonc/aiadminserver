package impl

import (
	"container/ring"
	"fmt"
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/brick/utils"
	"git.pinquest.cn/qlb/extrpkg/github.com/go-redis/redis"
	"git.pinquest.cn/qlb/qmsg"
	"math/rand"
	"sync"
	"time"
)

type assignCount struct {
	Uid uint64 `json:"uid"`
	Cnt uint32 `json:"cnt"`
}

// 判断会话是否需要分配
func needAssign(ctx *rpc.Context, msg *qmsg.ModelMsgBox, chat *qmsg.ModelChat) (*qmsg.ModelAssignChat, bool, error) {
	if chat == nil {
		return nil, false, nil
	}

	{ //判断资产情况
		var userRobot qmsg.ModelUserRobot
		err := UserRobot.WhereCorpApp(msg.CorpId, msg.AppId).Where(DbRobotUid, chat.RobotUid).First(ctx, &userRobot)
		if err != nil && !UserRobot.IsNotFoundErr(err) {
			log.Errorf("err:%v", err)
			return nil, false, err
		}
		//有资产
		if err == nil {
			return nil, false, nil
		}

		//非资产，判断是否已分配
		assignChat, err := AssignChat.getByCid(ctx, msg.CorpId, msg.AppId, chat.Id)
		if err != nil && !AssignChat.IsNotFoundErr(err) {
			log.Errorf("err:%v", err)
			return nil, false, err
		}
		//非资产，已分配
		if err == nil {
			return assignChat, false, nil
		}
	}

	if msg.MsgType != uint32(qmsg.MsgType_MsgTypeChat) {
		return nil, false, nil
	}

	session, err := getSessionConfigFromDb(ctx, msg.CorpId, msg.AppId)
	if err != nil {
		log.Errorf("err %v", err)
		return nil, false, err
	}

	//未设置会话配置会直接走到这里
	var isAssign bool
	if session.AssignSetting.IsBlockRobotMsg {
		if chat.UnreadCount > 0 {
			isAssign = true
		}
	} else {
		isAssign = true
	}

	return nil, isAssign, nil
}

/*
NOTE：以下轮询策略是本地内存内的轮询，所以在多服务的情况下，只能体现出相对的均衡。不是绝对均衡。
*/
var roundMgr = NewRoundRobin()

type RoundRobin struct {
	ringMap map[uint32]*RingBuffer
	initMap *sync.Map
	rwMutex *sync.RWMutex
}

type RingBuffer struct {
	mu         *sync.Mutex
	cursor     *ring.Ring
	expireTime uint32
}

func (b *RingBuffer) Pop() *ring.Ring {
	b.mu.Lock()
	defer b.mu.Unlock()
	cur := b.cursor
	b.cursor = b.cursor.Prev()
	return cur
}

func (b *RingBuffer) Push(v *ring.Ring) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.cursor.Link(v)
	return
}

func (b *RingBuffer) Del(v *ring.Ring) {
	b.mu.Lock()
	defer b.mu.Unlock()

	p := v.Prev()
	movedRing := p.Unlink(1)
	if movedRing == b.cursor {
		b.cursor = p
	}
	return
}
func (b *RingBuffer) Len() int {
	return b.cursor.Len()
}
func NewRoundRobin() *RoundRobin {
	return &RoundRobin{
		rwMutex: &sync.RWMutex{},
		initMap: &sync.Map{},
		ringMap: map[uint32]*RingBuffer{},
	}
}
func (r *RoundRobin) SetRingBuffer(key uint32, v *RingBuffer) {
	r.rwMutex.Lock()
	defer r.rwMutex.Unlock()
	r.ringMap[key] = v
	//r.ringMap.Store(key, v)
}

func (r *RoundRobin) getRingBuffer(key uint32, isIgnoreExp bool) (*RingBuffer, bool) {
	r.rwMutex.RLock()
	defer r.rwMutex.RUnlock()
	buffer, ok := r.ringMap[key]
	if buffer == nil {
		return nil, false
	}
	// 检查过期
	if !isIgnoreExp && buffer.expireTime < uint32(time.Now().Unix()) {
		return nil, false
	}
	return buffer, ok
}

func (r *RoundRobin) GetCursor(key uint32) *ring.Ring {
	buffer, ok := r.getRingBuffer(key, false)
	if ok {
		return nil
	}
	return buffer.Pop()
}

func (r *RoundRobin) InitRingBuffer(ctx *rpc.Context, corpId, appId uint32, oldRingBuffer *RingBuffer) (*RingBuffer, error) {
	_, isLoaded := r.initMap.LoadOrStore(corpId, true)
	defer r.initMap.Delete(corpId)
	if isLoaded {
		for {
			_, ok := r.initMap.Load(corpId)
			if !ok {
				break
			}
			time.Sleep(time.Millisecond * 10)
		}
		ringBuffer, ok := r.getRingBuffer(corpId, false)
		if ok && ringBuffer != nil {
			return ringBuffer, nil
		}
	}
	var userList []*qmsg.ModelUser
	err := User.WhereCorpApp(corpId, appId).
		Where(DbIsAssign, false).
		Where(DbIsDisable, false).OrderAsc(DbId).
		//Where(DbIsOnline, true).
		Find(ctx, &userList)
	if err != nil {
		log.Errorf("err: %v", err)
		return nil, err
	}

	// 对比userlist是否有更新，有更新才会重置游标
	if oldRingBuffer != nil {
		var userIdList []uint64
		for _, user := range userList {
			userIdList = append(userIdList, user.Id)
		}
		var oldUserIdList []uint64
		queueLen := oldRingBuffer.Len()
		var n int
		for {
			if n >= queueLen {
				break
			}
			n++
			r := oldRingBuffer.Pop()
			oldUserIdList = append(oldUserIdList, r.Value.(uint64))
		}
		a, b := utils.DiffSliceV2(userIdList, oldUserIdList)
		if len(a.([]uint64)) == 0 && len(b.([]uint64)) == 0 {
			oldRingBuffer.expireTime = uint32(time.Now().Unix() + 60)
			r.SetRingBuffer(corpId, oldRingBuffer)
			return oldRingBuffer, nil
		}
	}

	log.Debugf("assign_round 企业 %d 找到客服数量： %d", corpId, len(userList))
	_len := len(userList)
	if _len == 0 {
		return nil, nil
	}
	var cursor *ring.Ring
	_ring := ring.New(_len)
	//TODO：这里是伪随机@ciming
	randInt := rand.Intn(_len)
	for i, user := range userList {
		log.Debugf("assign_round add uid: %d, name: %s", user.Id, user.Username)
		if i == 0 {
			_ring.Value = user.Id
		} else {
			_ring = _ring.Next()
			_ring.Value = user.Id
		}
		if i == randInt {
			cursor = _ring
		}
	}
	log.Debugf("assign_round cursor: %v", cursor.Value)
	buffer := &RingBuffer{
		mu:     &sync.Mutex{},
		cursor: cursor,
		// 数据有效期：1分钟
		expireTime: uint32(time.Now().Unix() + 60),
	}
	r.SetRingBuffer(corpId, buffer)
	return buffer, nil
}

func (r *RoundRobin) GetRingBuffer(ctx *rpc.Context, corpId, appId uint32) (*RingBuffer, error) {
	var err error
	ringBuffer, ok := r.getRingBuffer(corpId, true)
	if !ok || (ringBuffer != nil && ringBuffer.expireTime < uint32(time.Now().Unix())) {
		// init
		ringBuffer, err = r.InitRingBuffer(ctx, corpId, appId, ringBuffer)
		if err != nil {
			log.Errorf("err: %v", err)
			return nil, err
		}
		if ringBuffer == nil {
			log.Warnf("corp: %d not found any user", corpId)
			return &RingBuffer{}, nil
		}
	}
	return ringBuffer, nil
}

func (r *RoundRobin) DelRingBuffer(corpId uint32) {
	log.Warn("Session settings are changed and the polling policy is cancelled. del ringBuffer")
	r.rwMutex.Lock()
	defer r.rwMutex.Unlock()
	delete(r.ringMap, corpId)
	return
}

// AddRing 加入轮询
func (r *RoundRobin) AddRing(ctx *rpc.Context, corpId, appId uint32, uid uint64) {
	log.Debugf("assign_round readying add uid %d with corp: %d", uid, corpId)
	ringBuffer, err := r.GetRingBuffer(ctx, corpId, appId)
	if err != nil {
		log.Errorf("err: %v", err)
		return
	}
	var isExist bool
	cur := ringBuffer.cursor
	cur.Do(func(a interface{}) {
		thisUid, ok := a.(uint64)
		if ok && thisUid == uid {
			isExist = true
		}
	})
	if !isExist {
		log.Infof("ringBuffer add Ring with uid: %d", uid)
		ringBuffer.Push(&ring.Ring{Value: uid})
	}
	return
}

// RemoveRing 从轮询移除
func (r *RoundRobin) RemoveRing(ctx *rpc.Context, corpId, appId uint32, uid uint64) {
	log.Debugf("assign_round readying remove corp:%d, uid: %d", corpId, uid)
	ringBuffer, err := r.GetRingBuffer(ctx, corpId, appId)
	if err != nil {
		log.Errorf("err: %v", err)
		return
	}
	cursor := ringBuffer.cursor
	if cursor == nil {
		log.Error("cursor nil")
		return
	}

	curUid, ok := cursor.Value.(uint64)
	if !ok || curUid == uid {
		if ringBuffer.Len() == 1 {
			r.DelRingBuffer(corpId)
		} else {
			log.Warnf("assignUid value: %v, remove element from ringBuffer", curUid)
			ringBuffer.Del(cursor)
		}
		return
	}
	for p := cursor.Prev(); p != cursor; p = p.Prev() {
		thisUid, ok := p.Value.(uint64)
		if !ok || thisUid == uid {
			log.Warnf("assignUid value: %v, remove element from ringBuffer", thisUid)
			ringBuffer.Del(p)
			return
		}
	}
	return
}

// findAssignUserByRound 查询可分配会话客服(轮询模式)
func findAssignUserByRound(ctx *rpc.Context, corpId, appId uint32, robotUid, chatExtId uint64, isGroup bool, exceptUid uint64, sessionConfig *qmsg.ModelSessionConfig) (assignUid uint64, scene qmsg.ModelAssignChat_StartScene, err error) {
	assignSetting := sessionConfig.AssignSetting
	if assignSetting == nil {
		err = fmt.Errorf("assignSetting is nil")
		log.Error(err)
		return
	}

	ringBuffer, err := roundMgr.GetRingBuffer(ctx, corpId, appId)
	if err != nil {
		log.Errorf("err: %v", err)
		return
	}
	queueLen := ringBuffer.Len()
	if queueLen == 0 {
		scene = qmsg.ModelAssignChat_StartSceneNil
		log.Infof("无可分配客服，放到待分配队列")
		return
	}
	var ok bool
	var n int
	for {
		if n >= queueLen {
			// 轮询一圈，没找到合适客服，退出轮询
			log.Warnf("did not find the right customer service, quit polling")
			return 0, qmsg.ModelAssignChat_StartSceneNil, nil
		}
		n++
		r := ringBuffer.Pop()
		assignUid, ok = r.Value.(uint64)
		if !ok || assignUid == 0 {
			log.Warnf("assign_round assignUid value: %v, remove element from ringBuffer", r.Value)
			ringBuffer.Del(r)
			continue
		}
		log.Debugf("assign_round pop uid: %d", assignUid)
		if assignUid == exceptUid {
			log.Infof("assign_round except uid: %d", exceptUid)
			continue
		}
		if assignSetting.OnlyOnline {
			// 判断是否在线
			log.Debugf("assign_round 确认客服[%d]是否在线", assignUid)
			var user qmsg.ModelUser
			err = User.WhereCorpApp(corpId, appId).Where(DbIsAssign, false).
				Where(DbIsDisable, false).Where(DbIsOnline, true).Where(DbId, assignUid).First(ctx, &user)
			if err != nil {
				if rpc.GetErrCode(err) == qmsg.ErrUserNotFound {
					log.Warnf("客服[%d]已下线", assignUid)
					// 这里重置nil，避免后续被带回去上层
					err = nil
					continue
				}
				log.Errorf("err: %v", err)
				return
			}
		} else {
			log.Debugf("assign_round 配置全部客服")
		}

		// 判断客服的最大接入会话数
		var count uint32
		count, err = AssignChat.WhereCorpApp(corpId, appId).Where(DbUid, assignUid).Count(ctx)
		if err != nil {
			log.Errorf("err:%v", err)
			return
		}
		if count >= assignSetting.MaxChatNum {
			log.Warnf("assign_round reach maximum number of connections: %d >= %d", count, assignSetting.MaxChatNum)
			continue
		}
		scene = qmsg.ModelAssignChat_StartSceneSys
		log.Infof("assign_round found uid[%d]", assignUid)
		return assignUid, scene, nil
	}
}

// findAssignUserByPriority 优先级模式：在线客服>未达最大会话数客服>历史处理客服>会话数最少客服>其他客服
func findAssignUserByPriority(ctx *rpc.Context, corpId, appId uint32, robotUid, chatExtId uint64, isGroup bool, exceptUid uint64, sessionConfig *qmsg.ModelSessionConfig) (assignUid uint64, scene qmsg.ModelAssignChat_StartScene, err error) {
	// 只是用来输出调试内容
	var chat qmsg.ModelChat
	if rpc.Meta.Role == "test" {
		var mChat *qmsg.ModelChat
		mChat, err = Chat.getByRobotAndExt(ctx, corpId, appId, robotUid, chatExtId, isGroup)
		if err != nil {
			log.Errorf("err:%v", err)
			return
		}
		chat = *mChat
	}

	// 1、先找在线客服
	var userList []*qmsg.ModelUser
	db := User.WhereCorpApp(corpId, appId).Where(DbIsAssign, false).Where(DbIsDisable, false)
	if sessionConfig.AssignSetting.OnlyOnline {
		log.Debugf("%s 配置：需要找在线客服", chat.Msg)
		db.Where(DbIsOnline, true)
	} else {
		log.Debugf("%s 配置：需要全部客服", chat.Msg)
	}
	if exceptUid != 0 {
		db.Where(DbId, "!=", exceptUid)
	}
	err = db.Find(ctx, &userList)
	if err != nil {
		log.Errorf("err:%v", err)
		return
	}

	log.Debugf("%s 找到客服数量： %d", chat.Msg, len(userList))

	if len(userList) == 0 {
		scene = qmsg.ModelAssignChat_StartSceneNil
		log.Debugf("%s 无可分配客服，放到待分配队列", chat.Msg)
		return
	}

	userMap := map[uint64]*qmsg.ModelUser{}
	utils.KeyBy(userList, "Id", &userMap)
	uidList := utils.PluckUint64(userList, "Id")

	// 2、找达到最大接入会话数的客服
	var assignCountList []*assignCount
	err = AssignChat.WhereCorpApp(corpId, appId).WhereIn(DbUid, uidList).Select(DbUid, "count(*) as cnt").Group(fmt.Sprintf("%s HAVING count(*) >= %d", DbUid, sessionConfig.AssignSetting.MaxChatNum)).Find(ctx, &assignCountList)
	if err != nil {
		log.Errorf("err:%v", err)
		return
	}

	log.Debugf("%s 找到大于最大分配数 %d 的客服数量： %d", chat.Msg, sessionConfig.AssignSetting.MaxChatNum, len(assignCountList))

	maxUserMap := map[uint64]*assignCount{}
	utils.KeyBy(assignCountList, "Uid", &maxUserMap)
	var tmpUidList []uint64

	for _, uid := range uidList {
		if _, ok := maxUserMap[uid]; !ok {
			tmpUidList = append(tmpUidList, uid)
		}
	}

	uidList = tmpUidList
	log.Debugf("%s 找到小于最大分配数 %d 的客服数量： %d", chat.Msg, sessionConfig.AssignSetting.MaxChatNum, len(uidList))

	if len(uidList) == 0 {

		scene = qmsg.ModelAssignChat_StartSceneNil
		log.Debugf("%s 无可分配客服，放到待分配队列", chat.Msg)
		return
	}

	// 3、找历史处理客服
	var assignChatList []*qmsg.ModelAssignChat
	err = AssignChat.WithTrash().WhereCorpApp(corpId, appId).Where(DbRobotUid, robotUid).Where(DbChatExtId, chatExtId).Where(DbIsGroup, isGroup).WhereIn(DbUid, uidList).Group(DbUid).Find(ctx, &assignChatList)
	if err != nil {
		log.Errorf("err:%v", err)
		return
	}

	log.Debugf("%s 找到历史处理过的客服数量： %d", chat.Msg, len(assignChatList))

	if len(assignChatList) == 1 {
		assignUid = assignChatList[0].Uid
		scene = qmsg.ModelAssignChat_StartSceneSys

		log.Debugf("%s 找到一个历史处理过的客服 ID： %d", chat.Msg, assignChatList[0].Uid)
		return
	} else if len(assignChatList) > 1 {
		uidList = utils.PluckUint64(assignChatList, "Uid")
	}

	// 4、找会话数最少客服
	var lessUserChat []*assignCount
	err = AssignChat.WhereCorpApp(corpId, appId).WhereIn(DbUid, uidList).Group(DbUid).Select(DbUid, "COUNT(*) AS cnt").OrderAsc("cnt").Find(ctx, &lessUserChat)
	if err != nil {
		log.Errorf("err:%v", err)
		return
	}

	//没有任何分配记录时lessUserChat 为空，uidList有值，所有uid 都没有分配会话,直接抓一个
	if len(lessUserChat) == 0 {
		assignUid = uidList[0]
		scene = qmsg.ModelAssignChat_StartSceneSys
		return
	}

	lessUserMap := map[uint64]*assignCount{}
	utils.KeyBy(lessUserChat, "Uid", &lessUserMap)

	for _, uid := range uidList {
		if _, ok := lessUserMap[uid]; !ok {
			assignUid = uid
			scene = qmsg.ModelAssignChat_StartSceneSys
			log.Debugf("%s 找到没有会话的客服 ID： %d", chat.Msg, uid)
			return
		}
	}

	assignUid = lessUserChat[0].Uid
	scene = qmsg.ModelAssignChat_StartSceneSys

	log.Debugf("%s 找到会话数最少的客服 ID： %d", chat.Msg, lessUserChat[0].Uid)

	return
}

// 查询可分配会话客服
func findAssignUser(ctx *rpc.Context, corpId, appId uint32, robotUid, chatExtId uint64, isGroup bool, exceptUid uint64) (assignUid uint64, scene qmsg.ModelAssignChat_StartScene, err error) {
	sessionConfig, err := getSessionConfig(ctx, corpId, appId)
	if err != nil {
		log.Errorf("err:%v", err)
		return
	}
	switch sessionConfig.AssignSetting.AssignStrategy {
	case uint32(qmsg.ModelSessionConfig_AssignStrategyPriority):
		return findAssignUserByPriority(ctx, corpId, appId, robotUid, chatExtId, isGroup, exceptUid, sessionConfig)
	case uint32(qmsg.ModelSessionConfig_AssignStrategyRound):
		return findAssignUserByRound(ctx, corpId, appId, robotUid, chatExtId, isGroup, exceptUid, sessionConfig)
	default:
		return findAssignUserByPriority(ctx, corpId, appId, robotUid, chatExtId, isGroup, exceptUid, sessionConfig)
	}
}

func assignUser(ctx *rpc.Context, corpId, appId uint32, robotUid, chatExtId, assignUid uint64, isGroup bool, scene qmsg.ModelAssignChat_StartScene) (*qmsg.ModelAssignChat, error) {

	resultAssignChat, err := AssignChat.create(ctx, corpId, appId, assignUid, 0, robotUid, chatExtId, isGroup, nil, scene)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	if assignUid == 0 {
		return nil, nil
	}

	chat, err := Chat.get(ctx, corpId, appId, resultAssignChat.Cid)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	err = pushWsMsg(ctx, corpId, appId, resultAssignChat.Uid, []*qmsg.WsMsgWrapper{
		{
			MsgType:    uint32(qmsg.WsMsgWrapper_MsgTypeAssignChat),
			AssignChat: resultAssignChat,
			Chat:       chat,
		},
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	return resultAssignChat, nil
}

// 分配会话给刚登录的客服
func assignChatForLogin(ctx *rpc.Context, user *qmsg.ModelUser) (uint64, error) {
	if user.IsAssign {
		return 0, nil
	}

	rowsAffected, err := AssignChat.assignChatListByUid(ctx, user.CorpId, user.AppId, user.Id)
	if err != nil {
		log.Errorf("err:%v", err)
		return 0, nil
	}

	return rowsAffected, nil
}

// 对未满会话数的客服重新分配
func reassignChatForFreeUser(ctx *rpc.Context, corpId, appId uint32) error {
	config, err := getSessionConfig(ctx, corpId, appId)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	var userList []*qmsg.ModelUser
	db := User.WhereCorpApp(corpId, appId).Where(DbIsDisable, false).Where(DbIsAssign, false)
	if config.AssignSetting.OnlyOnline {
		db.Where(DbIsOnline, true)
	}
	err = db.Find(ctx, &userList)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	for _, user := range userList {
		cnt, err := assignChatForLogin(ctx, user)
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}

		if cnt > 0 {
			err = pushWsMsg(ctx, corpId, appId, user.Id, []*qmsg.WsMsgWrapper{
				{
					MsgType: uint32(qmsg.WsMsgWrapper_MsgTypeIncreaseMaxChat),
				},
			})
			if err != nil {
				log.Errorf("err:%v", err)
				return err
			}
		}
	}

	return nil
}

// 清除掉客服分配的会话，并将这些会话重新分配
func deleteAndResignChat(ctx *rpc.Context, corpId, appId uint32, uid uint64) error {

	var assignChatList []*qmsg.ModelAssignChat
	err := AssignChat.WhereCorpApp(corpId, appId).Where(DbUid, uid).Find(ctx, &assignChatList)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	assignChatIdList := utils.PluckUint64(assignChatList, "Id")

	_ = AssignChat.setClosed(ctx, corpId, appId, map[string]interface{}{
		DbEndScene: qmsg.ModelAssignChat_EndSceneAssignChange,
	}, assignChatIdList...)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	err = LastContactChat.batchCreateOrUpdate(ctx, corpId, appId, assignChatIdList)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	err = pushWsMsgByClose(ctx, corpId, appId, assignChatList, false)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	for _, chat := range assignChatList {

		assignUid, scene, err := findAssignUser(ctx, corpId, appId, chat.RobotUid, chat.ChatExtId, chat.IsGroup, 0)
		if err != nil {
			log.Errorf("err %v", err)
			return err
		}

		_, err = assignUser(ctx, corpId, appId, chat.RobotUid, chat.ChatExtId, assignUid, chat.IsGroup, scene)
		if err != nil {
			log.Errorf("err %v", err)
			return err
		}
	}

	return nil
}

// 下班重新分配会话
func resignChat4Offline(ctx *rpc.Context, corpId, appId uint32, uid uint64) error {

	config, err := getSessionConfig(ctx, corpId, appId)
	if err != nil {
		log.Errorf("err %v", err)
		return err
	}

	if config.SessionSetting == nil || !config.SessionSetting.AllowOffWorkReassign {
		return nil
	}

	var assignChatList []*qmsg.ModelAssignChat
	err = AssignChat.WhereCorpApp(corpId, appId).Where(DbUid, uid).Find(ctx, &assignChatList)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	log.Debugf("need to resign len:%v", len(assignChatList))

	assignIdList := utils.PluckUint64(assignChatList, "Id")
	err = AssignChat.setClosed(ctx, corpId, appId, map[string]interface{}{
		DbEndScene: uint32(qmsg.ModelAssignChat_EndSceneClose),
	}, assignIdList...)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	_, err = TopChat.deleteByUidList(ctx, corpId, appId, []uint64{uid})
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	assignChatIdList := utils.PluckUint64(assignChatList, "Id")
	err = LastContactChat.batchCreateOrUpdate(ctx, corpId, appId, assignChatIdList)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	var wsList []*qmsg.WsMsgWrapper

	for _, chat := range assignChatList {

		wsList = append(wsList, &qmsg.WsMsgWrapper{
			MsgType: uint32(qmsg.WsMsgWrapper_MsgTypeTransferChatClose),
			Chat:    &qmsg.ModelChat{Id: chat.Cid},
		})

		assignUid, scene, err := findAssignUser(ctx, corpId, appId, chat.RobotUid, chat.ChatExtId, chat.IsGroup, 0)
		if err != nil {
			log.Errorf("err %v", err)
			continue
		}

		_, err = assignUser(ctx, corpId, appId, chat.RobotUid, chat.ChatExtId, assignUid, chat.IsGroup, scene)
		if err != nil {
			log.Errorf("err %v", err)
			continue
		}

	}

	err = pushWsMsg(ctx, corpId, appId, uid, wsList)
	if err != nil {
		log.Errorf("err:%v", err)
	}

	return nil
}

type assignInfo struct {
	Count uint32
}

const assignInfoPrefix = "assign_info_%d_%d"

func getWaitingCount(ctx *rpc.Context, corpId, appId uint32) (uint32, error) {
	info := &assignInfo{}
	err := s.RedisGroup.GetJson(fmt.Sprintf(assignInfoPrefix, corpId, appId), info)
	if err != nil {
		if err == redis.Nil {
			c, err := AssignChat.countWaiting(ctx, corpId, appId)
			if err != nil {
				log.Errorf("err:%v", err)
				return c, err
			}
			return c, nil
		} else {
			log.Errorf("err:%v", err)
			return 0, err
		}
	} else {
		return info.Count, nil
	}
}

func deleteAssignChatList(ctx *rpc.Context, corpId, appId uint32, idList []uint64, markChatUnread bool) ([]*qmsg.ModelAssignChat, error) {
	var assignChatList []*qmsg.ModelAssignChat
	err := AssignChat.WhereCorpApp(corpId, appId).WhereIn(DbId, idList).Find(ctx, &assignChatList)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	if len(assignChatList) == 0 {
		return assignChatList, nil
	}

	err = AssignChat.setClosed(ctx, corpId, appId, map[string]interface{}{
		DbEndScene: uint32(qmsg.ModelAssignChat_EndSceneAdmin),
	}, idList...)
	if err != nil {
		log.Errorf("err:%v", err)
		return assignChatList, err
	}

	var cidList []uint64
	for _, assignChat := range assignChatList {
		cidList = append(cidList, assignChat.Cid)

		err = pushWsMsg(ctx, corpId, appId, assignChat.Uid, []*qmsg.WsMsgWrapper{
			{
				MsgType:      uint32(qmsg.WsMsgWrapper_MsgTypeRemoveAssignChat),
				AssignChatId: assignChat.Id,
			},
		})
		if err != nil {
			log.Errorf("err:%v", err)
			return assignChatList, err
		}
	}

	_, err = TopChat.deleteByCidList(ctx, corpId, appId, cidList)
	if err != nil {
		log.Errorf("err:%v", err)
		return assignChatList, err
	}

	// 批量创建最近联系人记录
	assignChatIdList := utils.PluckUint64(assignChatList, "Id")
	err = LastContactChat.batchCreateOrUpdate(ctx, corpId, appId, assignChatIdList)
	if err != nil {
		log.Errorf("err:%v", err)
		return assignChatList, err
	}

	err = pushWsMsgByClose(ctx, corpId, appId, assignChatList, markChatUnread)
	if err != nil {
		log.Errorf("err:%v", err)
		return assignChatList, err
	}

	return assignChatList, nil
}

// 转移多个会话
func transferAssignChatList(ctx *rpc.Context, corpId, appId uint32, uid uint64, idList []uint64) error {
	var assignChatList []*qmsg.ModelAssignChat
	err := AssignChat.WhereCorpApp(corpId, appId).Where(DbUid, "!=", uid).WhereIn(DbId, idList).Find(ctx, &assignChatList)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	if len(assignChatList) == 0 {
		return nil
	}

	idList = utils.PluckUint64(assignChatList, "Id")
	err = AssignChat.setClosed(ctx, corpId, appId, map[string]interface{}{
		DbEndScene: uint32(qmsg.ModelAssignChat_EndSceneAdmin),
	}, idList...)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	var cidList []uint64
	var batchCreate []*qmsg.ModelAssignChat
	for _, assignChat := range assignChatList {
		cidList = append(cidList, assignChat.Cid)
		created, err := AssignChat.create(ctx, assignChat.CorpId, assignChat.AppId, uid, assignChat.Uid, assignChat.RobotUid, assignChat.ChatExtId, assignChat.IsGroup, nil, qmsg.ModelAssignChat_StartSceneAdmin)
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
		// 新好友会话，分配后仍然修改为新好友会话
		if assignChat.AssignChatType == uint32(qmsg.ModelAssignChat_AssignChatTypeNewFriend) {
			_, err = AssignChat.WhereCorpApp(assignChat.CorpId, assignChat.AppId).Where(DbId, assignChat.Id).Update(ctx, map[string]interface{}{
				DbAssignChatType: uint32(qmsg.ModelAssignChat_AssignChatTypeNewFriend),
			})
			if err != nil {
				log.Errorf("err:%v", err)
				return err
			}
		}
		batchCreate = append(batchCreate, created)
		if assignChat.Uid > 0 {
			// 加入最近联系人列表
			_, err = LastContactChat.createOrUpdateByAssignChatId(ctx, corpId, appId, assignChat.Id)
			if err != nil {
				log.Errorf("err:%v", err)
				return err
			}
		}
		err = pushWsMsg(ctx, corpId, appId, assignChat.Uid, []*qmsg.WsMsgWrapper{
			{
				MsgType:      uint32(qmsg.WsMsgWrapper_MsgTypeRemoveAssignChat),
				AssignChat:   assignChat,
				AssignChatId: assignChat.Id,
			},
		})
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
	}

	var chatList []*qmsg.ModelChat
	err = Chat.WhereCorpApp(corpId, appId).WhereIn(DbId, cidList).Find(ctx, &chatList)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	chatMap := map[uint64]*qmsg.ModelChat{}
	utils.KeyBy(chatList, "Id", &chatMap)

	for _, assignChat := range batchCreate {
		err = pushWsMsg(ctx, corpId, appId, assignChat.Uid, []*qmsg.WsMsgWrapper{
			{
				MsgType:    uint32(qmsg.WsMsgWrapper_MsgTypeAssignChat),
				AssignChat: assignChat,
				Chat:       chatMap[assignChat.Cid],
			},
		})
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
	}

	return nil
}

func deleteAssignChatById(ctx *rpc.Context, corpId, appId uint32, id, uid uint64, needCloseWs bool,
	endScene qmsg.ModelAssignChat_EndScene, targetUid uint64) (*qmsg.ModelAssignChat, error) {
	var assignChat qmsg.ModelAssignChat
	err := AssignChat.WhereCorpApp(corpId, appId).Where(DbId, id).Where(DbUid, uid).First(ctx, &assignChat)
	if err != nil && !AssignChat.IsNotFoundErr(err) {
		log.Errorf("err:%v", err)
		return nil, err
	}
	updateParams := map[string]interface{}{
		DbEndScene: uint32(endScene),
	}
	if endScene == qmsg.ModelAssignChat_EndSceneTransfer {
		updateParams[DbTargetUid] = targetUid
	}

	err = AssignChat.setClosed(ctx, corpId, appId, updateParams, id)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	_, err = TopChat.deleteByCid(ctx, corpId, appId, uid, assignChat.Cid)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	// 结束分配会话，往最近联系人表里插入一条数据
	_, err = LastContactChat.createOrUpdateByAssignChatId(ctx, corpId, appId, assignChat.Id)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	if uid != 0 {
		err = pushWsMsg(ctx, corpId, appId, uid, []*qmsg.WsMsgWrapper{
			{
				MsgType:      uint32(qmsg.WsMsgWrapper_MsgTypeRemoveAssignChat),
				AssignChatId: id,
			},
		})
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
	}

	if needCloseWs {
		err = pushWsMsgByClose(ctx, corpId, appId, []*qmsg.ModelAssignChat{&assignChat}, false)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
	}

	return &assignChat, nil
}

func deleteAssignChatByCidList(ctx *rpc.Context, corpId, appId uint32, uid uint64, cidList []uint64) error {
	var assignChatList []*qmsg.ModelAssignChat
	err := AssignChat.WhereCorpApp(corpId, appId).Where(DbUid, uid).WhereIn(DbCid, cidList).Find(ctx, &assignChatList)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	if len(assignChatList) == 0 {
		return nil
	}

	assignIdList := utils.PluckUint64(assignChatList, "Id")

	_ = AssignChat.setClosed(ctx, corpId, appId, map[string]interface{}{
		DbEndScene: qmsg.ModelAssignChat_EndSceneClose,
	}, assignIdList...)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	_, err = TopChat.deleteByUidCidList(ctx, corpId, appId, uid, cidList)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	err = LastContactChat.batchCreateOrUpdate(ctx, corpId, appId, assignIdList)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	err = pushWsMsgByClose(ctx, corpId, appId, assignChatList, false)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	_, err = AssignChat.assignChatListByUid(ctx, corpId, appId, uid)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	return nil
}

func deleteAssignChatByRobotUid(ctx *rpc.Context, corpId, appId uint32, robotUidList []uint64, endScene qmsg.ModelAssignChat_EndScene) error {
	var assignChatList []*qmsg.ModelAssignChat
	err := AssignChat.WhereCorpApp(corpId, appId).WhereIn(DbRobotUid, robotUidList).Find(ctx, &assignChatList)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	assignChatIdList := utils.PluckUint64(assignChatList, "Id")

	_ = AssignChat.setClosed(ctx, corpId, appId, map[string]interface{}{
		DbEndScene: endScene,
	}, assignChatIdList...)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	err = LastContactChat.batchCreateOrUpdate(ctx, corpId, appId, assignChatIdList)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	err = pushWsMsgByClose(ctx, corpId, appId, assignChatList, false)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	return nil
}

//
//func deleteAssignChatByUid(ctx *rpc.Context, corpId, appId uint32, uidList []uint64) error {
//	var assignChatList []*qmsg.ModelAssignChat
//	err := AssignChat.WhereCorpApp(corpId, appId).WhereIn(DbUid, uidList).Find(ctx, &assignChatList)
//	if err != nil {
//		log.Errorf("err:%v", err)
//		return err
//	}
//
//	assignChatIdList := utils.PluckUint64(assignChatList, "Id")
//
//	_ = AssignChat.setClosed(ctx, corpId, appId, map[string]interface{}{
//		DbEndScene: qmsg.ModelAssignChat_EndSceneAssignChange,
//	}, assignChatIdList...)
//	if err != nil {
//		log.Errorf("err:%v", err)
//		return err
//	}
//
//	_, err = TopChat.deleteByUidList(ctx, corpId, appId, uidList)
//	if err != nil {
//		log.Errorf("err:%v", err)
//		return err
//	}
//
//	err = LastContactChat.batchCreateOrUpdate(ctx, corpId, appId, assignChatIdList)
//	if err != nil {
//		log.Errorf("err:%v", err)
//		return err
//	}
//
//	err = pushWsMsgByClose(ctx, corpId, appId, assignChatList)
//	if err != nil {
//		log.Errorf("err:%v", err)
//		return err
//	}
//
//	return nil
//}
