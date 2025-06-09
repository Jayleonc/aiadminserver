package impl

import (
	"fmt"
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/dbproxy"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/brick/utils"
	"git.pinquest.cn/qlb/core"
	"git.pinquest.cn/qlb/extrpkg/github.com/go-redis/redis"
	"git.pinquest.cn/qlb/featswitch"
	"git.pinquest.cn/qlb/qmsg"
	"git.pinquest.cn/qlb/qmsgack"
	"git.pinquest.cn/qlb/websocket"
	"strconv"
	"strings"
)

// 按客服，存一下所有在线的会话
type WsConn struct {
	WsConn    *qmsg.WebSocketConn `json:"ws_conn"`
	ConnectAt uint32              `json:"connect_at"`
}

func (p *WsConn) Equals(rhs *WsConn) bool {
	a := p.WsConn
	b := rhs.WsConn
	return a.ServerIp == b.ServerIp && a.ConnId == b.ConnId
}

type WsConnections []*WsConn

//go:generate pie WsConnections.*
type WsConnList struct {
	List []*WsConn `json:"list"`
}

func genWsConnListRedisKey(corpId, appId uint32) string {
	return fmt.Sprintf("qmsg_ws_conn_list_%d_%d", corpId, appId)
}
func getWsConnList(corpId, appId uint32, uid uint64) (*WsConnList, error) {
	redisKey := genWsConnListRedisKey(corpId, appId)
	var res WsConnList
	err := s.RedisGroup.HMGetJson(redisKey, strconv.FormatUint(uid, 10), &res)
	if err != nil {
		if err != redis.Nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
	}
	return &res, nil
}
func (p *WsConnList) cleanDeadConn(serverIp string, serverStartAt uint32) int {
	n := WsConnections(p.List).Filter(func(conn *WsConn) bool {
		ws := conn.WsConn
		if ws == nil {
			return false
		}
		if ws.ServerIp == serverIp {
			if ws.ServerStartAt != serverStartAt {
				return false
			}
		}
		return true
	})
	d := len(p.List) - len(n)
	p.List = n
	return d
}
func (p *WsConnList) addWsConn(wsConn *qmsg.WebSocketConn) error {
	c := &WsConn{
		WsConn:    wsConn,
		ConnectAt: utils.Now(),
	}
	if WsConnections(p.List).FindFirstUsing(c.Equals) < 0 {
		p.List = append(p.List, c)
	}
	redisKey := genWsConnListRedisKey(wsConn.CorpId, wsConn.AppId)
	err := s.RedisGroup.HSetJson(
		redisKey, strconv.FormatUint(wsConn.Uid, 10), p.List, 0)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	return nil
}
func (p *WsConnList) delWsConn(wsConn *qmsg.WebSocketConn) error {
	c := &WsConn{
		WsConn:    wsConn,
		ConnectAt: utils.Now(),
	}
	resList := WsConnections(p.List).Filter(func(conn *WsConn) bool {
		return !conn.Equals(c)
	})
	p.List = resList
	redisKey := genWsConnListRedisKey(wsConn.CorpId, wsConn.AppId)
	var err error
	if len(p.List) == 0 {
		_, err = s.RedisGroup.HDel(redisKey, strconv.FormatUint(wsConn.Uid, 10))
	} else {
		err = s.RedisGroup.HSetJson(
			redisKey, strconv.FormatUint(wsConn.Uid, 10), p.List, 0)
	}
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	return nil
}

func OnWsReady(ctx *rpc.Context, req *qmsg.OnWsReadyReq) (*qmsg.OnWsReadyRsp, error) {
	var rsp qmsg.OnWsReadyRsp
	c := req.Conn
	l, err := getWsConnList(c.CorpId, c.AppId, c.Uid)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	n := l.cleanDeadConn(c.ServerIp, c.ServerStartAt)
	if n > 0 {
		log.Warnf("clean %d dead conn", n)
	}

	err = l.addWsConn(c)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	return &rsp, nil
}

func OnWsClose(ctx *rpc.Context, req *qmsg.OnWsCloseReq) (*qmsg.OnWsCloseRsp, error) {
	var rsp qmsg.OnWsCloseRsp
	c := req.Conn
	l, err := getWsConnList(c.CorpId, c.AppId, c.Uid)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	n := l.cleanDeadConn(c.ServerIp, c.ServerStartAt)
	if n > 0 {
		log.Warnf("clean %d dead conn", n)
	}
	err = l.delWsConn(c)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	return &rsp, nil
}

func pushWsMsgToAll(ctx *rpc.Context, corpId, appId uint32, wrapperList []*qmsg.WsMsgWrapper) error {
	var userList []*qmsg.ModelUser
	err := User.WhereCorpApp(corpId, appId).Find(ctx, &userList)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	for _, user := range userList {
		err = pushWsMsg(ctx, corpId, appId, user.Id, wrapperList)
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
	}

	return nil
}

func setOpenMsgAck(ctx *rpc.Context, corpId, appId uint32, wrapperList []*qmsg.WsMsgWrapper) ([]*qmsg.WsMsgWrapper, error) {
	enable, err := featswitch.GetFeatureFlagEnabled(ctx, &featswitch.GetFeatureFlagEnabledReq{
		Key:    "QMSG_MSG_ACK",
		CorpId: corpId,
		AppId:  appId,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return wrapperList, err
	}

	if !enable.Enabled {
		return wrapperList, nil
	}

	wrapperList[0].AckId = fmt.Sprintf("%d_%d", wrapperList[0].Chat.Id, wrapperList[0].Chat.MsgSeq)

	return wrapperList, nil
}

func pushWsMsg(ctx *rpc.Context, corpId, appId uint32, uid uint64, wrapperList []*qmsg.WsMsgWrapper) error {
	for _, wrapper := range wrapperList {
		data, err := utils.Pb2Json(wrapper)
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
		if wrapper.AckId != "" {
			ackIdItems := strings.Split(wrapper.AckId, "_")
			if len(ackIdItems) == 2 {
				chatId, _ := strconv.Atoi(ackIdItems[0])
				msgId, _ := strconv.Atoi(ackIdItems[1])
				_, err = qmsgack.AddAcknowledge(ctx, &qmsgack.AddAcknowledgeReq{
					MsgId:  uint64(msgId),
					MsgBuf: data,
					CorpId: corpId,
					AppId:  appId,
					Uid:    uid,
					ChatId: uint64(chatId),
				})
				if err != nil {
					log.Errorf("err:%v", err)
					return err
				}
			}
		}
		_, err = websocket.Push(ctx, &websocket.PushReq{
			BizType: uint32(core.PushType_PushTypeQMsg),
			BizId:   "",
			Data:    data,
			CorpId:  corpId,
			AppId:   appId,
			Uid:     uid,
		})
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}

		//不打全量日志，只打关键日志
		var assignChatId, chatId uint64
		if wrapper.AssignChat != nil {
			assignChatId = wrapper.AssignChat.Id
		}
		if wrapper.Chat != nil {
			chatId = wrapper.Chat.Id
		}
		log.Infof("push msg_type:%d, msg_id:%s assignChatId:%d chatId:%v uid %d", wrapper.MsgType, wrapper.MsgId, assignChatId, chatId, uid)
	}
	return nil
}

// 根据机器人归属通知有资产客服
func pushWsMsgByRobotUid(ctx *rpc.Context, corpId, appId uint32, robotUid uint64, wrapperList []*qmsg.WsMsgWrapper) error {
	// 通过 robot uid 查下消息应该发给哪个客服
	var list []*qmsg.ModelUserRobot
	err := UserRobot.Where(map[string]interface{}{
		DbCorpId:   corpId,
		DbAppId:    appId,
		DbRobotUid: robotUid,
	}).Find(ctx, &list)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	for _, v := range list {
		err = pushWsMsg(ctx, corpId, appId, v.Uid, wrapperList)
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
	}
	return nil
}

// 根据会话通知非资产跟有资产客服
func pushWsMsgByChat(ctx *rpc.Context, chat *qmsg.ModelChat, assignChat *qmsg.ModelAssignChat, wrapperList []*qmsg.WsMsgWrapper) error {
	if chat == nil {
		return nil
	}

	for _, wrapper := range wrapperList {
		if wrapper.MsgType == uint32(qmsg.WsMsgWrapper_MsgTypeMsgBox) {
			if wrapper.Chat != nil && wrapper.Chat.LastReceiveAt == 0 {
				wrapper.Chat.LastReceiveAt = wrapper.Chat.CreatedAt
			}
		}
	}

	err := pushWsMsgByRobotUid(ctx, chat.CorpId, chat.AppId, chat.RobotUid, wrapperList)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	if assignChat == nil {
		assignChat, err = AssignChat.getByCid(ctx, chat.CorpId, chat.AppId, chat.Id)
		if err != nil {
			if AssignChat.IsNotFoundErr(err) {
				return nil
			}
			log.Errorf("err:%v", err)
			return err
		}
	}

	if assignChat.Uid == 0 {
		return nil
	}

	err = pushWsMsg(ctx, chat.CorpId, chat.AppId, assignChat.Uid, wrapperList)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	return nil
}

// 根据机器人归属通知有资产客服
func pushWsMsgByRobotUidV2(ctx *rpc.Context, corpId, appId uint32, robotUid uint64, wrapperList []*qmsg.WsMsgWrapper) error {
	// 通过 robot uid 查下消息应该发给哪个客服
	var list []*qmsg.ModelUserRobot
	err := UserRobot.Where(map[string]interface{}{
		DbCorpId:   corpId,
		DbAppId:    appId,
		DbRobotUid: robotUid,
	}).Find(ctx, &list)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	// 检查是否需要过滤内容已读状态
	readStateFilterResult := map[uint64]map[uint64]bool{}
	chatUnReadCount := map[uint64]uint32{}
	if len(list) > 0 {
		userIdList := utils.PluckUint64(list, "Uid").Unique()
		for _, wrapper := range wrapperList {
			if wrapper.MsgBox == nil || wrapper.Chat == nil || wrapper.MsgType != uint32(qmsg.WsMsgWrapper_MsgTypeMsgBox) {
				continue
			}
			filterResult, err := filterChatFirstMsgReadState(ctx, userIdList, wrapper.MsgBox)
			if err != nil {
				// 这里更新失败不中断，不能影响推送消息到前端
				log.Errorf("err:%v", err)
				continue
			}

			// 更新会话数据表的未读数量
			if _, ok := chatUnReadCount[wrapper.Chat.Id]; !ok {
				// 判断如果有一个客服开启不提醒，就修改未读数
				filterRs := false
				for _, result := range filterResult {
					if result {
						filterRs = true
						break
					}
				}
				if filterRs {
					// 变更未读数量(未读状态是根据数量来的)
					updateMap := map[string]interface{}{
						DbUnreadCount: dbproxy.Expr(fmt.Sprintf("%s - %d", DbUnreadCount, 1)),
					}
					_, err = Chat.WhereCorpApp(wrapper.MsgBox.CorpId, wrapper.MsgBox.AppId).
						Where(map[string]interface{}{
							DbRobotUid:  wrapper.MsgBox.Uid,
							DbIsGroup:   wrapper.MsgBox.ChatType == uint32(qmsg.ChatType_ChatTypeGroup),
							DbChatExtId: wrapper.MsgBox.ChatId,
						}).
						Gt(DbUnreadCount, 0).
						Update(ctx, updateMap)
					if err != nil {
						// 这里更新不成功也不中断，不能影响推送消息
						log.Errorf("err:%v", err)
						continue
					}
				}
			}

			// 针对每个消息做过滤结果储存，下面需要针对每个客服做判断变化
			readStateFilterResult[wrapper.MsgBox.Id] = filterResult
			// 未读数量，这里储存最原始的未读数量，下面需要每个客服单个做变化
			chatUnReadCount[wrapper.Chat.Id] = wrapper.Chat.UnreadCount
		}
	}
	// 循环每个客服，推送消息到前端
	for _, v := range list {
		// 针对当前客服，循环每个消息，过滤未读提醒
		for _, wrapper := range wrapperList {
			if wrapper.MsgBox == nil || wrapper.Chat == nil || wrapper.MsgType != uint32(qmsg.WsMsgWrapper_MsgTypeMsgBox) {
				continue
			}
			readState, ok1 := readStateFilterResult[wrapper.MsgBox.Id]
			unReadCount, ok2 := chatUnReadCount[wrapper.Chat.Id]
			if !ok1 || !ok2 {
				continue
			}
			userReadState, ok := readState[v.Uid]
			if !ok {
				continue
			}
			// 重置未读数，可能上一个客服-1了
			wrapper.Chat.UnreadCount = unReadCount
			// 处理未读数量
			if userReadState && unReadCount > 0 {
				// 针对当前客服的配置，处理未读数量后推送
				wrapper.Chat.UnreadCount = unReadCount - 1
			}
		}
		// 推送消息到前端
		err = pushWsMsg(ctx, corpId, appId, v.Uid, wrapperList)
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
	}
	return nil
}

// 推送消息到无资产分配会话的客服
func pushWsMsgByAssignChat(ctx *rpc.Context, chat *qmsg.ModelChat, assignChat *qmsg.ModelAssignChat, wrapperList []*qmsg.WsMsgWrapper) error {
	// 检查无资产分配模式下是否有分配了客服
	var err error
	if assignChat == nil {
		assignChat, err = AssignChat.getByCid(ctx, chat.CorpId, chat.AppId, chat.Id)
		if err != nil {
			if AssignChat.IsNotFoundErr(err) {
				return nil
			}
			log.Errorf("err:%v", err)
			return err
		}
	}

	// 未分配无资产的客服
	if assignChat.Uid == 0 {
		return nil
	}

	// 处理无资产分配模式下的消息过滤
	for _, wrapper := range wrapperList {
		if wrapper.MsgBox == nil || wrapper.Chat == nil {
			continue
		}
		filterResult, err := filterChatFirstMsgReadState(ctx, []uint64{assignChat.Uid}, wrapper.MsgBox)
		if err != nil {
			// 这里更新失败不中断，不能影响推送消息到前端
			log.Errorf("err:%v", err)
			continue
		}
		// 更新会话数据表的未读数量
		if userReadState, ok := filterResult[assignChat.Uid]; ok {
			// 变更未读数量(未读状态是根据数量来的)
			updateMap := map[string]interface{}{
				DbUnreadCount: dbproxy.Expr(fmt.Sprintf("%s - %d", DbUnreadCount, 1)),
			}
			_, err = Chat.WhereCorpApp(wrapper.MsgBox.CorpId, wrapper.MsgBox.AppId).
				Where(map[string]interface{}{
					DbRobotUid:  wrapper.MsgBox.Uid,
					DbIsGroup:   wrapper.MsgBox.ChatType == uint32(qmsg.ChatType_ChatTypeGroup),
					DbChatExtId: wrapper.MsgBox.ChatId,
				}).
				Gt(DbUnreadCount, 0).
				Update(ctx, updateMap)
			if err != nil {
				// 这里更新不成功也不中断，不能影响推送消息
				log.Errorf("err:%v", err)
				continue
			}
			// 处理未读数量
			if userReadState && wrapper.Chat.UnreadCount > 0 {
				// 针对当前客服的配置，处理未读数量后推送
				wrapper.Chat.UnreadCount = wrapper.Chat.UnreadCount - 1
			}
		}
	}
	// 推送无资产分配模式下对应的客服
	err = pushWsMsg(ctx, chat.CorpId, chat.AppId, assignChat.Uid, wrapperList)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	return nil
}

// 根据会话通知非资产跟有资产客服
func pushWsMsgByChatV2(ctx *rpc.Context, chat *qmsg.ModelChat, assignChat *qmsg.ModelAssignChat, wrapperList []*qmsg.WsMsgWrapper) error {
	// 这里是通过会话分配通知到客服，没分配过的会话，不做通知
	if chat == nil {
		return nil
	}

	// 收到消息，纠正会话最后接收时间
	for _, wrapper := range wrapperList {
		if wrapper.MsgType == uint32(qmsg.WsMsgWrapper_MsgTypeMsgBox) {
			if wrapper.Chat != nil && wrapper.Chat.LastReceiveAt == 0 {
				wrapper.Chat.LastReceiveAt = wrapper.Chat.CreatedAt
			}
		}
	}

	// 检查并且分配给有资产对应机器人的客服
	err := pushWsMsgByRobotUidV2(ctx, chat.CorpId, chat.AppId, chat.RobotUid, wrapperList)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	// 推送无正常分配的会话客服
	err = pushWsMsgByAssignChat(ctx, chat, assignChat, wrapperList)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	return nil
}

// 如果会话在非资产客服就通知非资产客服，否则通知有资产客服
func pushWsMsgAssignPriority(ctx *rpc.Context, corpId, appId uint32, robotUid, chatExtId uint64, isGroup bool, wrapperList []*qmsg.WsMsgWrapper) error {
	chat, err := Chat.getByRobotAndExt(ctx, corpId, appId, robotUid, chatExtId, isGroup)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	assignChat, err := AssignChat.getByCid(ctx, corpId, appId, chat.Id)
	if err != nil {
		if AssignChat.IsNotFoundErr(err) {
			err = pushWsMsgByRobotUid(ctx, corpId, appId, robotUid, wrapperList)
			if err != nil {
				log.Errorf("err:%v", err)
				return err
			}
		} else {
			log.Errorf("err:%v", err)
			return err
		}
	} else {
		err = pushWsMsg(ctx, chat.CorpId, chat.AppId, assignChat.Uid, wrapperList)
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
	}

	return nil
}

// 找找这些分配会话是不是有资产客服分配出去的，是就通知一下
func pushWsMsgByClose(ctx *rpc.Context, corpId, appId uint32, assignChatList []*qmsg.ModelAssignChat, markChatUnread bool) error {
	var robotUidList []uint64
	robotChatMap := map[uint64][]uint64{}
	for _, assignChat := range assignChatList {
		robotUidList = append(robotUidList, assignChat.RobotUid)
		if m, ok := robotChatMap[assignChat.RobotUid]; ok {
			robotChatMap[assignChat.RobotUid] = append(m, assignChat.Cid)
		} else {
			robotChatMap[assignChat.RobotUid] = []uint64{assignChat.Cid}
		}
	}

	var userRobotList []*qmsg.ModelUserRobot
	err := UserRobot.WhereCorpApp(corpId, appId).WhereIn(DbRobotUid, robotUidList).Find(ctx, &userRobotList)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	for _, userRobot := range userRobotList {
		if cidList, ok := robotChatMap[userRobot.RobotUid]; ok {
			// 获取对应的会话列表
			chatMap := make(map[uint64]*qmsg.ModelChat)
			chatList, err := Chat.getListById(ctx, corpId, appId, cidList)
			if err != nil {
				log.Errorf("err:%v", err)
				// 不影响下推
			}
			for _, chat := range chatList {
				chatMap[chat.Id] = chat
			}

			var wsList []*qmsg.WsMsgWrapper
			for _, cid := range cidList {
				unReadCount := uint32(0)
				chat := &qmsg.ModelChat{}
				if chatItem, ok := chatMap[cid]; ok {
					chat = chatItem
					unReadCount = chat.UnreadCount
					// 标记了未读才去处理未读数逻辑
					if markChatUnread {
						if chatItem.UnreadCount == 0 {
							// 未读数为0则置1
							unReadCount = 1
						}
					}
				}
				wsList = append(wsList, &qmsg.WsMsgWrapper{
					MsgType: uint32(qmsg.WsMsgWrapper_MsgTypeTransferChatClose),
					Chat:    &qmsg.ModelChat{Id: cid, UnreadCount: unReadCount, ChatExtId: chat.ChatExtId, IsGroup: chat.IsGroup, RobotUid: chat.RobotUid},
				})
			}
			err = pushWsMsg(ctx, corpId, appId, userRobot.Uid, wsList)
			if err != nil {
				log.Errorf("err:%v", err)
			}
		}
	}

	return nil
}

func pushAiReplyWS(ctx *rpc.Context, corpId, appId uint32, robotUid, chatExtId uint64, isGroup bool, cliMsgId, replyText string) error {
	wrapper := &qmsg.WsMsgWrapper{
		MsgType: uint32(qmsg.WsMsgWrapper_MsgTypeAiReplyGenerated),
		AiReplyGenerated: &qmsg.WsMsgWrapper_AiReplyGenerated{
			CliMsgId: cliMsgId,
			ChatId:   chatExtId,
			RobotUid: robotUid,
			Reply:    replyText,
		},
	}
	return pushWsMsgAssignPriority(ctx, corpId, appId, robotUid, chatExtId, isGroup, []*qmsg.WsMsgWrapper{wrapper})
}
