package impl

import (
	"encoding/csv"
	"fmt"
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/json"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/brick/utils"
	"git.pinquest.cn/qlb/commonv2"
	"git.pinquest.cn/qlb/core"
	"git.pinquest.cn/qlb/iquan"
	"git.pinquest.cn/qlb/qmsg"
	"git.pinquest.cn/qlb/qrobot"
	"git.pinquest.cn/qlb/quan"
	"time"
)

func TransferChat(ctx *rpc.Context, req *qmsg.TransferChatReq) (*qmsg.TransferChatRsp, error) {
	var rsp *qmsg.TransferChatRsp
	var robotUid, chatExtId uint64
	var isGroup bool
	var chatType uint32
	var transferType uint32

	user, err := getUserCheck(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	if user.Id == req.Uid {
		return nil, rpc.InvalidArg("不能转给自己")
	}

	corpId, appId := user.CorpId, user.AppId
	uid := user.Id

	transferUser, err := User.getUserById(ctx, corpId, appId, req.Uid)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	if transferUser.IsAssign {
		return nil, rpc.InvalidArg("不能转给分配资产的客服")
	}

	if !transferUser.IsOnline {
		return nil, rpc.InvalidArg("该客服已下班")
	}

	if req.AssignChatId != 0 {
		var assignChat qmsg.ModelAssignChat
		err = AssignChat.WhereCorpApp(corpId, appId).Where(DbId, req.AssignChatId).Where(DbUid, uid).First(ctx, &assignChat)
		if err != nil {
			//warning.ReportMsg(ctx, fmt.Sprintf("err: %v", err))
			log.Errorf("err:%v", err)
			return nil, err
		}

		_, err = deleteAssignChatById(ctx, corpId, appId, req.AssignChatId, uid, false,
			qmsg.ModelAssignChat_EndSceneTransfer, req.Uid)
		if err != nil {
			//warning.ReportMsg(ctx, fmt.Sprintf("err: %v", err))
			log.Errorf("err:%v", err)
			return nil, err
		}

		robotUid = assignChat.RobotUid
		chatExtId = assignChat.ChatExtId
		isGroup = assignChat.IsGroup
		if isGroup {
			chatType = uint32(qmsg.ChatType_ChatTypeGroup)
		} else {
			chatType = uint32(qmsg.ChatType_ChatTypeSingle)
		}
		transferType = uint32(qmsg.ChatTransferChatRemarkMsg_TransferTypeNoAsset) //无资产客服转介

		_, err = AssignChat.assignChatListByUid(ctx, corpId, appId, uid)
		if err != nil {
			//warning.ReportMsg(ctx, fmt.Sprintf("err: %v", err))
			log.Errorf("err:%v", err)
			return nil, err
		}
	} else {
		chat, err := Chat.get(ctx, corpId, appId, req.Cid)
		if err != nil {
			//warning.ReportMsg(ctx, fmt.Sprintf("err: %v", err))
			log.Errorf("err:%v", err)
			return nil, err
		}

		assignChat, err := AssignChat.getByCid(ctx, corpId, appId, req.Cid)
		if err != nil && !AssignChat.IsNotFoundErr(err) {
			log.Errorf("err:%v", err)
			return nil, err
		}

		if assignChat != nil {
			//warning.ReportMsg(ctx, fmt.Sprintf("err: Assign chat has already assign"))
			return nil, rpc.InvalidArg("该会话已经被分配")
		}

		_, err = UserRobot.getByFull(ctx, corpId, appId, uid, chat.RobotUid)
		if err != nil {
			//warning.ReportMsg(ctx, fmt.Sprintf("err: %v", err))
			log.Errorf("err:%v", err)
			return nil, err
		}

		robotUid = chat.RobotUid
		chatExtId = chat.ChatExtId
		isGroup = chat.IsGroup
		if isGroup {
			chatType = uint32(qmsg.ChatType_ChatTypeGroup)
		} else {
			chatType = uint32(qmsg.ChatType_ChatTypeSingle)
		}
		transferType = uint32(qmsg.ChatTransferChatRemarkMsg_TransferTypeAsset) //有资产客服转介
	}

	transferAssignChat, err := AssignChat.create(ctx, corpId, appId, req.Uid, uid, robotUid, chatExtId, isGroup, nil, qmsg.ModelAssignChat_StartSceneTransfer)
	if err != nil {
		//warning.ReportMsg(ctx, fmt.Sprintf("err: %v", err))
		log.Errorf("err:%v", err)
		return nil, err
	}

	msgContent := &qmsg.ChatTransferChatRemarkMsg{
		Date:             time.Now().Format("2006年01月02日 15:04"),
		Mark:             req.Mark,
		Username:         user.Username,
		UserId:           fmt.Sprintf("%d", user.Id),
		TransferUsername: transferUser.Username,
		TransferUserId:   fmt.Sprintf("%d", transferUser.Id),
		TransferType:     transferType,
	}

	msgContentBuffer, err := json.Marshal(msgContent)
	if err != nil {
		return nil, err
	}

	cliMsgId := "transfer_assign_chat_" + utils.GenRandomStr()

	_, err = sendAssignMsg(ctx, &qmsg.SendMsgReq{
		Msg: &qmsg.ModelMsgBox{
			CorpId:      corpId,
			AppId:       appId,
			Uid:         transferAssignChat.RobotUid,
			CliMsgId:    cliMsgId,
			MsgType:     uint32(qmsg.MsgType_MsgTypeTransferChatRemark),
			ChatType:    chatType,
			ChatId:      transferAssignChat.ChatExtId,
			ChatMsgType: uint32(qmsg.ChatMsgType_ChatMsgTypeTransferChatRemark),
			Msg:         string(msgContentBuffer),
			SentAt:      uint32(time.Now().Unix()),
		},
	}, transferAssignChat, req.Mark)

	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	transferLog := &qmsg.ModelTransferLog{
		CorpId:       corpId,
		AppId:        appId,
		CliMsgId:     cliMsgId,
		ChatId:       transferAssignChat.ChatExtId,
		Uid:          transferAssignChat.RobotUid,
		AssignChatId: transferAssignChat.Id,
		Mark:         req.Mark,
		ChatType:     chatType,
	}
	_, err = TransferLog.updateOrCreate(ctx, transferLog)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	if req.AssignChatId == 0 {
		var userRobotList []*qmsg.ModelUserRobot
		err = UserRobot.WhereCorpApp(corpId, appId).Where(DbRobotUid, robotUid).Find(ctx, &userRobotList)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}

		for _, userRobot := range userRobotList {
			err = pushWsMsg(ctx, corpId, appId, userRobot.Uid, []*qmsg.WsMsgWrapper{
				{
					MsgType:      uint32(qmsg.WsMsgWrapper_MsgTypeRemoveAssignChat),
					AssignChatId: req.AssignChatId,
					Chat:         &qmsg.ModelChat{Id: req.Cid},
				},
			})
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, err
			}
		}
	}
	return rsp, nil
}

func CloseChat(ctx *rpc.Context, req *qmsg.CloseChatReq) (*qmsg.CloseChatRsp, error) {
	var rsp qmsg.CloseChatRsp

	user, err := getUserCheck(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	//非资产模式才触发的逻辑
	if req.AssignChatId > 0 {

		assignChat, err := deleteAssignChatById(ctx, user.CorpId, user.AppId, req.AssignChatId, user.Id, true,
			qmsg.ModelAssignChat_EndSceneClose, 0)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}

		_, err = AssignChat.assignChatListByUid(ctx, user.CorpId, user.AppId, user.Id)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}

		err = removeSendList4Chat(ctx, user.CorpId, user.AppId, []uint64{assignChat.Cid})
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}

		err = addCloseTransferMsg(ctx, user.CorpId, user.AppId, assignChat.Id, "客服结束会话")
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}

	}
	var aiConfig qmsg.ModelAiEntertainConfig
	err = aiEntertainConfig.WhereCorp(user.CorpId).First(ctx, &aiConfig)
	if err != nil && !aiEntertainConfig.IsNotFoundErr(err) {
		log.Errorf("err %v", err)
		return nil, err
	}

	var chat qmsg.ModelChat
	err = Chat.WhereCorpApp(user.CorpId, user.AppId).Where(DbId, req.ModelChatId).First(ctx, &chat)
	if err != nil {
		log.Errorf("err %v", err)
		return nil, err
	}

	//未配置智能助手
	if aiEntertainConfig.IsNotFoundErr(err) || req.ModelChatId == 0 {
		log.Debugf("%v %v %v ", aiEntertainConfig.IsNotFoundErr(err), req.ModelChatId == 0, aiConfig.Priority != uint32(qmsg.ModelAiEntertainConfig_True))
		return &rsp, nil
	}
	//私聊未开启
	if !chat.IsGroup && aiConfig.Priority != uint32(qmsg.ModelAiEntertainConfig_True) {
		log.Debugf("%v %v %v ", aiEntertainConfig.IsNotFoundErr(err), req.ModelChatId == 0, aiConfig.Priority != uint32(qmsg.ModelAiEntertainConfig_True))
		return &rsp, nil
	}
	//群聊未开启
	if chat.IsGroup && aiConfig.GroupPriority != uint32(qmsg.ModelAiEntertainConfig_True) {
		log.Debugf("%v %v %v ", aiEntertainConfig.IsNotFoundErr(err), req.ModelChatId == 0, aiConfig.GroupPriority != uint32(qmsg.ModelAiEntertainConfig_True))
		return &rsp, nil
	}

	if chat.Detail == nil {
		chat.Detail = &qmsg.ModelChat_Detail{}
	}

	chat.Detail.EntertainType = uint32(qmsg.ModelChat_Detail_Ai)
	_, err = Chat.WhereCorpApp(user.CorpId, user.AppId).Where(DbId, req.ModelChatId).Update(ctx, map[string]interface{}{
		DbDetail: chat.Detail,
	})
	if err != nil {
		log.Errorf("err %v", err)
		return nil, err
	}

	return &rsp, nil
}

func BatchCloseChat(ctx *rpc.Context, req *qmsg.BatchCloseChatReq) (*qmsg.BatchCloseChatRsp, error) {
	var rsp qmsg.BatchCloseChatRsp

	user, err := getUserCheck(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	if user.IsAssign {
		return nil, rpc.InvalidArg("分配资产客服不能批量结束会话")
	}

	assignChatList, err := AssignChat.getListByUid(ctx, user.CorpId, user.AppId, user.Id)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	cidList := utils.PluckUint64(assignChatList, "Cid")

	chatList, err := Chat.getListById(ctx, user.CorpId, user.AppId, cidList)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	config, err := getSessionConfig(ctx, user.CorpId, user.AppId)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	// AllowCloseUnansweredByUser默认开启
	if config.SessionSetting == nil || !config.SessionSetting.AllowCloseUnansweredByUser {
		chatList = qmsg.ChatList(chatList).Filter(func(chat *qmsg.ModelChat) bool {
			return chat.UnreadCount == 0
		})
	}

	err = core.NewListOptionProcessor(req.ListOption).
		AddBool(qmsg.BatchCloseChatReq_ListOptionIsGroup, func(val bool) error {
			chatList = qmsg.ChatList(chatList).Filter(func(chat *qmsg.ModelChat) bool {
				return chat.IsGroup == val
			})
			return nil
		}).
		AddBool(qmsg.BatchCloseChatReq_ListOptionIsUnread, func(val bool) error {
			chatList = qmsg.ChatList(chatList).Filter(func(chat *qmsg.ModelChat) bool {
				return chat.UnreadCount > 0
			})
			return nil
		}).
		Process()
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	cidList = utils.PluckUint64(chatList, "Id")

	err = deleteAssignChatByCidList(ctx, user.CorpId, user.AppId, user.Id, cidList)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	rsp.ClosedChatIdList = cidList
	err = removeSendList4Chat(ctx, user.CorpId, user.AppId, cidList)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	for _, assignChat := range assignChatList {
		err = addCloseTransferMsg(ctx, user.CorpId, user.AppId, assignChat.Id, "客服批量结束会话")
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}

	}

	var aiConfig qmsg.ModelAiEntertainConfig
	err = aiEntertainConfig.WhereCorp(user.CorpId).First(ctx, &aiConfig)
	if err != nil && !aiEntertainConfig.IsNotFoundErr(err) {
		log.Errorf("err %v", err)
		return nil, err
	}

	//未配置智能助手
	if aiEntertainConfig.IsNotFoundErr(err) {
		log.Debugf("%v  %v ", aiEntertainConfig.IsNotFoundErr(err), aiConfig.Priority != uint32(qmsg.ModelAiEntertainConfig_True))
		return &rsp, nil
	}

	for _, chat := range chatList {

		//私聊未开启
		if !chat.IsGroup && aiConfig.Priority != uint32(qmsg.ModelAiEntertainConfig_True) {
			log.Debugf("%v  %v ", aiEntertainConfig.IsNotFoundErr(err), aiConfig.Priority != uint32(qmsg.ModelAiEntertainConfig_True))
			continue
		}
		//群聊未开启
		if chat.IsGroup && aiConfig.GroupPriority != uint32(qmsg.ModelAiEntertainConfig_True) {
			log.Debugf("%v  %v ", aiEntertainConfig.IsNotFoundErr(err), aiConfig.Priority != uint32(qmsg.ModelAiEntertainConfig_True))
			continue
		}

		if chat.Detail == nil {
			chat.Detail = &qmsg.ModelChat_Detail{}
		}

		chat.Detail.EntertainType = uint32(qmsg.ModelChat_Detail_Ai)
		_, err = Chat.WhereCorpApp(user.CorpId, user.AppId).Where(DbId, chat.Id).Update(ctx, map[string]interface{}{
			DbDetail: chat.Detail,
		})
		if err != nil {
			log.Errorf("err %v", err)
			return nil, err
		}
	}

	return &rsp, nil

}

func GetAssignChatList(ctx *rpc.Context, req *qmsg.GetAssignChatListReq) (*qmsg.GetAssignChatListRsp, error) {

	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)

	var (
		err            error
		rsp            qmsg.GetAssignChatListRsp
		assignChatList []*qmsg.ModelAssignChat
		chatList       []*qmsg.ModelChat

		assignChatRspList bool
		chatRspMap        bool
		accountRspMap     bool
		userRspMap        bool
		quanUserRspMap    bool
		lastAssignRspMap  bool
		hasGetChat        bool
		accountListRsp    *qrobot.GetAccountListRsp
		groupListRsp      *qrobot.GetGroupChatListSysRsp
		// 是否是查询最近联系人
		isTrashOnly bool
		// 是否是倒序查询
		isOrderByDesc bool
	)

	db := AssignChat.NewList(req.ListOption).WhereCorpApp(corpId, appId)

	err = core.NewListOptionProcessor(req.ListOption).
		AddUint64(qmsg.GetAssignChatListReq_ListOptionUid, func(val uint64) error {
			db.Where(DbUid, val)
			return nil
		}).
		AddBool(qmsg.GetAssignChatListReq_ListOptionUidNotZero, func(val bool) error {
			if val {
				db.Where(DbUid, "!=", 0)
			} else {
				db.Where(DbUid, 0)
			}
			return nil
		}).
		AddBool(qmsg.GetAssignChatListReq_ListOptionExceptWaiting, func(val bool) error {
			db.WhereRaw("!(uid = 0 AND deleted_at = 0)")
			return nil
		}).
		AddUint64(qmsg.GetAssignChatListReq_ListOptionRobotUid, func(val uint64) error {
			db.Where(DbRobotUid, val)
			return nil
		}).
		AddUint64List(qmsg.GetAssignChatListReq_ListOptionRobotUidList, func(val []uint64) error {
			db.WhereIn(DbRobotUid, val)
			return nil
		}).
		AddBool(qmsg.GetAssignChatListReq_ListOptionAccountRspMap, func(val bool) error {
			accountRspMap = val
			return nil
		}).
		AddBool(qmsg.GetAssignChatListReq_ListOptionUserRspMap, func(val bool) error {
			userRspMap = val
			return nil
		}).
		AddUint64(qmsg.GetAssignChatListReq_ListOptionUidCidAllDelete, func(val uint64) error {
			db.WhereRaw(fmt.Sprintf("cid NOT IN (SELECT cid FROM qmsg_assign_chat WHERE (`corp_id` = %d) AND (`app_id` = %d) AND (`uid` = %d) AND deleted_at = 0)", corpId, appId, val))
			return nil
		}).
		AddBool(qmsg.GetAssignChatListReq_ListOptionRspAssignChatList, func(val bool) error {
			assignChatRspList = val
			return nil
		}).
		AddBool(qmsg.GetAssignChatListReq_ListOptionRspChatMap, func(val bool) error {
			chatRspMap = val
			return nil
		}).
		AddBool(qmsg.GetAssignChatListReq_ListOptionRspLastAssignMap, func(val bool) error {
			lastAssignRspMap = val
			return nil
		}).
		AddUint64(qmsg.GetAssignChatListReq_ListOptionCid, func(val uint64) error {
			db.Where(DbCid, val)
			return nil
		}).
		AddBool(qmsg.GetAssignChatListReq_ListOptionWithTrash, func(val bool) error {
			if val {
				db.Unscoped()
			}
			return nil
		}).
		AddBool(qmsg.GetAssignChatListReq_ListOptionRspQuanUserMap, func(val bool) error {
			quanUserRspMap = val
			return nil
		}).
		AddBool(qmsg.GetAssignChatListReq_ListOptionTrashOnly, func(val bool) error {
			if val {
				db.Unscoped().Where(DbDeletedAt, ">", 0)
				isTrashOnly = true
			} else {
				db.Unscoped().Where(DbDeletedAt, 0)
			}
			return nil
		}).
		AddUint32(qmsg.GetAssignChatListReq_ListOptionOrderBy, func(val uint32) error {
			switch val {
			case uint32(qmsg.GetAssignChatListReq_OrderByCreatedDesc):
				db.OrderDesc(DbCreatedAt)
				isOrderByDesc = true
			case uint32(qmsg.GetAssignChatListReq_OrderByCreatedAsc):
				db.OrderAsc(DbCreatedAt)
			case uint32(qmsg.GetAssignChatListReq_OrderByDeletedDesc):
				isOrderByDesc = true
				db.OrderDesc(DbDeletedAt)
			case uint32(qmsg.GetAssignChatListReq_OrderByDeletedAsc):
				db.OrderAsc(DbDeletedAt)
			}
			return nil
		}).
		AddUint32(qmsg.GetAssignChatListReq_ListOptionStartScene, func(val uint32) error {
			db.Where(DbStartScene, val)
			return nil
		}).
		AddUint32(qmsg.GetAssignChatListReq_ListOptionEndScene, func(val uint32) error {
			db.Where(DbEndScene, val)
			return nil
		}).
		AddUint32(qmsg.GetAssignChatListReq_ListOptionLteCreatedAt, func(val uint32) error {
			db.Where(DbCreatedAt, "<=", val)
			return nil
		}).
		AddUint32(qmsg.GetAssignChatListReq_ListOptionGteCreatedAt, func(val uint32) error {
			db.Where(DbCreatedAt, ">=", val)
			return nil
		}).
		AddString(qmsg.GetAssignChatListReq_ListOptionContactName, func(val string) error {
			hasGetChat = true
			var chatExtIdList []uint64

			offset := uint32(0)
			limit := uint32(2000)
			for {

				accountListRsp, err = qrobot.GetAccountList(ctx, &qrobot.GetAccountListReq{
					ListOption: core.NewListOption().AddOpt(qrobot.GetAccountListReq_ListOptionName, val).
						SetOffset(offset).SetLimit(limit),
				})
				if err != nil {
					log.Errorf("err:%v", err)
					return err
				}
				if len(accountListRsp.List) != 0 {
					chatExtIdList = append(chatExtIdList, utils.PluckUint64(accountListRsp.List, "Id")...)
				}
				if len(accountListRsp.List) < int(limit) {
					break
				}
				offset += limit
			}

			groupListRsp, err = qrobot.GetGroupChatListSys(ctx, &qrobot.GetGroupChatListSysReq{
				ListOption: core.NewListOption().AddOpt(qrobot.GetGroupChatListSysReq_ListOptionName, val),
				CorpId:     corpId,
				AppId:      appId,
				Unscoped:   true,
			})
			if err != nil {
				log.Errorf("err:%v", err)
				return err
			}
			chatExtIdList = append(chatExtIdList, utils.PluckUint64(groupListRsp.List, "Id")...)

			db.WhereIn(DbChatExtId, chatExtIdList)
			return nil
		}).
		AddUint32(qmsg.GetAssignChatListReq_ListOptionLteDeletedAt, func(val uint32) error {
			db.Where(DbDeletedAt, "<=", val)
			return nil
		}).
		AddUint32(qmsg.GetAssignChatListReq_ListOptionGteDeletedAt, func(val uint32) error {
			db.Where(DbDeletedAt, ">=", val)
			return nil
		}).
		AddUint64(qmsg.GetAssignChatListReq_ListOptionOriginUid, func(val uint64) error {
			db.Where(DbOriginUid, val)
			return nil
		}).
		AddUint32(qmsg.GetAssignChatListReq_ListOptionAssignChatType, func(val uint32) error {
			db.Where(DbAssignChatType, val)
			return nil
		}).
		Process()

	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	rsp.Paginate, err = db.FindPaginate(ctx, &assignChatList)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	chatIdList := utils.PluckUint64(assignChatList, "Cid")

	chatList, err = Chat.getListById(ctx, corpId, appId, chatIdList)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	if accountRspMap && len(chatList) > 0 {

		if !hasGetChat {
			accountIdList := utils.PluckUint64(chatList, "ChatExtId")

			accountListRsp, err = qrobot.GetAccountList(ctx, &qrobot.GetAccountListReq{
				ListOption: core.NewListOption().AddOpt(qrobot.GetAccountListReq_ListOptionIdList, accountIdList),
			})
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, err
			}

			groupListRsp, err = qrobot.GetGroupChatListSys(ctx, &qrobot.GetGroupChatListSysReq{
				ListOption: core.NewListOption().
					AddOpt(uint32(qrobot.GetGroupChatListSysReq_ListOptionGroupIdList), accountIdList),
				CorpId: corpId,
				AppId:  appId,
			})
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, err
			}
			extContactFollowMap, err := getExtContactFollowMap(ctx, corpId, appId, chatList, accountListRsp)
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, err
			}
			rsp.ExtContactFollowMap = extContactFollowMap
		}

		accountMap := map[uint64]*qrobot.ModelAccount{}
		utils.KeyBy(accountListRsp.List, "Id", &accountMap)
		rsp.AccountMap = accountMap

		groupMap := map[uint64]*qrobot.ModelGroupChat{}
		utils.KeyBy(groupListRsp.List, "Id", &groupMap)
		rsp.GroupMap = groupMap
	}

	if userRspMap && len(assignChatList) > 0 {
		originUidList := utils.PluckUint64(assignChatList, "OriginUid")
		uidList := append(originUidList, utils.PluckUint64(assignChatList, "Uid")...)

		var userList []*qmsg.ModelUser
		err = User.WhereCorpApp(corpId, appId).WhereIn(DbId, uidList).Find(ctx, &userList)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}

		userMap := map[uint64]*qmsg.ModelUser{}
		utils.KeyBy(userList, "Id", &userMap)
		rsp.UserMap = userMap
	}

	if quanUserRspMap && len(assignChatList) > 0 {
		robotUidList := utils.PluckUint64(assignChatList, "RobotUid")

		listRsp, err := iquan.GetUserList(ctx, &iquan.GetUserListReq{
			ListOption: core.NewListOption().AddOpt(iquan.GetUserListReq_ListOptionUidList, robotUidList),
			CorpId:     corpId,
			AppId:      appId,
		})
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}

		userMap := map[uint64]*quan.ModelUser{}
		utils.KeyBy(listRsp.List, "Id", &userMap)
		rsp.QuanUserMap = userMap
	}

	if lastAssignRspMap && len(assignChatList) > 0 {
		cidList := utils.PluckUint64(assignChatList, "Cid")
		var lastAssignChatList []*qmsg.ModelAssignChat
		err = Db().FilterCorpAndApp(corpId, appId).Unscoped().QueryBySql(ctx, &lastAssignChatList, fmt.Sprintf("SELECT * FROM qmsg_assign_chat AS a RIGHT JOIN (SELECT MAX(created_at) as created_at FROM qmsg_assign_chat WHERE (`corp_id` = ?) AND (`app_id` = ?) AND cid IN (?) AND uid != 0 GROUP BY cid) AS b ON a.created_at = b.created_at AND (a.`corp_id` = ?) AND (a.`app_id` = ?) AND a.cid IN (?) AND a.uid != 0 GROUP BY a.cid ORDER BY NULL"), corpId, appId, cidList, corpId, appId, cidList).Error()
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}

		lastAssignMap := map[uint64]*qmsg.ModelAssignChat{}
		utils.KeyBy(lastAssignChatList, "Cid", &lastAssignMap)
		rsp.LastAssignMap = lastAssignMap

		uidList := utils.PluckUint64(lastAssignChatList, "Uid")
		var userList []*qmsg.ModelUser
		err = User.WhereCorpApp(corpId, appId).WhereIn(DbId, uidList).Find(ctx, &userList)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}

		userMap := map[uint64]*qmsg.ModelUser{}
		utils.KeyBy(userList, "Id", &userMap)
		if rsp.UserMap == nil {
			rsp.UserMap = userMap
		} else {
			for _, m := range userMap {
				rsp.UserMap[m.Id] = m
			}
		}
	}

	if assignChatRspList {
		rsp.AssignChatList = assignChatList
	} else {
		// 默认走这里
		rsp.List = chatList
		assignChatMap := assignChatList2Map(assignChatList, isTrashOnly, isOrderByDesc)
		rsp.AssignChatMap = assignChatMap
	}

	if chatRspMap {
		chatMap := map[uint64]*qmsg.ModelChat{}
		utils.KeyBy(chatList, "Id", &chatMap)
		rsp.ChatMap = chatMap
	}

	return &rsp, nil
}

func getExtContactFollowMap(ctx *rpc.Context, corpId, appId uint32, chatList []*qmsg.ModelChat,
	accountListRsp *qrobot.GetAccountListRsp) (map[string]*quan.ModelExtContactFollow, error) {
	accountIdList := utils.PluckUint64(accountListRsp.List, "ExtUid")
	robotUidList := utils.PluckUint64(chatList, "RobotUid")
	quanSrv := quanService{}
	extContactMap, err := quanSrv.getAccountFollowMap(ctx, corpId, appId, robotUidList, accountIdList)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	return extContactMap, nil
}

// @desc: 会话list转map
func assignChatList2Map(assignChatList []*qmsg.ModelAssignChat, trashOnly bool, isOrderByDesc bool) map[uint64]*qmsg.ModelAssignChat {
	assignChatMap := map[uint64]*qmsg.ModelAssignChat{}
	// 如果查询最近联系人，并且是按时间倒序排序，使用最新assign_chat数据(解决第一次查询最近联系人时，顺序错误问题)
	if trashOnly && isOrderByDesc {
		for _, assignChat := range assignChatList {
			if ac, ok := assignChatMap[assignChat.Cid]; ok {
				// 同一个会话，使用最新的数据
				if assignChat.CreatedAt > ac.CreatedAt {
					assignChatMap[assignChat.Cid] = assignChat
				}
			} else {
				assignChatMap[assignChat.Cid] = assignChat
			}
		}
	} else {
		utils.KeyBy(assignChatList, "Cid", &assignChatMap)
	}
	return assignChatMap
}

func GetAssignChat(ctx *rpc.Context, req *qmsg.GetAssignChatReq) (*qmsg.GetAssignChatRsp, error) {
	var rsp qmsg.GetAssignChatRsp

	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)

	assignChat, err := AssignChat.getByCid(ctx, corpId, appId, req.Cid)
	if err != nil {
		//warning.ReportMsg(ctx, fmt.Sprintf("err: %v", err))
		log.Errorf("err:%v", err)
		return nil, err
	}

	rsp.AssignChat = assignChat

	return &rsp, nil
}

func GetWaitingAssignCount(ctx *rpc.Context, req *qmsg.GetWaitingAssignCountReq) (*qmsg.GetWaitingAssignCountRsp, error) {
	var rsp qmsg.GetWaitingAssignCountRsp
	var err error

	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)

	rsp.Count, err = getWaitingCount(ctx, corpId, appId)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	return &rsp, nil
}

func CloseChatAdmin(ctx *rpc.Context, req *qmsg.CloseChatAdminReq) (*qmsg.CloseChatAdminRsp, error) {
	var rsp qmsg.CloseChatAdminRsp

	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)

	assignChatList, err := deleteAssignChatList(ctx, corpId, appId, req.AssignChatIdList, false)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	if assignChatList != nil && len(assignChatList) > 0 {
		cidList := utils.PluckUint64(assignChatList, "Cid")
		err = removeSendList4Chat(ctx, corpId, appId, cidList)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
		for _, assignChat := range assignChatList {
			err = addCloseTransferMsg(ctx, corpId, appId, assignChat.Id, "后台结束会话")
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, err
			}
		}
	}
	return &rsp, nil
}

func TransferChatAdmin(ctx *rpc.Context, req *qmsg.TransferChatAdminReq) (*qmsg.TransferChatAdminRsp, error) {
	var rsp qmsg.TransferChatAdminRsp

	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)

	err := transferAssignChatList(ctx, corpId, appId, req.Uid, req.AssignChatIdList)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	return &rsp, nil
}

func CreateAssignChat(ctx *rpc.Context, req *qmsg.CreateAssignChatReq) (*qmsg.CreateAssignChatRsp, error) {
	var rsp qmsg.CreateAssignChatRsp

	user, err := getUserCheck(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	config, err := getSessionConfig(ctx, user.CorpId, user.AppId)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	c, err := AssignChat.WhereCorpApp(user.CorpId, user.AppId).Where(DbUid, user.Id).Count(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	if config.AssignSetting.MaxChatNum <= c {
		return nil, rpc.CreateErrorWithMsg(int32(qmsg.ErrCode_ErrUserAssignTopLimit), "当前会话已达上限，请结束其他会话后，再发起此聊天")
	}

	chat, err := Chat.get(ctx, user.CorpId, user.AppId, req.Cid)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	var assignChat qmsg.ModelAssignChat
	err = AssignChat.WhereCorpApp(user.CorpId, user.AppId).Where(DbCid, req.Cid).First(ctx, &assignChat)
	if err != nil {

		if !AssignChat.IsNotFoundErr(err) {
			log.Errorf("err:%v", err)
			return nil, err
		}

		//非资产模式的客服发起操作，机器人归属于任意客服，都不允许操作
		var cnt uint32
		cnt, err = UserRobot.WhereCorpApp(user.CorpId, user.AppId).Where(DbRobotUid, chat.RobotUid).Count(ctx)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
		if cnt > 0 {
			return nil, rpc.CreateError(qmsg.ErrNotHasRobotPerm)
		}

	} else {

		if assignChat.Uid != 0 && user.Id != assignChat.Uid {
			var currentUser qmsg.ModelUser
			err = User.WhereCorpApp(user.CorpId, user.AppId).Where(DbId, assignChat.Uid).First(ctx, &currentUser)
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, err
			}

			return nil, rpc.CreateErrorWithMsg(int32(qmsg.ErrCode_ErrChatAlreadyAssign), fmt.Sprintf("此会话当前由客服%s在处理，你无法发起会话。", currentUser.Username))
		}
	}

	// 如果是排队会话,只修改数据
	if assignChat.Id > 0 && assignChat.Uid == 0 {
		_, err = AssignChat.assignUnAssignedChat(ctx, user.CorpId, user.AppId, user.Id, qmsg.ModelAssignChat_StartSceneRecent, assignChat.Id)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
		assignChat.Uid = user.Id
		assignChat.StartScene = uint32(qmsg.ModelAssignChat_StartSceneRecent)
	} else {
		createdAssignChat, err := AssignChat.create(ctx, user.CorpId, user.AppId, user.Id, 0, chat.RobotUid, chat.ChatExtId, chat.IsGroup, chat, qmsg.ModelAssignChat_StartSceneRecent)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
		assignChat = *createdAssignChat
	}

	err = pushWsMsg(ctx, user.CorpId, user.AppId, user.Id, []*qmsg.WsMsgWrapper{
		{
			MsgType:    uint32(qmsg.WsMsgWrapper_MsgTypeAssignChat),
			AssignChat: &assignChat,
			Chat:       chat,
		},
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	rsp.AssignChat = &assignChat

	return &rsp, nil
}

func StaffTransferChat(ctx *rpc.Context, req *qmsg.TransferChatReq) (*qmsg.TransferChatRsp, error) {
	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)

	var rsp qmsg.TransferChatRsp
	var robotUid, chatExtId uint64
	var isGroup bool
	var chatType uint32

	transferUser, err := User.getUserById(ctx, corpId, appId, req.Uid)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	if transferUser.IsAssign {
		return nil, rpc.InvalidArg("不能转给分配资产的客服")
	}

	if !transferUser.IsOnline {
		return nil, rpc.InvalidArg("该客服已下班")
	}

	chat, err := Chat.get(ctx, corpId, appId, req.Cid)
	if err != nil {
		//warning.ReportMsg(ctx, fmt.Sprintf("err: %v", err))
		log.Errorf("err:%v", err)
		return nil, err
	}

	assignChat, err := AssignChat.getByCid(ctx, corpId, appId, req.Cid)
	if err != nil && !AssignChat.IsNotFoundErr(err) {
		log.Errorf("err:%v", err)
		return nil, err
	}

	if assignChat != nil {
		//warning.ReportMsg(ctx, fmt.Sprintf("err: Assign chat has already assign"))
		return nil, rpc.InvalidArg("智能接待中，请到[会话管理]结束会话")
	}

	robotUid = chat.RobotUid
	chatExtId = chat.ChatExtId
	isGroup = chat.IsGroup
	if isGroup {
		chatType = uint32(qmsg.ChatType_ChatTypeGroup)
	} else {
		chatType = uint32(qmsg.ChatType_ChatTypeSingle)
	}

	transferAssignChat, err := AssignChat.create(ctx, corpId, appId, req.Uid, 0, robotUid, chatExtId, isGroup, nil, qmsg.ModelAssignChat_StartSceneAdmin)
	if err != nil {
		//warning.ReportMsg(ctx, fmt.Sprintf("err: %v", err))
		log.Errorf("err:%v", err)
		return nil, err
	}

	chat.Detail.EntertainType = uint32(qmsg.ModelChat_Detail_Manual)

	_, err = aiEntertainRecord.WhereCorpApp(chat.CorpId, chat.AppId).
		Where(DbRobotUid, chat.RobotUid).
		Where(DbModelChatId, chat.Id).
		Where(DbEndAt, 0).
		Update(ctx, map[string]interface{}{
			DbEndAt: utils.Now(),
		})
	if err != nil {
		log.Errorf("err %v", err)
		return nil, err
	}

	//修改会话状态 ai or 人工
	_, err = Chat.WhereCorpApp(chat.CorpId, chat.AppId).Where(map[string]interface{}{
		DbId: chat.Id,
	}).Update(ctx, map[string]interface{}{
		DbDetail: chat.Detail,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	msgContent := &qmsg.ChatTransferChatRemarkMsg{
		Date:             time.Now().Format("2006年01月02日 15:04"),
		Mark:             req.Mark,
		Username:         "系统",
		TransferUsername: transferUser.Username,
		TransferUserId:   fmt.Sprintf("%d", transferUser.Id),
		TransferType:     uint32(qmsg.ChatTransferChatRemarkMsg_TransferTypeSystem),
	}

	msgContentBuffer, err := json.Marshal(msgContent)
	if err != nil {
		return nil, err
	}

	cliMsgId := "transfer_assign_chat_" + utils.GenRandomStr()

	_, err = sendAssignMsg(ctx, &qmsg.SendMsgReq{
		Msg: &qmsg.ModelMsgBox{
			CorpId:      corpId,
			AppId:       appId,
			Uid:         transferAssignChat.RobotUid,
			CliMsgId:    cliMsgId,
			MsgType:     uint32(qmsg.MsgType_MsgTypeTransferChatRemark),
			ChatType:    chatType,
			ChatId:      transferAssignChat.ChatExtId,
			ChatMsgType: uint32(qmsg.ChatMsgType_ChatMsgTypeTransferChatRemark),
			Msg:         string(msgContentBuffer),
			SentAt:      uint32(time.Now().Unix()),
		},
	}, transferAssignChat, req.Mark)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	transferLog := &qmsg.ModelTransferLog{
		CorpId:       corpId,
		AppId:        appId,
		CliMsgId:     cliMsgId,
		ChatId:       transferAssignChat.ChatExtId,
		Uid:          transferAssignChat.RobotUid,
		AssignChatId: transferAssignChat.Id,
		Mark:         req.Mark,
		ChatType:     chatType,
	}
	_, err = TransferLog.updateOrCreate(ctx, transferLog)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	if req.AssignChatId == 0 {
		var userRobotList []*qmsg.ModelUserRobot
		err = UserRobot.WhereCorpApp(corpId, appId).Where(DbRobotUid, robotUid).Find(ctx, &userRobotList)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}

		for _, userRobot := range userRobotList {
			err = pushWsMsg(ctx, corpId, appId, userRobot.Uid, []*qmsg.WsMsgWrapper{
				{
					MsgType:      uint32(qmsg.WsMsgWrapper_MsgTypeRemoveAssignChat),
					AssignChatId: req.AssignChatId,
					Chat:         &qmsg.ModelChat{Id: req.Cid},
				},
			})
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, err
			}
		}
	}

	return &rsp, nil
}

func ExportAssignChatList(ctx *rpc.Context, req *qmsg.GetAssignChatListReq) (*qmsg.ExportAssignChatListRsp, error) {
	var rsp qmsg.ExportAssignChatListRsp

	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)

	taskKey, err := commonv2.ExportExcel(ctx, "", "", func(ctx *rpc.Context, w *csv.Writer) error {
		var title []string = []string{"会话信息", "所属成员", "会话类型", "接待客服", "发起时间", "发起场景", "结束时间", "结束场景", "转接客服", "状态"}
		err := w.Write(title)
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
		var limit uint32 = 100
		req.ListOption.Offset = 0
		req.ListOption.Limit = limit
		for {
			assignChatList, paginate, err := getAssignChatList(ctx, corpId, appId, req)

			// 数据量防护
			if paginate.Total > 1000 {
				return rpc.InvalidArg("数据量过大，请联系运营导出")
			}
			req.ListOption.SkipCount = true

			if err != nil {
				log.Errorf("err:%v", err)
				return err
			}
			if len(assignChatList) == 0 {
				break
			}
			originUidList := utils.PluckUint64(assignChatList, "TargetUid").Filter(utils.Uint64Gt0)
			uidList := append(originUidList, utils.PluckUint64(assignChatList, "Uid")...)
			userMap, err := getUserMap(ctx, corpId, appId, uidList)
			if err != nil {
				log.Errorf("err:%v", err)
				return err
			}
			robotUidList := utils.PluckUint64(assignChatList, "RobotUid")
			robotUserMap, err := getRobotUserMap(ctx, corpId, appId, robotUidList)
			if err != nil {
				log.Errorf("err:%v", err)
				return err
			}
			chatExtIdList := utils.PluckUint64(assignChatList, "ChatExtId")
			accountMap, err := getAccountMap(ctx, corpId, appId, chatExtIdList)
			if err != nil {
				log.Errorf("err:%v", err)
				return err
			}
			groupMap, err := getGroupMap(ctx, corpId, appId, chatExtIdList)
			if err != nil {
				log.Errorf("err:%v", err)
				return err
			}
			for _, assignChat := range assignChatList {
				// var title []string = []string{"会话信息", "所属成员", "会话类型", "接待客服", "发起时间", "发起场景", "结束时间", "结束场景", "转接客服", "状态"}
				var chatName = ""
				if assignChat.IsGroup {
					if group, ok := groupMap[assignChat.ChatExtId]; ok {
						chatName = group.Name
					}
				} else {
					if account, ok := accountMap[assignChat.ChatExtId]; ok {
						chatName = account.Name
					}
				}
				var robotUserName = ""
				if robotUser, ok := robotUserMap[assignChat.RobotUid]; ok {
					robotUserName = robotUser.WwUserName
				}
				var assignChatType = ""
				switch assignChat.AssignChatType {
				case uint32(qmsg.ModelAssignChat_AssignChatTypeNewFriend):
					assignChatType = "新好友会话"
				case uint32(qmsg.ModelAssignChat_AssignChatTypeOldFriend):
					assignChatType = "旧会话"
				default:
				}
				var userName = ""
				if user, ok := userMap[assignChat.Uid]; ok {
					userName = user.Username
				}
				var createTime = utils.TimeStamp2DateYmdhis(assignChat.CreatedAt)
				var startScene = getStartSceneName(assignChat.StartScene)
				var endScene = getEndSceneName(assignChat.EndScene)
				var closeTime = ""
				if assignChat.DeletedAt > 0 {
					closeTime = utils.TimeStamp2DateYmdhis(assignChat.DeletedAt)
				}
				var targetUserName = ""
				if user, ok := userMap[assignChat.TargetUid]; ok {
					targetUserName = user.Username
				}
				var status = "处理中"
				if assignChat.DeletedAt > 0 {
					status = "已结束"
				}
				var data []string
				data = append(data, chatName, robotUserName, assignChatType, userName, createTime, startScene, closeTime, endScene, targetUserName, status)
				err = w.Write(data)
				if err != nil {
					log.Errorf("err:%v", err)
					return err
				}
			}
			req.ListOption.Offset += limit
		}
		return nil
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	rsp.TaskKey = taskKey

	return &rsp, nil
}

func getAssignChatList(ctx *rpc.Context, corpId, appId uint32, req *qmsg.GetAssignChatListReq) ([]*qmsg.ModelAssignChat, *core.Paginate, error) {
	db := AssignChat.NewList(req.ListOption).WhereCorpApp(corpId, appId)
	var assignChatList []*qmsg.ModelAssignChat
	err := core.NewListOptionProcessor(req.ListOption).
		AddUint64(qmsg.GetAssignChatListReq_ListOptionUid, func(val uint64) error {
			db.Where(DbUid, val)
			return nil
		}).
		AddBool(qmsg.GetAssignChatListReq_ListOptionExceptWaiting, func(val bool) error {
			db.WhereRaw("!(uid = 0 AND deleted_at = 0)")
			return nil
		}).
		AddUint64(qmsg.GetAssignChatListReq_ListOptionRobotUid, func(val uint64) error {
			db.Where(DbRobotUid, val)
			return nil
		}).
		AddUint64List(qmsg.GetAssignChatListReq_ListOptionRobotUidList, func(val []uint64) error {
			db.WhereIn(DbRobotUid, val)
			return nil
		}).
		AddBool(qmsg.GetAssignChatListReq_ListOptionWithTrash, func(val bool) error {
			if val {
				db.Unscoped()
			}
			return nil
		}).
		AddBool(qmsg.GetAssignChatListReq_ListOptionTrashOnly, func(val bool) error {
			if val {
				db.Unscoped().Where(DbDeletedAt, ">", 0)
			} else {
				db.Unscoped().Where(DbDeletedAt, 0)
			}
			return nil
		}).
		AddUint32(qmsg.GetAssignChatListReq_ListOptionStartScene, func(val uint32) error {
			db.Where(DbStartScene, val)
			return nil
		}).
		AddUint32(qmsg.GetAssignChatListReq_ListOptionEndScene, func(val uint32) error {
			db.Where(DbEndScene, val)
			return nil
		}).
		AddUint32(qmsg.GetAssignChatListReq_ListOptionLteCreatedAt, func(val uint32) error {
			db.Where(DbCreatedAt, "<=", val)
			return nil
		}).
		AddUint32(qmsg.GetAssignChatListReq_ListOptionGteCreatedAt, func(val uint32) error {
			db.Where(DbCreatedAt, ">=", val)
			return nil
		}).
		AddString(qmsg.GetAssignChatListReq_ListOptionContactName, func(val string) error {
			var chatExtIdList []uint64
			accountListRsp, err := qrobot.GetAccountList(ctx, &qrobot.GetAccountListReq{
				ListOption: core.NewListOption().AddOpt(qrobot.GetAccountListReq_ListOptionName, val),
			})
			if err != nil {
				log.Errorf("err:%v", err)
				return err
			}
			chatExtIdList = utils.PluckUint64(accountListRsp.List, "Id")
			groupListRsp, err := qrobot.GetGroupChatListSys(ctx, &qrobot.GetGroupChatListSysReq{
				ListOption: core.NewListOption().AddOpt(qrobot.GetGroupChatListSysReq_ListOptionName, val),
				CorpId:     corpId,
				AppId:      appId,
				Unscoped:   true,
			})
			if err != nil {
				log.Errorf("err:%v", err)
				return err
			}
			chatExtIdList = append(chatExtIdList, utils.PluckUint64(groupListRsp.List, "Id")...)
			db.WhereIn(DbChatExtId, chatExtIdList)
			return nil
		}).
		AddUint32(qmsg.GetAssignChatListReq_ListOptionLteDeletedAt, func(val uint32) error {
			db.Where(DbDeletedAt, "<=", val)
			return nil
		}).
		AddUint32(qmsg.GetAssignChatListReq_ListOptionGteDeletedAt, func(val uint32) error {
			db.Where(DbDeletedAt, ">=", val)
			return nil
		}).
		AddUint64(qmsg.GetAssignChatListReq_ListOptionOriginUid, func(val uint64) error {
			db.Where(DbOriginUid, val)
			return nil
		}).
		AddUint32(qmsg.GetAssignChatListReq_ListOptionAssignChatType, func(val uint32) error {
			db.Where(DbAssignChatType, val)
			return nil
		}).
		Process()

	if err != nil {
		log.Errorf("err:%v", err)
		return nil, nil, err
	}

	paginate, err := db.FindPaginate(ctx, &assignChatList)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, nil, err
	}
	return assignChatList, paginate, nil
}

func getRobotUserMap(ctx *rpc.Context, corpId, appId uint32, robotUidList []uint64) (map[uint64]*quan.ModelUser, error) {
	listRsp, err := iquan.GetUserList(ctx, &iquan.GetUserListReq{
		ListOption: core.NewListOption().AddOpt(iquan.GetUserListReq_ListOptionUidList, robotUidList),
		CorpId:     corpId,
		AppId:      appId,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	userMap := map[uint64]*quan.ModelUser{}
	utils.KeyBy(listRsp.List, "Id", &userMap)
	return userMap, nil
}

func getAccountMap(ctx *rpc.Context, corpId, appId uint32, accountIdList []uint64) (map[uint64]*qrobot.ModelAccount, error) {
	accountListRsp, err := qrobot.GetAccountList(ctx, &qrobot.GetAccountListReq{
		ListOption: core.NewListOption().AddOpt(qrobot.GetAccountListReq_ListOptionIdList, accountIdList),
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	accountMap := map[uint64]*qrobot.ModelAccount{}
	utils.KeyBy(accountListRsp.List, "Id", &accountMap)
	return accountMap, nil
}

func getGroupMap(ctx *rpc.Context, corpId, appId uint32, groupIdList []uint64) (map[uint64]*qrobot.ModelGroupChat, error) {
	groupListRsp, err := qrobot.GetGroupChatListSys(ctx, &qrobot.GetGroupChatListSysReq{
		ListOption: core.NewListOption().
			AddOpt(uint32(qrobot.GetGroupChatListSysReq_ListOptionGroupIdList), groupIdList),
		CorpId: corpId,
		AppId:  appId,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	groupMap := map[uint64]*qrobot.ModelGroupChat{}
	utils.KeyBy(groupListRsp.List, "Id", &groupMap)
	return groupMap, nil
}

func getStartSceneName(startScene uint32) string {
	switch startScene {
	case uint32(qmsg.ModelAssignChat_StartSceneSys):
		return "系统自动分配"
	case uint32(qmsg.ModelAssignChat_StartSceneAdmin):
		return "后台手动分配"
	case uint32(qmsg.ModelAssignChat_StartSceneTransfer):
		return "客服转介"
	case uint32(qmsg.ModelAssignChat_StartSceneRecent):
		return "客服主动发起：最近联系"
	default:
		return ""
	}
}

func getEndSceneName(endScene uint32) string {
	switch endScene {
	case uint32(qmsg.ModelAssignChat_EndSceneContactAutoClose):
		return "用户未回复，超时系统自动结束"
	case uint32(qmsg.ModelAssignChat_EndSceneUserAutoClose):
		return "客服未回复，超时系统自动结束"
	case uint32(qmsg.ModelAssignChat_EndSceneClose):
		return "客服结束"
	case uint32(qmsg.ModelAssignChat_EndSceneTransfer):
		return "客服转介"
	case uint32(qmsg.ModelAssignChat_EndSceneAdmin):
		return "后台结束"
	case uint32(qmsg.ModelAssignChat_EndSceneUserExit):
		return "成员账号异常"
	case uint32(qmsg.ModelAssignChat_EndSceneRobotBind):
		return "成员绑定客服"
	case uint32(qmsg.ModelAssignChat_EndSceneAssignChange):
		return "客服权限变更"
	default:
		return ""
	}
}

func ApiTransferChat(ctx *rpc.Context, req *qmsg.ApiTransferChatReq) (*qmsg.ApiTransferChatRsp, error) {
	var rsp qmsg.ApiTransferChatRsp

	corpId, appId := req.CorpId, req.AppId
	transferUser, err := User.getUserById(ctx, corpId, appId, req.Uid)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	if transferUser.IsAssign {
		return nil, rpc.InvalidArg("不能转给分配资产的客服")
	}

	if !transferUser.IsOnline {
		return nil, rpc.InvalidArg("该客服已下班")
	}

	if transferUser.IsDisable {
		return nil, rpc.InvalidArg("该客服已禁用")
	}

	chat, err := Chat.getByRobotAndExt(ctx, corpId, appId, req.RobotUid, req.ExtAccountId, req.IsTransferGroup)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	if req.IsMarkChatUnread {
		if chat.UnreadCount == 0 {
			// 如果标为未读则判断当前会话是否有未读数，如果没有则加1
			_, err = Chat.updateMap(ctx, corpId, appId, chat.Id, map[string]interface{}{
				DbUnreadCount: 1,
			})
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, err
			}
			chat.UnreadCount = chat.UnreadCount + 1
		}
	}

	assignChat, err := AssignChat.getByCid(ctx, corpId, appId, chat.Id)
	if err != nil && !AssignChat.IsNotFoundErr(err) {
		log.Errorf("err:%v", err)
		return nil, err
	}

	uid := req.Uid
	if assignChat != nil && assignChat.Id > 0 {
		//已分配
		if assignChat.Uid == req.Uid {
			return &rsp, nil
		}

		uid = assignChat.Uid

		err = AssignChat.setClosed(ctx, corpId, appId, map[string]interface{}{
			DbEndScene: uint32(qmsg.ModelAssignChat_EndSceneAdmin),
		}, []uint64{assignChat.Id}...)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}

		if assignChat.Uid > 0 {
			// 加入最近联系人列表
			_, err = LastContactChat.createOrUpdateByAssignChatId(ctx, corpId, appId, assignChat.Id)
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, err
			}
		}

		err = pushWsMsg(ctx, corpId, appId, assignChat.Uid, []*qmsg.WsMsgWrapper{
			{
				MsgType:       uint32(qmsg.WsMsgWrapper_MsgTypeRemoveAssignChat),
				AssignChat:    assignChat,
				AssignChatId:  assignChat.Id,
				IsApiTransfer: true,
			},
		})
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
	}

	newAssign, err := AssignChat.create(ctx, corpId, appId, req.Uid, uid, req.RobotUid, req.ExtAccountId, req.IsTransferGroup, nil, qmsg.ModelAssignChat_StartSceneAdmin)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	var chatType uint32
	if chat.IsGroup {
		chatType = uint32(qmsg.ChatType_ChatTypeGroup)
	} else {
		chatType = uint32(qmsg.ChatType_ChatTypeSingle)
	}

	var transferType uint32
	var userName string
	var userId uint64
	if req.Uid == uid {
		transferType = uint32(qmsg.ChatTransferChatRemarkMsg_TransferTypeSystem) //系统自动分配
		userName = "系统"
	} else {
		transferType = uint32(qmsg.ChatTransferChatRemarkMsg_TransferTypeNoAsset) // 无资产客服转介
		var originUser *qmsg.ModelUser
		originUser, err = User.getUserById(ctx, corpId, appId, uid)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
		userName = originUser.Username
		userId = originUser.Id
	}

	msgContent := &qmsg.ChatTransferChatRemarkMsg{
		Date:             time.Now().Format("2006年01月02日 15:04"),
		Mark:             req.Mark,
		Username:         userName,
		UserId:           fmt.Sprintf("%d", userId),
		TransferUsername: transferUser.Username,
		TransferUserId:   fmt.Sprintf("%d", transferUser.Id),
		TransferType:     transferType,
	}

	msgContentBuffer, err := json.Marshal(msgContent)
	if err != nil {
		return nil, err
	}
	cliMsgId := "transfer_assign_chat_" + utils.GenRandomStr()
	msg := &qmsg.ModelMsgBox{
		CorpId:      corpId,
		AppId:       appId,
		Uid:         newAssign.RobotUid,
		CliMsgId:    cliMsgId,
		MsgType:     uint32(qmsg.MsgType_MsgTypeTransferChatRemark),
		ChatType:    chatType,
		ChatId:      newAssign.ChatExtId,
		ChatMsgType: uint32(qmsg.ChatMsgType_ChatMsgTypeTransferChatRemark),
		Msg:         string(msgContentBuffer),
		SentAt:      uint32(time.Now().Unix()),
	}
	chat, _, err = addMsg(ctx, msg, true)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	transferLog := &qmsg.ModelTransferLog{
		CorpId:       corpId,
		AppId:        appId,
		CliMsgId:     cliMsgId,
		ChatId:       newAssign.ChatExtId,
		Uid:          newAssign.RobotUid,
		AssignChatId: newAssign.Id,
		Mark:         req.Mark,
		ChatType:     chatType,
	}
	_, err = TransferLog.updateOrCreate(ctx, transferLog)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	wrapper := &qmsg.WsMsgWrapper{
		MsgType:       uint32(qmsg.WsMsgWrapper_MsgTypeAssignChat),
		MsgBox:        msg,
		Chat:          chat,
		AssignChat:    newAssign,
		IsApiTransfer: true,
	}

	err = pushWsMsgByRobotUid(ctx, chat.CorpId, chat.AppId, chat.RobotUid, []*qmsg.WsMsgWrapper{wrapper})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	err = pushWsMsg(ctx, corpId, appId, newAssign.Uid, []*qmsg.WsMsgWrapper{
		{
			MsgType:       uint32(qmsg.WsMsgWrapper_MsgTypeAssignChat),
			AssignChat:    newAssign,
			Chat:          chat,
			IsApiTransfer: true,
			MsgBox:        msg,
		},
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	return &rsp, nil
}

func CloseChat4Api(ctx *rpc.Context, req *qmsg.CloseChat4ApiReq) (*qmsg.CloseChat4ApiRsp, error) {
	var rsp qmsg.CloseChat4ApiRsp

	corpId, appId := req.CorpId, req.AppId
	transferUser, err := User.getUserById(ctx, corpId, appId, req.Uid)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	chat, err := Chat.getByRobotAndExt(ctx, corpId, appId, req.RobotUid, req.ExtAccountId, req.IsTransferGroup)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	assignChat, err := AssignChat.getByCid(ctx, corpId, appId, chat.Id)
	if err != nil && !AssignChat.IsNotFoundErr(err) {
		log.Errorf("err:%v", err)
		return nil, err
	}

	if assignChat == nil {
		return nil, rpc.CreateError(qmsg.ErrAssignChatNotFound)
	}

	if assignChat.Uid != transferUser.Id {
		return nil, rpc.CreateError(qmsg.ErrNotHasRobotPerm)
	}

	if req.IsMarkChatUnread {
		if chat.UnreadCount == 0 {
			// 如果标为未读则判断当前会话是否有未读数，如果没有则加1
			_, err = Chat.updateMap(ctx, corpId, appId, chat.Id, map[string]interface{}{
				DbUnreadCount: 1,
			})
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, err
			}
		}
	}

	assignChatList, err := deleteAssignChatList(ctx, corpId, appId, []uint64{assignChat.Id}, req.IsMarkChatUnread)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	if assignChatList != nil && len(assignChatList) > 0 {
		cidList := utils.PluckUint64(assignChatList, "Cid")
		err = removeSendList4Chat(ctx, corpId, appId, cidList)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}

		for _, assign := range assignChatList {
			err = addCloseTransferMsg(ctx, corpId, appId, assign.Id, req.Mark)
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, err
			}
		}
	}
	return &rsp, nil
}

func SetReply4Api(ctx *rpc.Context, req *qmsg.SetReply4ApiReq) (*qmsg.SetReply4ApiRsp, error) {
	var rsp qmsg.SetReply4ApiRsp

	corpId, appId := req.CorpId, req.AppId

	var chat qmsg.ModelChat
	err := Chat.WhereCorpApp(corpId, appId).Where(DbRobotUid, req.RobotUid).Where(DbIsGroup, false).Where(DbChatExtId, req.ExtAccountId).First(ctx, &chat)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	if chat.SenderAccountId == req.ExtAccountId {
		return nil, rpc.CreateError(qmsg.ErrUserLastSendNotRobot)
	}

	_, err = Chat.WhereCorpApp(corpId, appId).Where(DbRobotUid, req.RobotUid).Where(DbIsGroup, false).
		Where(DbChatExtId, req.ExtAccountId).Update(ctx, map[string]interface{}{
		DbIsReply: true,
	})

	if err != nil {
		log.Errorf("err %v", err)
		return nil, err
	}

	chat.IsReply = true

	// 非智能助手处理
	wrapperList := []*qmsg.WsMsgWrapper{
		{
			MsgType: uint32(qmsg.WsMsgWrapper_MsgTypeAlreadyReply),
			Chat:    &chat,
		},
	}

	//里面会查assignChat
	err = pushWsMsgByChatV2(ctx, &chat, nil, wrapperList)
	if err != nil {
		log.Errorf("err %v", err)
	}

	return &rsp, nil
}
