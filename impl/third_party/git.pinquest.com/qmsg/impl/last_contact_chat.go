package impl

import (
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/dbx"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/brick/utils"
	"git.pinquest.cn/qlb/core"
	"git.pinquest.cn/qlb/iquan"
	"git.pinquest.cn/qlb/qmsg"
	"git.pinquest.cn/qlb/qrobot"
	"git.pinquest.cn/qlb/quan"
)

func (_ *TLastContactChat) delete(ctx *rpc.Context, corpId, appId uint32, uid, cid uint64) (dbx.DeleteResult, error) {
	res, err := LastContactChat.WhereCorpApp(corpId, appId).Where(DbUid, uid).Where(DbCid, cid).Delete(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return res, err
	}
	return res, nil
}

func (_ *TLastContactChat) deleteByCidList(ctx *rpc.Context, corpId, appId uint32, uid uint64,
	cidList []uint64) (dbx.DeleteResult,
	error) {
	res, err := LastContactChat.WhereCorpApp(corpId, appId).Where(DbUid, uid).WhereIn(DbCid, cidList).Delete(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return res, err
	}
	return res, nil
}

func (_ *TLastContactChat) get(ctx *rpc.Context, corpId, appId uint32, uid, cid uint64) (*qmsg.ModelLastContactChat, error) {
	var lastContactChat qmsg.ModelLastContactChat
	err := LastContactChat.WhereCorpApp(corpId, appId).Where(DbUid, uid).Where(DbCid, cid).First(ctx, &lastContactChat)
	if err != nil {
		if LastContactChat.IsNotFoundErr(err) {
			return nil, nil
		}
		log.Errorf("err:%v", err)
		return nil, err
	}
	return &lastContactChat, nil
}

// @desc: 批量创建最近联系人记录
func (p *TLastContactChat) batchCreateOrUpdate(ctx *rpc.Context, corpId, appId uint32, assignChatIdList []uint64) error {
	if len(assignChatIdList) == 0 {
		return nil
	}
	assignChatList, err := AssignChat.getListWithTrash(ctx, corpId, appId, assignChatIdList)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	for _, assignChat := range assignChatList {
		if assignChat.DeletedAt == 0 {
			continue
		}
		_, err := p.createOrUpdate(ctx, assignChat)
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
	}
	return nil
}

func (p *TLastContactChat) createOrUpdateByAssignChatId(ctx *rpc.Context, corpId, appId uint32, assignChatId uint64) (*qmsg.ModelLastContactChat, error) {
	assignChat, err := AssignChat.getWithTrash(ctx, corpId, appId, assignChatId)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	return p.createOrUpdate(ctx, assignChat)
}

func (_ *TLastContactChat) createOrUpdate(ctx *rpc.Context, assignChat *qmsg.ModelAssignChat) (*qmsg.ModelLastContactChat, error) {
	if assignChat.Uid == 0 {
		log.Warnf("warn: 待分配的会话-%v", assignChat)
		return nil, nil
	}
	if assignChat.DeletedAt == 0 {
		log.Errorf("err: 正在处理中的会话,不能写入最近联系人, cid:%v", assignChat)
		return nil, rpc.CreateError(qmsg.ErrAssignChatProcessing)
	}
	lastContactChat, err := LastContactChat.get(ctx, assignChat.CorpId, assignChat.AppId, assignChat.Uid, assignChat.Cid)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	if lastContactChat == nil {
		lastContactChat = &qmsg.ModelLastContactChat{
			CorpId:           assignChat.CorpId,
			AppId:            assignChat.AppId,
			Uid:              assignChat.Uid,
			Cid:              assignChat.Cid,
			LastAssignChatId: assignChat.Id,
			LastClosedAt:     assignChat.DeletedAt,
		}
		err := LastContactChat.Create(ctx, lastContactChat)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
	} else {
		if lastContactChat.LastClosedAt > assignChat.DeletedAt {
			log.Infof("当前会话关闭时间小于最近关闭时间: lastContactChat:%v, assignChat:%v", lastContactChat, assignChat)
			return nil, nil
		}
		lastContactChat.LastAssignChatId = assignChat.Id
		lastContactChat.LastClosedAt = assignChat.DeletedAt
		_, err := LastContactChat.WhereCorpApp(assignChat.CorpId, assignChat.AppId).Where(DbUid,
			assignChat.Uid).Where(DbCid,
			assignChat.Cid).Update(ctx, map[string]interface{}{
			DbLastAssignChatId: assignChat.Id,
			DbLastClosedAt:     assignChat.DeletedAt,
		})
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
	}
	return lastContactChat, nil
}

func (_ *TLastContactChat) getList(ctx *rpc.Context, lastContactChat *qmsg.ModelLastContactChat) error {
	err := LastContactChat.Create(ctx, lastContactChat)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	return nil
}

func GetLastContactChatList(ctx *rpc.Context, req *qmsg.GetLastContactChatListReq) (*qmsg.GetLastContactChatListRsp, error) {
	var rsp qmsg.GetLastContactChatListRsp
	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)
	db := LastContactChat.WithTrash().NewList(req.ListOption).WhereCorpApp(corpId, appId)
	err := core.NewListOptionProcessor(req.ListOption).
		AddUint64(qmsg.GetLastContactChatListReq_ListOptionUid, func(val uint64) error {
			db.Where(DbUid, val)
			return nil
		}).
		AddUint64(qmsg.GetLastContactChatListReq_ListOptionCid, func(val uint64) error {
			db.Where(DbCid, val)
			return nil
		}).
		AddUint32(qmsg.GetLastContactChatListReq_ListOptionLteClosedAt, func(val uint32) error {
			db.Where(DbLastClosedAt, "<=", val)
			return nil
		}).
		AddUint32(qmsg.GetLastContactChatListReq_ListOptionGteClosedAt, func(val uint32) error {
			db.Where(DbLastClosedAt, ">=", val)
			return nil
		}).
		AddUint32(qmsg.GetLastContactChatListReq_ListOptionOrderBy, func(val uint32) error {
			switch val {
			case uint32(qmsg.GetLastContactChatListReq_OrderByClosedAtDesc):
				db.OrderDesc(DbLastClosedAt)
			case uint32(qmsg.GetLastContactChatListReq_OrderByClosedAtAsc):
				db.OrderAsc(DbLastClosedAt)
			}
			return nil
		}).
		Process()

	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	var lastContactChatList []*qmsg.ModelLastContactChat
	rsp.Paginate, err = db.FindPaginate(ctx, &lastContactChatList)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	if len(lastContactChatList) == 0 {
		return &rsp, nil
	}

	err = fillChatInfo(ctx, corpId, appId, &rsp, lastContactChatList)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	err = fillAccountInfo(ctx, corpId, appId, &rsp)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	err = fillGroupMap(ctx, corpId, appId, &rsp)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	uidList := utils.PluckUint64(lastContactChatList, "Uid")
	err = fillUserMap(ctx, corpId, appId, &rsp, uidList)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	return &rsp, nil
}

// @desc: 填充会话分配信息(qmsg_assign_chat、qmsg_chat)
func fillChatInfo(ctx *rpc.Context, corpId, appId uint32, rsp *qmsg.GetLastContactChatListRsp,
	lastContactChatList []*qmsg.ModelLastContactChat) error {
	lastAssignChatIdList := utils.PluckUint64(lastContactChatList, "LastAssignChatId")
	assignChatList, err := AssignChat.getListWithTrash(ctx, corpId, appId, lastAssignChatIdList)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	rsp.AssignChatMap = map[uint64]*qmsg.ModelAssignChat{}
	utils.KeyBy(assignChatList, "Cid", &rsp.AssignChatMap)

	chatIdList := utils.PluckUint64(assignChatList, "Cid")
	err = fillChatList(ctx, corpId, appId, rsp, chatIdList)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	return nil
}

// @desc: 填充会话信息(qmsg_chat)
func fillChatList(ctx *rpc.Context, corpId, appId uint32, rsp *qmsg.GetLastContactChatListRsp, chatIdList []uint64) error {
	var err error
	rsp.List, err = Chat.getListById(ctx, corpId, appId, chatIdList)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	return nil
}

// @desc: 填充客服信息(qmsg_user)
func fillUserMap(ctx *rpc.Context, corpId, appId uint32, rsp *qmsg.GetLastContactChatListRsp, uidList []uint64) error {
	var userList []*qmsg.ModelUser
	err := User.WhereCorpApp(corpId, appId).WhereIn(DbId, uidList).Find(ctx, &userList)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	rsp.UserMap = map[uint64]*qmsg.ModelUser{}
	utils.KeyBy(userList, "Id", &rsp.UserMap)
	return nil
}

// @desc: 填充联系人好友信息(quan_ext_contact_follow)
func fillExtContactFollowMap(ctx *rpc.Context, corpId, appId uint32, rsp *qmsg.GetLastContactChatListRsp,
	accountList []*qrobot.ModelAccount) error {
	accountMap := map[uint64]*qrobot.ModelAccount{}
	utils.KeyBy(accountList, "ExtUid", &accountMap)
	accountIdList := utils.PluckUint64(accountList, "ExtUid")
	var followListRsp *iquan.GetExtContactFollowListRsp
	robotUidList := utils.PluckUint64(rsp.List, "RobotUid")
	followListRsp, err := iquan.GetExtContactFollowList(ctx, &iquan.GetExtContactFollowListReq{
		ListOption: core.NewListOption().
			AddOpt(iquan.GetExtContactFollowListReq_ListOptionExtUidList, accountIdList).
			AddOpt(iquan.GetExtContactFollowListReq_ListOptionUidList, robotUidList),
		CorpId: corpId,
		AppId:  appId,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	rsp.ExtContactFollowMap = make(map[string]*quan.ModelExtContactFollow)
	if followListRsp.List != nil {
		for _, follow := range followListRsp.List {
			if qrobotAccount, ok := accountMap[follow.ExtUid]; ok {
				key := utils.JoinUint64([]uint64{follow.Uid, qrobotAccount.Id}, "_")
				rsp.ExtContactFollowMap[key] = follow
			}
		}
	}
	return nil
}

// 填充账户信息(qrobot_account)
func fillAccountInfo(ctx *rpc.Context, corpId, appId uint32, rsp *qmsg.GetLastContactChatListRsp) error {
	accountChatList := qmsg.ChatList(rsp.List).Filter(func(chat *qmsg.ModelChat) bool {
		return !chat.IsGroup
	})
	if len(accountChatList) == 0 {
		return nil
	}
	accountIdList := utils.PluckUint64(accountChatList, "ChatExtId")

	accountListRsp, err := qrobot.GetAccountList(ctx, &qrobot.GetAccountListReq{
		ListOption: core.NewListOption().AddOpt(qrobot.GetAccountListReq_ListOptionIdList, accountIdList),
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	if accountListRsp == nil || len(accountListRsp.List) == 0 {
		return nil
	}
	rsp.AccountMap = map[uint64]*qrobot.ModelAccount{}
	utils.KeyBy(accountListRsp.List, "Id", &rsp.AccountMap)

	err = fillExtContactFollowMap(ctx, corpId, appId, rsp, accountListRsp.List)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	return nil
}

// @desc: 填充群组信息(qrobot_group_chat)
func fillGroupMap(ctx *rpc.Context, corpId, appId uint32, rsp *qmsg.GetLastContactChatListRsp) error {
	groupChatList := qmsg.ChatList(rsp.List).Filter(func(chat *qmsg.ModelChat) bool {
		return chat.IsGroup
	})
	if len(groupChatList) == 0 {
		return nil
	}
	groupIdList := utils.PluckUint64(groupChatList, "ChatExtId")
	groupListRsp, err := qrobot.GetGroupChatListSys(ctx, &qrobot.GetGroupChatListSysReq{
		ListOption: core.NewListOption().
			AddOpt(uint32(qrobot.GetGroupChatListSysReq_ListOptionGroupIdList), groupIdList),
		CorpId: corpId,
		AppId:  appId,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	if groupListRsp == nil || len(groupListRsp.List) == 0 {
		return nil
	}
	rsp.GroupMap = map[uint64]*qrobot.ModelGroupChat{}
	utils.KeyBy(groupListRsp.List, "Id", &rsp.GroupMap)
	return nil
}
