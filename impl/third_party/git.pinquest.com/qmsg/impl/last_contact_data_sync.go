package impl

import (
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/qmsg"
)

func SyncLastContact(ctx *rpc.Context, startId uint64, limit uint32) error {
	for {
		log.Infof("sync batch: startId:%d,limit:%d", startId, limit)
		assignChatList, err := getSyncAssignChatList(ctx, startId, limit)
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
		if len(assignChatList) == 0 {
			break
		}
		for _, assignChat := range assignChatList {
			err = sync2LastContactChat(ctx, assignChat)
			if err != nil {
				log.Errorf("err:%v", err)
				return err
			}
		}
		startId = assignChatList[len(assignChatList)-1].Id
	}
	log.Infof("sync end.")
	return nil
}

func getSyncAssignChatList(ctx *rpc.Context, startId uint64, limit uint32) ([]*qmsg.ModelAssignChat,
	error) {
	var assignChatList []*qmsg.ModelAssignChat
	_, err := AssignChat.WithTrash().
		Where(DbId, ">", startId).
		Where(DbUid, ">", 0).
		Where(DbDeletedAt, ">", 0).
		SetLimit(limit).
		FindPaginate(ctx, &assignChatList)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	return assignChatList, nil
}

func sync2LastContactChat(ctx *rpc.Context, assignChat *qmsg.ModelAssignChat) error {
	// 查询处理中的会话
	openAssignChat, err := AssignChat.getByCid(ctx, assignChat.CorpId, assignChat.AppId, assignChat.Cid)
	if err != nil && !AssignChat.IsNotFoundErr(err) {
		log.Errorf("err:%v", err)
		return err
	}
	// 有正在处理中的会话，则不需要保存到最近联系人
	if openAssignChat != nil {
		return nil
	}
	_, err = LastContactChat.createOrUpdate(ctx, assignChat)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	return nil
}
