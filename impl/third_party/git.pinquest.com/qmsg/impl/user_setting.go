package impl

import (
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/qmsg"
)

func SetChatDisplaySort(ctx *rpc.Context, req *qmsg.SetChatDisplaySortReq) (*qmsg.SetChatDisplaySortRsp, error) {
	var rsp qmsg.SetChatDisplaySortRsp

	u, err := getUserCheck(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	// 兼容旧账号设置配置
	if req.ChatFirstMsgReadType < uint32(qmsg.ModelUser_ChatFirstMsgReadTypeNotRead) {
		req.ChatFirstMsgReadType = uint32(qmsg.ModelUser_ChatFirstMsgReadTypeNotRead)
	}

	_, err = User.WhereCorpApp(u.CorpId, u.AppId).Where(DbId, u.Id).Update(ctx, map[string]interface{}{
		DbChatDisplaySort:      req.ChatDisplaySort,
		DbChatFirstMsgReadType: req.ChatFirstMsgReadType,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	return &rsp, nil
}

// 获取用户配置：成为好友后第一条消息是否不提醒
func getUserSettingChatFirstMsgReadTypeForUserIdList(ctx *rpc.Context, corpId, appId uint32, userIdList []uint64) (map[uint64]uint32, error) {
	chatFirstMsgReadTypeMap := map[uint64]uint32{}
	var userList []*qmsg.ModelUser
	err := User.WhereCorpApp(corpId, appId).
		Select(DbId, DbChatFirstMsgReadType).
		WhereIn(DbId, userIdList).
		Find(ctx, &userList)
	if err != nil {
		log.Errorf("err:%v", err)
		return chatFirstMsgReadTypeMap, err
	}
	for _, user := range userList {
		chatFirstMsgReadTypeMap[user.Id] = user.ChatFirstMsgReadType
	}
	return chatFirstMsgReadTypeMap, nil
}
