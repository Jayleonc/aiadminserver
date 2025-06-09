package impl

import (
	"fmt"
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/brick/utils"
	"git.pinquest.cn/qlb/qmsg"
)

const getUserListByMsgIdListPrefix = "qmsg_get_user_list_by_msg_id_list"
const getUserListForAccountPrefix = "qmsg_get_user_list_for_account"

func GetUserListByMsgIdList(ctx *rpc.Context, req *qmsg.GetUserListByMsgIdListReq) (*qmsg.GetUserListByMsgIdListRsp, error) {
	var rsp qmsg.GetUserListByMsgIdListRsp
	corpId := req.CorpId
	appId := req.AppId

	var (
		msgRecordList []*qmsg.ModelSendRecord
		userList      []*qmsg.ModelUser
	)

	var err error
	// 限频
	//if rpc.Meta.IsReleaseRole() {
	//	err = FreqLimit(ctx, fmt.Sprintf("%s_%d", getUserListByMsgIdListPrefix, corpId), 10)
	//	if err != nil {
	//		log.Errorf("err: %v", err)
	//		return nil, rpc.CreateErrorWithMsg(-1, err.Error())
	//	}
	//}

	// 取消息对应的客服列表
	db := SendRecord.WhereCorpApp(corpId, appId).
		Select(DbUid, DbYcMsgId, DbRobotUid, DbMsgBoxId).
		WhereIn(DbYcMsgId, req.MsgIdList)

	if req.RobotUid > 0 {
		db = db.Where(DbRobotUid, req.RobotUid)
	}
	err = db.Find(ctx, &msgRecordList)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	uidList := utils.PluckUint64(msgRecordList, "Uid")

	// 取客服信息
	err = User.WhereCorpApp(corpId, appId).
		Select(DbUsername, DbId, DbLoginId, DbStrOpenId).
		WhereIn(DbId, uidList).
		Find(ctx, &userList)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	userIdMap := utils.KeyByV2(userList, "Id").(map[uint64]*qmsg.ModelUser)

	msgBoxIdList := utils.PluckUint64(msgRecordList, "MsgBoxId")

	var msgBoxList []*qmsg.ModelMsgBox
	err = ChoiceMsgBoxDb(MsgBox.WhereCorpApp(corpId, appId), corpId).WhereIn(DbId, msgBoxIdList).Find(ctx, &msgBoxList)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	recordMap := utils.KeyByV2(msgRecordList, "MsgBoxId").(map[uint64]*qmsg.ModelSendRecord)

	msgIdUserMap := make(map[string]string)
	for _, msg := range msgBoxList {
		rec, ok := recordMap[msg.Id]
		if !ok {
			continue
		}

		user, isOk := userIdMap[rec.Uid]
		if !isOk {
			continue
		}

		tmp := &qmsg.GetUserListByMsgIdListRsp_MsgInfo{
			MsgId:     rec.YcMsgId,
			RobotUid:  rec.RobotUid,
			UserName:  user.Username,
			LoginId:   user.LoginId,
			StrOpenId: user.StrOpenId,
		}

		switch qmsg.ChatType(msg.ChatType) {
		case qmsg.ChatType_ChatTypeGroup:
			tmp.GroupId = msg.ChatId
		case qmsg.ChatType_ChatTypeSingle:
			tmp.ExtAccountId = msg.ChatId
		default:
			continue

		}

		msgIdUserMap[rec.YcMsgId] = user.Username
		rsp.List = append(rsp.List, tmp)
	}
	//
	//for _, rec := range msgRecordList {
	//	user, isOk := userIdMap[rec.Uid]
	//	if !isOk {
	//		continue
	//	}
	//
	//	msgIdUserMap[rec.YcMsgId] = user.Username
	//	rsp.List = append(rsp.List, &qmsg.GetUserListByMsgIdListRsp_MsgInfo{
	//		MsgId:    rec.YcMsgId,
	//		RobotUid: rec.RobotUid,
	//		UserName: user.Username,
	//		LoginId:  user.LoginId,
	//	})
	//}
	rsp.MsgIdUserMap = msgIdUserMap
	return &rsp, nil
}

func GetUserListForAccount(ctx *rpc.Context, req *qmsg.GetUserListForAccountReq) (*qmsg.GetUserListForAccountRsp, error) {
	var rsp qmsg.GetUserListForAccountRsp
	corpId := req.CorpId
	appId := req.AppId

	var (
		assignChatList []*qmsg.ModelAssignChat
		userList       []*qmsg.ModelUser
	)
	// 限频
	err := FreqLimit(ctx, fmt.Sprintf("%s_%d", getUserListForAccountPrefix, corpId), 10)
	if err != nil {
		log.Errorf("err: %v", err)
		return nil, err
	}

	// 无资产/有资产但有转介的数据
	err = AssignChat.WhereCorpApp(corpId, appId).
		Select(DbUid).
		Where(DbRobotUid, req.RobotUid).
		Where(DbChatExtId, req.AccountId).
		Where(DbIsGroup, false).
		Where(DbEndScene, 0). // 场景还没结束
		Find(ctx, &assignChatList)
	if err != nil {
		log.Errorf("err: %v", err)
		return nil, err
	}
	uidList := utils.PluckUint64(assignChatList, "Uid")

	if len(uidList) > 0 {
		// 取客服信息
		err = User.WhereCorpApp(corpId, appId).
			Select(DbUsername, DbId, DbLoginId, DbStrOpenId).
			WhereIn(DbId, uidList).
			Find(ctx, &userList)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
		usernameList := utils.PluckString(userList, "Username")
		rsp.UserNameList = usernameList
		rsp.UserList = userList
		return &rsp, nil
	}

	// 到这里表示没有分配记录，有资产的去ModelUserRobot取资产绑定信息
	var userRobotList []*qmsg.ModelUserRobot
	err = UserRobot.WhereCorpApp(corpId, appId).
		Select(DbUid).
		Where(DbRobotUid, req.RobotUid).Find(ctx,
		&userRobotList)
	if err != nil {
		log.Errorf("err: %v", err)
		return nil, err
	}
	uidList = utils.PluckUint64(userRobotList, "Uid")
	if len(uidList) > 0 {
		// 取客服信息
		err = User.WhereCorpApp(corpId, appId).
			Select(DbUsername, DbId, DbLoginId, DbStrOpenId).
			WhereIn(DbId, uidList).
			Find(ctx, &userList)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
		usernameList := utils.PluckString(userList, "Username")
		rsp.UserNameList = usernameList
		rsp.UserList = userList
		return &rsp, nil
	}
	return &rsp, nil
}
