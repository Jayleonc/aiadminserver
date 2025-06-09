package impl

import (
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/dbx"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/brick/utils"
	"git.pinquest.cn/qlb/core"
	"git.pinquest.cn/qlb/qmsg"
	"git.pinquest.cn/qlb/qrobot"
)

var UserTag = dbx.NewModel(&dbx.ModelConfig{
	Type:            &qmsg.ModelUserTag{},
	NotFoundErrCode: qmsg.ErrUserTagNotFound,
})

var UserTagRelation = dbx.NewModel(&dbx.ModelConfig{
	Type:            &qmsg.ModelUserTagRelation{},
	NotFoundErrCode: qmsg.ErrUserTagRelationNotFound,
})

const TagLimit uint32 = 100

func AddUserTag(ctx *rpc.Context, req *qmsg.AddUserTagReq) (*qmsg.AddUserTagRsp, error) {
	var rsp qmsg.AddUserTagRsp

	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)
	user, err := getUserCheck(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	cnt, err := UserTag.WhereCorpApp(corpId, appId).Where(DbUid, user.Id).Count(ctx)
	if err != nil {
		log.Errorf("err %v", err)
		return nil, err
	}

	if cnt >= TagLimit {
		return nil, rpc.CreateError(qmsg.ErrUserTagOverLimit)
	}

	var tagInfo qmsg.ModelUserTag
	err = UserTag.WhereCorpApp(corpId, appId).Unscoped().Where(DbUid, user.Id).Where(DbTagName, req.TagName).First(ctx, &tagInfo)
	if err != nil && !UserTag.IsNotFoundErr(err) {
		log.Errorf("err %v", err)
		return nil, err
	}

	if tagInfo.Id > 0 {
		if tagInfo.DeletedAt == 0 {
			return nil, rpc.CreateError(qmsg.ErrTagNameExist)
		} else {
			_, err = UserTag.WhereCorpApp(corpId, appId).Unscoped().Where(DbUid, user.Id).Where(DbTagName, req.TagName).Update(ctx, map[string]interface{}{
				DbDeletedAt: 0,
			})
			if err != nil {
				log.Errorf("err %v", err)
				return nil, err
			}

			rsp.TagName = req.TagName
			rsp.TagId = tagInfo.Id

			return &rsp, nil
		}
	}

	newTag := &qmsg.ModelUserTag{
		CorpId:  corpId,
		AppId:   appId,
		Uid:     user.Id,
		TagName: req.TagName,
	}

	err = UserTag.WhereCorpApp(corpId, appId).Create(ctx, newTag)
	if err != nil {
		log.Errorf("err %v", err)
		return nil, err
	}

	rsp.TagName = req.TagName
	rsp.TagId = newTag.Id

	return &rsp, nil
}

func SetUserTag(ctx *rpc.Context, req *qmsg.SetUserTagReq) (*qmsg.SetUserTagRsp, error) {
	var rsp qmsg.SetUserTagRsp

	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)
	user, err := getUserCheck(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	cnt, err := UserTag.WhereCorpApp(corpId, appId).Where(DbUid, user.Id).Where(DbTagName, req.TagName).Count(ctx)
	if err != nil {
		log.Errorf("err %v", err)
		return nil, err
	}

	if cnt > 0 {
		return nil, rpc.CreateError(qmsg.ErrTagNameExist)
	}

	row, err := UserTag.WhereCorpApp(corpId, appId).Where(DbUid, user.Id).Where(DbId, req.TagId).Update(ctx, map[string]interface{}{
		DbTagName: req.TagName,
	})
	if err != nil {
		log.Errorf("err %v", err)
		return nil, err
	}

	if row.RowsAffected == 0 {
		return nil, rpc.CreateError(qmsg.ErrUserTagNotFound)
	}

	return &rsp, nil
}

func DelUserTag(ctx *rpc.Context, req *qmsg.DelUserTagReq) (*qmsg.DelUserTagRsp, error) {
	var rsp qmsg.DelUserTagRsp

	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)
	user, err := getUserCheck(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	//先删标签关系，再删标签，异常情况，再操作一次即可，不做事务

	_, err = UserTagRelation.WhereCorpApp(corpId, appId).Where(DbUid, user.Id).Where(DbTagId, req.TagId).Delete(ctx)
	if err != nil {
		log.Errorf("err %v", err)
		return nil, err
	}

	row, err := UserTag.WhereCorpApp(corpId, appId).Where(DbUid, user.Id).Where(DbId, req.TagId).Delete(ctx)
	if err != nil {
		log.Errorf("err %v", err)
		return nil, err
	}

	if row.RowsAffected == 0 {
		return nil, rpc.CreateError(qmsg.ErrUserTagNotFound)
	}

	return &rsp, nil
}

func SetExtContactUserTag(ctx *rpc.Context, req *qmsg.SetExtContactUserTagReq) (*qmsg.SetExtContactUserTagRsp, error) {
	var rsp qmsg.SetExtContactUserTagRsp

	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)
	user, err := getUserCheck(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	cnt, err := UserTag.WhereCorpApp(corpId, appId).Where(DbUid, user.Id).Where(DbId, req.TagId).Count(ctx)
	if err != nil {
		log.Errorf("err %v", err)
		return nil, err
	}

	if cnt == 0 {
		return nil, rpc.CreateError(qmsg.ErrUserTagNotFound)
	}

	if req.IsSet {

		cnt, err = UserTagRelation.WhereCorpApp(corpId, appId).Where(DbUid, user.Id).Where(DbAccountId, req.ExtAccountId).Count(ctx)
		if err != nil {
			log.Errorf("err %v", err)
			return nil, err
		}

		if cnt >= 10 {
			return nil, rpc.CreateError(qmsg.ErrAccountTagOverLimit)
		}

		var rel qmsg.ModelUserTagRelation
		_, err = UserTagRelation.NewScope().FirstOrCreate(ctx, map[string]interface{}{
			DbCorpId:    corpId,
			DbAppId:     appId,
			DbUid:       user.Id,
			DbTagId:     req.TagId,
			DbAccountId: req.ExtAccountId,
		}, map[string]interface{}{}, &rel)
		if err != nil {
			log.Errorf("err %v", err)
			return nil, err
		}
	} else {
		_, err = UserTagRelation.WhereCorpApp(corpId, appId).Where(DbUid, user.Id).Where(DbTagId, req.TagId).Where(DbAccountId, req.ExtAccountId).ForceDelete(ctx)
		if err != nil {
			log.Errorf("err %v", err)
			return nil, err
		}
	}

	return &rsp, nil
}

func GetExtContactListByUserTag(ctx *rpc.Context, req *qmsg.GetExtContactListByUserTagReq) (*qmsg.GetExtContactListByUserTagRsp, error) {
	var rsp qmsg.GetExtContactListByUserTagRsp

	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)
	user, err := getUserCheck(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	db := UserTagRelation.WhereCorpApp(corpId, appId).Where(DbUid, user.Id)
	if len(req.TagIdList) > 0 {
		db.WhereIn(DbTagId, req.TagIdList)
	}

	var list []*qmsg.ModelUserTagRelation
	err = db.Find(ctx, &list)
	if err != nil {
		log.Errorf("err %v", err)
		return nil, err
	}

	if len(list) == 0 {
		return &rsp, nil
	}

	extAccountIdList := utils.PluckUint64(list, "AccountId")

	var robotUidList []uint64
	if user.IsAssign {
		var userRobotList []*qmsg.ModelUserRobot
		err = UserRobot.WhereCorpApp(corpId, appId).Select(DbRobotUid).Where(DbUid, user.Id).Find(ctx, &userRobotList)
		if err != nil {
			log.Errorf("err %v", err)
			return nil, err
		}

		robotUidList = utils.PluckUint64(userRobotList, "RobotUid")
	} else {

		var assignList []*qmsg.ModelAssignChat
		err = AssignChat.WhereCorpApp(corpId, appId).Where(DbUid, user.Id).Select(DbRobotUid).
			Where(DbEndScene, qmsg.ModelAssignChat_EndSceneNil).Find(ctx, &assignList)
		if err != nil {
			log.Errorf("err %v", err)
			return nil, err
		}

		robotUidList = utils.PluckUint64(assignList, "RobotUid").Unique()
	}

	if len(robotUidList) == 0 {
		return &rsp, nil
	}

	var chatList []*qmsg.ModelChat
	err = Chat.WhereCorpApp(corpId, appId).WhereIn(DbRobotUid, robotUidList).WhereIn(DbChatExtId, extAccountIdList).Where(DbIsGroup, 0).Find(ctx, &chatList)
	if err != nil {
		log.Errorf("err %v", err)
		return nil, err
	}

	// 定义一个 chatList的map映射，key为chat_ext_id
	chatMap := make(map[uint64]struct{})
	for _, v := range chatList {
		chatMap[v.ChatExtId] = struct{}{}
	}

	aMap, err := getFollowInfo(ctx, corpId, appId, robotUidList, extAccountIdList)
	if err != nil {
		log.Errorf("err %v", err)
		return nil, err
	}

	rsp.FollowMap = make(map[uint64]*qmsg.GetExtContactListByUserTagRsp_FollowList)
	for _, v := range list {

		extList, ok := aMap[v.AccountId]
		if !ok {
			continue
		}
		_, ok = chatMap[v.AccountId]
		if !ok {
			continue
		}

		if len(extList) == 0 {
			log.Infof("found nothing")
			continue
		}

		if rsp.FollowMap[v.TagId] == nil {
			rsp.FollowMap[v.TagId] = &qmsg.GetExtContactListByUserTagRsp_FollowList{}
		}
		rsp.FollowMap[v.TagId].FollowList = append(rsp.FollowMap[v.TagId].FollowList, extList...)
	}

	rsp.ChatList = chatList

	return &rsp, nil
}

func getFollowInfo(ctx *rpc.Context, corpId, appId uint32, robotUidList []uint64, extAccountIdList []uint64) (map[uint64][]*qmsg.FollowInfo, error) {

	followMap := make(map[uint64][]*qmsg.FollowInfo) //key: ext account id

	aRsp, err := qrobot.GetAccountListSys(ctx, &qrobot.GetAccountListSysReq{
		ListOption: core.NewListOption().SetSkipCount().SetLimit(uint32(len(extAccountIdList))).AddOpt(qrobot.GetAccountListSysReq_ListOptionIdList, extAccountIdList),
		CorpId:     corpId,
		AppId:      appId,
	})

	if err != nil {
		log.Errorf("err %v", err)
		return nil, err
	}

	accountMap := make(map[uint64]*qrobot.ModelAccount)
	utils.KeyBy(aRsp.List, "Id", &accountMap)

	//for _, robotUid := range robotUidList {
	//var accFollowRsp *qrobot.GetAccountFollowListSysRsp
	accFollowRsp, err := qrobot.GetAccountFollowListSys(ctx, &qrobot.GetAccountFollowListSysReq{
		ListOption: core.NewListOption().
			AddOpt(qrobot.GetAccountFollowListSysReq_ListOptionExtAccountIdList, extAccountIdList).
			AddOpt(qrobot.GetAccountFollowListSysReq_ListOptionRobotUidList, robotUidList),
		//RobotUid:   robotUid,
		CorpId: corpId,
		AppId:  appId,
	})
	if err != nil {
		log.Errorf("err %v", err)
		return nil, err
	}

	if len(accFollowRsp.List) == 0 {
		return followMap, nil
	}

	for _, v := range accFollowRsp.FollowList {

		acc, ok := accountMap[v.ExtAccountId]
		if !ok {
			continue
		}

		followMap[v.ExtAccountId] = append(followMap[v.ExtAccountId], &qmsg.FollowInfo{
			ExtAccountId: v.ExtAccountId,
			YcSerialNo:   acc.YcSerialNo,
			RobotUid:     v.RobotUid,
			ExtUid:       acc.ExtUid,
			Name:         acc.Name,
			Avatar: func() string {
				if acc.Profile == nil {
					return ""
				}
				return acc.Profile.Avatar
			}(),
		})
	}
	//}
	return followMap, nil
}

func GetUserTagList(ctx *rpc.Context, req *qmsg.GetUserTagListReq) (*qmsg.GetUserTagListRsp, error) {
	var rsp qmsg.GetUserTagListRsp

	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)
	user, err := getUserCheck(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	var tagList []*qmsg.ModelUserTag
	err = UserTag.WhereCorpApp(corpId, appId).Where(DbUid, user.Id).Find(ctx, &tagList)
	if err != nil {
		log.Errorf("err %v", err)
		return nil, err
	}
	for _, v := range tagList {
		rsp.TagList = append(rsp.TagList, &qmsg.TagInfo{
			TagId:   v.Id,
			TagName: v.TagName,
		})
	}

	return &rsp, nil
}

func GetExtContactTagList(ctx *rpc.Context, req *qmsg.GetExtContactTagListReq) (*qmsg.GetExtContactTagListRsp, error) {
	var rsp qmsg.GetExtContactTagListRsp

	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)

	user, err := getUserCheck(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	var relaList []*qmsg.ModelUserTagRelation
	err = UserTagRelation.WhereCorpApp(corpId, appId).Where(DbUid, user.Id).Where(DbAccountId, req.ExtAccountId).Find(ctx, &relaList)
	if err != nil {
		log.Errorf("err %v", err)
		return nil, err
	}

	tagIdList := utils.PluckUint64(relaList, "TagId")

	if len(tagIdList) > 0 {
		var tagList []*qmsg.ModelUserTag
		err = UserTag.WhereCorpApp(corpId, appId).Where(DbUid, user.Id).WhereIn(DbId, tagIdList).Find(ctx, &tagList)
		if err != nil {
			log.Errorf("err %v", err)
			return nil, err
		}

		for _, v := range tagList {
			rsp.TagList = append(rsp.TagList, &qmsg.TagInfo{
				TagId:   v.Id,
				TagName: v.TagName,
			})
		}
	}

	return &rsp, nil
}
