package impl

import (
	"fmt"
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/brick/utils"
	"git.pinquest.cn/qlb/brick/utils/routine"
	"git.pinquest.cn/qlb/core"
	"git.pinquest.cn/qlb/extrpkg/github.com/elliotchance/pie/pie"
	"git.pinquest.cn/qlb/extrpkg/github.com/go-redis/redis"
	"git.pinquest.cn/qlb/qmsg"
	"git.pinquest.cn/qlb/qrobot"
	"time"
)

// 权限相关的根据关注群的逻辑走
// GroupMember有accountId，用作关联机器人
// Account有uid可以关联robotUid
func GetGroupChatList(ctx *rpc.Context, req *qmsg.GetGroupChatListReq) (*qmsg.GetGroupChatListRsp, error) {
	var rsp qmsg.GetGroupChatListRsp
	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)
	user, err := getUserCheck(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	if user.IsAssign {
		err = ensureHasRobotPerm(ctx, user, req.RobotUid)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
	}

	var hasWatch bool

	var appType uint32
	listOption := core.NewListOption().SetLimit(req.ListOption.Limit).SetOffset(req.ListOption.Offset)
	err = core.NewListOptionProcessor(req.ListOption).
		AddUint64List(
			qmsg.GetGroupChatListReq_ListOptionIdList,
			func(valList []uint64) error {
				listOption.AddOpt(qrobot.GetGroupChatListSysReq_ListOptionGroupIdList, valList)
				return nil
			}).
		AddTimeStampRange(
			qmsg.GetGroupChatListReq_ListOptionGroupCreatedAt,

			func(beginAt, endAt uint32) error {
				listOption.AddOpt(qrobot.GetGroupChatListSysReq_ListOptionGroupCreatedAt, fmt.Sprintf("%v,%v", beginAt, endAt))
				return nil
			}).
		AddUint64(qmsg.GetGroupChatListReq_ListOptionGroupCategoryId,
			func(val uint64) error {
				listOption.AddOpt(qrobot.GetGroupChatListSysReq_ListOptionCropGroupCategoryId, val)
				return nil
			}).
		AddUint64(qmsg.GetGroupChatListReq_ListOptionGroupTagId,
			func(val uint64) error {
				listOption.AddOpt(qrobot.GetGroupChatListSysReq_ListOptionCropGroupTagId, val)
				return nil
			}).
		AddString(qmsg.GetGroupChatListReq_ListOptionGroupName,
			func(val string) error {
				listOption.AddOpt(qrobot.GetGroupChatListSysReq_ListOptionName, val)
				return nil
			}).
		AddUint32(qmsg.GetGroupChatListReq_ListOptionAppType,
			func(val uint32) error {
				appType = val
				return nil
			}).
		AddBool(qmsg.GetGroupChatListReq_ListOptionIsWatch,
			func(val bool) error {
				listOption.AddOpt(qrobot.GetGroupChatListSysReq_ListOptionRobotWatchGroup, req.RobotUid)
				hasWatch = val
				return nil
			}).
		Process()
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	//前端入参有iswatch是，只筛选开通或者未开通部分数据
	if !hasWatch {
		listOption.AddOpt(qrobot.GetGroupChatListSysReq_ListOptionRobotUid, req.RobotUid)
	}

	listRsp, err := qrobot.GetGroupChatListSys(ctx, &qrobot.GetGroupChatListSysReq{
		CorpId:  corpId,
		AppId:   appId,
		AppType: appType,
		ListOption: listOption.
			AddOpt(qrobot.GetGroupChatListSysReq_ListOptionGroupType, uint32(qrobot.GroupType_GroupTypeOuter)).
			AddOpt(qrobot.GetGroupChatListSysReq_ListOptionIsRelation, true),
		//AddOpt(qrobot.GetGroupChatListSysReq_ListOptionUseSlave, true),
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	rsp.Paginate = listRsp.Paginate
	rsp.List = listRsp.List
	rsp.RelationMap = listRsp.RelationMap
	return &rsp, nil
}
func GetGroupChatMemberList(ctx *rpc.Context, req *qmsg.GetGroupChatMemberListReq) (*qmsg.GetGroupChatMemberListRsp, error) {
	var rsp qmsg.GetGroupChatMemberListRsp
	user, err := getUserCheck(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	if user.IsAssign {
		err = ensureHasRobotPerm(ctx, user, req.RobotUid)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
	}

	_, err = qrobot.CheckRobotInGroupChat(ctx, &qrobot.CheckRobotInGroupChatReq{
		RobotUid:    req.RobotUid,
		GroupChatId: req.GroupId,
		CorpId:      user.CorpId,
		AppId:       user.AppId,
	})
	if rpc.GetErrCode(err) == qrobot.ErrRobotIsNotWatchThisGroup {
	} else if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	listOption := core.NewListOption().SetLimit(req.ListOption.Limit).SetOffset(req.ListOption.Offset)
	err = core.NewListOptionProcessor(req.ListOption).
		AddBool(
			qmsg.GetGroupChatMemberListReq_ListOptionIncludeDeleted,
			func(val bool) error {
				listOption.AddOpt(qrobot.GetGroupChatMemberListSysReq_ListOptionIncludeDeleted, val)
				return nil
			}).
		AddUint64List(
			qmsg.GetGroupChatMemberListReq_ListOptionIdList,
			func(valList []uint64) error {
				listOption.AddOpt(qrobot.GetGroupChatMemberListSysReq_ListOptionIdList, valList)
				return nil
			}).
		Process()
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	listRsp, err := qrobot.GetGroupChatMemberListSys(ctx, &qrobot.GetGroupChatMemberListSysReq{
		CorpId: user.CorpId,
		AppId:  user.AppId,
		ListOption: listOption.
			AddOpt(uint32(qrobot.GetGroupChatMemberListSysReq_ListOptionGroupIdList), []uint64{req.GroupId}),
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	rsp.Paginate = listRsp.Paginate
	rsp.List = listRsp.List
	var accountIdList []uint64
	for _, v := range rsp.List {
		accountIdList = append(accountIdList, v.AccountId)
	}
	var modelAccountMap map[uint64]*qrobot.ModelAccount
	err = Db().FilterCorpAndApp(user.CorpId, user.AppId).
		In(DbId, accountIdList).
		Find2Map(ctx, &modelAccountMap).Error()
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	var uidList []uint64
	for _, v := range modelAccountMap {
		uidList = append(uidList, v.Uid)
	}
	var modelAccountFollowList []*qrobot.ModelAccountFollow
	err = Db().FilterCorpAndApp(user.CorpId, user.AppId).
		In(DbRobotUid, uidList).
		In(DbExtAccountId, accountIdList).
		NotEq(DbAlias, "").
		Find(ctx, &modelAccountFollowList).Error()
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	var modelAccountFollowMap map[uint64]*qrobot.ModelAccountFollow
	utils.KeyBy(modelAccountFollowList, "RobotUid", &modelAccountFollowMap)
	rsp.MemberInfoMap = make(map[uint64]*qmsg.GetGroupChatMemberListRsp_MemberInfo)
	for _, v := range rsp.List {
		if m, ok := modelAccountMap[v.AccountId]; ok {
			uid := m.Uid
			if m2, ok := modelAccountFollowMap[uid]; ok {
				rsp.MemberInfoMap[v.Id] = &qmsg.GetGroupChatMemberListRsp_MemberInfo{
					Alias: m2.Alias,
				}
			}
		}
	}
	return &rsp, nil
}

func getRobotGroupChatIdList(ctx *rpc.Context, corpId uint32, appId uint32, robotUid uint64, isWatch uint32) ([]uint64, error) {
	var groupIdList []uint64
	offset := uint32(0)
	limit := uint32(2000)
	for {
		listOption := core.NewListOption().
			SetOffset(offset).
			SetLimit(limit).
			AddOpt(qrobot.GetGroupIdListSysReq_ListOptionMemberUidList, robotUid)

		if isWatch == 1 {
			listOption.AddOpt(qrobot.GetGroupIdListSysReq_ListOptionIsWatch, true)
		}
		if isWatch == 0 {
			listOption.AddOpt(qrobot.GetGroupIdListSysReq_ListOptionIsWatch, false)
		}

		// 先从群成员列表中获取到机器人所在的群id
		groupIdListRsp, err := qrobot.GetGroupIdListSys(ctx, &qrobot.GetGroupIdListSysReq{
			CorpId:     corpId,
			AppId:      appId,
			ListOption: listOption,
		})
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}

		if len(groupIdListRsp.GroupIdList) == 0 {
			break
		}

		groupIdList = append(groupIdList, groupIdListRsp.GroupIdList...)
		offset += limit
	}

	return groupIdList, nil
}

func getNotInGroupChatIdList(ctx *rpc.Context, corpId uint32, appId uint32, groupIdList []uint64) ([]uint64, error) {
	var notInGroupIdList []uint64
	var chunkErr error
	pie.Uint64s(groupIdList).Chunk(500, func(chunkGroupIdList pie.Uint64s) (stopped bool) {
		getGroupChatRsp, err := qrobot.GetGroupChatListSys(ctx, &qrobot.GetGroupChatListSysReq{
			CorpId: corpId,
			AppId:  appId,
			ListOption: core.NewListOption().SetLimit(500).
				AddOpt(qrobot.GetGroupChatListSysReq_ListOptionGroupIdList, chunkGroupIdList).
				AddOpt(qrobot.GetGroupChatListSysReq_ListOptionSelectFields, []string{DbId}),
		})
		if err != nil {
			log.Errorf("err:%v", err)
			chunkErr = err
			return true
		}
		inGroupIdList := utils.PluckUint64(getGroupChatRsp.List, "Id")
		notInList, _ := utils.DiffSliceV2(chunkGroupIdList, inGroupIdList)
		for _, groupId := range notInList.(pie.Uint64s) {
			notInGroupIdList = append(notInGroupIdList, groupId)
		}
		return false
	})
	if chunkErr != nil {
		return nil, chunkErr
	}
	return notInGroupIdList, nil
}

func GetGroupChatListV2(ctx *rpc.Context, req *qmsg.GetGroupChatListV2Req) (*qmsg.GetGroupChatListV2Rsp, error) {
	var rsp qmsg.GetGroupChatListV2Rsp
	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)
	user, err := getUserCheck(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	if user.IsAssign {
		err = ensureHasRobotPerm(ctx, user, req.RobotUid)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
	}

	var hasWatch bool
	var appType uint32
	var groupIdList []uint64
	cacheKey := fmt.Sprintf("qmsg_robot_group_chat_id_list_%d_%d_%d", user.CorpId, user.AppId, req.RobotUid)
	isWatch := uint32(2)

	listOption := core.NewListOption()
	err = core.NewListOptionProcessor(req.ListOption).
		AddUint32(qmsg.GetGroupChatListReq_ListOptionAppType,
			func(val uint32) error {
				appType = val
				return nil
			}).
		AddBool(qmsg.GetGroupChatListReq_ListOptionIsWatch,
			func(val bool) error {
				hasWatch = val
				if hasWatch {
					isWatch = 1
					cacheKey = fmt.Sprintf("qmsg_robot_group_chat_id_list_%d_%d_%d_watch", user.CorpId, user.AppId, req.RobotUid)
				} else {
					isWatch = 0
					cacheKey = fmt.Sprintf("qmsg_robot_group_chat_id_list_%d_%d_%d_no_watch", user.CorpId, user.AppId, req.RobotUid)
				}
				return nil
			}).
		Process()
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	var newGroupIdList []uint64

	// 查询缓存
	err = s.RedisGroup.GetJson(cacheKey, &newGroupIdList)
	if err != nil {
		if err != redis.Nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
		// 查询并写入缓存
		groupIdList, err = getRobotGroupChatIdList(ctx, user.CorpId, user.AppId, req.RobotUid, isWatch)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}

		var notInGroupChatIdMap = make(map[uint64]struct{})

		// 可能会存在群成员列表有这个群，但实际群已经被解散的情况，初次获取时做一个兜底处理
		if req.ListOption.Offset == 0 {
			notInGroupChatIdList, err := getNotInGroupChatIdList(ctx, user.CorpId, user.AppId, groupIdList)
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, err
			}
			for _, groupId := range notInGroupChatIdList {
				notInGroupChatIdMap[groupId] = struct{}{}
			}
			if len(notInGroupChatIdList) > 0 {
				routine.Go(ctx, func(ctx *rpc.Context) error {
					var chunkErr error
					pie.Uint64s(notInGroupChatIdList).Chunk(50, func(chunkGroupIdList pie.Uint64s) (stopped bool) {
						//移除这些群下面的成员
						_, err = qrobot.RemoveLocalGroupChatMemberSys(ctx, &qrobot.RemoveLocalGroupChatMemberSysReq{
							CorpId:        user.CorpId,
							AppId:         user.AppId,
							GroupIdList:   notInGroupChatIdList,
							MemberUidList: []uint64{req.RobotUid},
						})
						if err != nil {
							log.Errorf("err:%v", err)
							chunkErr = err
							return true
						}
						time.Sleep(50 * time.Millisecond)
						return false
					})
					return chunkErr
				})
			}
		}
		for _, groupId := range groupIdList {
			_, ok := notInGroupChatIdMap[groupId]
			if !ok {
				newGroupIdList = append(newGroupIdList, groupId)
			}
		}
		err = s.RedisGroup.SetJson(cacheKey, newGroupIdList, 600*time.Second)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
	}

	log.Debugf("GroupIdList:%v", newGroupIdList)

	// 根据偏移量从newGroupIdList中截取id
	if req.ListOption.Offset < uint32(len(newGroupIdList)) {
		groupIdList = newGroupIdList[req.ListOption.Offset:]
		if uint32(len(groupIdList)) > req.ListOption.Limit {
			groupIdList = groupIdList[:req.ListOption.Limit]
		}
	} else {
		return &rsp, nil
	}

	listRsp, err := qrobot.GetGroupChatListSys(ctx, &qrobot.GetGroupChatListSysReq{
		CorpId:  corpId,
		AppId:   appId,
		AppType: appType,
		ListOption: listOption.
			AddOpt(qrobot.GetGroupChatListSysReq_ListOptionGroupType, uint32(qrobot.GroupType_GroupTypeOuter)).
			AddOpt(qrobot.GetGroupChatListSysReq_ListOptionIsRelation, true).
			AddOpt(qrobot.GetGroupChatListSysReq_ListOptionUseSlave, true).
			AddOpt(qrobot.GetGroupChatListSysReq_ListOptionGroupIdList, groupIdList),
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	rsp.Paginate = listRsp.Paginate
	if req.ListOption.Limit+req.ListOption.Offset < uint32(len(newGroupIdList)) {
		rsp.Paginate.HasMore = true
	} else {
		rsp.Paginate.HasMore = false
	}
	rsp.Paginate.Limit = req.ListOption.Limit
	rsp.Paginate.Offset = req.ListOption.Offset
	rsp.Paginate.Total = uint32(len(newGroupIdList))
	rsp.List = listRsp.List
	rsp.RelationMap = listRsp.RelationMap
	return &rsp, nil
}
