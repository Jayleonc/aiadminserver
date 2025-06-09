package impl

import (
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/brick/utils"
	"git.pinquest.cn/qlb/brick/utils/routine"
	"git.pinquest.cn/qlb/core"
	"git.pinquest.cn/qlb/extrpkg/github.com/elliotchance/pie/pie"
	"git.pinquest.cn/qlb/iquan"
	"git.pinquest.cn/qlb/qmsg"
	"git.pinquest.cn/qlb/qrobot"
	"git.pinquest.cn/qlb/quan"
)

func GetAccountList(ctx *rpc.Context, req *qmsg.GetAccountListReq) (*qmsg.GetAccountListRsp, error) {
	var rsp qmsg.GetAccountListRsp

	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)

	listRsp, err := qrobot.GetAccountListSys(ctx, &qrobot.GetAccountListSysReq{
		ListOption: req.ListOption,
		CorpId:     corpId,
		AppId:      appId,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	rsp.List = listRsp.List
	rsp.Paginate = listRsp.Paginate

	return &rsp, nil
}

func refreshLatestProfile(ctx *rpc.Context, corpId, appId uint32, robotUid uint64, accountList []*qrobot.ModelAccount) ([]*qmsg.Account, error) {
	var newList []*qmsg.Account
	for _, acc := range accountList {
		newAcc := &qmsg.Account{
			Id:          acc.Id,
			CreatedAt:   acc.CreatedAt,
			UpdatedAt:   acc.UpdatedAt,
			CorpId:      acc.CorpId,
			AppId:       acc.AppId,
			YcSerialNo:  acc.YcSerialNo,
			AccountType: acc.AccountType,
			Uid:         acc.Uid,
			ExtUid:      acc.ExtUid,
			StrOpenId:   acc.StrOpenId,
		}
		profile := acc.Profile
		if profile != nil {
			newAcc.Profile = &qmsg.Profile{
				Gender:   profile.Gender,
				Name:     profile.Name,
				Avatar:   profile.Avatar,
				Phone:    profile.Phone,
				CorpName: profile.CorpName,
			}
			if acc.Profile != nil {
				newAcc.Profile.Alias = acc.Profile.Alias
			}
		}
		newList = append(newList, newAcc)

		if acc.AccountType != uint32(qrobot.AccountType_AccountTypeStaff) && acc.ExtUid == 0 {
			routine.Go(ctx, func(ctx *rpc.Context) error {
				LogStream.LogStreamSubType(ctx, ExtUidZeroSubType, &ExtUidZeroStream{
					CorpId:    acc.CorpId,
					AccountId: acc.Id,
				})
				return nil
			})
		}
	}
	newList, err := refreshLatestStaffProfile4AccountList(ctx, corpId, appId, newList)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	newList, err = refreshLatestExtContactProfile4AccountList(ctx, corpId, appId, robotUid, newList)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	return newList, nil
}

func refreshLatestProfile4AccountList(ctx *rpc.Context, corpId, appId uint32, robotUid uint64, list []*qrobot.ModelAccount) ([]*qmsg.Account, error) {
	if len(list) == 0 {
		return nil, nil
	}
	extAccList := utils.PluckUint64(list, "Id")
	followRsp, err := qrobot.GetAccountFollowList4FollowSys(ctx, &qrobot.GetAccountFollowList4FollowSysReq{
		ListOption: core.NewListOption().SetSkipCount().AddOpt(uint32(qrobot.GetAccountFollowList4FollowSysReq_ListOptionExtAccountIdList),
			extAccList),
		RobotUid: robotUid,
		CorpId:   corpId,
		AppId:    appId,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	var followMap map[uint64]*qrobot.Account
	utils.KeyBy(followRsp.List, "Id", &followMap)
	var newList []*qmsg.Account
	for _, acc := range list {
		follow := followMap[acc.Id]
		if follow == nil {
			follow = &qrobot.Account{}
		}
		newAcc := &qmsg.Account{
			Id:          acc.Id,
			CreatedAt:   acc.CreatedAt,
			UpdatedAt:   acc.UpdatedAt,
			CorpId:      acc.CorpId,
			AppId:       acc.AppId,
			YcSerialNo:  acc.YcSerialNo,
			AccountType: acc.AccountType,
			Uid:         acc.Uid,
			ExtUid:      acc.ExtUid,
			StrOpenId:   acc.StrOpenId,
		}
		profile := acc.Profile
		if profile != nil {
			newAcc.Profile = &qmsg.Profile{
				Gender:   profile.Gender,
				Name:     profile.Name,
				Avatar:   profile.Avatar,
				Phone:    profile.Phone,
				CorpName: profile.CorpName,
			}
			if follow.Profile != nil {
				newAcc.Profile.Alias = follow.Profile.Alias
			}
		}
		newList = append(newList, newAcc)
	}
	newList, err = refreshLatestStaffProfile4AccountList(ctx, corpId, appId, newList)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	newList, err = refreshLatestExtContactProfile4AccountList(ctx, corpId, appId, robotUid, newList)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	return newList, nil
}

func refreshLatestStaffProfile4AccountList(ctx *rpc.Context, corpId, appId uint32, list []*qmsg.Account) ([]*qmsg.Account, error) {
	isStaff := func(acc *qmsg.Account) bool {
		return acc.AccountType == uint32(qrobot.AccountType_AccountTypeStaff)
	}
	staffList := qmsg.AccountList(list).Filter(isStaff)
	uidList := pie.Uint64s(utils.PluckUint64(staffList, "Uid")).Filter(utils.Uint64Gt0)
	if len(uidList) > 0 {
		var userMap map[uint64]*quan.ModelUser
		userListRsp, err := iquan.GetUserList(ctx, &iquan.GetUserListReq{
			ListOption: core.NewListOption().SetSkipCount().
				AddOpt(uint32(iquan.GetUserListReq_ListOptionUidList), uidList),
			CorpId: corpId,
			AppId:  appId,
		})
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
		utils.KeyBy(userListRsp.List, "Id", &userMap)
		for _, acc := range list {
			if !isStaff(acc) {
				continue
			}
			user := userMap[acc.Uid]
			if user == nil {
				continue
			}
			acc.Profile = mergeProfile4User(acc.Profile, user)
		}
	}
	return list, nil
}

func refreshLatestExtContactProfile4AccountList(ctx *rpc.Context, corpId, appId uint32, robotUid uint64, list []*qmsg.Account) ([]*qmsg.Account, error) {
	ectContactIdList := utils.PluckUint64(list, "ExtUid").Filter(utils.Uint64Gt0)
	if len(ectContactIdList) > 0 {
		var extContactMap map[uint64]*quan.ModelExtContactFollow
		listRsp, err := iquan.GetExtContactFollowList(ctx, &iquan.GetExtContactFollowListReq{
			ListOption: core.NewListOption().
				AddOpt(iquan.GetExtContactFollowListReq_ListOptionExtUidList, ectContactIdList).
				AddOpt(iquan.GetExtContactFollowListReq_ListOptionUidList, []uint64{robotUid}),
			CorpId: corpId,
			AppId:  appId,
		})
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
		utils.KeyBy(listRsp.List, "ExtUid", &extContactMap)
		for _, acc := range list {
			user := extContactMap[acc.ExtUid]
			if user == nil {
				continue
			}
			acc.Profile = mergeProfile4ExtContact(acc.Profile, user)
			acc.DeletedByExtContact = user.DeletedByExtContact
			acc.DeletedByStaff = user.DeletedByStaff
			if user.Info != nil {
				acc.Profile.Desc = user.Info.Desc
			}
		}
	}
	return list, nil
}

func GetAccountProfileList(ctx *rpc.Context, req *qmsg.GetAccountProfileListReq) (*qmsg.GetAccountProfileListRsp, error) {
	var rsp qmsg.GetAccountProfileListRsp
	qRsp, err := qrobot.GetAccountList(ctx, &qrobot.GetAccountListReq{
		ListOption: req.ListOption.CloneChangeOptTypes(qmsg.GetAccountProfileListReq_ListOptionIdList,
			qrobot.GetAccountListReq_ListOptionIdList),
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	qrobot.AccountList(qRsp.List).Each(func(acc *qrobot.ModelAccount) {
		p := &qmsg.GetAccountProfileListRsp_Profile{
			AccountId: acc.Id,
			Name:      "",
			Avatar:    "",
		}
		profile := acc.Profile
		if profile != nil {
			p.Name = profile.Name
			p.Avatar = profile.Avatar
		}
		rsp.List = append(rsp.List, p)
	})
	return &rsp, nil
}

func GetAccountFollowList(ctx *rpc.Context, req *qmsg.GetAccountFollowListReq) (*qmsg.GetAccountFollowListRsp, error) {
	var rsp qmsg.GetAccountFollowListRsp
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

	// 这里做下兼容，当不是指定ID获取的场景下（非获取列表）才去获取包括被软删除的数据
	listOptions := req.ListOption.CloneChangeOptTypes(uint32(qmsg.GetAccountFollowListReq_ListOptionIdList),
		uint32(qrobot.GetAccountFollowListSysReq_ListOptionExtAccountIdList)).
		// 增加id倒排，解决按时间排序导致分页数据重复问题
		AddOpt(qrobot.GetAccountFollowListSysReq_ListOptionWithOrderById, qrobot.GetAccountFollowListSysReq_OrderByIdDesc)
	// 判断是否指定ID查询的，指定ID查询需要查出被软删除的（历史坑）
	err = core.NewListOptionProcessor(req.ListOption).
		AddUint64List(qmsg.GetAccountFollowListReq_ListOptionIdList, func(valList []uint64) error {
			if len(valList) > 0 {
				listOptions = listOptions.AddOpt(qrobot.GetAccountFollowListSysReq_ListOptionWithTrash, true)
			}
			return nil
		}).
		Process()
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	listRsp, err := qrobot.GetAccountFollowListSys(ctx, &qrobot.GetAccountFollowListSysReq{
		ListOption: listOptions,
		RobotUid:   req.RobotUid,
		CorpId:     user.CorpId,
		AppId:      user.AppId,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	rsp.List, err = refreshLatestProfile(ctx, user.CorpId, user.AppId, req.RobotUid, listRsp.List)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	rsp.Paginate = listRsp.Paginate
	return &rsp, nil
}

func mergeProfile4ExtContact(profile *qmsg.Profile, ext *quan.ModelExtContactFollow) *qmsg.Profile {
	var res qmsg.Profile
	if profile != nil {
		res = *profile
	}
	if ext.Name != "" && ext.Name != res.Name {
		res.Name = ext.Name
	}
	if ext.Avatar != "" && ext.Avatar != res.Avatar {
		res.Avatar = ext.Avatar
	}
	if ext.CorpName != "" && ext.CorpName != res.CorpName {
		res.CorpName = ext.CorpName
	}
	if ext.Info != nil && ext.Info.Remark != "" && ext.Info.Remark != res.RemarkName {
		res.RemarkName = ext.Info.Remark
	}
	return &res
}

func mergeProfile4User(profile *qmsg.Profile, user *quan.ModelUser) *qmsg.Profile {
	var res qmsg.Profile
	if profile != nil {
		res = *profile
	}
	if user.WwUserName != "" && user.WwUserName != res.Name {
		res.Name = user.WwUserName
	}
	if user.Gender > 0 && user.Gender != res.Gender {
		res.Gender = user.Gender
	}
	if user.WwAvatar != "" && user.WwAvatar != res.Avatar {
		res.Avatar = user.WwAvatar
	}
	return &res
}

func coverAccountQrobot2Qmsg(acc *qrobot.ModelAccount) *qmsg.Account {
	newAcc := qmsg.Account{
		Id:          acc.Id,
		CreatedAt:   acc.CreatedAt,
		UpdatedAt:   acc.UpdatedAt,
		CorpId:      acc.CorpId,
		AppId:       acc.AppId,
		YcSerialNo:  acc.YcSerialNo,
		AccountType: acc.AccountType,
		Uid:         acc.Uid,
		ExtUid:      acc.ExtUid,
		IsRobot:     true,
		RobotType:   uint32(qmsg.RobotType_RobotTypePlatform),
		StrOpenId:   acc.StrOpenId,
	}
	if !acc.IsRobot {
		newAcc.RobotType = uint32(qmsg.RobotType_RobotTypeHostAccount)
	}
	profile := acc.Profile
	if profile == nil {
		profile = &qrobot.ModelAccount_Profile{}
	}
	newAcc.Profile = &qmsg.Profile{
		Gender:     profile.Gender,
		Name:       profile.Name,
		Avatar:     profile.Avatar,
		WwUserId:   profile.WwUserId,
		CorpName:   profile.CorpName,
		Alias:      profile.Alias,
		RemarkName: profile.RemarkName,
	}
	return &newAcc
}

func coverQRobot2Account(acc *qrobot.ModelQRobot) *qmsg.Account {
	newAcc := qmsg.Account{
		CreatedAt:   acc.CreatedAt,
		UpdatedAt:   acc.UpdatedAt,
		CorpId:      acc.CorpId,
		AppId:       acc.AppId,
		YcSerialNo:  acc.StrRobotId,
		AccountType: uint32(qrobot.AccountType_AccountTypeStaff),
		Uid:         acc.Uid,
		IsRobot:     true,
		RobotType:   uint32(qmsg.RobotType_RobotTypePlatform),
		StrOpenId:   acc.StrOpenId,
	}
	newAcc.Profile = &qmsg.Profile{
		Gender:     acc.Sex,
		Name:       acc.Name,
		Avatar:     acc.AvatarUrl,
		WwUserId:   acc.UserId,
		Alias:      acc.Alias,
		RemarkName: acc.Alias,
	}
	return &newAcc
}

func coverUser2Account(acc *quan.ModelUser) *qmsg.Account {
	newAcc := qmsg.Account{
		CreatedAt:   acc.CreatedAt,
		UpdatedAt:   acc.UpdatedAt,
		CorpId:      acc.CorpId,
		AppId:       acc.AppId,
		YcSerialNo:  acc.YcSerialNo,
		AccountType: uint32(qrobot.AccountType_AccountTypeStaff),
		Uid:         acc.Id,
		IsRobot:     true,
		RobotType:   uint32(qmsg.RobotType_RobotTypePlatform),
	}
	if !acc.IsRobot {
		newAcc.RobotType = uint32(qmsg.RobotType_RobotTypeHostAccount)
	}
	newAcc.Profile = &qmsg.Profile{
		Gender:   acc.Gender,
		Name:     acc.WwUserName,
		Avatar:   acc.WwAvatar,
		WwUserId: acc.WwUserId,
	}
	return &newAcc
}

func GetAccountBaseInfoList(ctx *rpc.Context, req *qmsg.GetAccountBaseInfoListReq) (*qmsg.GetAccountBaseInfoListRsp, error) {
	var rsp qmsg.GetAccountBaseInfoListRsp

	user, err := getUserCheck(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	err = ensureHasRobotPerm(ctx, user, req.RobotUid)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	listOption := core.NewListOption().
		AddOpt(uint32(qrobot.GetAccountListReq_ListOptionRobotUidFollow), req.RobotUid).
		AddOpt(uint32(qrobot.GetAccountListReq_ListOptionIdList), req.AccountIdList)
	listRsp, err := qrobot.GetAccountList(ctx, &qrobot.GetAccountListReq{
		ListOption: listOption,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	list, err := refreshLatestProfile4AccountList(ctx, user.CorpId, user.AppId, req.RobotUid, listRsp.List)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	qmsg.AccountList(list).Each(func(acc *qmsg.Account) {
		newAcc := &qmsg.Account{
			Id:          acc.Id,
			CreatedAt:   acc.CreatedAt,
			UpdatedAt:   acc.UpdatedAt,
			CorpId:      acc.CorpId,
			AppId:       acc.AppId,
			AccountType: acc.AccountType,
			ExtUid:      acc.ExtUid,
			Profile:     nil,
			StrOpenId:   acc.StrOpenId,
		}

		if acc.Profile != nil {
			newAcc.Profile = &qmsg.Profile{
				Name:     acc.Profile.Name,
				Avatar:   acc.Profile.Avatar,
				CorpName: acc.Profile.CorpName,
			}
		}

		rsp.List = append(rsp.List, newAcc)
	})

	return &rsp, nil
}

func GetUserAccountList(ctx *rpc.Context, req *qmsg.GetUserAccountListReq) (*qmsg.GetUserAccountListRsp, error) {
	var rsp qmsg.GetUserAccountListRsp

	user, err := getUserCheck(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	if user.IsAssign {

		var userRobotList []*qmsg.ModelUserRobot
		if len(req.UidList) > 0 {
			userRobotList, err = UserRobot.getListByRobotUidList(ctx, user.CorpId, user.AppId, user.Id, req.UidList)
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, err
			}
		} else {
			userRobotList, err = UserRobot.getListByUid(ctx, user.CorpId, user.AppId, user.Id)
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, err
			}
		}

		robotUidList := utils.PluckUint64(userRobotList, "RobotUid")
		listRsp, err := qrobot.GetAccountListSys(ctx, &qrobot.GetAccountListSysReq{
			ListOption: core.NewListOption().SetSkipCount().AddOpt(qrobot.GetAccountListSysReq_ListOptionUidList, robotUidList),
			CorpId:     user.CorpId,
			AppId:      user.AppId,
		})
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}

		rsp.List = listRsp.List
	} else {
		if len(req.UidList) == 0 {
			return nil, rpc.InvalidArg("非分配客服必须要 uid list")
		}

		listRsp, err := qrobot.GetAccountListSys(ctx, &qrobot.GetAccountListSysReq{
			ListOption: core.NewListOption().SetSkipCount().AddOpt(qrobot.GetAccountListSysReq_ListOptionUidList, req.UidList),
			CorpId:     user.CorpId,
			AppId:      user.AppId,
		})
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}

		rsp.List = listRsp.List
	}

	return &rsp, nil
}

func MatchExt(ctx *rpc.Context, req *qmsg.MatchExtReq) (*qmsg.MatchExtRsp, error) {
	var rsp qmsg.MatchExtRsp

	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)

	extRsp, err := qrobot.MatchExtSys(ctx, &qrobot.MatchExtSysReq{
		RobotUid:     req.RobotUid,
		ExtAccountId: req.ExtAccountId,
		CorpId:       corpId,
		AppId:        appId,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	rsp.ExtUid = extRsp.ExtUid

	return &rsp, nil
}
