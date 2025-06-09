package impl

import (
	"fmt"
	"git.pinquest.cn/qlb/brick/utils/routine"
	"git.pinquest.cn/qlb/qopen"
	"git.pinquest.cn/qlb/qrt"
	"strconv"
	"strings"
	"time"

	"git.pinquest.cn/base/datacrypt"
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/json"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/brick/smq"
	"git.pinquest.cn/qlb/brick/utils"
	"git.pinquest.cn/qlb/core"
	"git.pinquest.cn/qlb/extrpkg/github.com/elliotchance/pie/pie"
	"git.pinquest.cn/qlb/extrpkg/github.com/go-redis/redis"
	"git.pinquest.cn/qlb/iquan"
	"git.pinquest.cn/qlb/pindef"
	"git.pinquest.cn/qlb/qmsg"
	"git.pinquest.cn/qlb/qoperationrecord"
	"git.pinquest.cn/qlb/qops"
	"git.pinquest.cn/qlb/qrobot"
	"git.pinquest.cn/qlb/quan"
	"git.pinquest.cn/qlb/qwhale"
)

const (
	secretHashSecret = "$$%^&&dfgSDFdfGDS$%%^"
)

type UserUtil struct{}

func (UserUtil) getSecretHash(plainText string) (hash string) {
	if plainText == "" {
		panic(any("plain text empty"))
	}
	hash = utils.StrMd5(utils.StrMd5(plainText) + secretHashSecret)
	return
}
func (UserUtil) checkSecret(inputSecret, userSecret string) (correct bool) {
	inputSecret = strings.ToLower(inputSecret)
	s := utils.StrMd5(inputSecret + secretHashSecret)
	correct = s == userSecret
	return
}

var userUtil UserUtil

func getQUserCheck(ctx *rpc.Context) (*quan.ModelUser, error) {
	user, err := quan.GetUserCheck(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	return user, nil
}
func getUserCheck(ctx *rpc.Context) (*qmsg.ModelUser, error) {
	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)
	uid, _ := core.GetHeaderUId2Int(ctx)
	if uid == 0 {
		return nil, pindef.ErrNotLogin
	}
	u, err := User.get(ctx, corpId, appId, uid)
	if err != nil {
		if User.IsNotFoundErr(err) {
			// 使用的过程中，用户被删了
			log.Warnf("not found user, corp %d app %d uid %d", corpId, appId, uid)
			return nil, rpc.CreateError(qmsg.ErrUserHasDeleted)
		} else {
			log.Errorf("err:%v", err)
			return nil, err
		}
	}
	if u.IsDisable {
		return nil, rpc.CreateError(qmsg.ErrUserDisabled)
	}
	if u.ContactFilter == nil {
		u.ContactFilter = &quan.AdvanceExtContactFilter{
			FilterUser: &quan.AdvanceExtContactFilter_FilterUser{
				SelectType:     uint32(quan.SelectType_SelectTypeAssign),
				UidList:        []uint64{},
				UserSelectType: uint32(quan.UserSelectType_UserSelectTypeAll),
			},
			FilterExtContact: &quan.AdvanceExtContactFilter_FilterExtContact{
				ExtContactCondition: uint32(quan.AdvanceExtContactFilter_ExtContactConditionAll),
				Filter:              nil,
				ExtUidList:          nil,
				FollowIdList:        nil,
			},
		}
	}
	if u.GroupFilter == nil {
		u.GroupFilter = &qmsg.GroupFilter{
			SelectType:          uint32(quan.SelectType_SelectTypeAll),
			GroupCategoryIdList: nil,
			GroupIdList:         nil,
		}
	}
	return u, nil
}

func AddUser(ctx *rpc.Context, req *qmsg.AddUserReq) (*qmsg.AddUserRsp, error) {
	var rsp qmsg.AddUserRsp
	u := req.User
	qu, err := getQUserCheck(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	corpId := qu.CorpId
	appId := qu.AppId
	u.CorpId = corpId
	u.AppId = appId
	u.IsAssign = true
	// 判断下帐号ID是否存在
	err = User.Where(DbLoginId, u.LoginId).
		EnsureNotExists(ctx, qmsg.ErrLoginIdExisted)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	// TODO: 数量限制
	pwdPlainText := u.Password
	u.Password = userUtil.getSecretHash(pwdPlainText)
	addUser := func() error {
		u.StrOpenId, err = GenAndCheckUserOpenId(ctx, u.CorpId, u.AppId)
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
		err = User.Create(ctx, u)
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
		return nil
	}
	delUser := func() error {
		_, err = User.Where(DbId, u.Id).Delete(ctx)
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
		return nil
	}
	if len(req.RobotUidList) > 0 {
		iRsp, err := iquan.GetUserList(ctx, &iquan.GetUserListReq{
			ListOption: core.NewListOption().SetSkipCount().AddOpt(iquan.GetUserListReq_ListOptionUidList, req.RobotUidList),
			CorpId:     corpId,
			AppId:      appId,
		})
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
		if len(iRsp.List) != len(req.RobotUidList) {
			return nil, rpc.CreateError(qmsg.ErrNotHasRobotPerm)
		}
		var addList []*qmsg.ModelUserRobot
		quan.UserList(iRsp.List).Each(func(user *quan.ModelUser) {
			r := &qmsg.ModelUserRobot{
				RobotUid: user.Id,
			}
			if user.IsRobot {
				r.RobotType = uint32(qmsg.RobotType_RobotTypePlatform)
			} else {
				r.RobotType = uint32(qmsg.RobotType_RobotTypeHostAccount)
			}
			addList = append(addList, r)
		})
		existCount, err := UserRobot.
			WhereCorpApp(corpId, appId).
			WhereIn(DbRobotUid, req.RobotUidList).
			Count(ctx)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
		if existCount > 0 {
			return nil, rpc.CreateError(qmsg.ErrRobotHasDistribute)
		}
		err = addUser()
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
		var robotUidList []uint64
		qmsg.UserRobotList(addList).
			Map(func(x *qmsg.ModelUserRobot) *qmsg.ModelUserRobot {
				x.Uid = u.Id
				x.CorpId = u.CorpId
				x.AppId = u.AppId
				robotUidList = append(robotUidList, x.RobotUid)
				return x
			})
		err = UserRobot.batchCreate(ctx, u.CorpId, u.AppId, addList, robotUidList)
		if err != nil {
			log.Errorf("err:%v", err)
			e := delUser()
			if e != nil {
				log.Errorf("err:%v", e)
				return nil, e
			}
			return nil, err
		}
	} else {
		err = addUser()
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
	}
	rsp.User = u
	return &rsp, nil
}

func createUser(ctx *rpc.Context, corpId, appId uint32, msgUser *qmsg.ModelUser) (*qmsg.ModelUser, error) {
	err := setUserDefaultFilter(msgUser)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	msgUser.CorpId = corpId
	msgUser.AppId = appId
	// 判断下帐号ID是否存在
	err = User.Where(DbLoginId, msgUser.LoginId).
		EnsureNotExists(ctx, qmsg.ErrLoginIdExisted)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	// TODO: 数量限制
	pwdPlainText := msgUser.Password
	msgUser.Password = userUtil.getSecretHash(pwdPlainText)

	msgUser.StrOpenId, err = GenAndCheckUserOpenId(ctx, corpId, appId)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	err = User.Create(ctx, msgUser)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	return msgUser, nil
}

func AddUserV2(ctx *rpc.Context, req *qmsg.AddUserReq) (*qmsg.AddUserRsp, error) {
	var rsp qmsg.AddUserRsp
	qu, err := getQUserCheck(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	u := req.User
	// 解密密码
	cryptRsp, err := datacrypt.Decrypt4RSA(ctx, &datacrypt.Decrypt4RSAReq{Ciphertext: u.Password})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	err = validatePassword(cryptRsp.Plaintext)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	u.Password = cryptRsp.Plaintext

	corpId, appId := qu.CorpId, qu.AppId
	msgUser, err := createUser(ctx, corpId, appId, u)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	uidList := req.User.ContactFilter.FilterUser.UidList
	if req.User.IsAssign && req.User.ContactFilter.FilterExtContact.ExtContactCondition == uint32(quan.AdvanceExtContactFilter_ExtContactConditionAll) {
		if len(uidList) > 0 {
			err = batchCreateUserRobot(ctx, corpId, appId, u.Id, uidList)
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, err
			}
		}
	}

	rsp.User = msgUser

	err = callbackKefuInfoChange(ctx, msgUser.CorpId, msgUser.AppId, msgUser.LoginId, msgUser.StrOpenId)
	if err != nil {
		log.Errorf("err %v", err)
		return nil, err
	}
	return &rsp, nil
}
func SetUser(ctx *rpc.Context, req *qmsg.SetUserReq) (*qmsg.SetUserRsp, error) {
	var rsp qmsg.SetUserRsp
	return &rsp, nil
}

// 客服管理——详情：后端多返回一个hostaccount列表
func GetUser(ctx *rpc.Context, req *qmsg.GetUserReq) (*qmsg.GetUserRsp, error) {
	var rsp qmsg.GetUserRsp
	user, err := getQUserCheck(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	var u qmsg.ModelUser
	err = Db().FilterCorpAndAppByCtx(ctx).FilterCorpAndApp(user.CorpId, user.AppId).Eq(DbId, req.Id).NotFoundErr(qmsg.ErrUserNotFound).First(ctx, &u).Error()
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	var userRobotList []*qmsg.ModelUserRobot
	err = UserRobot.Where(map[string]interface{}{
		DbCorpId: user.CorpId,
		DbAppId:  user.AppId,
		DbUid:    req.Id,
	}).Find(ctx, &userRobotList)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	if len(userRobotList) > 0 {
		uidList := utils.PluckUint64(userRobotList, "RobotUid")
		listRsp, err := iquan.GetUserList(ctx, &iquan.GetUserListReq{
			ListOption: core.NewListOption().AddOpt(iquan.GetUserListReq_ListOptionUidList, uidList),
			CorpId:     user.CorpId,
			AppId:      user.AppId,
		})
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}

		rsp.UserList = listRsp.List
	}

	rsp.User = &u
	rsp.UserCategory, err = getUserCategory(ctx, &u)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	// 下面是兼容1.0
	getUidList := func(f func(acc *qmsg.ModelUserRobot) bool) []uint64 {
		return utils.PluckUint64(qmsg.UserRobotList(userRobotList).
			Filter(f), "RobotUid")
	}
	qrobotUidList := getUidList(filterRobot)
	// 旧的逻辑处理
	if len(qrobotUidList) > 0 {
		qrsp, err := qrobot.GetQRobotListSys(ctx, &qrobot.GetQRobotListSysReq{
			ListOption: core.NewListOption().
				SetSkipCount().
				AddOpt(uint32(qrobot.GetQRobotListSysReq_ListOptionUidList), qrobotUidList),
			AppId:  user.AppId,
			CorpId: user.CorpId,
		})
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
		rsp.RobotList = qrsp.List
	}
	rsp.StaffList, err = GetStaffListByUidList(ctx, user.CorpId, user.AppId, getUidList(filterHostAccount))
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	robotMap := map[uint64]*qrobot.ModelQRobot{}
	utils.KeyBy(rsp.RobotList, "Uid", &robotMap)
	rsp.RobotMap = robotMap

	return &rsp, nil
}

func GetOperatorLogList(ctx *rpc.Context, req *qmsg.GetOperatorLogListReq) (*qmsg.GetOperatorLogListRsp, error) {
	var rsp qmsg.GetOperatorLogListRsp

	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)

	db := OperatorLog.NewList(req.ListOption).WhereCorpApp(corpId, appId).OrderDesc(DbCreatedAt)

	err := core.NewListOptionProcessor(req.ListOption).
		AddTimeStampRange(
			qmsg.GetOperatorLogListReq_ListOptionCreatedAt,
			func(beginAt, endAt uint32) error {
				db.Where(fmt.Sprintf("? < %s AND %s < ?", DbCreatedAt, DbCreatedAt), beginAt, endAt)
				return nil
			}).
		AddUint64(
			qmsg.GetOperatorLogListReq_ListOptionUid,
			func(val uint64) error {
				db.Where(DbUid, val)
				return nil
			}).
		Process()
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	rsp.Paginate, err = db.FindPaginate(ctx, &rsp.List)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	return &rsp, nil
}

func GetUserList(ctx *rpc.Context, req *qmsg.GetUserListReq) (*qmsg.GetUserListRsp, error) {
	user, err := getQUserCheck(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	// TODO: 权限
	return getUserList(ctx, user.CorpId, user.AppId, req.ListOption)
}
func UpdateUserPassword(ctx *rpc.Context, req *qmsg.UpdateUserPasswordReq) (*qmsg.UpdateUserPasswordRsp, error) {
	var rsp qmsg.UpdateUserPasswordRsp

	// 解密密码
	cryptRsp, err := datacrypt.Decrypt4RSA(ctx, &datacrypt.Decrypt4RSAReq{Ciphertext: req.Password})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	err = validatePassword(cryptRsp.Plaintext)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	req.Password = cryptRsp.Plaintext

	err = Db().Model(&qmsg.ModelUser{}).FilterCorpAndAppByCtx(ctx).Eq(DbId, req.Id).Update(ctx, map[string]interface{}{
		DbPassword: userUtil.getSecretHash(req.Password),
	}).Error()
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	return &rsp, nil
}

func GetRobotList(ctx *rpc.Context, req *qmsg.GetRobotListReq) (*qmsg.GetRobotListRsp, error) {
	var rsp qmsg.GetRobotListRsp
	user, err := getQUserCheck(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	option := req.ListOption
	option.AddOpt(qrobot.GetQrobotListReq_ListOptionState,
		qrobot.GetQrobotListReq_StateNormal)
	robotRsp, err := qrobot.GetQrobotList(ctx, &qrobot.GetQrobotListReq{
		ListOption:   option,
		OnlyUserList: true,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	userRobotList, err := UserRobot.GetAllRobotList(ctx, user.CorpId, user.AppId)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	utils.KeyBy(userRobotList, "RobotUid", &rsp.DistributeMap)
	rsp.RobotList = robotRsp.List
	return &rsp, nil
}
func GetUserRobotList(ctx *rpc.Context, req *qmsg.GetUserRobotListReq) (*qmsg.GetUserRobotListRsp, error) {
	var rsp qmsg.GetUserRobotListRsp
	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)
	uid, err := core.GetPinHeaderUId2Int(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	//用户禁用校验
	user, err := User.get(ctx, corpId, appId, uid)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	if user.IsDisable {
		return nil, rpc.CreateError(qmsg.ErrUserDisabled)
	}
	var robotList []*qmsg.ModelUserRobot
	err = UserRobot.Where(map[string]interface{}{
		DbCorpId: corpId,
		DbAppId:  appId,
		DbUid:    user.Id,
	}).Find(ctx, &robotList)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	if len(robotList) > 0 {
		// 兼容旧版本的逻辑
		robotMap := map[uint64]*qrobot.ModelQRobot{}
		{
			robotUidList := utils.PluckUint64(qmsg.UserRobotList(robotList).
				Filter(func(robot *qmsg.ModelUserRobot) bool {
					return robot.RobotType == 0 || robot.RobotType == uint32(qmsg.RobotType_RobotTypePlatform)
				}), "RobotUid")
			rsp.UserRobotList, err = Robot.GetRobotListByRobotUid(ctx, corpId, appId, robotUidList)
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, err
			}
			utils.KeyBy(rsp.UserRobotList, "Uid", &robotMap)
		}
		// 新逻辑
		{
			uidList := utils.PluckUint64(robotList, "RobotUid")
			accountList, err := GetRobotAccountByUidList(ctx, corpId, appId, uidList)
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, err
			}

			quanUserList, err := iquan.GetUserList(ctx, &iquan.GetUserListReq{
				ListOption: core.NewListOption().SetSkipCount().AddOpt(iquan.GetUserListReq_ListOptionUidList, uidList),
				CorpId:     corpId,
				AppId:      appId,
			})
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, err
			}
			userMap := utils.PluckUint64Map(quanUserList.List, "Id")
			accountList = qmsg.AccountList(accountList).Filter(func(account *qmsg.Account) bool {
				return userMap[account.Uid]
			})

			qmsg.AccountList(accountList).Each(func(acc *qmsg.Account) {
				if acc.Uid == 0 {
					rsp.AccountList = append(rsp.AccountList, acc)
					return
				}
				robot := robotMap[acc.Uid]
				if robot == nil {
					rsp.AccountList = append(rsp.AccountList, acc)
					return
				}
				newAcc := coverQRobot2Account(robot)
				newAcc.Id = acc.Id
				rsp.AccountList = append(rsp.AccountList, newAcc)
			})
			hostRsp, err := qwhale.GetHostAccountListSys(ctx, &qwhale.GetHostAccountListSysReq{
				ListOption: core.NewListOption().AddOpt(qwhale.GetHostAccountListSysReq_ListOptionUidList, uidList),
				//AddOpt(qwhale.GetHostAccountListSysReq_ListOptionWithTrash, true),
				CorpId: corpId,
				AppId:  appId,
			})
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, err
			}

			hostMap := map[uint64]*qwhale.ModelHostAccount{}
			utils.KeyBy(hostRsp.List, "Uid", &hostMap)
			rsp.HostAccountMap = hostMap
		}
	}
	// 拿到所有的Uid
	var uidList pie.Uint64s
	for _, account := range rsp.AccountList {
		uidList = append(uidList, account.Uid)
	}
	for _, robot := range rsp.UserRobotList {
		uidList = append(uidList, robot.Uid)
	}
	for _, account := range rsp.HostAccountMap {
		uidList = append(uidList, account.Uid)
	}
	if len(uidList) > 0 {
		listRsp, err := iquan.GetUserList(ctx, &iquan.GetUserListReq{
			ListOption: core.NewListOption().AddOpt(iquan.GetUserListReq_ListOptionUidList, uidList.Unique()),
			CorpId:     corpId,
			AppId:      appId,
		})
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
		rsp.LicenseMap = listRsp.LicenseMap
	}
	return &rsp, nil
}

func UpdateUserName(ctx *rpc.Context, req *qmsg.UpdateUserNameReq) (*qmsg.UpdateUserNameRsp, error) {
	var rsp qmsg.UpdateUserNameRsp
	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)
	err := Db().Model(&qmsg.ModelUser{}).FilterCorpAndAppByCtx(ctx).Eq(DbId, req.Id).
		Update(ctx, map[string]interface{}{
			DbUsername: req.Username,
		}).Error()
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	user, err := User.get(ctx, corpId, appId, req.Id)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	err = callbackKefuInfoChange(ctx, user.CorpId, user.AppId, user.LoginId, user.StrOpenId)
	if err != nil {
		log.Errorf("err %v", err)
		return nil, err
	}
	return &rsp, nil
}

func UpdateUserStatus(ctx *rpc.Context, req *qmsg.UpdateUserStatusReq) (*qmsg.UpdateUserStatusRsp, error) {
	var rsp qmsg.UpdateUserStatusRsp
	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)
	_, err := User.updateUser(ctx, corpId, appId, req.Id, map[string]interface{}{
		DbIsDisable: req.IsDisable,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	user, err := User.get(ctx, corpId, appId, req.Id)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	if !user.IsAssign {
		if req.IsDisable {
			roundMgr.RemoveRing(ctx, corpId, appId, user.Id)
		} else {
			roundMgr.AddRing(ctx, corpId, appId, user.Id)
		}
	}
	opName := "启用"
	if req.IsDisable {
		opName = "禁用"
		if !user.IsAssign {
			err = deleteAndResignChat(ctx, corpId, appId, req.Id)
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, err
			}

			_, err = TopChat.deleteByUidList(ctx, corpId, appId, []uint64{req.Id})
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, err
			}
		}

		wrapper := &qmsg.WsMsgWrapper{
			MsgType: uint32(qmsg.WsMsgWrapper_MsgTypeUserDisabled),
		}
		err = pushWsMsg(ctx, corpId, appId, req.Id, []*qmsg.WsMsgWrapper{wrapper})
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
	}

	err = callbackKefuInfoChange(ctx, user.CorpId, user.AppId, user.LoginId, user.StrOpenId)
	if err != nil {
		log.Errorf("err %v", err)
		return nil, err
	}
	opUser, err := quan.GetUserCheck(ctx)
	//记录操作记录
	opReq := qoperationrecord.AddBizOperationRecordSysReq{
		CorpId:         opUser.CorpId,
		AppId:          opUser.AppId,
		OpType:         uint32(qoperationrecord.ModelBizOperationRecord_OperationTypeDelete),
		ModuleCode:     uint32(qoperationrecord.ModelBizOperationRecord_ModuleCustomerServiceManageList),
		OpTime:         utils.Now(),
		OperatorUid:    opUser.Id,
		OperatorName:   opUser.WwUserName,
		OperatorAvatar: opUser.WwAvatar,
		StrDetail:      fmt.Sprintf("【%s】%s了客服账号【%s】", opUser.WwUserName, opName, user.Username),
	}
	_, err = qoperationrecord.AddBizOperationRecordSys(ctx, &opReq)
	if err != nil {
		log.Errorf("err:%v", err)
	}

	return &rsp, nil
}

func GetUserStat(ctx *rpc.Context, _ *qmsg.GetUserStatReq) (*qmsg.GetUserStatRsp, error) {
	var rsp qmsg.GetUserStatRsp
	u, err := getQUserCheck(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	rsp.TotalUserCount, err = User.WhereCorpApp(u.CorpId, u.AppId).Count(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	rsp.EnableUserCount, err = User.WhereCorpApp(u.CorpId, u.AppId).Where(DbIsDisable, false).Count(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	all, err := s.RedisGroup.HGetAll(genWsConnListRedisKey(u.CorpId, u.AppId))
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	log.Infof("all %+v", all)
	rsp.OnlineUserCount = uint32(len(all))

	rsp.WsAndOnlineCount, err = getWsAndOnlineCount(ctx, u.CorpId, u.AppId)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	return &rsp, nil
}
func GetRobotAccountByUidList(ctx *rpc.Context, corpId, appId uint32, uidList []uint64) (list []*qmsg.Account, err error) {
	if len(uidList) == 0 {
		return nil, err
	}
	//accMap := map[uint64]*qmsg.Account{}
	//{
	qRsp, err := qrobot.GetAccountListSys(ctx, &qrobot.GetAccountListSysReq{
		ListOption: core.NewListOption().SetSkipCount().
			AddOpt(uint32(qrobot.GetAccountListSysReq_ListOptionUidList), uidList),
		AppId:  appId,
		CorpId: corpId,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	qrobot.AccountList(qRsp.List).
		Each(func(acc *qrobot.ModelAccount) {
			list = append(list, coverAccountQrobot2Qmsg(acc))
		})
	//}
	//{
	//	qRsp, err := iquan.GetUserList(ctx, &iquan.GetUserListReq{
	//		ListOption: core.NewListOption().SetSkipCount().
	//			AddOpt(uint32(iquan.GetUserListReq_ListOptionUidList), uidList),
	//		AppId:  appId,
	//		CorpId: corpId,
	//	})
	//	if err != nil {
	//		log.Errorf("err:%v", err)
	//		return nil, err
	//	}
	//
	//	quan.UserList(qRsp.List).
	//		Each(func(user *quan.ModelUser) {
	//			acc := accMap[user.Id]
	//			if acc == nil {
	//				acc = coverUser2Account(user)
	//			}
	//			// TODO acc非空，merge一下信息
	//
	//			list = append(list, acc)
	//		})
	//}
	return list, nil
}
func getUserList(ctx *rpc.Context, corpId, appId uint32, listOption *core.ListOption) (*qmsg.GetUserListRsp, error) {
	var rsp qmsg.GetUserListRsp

	var (
		rspWithLogin      bool
		rspWithChatCount  bool
		hasValidRobotName = true
	)

	scope := User.NewList(listOption).
		WhereCorpApp(corpId, appId).
		OrderDesc(DbId)
	err := core.NewListOptionProcessor(listOption).
		AddString(
			qmsg.GetUserListReq_ListOptionName,
			func(val string) error {
				cond := map[string]interface{}{}

				cond[fmt.Sprintf("%s LIKE", DbUsername)] = fmt.Sprintf("%%%s%%", val)

				listRsp, err := iquan.GetUserList(ctx, &iquan.GetUserListReq{
					ListOption: core.NewListOption().AddOpt(iquan.GetUserListReq_ListOptionName, val),
					CorpId:     corpId,
					AppId:      appId,
				})
				if err != nil {
					log.Errorf("err:%v", err)
					return err
				}

				if len(listRsp.List) > 0 {
					robotUidList := utils.PluckUint64(listRsp.List, "Id")

					var userRobotList []*qmsg.ModelUserRobot
					err = UserRobot.WhereCorpApp(corpId, appId).WhereIn(DbRobotUid, robotUidList).Find(ctx, &userRobotList)
					if err != nil {
						log.Errorf("err:%v", err)
						return err
					}

					if len(userRobotList) > 0 {
						cond[fmt.Sprintf("%s in", DbId)] = utils.PluckUint64(userRobotList, "Uid")
					}
				}

				scope.OrWhere(cond)

				return nil
			}).
		AddBool(
			qmsg.GetUserListReq_ListOptionIsDisable,
			func(val bool) error {
				scope.Where(DbIsDisable, val)
				return nil
			}).
		AddTimeStampRange(
			qmsg.GetUserListReq_ListOptionCreatedAt,
			func(beginAt, endAt uint32) error {
				scope.Where(fmt.Sprintf("? < %s AND %s < ?", DbCreatedAt, DbCreatedAt), beginAt, endAt)
				return nil
			}).
		AddBool(
			qmsg.GetUserListReq_ListOptionIsOrderByAsc,
			func(val bool) error {
				if val {
					scope.ResetOrderAsc(DbId)
				} else {
					scope.ResetOrderDesc(DbId)
				}
				return nil
			}).
		AddBool(
			qmsg.GetUserListReq_ListOptionIsOnline,
			func(val bool) error {
				scope.Where(DbIsOnline, val)
				return nil
			}).
		AddBool(
			qmsg.GetUserListReq_ListOptionIsOnWs,
			func(val bool) error {
				all, err := s.RedisGroup.HGetAll(genWsConnListRedisKey(corpId, appId))
				if err != nil {
					if err == redis.Nil {
						scope.Where(1, 2)
						return nil
					}
					log.Errorf("err:%v", err)
					return err
				}

				var uidList []string
				for user, _ := range all {
					uidList = append(uidList, user)
				}

				if val {
					scope.WhereIn(DbId, uidList)
				} else {
					scope.WhereNotIn(DbId, uidList)
				}
				rspWithLogin = true
				return nil
			}).
		AddBool(
			qmsg.GetUserListReq_ListOptionRspWithLogin,
			func(val bool) error {
				rspWithLogin = val
				return nil
			}).
		AddBool(
			qmsg.GetUserListReq_ListOptionRspWithChatCount,
			func(val bool) error {
				rspWithChatCount = val
				return nil
			}).
		AddString(
			qmsg.GetUserListReq_ListOptionLoginID,
			func(val string) error {
				if len(val) > 0 {
					scope.Where(DbLoginId, val)
				}
				return nil
			}).
		AddString(
			qmsg.GetUserListReq_ListOptionRobotName,
			func(val string) error {
				if len(val) == 0 {
					return nil
				}
				getRobotListV2Rsp, err := qops.GetRobotListV2(ctx, &qops.GetRobotListV2Req{
					ListOption: core.NewListOption().
						AddOpt(qops.GetRobotListV2Req_ListOptionName, val).
						SetOffset(uint32(0)).
						SetLimit(uint32(100)),
					CorpId:    corpId,
					RobotType: uint32(0),
				})
				if err != nil {
					log.Errorf("err:%v", err)
					hasValidRobotName = false
					return nil
				}
				log.Debugf("GetRobotListV2Rsp.List len is %d", len(getRobotListV2Rsp.List))

				if len(getRobotListV2Rsp.List) == 0 {
					hasValidRobotName = false
					return nil
				}
				robotIds := utils.PluckUint64(getRobotListV2Rsp.List, "Id")
				log.Debugf("robotIds len is %d", len(robotIds))

				uIds := utils.PluckUint64(getRobotListV2Rsp.List, "Uid")
				var userList []*qmsg.ModelUserRobot
				err = UserRobot.WhereCorpApp(corpId, appId).WhereIn(DbRobotUid, uIds).Find(ctx, &userList)
				if err != nil {
					log.Errorf("err:%v", err)
					hasValidRobotName = false
					return nil
				}
				userIds := utils.PluckUint64(userList, "Uid")
				scope.WhereIn(DbId, userIds)
				return nil
			}).
		AddUint64(
			qmsg.GetUserListReq_ListOptionUserCategoryId,
			func(val uint64) error {
				scope.Where(DbUserCategoryId, val)
				return nil
			}).
		AddString(
			qmsg.GetUserListReq_ListOptionStrOpenId,
			func(val string) error {
				if len(val) > 0 {
					scope.Where(DbStrOpenId, val)
				}
				return nil
			}).
		AddUint64List(
			qmsg.GetUserListReq_ListOptionUserCategoryIdList,
			func(valList []uint64) error {
				scope.WhereIn(DbUserCategoryId, valList)
				return nil
			}).
		Process()
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	log.Debugf("hasValidRobotName:%v", hasValidRobotName)
	if !hasValidRobotName {
		return &rsp, nil
	}

	rsp.Paginate, err = scope.FindPaginate(ctx, &rsp.List)
	log.Debugf("qmsg.GetUserListRsp.List len is %d", len(rsp.List))
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	rsp.RobotMap = map[uint64]*qmsg.GetUserListRsp_RobotList{}
	if len(rsp.List) > 0 {
		var userCategoryIdList []uint64
		for _, item := range rsp.List {
			if item.UserCategoryId > 0 {
				userCategoryIdList = append(userCategoryIdList, item.UserCategoryId)
			}
		}
		if len(userCategoryIdList) > 0 {
			var userCategoryList []*qmsg.ModelUserCategory
			err = UserCategory.WhereCorpApp(corpId, appId).WhereIn(DbId, userCategoryIdList).Find(ctx, &userCategoryList)
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, err
			}
			rsp.UserCategoryMap = make(map[uint64]string)
			for _, item := range userCategoryList {
				rsp.UserCategoryMap[item.Id] = item.Name
			}
		}
		uidList := utils.PluckUint64(rsp.List, "Id")
		var userRobotList []*qmsg.ModelUserRobot
		err = UserRobot.WhereCorpApp(corpId, appId).
			WhereIn(DbUid, uidList).
			Find(ctx, &userRobotList)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
		robotUidList := utils.PluckUint64(userRobotList, "RobotUid")
		// 拉下机器人信息
		if len(robotUidList) > 0 {
			getUserListRsp, err := iquan.GetUserList(ctx, &iquan.GetUserListReq{
				ListOption: core.NewListOption().
					AddOpt(iquan.GetUserListReq_ListOptionUidList, robotUidList),
				CorpId: corpId,
				AppId:  appId,
			})
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, err
			}
			userList := getUserListRsp.List
			var userMap map[uint64]*quan.ModelUser
			utils.KeyBy(userList, "Id", &userMap)
			accOnlineMap := map[uint64]bool{}
			{
				hostAccUidList := utils.PluckUint64(quan.UserList(userList).
					FilterNot(func(user *quan.ModelUser) bool {
						return user.IsRobot
					}), "Id")
				if len(hostAccUidList) > 0 {
					qRsp, err := qwhale.GetHostAccountListSys(ctx, &qwhale.GetHostAccountListSysReq{
						ListOption: core.NewListOption().SetSkipCount().AddOpt(qwhale.GetHostAccountListSysReq_ListOptionUidList, hostAccUidList),
						CorpId:     corpId,
						AppId:      appId,
					})
					if err != nil {
						log.Errorf("err:%v", err)
						return nil, err
					}
					qwhale.HostAccountList(qRsp.List).
						Each(func(acc *qwhale.ModelHostAccount) {
							accOnlineMap[acc.Uid] = acc.IsOnline
						})
				}
			}
			// 平台号的处理
			robotMap := map[uint64]*qrobot.ModelQRobot{}
			{
				uidList := utils.PluckUint64(quan.UserList(userList).
					Filter(func(user *quan.ModelUser) bool {
						return user.IsRobot
					}), "Id")
				if len(uidList) > 0 {
					qRsp, err := qrobot.GetQRobotListSys(ctx, &qrobot.GetQRobotListSysReq{
						ListOption: core.NewListOption().SetSkipCount().
							AddOpt(qrobot.GetQRobotListSysReq_ListOptionUidList, uidList),
						CorpId: corpId,
						AppId:  appId,
					})
					if err != nil {
						log.Errorf("err:%v", err)
						return nil, err
					}
					utils.KeyBy(qRsp.List, "Uid", &robotMap)
				}
			}
			for _, userRobot := range userRobotList {
				r := rsp.RobotMap[userRobot.Uid]
				if r == nil {
					r = &qmsg.GetUserListRsp_RobotList{}
				}
				if u, ok := userMap[userRobot.RobotUid]; ok {
					item := &qmsg.GetUserListRsp_Robot{
						Uid:  userRobot.RobotUid,
						Name: u.WwUserName,
					}
					if u.IsRobot {
						item.RobotType = uint32(qmsg.RobotType_RobotTypePlatform)
						item.IsOnline = true
						robot := robotMap[u.Id]
						if robot != nil {
							switch qrobot.ModelQRobot_RobotState(robot.RobotState) {
							case qrobot.ModelQRobot_RobotStateNil, qrobot.ModelQRobot_RobotStateNormal:
								item.IsOnline = true
								item.State = uint32(qmsg.Staff_StateNormal)
							case qrobot.ModelQRobot_RobotStateBaned:
								item.State = uint32(qmsg.Staff_StateBaned)
							}
						}
					} else {
						item.RobotType = uint32(qmsg.RobotType_RobotTypeHostAccount)
						item.IsOnline = accOnlineMap[u.Id]
						item.State = uint32(qmsg.Staff_StateNormal)
					}
					r.RobotList = append(r.RobotList, item)
				}
				rsp.RobotMap[userRobot.Uid] = r
			}
		}

		if rspWithLogin {
			loginMap := map[uint64]bool{}

			all, err := s.RedisGroup.HGetAll(genWsConnListRedisKey(corpId, appId))
			if err != nil && err != redis.Nil {
				log.Errorf("err:%v", err)
				return nil, err
			}

			for key, _ := range all {
				uid, _ := strconv.ParseUint(key, 0, 64)
				loginMap[uid] = true
			}

			rsp.LoginMap = loginMap
		}

		if rspWithChatCount {
			chatCountMap := map[uint64]uint32{}
			var assignChatUidList []uint64
			for _, user := range rsp.List {
				if !user.IsAssign {
					assignChatUidList = append(assignChatUidList, user.Id)
				}
			}

			if len(assignChatUidList) > 0 {
				var assignChatList []*qmsg.ModelAssignChat
				err = AssignChat.WhereCorpApp(corpId, appId).WhereIn(DbUid, assignChatUidList).Find(ctx, &assignChatList)
				if err != nil {
					log.Errorf("err:%v", err)
					return nil, err
				}

				for _, assignChat := range assignChatList {
					if _, ok := chatCountMap[assignChat.Uid]; ok {
						chatCountMap[assignChat.Uid]++
					} else {
						chatCountMap[assignChat.Uid] = 1
					}
				}
				rsp.ChatCountMap = chatCountMap
			}
		}
	}
	return &rsp, nil
}
func GetUserListStaff(ctx *rpc.Context, req *qmsg.GetUserListStaffReq) (*qmsg.GetUserListStaffRsp, error) {
	var rsp qmsg.GetUserListStaffRsp
	listRsp, err := getUserList(ctx, req.CorpId, req.AppId, req.ListOption)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	rsp = qmsg.GetUserListStaffRsp{
		Paginate: listRsp.Paginate,
		List:     listRsp.List,
	}
	rsp.RobotMap = map[uint64]*qmsg.GetUserListStaffRsp_RobotList{}
	for key, robotList := range listRsp.RobotMap {
		rsp.RobotMap[key] = &qmsg.GetUserListStaffRsp_RobotList{
			RobotList: []*qmsg.GetUserListStaffRsp_Robot{},
		}
		for _, robot := range robotList.RobotList {
			rsp.RobotMap[key].RobotList = append(rsp.RobotMap[key].RobotList, &qmsg.GetUserListStaffRsp_Robot{
				Uid:       robot.Uid,
				Name:      robot.Name,
				RobotType: robot.RobotType,
				IsOnline:  robot.IsOnline,
				State:     robot.State,
			})
		}
	}
	return &rsp, nil
}

func GetUserRobotListSys(ctx *rpc.Context, req *qmsg.GetUserRobotListSysReq) (*qmsg.GetUserRobotListSysRsp, error) {
	var rsp qmsg.GetUserRobotListSysRsp

	db := UserRobot.WhereCorpApp(req.CorpId, req.AppId)
	if req.RobotUid > 0 && len(req.RobotUidList) > 0 {
		req.RobotUidList = append(req.RobotUidList, req.RobotUid)
		db.WhereIn(DbRobotUid, req.RobotUidList)
	} else if req.RobotUid > 0 {
		db.Where(DbRobotUid, req.RobotUid)
	} else if len(req.RobotUidList) > 0 {
		db.WhereIn(DbRobotUid, req.RobotUidList)
	}
	err := db.Find(ctx, &rsp.UserRobotList)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	return &rsp, nil
}

func SetOnline(ctx *rpc.Context, req *qmsg.SetOnlineReq) (*qmsg.SetOnlineRsp, error) {
	var rsp qmsg.SetOnlineRsp

	u, err := getUserCheck(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	_, err = User.WhereCorpApp(u.CorpId, u.AppId).Where(DbId, u.Id).Update(ctx, map[string]interface{}{
		DbIsOnline: req.IsOnline,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	err = setUserWorkStateOperateTime(u)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	var logType qmsg.UserLog_LogType
	if req.IsOnline {
		if !u.IsAssign {
			roundMgr.AddRing(ctx, u.CorpId, u.AppId, u.Id)
		}
		logType = qmsg.UserLog_LogTypeOnline
		_, err = assignChatForLogin(ctx, u)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
	} else {
		//if !u.IsAssign {
		//	roundMgr.RemoveRing(ctx, u.CorpId, u.AppId, u.Id)
		//}
		logType = qmsg.UserLog_LogTypeOffline
		err = resignChat4Offline(ctx, u.CorpId, u.AppId, u.Id)
		if err != nil {
			log.Errorf("err %v", err)
			return nil, err
		}
	}

	_, err = OperatorLog.create(ctx, u.CorpId, u.AppId, u.Id, logType)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	user, err := User.get(ctx, u.CorpId, u.AppId, u.Id)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	//新增回调
	err = callbackKefuStateChange(ctx, u.CorpId, u.AppId, user.LoginId, user.StrOpenId, req.IsOnline)
	if err != nil {
		log.Errorf("err %v", err)
	}

	return &rsp, nil
}

func UpdateUserLoginId(ctx *rpc.Context, req *qmsg.UpdateUserLoginIdReq) (*qmsg.UpdateUserLoginIdRsp, error) {
	var rsp qmsg.UpdateUserLoginIdRsp

	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)

	err := User.Where(DbLoginId, req.LoginId).
		EnsureNotExists(ctx, qmsg.ErrLoginIdExisted)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	_, err = User.WhereCorpApp(corpId, appId).Where(DbId, req.Id).
		Update(ctx, map[string]interface{}{
			DbLoginId: req.LoginId,
		})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	user, err := User.get(ctx, corpId, appId, req.Id)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	err = callbackKefuInfoChange(ctx, user.CorpId, user.AppId, user.LoginId, user.StrOpenId)
	if err != nil {
		log.Errorf("err %v", err)
		return nil, err
	}

	return &rsp, nil
}

func UpdateUserAssign(ctx *rpc.Context, req *qmsg.UpdateUserAssignReq) (*qmsg.UpdateUserAssignRsp, error) {
	var rsp qmsg.UpdateUserAssignRsp
	var err error

	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)

	var user qmsg.ModelUser
	err = User.WhereCorpApp(corpId, appId).Where(DbId, req.Id).First(ctx, &user)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	err = updateUserAssign(ctx, corpId, appId, &user, req.ContactFilter, req.GroupFilter, req.Id, req.IsAssign)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	opUser, err := quan.GetUserCheck(ctx)
	//记录操作记录
	opReq := qoperationrecord.AddBizOperationRecordSysReq{
		CorpId:         opUser.CorpId,
		AppId:          opUser.AppId,
		OpType:         uint32(qoperationrecord.ModelBizOperationRecord_OperationTypeModify),
		ModuleCode:     uint32(qoperationrecord.ModelBizOperationRecord_ModuleCustomerServiceManageList),
		OpTime:         utils.Now(),
		OperatorUid:    opUser.Id,
		OperatorName:   opUser.WwUserName,
		OperatorAvatar: opUser.WwAvatar,
		StrDetail:      fmt.Sprintf("【%s】修改了客服账号【%s】的分配资产", opUser.WwUserName, user.Username),
	}
	_, err = qoperationrecord.AddBizOperationRecordSys(ctx, &opReq)
	if err != nil {
		log.Errorf("err:%v", err)
	}
	return &rsp, nil
}

func batchCreateUserRobot(ctx *rpc.Context, corpId, appId uint32, uid uint64, robotUidList []uint64) error {
	var addList []*qmsg.ModelUserRobot

	iRsp, err := iquan.GetUserList(ctx, &iquan.GetUserListReq{
		ListOption: core.NewListOption().SetSkipCount().AddOpt(iquan.GetUserListReq_ListOptionUidList, robotUidList),
		CorpId:     corpId,
		AppId:      appId,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	quan.UserList(iRsp.List).Each(func(user *quan.ModelUser) {
		r := &qmsg.ModelUserRobot{
			RobotUid: user.Id,
			CorpId:   corpId,
			AppId:    appId,
			Uid:      uid,
		}
		if user.IsRobot {
			r.RobotType = uint32(qmsg.RobotType_RobotTypePlatform)
		} else {
			r.RobotType = uint32(qmsg.RobotType_RobotTypeHostAccount)
		}
		addList = append(addList, r)
	})

	err = UserRobot.batchCreate(ctx, corpId, appId, addList, robotUidList)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	return nil
}

func setUserDefaultFilter(user *qmsg.ModelUser) error {
	if user.IsAssign {
		if user.ContactFilter == nil {
			user.ContactFilter = &quan.AdvanceExtContactFilter{
				FilterUser: &quan.AdvanceExtContactFilter_FilterUser{
					SelectType:     uint32(quan.SelectType_SelectTypeAssign),
					UidList:        []uint64{},
					UserSelectType: uint32(quan.UserSelectType_UserSelectTypeAll),
				},
				FilterExtContact: &quan.AdvanceExtContactFilter_FilterExtContact{
					ExtContactCondition: uint32(quan.AdvanceExtContactFilter_ExtContactConditionAll),
					Filter:              nil,
					ExtUidList:          nil,
					FollowIdList:        nil,
				},
			}
		}
		if user.GroupFilter == nil {
			user.GroupFilter = &qmsg.GroupFilter{
				SelectType:          uint32(quan.SelectType_SelectTypeAll),
				GroupCategoryIdList: nil,
				GroupIdList:         nil,
			}
		}
	}

	return nil
}

func GetAssignedRobotUidList(ctx *rpc.Context, req *qmsg.GetAssignedRobotUidListReq) (*qmsg.GetAssignedRobotUidListRsp, error) {
	var rsp qmsg.GetAssignedRobotUidListRsp
	return &rsp, nil
}

func SetUserRobots(ctx *rpc.Context, req *qmsg.SetUserRobotsReq) (*qmsg.SetUserRobotsRsp, error) {
	var rsp qmsg.SetUserRobotsRsp
	var err error
	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)
	isChanged := false
	if len(req.RobotUidList) == 0 {
		err = Db().FilterCorpAndAppByCtx(ctx).Model(&qmsg.ModelUserRobot{}).
			Eq(DbUid, req.Id).Delete(ctx).Error()
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
		return &rsp, nil
	} else if len(req.RobotUidList) > 0 {
		iRsp, err := iquan.GetUserList(ctx, &iquan.GetUserListReq{
			ListOption: core.NewListOption().SetSkipCount().AddOpt(iquan.GetUserListReq_ListOptionUidList, req.RobotUidList),
			CorpId:     corpId,
			AppId:      appId,
		})
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
		robotUidList := utils.PluckUint64(quan.UserList(iRsp.List).Filter(func(user *quan.ModelUser) bool {
			return user.IsRobot
		}), "Id")
		accUidList := utils.PluckUint64(quan.UserList(iRsp.List).FilterNot(func(user *quan.ModelUser) bool {
			return user.IsRobot
		}), "Id")
		existRobotList, err := UserRobot.GetRobotList(ctx, corpId, appId, req.Id)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
		var addList []*qmsg.ModelUserRobot
		var addUidList, delUidList []uint64
		{
			// 处理圈客宝
			oldList := pie.Uint64s(utils.PluckUint64(existRobotList.Filter(filterRobot), "RobotUid"))
			add, del := oldList.Diff(robotUidList)
			delUidList = append(delUidList, del...)
			add.Each(func(uid uint64) {
				addUidList = append(addUidList, uid)
				addList = append(addList, &qmsg.ModelUserRobot{
					Uid:       req.Id,
					AppId:     appId,
					CorpId:    corpId,
					RobotUid:  uid,
					RobotType: uint32(qmsg.RobotType_RobotTypePlatform),
				})
			})
		}
		{
			// 鲸量
			oldList := pie.Uint64s(utils.PluckUint64(existRobotList.Filter(filterHostAccount), "RobotUid"))
			add, del := oldList.Diff(accUidList)
			delUidList = append(delUidList, del...)
			add.Each(func(uid uint64) {
				addUidList = append(addUidList, uid)
				addList = append(addList, &qmsg.ModelUserRobot{
					CorpId:    corpId,
					AppId:     appId,
					Uid:       req.Id,
					RobotUid:  uid,
					RobotType: uint32(qmsg.RobotType_RobotTypeHostAccount),
				})
			})
		}
		if len(addList) > 0 {
			isChanged = true
			existCount, err := UserRobot.
				WhereCorpApp(corpId, appId).
				WhereIn(DbRobotUid, addUidList).
				Count(ctx)
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, err
			}
			if existCount > 0 {
				return nil, rpc.CreateError(qmsg.ErrRobotHasDistribute)
			}
			err = UserRobot.batchCreate(ctx, corpId, appId, addList, addUidList)
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, err
			}
		}
		if len(delUidList) > 0 {
			isChanged = true
			_, err = UserRobot.WhereCorpApp(corpId, appId).
				Where(DbUid, req.Id).WhereIn(DbRobotUid, delUidList).Delete(ctx)
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, err
			}
		}
	}
	if isChanged {
		wrapper := &qmsg.WsMsgWrapper{
			MsgType: uint32(qmsg.WsMsgWrapper_MsgTypeUserRobotsChanged),
		}
		err = pushWsMsg(ctx, corpId, appId, req.Id, []*qmsg.WsMsgWrapper{wrapper})
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
	}
	return &rsp, nil
}

func GetUserListV2(ctx *rpc.Context, req *qmsg.GetUserListV2Req) (*qmsg.GetUserListV2Rsp, error) {
	var rsp qmsg.GetUserListV2Rsp

	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)

	db := User.NewList(req.ListOption).WhereCorpApp(corpId, appId)

	err := core.NewListOptionProcessor(req.ListOption).
		AddString(
			qmsg.GetUserListV2Req_ListOptionName,
			func(val string) error {
				db.WhereLike(DbUsername, fmt.Sprintf("%%%s%%", val))
				return nil
			}).
		AddBool(
			qmsg.GetUserListV2Req_ListOptionIsDisable,
			func(val bool) error {
				db.Where(DbIsDisable, val)
				return nil
			}).
		AddBool(
			qmsg.GetUserListV2Req_ListOptionIsOnline,
			func(val bool) error {
				db.Where(DbIsOnline, val)
				return nil
			}).
		AddBool(
			qmsg.GetUserListV2Req_ListOptionIsAssign,
			func(val bool) error {
				db.Where(DbIsAssign, val)
				return nil
			}).
		AddUint64(
			qmsg.GetUserListV2Req_ListOptionNotUid,
			func(val uint64) error {
				db.Where(DbId, "!=", val)
				return nil
			}).
		Process()

	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	rsp.Paginate, err = db.FindPaginate(ctx, &rsp.List)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	return &rsp, nil
}

func GetAccountInfo(ctx *rpc.Context, req *qmsg.GetAccountInfoReq) (*qmsg.GetAccountInfoRsp, error) {
	var rsp qmsg.GetAccountInfoRsp

	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)

	listRsp, err := qwhale.GetHostAccountListSys(ctx, &qwhale.GetHostAccountListSysReq{
		ListOption: core.NewListOption().AddOpt(qwhale.GetHostAccountListSysReq_ListOptionUidList, req.UidList),
		CorpId:     corpId,
		AppId:      appId,
		IsUnscoped: false,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	infoMap := map[uint64]*qmsg.GetAccountInfoRsp_Info{}
	for _, account := range listRsp.List {
		infoMap[account.Uid] = &qmsg.GetAccountInfoRsp_Info{
			IsOnline:    account.IsOnline,
			LastLoginAt: account.LastLoginAt,
		}
	}
	rsp.InfoMap = infoMap

	return &rsp, nil
}

func UpdateUserDetail(ctx *rpc.Context, req *qmsg.UpdateUserDetailReq) (*qmsg.UpdateUserDetailRsp, error) {
	var rsp qmsg.UpdateUserDetailRsp

	user, err := getUserCheck(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	_, err = User.WhereCorpApp(user.CorpId, user.AppId).Where(DbId, user.Id).Update(ctx, map[string]interface{}{
		DbDetail: req.User.Detail,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	return &rsp, nil
}

func UnbindHostAccountMq(ctx *rpc.Context, req *smq.ConsumeReq) (*smq.ConsumeRsp, error) {
	return qmsg.MqUnbindHostAccountMsg.Process(ctx, req, handleUnbindHostAccount)
}

// @desc 扫码号注销处理
func handleUnbindHostAccount(ctx *rpc.Context, mqReq *smq.ConsumeReq, data interface{}) error {
	req := data.(*qwhale.UnbindHostAccountMqReq)

	// 并发控制
	lock := setUnbindHostAccountLock(req.Uid)
	if lock {
		return nil
	}
	defer func() {
		freeUnbindHostAccountLock(req.Uid)
	}()

	err := unbindHostAccount4User(ctx, req)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	err = unbindHostAccount4Assign(ctx, req)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	return nil
}

// 扫码号注销-有资产客服处理
func unbindHostAccount4User(ctx *rpc.Context, req *qwhale.UnbindHostAccountMqReq) error {
	// 查询该扫码号的所有资产分配记录
	userRobotList, err := UserRobot.getListByRobotUid(ctx, req.CorpId, req.AppId, req.Uid)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	if len(userRobotList) == 0 {
		return nil
	}
	for _, userRobot := range userRobotList {
		// 删除资产分配
		_, err = UserRobot.WhereCorpApp(userRobot.CorpId, userRobot.AppId).Where(DbId, userRobot.Id).Delete(ctx)
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
		wrapper := &qmsg.WsMsgWrapper{
			MsgType: uint32(qmsg.WsMsgWrapper_MsgTypeHostAccountLogout),
		}
		err = pushWsMsg(ctx, userRobot.CorpId, userRobot.AppId, userRobot.Uid, []*qmsg.WsMsgWrapper{wrapper})
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
	}
	return nil
}

// 扫码号注销-无资产客服处理
func unbindHostAccount4Assign(ctx *rpc.Context, req *qwhale.UnbindHostAccountMqReq) error {
	robotUidList := []uint64{req.Uid}
	assignChats, err := AssignChat.getListByRobotUidList(ctx, req.CorpId, req.AppId, robotUidList)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	if len(assignChats) == 0 {
		return nil
	}

	uidAssignChatMap := map[uint64][]*qmsg.ModelAssignChat{}
	for _, assignChat := range assignChats {
		if m, ok := uidAssignChatMap[assignChat.Uid]; ok {
			uidAssignChatMap[assignChat.Uid] = append(m, assignChat)
		} else {
			uidAssignChatMap[assignChat.Uid] = []*qmsg.ModelAssignChat{assignChat}
		}
	}

	for uid, assignChatList := range uidAssignChatMap {
		idList := utils.PluckUint64(assignChatList, "Id")
		err = AssignChat.setClosed(ctx, req.CorpId, req.AppId, map[string]interface{}{
			DbEndScene: qmsg.ModelAssignChat_EndSceneUserExit,
		}, idList...)
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}

		if uid == 0 {
			continue
		}

		cidList := utils.PluckUint64(assignChatList, "Cid")
		_, err = TopChat.deleteByUidCidList(ctx, req.CorpId, req.AppId, uid, cidList)
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}

		// 加入最近联系人列表
		err = LastContactChat.batchCreateOrUpdate(ctx, req.CorpId, req.AppId, idList)
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}

		wrapper := &qmsg.WsMsgWrapper{
			MsgType: uint32(qmsg.WsMsgWrapper_MsgTypeHostAccountLogout),
		}
		err = pushWsMsg(ctx, req.CorpId, req.AppId, uid, []*qmsg.WsMsgWrapper{wrapper})
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
	}
	return nil
}

// 设置扫码号注销同步锁
func setUnbindHostAccountLock(robotUid uint64) bool {
	key := fmt.Sprintf("qmsg_unbind_host_account_lock_%d", robotUid)
	val := make([]byte, 0)
	rst, err := s.RedisGroup.SetNX(key, val, 1*time.Minute)
	if err != nil {
		log.Errorf("err:%v", err)
		return false
	}
	return !rst
}

// 释放扫码号注销同步锁
func freeUnbindHostAccountLock(robotUid uint64) {
	key := fmt.Sprintf("qmsg_unbind_host_account_lock_%d", robotUid)
	err := s.RedisGroup.Del(key)
	if err != nil {
		log.Errorf("err:%v", err)
	}
}

// 设置用户修改工作状态的操作时间
func setUserWorkStateOperateTime(user *qmsg.ModelUser) error {
	todayDate := utils.GetTodayDate()
	nowMinutes := getNowMinutes()
	err := s.RedisGroup.SetJson(fmt.Sprintf(UserSetWorkState, todayDate, user.CorpId, user.Id), nowMinutes, time.Hour*24)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	return nil
}

// 获取用户修改工作状态的操作时间
func getUserWorkStateOperateTime(user *qmsg.ModelUser) (int, error) {
	todayDate := utils.GetTodayDate()
	redisKey := fmt.Sprintf(UserSetWorkState, todayDate, user.CorpId, user.Id)
	var operateTime int
	err := s.RedisGroup.GetJson(redisKey, &operateTime)
	if err != nil {
		if err == redis.Nil {
			return 0, nil
		}
		log.Errorf("err:%v", err)
		return operateTime, err
	}
	return operateTime, nil
}

// 删除用户修改工作状态的操作时间
func delUserWorkStateOperateTime(user *qmsg.ModelUser) error {
	todayDate := utils.GetTodayDate()
	err := s.RedisGroup.Del(fmt.Sprintf(UserSetWorkState, todayDate, user.CorpId, user.Id))
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	return nil
}

// 获取机器人对应所分配的客服
func GetUserByRobotUidList(ctx *rpc.Context, req *qmsg.GetUserByRobotUidListReq) (*qmsg.GetUserByRobotUidListRsp, error) {
	var rsp qmsg.GetUserByRobotUidListRsp

	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)
	var userRobotList []*qmsg.ModelUserRobot
	err := UserRobot.WhereCorpApp(corpId, appId).
		WhereIn(DbRobotUid, req.RobotUidList).
		Find(ctx, &userRobotList)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	// 分组取出所有机器人列表
	robotList := map[uint64][]*qmsg.ModelUserRobot{}
	for _, userRobot := range userRobotList {
		if robotList[userRobot.RobotUid] == nil {
			robotList[userRobot.RobotUid] = []*qmsg.ModelUserRobot{}
		}
		robotList[userRobot.RobotUid] = append(robotList[userRobot.RobotUid], userRobot)
	}
	// 每一组各查找客服
	rsp.RobotList = []*qmsg.GetUserByRobotUidListRsp_Robot{}
	for robotUid, userRobotList := range robotList {
		userIdList := utils.PluckUint64(userRobotList, "Uid")
		var userList []*qmsg.ModelUser
		err = User.WhereCorpApp(corpId, appId).
			WhereIn(DbId, userIdList).
			Find(ctx, &userList)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
		rsp.RobotList = append(rsp.RobotList, &qmsg.GetUserByRobotUidListRsp_Robot{
			RobotUid: robotUid,
			User:     userList,
		})
	}

	return &rsp, nil
}

func AddUserSys(ctx *rpc.Context, req *qmsg.AddUserSysReq) (*qmsg.AddUserSysRsp, error) {
	var rsp qmsg.AddUserSysRsp
	corpId, appId := req.CorpId, req.AppId
	userRsp, err := iquan.GetUser(ctx, &iquan.GetUserReq{CorpId: req.CorpId,
		AppId:    req.AppId,
		WwUserId: req.WwUserId,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	if !userRsp.User.InAppScope {
		err = rpc.CreateError(quan.ErrUserNotInAppScope)
		log.Errorf("err:%v", err)
		return nil, err
	}

	user := req.User
	if len(req.RobotUidList) > 0 {
		user.IsAssign = true
	}
	if user.IsAssign {
		if user.ContactFilter == nil {
			user.ContactFilter = &quan.AdvanceExtContactFilter{}
		}
		if user.ContactFilter.FilterExtContact == nil {
			user.ContactFilter.FilterExtContact = &quan.AdvanceExtContactFilter_FilterExtContact{
				ExtContactCondition: uint32(quan.AdvanceExtContactFilter_ExtContactConditionAll),
			}
		}
		if user.ContactFilter.FilterUser == nil {
			user.ContactFilter.FilterUser = &quan.AdvanceExtContactFilter_FilterUser{
				UserSelectType: uint32(quan.UserSelectType_UserSelectTypeAll),
				UidList:        req.RobotUidList,
			}
		}
	}
	user, err = createUser(ctx, req.CorpId, req.AppId, user)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	if user.ContactFilter != nil && user.ContactFilter.FilterUser != nil {
		uidList := user.ContactFilter.FilterUser.UidList
		if user.ContactFilter.FilterExtContact != nil {
			if user.IsAssign && user.ContactFilter.FilterExtContact.ExtContactCondition == uint32(quan.AdvanceExtContactFilter_ExtContactConditionAll) {
				if len(uidList) > 0 {
					err = batchCreateUserRobot(ctx, corpId, appId, user.Id, uidList)
					if err != nil {
						log.Errorf("err:%v", err)
						return nil, err
					}
				}
			}
		}
	}
	rsp.User = user
	return &rsp, nil
}

func GetUserListSys(ctx *rpc.Context, req *qmsg.GetUserListSysReq) (*qmsg.GetUserListRsp, error) {
	return getUserList(ctx, req.CorpId, req.AppId, req.ListOption)
}

func validatePassword(pwd string) error {
	if pwdLen := len(pwd); pwdLen < 6 || pwdLen > 20 {
		return rpc.CreateErrorWithMsg(qmsg.ErrUserOrPasswordError, "the length of password out of limit")
	}
	return nil
}

func GetUser4Robot(ctx *rpc.Context, req *qmsg.GetUser4RobotReq) (*qmsg.GetUser4RobotRsp, error) {
	var rsp qmsg.GetUser4RobotRsp
	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)

	var userRobotList []*qmsg.ModelUserRobot
	err := UserRobot.WhereCorpApp(corpId, appId).Where(DbRobotUid, req.RobotUid).Find(ctx, &userRobotList)
	if err != nil {
		log.Errorf("err %v", err)
		return nil, err
	}

	userDb := User.WhereCorpApp(corpId, appId).Where(DbIsOnline, true)

	uidList := utils.PluckUint64(userRobotList, "Uid")
	if len(uidList) > 0 {
		userDb.WhereIn(DbId, uidList)
		rsp.IsAssignMode = true
	}

	var userList []*qmsg.ModelUser

	err = userDb.Find(ctx, &userList)
	if err != nil {
		log.Errorf("err %v", err)
		return nil, err
	}

	rsp.UserList = userList

	return &rsp, nil
}

func SetUserSessionGroupSetting(ctx *rpc.Context, req *qmsg.SetUserSessionGroupSettingReq) (*qmsg.SetUserSessionGroupSettingRsp, error) {
	var rsp qmsg.SetUserSessionGroupSettingRsp

	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)
	uid, _ := core.GetPinHeaderUId2Int(ctx)
	if uid == 0 {
		return nil, pindef.ErrNotLogin
	}

	var msgUser qmsg.ModelUser
	err := User.WhereCorpApp(corpId, appId).Where(DbId, uid).First(ctx, &msgUser)
	if err != nil {
		log.Errorf("err %v", err)
		return nil, err
	}

	if msgUser.Detail == nil {
		msgUser.Detail = &qmsg.ModelUser_Detail{}
	}

	msgUser.Detail.SessionGroupSetting = req.SessionGroupSetting

	_, err = User.WhereCorpApp(corpId, appId).Where(DbId, uid).Update(ctx, map[string]interface{}{
		DbDetail: msgUser.Detail,
	})
	if err != nil {
		log.Errorf("err %v", err)
		return nil, err
	}

	if err != nil {
		log.Errorf("err %v", err)
		return nil, err
	}

	return &rsp, nil
}

func getUserMap(ctx *rpc.Context, corpId, appId uint32, uidList []uint64) (map[uint64]*qmsg.ModelUser, error) {
	var userList []*qmsg.ModelUser
	err := User.WhereCorpApp(corpId, appId).WhereIn(DbId, uidList).Find(ctx, &userList)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	userMap := map[uint64]*qmsg.ModelUser{}
	utils.KeyBy(userList, "Id", &userMap)
	return userMap, nil
}

func UpdateUserCategory(ctx *rpc.Context, req *qmsg.UpdateUserCategoryReq) (*qmsg.UpdateUserCategoryRsp, error) {
	var rsp qmsg.UpdateUserCategoryRsp
	user, err := getQUserCheck(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	err = updateUserCategory(ctx, user.CorpId, user.AppId, req.Id, req.UserCategoryId)
	if err != nil {
		log.Errorf("err %v", err)
		return nil, err
	}
	return &rsp, nil
}

func callbackKefuInfoChange(ctx *rpc.Context, corpId, appId uint32, loginId string, strOpenId string) error {

	type KefuInfo struct {
		CustomerServiceAccount string `json:"customer_service_account"`
		CustomerServiceOpenId  string `json:"customer_service_open_id"`
	}

	type CbContent struct {
		EventType uint32   `json:"event_type"`
		ErrCode   int      `json:"err_code"`
		ErrMsg    string   `json:"err_msg"`
		Data      KefuInfo `json:"data"`
	}

	cbByte, err := json.Marshal(&CbContent{
		EventType: uint32(qopen.EventType_EventKefuChange),
		ErrCode:   0,
		ErrMsg:    "success",
		Data: KefuInfo{
			CustomerServiceAccount: loginId,
			CustomerServiceOpenId:  strOpenId,
		},
	})
	if err != nil {
		log.Errorf("err %v", err)
		return err
	}

	_, err = qopen.SendCallBackToCropSys(ctx, &qopen.SendCallBackToCropSysReq{
		CorpId:    corpId,
		AppId:     appId,
		Content:   string(cbByte),
		EventType: uint32(qopen.EventType_EventKefuChange),
	})
	if err != nil {
		log.Errorf("err %v", err)
		return err
	}
	return nil
}

func GenAndCheckUserOpenId(ctx *rpc.Context, corpId, appId uint32) (string, error) {
	errCode := 99
	var err error
	for i := 0; i < 3; i++ {
		openId := qrt.GenStrOpenId4Account()
		err = User.Where(map[string]interface{}{
			DbCorpId:    corpId,
			DbAppId:     appId,
			DbStrOpenId: openId,
		}).EnsureNotExists(ctx, errCode)
		if err != nil {
			log.Errorf("err:%v", err)
			if rpc.GetErrCode(err) == errCode {
				log.Warnf("corpId:%d,appId:%d strOpenId:%s already existed", corpId, appId, openId)
				continue
			}
			return "", err
		}
		return openId, nil
	}
	return "", err
}

func UpdateUserCategorySys(ctx *rpc.Context, req *qmsg.UpdateUserCategorySysReq) (*qmsg.UpdateUserCategorySysRsp, error) {
	var rsp qmsg.UpdateUserCategorySysRsp
	err := updateUserCategory(ctx, req.CorpId, req.AppId, req.Id, req.UserCategoryId)
	if err != nil {
		log.Errorf("err %v", err)
		return nil, err
	}
	return &rsp, nil
}

func updateUserCategory(ctx *rpc.Context, corpId, appId uint32, id, userCategoryId uint64) error {
	if userCategoryId > 0 {
		var userCategory qmsg.ModelUserCategory
		err := UserCategory.WhereCorpApp(corpId, appId).Where(DbId, userCategoryId).First(ctx, &userCategory)
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
	}
	_, err := User.WhereCorpApp(corpId, appId).Where(DbId, id).Update(ctx, map[string]interface{}{
		DbUserCategoryId: userCategoryId,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	// 变更通知到开平
	msgUser, err := User.get(ctx, corpId, appId, id)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	err = callbackKefuInfoChange(ctx, msgUser.CorpId, msgUser.AppId, msgUser.LoginId, msgUser.StrOpenId)
	if err != nil {
		log.Errorf("err %v", err)
		return err
	}
	return nil
}

// callbackKefuStatusChange 客服上下班状态变更
func callbackKefuStateChange(ctx *rpc.Context, corpId, appId uint32, loginId string, strOpenId string, isOnline bool) error {
	type KefuInfo struct {
		CustomerServiceAccount string `json:"customer_service_account"`
		CustomerServiceOpenId  string `json:"customer_service_open_id"`
		IsOnline               bool   `json:"is_online"`
		UpdateAt               int32  `json:"update_at"`
	}
	type CbContent struct {
		EventType uint32   `json:"event_type"`
		ErrCode   int      `json:"err_code"`
		ErrMsg    string   `json:"err_msg"`
		Data      KefuInfo `json:"data"`
	}

	cbByte, err := json.Marshal(&CbContent{
		EventType: uint32(qopen.EventType_EventKefuStateChange),
		ErrCode:   0,
		ErrMsg:    "success",
		Data: KefuInfo{
			CustomerServiceAccount: loginId,
			CustomerServiceOpenId:  strOpenId,
			IsOnline:               isOnline,
			UpdateAt:               int32(time.Now().Unix()),
		},
	})
	if err != nil {
		log.Errorf("err %v", err)
		return err
	}
	routine.Go(ctx, func(ctx *rpc.Context) error {
		var err error
		_, err = qopen.SendCallBackToCropSys(ctx, &qopen.SendCallBackToCropSysReq{
			CorpId:    corpId,
			AppId:     appId,
			Content:   string(cbByte),
			EventType: uint32(qopen.EventType_EventKefuStateChange),
		})
		if err != nil {
			log.Errorf("err %v", err)
		}
		return err
	})
	return nil
}

func UpdateUserAssignSys(ctx *rpc.Context, req *qmsg.UpdateUserAssignSysReq) (*qmsg.UpdateUserAssignSysRsp, error) {
	var rsp qmsg.UpdateUserAssignSysRsp
	var user qmsg.ModelUser
	err := User.WhereCorpApp(req.CorpId, req.AppId).Where(DbId, req.Id).First(ctx, &user)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	err = updateUserAssign(ctx, req.CorpId, req.AppId, &user, req.ContactFilter, req.GroupFilter, req.Id, true)
	if err != nil {
		log.Errorf("err %v", err)
		return nil, err
	}

	opReq := qoperationrecord.AddBizOperationRecordSysReq{
		CorpId:       req.CorpId,
		AppId:        req.AppId,
		OpType:       uint32(qoperationrecord.ModelBizOperationRecord_OperationTypeModify),
		ModuleCode:   uint32(qoperationrecord.ModelBizOperationRecord_ModuleCustomerServiceManageList),
		OpTime:       utils.Now(),
		OperatorName: "系统",
		StrDetail:    fmt.Sprintf("【%s】修改了客服账号【%s】的分配资产", "系统", user.Username),
	}
	_, err = qoperationrecord.AddBizOperationRecordSys(ctx, &opReq)
	if err != nil {
		log.Errorf("err:%v", err)
	}
	return &rsp, nil
}

func updateUserAssign(ctx *rpc.Context, corpId, appId uint32, user *qmsg.ModelUser, contactFilter *quan.AdvanceExtContactFilter, groupFilter *qmsg.GroupFilter, userId uint64, isAssign bool) error {

	// 非分配改非分配等于不变
	if !user.IsAssign && !isAssign {
		return nil
	}

	err := setUserDefaultFilter(user)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	var uidList []uint64
	if contactFilter != nil && contactFilter.FilterUser != nil {
		uidList = contactFilter.FilterUser.UidList
	}

	updateMap := map[string]interface{}{
		DbIsAssign:      isAssign,
		DbContactFilter: contactFilter,
		DbGroupFilter:   groupFilter,
	}
	// 分配资产模式，没有工作时间设置及系统自动修改工作状态等逻辑
	if isAssign {
		updateMap[DbIsUseOfficeHour] = false
		updateMap[DbIsAutoChangeWorkState] = false
	}

	_, err = User.WhereCorpApp(corpId, appId).Where(DbId, userId).
		Update(ctx, updateMap)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	if isAssign {

		var userRobotList []*qmsg.ModelUserRobot
		err = UserRobot.WhereCorpApp(corpId, appId).Where(DbUid, userId).Find(ctx, &userRobotList)
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}

		robotUidList := utils.PluckUint64(userRobotList, "RobotUid")

		add, del := robotUidList.Diff(uidList)

		if len(add) > 0 {
			err = batchCreateUserRobot(ctx, corpId, appId, userId, add)
			if err != nil {
				log.Errorf("err:%v", err)
				return err
			}
		}

		if len(del) > 0 {
			_, err = UserRobot.WhereCorpApp(corpId, appId).Where(DbUid, userId).WhereIn(DbRobotUid, del).Delete(ctx)
			if err != nil {
				log.Errorf("err:%v", err)
				return err
			}

			var topChatList []*qmsg.ModelTopChat

			//原逻辑编辑过就会把客服的置顶全删
			//抽出来按modelChat.Id来删除
			err = TopChat.WhereCorpApp(corpId, appId).Where(DbUid, userId).Select(DbCid).Find(ctx, &topChatList)
			if err != nil {
				log.Errorf("err %v", err)
				return err
			}

			if len(topChatList) > 0 {
				cidList := utils.PluckUint64(topChatList, "Cid")

				var chatList []*qmsg.ModelChat
				err = Chat.WhereCorpApp(corpId, appId).WhereIn(DbId, cidList).Select(DbId, DbRobotUid).Find(ctx, &chatList)
				if err != nil {
					log.Errorf("err %v", err)
					return err
				}

				delMap := make(map[uint64]struct{})
				for _, v := range del {
					delMap[v] = struct{}{}
				}

				var delCidList []uint64
				for _, v := range chatList {
					if _, ok := delMap[v.RobotUid]; ok {
						delCidList = append(delCidList, v.Id)
					}
				}

				if len(delCidList) > 0 {
					_, err = TopChat.deleteByUidCidList(ctx, corpId, appId, userId, delCidList)
					if err != nil {
						log.Errorf("err %v", err)
						return err
					}
				}
			}

		}

		err = deleteAndResignChat(ctx, corpId, appId, userId)
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}

		err = pushWsMsg(ctx, corpId, appId, userId, []*qmsg.WsMsgWrapper{
			{
				MsgType: uint32(qmsg.WsMsgWrapper_MsgTypeUserRobotsChanged),
			},
		})
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
	} else {
		_, err := UserRobot.WhereCorpApp(corpId, appId).Where(DbUid, userId).Delete(ctx)
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}

		_, err = AssignChat.assignChatListByUid(ctx, corpId, appId, userId)
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
	}

	// 由非分配模式(开启了工作时间设置)修改为分配模式，删除工作时间相关设置
	if !user.IsAssign && user.IsUseOfficeHour && isAssign {
		_, err = UserOfficeHour.WhereCorpApp(user.CorpId, user.AppId).Where(DbUid, user.Id).Delete(ctx)
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
	}

	if user.IsAssign != isAssign {
		_, err = TopChat.deleteByUidList(ctx, corpId, appId, []uint64{userId})
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}

		err = pushWsMsg(ctx, corpId, appId, userId, []*qmsg.WsMsgWrapper{
			{
				MsgType: uint32(qmsg.WsMsgWrapper_MsgTypeChangeAssign),
			},
		})
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
	}

	err = callbackKefuInfoChange(ctx, user.CorpId, user.AppId, user.LoginId, user.StrOpenId)
	if err != nil {
		log.Errorf("err %v", err)
	}

	return nil
}
