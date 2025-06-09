package impl

import (
	"fmt"
	"git.pinquest.cn/qlb/extrpkg/github.com/go-redis/redis"
	"git.pinquest.cn/qlb/featswitch"
	"git.pinquest.cn/qlb/qauth"
	"strconv"
	"strings"
	"time"

	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/revproxy"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/brick/utils"
	"git.pinquest.cn/qlb/core"
	"git.pinquest.cn/qlb/qmsg"
	"git.pinquest.cn/qlb/quan"
	"github.com/jinzhu/now"
)

const (
	HeaderApp = "x-brick-app"
)

func Login(ctx *rpc.Context, req *qmsg.LoginReq) (*qmsg.LoginRsp, error) {
	var rsp qmsg.LoginRsp
	var u qmsg.ModelUser
	var err error

	lockKey := fmt.Sprintf("qmsg_login_lock_%s", req.LoginId)
	exist, err := s.RedisGroup.Exists(lockKey)
	if err != nil {
		log.Errorf("err %v", err)
		return nil, err
	}
	if exist {
		return nil, rpc.CreateErrorWithMsg(qmsg.ErrUserOrPasswordError, "密码输入错误次数过多， 请5分钟后再试")
	}

	lockLogin := func() {
		key := fmt.Sprintf("qmsg_login_check_lock_%s", req.LoginId)
		var score uint32
		b, err := s.RedisGroup.Get(key)
		if err != nil {
			if err == redis.Nil {
				score = 1
			} else {
				log.Errorf("err %v", err)
				return
			}
		} else {
			t, _ := strconv.ParseUint(string(b), 10, 64)
			score = uint32(t)
		}

		if score >= 5 {
			err = s.RedisGroup.Set(lockKey, []byte{'1'}, time.Minute*5)
			if err != nil {
				log.Errorf("err %v", err)
				return
			}

			err = s.RedisGroup.Del(key)
			if err != nil {
				log.Errorf("err %v", err)
				return
			}
		} else {
			score++
			err = s.RedisGroup.Set(key, []byte(fmt.Sprintf("%d", score)), time.Hour*24)
			if err != nil {
				log.Errorf("err %v", err)
				return
			}
		}

	}

	err = User.Where(DbLoginId, req.LoginId).First(ctx, &u)
	if err != nil {
		if User.IsNotFoundErr(err) {
			lockLogin()
			log.Error("login id not exist")
			return nil, rpc.CreateError(qmsg.ErrUserOrPasswordError)
		}
		log.Errorf("err:%v", err)
		return nil, err
	}

	correct := userUtil.checkSecret(req.Secret, u.Password)

	if !correct {
		lockLogin()
		return nil, rpc.CreateError(qmsg.ErrUserOrPasswordError)
	}

	if u.IsDisable {
		return nil, rpc.CreateError(qmsg.ErrUserDisabled)
	}

	// 屏蔽高级功能
	enable, err := featswitch.GetFeatureFlagEnabled(ctx, &featswitch.GetFeatureFlagEnabledReq{
		Key:    "SHIELD_ADVANCED_FEATURES",
		CorpId: u.CorpId,
		AppId:  u.AppId,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	if enable.Enabled {
		return nil, rpc.CreateError(qmsg.ErrUserDisabled)
	}

	exp := uint32(-time.Since(now.BeginningOfDay().AddDate(0, 0, 15).Add(time.Hour * 3)).Seconds())

	setter := core.LoginSuccess(ctx, u.CorpId, u.AppId, u.Id, exp)
	rsp.Sid = setter.GetSid()
	rsp.SidExpiredAt = utils.Now() + exp
	rsp.User = &u

	_ = ctx.SetRspHeader("x-brick-project-id", projectId)
	_ = ctx.SetRspHeader(HeaderApp, qauth.Qmsg)
	_ = ctx.SetRspHeader(core.PinHeaderSidType, qauth.Qmsg)

	if !u.IsAssign {
		_, err = assignChatForLogin(ctx, &u)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
	}

	_, err = OperatorLog.create(ctx, u.CorpId, u.AppId, u.Id, qmsg.UserLog_LogTypeLogin)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	version := 1

	if strings.Contains(ctx.GetReqHeader("X-Requested-With"), "v2") {
		version = 2
	}

	LogStream.LogStreamSubType(ctx, LoginSubType, &LoginStream{
		CorpId:   u.CorpId,
		Uid:      u.Id,
		Version:  version,
		IsAssign: u.IsAssign,
	})

	return &rsp, nil
}
func GetRobotToken(ctx *rpc.Context, req *qmsg.GetRobotTokenReq) (*qmsg.GetRobotTokenRsp, error) {
	var rsp qmsg.GetRobotTokenRsp
	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)
	uid, err := core.GetPinHeaderUId2Int(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	if req.AssignChatId == 0 {
		c, err := UserRobot.WhereCorpApp(corpId, appId).WhereIn(DbRobotUid, req.UidList).Where(DbUid, uid).Count(ctx)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
		if len(req.UidList) != int(c) {
			return nil, rpc.InvalidArg("没有企业号权限")
		}
	} else {
		assignChat, err := AssignChat.get(ctx, corpId, appId, req.AssignChatId)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}

		if assignChat.Uid != uid {
			return nil, rpc.CreateError(qmsg.ErrNotHasRobotPerm)
		}
	}

	tokenRsp, err := quan.GetTokenSys(ctx, &quan.GetTokenSysReq{
		CorpId:                corpId,
		AppId:                 appId,
		UidList:               req.UidList,
		NeedAssignAdminRole:   true,
		SkipAuthAppScopeCheck: true,
		QmsgUserId:            uid,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	token := map[uint64]*qmsg.GetRobotTokenRsp_Token{}
	for _, t := range tokenRsp.UserMap {
		token[t.User.Id] = &qmsg.GetRobotTokenRsp_Token{
			Sid:          t.Sid,
			SidExpiredAt: t.SidExpiredAt,
			User:         t.User,
		}
	}
	rsp.TokenMap = token
	return &rsp, nil
}

func LoginStaff(ctx *rpc.Context, req *qmsg.LoginStaffReq) (*qmsg.LoginStaffRsp, error) {
	var rsp qmsg.LoginStaffRsp
	var user qmsg.ModelUser
	err := User.Where(DbLoginId, req.LoginId).WhereCorpApp(req.CorpId, req.AppId).First(ctx, &user)
	if err != nil {
		if User.IsNotFoundErr(err) {
			return nil, rpc.CreateError(qmsg.ErrUserOrPasswordError)
		}
		log.Errorf("err:%v", err)
		return nil, err
	}
	if user.IsDisable {
		return nil, rpc.CreateError(qmsg.ErrUserDisabled)
	}
	exp := uint32(-time.Since(now.BeginningOfDay().AddDate(0, 0, 15).Add(time.Hour * 3)).Seconds())
	setter := core.LoginSuccess(ctx, user.CorpId, user.AppId, user.Id, exp)
	rsp.Sid = setter.GetSid()
	rsp.SidExpiredAt = utils.Now() + exp
	rsp.User = &user
	_ = ctx.SetRspHeader(revproxy.RspSkipDelOldSession, "1")
	_ = ctx.SetRspHeader(revproxy.RspSkipSidCookie, "1")
	_ = ctx.SetRspHeader("x-brick-project-id", projectId)
	_ = ctx.SetRspHeader(HeaderApp, "qmsg_staff")
	_ = ctx.SetRspHeader(core.PinHeaderSidType, qauth.Qmsg)

	if !user.IsAssign {
		_, err = assignChatForLogin(ctx, &user)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
	}

	version := 1

	if strings.Contains(ctx.GetReqHeader("X-Requested-With"), "v2") {
		version = 2
	}

	LogStream.LogStreamSubType(ctx, LoginSubType, &LoginStream{
		CorpId:   user.CorpId,
		Uid:      user.Id,
		Version:  version,
		StaffId:  ctx.GetReqHeader2Uint("X-Brick-Staff-Id"),
		IsAssign: user.IsAssign,
	})

	return &rsp, nil
}
