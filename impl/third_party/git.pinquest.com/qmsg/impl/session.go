package impl

import (
	"fmt"
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/core"
	"git.pinquest.cn/qlb/extrpkg/github.com/go-redis/redis"
	"git.pinquest.cn/qlb/qmsg"
)

const (
	SessionConfigPrefix = "qmsg_session_config"
)

func getSessionConfig(ctx *rpc.Context, corpId, appId uint32) (*qmsg.ModelSessionConfig, error) {
	var sessionConfig qmsg.ModelSessionConfig
	err := s.RedisGroup.HGetPb(SessionConfigPrefix, fmt.Sprintf("%d", corpId), &sessionConfig)
	if err != nil {
		if err == redis.Nil {
			sessionConfig, err := getSessionConfigFromDb(ctx, corpId, appId)
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, err
			}

			err = s.RedisGroup.HSetPb(SessionConfigPrefix, fmt.Sprintf("%d", corpId), sessionConfig, 0)
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, err
			}

			return sessionConfig, nil
		}
		log.Errorf("err:%v", err)
		return nil, err
	}
	// todo 打日志看看redis数据
	log.Debugf("sessionConfig:%v", sessionConfig)
	return &sessionConfig, nil
}

func getSessionConfigFromDb(ctx *rpc.Context, corpId, appId uint32) (*qmsg.ModelSessionConfig, error) {
	var sessionConfig qmsg.ModelSessionConfig
	err := SessionConfig.WhereCorpApp(corpId, appId).First(ctx, &sessionConfig)
	if err != nil {
		if SessionConfig.IsNotFoundErr(err) {
			return &qmsg.ModelSessionConfig{
				AssignSetting: &qmsg.ModelSessionConfig_AssignSetting{
					OnlyOnline:      false,
					MaxChatNum:      99,
					IsBlockRobotMsg: false,
				},
				SessionSetting: &qmsg.ModelSessionConfig_SessionSetting{
					AllowCloseUnansweredByUser:    true,
					AllowCloseBySys:               false,
					CloseBySysMinutes:             0,
					AllowAutoReassignUnanswered:   false,
					AutoReassignUnansweredMinutes: 0,
				},
				OfficeHourSetting: &qmsg.ModelSessionConfig_OfficeHourSetting{
					AllowOnWorkNotify:  false,
					AllowOffWorkNotify: false,
				},
				CorpId: corpId,
				AppId:  appId,
			}, nil
		}
		log.Errorf("err:%v", err)
		return nil, err
	}

	return &sessionConfig, nil
}

func UpdateSessionConfig(ctx *rpc.Context, req *qmsg.UpdateSessionConfigReq) (*qmsg.UpdateSessionConfigRsp, error) {
	var rsp qmsg.UpdateSessionConfigRsp

	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)

	m := req.SessionConfig
	m.CorpId = corpId
	m.AppId = appId

	if m == nil {
		return nil, rpc.CreateError(int32(rpc.InvalidArgErrCode))
	}

	var oldSessionConfig qmsg.ModelSessionConfig
	err := SessionConfig.WhereCorpApp(corpId, appId).First(ctx, &oldSessionConfig)
	if err != nil {
		if SessionConfig.IsNotFoundErr(err) {
			err = SessionConfig.Create(ctx, &m)
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, err
			}
		} else {
			log.Errorf("err:%v", err)
			return nil, err
		}
	} else {
		_, err = SessionConfig.WhereCorpApp(corpId, appId).Save(ctx, &m)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
	}

	err = s.RedisGroup.HSetPb(SessionConfigPrefix, fmt.Sprintf("%d", corpId), m, 0)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	if oldSessionConfig.AssignSetting != nil {
		// 如果从轮询模式切换到其他模式，删除ringBuffer缓存
		if oldSessionConfig.AssignSetting.AssignStrategy == uint32(qmsg.ModelSessionConfig_AssignStrategyRound) &&
			m.AssignSetting.AssignStrategy != uint32(qmsg.ModelSessionConfig_AssignStrategyRound) {
			roundMgr.DelRingBuffer(corpId)
		}
		// 会话上限加大时，要调整所有客服的会话
		if oldSessionConfig.Id > 0 && oldSessionConfig.AssignSetting.MaxChatNum < m.AssignSetting.MaxChatNum {
			err = reassignChatForFreeUser(ctx, corpId, appId)
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, err
			}
		}
	}

	if m.SessionSetting != nil {
		// 不允许系统自动关闭超时会话，删除redis队列
		if !m.SessionSetting.AllowCloseBySys || m.SessionSetting.CloseBySysMinutes == 0 {
			err = deleteUserSendRedis(corpId)
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, err
			}
		}
		// 不允许系统重新分配，删除redis队列
		if !m.SessionSetting.AllowAutoReassignUnanswered || m.SessionSetting.AutoReassignUnansweredMinutes == 0 {
			err = deleteContactSendRedis(corpId)
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, err
			}
		}
	}

	return &rsp, nil
}

func GetSessionConfig(ctx *rpc.Context, req *qmsg.GetSessionConfigReq) (*qmsg.GetSessionConfigRsp, error) {
	var rsp qmsg.GetSessionConfigRsp
	var err error

	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)

	session, err := getSessionConfigFromDb(ctx, corpId, appId)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	rsp.SessionConfig = session
	return &rsp, nil
}

func getSessionSetting(ctx *rpc.Context, corpId, appId uint32) *qmsg.ModelSessionConfig_SessionSetting {
	sessionConfig, err := getSessionConfig(ctx, corpId, appId)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil
	}
	if sessionConfig == nil || sessionConfig.SessionSetting == nil {
		return nil
	}
	return sessionConfig.SessionSetting
}
