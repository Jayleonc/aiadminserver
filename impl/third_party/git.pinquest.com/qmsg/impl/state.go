package impl

import (
	"time"

	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/dbproxy"
	"git.pinquest.cn/qlb/brick/perm"
	"git.pinquest.cn/qlb/brick/revproxy"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/brick/utils/routine"
	"git.pinquest.cn/qlb/brick/warning"
	"git.pinquest.cn/qlb/core"
	"git.pinquest.cn/qlb/qmsg"
	"git.pinquest.cn/qlb/qmsg/qmsgdb"
	"git.pinquest.cn/qlb/qrt"
	"git.pinquest.cn/qlb/schedulerv2"
)

var s *State

type State struct {
	Conf *Config
	StateDb
	StateRedis
	StateObjCache

	CoverMsg *qrt.CoverMsg
	SeqGen   *ActionFreqHub
}

func NewState() *State {
	var s State
	s.Conf = GetConfig()
	return &s
}

func Db() *dbproxy.Scope {
	return dbproxy.NewScope()
}

func InitState() error {
	s = NewState()

	rpc.SetWarningFunc(warning.WarningFunc)

	err := InitStateDb()
	if err != nil {
		return err
	}

	err = InitStateRedis()
	if err != nil {
		return err
	}

	err = InitStateObjCache()
	if err != nil {
		return err
	}

	err = initLogStream()
	if err != nil {
		return err
	}

	err = InitFrequency()
	if err != nil {
		return err
	}

	rpc.AddBeforeProcess(perm.BeforeRpcProcess)

	err = initMq()
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	s.SeqGen = NewActionFreqHub(nil, ActionFreqHubConf{
		GenKey: func(i *ActionExec) int {
			return int(i.DataIn.(uint64) % 50)
		},
		Exec: func(i *ActionExec) error {
			ctx := core.NewInternalRpcCtx(0, 0, 0)
			uid := i.DataIn.(uint64)

			var seq uint64
			err = genNextSeq(ctx, uid, &seq)
			if err != nil {
				log.Errorf("err:%v", err)
				return err
			}

			i.DataOut = seq

			return nil
		},
		ExecutorMax: 50,
		ExecTimeout: time.Second * 5,
	})

	if rpc.Meta.IsReleaseRole() {
		log.SetLogLevel(log.InfoLevel)
	} else {
		log.SetLogLevel(log.DebugLevel)
	}
	err = dbproxy.RegisterDivDbBlacklistBySrv(nil, qmsg.ServiceName)
	if err != nil {
		log.Errorf("err:%v", err)
	}
	err = dbproxy.RegisterDivDbBlacklist(nil,
		&qmsg.ModelUser{},
		&qmsg.ModelSidebarTabSort{},
	)
	if err != nil {
		log.Errorf("err:%v", err)
	}
	routine.Go(nil, func(ctx *rpc.Context) error {
		qmsgdb.Register()
		err = dbproxy.CreateTablesWithPb(
			nil,
			s.Conf.Db,
			&qmsg.ModelMsgBox{},
			&qmsg.ModelSeq{},
			&qmsg.ModelUser{},
			&qmsg.ModelUserRobot{},
			&qmsg.ModelChat{},
			&qmsg.ModelSensitiveWordRule{},
			&qmsg.ModelSensitiveOptRecord{},
			&qmsg.ModelSessionConfig{},
			&qmsg.ModelSidebarSetting{},
			&qmsg.ModelIframeSetting{},
			&qmsg.ModelExtContactLostLog{},
			&qmsg.ModelRobotSingleChatStat{},
			&qmsg.ModelServiceSingleChatStat{},
			&qmsg.ModelSingleChatStat{},
			&qmsg.ModelAssignChat{},
			&qmsg.ModelOperatorLog{},
			&qmsg.ModelTopChat{},
			&qmsg.ModelAtRecord{},
			&qmsg.ModelSendRecord{},
			&qmsg.ModelSafeConfig{},
			&qmsg.ModelOfficeHour{},
			&qmsg.ModelLastContactChat{},
			&qmsg.ModelUserOfficeHour{},
			&qmsg.ModelMsgQualityStatRule{},
			&qmsg.ModelRobotSingleStat{},
			&qmsg.ModelCustomerServiceSingleStat{},
			&qmsg.ModelMsgStatTimeScope{},
			&qmsg.ModelMsgStatMsgCache{},
			&qmsg.ModelMsgStatTimeOutDetail{},
			&qmsg.ModelSidebarTabSort{},
			&qmsg.ModelAiEntertainConfig{},
			&qmsg.ModelAiEntertainConfigEditRecord{},
			&qmsg.ModelAiEntertainDetail{},
			&qmsg.ModelAiEntertainRecord{},
			&qmsg.ModelExtSensitiveOptRecord{},
			&qmsg.ModelUserCategory{},
			&qmsg.ModelSystemMsgRecord{},
			&qmsg.ModelUserTag{},
			&qmsg.ModelUserTagRelation{},
			&qmsg.ModelLastChat{},
			&qmsg.ModelChatForAlert{},
			&qmsg.ModelResetChatUnreadRecord{},
			&qmsg.ModelTransferLog{}, // 转介记录

		)
		if err != nil {
			log.Errorf("err:%v", err)
		}
		err = dbproxy.CreateTablesWithPb(
			nil,
			s.Conf.DbMsgBox,
			&qmsg.ModelMsgBox{},
		)
		if err != nil {
			log.Errorf("err:%v", err)
		}
		err = dbproxy.CreateTableDepotsWithPb(ctx, s.Conf.DbMsgBox, &qmsg.ModelMsgBox{}, 20)
		if err != nil {
			log.Errorf("err:%v", err)
		}
		err = dbproxy.RegisterDivDbBlacklist(nil,
			&qmsg.ModelUser{},
			&qmsg.ModelMsgQualityStatRule{},
			&qmsg.ModelRobotSingleStat{},
			&qmsg.ModelCustomerServiceSingleStat{},
			&qmsg.ModelMsgStatTimeScope{},
			&qmsg.ModelAiEntertainConfig{},
			&qmsg.ModelAiEntertainConfigEditRecord{},
			&qmsg.ModelAiEntertainDetail{},
			&qmsg.ModelAiEntertainRecord{},
			&qmsg.ModelExtSensitiveOptRecord{},
			&qmsg.ModelUserCategory{},
			&qmsg.ModelSystemMsgRecord{},
			&qmsg.ModelChatForAlert{},
			&qmsg.ModelResetChatUnreadRecord{},
		)
		if err != nil {
			log.Errorf("err:%v", err)
		}
		// 注册权限
		err = revproxy.RegisterAccess(
			qmsg.SvrName,
			uint32(revproxy.AccessLevel_AccessLevelAuth),
			qmsg.Path2UserType)
		if err != nil {
			log.Errorf("err:%v", err)
		}
		err = RegisterFreqRules()
		if err != nil {
			log.Errorf("err:%v", err)
		}
		// 添加定时任务
		for _, taskReq := range SchedulerTaskList {
			_, err = schedulerv2.AddTask(ctx, taskReq)
			if err != nil {
				log.Errorf("err:%v", err)
			}
		}
		return nil
	})

	merchantId, err := qrt.GetMerchantId(nil)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	s.CoverMsg = qrt.NewCoverMsg(merchantId, s.Conf.ResourceType, s.Conf.ResourceType4CI, s.RedisGroup)

	return nil
}
