package impl

import (
	"git.pinquest.cn/base/aiadmin"
	"git.pinquest.cn/base/aiadmin/aiadmindb"
	"git.pinquest.cn/base/aiadminserver/impl/global"
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/dbproxy"
	"git.pinquest.cn/qlb/brick/perm"
	"git.pinquest.cn/qlb/brick/revproxy"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/brick/utils/routine"
	"git.pinquest.cn/qlb/brick/warning"
)

var S *State

type State struct {
	Conf *Config
	StateDb
	StateRedis
	StateObjCache
}

func NewState() *State {
	var s State
	s.Conf = GetConfig()
	// 【新增】在 S.Conf 被赋值后，立即设置全局DB配置名
	if s.Conf != nil && s.Conf.Db != "" {
		global.SetDBConfigName(s.Conf.Db)
		log.Infof("Global DB config name set to: %s", s.Conf.Db)
	} else {
		log.Warnf("S.Conf or S.Conf.Db is not initialized, global DB config name not set.")
	}

	return &s
}

func Db() *dbproxy.Scope {
	return dbproxy.NewScope()
}

func InitState() error {
	S = NewState()

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

	err = InitMq()
	if err != nil {
		return err
	}

	err = InitStateLLM()
	if err != nil {
		return err
	}

	err = InitStateMilvusIndexer()
	if err != nil {
		return err
	}

	err = InitStateMilvusRetriever()
	if err != nil {
		return err
	}

	// 初始化工作流缓冲服务
	InitWorkflowBufferService(S.RedisGroup)

	rpc.AddBeforeProcess(perm.BeforeRpcProcess)

	if rpc.Meta.IsReleaseRole() {
		log.SetLogLevel(log.InfoLevel)
	} else {
		log.SetLogLevel(log.DebugLevel)
	}

	routine.Go(nil, func(ctx *rpc.Context) error {
		aiadmindb.Register()

		err = dbproxy.CreateTablesWithPb(
			nil,
			S.Conf.Db,
			&aiadmin.ModelDataset{},
			&aiadmin.ModelFAQ{},
			&aiadmin.ModelFAQCategory{},
			&aiadmin.ModelEmbeddingRecord{},
			&aiadmin.ModelDatasetProcessRule{},
			&aiadmin.ModelFile{},
			&aiadmin.ModelFAQManualMedia{},
			&aiadmin.ModelManualMaterial{},
			&aiadmin.ModelWorkflowDatasetUsage{},
			// 工作流
			&aiadmin.ModelWorkflow{},
			&aiadmin.ModelWorkflowMember{},
			&aiadmin.ModelWorkflowVersion{},
			&aiadmin.ModelWorkflowDatasetUsage{},
			&aiadmin.ModelWorkflowRun{},
			&aiadmin.ModelWorkflowNodeExecution{},
		)
		if err != nil {
			log.Errorf("err:%v", err)
		}

		// 注册权限
		err = revproxy.RegisterAccess(
			aiadmin.SvrName,
			uint32(revproxy.AccessLevel_AccessLevelAuth),
			aiadmin.Path2UserType)
		if err != nil {
			log.Errorf("err:%v", err)
		}

		err = dbproxy.RegisterDivDbBlacklist(nil,
			&aiadmin.ModelDataset{},
			&aiadmin.ModelFAQ{},
			&aiadmin.ModelFAQCategory{},
			&aiadmin.ModelEmbeddingRecord{},
			&aiadmin.ModelDatasetProcessRule{},
			&aiadmin.ModelFile{},
			&aiadmin.ModelFAQManualMedia{},
			&aiadmin.ModelManualMaterial{},
			&aiadmin.ModelWorkflowDatasetUsage{},
			// 工作流
			&aiadmin.ModelWorkflow{},
			&aiadmin.ModelWorkflowMember{},
			&aiadmin.ModelWorkflowVersion{},
			&aiadmin.ModelWorkflowDatasetUsage{},
			&aiadmin.ModelWorkflowRun{},
			&aiadmin.ModelWorkflowNodeExecution{},
		)
		if err != nil {
			log.Errorf("err %v", err)
		}

		err = RegisterFreqRules()
		if err != nil {
			log.Errorf("err:%v", err)
		}

		return nil
	})

	return nil
}
