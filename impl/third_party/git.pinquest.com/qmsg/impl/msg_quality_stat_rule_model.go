package impl

import (
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/brick/utils"
	"git.pinquest.cn/qlb/qmsg"
	"time"
)

// Save 保存(新增)会话质检规则
func (m *TMsgQualityStatRule) Save(ctx *rpc.Context, msgStatRule *qmsg.ModelMsgQualityStatRule) (*qmsg.ModelMsgQualityStatRule, error) {
	latestStatRule, err := m.getLatestStatRule(ctx, msgStatRule.CorpId, msgStatRule.AppId)
	if err != nil {
		if !MsgQualityStatRule.IsNotFoundErr(err) {
			log.Errorf("err:%v", err)
			return nil, err
		}
		//第一次配置，直接新增
		err = m.Create(ctx, &msgStatRule)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
	} else {
		//创建时间如果是今天前，则直接新增一条新的，否则直接更新这条
		if latestStatRule.CreatedAt < uint32(utils.BeginTimeStampOfDate(time.Now())) {
			err = m.Create(ctx, &msgStatRule)
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, err
			}
		} else {
			_, err = MsgQualityStatRule.Where(DbId, latestStatRule.Id).Update(ctx, utils.OrmStruct2Map(msgStatRule))
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, err
			}
		}
	}
	return msgStatRule, nil
}

// getLatestStatRule 获取最新的统计规则
func (m *TMsgQualityStatRule) getLatestStatRule(ctx *rpc.Context, corpId, appId uint32) (*qmsg.ModelMsgQualityStatRule, error) {
	var msgRule qmsg.ModelMsgQualityStatRule
	err := m.WhereCorpApp(corpId, appId).OrderDesc(DbCreatedAt).First(ctx, &msgRule)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	return &msgRule, nil
}

// GetCurrentInUseStatRule 获取当前正在使用的规则
func (m *TMsgQualityStatRule) GetCurrentInUseStatRule(ctx *rpc.Context, corpId, appId uint32) (*qmsg.ModelMsgQualityStatRule, error) {
	var msgRule qmsg.ModelMsgQualityStatRule
	err := m.WhereCorpApp(corpId, appId).
		Lte(DbCreatedAt, utils.BeginTimeStampOfDate(time.Now())).
		OrderDesc(DbCreatedAt).First(ctx, &msgRule)
	if err != nil {
		if !MsgQualityStatRule.IsNotFoundErr(err) {
			log.Errorf("err:%v", err)
		}
		return nil, err
	}
	return &msgRule, nil
}

// GetNeedRunSchedRuleList 只获取昨天前开启了会话质检规则的数据 	4.7执行数据汇总，汇总4.6的数据，4.6使用的是4.6前创建的规则
func (m *TMsgQualityStatRule) GetNeedRunSchedRuleList(ctx *rpc.Context) ([]*qmsg.ModelMsgQualityStatRule, error) {
	var msgRules []*qmsg.ModelMsgQualityStatRule
	err := m.Where(DbEnableState, qmsg.ModelMsgQualityStatRule_EnableStateOpen).
		Lte(DbCreatedAt, utils.EndTimeStampOfDate(time.Now())-86400*2).
		OrderDesc(DbCreatedAt).
		Find(ctx, &msgRules)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	//同一个企业如果满足条件的有多条，则取最后创建的那条规则
	var realRules []*qmsg.ModelMsgQualityStatRule
	corpMap := make(map[uint32]struct{})
	for _, rule := range msgRules {
		if _, ok := corpMap[rule.CorpId]; !ok {
			corpMap[rule.CorpId] = struct{}{}
			realRules = append(realRules, rule)
		}
	}
	return realRules, nil
}
