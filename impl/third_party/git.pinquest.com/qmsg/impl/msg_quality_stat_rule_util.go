package impl

import (
	"fmt"
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/brick/utils"
	"git.pinquest.cn/qlb/extrpkg/github.com/go-redis/redis"
	"git.pinquest.cn/qlb/qmsg"
	"time"
)

const RuleKey = "qmsg_msg_qualify_stat_rule"

// beforeSaveMsgQualityStatRule
func beforeSaveMsgQualityStatRule(msgStatRule *qmsg.ModelMsgQualityStatRule) error {
	if msgStatRule.CorpId == 0 || msgStatRule.AppId == 0 {
		return rpc.InvalidArg("corpId or appId is required")
	}

	// check enable_state
	_, ok := qmsg.ModelMsgQualityStatRule_EnableState_name[int32(msgStatRule.EnableState)]
	if !ok || msgStatRule.EnableState == uint32(qmsg.ModelMsgQualityStatRule_EnableStateNil) {
		log.Warnf("enable_state not exist :%v", msgStatRule.EnableState)
		msgStatRule.EnableState = uint32(qmsg.ModelMsgQualityStatRule_EnableStateClose)
	}

	// check detail
	detail := msgStatRule.Detail
	if detail == nil {
		log.Warnf("detail is nil")
		msgStatRule.Detail = &qmsg.ModelMsgQualityStatRule_Detail{}
		detail = msgStatRule.Detail
	}

	// check stat_user_scope
	_, ok = qmsg.ModelMsgQualityStatRule_StatUserScopeType_name[int32(detail.StatUserScopeType)]
	if !ok || detail.StatUserScopeType == uint32(qmsg.ModelMsgQualityStatRule_StatUserScopeTypeNil) {
		log.Warnf("stat_user_scope not exist :%v", detail.StatUserScopeType)
		detail.StatUserScopeType = uint32(qmsg.ModelMsgQualityStatRule_StatUserScopeTypeAll)
	}
	if detail.StatUserScopeType == uint32(qmsg.ModelMsgQualityStatRule_StatUserScopeTypeSpecified) {
		if len(detail.UidList) == 0 {
			return rpc.InvalidArg("uid is nil")
		}
	}

	// check stat_time_scope
	_, ok = qmsg.StatTimeScopeType_name[int32(detail.StatTimeScopeType)]
	if !ok || detail.StatTimeScopeType == uint32(qmsg.StatTimeScopeType_StatTimeScopeTypeNil) {
		log.Warnf("stat_time_scope not exist :%v", detail.StatTimeScopeType)
		detail.StatTimeScopeType = uint32(qmsg.StatTimeScopeType_StatTimeScopeTypeAll)
	}
	if detail.StatTimeScopeType == uint32(qmsg.StatTimeScopeType_StatTimeScopeTypeSpecified) {
		if len(detail.TimeScopeList) == 0 {
			return rpc.InvalidArg("time_scope_list is Nil")
		}
		for _, scope := range detail.TimeScopeList {
			if len(scope.WeekList) == 0 {
				return rpc.InvalidArg("time_scope_list.week_list is nil")
			}
		}
	}

	// check over_time_notify
	if detail.OverTimeNotify == nil {
		detail.OverTimeNotify = &qmsg.ModelMsgQualityStatRule_NotifyConfig{
			EnableNotify: false,
		}
	}
	if detail.OverTimeNotify.EnableNotify {
		if len(detail.OverTimeNotify.UidList) == 0 {
			return rpc.InvalidArg("over_time_notify.uid_list is Nil")
		}
	}

	// check sensitive_words_notify
	if detail.SensitiveWordsNotify == nil {
		detail.SensitiveWordsNotify = &qmsg.ModelMsgQualityStatRule_NotifyConfig{
			EnableNotify: false,
		}
	}
	if detail.SensitiveWordsNotify.EnableNotify {
		if len(detail.SensitiveWordsNotify.UidList) == 0 {
			return rpc.InvalidArg("sensitive_words_notify.uid_list is Nil")
		}
	}

	// check response_time
	if detail.ReplyTime == nil {
		log.Warnf("detail.ResponseTime is nil")
		detail.ReplyTime = &qmsg.ModelMsgQualityStatRule_ResponseTime{
			Type: uint32(qmsg.ModelMsgQualityStatRule_TimeTypeSecond),
			Time: 0,
		}
	} else {
		_, ok = qmsg.ModelMsgQualityStatRule_TimeType_name[int32(detail.ReplyTime.Type)]
		if !ok || detail.ReplyTime.Type == uint32(qmsg.ModelMsgQualityStatRule_TimeTypeNil) {
			log.Warnf("detail.ResponseTime.Type not exist :%v", detail.ReplyTime.Type)
			detail.ReplyTime.Type = uint32(qmsg.ModelMsgQualityStatRule_TimeTypeSecond)
		}
	}

	if detail.FirstReplyOverTime == nil {
		log.Warnf("detail.ResponseTime is nil")
		detail.FirstReplyOverTime = &qmsg.ModelMsgQualityStatRule_ResponseTime{
			Type: uint32(qmsg.ModelMsgQualityStatRule_TimeTypeSecond),
			Time: 0,
		}
	} else {
		_, ok = qmsg.ModelMsgQualityStatRule_TimeType_name[int32(detail.FirstReplyOverTime.Type)]
		if !ok || detail.FirstReplyOverTime.Type == uint32(qmsg.ModelMsgQualityStatRule_TimeTypeNil) {
			log.Warnf("detail.FirstReplyOverTime.Type not exist :%v", detail.FirstReplyOverTime.Type)
			detail.FirstReplyOverTime.Type = uint32(qmsg.ModelMsgQualityStatRule_TimeTypeSecond)
		}
	}

	if detail.LateReplyOverTime == nil {
		log.Warnf("detail.ResponseTime is nil")
		detail.LateReplyOverTime = &qmsg.ModelMsgQualityStatRule_ResponseTime{
			Type: uint32(qmsg.ModelMsgQualityStatRule_TimeTypeSecond),
			Time: 0,
		}
	} else {
		_, ok = qmsg.ModelMsgQualityStatRule_TimeType_name[int32(detail.LateReplyOverTime.Type)]
		if !ok || detail.LateReplyOverTime.Type == uint32(qmsg.ModelMsgQualityStatRule_TimeTypeNil) {
			log.Warnf("detail.LateReplyOverTime.Type not exist :%v", detail.LateReplyOverTime.Type)
			detail.LateReplyOverTime.Type = uint32(qmsg.ModelMsgQualityStatRule_TimeTypeSecond)
		}
	}

	return nil
}

// genMsgQualityStatRuleKey
func genMsgQualityStatRuleKey(corpId, appId uint32) string {
	return fmt.Sprintf("%s_%d_%d_%s", RuleKey, corpId, appId, utils.TimeNowCompactYmd())
}

// getMsgQualityStatRule 获取该租户当前生效的规则
func getMsgQualityStatRule(ctx *rpc.Context, corpId, appId uint32) (*qmsg.ModelMsgQualityStatRule, error) {
	var msgRule qmsg.ModelMsgQualityStatRule
	ruleKey := genMsgQualityStatRuleKey(corpId, appId)
	err := s.RedisGroup.GetJson(ruleKey, &msgRule)
	if err != nil {
		if err != redis.Nil {
			log.Errorf("err:%v", err)
			return nil, err
		}

		msgRuleFromDb, err := MsgQualityStatRule.GetCurrentInUseStatRule(ctx, corpId, appId)
		if err != nil {
			if !MsgQualityStatRule.IsNotFoundErr(err) {
				log.Errorf("err:%v", err)
				return nil, err
			}
			//昨天的查不到，之后查询也不会有，还是加个缓存
			msgRule = qmsg.ModelMsgQualityStatRule{}
		} else {
			msgRule = *msgRuleFromDb
		}

		err = s.RedisGroup.SetJson(ruleKey, msgRule, 24*time.Hour)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}

	}
	if msgRule.Id == 0 {
		return nil, MsgQualityStatRule.GetNotFoundErr()
	}
	return &msgRule, nil
}

// getSecondsFromResponseTime 将时间转换为秒
func getSecondsFromResponseTime(responseTime *qmsg.ModelMsgQualityStatRule_ResponseTime) uint32 {
	if responseTime.Type == uint32(qmsg.ModelMsgQualityStatRule_TimeTypeMinute) {
		return responseTime.Time * 60
	}
	return responseTime.Time
}
