package impl

import (
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/core"
	"git.pinquest.cn/qlb/qmsg"
)

func SetMsgQualityStatRule(ctx *rpc.Context, req *qmsg.SetMsgQualityStatRuleReq) (*qmsg.SetMsgQualityStatRuleRsp, error) {
	var rsp qmsg.SetMsgQualityStatRuleRsp

	log.Infof("req")
	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)
	if corpId == 0 || appId == 0 {
		return nil, rpc.InvalidArg("not login")
	}
	req.MsgQualityStatRule.CorpId = corpId
	req.MsgQualityStatRule.AppId = appId

	err := beforeSaveMsgQualityStatRule(req.MsgQualityStatRule)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	statRule, err := MsgQualityStatRule.Save(ctx, req.MsgQualityStatRule)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	rsp.MsgQualityStatRule = statRule
	return &rsp, nil
}

func GetMsgQualityStatRule(ctx *rpc.Context, _ *qmsg.GetMsgQualityStatRuleReq) (*qmsg.GetMsgQualityStatRuleRsp, error) {
	var rsp qmsg.GetMsgQualityStatRuleRsp
	var uidList []uint64
	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)
	if corpId == 0 || appId == 0 {
		return nil, rpc.InvalidArg("not login")
	}
	rule, err := MsgQualityStatRule.getLatestStatRule(ctx, corpId, appId)
	if err != nil {
		log.Errorf("err:%v", err)
		if MsgQualityStatRule.IsNotFoundErr(err) {
			return nil, nil
		}
		return nil, err
	}
	rsp.MsgQualityStatRule = rule

	// 统计成员范围 - 平台号和扫码号 和 企微用户
	if rule.Detail == nil {
		return &rsp, nil
	}
	// 获取平台号或扫码号信息
	uidList = append(uidList, rule.Detail.UidList...)

	// 超时触发提醒 - 正常企业成员
	if rule.Detail.OverTimeNotify != nil {
		uidList = append(uidList, rule.Detail.OverTimeNotify.UidList...)
	}
	// 敏感词触发提醒 - 正常企业成员
	if rule.Detail.SensitiveWordsNotify != nil {
		uidList = append(uidList, rule.Detail.SensitiveWordsNotify.UidList...)
	}
	if len(uidList) > 0 {
		rsp.UserMap, err = getStaffUserList(ctx, corpId, appId, uidList)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
	}

	return &rsp, nil
}
