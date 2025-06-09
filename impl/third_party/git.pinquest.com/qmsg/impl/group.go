package impl

import (
	"fmt"
	"git.pinquest.cn/qlb/brick/smq"
	"git.pinquest.cn/qlb/qwhale"
	"time"

	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/brick/utils"
	"git.pinquest.cn/qlb/core"
	"git.pinquest.cn/qlb/qmsg"
	"git.pinquest.cn/qlb/qrobot"
	"git.pinquest.cn/qlb/quan"
	"git.pinquest.cn/qlb/yc"
)

func InviterFriendsJoinGroup(ctx *rpc.Context, req *qmsg.InviterFriendsJoinGroupReq) (*qmsg.InviterFriendsJoinGroupRsp, error) {
	var rsp qmsg.InviterFriendsJoinGroupRsp

	if len(req.InviterInfo) == 0 {

		return nil, rpc.CreateError(qmsg.ErrInviterFriendsJoinGroupInvalidFriends)
	}
	_, err := quan.GetUserCheck(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, rpc.CreateError(qmsg.ErrRobotPermissionDenied)
	}

	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)

	qrgRsp, err := qrobot.GetGroupChatSys(ctx, &qrobot.GetGroupChatSysReq{
		GroupChatId: req.GroupId,
		CorpId:      corpId,
		AppId:       appId,
	})
	if nil != err {
		return nil, err
	}

	err = batchInviterFriendsJoinGroup(ctx, req.RobotType, req.RobotUid, req.InviterInfo, qrgRsp.GroupChat)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	return &rsp, nil
}

func batchInviterFriendsJoinGroup(ctx *rpc.Context, robotType qmsg.RobotType, invitee uint64, inviterMemberInfo []*qmsg.InviterFriendsJoinGroupReq_InviterInfo, group *qrobot.ModelGroupChat) error {
	if nil == group {
		return rpc.CreateError(qmsg.ErrChatNotFound)
	}

	merchantId, err := getMerchantId(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)

	robotSerialNo, err := GetRobotSerialNo(ctx, robotType, invitee)
	if nil != err {
		return rpc.CreateError(qmsg.ErrUserNotFound)
	}

	inviterFansList := []uint64{}
	inviterCorpMemberList := []uint64{}
	for _, v := range inviterMemberInfo {
		switch v.Type {
		case qmsg.FansType_FansTypeFans:
			inviterFansList = append(inviterFansList, v.InviterUid)
		case qmsg.FansType_FansTypeCorp:
			inviterCorpMemberList = append(inviterCorpMemberList, v.InviterUid)
		}
	}

	//获取被邀请粉丝信息
	qralRsp, err := qrobot.GetAccountListSys(ctx, &qrobot.GetAccountListSysReq{
		ListOption: core.NewListOption(qrobot.GetAccountListSysReq_ListOptionIdList, inviterFansList).SetSkipCount().SetLimit(uint32(len(inviterFansList))),
		CorpId:     corpId,
		AppId:      appId,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	dbInviterMemberList := qralRsp.List

	inviterMemberListSn := []string{}
	for _, v := range dbInviterMemberList {
		inviterMemberListSn = append(inviterMemberListSn, v.YcSerialNo)
	}

	//调yc执行
	currTime := time.Now()
	ycRsp, err := yc.InviterJoinGroup(ctx, merchantId, &yc.InviterJoinGroupReq{
		MerchantNo:       merchantId,
		RobotSerialNo:    robotSerialNo,
		GroupSerialNo:    group.StrGroupId,
		ContactSerialNos: inviterMemberListSn,
		RelaSerialNo:     fmt.Sprintf("qmsg_batch_inviter_group_members_%d_%d", group.Id, utils.Now()),
	}, &yc.BizContext{
		CorpId:  corpId,
		Module:  "qmsg",
		Context: "batchInviteeMemberList",
	})
	inviteFriendsJoinGroupFailed(ctx, corpId, appId, invitee, robotSerialNo, qralRsp.List, group, ycRsp)
	defer log.Debugf("yc InviterJoinGroup use time:%v", currTime.Unix()-time.Now().Unix())
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	return nil
}

func DelMemberByGroup(ctx *rpc.Context, req *qmsg.DelMemberByGroupReq) (*qmsg.DelMemberByGroupRsp, error) {
	var rsp qmsg.DelMemberByGroupRsp

	_, err := quan.GetUserCheck(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)

	qrgRsp, err := qrobot.GetGroupChatSys(ctx, &qrobot.GetGroupChatSysReq{
		GroupChatId: req.GroupId,
		CorpId:      corpId,
		AppId:       appId,
	})
	if nil != err {
		return nil, err
	}

	err = batchRemoveMemberList(ctx, req.RobotType, req.RobotUid, req.MemberIdList, qrgRsp.GroupChat)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	return &rsp, nil
}

func batchRemoveMemberList(ctx *rpc.Context, robotType qmsg.RobotType, sponsor uint64, delMemberList []uint64, group *qrobot.ModelGroupChat) error {
	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)

	//判断是不是白撞的
	//获取被邀请粉丝信息
	qrgcmRsp, err := qrobot.GetGroupChatMemberListSys(ctx, &qrobot.GetGroupChatMemberListSysReq{
		ListOption: core.NewListOption(qrobot.GetGroupChatMemberListSysReq_ListOptionIdList, delMemberList),
		CorpId:     corpId,
		AppId:      appId,
	})
	if nil != err {
		return err
	}
	if nil == qrgcmRsp.List || 0 == len(qrgcmRsp.List) {
		return rpc.CreateError(qmsg.ErrChatNotFound)
	}

	robotSerialNo, err := GetRobotSerialNo(ctx, robotType, sponsor)
	if nil != err {
		return rpc.CreateError(qmsg.ErrUserNotFound)
	}

	//判断是不是群大佬
	qrSponsorRsp, err := qrobot.GetGroupChatMemberListSys(ctx, &qrobot.GetGroupChatMemberListSysReq{
		ListOption: core.NewListOption().
			AddOpt(qrobot.GetGroupChatMemberListSysReq_ListOptionGroupIdList, group.Id).
			AddOpt(qrobot.GetGroupChatMemberListSysReq_ListOptionUidIdList, sponsor),
		CorpId: corpId,
		AppId:  appId,
	})
	if nil != err {
		return err
	}
	if nil == qrSponsorRsp.List || 0 == len(qrSponsorRsp.List) {
		return err
	}

	delMemberListSn := []string{}
	for _, v := range qrgcmRsp.List {
		if v.AdminType >= qrSponsorRsp.List[0].AdminType {
			log.Error("the sponsor is no permissions")
			return rpc.CreateError(qmsg.ErrRobotPermissionDenied)
		}

		delMemberListSn = append(delMemberListSn, v.StrMemberId)
	}

	merchantId, err := getMerchantId(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	currTime := time.Now()
	_, err = yc.RemoveMember(ctx, merchantId, &yc.RemoveMemberReq{
		MerchantNo:      merchantId,
		RobotSerialNo:   robotSerialNo,
		GroupSerialNo:   group.StrGroupId,
		MemberSerialNos: delMemberListSn,
		RelaSerialNo:    fmt.Sprintf("qwhale_batch_del_group_members_%d_%d", group.Id, utils.Now()),
	}, &yc.BizContext{
		CorpId:  corpId,
		Module:  "qmsg",
		Context: "batchRemoveMemberList",
	})
	defer log.Debugf("yc InviterJoinGroup use time:%v", currTime.Unix()-time.Now().Unix())
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	return nil
}

func ReceiveJoinGroupMq(ctx *rpc.Context, req *smq.ConsumeReq) (*smq.ConsumeRsp, error) {
	return MqInviteJoinGroup.ProcessV2(ctx, req, func(ctx *rpc.Context, req *smq.ConsumeReq, data interface{}) (*smq.ConsumeRsp, error) {
		var rsp smq.ConsumeRsp

		return &rsp, nil
	})
}

func SetGroupChatComment(ctx *rpc.Context, req *qmsg.SetGroupChatCommentReq) (*qmsg.SetGroupChatCommentRsp, error) {
	var rsp qmsg.SetGroupChatCommentRsp

	_, err := getUserCheck(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)

	_, err = qwhale.SetGroupChatCommentSys(ctx, &qwhale.SetGroupChatCommentSysReq{
		GroupChatId: req.GroupId,
		CorpId:      corpId,
		AppId:       appId,
		Comment:     req.Comment,
	})
	if nil != err {
		return nil, err
	}

	return &rsp, nil
}
