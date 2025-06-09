package impl

import (
	"fmt"
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/brick/smq"
	"git.pinquest.cn/qlb/brick/utils"
	"git.pinquest.cn/qlb/core"
	"git.pinquest.cn/qlb/iquan"
	"git.pinquest.cn/qlb/qmsg"
	"git.pinquest.cn/qlb/qrobot"
	"git.pinquest.cn/qlb/quan"
	"git.pinquest.cn/qlb/qwhale"
	"git.pinquest.cn/qlb/yc"
	"strconv"
	"time"
)

func AddFriendInGroup(ctx *rpc.Context, req *qmsg.AddFriendInGroupReq) (*qmsg.AddFriendInGroupRsp, error) {
	var rsp qmsg.AddFriendInGroupRsp

	_, err := quan.GetUserCheck(ctx)
	if nil != err {
		log.Errorf("err:%v", err)
		return nil, err
	}

	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)

	//查找群信息
	groupIdInt, err := strconv.Atoi(req.GroupId)
	if nil != err {
		return nil, err
	}
	qrgRsp, err := qrobot.GetGroupChatSys(ctx, &qrobot.GetGroupChatSysReq{
		GroupChatId: uint64(groupIdInt),
		CorpId:      corpId,
		AppId:       appId,
	})
	if nil != err {
		return nil, err
	}

	robotUid, err := strconv.Atoi(req.RobotUid)
	if nil != err {
		return nil, fmt.Errorf("robot uid err,uid:")
	}

	robotSerialNo, err := GetRobotSerialNo(ctx, req.RobotType, uint64(robotUid))
	if nil != err {
		return nil, err
	}

	//判断是不是白撞的
	memberIdInt, err := strconv.Atoi(req.MemberId)
	if nil != err {
		return nil, err
	}
	qrgcmRsp, err := qrobot.GetGroupChatMemberListSys(ctx, &qrobot.GetGroupChatMemberListSysReq{
		ListOption: core.NewListOption(qrobot.GetGroupChatMemberListSysReq_ListOptionIdList, []uint64{uint64(memberIdInt)}),
		CorpId:     corpId,
		AppId:      appId,
	})
	if nil != err {
		return nil, err
	}
	if nil == qrgcmRsp.List || 0 == len(qrgcmRsp.List) {
		return nil, fmt.Errorf("empty valid member list")
	}

	pubRsp, err := MqAddFriendInGroup.PubV2(
		ctx,
		&smq.PubReq{
			Hash: utils.HashStr(robotSerialNo),
		},
		&qmsg.AddFriendInGroupReqMq{
			RobotSerialNo:  robotSerialNo,
			GroupSerialNo:  qrgRsp.GroupChat.StrGroupId,
			MemberSerialNo: qrgcmRsp.List[0].StrMemberId,
			Message:        req.Message,
			CorpId:         int32(corpId),
		})
	if nil != err {
		log.Errorf("err:%v", err)
	} else if pubRsp != nil {
		log.Infof("pub mq ret %s", pubRsp.MsgId)
	}

	return &rsp, nil
}

func AddFriendInGroupMq(ctx *rpc.Context, req *smq.ConsumeReq) (*smq.ConsumeRsp, error) {
	return MqAddFriendInGroup.ProcessV2(ctx, req, func(ctx *rpc.Context, req *smq.ConsumeReq, data interface{}) (*smq.ConsumeRsp, error) {
		var rsp smq.ConsumeRsp

		merchantId, err := getMerchantId(ctx)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}

		msg, ok := data.(*qmsg.AddFriendInGroupReqMq)
		if !ok {
			return nil, fmt.Errorf("type switch err:*qmsg.AddFriendInGroupReqMq")
		}
		_, err = yc.AddContactByGroup(ctx, merchantId, &yc.AddContactByGroupReq{
			MerchantNo:     merchantId,
			RobotSerialNo:  msg.RobotSerialNo,
			GroupSerialNo:  msg.GroupSerialNo,
			MemberSerialNo: msg.MemberSerialNo,
			RelaSerialNo:   fmt.Sprintf("qmsg_add_group_friend_%d_%d", msg.CorpId, utils.Now()),
		}, &yc.BizContext{
			CorpId:  uint32(msg.CorpId),
			Module:  "qmsg",
			Context: "addFriendInGroupMq",
		})
		if nil != err {
			return nil, err
		}

		return &rsp, nil
	})
}

func AddFriendByPhone(ctx *rpc.Context, req *qmsg.AddFriendByPhoneReq) (*qmsg.AddFriendByPhoneRsp, error) {
	var rsp qmsg.AddFriendByPhoneRsp

	friendtype := req.Type - 1
	if friendtype < 0 {
		return nil, fmt.Errorf("friend type err:%v", req.Type)
	}

	robotSerialNo, err := GetRobotSerialNo(ctx, req.RobotType, req.RobotUid)
	if nil != err {
		return nil, err
	}

	pubRsp, err := MqAddFriendByPhone.PubV2(
		ctx,
		&smq.PubReq{
			Hash: utils.HashStr(robotSerialNo),
		},
		&qmsg.AddFriendByPhoneReqMq{
			RobotSerialNo: robotSerialNo,
			Type:          friendtype,
			PhoneNumber:   req.PhoneNumber,
			Message:       req.Message,
			Uid:           req.RobotUid,
		})
	if nil != err {
		log.Errorf("err:%v", err)
	} else if pubRsp != nil {
		log.Infof("pub mq ret %s", pubRsp.MsgId)
	}

	return &rsp, nil
}

func AddFriendByPhoneMq(ctx *rpc.Context, req *smq.ConsumeReq) (*smq.ConsumeRsp, error) {
	return MqAddFriendByPhone.ProcessV2(ctx, req, func(ctx *rpc.Context, req *smq.ConsumeReq, data interface{}) (*smq.ConsumeRsp, error) {
		var rsp smq.ConsumeRsp

		merchantId, err := getMerchantId(ctx)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}

		corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)

		msg := data.(*qmsg.AddFriendByPhoneReqMq)
		ycRsp, err := yc.AddWechatViaPhoneNumber(ctx, merchantId, &yc.AddWechatViaPhoneNumberReq{
			MerchantNo:    merchantId,
			RobotSerialNo: msg.RobotSerialNo,
			Type:          uint32(msg.Type),
			PhoneNumber:   msg.PhoneNumber,
			Message:       msg.Message,
			RelaSerialNo:  fmt.Sprintf("qmsg_add_friend2phone_%d_%d", corpId, utils.Now()),
		}, &yc.BizContext{
			CorpId:  corpId,
			Module:  "qmsg",
			Context: "addFriendByPhoneMq",
		})
		if nil != err && rpc.GetErrCode(err) != yc.ErrApiSysErr && rpc.GetErrCode(err) != yc.ErrFriendAlreadyAdded {
			log.Errorf("err:%v", err)
			//需要通知前端执行结果
			//return nil, err
		}

		//缓存住手机号，成为好友的通知里面会读取手机号给出去
		redisKey := fmt.Sprintf("phone_cache:%v", ycRsp.SerialNo)
		err = s.RedisGroup.Set(redisKey, []byte(msg.PhoneNumber), 7*24*60*time.Minute)
		if err != nil {
			log.Errorf("err %v", err)
			err = nil
		}

		res := qmsg.WsMsgWrapper_AddFriendByPhoneResult{Phone: msg.PhoneNumber, Message: ycRsp.Message}
		if ycRsp.Success {
			res.ResultCode = int32(qmsg.WsMsgWrapper_AddFriendByPhoneResult_ResultTypeSuccess)
			// 这里请求超时会panic,     errcode 269006, errmsg 请求超时
			if ycRsp.Data != nil {
				res.Name = ycRsp.Data.Name
				res.Avatar = ycRsp.Data.ProfilePhoto
			}
		} else {
			if ycRsp.Message == "已经是好友了" {
				res.ResultCode = int32(qmsg.WsMsgWrapper_AddFriendByPhoneResult_ResultTypeOldFriend)

				dbContactPhone, err := quan.GetContactByPhone(ctx, &quan.GetContactByPhoneReq{
					AppId:  appId,
					CorpId: corpId,
					Phone:  msg.PhoneNumber,
				})
				if nil == err {
					res.Name = dbContactPhone.ExtContact.Name
					res.Avatar = dbContactPhone.ExtContact.Avatar
				}
			} else {
				res.ResultCode = int32(qmsg.WsMsgWrapper_AddFriendByPhoneResult_ResultTypeUndefined)
			}
			addFriendByPhoneFailed(ctx, corpId, appId, msg.RobotSerialNo, msg.PhoneNumber, ycRsp)
		}

		err = pushWsMsgByRobotUid(ctx, corpId, appId, msg.Uid, []*qmsg.WsMsgWrapper{
			{
				MsgType:              uint32(qmsg.WsMsgWrapper_MsgTypeAddFriendByPhone),
				AddFriendPhoneResult: &res,
			},
		})
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}

		return &rsp, nil
	})
}

func GroupMemberIsFriend(ctx *rpc.Context, req *qmsg.GroupMemberIsFriendReq) (*qmsg.GroupMemberIsFriendRsp, error) {
	var rsp qmsg.GroupMemberIsFriendRsp

	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)

	robotInt, err := strconv.Atoi(req.RobotUid)
	if nil != err {
		return nil, err
	}
	accountInt, err := strconv.Atoi(req.AccountId)
	if nil != err {
		return nil, err
	}

	rsp.IsFriend = false

	qafRsp, err := qrobot.GetAccountFollowSys(ctx, &qrobot.GetAccountFollowSysReq{
		CorpId:    corpId,
		AppId:     appId,
		RobotUid:  uint64(robotInt),
		AccountId: uint64(accountInt),
	})
	if nil != err {
		return &rsp, nil
	}

	if nil == qafRsp.Account {
		return &rsp, nil
	}

	extContactFollowRsp, err := iquan.GetExtContactFollow(ctx, &iquan.GetExtContactFollowReq{
		CorpId: corpId,
		AppId:  appId,
		Uid:    uint64(robotInt),
		ExtUid: qafRsp.Account.ExtUid,
	})

	if err != nil {
		log.Errorf("err:%v", err)
		return &rsp, nil
	}
	if extContactFollowRsp.Follow != nil && !extContactFollowRsp.Follow.DeletedByExtContact && !extContactFollowRsp.Follow.DeletedByStaff {
		rsp.IsFriend = true
	}

	return &rsp, nil
}

func GetRobotSerialNo(ctx *rpc.Context, robotType qmsg.RobotType, uid uint64) (string, error) {
	res := ""

	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)

	switch robotType {
	case qmsg.RobotType_RobotTypePlatform:
		qrReq := &qrobot.GetAccountByRobotUidReq{RobotUid: uid}
		qrRsp, err := qrobot.GetAccountByRobotUid(ctx, qrReq)
		if nil != err {
			log.Errorf("err:%v", err)
			return "", err
		}
		if nil == qrRsp.Account {
			log.Errorf("err:%v", err)
			return "", fmt.Errorf("qrobot account indo is nil,uid:")
		}
		res = qrRsp.Account.YcSerialNo

	case qmsg.RobotType_RobotTypeHostAccount:
		qwReq := &qwhale.GetHostAccountListSysReq{
			CorpId:     corpId,
			AppId:      appId,
			ListOption: core.NewListOption(qwhale.GetHostAccountListSysReq_ListOptionUidList, []uint64{uid}),
		}
		qwRsp, err := qwhale.GetHostAccountListSys(ctx, qwReq)

		if nil != err {
			log.Errorf("err:%v", err)
			return "", err
		}

		if 0 == len(qwRsp.List) {
			log.Errorf("qwRsp.List is nil")
			return "", fmt.Errorf("qwhale host account indo is nil,uid:")
		}

		res = qwRsp.List[0].StrRobotId

	default:
		return "", fmt.Errorf("robot type err")
	}

	return res, nil
}
