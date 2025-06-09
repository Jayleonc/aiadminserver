package impl

import (
	"fmt"
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/brick/smq"
	"git.pinquest.cn/qlb/brick/utils"
	"git.pinquest.cn/qlb/qris"
	"git.pinquest.cn/qlb/qrobot"
	"git.pinquest.cn/qlb/yc"
)

func addFriendByPhoneFailed(ctx *rpc.Context, corpId, appId uint32, robotSn, phone string, rsp *yc.AddWechatViaPhoneNumberRsp) {
	var recordList []*qris.ModelRobotOptFailedRecord
	recordList = append(recordList, &qris.ModelRobotOptFailedRecord{
		CorpId:  corpId,
		AppId:   appId,
		RobotSn: robotSn,
		OptType: uint32(qris.RobotOptType_OptAddFriendByPhone),
		OptAt:   utils.Now(),
		FailMsg: rsp.Message,
		Detail: &qris.ModelRobotOptFailedRecord_Detail{
			Remark:  fmt.Sprintf("添加手机号:%v", phone),
			BizType: uint32(qris.ModelRobotOptFailedRecord_BizAddByPhoneWithQmsg),
		},
	})
	_, err := qris.AddRobotOptFailedRecordMq.PubV2(ctx, &smq.PubReq{}, qris.AddRobotOptFailedRecordMqMsg{
		RecordList: recordList,
	})
	if err != nil {
		log.Errorf("err:%v", err)
	}
}

func inviteFriendsJoinGroupFailed(ctx *rpc.Context, corpId, appId uint32, robotUid uint64, robotSn string, inviterList []*qrobot.ModelAccount, group *qrobot.ModelGroupChat, rsp *yc.InviterJoinGroupRsp) {
	if rsp.Data == nil {
		return
	}
	var inviterMap map[string]*qrobot.ModelAccount
	utils.KeyBy(inviterList, "YcSerialNo", &inviterMap)
	var recordList []*qris.ModelRobotOptFailedRecord
	if len(rsp.Data.QueryFailed) > 0 {
		for _, failedInviter := range rsp.Data.QueryFailed {
			if inviter, ok := inviterMap[failedInviter]; ok {
				recordList = append(recordList, &qris.ModelRobotOptFailedRecord{
					CorpId:   corpId,
					AppId:    appId,
					RobotUid: robotUid,
					RobotSn:  robotSn,
					OptType:  uint32(qris.RobotOptType_OptInviteFriendJoinGroup),
					OptAt:    utils.Now(),
					FailMsg:  rsp.Message,
					Detail: &qris.ModelRobotOptFailedRecord_Detail{
						Remark:  fmt.Sprintf("邀请%v进入%v群", inviter.Name, group.Name),
						BizType: uint32(qris.ModelRobotOptFailedRecord_BizInvite2GroupWithQmsg),
						BizId:   inviter.Id,
					},
				})
			}
		}
	}

	if len(rsp.Data.InviteFailTips) > 0 {
		for _, failTips := range rsp.Data.InviteFailTips {
			for _, failContact := range failTips.FailContacts {
				if contact, ok := inviterMap[failContact.ContactSerialNo]; ok {
					recordList = append(recordList, &qris.ModelRobotOptFailedRecord{
						CorpId:   corpId,
						AppId:    appId,
						RobotUid: robotUid,
						RobotSn:  robotSn,
						OptType:  uint32(qris.RobotOptType_OptInviteFriendJoinGroup),
						OptAt:    utils.Now(),
						FailMsg:  failTips.FailTips,
						Detail: &qris.ModelRobotOptFailedRecord_Detail{
							Remark:  fmt.Sprintf("邀请%v进入%v群", contact.Name, group.Name),
							BizType: uint32(qris.ModelRobotOptFailedRecord_BizInvite2GroupWithQmsg),
							BizId:   contact.Id,
						},
					})
				}
			}
		}
	}

	if len(recordList) == 0 {
		return
	}

	_, err := qris.AddRobotOptFailedRecordMq.PubV2(ctx, &smq.PubReq{}, qris.AddRobotOptFailedRecordMqMsg{
		RecordList: recordList,
	})
	if err != nil {
		log.Errorf("err:%v", err)
	}
}
