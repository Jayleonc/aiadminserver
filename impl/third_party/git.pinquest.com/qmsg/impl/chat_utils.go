package impl

import (
	"fmt"
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/json"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/brick/utils"
	"git.pinquest.cn/qlb/core"
	"git.pinquest.cn/qlb/extrpkg/github.com/go-redis/redis"
	"git.pinquest.cn/qlb/featswitch"
	"git.pinquest.cn/qlb/qmsg"
	"git.pinquest.cn/qlb/qrobot"
	"git.pinquest.cn/qlb/quan"
	"git.pinquest.cn/qlb/qwhale"
	"strconv"
	"time"
)

const (
	UserLastSend    = "qmsg_user_last_send_%d"
	ContactLastSend = "qmsg_contact_last_send_%d"
)

func updateChatFormFollow(chat *qmsg.ModelChat, follow *quan.ModelExtContactFollow) {
	if chat.Detail == nil {
		chat.Detail = &qmsg.ModelChat_Detail{}
	}

	chat.Detail.Avatar = follow.Avatar
	chat.Detail.Remark = follow.Info.Remark
	chat.Detail.Name = follow.Name
	chat.Detail.CorpName = follow.CorpName
	chat.Detail.UserType = follow.UserType
	chat.Detail.UpdatedAt = uint32(time.Now().Unix())

	if chat.Detail.DeletedByExtContactAt == 0 && follow.DeletedByExtContact {
		chat.Detail.DeletedByExtContactAt = uint32(time.Now().Unix())
	}
	if !follow.DeletedByExtContact {
		chat.Detail.DeletedByExtContactAt = 0
	}
	if chat.Detail.DeletedByStaffAt == 0 && follow.DeletedByStaff {
		chat.Detail.DeletedByStaffAt = uint32(time.Now().Unix())
	}
	if !follow.DeletedByStaff {
		chat.Detail.DeletedByStaffAt = 0
	}

	if chat.Detail.DeletedByExtContactAt == 0 {
		chat.Detail.SerialNo = ""
	}

	if chat.Detail.DeletedByExtContactAt == 0 && chat.Detail.DeletedByStaffAt == 0 {
		chat.Detail.YcOneSideType = 0
	}
}

func getChat(ctx *rpc.Context, corpId, appId uint32, robotUid, chatExtId uint64, isGroup, isStrict bool) (*qmsg.ModelChat, *qmsg.ModelAssignChat, error) {
	chat, err := Chat.getByRobotAndExt(ctx, corpId, appId, robotUid, chatExtId, isGroup)
	if Chat.IsNotFoundErr(err) {
		if isStrict {
			if !isGroup {
				_, err := qrobot.GetAccountFollowSys(ctx, &qrobot.GetAccountFollowSysReq{
					CorpId:    corpId,
					AppId:     appId,
					RobotUid:  robotUid,
					AccountId: chatExtId,
				})
				if err != nil {
					log.Errorf("err:%v", err)
					return nil, nil, err
				}
			} else {
				listRsp, err := qrobot.GetGroupChatMemberListSys(ctx, &qrobot.GetGroupChatMemberListSysReq{
					ListOption: core.NewListOption().
						AddOpt(qrobot.GetGroupChatMemberListSysReq_ListOptionGroupIdList, chatExtId).
						AddOpt(qrobot.GetGroupChatMemberListSysReq_ListOptionUidIdList, robotUid).
						SetLimit(1).SetSkipCount(),
					CorpId: corpId,
					AppId:  appId,
				})
				if err != nil {
					log.Errorf("err:%v", err)
					return nil, nil, err
				}

				if len(listRsp.List) == 0 {
					return nil, nil, rpc.InvalidArg("未知群")
				}
			}
		}

		chat = &qmsg.ModelChat{}
		chat.RobotUid = robotUid
		chat.ChatExtId = chatExtId
		chat.IsGroup = isGroup
		chat.CorpId = corpId
		chat.AppId = appId
		err = Chat.Create(ctx, &chat)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, nil, err
		}
	} else if err != nil {
		//warning.ReportMsg(ctx, fmt.Sprintf("err: %v", err))
		log.Errorf("err:%v", err)
		return nil, nil, err
	}

	assignChat, err := AssignChat.getByCid(ctx, corpId, appId, chat.Id)
	if err != nil && !AssignChat.IsNotFoundErr(err) {
		log.Errorf("err:%v", err)
		return chat, nil, err
	}

	return chat, assignChat, nil
}

func getSendRedisList(key string, tillAt uint32, offset, limit int64) ([]string, error) {
	return s.RedisGroup.ZRangeByScore(key, redis.ZRangeBy{
		Min:    "-inf",
		Max:    fmt.Sprintf("%d", tillAt),
		Offset: offset,
		Count:  limit,
	})
}

// @desc: 删除客服最近发送时间列表
func removeUserSendRedis(corpId uint32, chatIdList []string) error {
	_, err := s.RedisGroup.ZRem(fmt.Sprintf(UserLastSend, corpId), chatIdList)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	return nil
}

// @desc: 更新客服最近发送时间，删除联系人最近发送时间
func updateUserSendRedis(ctx *rpc.Context, corpId, appId, sendAt uint32, chatId uint64) error {
	sessionSetting := getSessionSetting(ctx, corpId, appId)
	if sessionSetting == nil {
		return nil
	}
	if sessionSetting.AllowCloseBySys && sessionSetting.CloseBySysMinutes > 0 {
		err := s.RedisGroup.ZAdd(fmt.Sprintf(UserLastSend, corpId), redis.Z{
			Score:  float64(sendAt),
			Member: chatId,
		})
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
	}
	_, err := s.RedisGroup.ZRem(fmt.Sprintf(ContactLastSend, corpId), chatId)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	return nil
}

// @desc: 更新联系人最近发送时间，删除客服最近发送时间
func updateContactSendRedis(ctx *rpc.Context, corpId, appId, sendAt uint32, chatId uint64) error {
	sessionSetting := getSessionSetting(ctx, corpId, appId)
	if sessionSetting == nil {
		return nil
	}

	_, err := s.RedisGroup.ZRem(fmt.Sprintf(UserLastSend, corpId), chatId)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	if sessionSetting.AllowAutoReassignUnanswered && sessionSetting.AutoReassignUnansweredMinutes > 0 {
		err := s.RedisGroup.ZAdd(fmt.Sprintf(ContactLastSend, corpId), redis.Z{
			Score:  float64(sendAt),
			Member: chatId,
		})
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
	}

	return nil
}

// 更新联系人最近发送消息时间(这里保存的不仅仅是客户最后发消息时间，当客服处理超时，触发重新分配时，也会更新这个时间，避免死循环分配)
func updateContactSendAt(ctx *rpc.Context, corpId, appId, sendAt uint32, chatId uint64) error {
	sessionSetting := getSessionSetting(ctx, corpId, appId)
	err := doUpdateContactSendAt(sessionSetting, corpId, sendAt, chatId)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	return nil
}

// 更新联系人最近发送消息时间(这里保存的不仅仅是客户最后发消息时间，当客服处理超时，触发重新分配时，也会更新这个时间，避免死循环分配)
func batchUpdateContactSendAt(ctx *rpc.Context, corpId, appId, sendAt uint32, chatIdList []uint64) error {
	sessionSetting := getSessionSetting(ctx, corpId, appId)
	for _, chatId := range chatIdList {
		err := doUpdateContactSendAt(sessionSetting, corpId, sendAt, chatId)
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
	}
	return nil
}

// 更新联系人最近发送消息时间(这里保存的不仅仅是客户最后发消息时间，当客服处理超时，触发重新分配时，也会更新这个时间，避免死循环分配)
func doUpdateContactSendAt(sessionSetting *qmsg.ModelSessionConfig_SessionSetting, corpId, sendAt uint32,
	chatId uint64) error {
	if sessionSetting == nil || !sessionSetting.AllowAutoReassignUnanswered ||
		sessionSetting.AutoReassignUnansweredMinutes == 0 {
		return nil
	}
	score, err := s.RedisGroup.ZScore(fmt.Sprintf(ContactLastSend, corpId), strconv.Itoa(int(chatId)))
	if err != nil && err != redis.Nil {
		log.Errorf("err:%v", err)
		return err
	}
	if score > 0 {
		err = s.RedisGroup.ZAdd(fmt.Sprintf(ContactLastSend, corpId), redis.Z{
			Score:  float64(sendAt),
			Member: chatId,
		})
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
	}
	return nil
}

// @desc: 删除联系人最后发送时间队列
func deleteContactSendRedis(corpId uint32) error {
	err := s.RedisGroup.Del(fmt.Sprintf(ContactLastSend, corpId))
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	return nil
}

// @desc: 删除客服最后发送时间队列
func deleteUserSendRedis(corpId uint32) error {
	err := s.RedisGroup.Del(fmt.Sprintf(UserLastSend, corpId))
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	return nil
}

// @desc: 删除客服最近发送时间列表
func removeSendList4Chat(ctx *rpc.Context, corpId, appId uint32, chatIdList []uint64) error {
	sessionSetting := getSessionSetting(ctx, corpId, appId)
	if sessionSetting == nil {
		return nil
	}
	if sessionSetting.AllowAutoReassignUnanswered && sessionSetting.AutoReassignUnansweredMinutes > 0 {
		for _, chatId := range chatIdList {
			_, err := s.RedisGroup.ZRem(fmt.Sprintf(UserLastSend, corpId), chatId)
			if err != nil {
				log.Errorf("err:%v", err)
				return err
			}
		}
	}
	if sessionSetting.AllowCloseBySys && sessionSetting.CloseBySysMinutes > 0 {
		for _, chatId := range chatIdList {
			_, err := s.RedisGroup.ZRem(fmt.Sprintf(ContactLastSend, corpId), chatId)
			if err != nil {
				log.Errorf("err:%v", err)
				return err
			}
		}
	}
	return nil
}

// @desc: 填充会话列表中的好友备注
func fillRemark(ctx *rpc.Context, corpId, appId uint32, robotUidList, extUidList []uint64, rsp *qmsg.GetChatListRsp) error {
	quanSrv := quanService{}
	extContactFollowMap, err := quanSrv.getAccountFollowMap(ctx, corpId, appId, robotUidList, extUidList)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	for _, chat := range rsp.List {
		account, ok := rsp.AccountMap[chat.ChatExtId]
		if !ok {
			continue
		}
		key := utils.JoinUint64([]uint64{chat.RobotUid, account.ExtUid}, "_")
		if extContactFollow, ok := extContactFollowMap[key]; ok {
			if extContactFollow.Info == nil {
				continue
			}
			if chat.Detail == nil {
				chat.Detail = &qmsg.ModelChat_Detail{}
			}
			chat.Detail.Remark = extContactFollow.Info.Remark
		}
	}
	return nil
}

// @desc: 添加关闭会话的消息
func addCloseTransferMsg(ctx *rpc.Context, corpId, appId uint32, assignChatId uint64, mark string) error {
	var transferLog qmsg.ModelTransferLog
	err := TransferLog.WhereCorpApp(corpId, appId).Where("assign_chat_id", assignChatId).First(ctx, &transferLog)
	if err != nil {
		if !TransferLog.IsNotFoundErr(err) {
			log.Errorf("err:%v", err)
			return err
		}
		return nil
	}

	var old qmsg.ModelMsgBox
	err = ChoiceMsgBoxDb(MsgBox.W(corpId, appId, transferLog.Uid).
		Where(DbCliMsgId, transferLog.CliMsgId), transferLog.CorpId).First(ctx, &old)
	if err != nil {
		if !MsgBox.IsNotFoundErr(err) {
			log.Errorf("err:%v", err)
			return err
		}
		log.Errorf("err:找不转介记录")
		return nil
	}

	var msg qmsg.ChatTransferChatRemarkMsg
	err = json.Unmarshal([]byte(old.Msg), &msg)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	msg.Date = time.Now().Format("2006年01月02日 15:04")
	msg.TransferType = uint32(qmsg.ChatTransferChatRemarkMsg_TransferTypeEnd)
	msg.Mark = mark
	msgBuffer, err := json.Marshal(msg)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	cliMsgId := "transfer_assign_chat_" + utils.GenRandomStr()
	_, _, err = addMsg(ctx, &qmsg.ModelMsgBox{
		CorpId:      corpId,
		AppId:       appId,
		Uid:         old.Uid,
		CliMsgId:    cliMsgId,
		MsgType:     uint32(qmsg.MsgType_MsgTypeTransferChatRemark),
		ChatType:    old.ChatType,
		ChatId:      old.ChatId,
		ChatMsgType: uint32(qmsg.ChatMsgType_ChatMsgTypeTransferChatRemark),
		Msg:         string(msgBuffer),
		SentAt:      uint32(time.Now().Unix()),
	}, false)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	return nil
}

func checkForCuzIMRevoke(ctx *rpc.Context, robotSn string) error {
	log.Debugf("check for CUSTOMER IM REVOKE : %v", robotSn)

	hostAcc, err := qwhale.GetHostAccountBySn(ctx, &qwhale.GetHostAccountBySnReq{
		StrRobotId: robotSn,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	const fsKey = "QMSG_ALLOW_SHOW_REVOKE"
	enabled, err := featswitch.GetFeatureFlagEnabled(ctx, &featswitch.GetFeatureFlagEnabledReq{Key: fsKey, CorpId: hostAcc.HostAccount.CorpId, AppId: hostAcc.HostAccount.AppId})
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	if !enabled.Enabled {
		log.Error("unsupported msg type")
		return rpc.InvalidArg("unsupported msg type")
	}

	return nil
}
