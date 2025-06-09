package impl

import (
	"errors"
	"fmt"
	"git.pinquest.cn/qlb/brick/json"
	"math/rand"
	"strings"
	"sync"
	"time"

	"git.pinquest.cn/qlb/extrpkg/github.com/elliotchance/pie/pie"
	"git.pinquest.cn/qlb/quan"
	"git.pinquest.cn/qlb/yc"

	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/dbproxy"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/brick/smq"
	"git.pinquest.cn/qlb/brick/utils"
	"git.pinquest.cn/qlb/core"
	"git.pinquest.cn/qlb/qmsg"
	"git.pinquest.cn/qlb/qrobot"
)

const (
	maxSyncMsgCount    = 10000
	maxSyncMsgLimit    = 1000
	maxExecAddMsgCount = 50
)

var ErrAllocSeqConflict = errors.New("alloc seq conflict")
var ErrAddMsgConflict = errors.New("add msg conflict")
var maxExecAddMsgChan = make(chan bool, maxExecAddMsgCount)

var MsgFilterText = []string{
	"我已经添加了你，现在我们可以开始聊天了。",
	"我已经添加了你，现在我们可以开始聊天了",
	"我通过了你的联系人验证请求，现在我们可以开始聊天了",
	"我已經添加了你，現在我們可以開始聊天了。",
	"我已經添加了你，現在我們可以開始聊天了",
}

// 检查消息是否命中成为好友后第一条不提醒内容
func isChatFirstMsgReadState(msgBox *qmsg.ModelMsgBox) bool {
	// 私聊：判断成为好友后，第一条文本消息是否要未读提醒
	// 先检测文本内容是否符合，目前是写死内容，注意：这里只能识别微信系统自带的内容（无法识别用户自定义的内容）
	if msgBox.ChatType == uint32(qmsg.ChatType_ChatTypeSingle) &&
		msgBox.MsgType == uint32(qmsg.MsgType_MsgTypeChat) &&
		msgBox.ChatMsgType == uint32(qmsg.ChatMsgType_ChatMsgTypeText) &&
		utils.InSliceStr(msgBox.Msg, MsgFilterText) {
		// 命中，标记已读
		return true
	}
	// 未命中，标记未读
	return false
}

// 针对成为好友后，第一条文本消息的已读状态过滤
func filterChatFirstMsgReadState(ctx *rpc.Context, userIdList []uint64, msgBox *qmsg.ModelMsgBox) (map[uint64]bool, error) {
	filterResult := map[uint64]bool{}
	// 先检查消息是否符合不提醒范围
	if !isChatFirstMsgReadState(msgBox) {
		// 不符合，不用做处理
		return filterResult, nil
	}
	// 查询客服配置
	chatFirstMsgReadTypeSettingList, err := getUserSettingChatFirstMsgReadTypeForUserIdList(ctx, msgBox.CorpId, msgBox.AppId, userIdList)
	if err != nil {
		log.Errorf("err:%v", err)
		return filterResult, err
	}
	// 检查客服账号是否设置了不提醒第一条消息
	for userId, chatFirstMsgReadType := range chatFirstMsgReadTypeSettingList {
		// 开启了不提醒
		if chatFirstMsgReadType == uint32(qmsg.ModelUser_ChatFirstMsgReadTypeRead) {
			filterResult[userId] = true
			continue
		}
		filterResult[userId] = false
	}
	return filterResult, nil
}

func getTop1Msg(ctx *rpc.Context, corpId, appId uint32, uid uint64) (*qmsg.ModelMsgBox, error) {
	var top1 qmsg.ModelMsgBox
	err := ChoiceMsgBoxDb(MsgBox.W(corpId, appId, uid).
		Select("Max(msg_seq) as msg_seq"), corpId).
		First(ctx, &top1)
	if err != nil {
		if MsgBox.IsNotFoundErr(err) {
			// 没有消息
			return nil, nil
		} else {
			log.Errorf("err:%v", err)
			return nil, err
		}
	}
	return &top1, nil
}

func getOrAddSeq(ctx *rpc.Context, uid uint64) (*qmsg.ModelSeq, error) {
	var s qmsg.ModelSeq
	err := Seq.Where(DbUid, uid).First(ctx, &s)
	if err != nil {
		if Seq.IsNotFoundErr(err) {
			s = qmsg.ModelSeq{
				Uid:    uid,
				CurSeq: 0,
			}
			err = Seq.Create(ctx, &s)
			if err != nil {
				if rpc.GetErrCode(err) == dbproxy.ErrModelUniqueIndexConflict {
					log.Warnf("create seq unique index conflict")
					err = Seq.Where(DbUid, uid).First(ctx, &s)
					if err != nil {
						log.Errorf("err:%v", err)
						return nil, err
					}
					log.Infof("get seq back %+v", s)
				} else {
					log.Errorf("err:%v", err)
					return nil, err
				}
			} else {
				log.Infof("create seq %+v", s)
			}
		} else {
			log.Errorf("err:%v", err)
			return nil, err
		}
	}
	return &s, nil
}
func getNextSeq(ctx *rpc.Context, uid uint64, nextSeq *uint64) error {
	// return genNextSeq(ctx, uid, nextSeq)

	rsp, err := qmsg.GetNextSeq(ctx, &qmsg.GetNextSeqReq{Uid: uid})
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	*nextSeq = rsp.NextSeq

	return nil
}
func setSeqAtLeast(ctx *rpc.Context, uid, seq uint64) error {
	res, err := Seq.Where(map[string]interface{}{
		DbUid:          uid,
		DbCurSeq + "<": seq,
	}).Update(ctx, map[string]interface{}{
		DbCurSeq: seq,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	log.Infof("set seq to %d return %d rows, uid %d", seq, res.RowsAffected, uid)
	return nil
}
func checkCliMsgId(ctx *rpc.Context, corpId, appId uint32, uid uint64, cliMsgId string) error {
	var old qmsg.ModelMsgBox
	err := ChoiceMsgBoxDb(MsgBox.W(corpId, appId, uid, DbCliMsgId, cliMsgId), corpId).
		First(ctx, &old)
	if err != nil {
		if !MsgBox.IsNotFoundErr(err) {
			log.Errorf("err:%v", err)
			return err
		}
	} else {
		log.Warnf("cli msg id %s existed", cliMsgId)
		return rpc.CreateError(qmsg.ErrCliMsgIdExisted)
	}
	return nil
}
func ensureHasRobotPerm(ctx *rpc.Context, user *qmsg.ModelUser, robotUidList ...uint64) error {
	var robotList []*qmsg.ModelUserRobot
	err := UserRobot.Select(DbRobotUid).Where(map[string]interface{}{
		DbCorpId: user.CorpId,
		DbAppId:  user.AppId,
		DbUid:    user.Id,
	}).Find(ctx, &robotList)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	uidMap := utils.PluckUint64Map(robotList, "RobotUid")
	if len(robotUidList) > 0 {
		for _, robotUid := range robotUidList {
			if _, b := uidMap[robotUid]; !b {
				return rpc.CreateError(qmsg.ErrNotHasRobotPerm)
			}
		}
	}
	return nil
}

// 2023-01-12 一些特殊场景的，需要客服工作台及时知道结果的，skipWait传入true，跳过排队的等待
func addMsg(ctx *rpc.Context, msg *qmsg.ModelMsgBox, skipWait bool) (*qmsg.ModelChat, uint64, error) {
	// 查下消息是不是已经加过了
	var old qmsg.ModelMsgBox
	err := ChoiceMsgBoxDb(MsgBox.W(msg.CorpId, msg.AppId, msg.Uid).
		Where(DbCliMsgId, msg.CliMsgId), msg.CorpId).First(ctx, &old)
	if err != nil {
		if !MsgBox.IsNotFoundErr(err) {
			log.Errorf("err:%v", err)
			return nil, 0, err
		}
	} else {
		// 跳过不 add, 返回当时的 msg seq
		msg.MsgSeq = old.MsgSeq
		return nil, 0, nil
	}

	// 控制并发,不然dbproxy很容易熔断
	// 2022-9-19 缩小了控制的代码范围，提高了并发速度，也引起了db的熔断，降低了mq的并发数来进行平衡
	for i := 0; i < 3; i++ {
		var top1 *qmsg.ModelMsgBox
		err = func() error {
			if !skipWait {
				maxExecAddMsgChan <- true
				defer func() {
					<-maxExecAddMsgChan
				}()
			}
			err := getNextSeq(ctx, msg.Uid, &msg.MsgSeq)
			if err != nil {
				log.Errorf("err:%v", err)
				return err
			}

			top1, err = getTop1Msg(ctx, msg.CorpId, msg.AppId, msg.Uid)
			if err != nil {
				log.Errorf("err:%v", err)
				return err
			}

			return nil
		}()
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, 0, err
		}

		if top1 != nil && top1.MsgSeq >= msg.MsgSeq {
			// seq 表示的记录落后了, 需要修正
			log.Warnf("top1 msg seq %d, next seq %d, need to set seq table",
				top1.MsgSeq, msg.MsgSeq)
			_ = setSeqAtLeast(ctx, msg.Uid, top1.MsgSeq)
			continue
		}

		msg.Id = 0
		err = ChoiceMsgBoxDb(MsgBox.Where(map[string]interface{}{
			DbCorpId:         msg.CorpId,
			DbAppId:          msg.AppId,
			DbUid:            msg.Uid,
			DbMsgSeq + " >=": msg.MsgSeq,
		}), msg.CorpId).
			CondCreate(ctx, uint32(dbproxy.CondInsertModelReq_MethodNotExists), msg)
		if err != nil {
			if rpc.GetErrCode(err) == dbproxy.ErrCondNotMatch {
				log.Warnf("create conflict, continue")
				continue
			}
			log.Errorf("err:%v", err)
			return nil, 0, err
		}

		chat, lastSeq := updateOrCreateChat(ctx, msg)
		if chat == nil {
			log.Errorf("err:updateOrCreateChat error, chat is nil, %v", msg)
			return nil, 0, rpc.CreateError(int32(qmsg.ErrCode_ErrCreateOrUpdateChat))
		}

		return chat, lastSeq, nil
	}
	log.Warnf("add msg final fail, msg %+v", msg)
	return nil, 0, ErrAddMsgConflict
}

// 检查是否使用智能助手
func isUseAiEntertain(ctx *rpc.Context, aiConfig *qmsg.ModelAiEntertainConfig, msg *qmsg.ModelMsgBox, chat *qmsg.ModelChat) (useAi bool, err error) {

	log.Debugf("[ai_msg] isUserAi")

	isInManualTime := func(ctx *rpc.Context, corpId, appId, manualType uint32) (bool, error) {
		// 全部时间可转人工
		if manualType == uint32(qmsg.ModelAiEntertainConfig_AllTime) {
			return true, nil
		}

		if manualType == uint32(qmsg.ModelAiEntertainConfig_WorkTime) {
			// 3.是否在工作时间
			isOnWork, err := getIsOnWork(ctx, corpId, appId)
			if err != nil {
				log.Errorf("err %v", err)
				return false, err
			}

			// 非工作时间
			if isOnWork {
				log.Debugf("[ai_msg] isUserAi")
				return true, nil
			}

			log.Debugf("[ai_msg] isUserAi")
		}
		log.Debugf("[ai_msg] isUserAi")
		return false, nil
	}

	// 未配置智能助手
	if aiConfig == nil {
		log.Debugf("[ai_msg] isUserAi")

		return false, nil
	}

	var allowUseAi bool
	// 已分配给智能助手
	if chat.Detail != nil && chat.Detail.EntertainType == uint32(qmsg.ModelChat_Detail_Ai) {
		log.Debugf("[ai_msg] isUserAi")

		allowUseAi = true
	}

	if chat.Detail == nil || chat.Detail.EntertainType == uint32(qmsg.ModelChat_Detail_Manual) || chat.Detail.EntertainType == uint32(qmsg.ModelChat_Detail_Nil) {
		log.Debugf("[ai_msg] isUserAi")

		return false, nil
	}

	// 1.是否优先客服招待 私聊
	if !chat.IsGroup && aiConfig.Priority == uint32(qmsg.ModelAiEntertainConfig_True) {
		log.Debugf("[ai_msg] isUserAi")
		allowUseAi = true
	}

	// 1.是否优先客服招待 群聊
	if chat.IsGroup && aiConfig.GroupPriority == uint32(qmsg.ModelAiEntertainConfig_True) {
		log.Debugf("[ai_msg] isUserAi")
		allowUseAi = true
	}

	// 无法使用智能助手
	if !allowUseAi {
		log.Debugf("[ai_msg] isUserAi")

		return false, nil
	}

	var isManualMsg bool
	// 群聊 且 转人工的消息类型

	if chat.IsGroup && utils.InSliceUint32(msg.ChatMsgType, aiConfig.ManualGroupChatMsgTypeList) {
		log.Debugf("[ai_msg] isUserAi")

		isManualMsg = true
	}

	// 私聊 且 转人工的消息类型
	if !chat.IsGroup && utils.InSliceUint32(msg.ChatMsgType, aiConfig.ManualChatMsgTypeList) {
		log.Debugf("[ai_msg] isUserAi")

		isManualMsg = true
	}

	// 文本内容是否触发转人工
	log.Debugf("%v  %v %v", msg.ChatMsgType, msg.Msg, aiConfig.ManualKeyword)
	if msg.ChatMsgType == uint32(qmsg.ChatMsgType_ChatMsgTypeText) && msg.Msg == aiConfig.ManualKeyword {
		log.Debugf("[ai_msg] isUserAi")

		isManualMsg = true
	}

	// 没有满足转人工的条件
	if !isManualMsg {
		log.Debugf("[ai_msg] isUserAi")

		return true, nil
	}

	inWorkTime, err := isInManualTime(ctx, msg.CorpId, msg.AppId, aiConfig.ManualType)
	if err != nil {
		log.Errorf("err %v", err)
		return false, err
	}

	if inWorkTime {
		log.Debugf("[ai_msg] isUserAi")

		return false, nil
	}

	log.Debugf("[ai_msg] isUserAi")

	return true, nil
}

func receiveMsgAssign(ctx *rpc.Context, msg *qmsg.ModelMsgBox, chat *qmsg.ModelChat) (assignChat *qmsg.ModelAssignChat, useAi bool, err error) {

	var aiConfig qmsg.ModelAiEntertainConfig
	err = aiEntertainConfig.WhereCorpApp(msg.CorpId, msg.AppId).First(ctx, &aiConfig)
	if err != nil && rpc.GetErrCode(err) != qmsg.ErrAiEntertainConfigNotFound {
		log.Errorf("err %v", err)
		return nil, false, err
	}

	assignChat, isAssign, err := needAssign(ctx, msg, chat)
	if err != nil {
		log.Errorf("err %v", err)
		return nil, false, err
	}

	useAi, err = isUseAiEntertain(ctx, &aiConfig, msg, chat)
	if err != nil {
		log.Errorf("err %v", err)
		return nil, false, err
	}

	// 非资产，不使用智能助手
	if isAssign && !useAi {
		assignUid, scene, err := findAssignUser(ctx, msg.CorpId, msg.AppId, msg.Uid, msg.ChatId, msg.ChatType == uint32(qmsg.ChatType_ChatTypeGroup), 0)
		if err != nil {
			log.Errorf("err %v", err)
			return nil, false, err
		}

		assignChat, err = assignUser(ctx, msg.CorpId, msg.AppId, msg.Uid, msg.ChatId, assignUid, msg.ChatType == uint32(qmsg.ChatType_ChatTypeGroup), scene)
		if err != nil {
			log.Errorf("err %v", err)
			return nil, false, err
		}
	}

	return assignChat, useAi, nil
}

/*
TODO 优化方案：
- 判断如果是媒体文件，同步请求由创的接口拿到url，并记录到数据库
- 异步   下载  ->  上传  ->   修改旧的msg为新的连接
*/
func processRecvYcChatMsg(ctx *rpc.Context, req *smq.ConsumeReq, data interface{}) error {
	msg := data.(*qrobot.RecvChatMsgReq)
	recvData := msg.MsgData
	extraMsg := msg.ExtraMsg

	log.Debugf("get msg from %v to %v", recvData.SenderSerialNo, recvData.ReceiverSerialNo)
	log.Debugf("YcChatMsg:%+v", recvData)

	// 新逻辑！！！
	// 获取机器人的信息
	robot, err := getRobotBySn(ctx, req.CorpId, req.AppId, msg.RobotSerialNo)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	var chatType qmsg.ChatType
	var chatId uint64
	var acc *qrobot.ModelAccount
	var mentionName string

	switch msg.ChatType {
	case uint32(qrobot.ChatType_ChatTypeSingle):
		chatType = qmsg.ChatType_ChatTypeSingle

		accountRsp, err := qrobot.GetAccountByYcSerialNo(ctx, &qrobot.GetAccountByYcSerialNoReq{
			CorpId:     robot.CorpId,
			AppId:      robot.AppId,
			YcSerialNo: recvData.SenderSerialNo,
		})
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
		account := accountRsp.Account
		// 过滤掉员工和未知用户的消息
		if account.AccountType == uint32(qrobot.AccountType_AccountTypeStaff) {
			return nil
		}

		acc = account
		chatId = account.Id
		mentionName = fmt.Sprintf("[%s]", account.Name)
	case uint32(qrobot.ChatType_ChatTypeGroup):
		chatType = qmsg.ChatType_ChatTypeGroup
		groupChatListRsp, err := qrobot.GetGroupChatListSys(ctx, &qrobot.GetGroupChatListSysReq{
			ListOption: core.NewListOption().
				SetSkipCount().
				SetLimit(1).
				AddOpt(uint32(qrobot.GetGroupChatListSysReq_ListOptionStrGroupId),
					[]string{recvData.ReceiverSerialNo}),
			CorpId: robot.CorpId,
			AppId:  robot.AppId,
		})
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
		if len(groupChatListRsp.List) == 0 {
			return rpc.CreateError(qrobot.ErrGroupChatNotFound)
		}
		group := groupChatListRsp.List[0]
		// 临时过滤没有关注群的消息
		if msg.ChatType == uint32(qmsg.ChatType_ChatTypeGroup) {
			_, err = qrobot.CheckRobotInGroupChat(ctx, &qrobot.CheckRobotInGroupChatReq{
				RobotUid:    robot.Uid,
				GroupChatId: group.Id,
				CorpId:      group.CorpId,
				AppId:       group.AppId,
			})
			if err != nil {
				log.Warnf("err:%v", err)
				return err
			}
		}
		// 如果不是外部群，就过滤掉
		if group.GroupType != uint32(qrobot.GroupType_GroupTypeOuter) {
			log.Warnf("group is not outer")
			return nil
		}
		chatId = group.Id
		qrobotRsp, err := qrobot.GetAccountByYcSerialNo(ctx, &qrobot.GetAccountByYcSerialNoReq{
			CorpId:     group.CorpId,
			AppId:      group.AppId,
			YcSerialNo: recvData.SenderSerialNo,
		})
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
		acc = qrobotRsp.Account
		mentionName = fmt.Sprintf("[%s]/[%s]", acc.Name, group.Name)

	}

	err = handleReceiveMsg(ctx, robot.CorpId, robot.AppId, robot.Uid, chatId, acc, msg.RobotSerialNo, extraMsg, recvData, msg.SentAt, chatType, mentionName, true)
	if err != nil {
		if rpc.GetErrCode(err) == rpc.InvalidArgErrCode {
			return nil
		}
		log.Errorf("err:%v", err)
		return err
	}

	return nil
}

func getRobotBySn(ctx *rpc.Context, corpId, appId uint32, robotSn string) (*qrobot.ModelAccount, error) {
	robotRsp, err := qrobot.GetAccountByYcSerialNo(ctx, &qrobot.GetAccountByYcSerialNoReq{
		CorpId:     corpId,
		AppId:      appId,
		YcSerialNo: robotSn,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	return robotRsp.Account, nil
}
func getRobotByUid(ctx *rpc.Context, corpId, appId uint32, uid uint64) (*qrobot.ModelAccount, error) {
	rsp, err := qrobot.GetAccountListSys(ctx, &qrobot.GetAccountListSysReq{
		ListOption: core.NewListOption().SetSkipCount().
			AddOpt(qrobot.GetAccountListSysReq_ListOptionUidList, uid).SetLimit(1),
		CorpId: corpId,
		AppId:  appId,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	if len(rsp.List) == 0 {
		return nil, rpc.CreateError(qrobot.ErrRobotNotFound)
	}
	return rsp.List[0], nil
}

// TODO: 设计不合理，这里要改成用mq发送
func sendAssignMsg(ctx *rpc.Context, req *qmsg.SendMsgReq, assignChat *qmsg.ModelAssignChat, remark string) (*qmsg.SendMsgRsp, error) {
	var rsp qmsg.SendMsgRsp
	msg := req.Msg
	chat, _, err := addMsg(ctx, msg, false)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	var remarkMsg qmsg.ChatTransferChatRemarkMsg

	err = json.Unmarshal([]byte(msg.Msg), &remarkMsg)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	wrapper := &qmsg.WsMsgWrapper{
		MsgType: uint32(qmsg.WsMsgWrapper_MsgTypeAssignChat),
		MsgBox: &qmsg.ModelMsgBox{
			CorpId:      msg.CorpId,
			AppId:       msg.AppId,
			Uid:         msg.Uid,
			CliMsgId:    msg.CliMsgId,
			MsgType:     uint32(qmsg.MsgType_MsgTypeSystem),
			ChatType:    msg.ChatType,
			ChatId:      msg.ChatId,
			ChatMsgType: uint32(qmsg.ChatMsgType_ChatMsgTypeNil),
			Msg:         fmt.Sprintf("%s在 %s 将此对话分配给 %s", remarkMsg.Username, time.Now().Format("2006-01-02 15:04"), remarkMsg.TransferUsername),
			SentAt:      msg.SentAt,
		},
		Chat:       chat,
		AssignChat: assignChat,
		Remark:     remark,
	}
	err = pushWsMsgByChat(ctx, chat, assignChat, []*qmsg.WsMsgWrapper{wrapper})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	return &rsp, nil
}

func needBlock(ctx *rpc.Context, msg *qmsg.ModelMsgBox) (bool, error) {
	if msg.ChatType == uint32(qmsg.ChatType_ChatTypeGroup) {
		groupRsp, err := qrobot.GetGroupChatListSys(ctx, &qrobot.GetGroupChatListSysReq{
			ListOption: core.NewListOption().SetSkipCount().SetLimit(1).
				AddOpt(qrobot.GetGroupChatListSysReq_ListOptionGroupIdList, msg.ChatId),
			CorpId: msg.CorpId,
			AppId:  msg.AppId,
		})
		if err != nil {
			log.Errorf("err:%v", err)
			return false, err
		}
		if len(groupRsp.List) == 0 {
			return false, rpc.InvalidArg("group not found")
		}

		group := groupRsp.List[0]
		isWatch := true
		for _, uid := range group.WatchRobotIdList {
			if uid == msg.Uid {
				isWatch = false
				break
			}
		}
		for _, uid := range group.WatchHostAccountUidList {
			if uid == msg.Uid {
				isWatch = false
				break
			}
		}
		// 退出群聊处理：因为退出群聊后，已经取消关注群，但又需要给客服工作台发送消息，让客户感知自己已退出群，所以这里isWatch设置为false
		if isWatch && msg.MsgType == uint32(qmsg.MsgType_MsgTypeGroupMemberLeft) {
			isWatch = false
		}
		return isWatch, nil
	}

	return false, nil
}

func genNextSeq(ctx *rpc.Context, uid uint64, nextSeq *uint64) error {
	for i := 0; i < 3; i++ {
		s, err := getOrAddSeq(ctx, uid)
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
		next := s.CurSeq + 1
		res, err := Seq.Where(map[string]interface{}{
			DbId:     s.Id,
			DbCurSeq: s.CurSeq,
		}).Update(ctx, map[string]interface{}{
			DbCurSeq: next,
		})
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
		if res.RowsAffected > 0 {
			*nextSeq = next
			log.Infof("alloc next seq %d for uid %d", next, uid)
			return nil
		} else {
			waitMs := rand.Intn(10) + 1
			log.Warnf("alloc conflict for uid %d, wait %d ms", uid, waitMs)
			time.Sleep(time.Duration(waitMs) * time.Millisecond)
		}
	}
	log.Warnf("alloc final fail, uid %d", uid)
	return ErrAllocSeqConflict
}

var (
	ErrGenSeqTimeout = errors.New("gen seq timeout")
)

type ActionFreq interface {
	GenKey(i interface{}) string
	Exec(i interface{}) string
}

type ActionExec struct {
	DataIn, DataOut interface{}

	createAt time.Time
	err      error
	w        *sync.WaitGroup
}

type ActionFreqExecutor struct {
	in chan *ActionExec
}

func NewActionFreqExecutor(ctx *rpc.Context, cacheMax uint, timeout time.Duration, exec func(i *ActionExec) error) *ActionFreqExecutor {
	p := &ActionFreqExecutor{
		in: make(chan *ActionExec, cacheMax),
	}

	go func() {
		for {
			select {
			case val := <-p.in:
				// 超时了就不处理了
				if timeout > 0 && time.Since(val.createAt) > timeout {
					log.Infof("err:%v", val.err)
					val.err = ErrGenSeqTimeout
				} else {
					err := exec(val)
					if err != nil {
						log.Errorf("err:%v", err)
						val.err = err
					}
				}

				val.w.Done()
			}
		}
	}()

	return p
}

func (p *ActionFreqExecutor) Enqueue(ctx *rpc.Context, val *ActionExec) error {
	select {
	case p.in <- val:
		return nil
	}
}

type ActionFreqHub struct {
	queueList []*ActionFreqExecutor
	GenKey    func(i *ActionExec) int

	wgPool sync.Pool
}

type ActionFreqHubConf struct {
	// 分配执行器的规则
	GenKey func(i *ActionExec) int

	// 执行的主体逻辑
	Exec func(i *ActionExec) error

	// 最大的执行器数量
	ExecutorMax int

	// 执行器最多允许排队中的数量
	// 默认为 1000
	CacheMax uint

	// 执行等待的超时时间，如果超过了这个时间在执行会丢弃，不触发Exec
	// 默认为 5s，<0 表示不超时
	ExecTimeout time.Duration
}

func NewActionFreqHub(ctx *rpc.Context, conf ActionFreqHubConf) *ActionFreqHub {
	p := &ActionFreqHub{
		queueList: []*ActionFreqExecutor{},

		GenKey: conf.GenKey,
	}

	p.wgPool.New = func() interface{} {
		return &sync.WaitGroup{}
	}

	if conf.GenKey == nil {
		panic("miss gen key func")
	}

	if conf.Exec == nil {
		panic("miss exec func")
	}

	execTimeout := conf.ExecTimeout
	if execTimeout == 0 {
		execTimeout = time.Second * 5
	}

	cacheMax := conf.CacheMax
	if cacheMax <= 0 {
		cacheMax = 1000
	}

	p.queueList = make([]*ActionFreqExecutor, conf.ExecutorMax, conf.ExecutorMax)
	for i := 0; i < conf.ExecutorMax; i++ {
		p.queueList[i] = NewActionFreqExecutor(ctx, cacheMax, execTimeout, conf.Exec)
	}

	return p
}

// enQueueTimeout <= 0 表示不超时
// waitTimeout 默认值为 1s
func (p *ActionFreqHub) SubmitAndWait(ctx *rpc.Context, val *ActionExec) (*ActionExec, error) {
	i := p.GenKey(val)
	queue := p.queueList[i]
	if queue == nil {
		return nil, fmt.Errorf("queue is nil %d %d", i, len(p.queueList))
	}

	val.createAt = time.Now()
	val.w = p.wgPoolGet()
	val.w.Add(1)
	err := queue.Enqueue(ctx, val)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	val.w.Wait()
	p.wgPoolPut(val.w)

	if val.err != nil {
		log.Infof("err:%v", val.err)
		return nil, val.err
	}

	return val, nil
}

func (p *ActionFreqHub) wgPoolGet() *sync.WaitGroup {
	return p.wgPool.Get().(*sync.WaitGroup)
}
func (p *ActionFreqHub) wgPoolPut(wg *sync.WaitGroup) {
	p.wgPool.Put(wg)
}

func extSensitiveWordCheck(ctx *rpc.Context, msgBox *qmsg.ModelMsgBox, chat *qmsg.ModelChat, robotAcc *qrobot.ModelAccount, mentionName string) error {
	log.Debugf("ai_msg_sen")
	if msgBox.ChatMsgType != uint32(qmsg.ChatMsgType_ChatMsgTypeText) || msgBox.Msg == "" {
		log.Debugf("not text , jump")
		return nil
	}
	var rule qmsg.ModelSensitiveWordRule
	err := SenWordRule.WhereCorpApp(chat.CorpId, chat.AppId).
		Where(DbRuleType, uint32(qmsg.ModelSensitiveWordRule_ExtUser)).
		First(ctx, &rule)
	if err != nil {
		if SenWordRule.IsNotFoundErr(err) {
			return nil
		}
		log.Errorf("err %v", ErrAllocSeqConflict)
		return err
	}

	// 未开启或没有敏感词规则，直接通过
	if !rule.Enabled || rule.Rule == nil || len(rule.Rule.WordList) == 0 {
		log.Debugf("rule not set , jump")
		return nil
	}
	var words []string
	for _, word := range rule.Rule.WordList {

		if strings.Contains(msgBox.Msg, word) {
			words = append(words, word)
		}
	}
	// 发送内容不包含任何敏感词
	if len(words) == 0 {
		log.Debugf("not hit , jump")
		return nil
	}

	var accountId, groupId uint64
	if chat.IsGroup {
		groupId = chat.ChatExtId
		accountId = msgBox.SenderAccountId
	} else {
		accountId = chat.ChatExtId
	}

	const limitLen int = 1024 // db max var chat(1024)
	content := msgBox.Msg

	if len([]rune(content)) > limitLen {
		content = string([]rune(content)[:limitLen])
	}

	record := &qmsg.ModelExtSensitiveOptRecord{
		CorpId:            chat.CorpId,
		AppId:             chat.AppId,
		RobotUid:          chat.RobotUid,
		AccountId:         accountId,
		GroupId:           groupId,
		ModelChatId:       chat.Id,
		MsgBoxId:          msgBox.Id,
		TriggerTime:       msgBox.CreatedAt,
		SensitiveWordList: words,
		Content:           content,
	}

	err = ExtSensitiveOptRecord.Create(ctx, &record)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	// 发送回复
	if rule.Rule.IsReply && rule.Rule.ReplyContext != "" {
		uuid, _ := utils.GenUUID()
		pubRsp := qmsg.MqSendChatMsg.PubOrExecInRoutine(
			ctx, chat.CorpId, chat.AppId,
			&qmsg.SendChatMsgMqReq{
				SendReq: &qmsg.SendChatMsgReq{
					RobotUid:    chat.RobotUid,
					CliMsgId:    fmt.Sprintf("ext_sensitive_%d_%s", chat.RobotUid, uuid),
					ChatType:    msgBox.ChatType,
					ChatId:      msgBox.ChatId,
					ChatMsgType: uint32(qmsg.ChatMsgType_ChatMsgTypeText),
					Msg:         rule.Rule.ReplyContext,
				},
				SendAt: utils.Now(),
				User: &qmsg.ModelUser{
					CorpId:   chat.CorpId,
					AppId:    chat.AppId,
					LoginId:  "ext_sensitive",
					Username: "ext_sensitive",
				},
			}, processSendChatMsg)
		if pubRsp != nil {
			log.Infof("pub rsp %s", pubRsp.MsgId)
		}
	}

	// 是否打开推送开关
	if rule.Rule.IsNotify {
		//content := fmt.Sprintf("成员[%s] 的客户/群聊[%s] 在%s 发送的消息命中敏感词 %v, 请留意查看", robotAcc.Name, mentionName, time.Unix(int64(msgBox.CreatedAt), 0).Format("2006-01-02 15:04"), words)
		//content := fmt.Sprintf("所属成员：【%s】\n\n客户/群聊：【%s】\n\n时间：%s\n\n命中敏感词：【%v】\n\n消息原文：%v",
		//	robotAcc.Name, mentionName, time.Unix(int64(msgBox.CreatedAt), 0).Format("2006-01-02 15:04"), words, msgBox.Msg)

		notifyContent := fmt.Sprintf("所属成员：%s\n\n客户/群聊：%s\n\n时间：%s\n\n命中敏感词：%v\n\n消息原文：%v",
			robotAcc.Name, mentionName, time.Unix(int64(msgBox.CreatedAt), 0).Format("2006-01-02 15:04"), words, msgBox.Msg)

		var uidList []uint64
		var pubList []uint64
		// 是否有推成员本身
		if rule.Rule.NotifyType == uint32(qmsg.ModelSensitiveWordRule_NotifyRobotSelf) ||
			rule.Rule.NotifyType == uint32(qmsg.ModelSensitiveWordRule_NotifyRobotAndOther) {

			uidList = append(uidList, robotAcc.Uid)
		}

		// 是否有推指定成员
		if rule.Rule.NotifyType == uint32(qmsg.ModelSensitiveWordRule_NotifyOther) ||
			rule.Rule.NotifyType == uint32(qmsg.ModelSensitiveWordRule_NotifyRobotAndOther) {
			uidList = append(uidList, rule.Rule.UidList...)
		}

		// 是否指定成员组
		if rule.Rule.NotifyType == uint32(qmsg.ModelSensitiveWordRule_NotifyMemberGroup) {
			for _, memberGroup := range rule.Rule.MemberGroupList {
				// 判断该机器人是否是成员组中的成员
				if !utils.InSliceUint64(chat.RobotUid, memberGroup.MemberUidList) {
					continue
				}
				// 如果是则将配置的接收成员列表则加入推送
				uidList = append(uidList, memberGroup.ReceiverUidList...)
			}
		}

		for _, uid := range uidList {
			// 按成员去重
			isDup, err := checkNotifyDeDupCache(notifyContent, uid)
			if err != nil {
				log.Errorf("err %v", err)
			}
			if !isDup {
				pubList = append(pubList, uid)
			}
		}

		pubList = pie.Uint64s(pubList).Unique()

		err = notifyStaff(ctx, chat.CorpId, chat.AppId, notifyContent, pubList)
		if err != nil {
			log.Errorf("err %v", err)
		}

	}

	return nil
}

func sendFakeMsg(ctx *rpc.Context, reqMsg *qmsg.ModelMsgBox) error {

	robotUid := reqMsg.Uid
	accId := reqMsg.ChatId

	qrobotRsp, err := qrobot.GetAccountByRobotUidSys(ctx, &qrobot.GetAccountByRobotUidSysReq{
		RobotUid: robotUid,
		CorpId:   reqMsg.CorpId,
		AppId:    reqMsg.AppId,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	robotAcc := qrobotRsp.Account

	accRsp, err := qrobot.GetAccountListSys(ctx, &qrobot.GetAccountListSysReq{
		ListOption: core.NewListOption().SetSkipCount().SetLimit(1).AddOpt(qrobot.GetAccountListSysReq_ListOptionIdList, []uint64{accId}),
		CorpId:     reqMsg.CorpId,
		AppId:      reqMsg.AppId,
	})
	if err != nil {
		log.Errorf("err %v", err)
		return err
	}

	if len(accRsp.List) == 0 {
		log.Warnf("acc not found :%d", accId)
		return nil
	}

	custormerAcc := accRsp.List[0]

	msgContent := "我通过了你的联系人验证请求，现在我们可以开始聊天了（sys）"
	msgId := fmt.Sprintf("become_fri_fake_msg_%d_%d", custormerAcc.Id, utils.Now())
	msg := &qrobot.RecvSingleChatMsgReq{
		RobotType:       uint32(yc.RobotType_RobotTypePlatform),
		CorpId:          robotAcc.CorpId,
		AppId:           robotAcc.AppId,
		RobotUid:        robotAcc.Uid,
		RobotSn:         robotAcc.YcSerialNo,
		ReceiverAccount: robotAcc,
		SenderAccount:   custormerAcc,
		MsgId:           msgId,
		MsgDate: &quan.ExtraMsg{
			ExtraMsgType: uint32(quan.ExtraMsgType_ExtraMsgTypeText),
			MsgText:      msgContent,
		},
		YcReq: &yc.ReceiveContactPrivateMessageReq{
			RobotSerialNo: robotAcc.YcSerialNo,
			Data: &yc.RecvMsgData{
				MsgType:          uint32(yc.MsgType_MsgTypeText),
				MsgId:            msgId,
				SenderSerialNo:   custormerAcc.YcSerialNo,
				ReceiverSerialNo: robotAcc.YcSerialNo,
				MsgContent:       msgContent,
			},
			RobotType: 0,
		},
		SentAt: utils.Now(),
	}
	ycReq := msg.YcReq
	recvData := ycReq.Data
	sender := msg.SenderAccount

	chatId := sender.Id

	log.Debugf("send at:%d", msg.SentAt)

	mentionName := fmt.Sprintf("[%s]", sender.Name)

	err = handleReceiveMsg(ctx, robotAcc.CorpId, robotAcc.AppId, robotAcc.Uid, chatId, custormerAcc, msg.RobotSn, msg.MsgDate, recvData, msg.SentAt, qmsg.ChatType_ChatTypeSingle, mentionName, false)
	if err != nil {
		if rpc.GetErrCode(err) == rpc.InvalidArgErrCode {
			return nil
		}
		log.Errorf("err:%v", err)
		return err
	}
	return nil
}
