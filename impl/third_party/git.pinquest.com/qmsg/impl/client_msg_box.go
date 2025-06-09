package impl

import (
	"fmt"
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/json"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/core"
	"git.pinquest.cn/qlb/qmsg"
)

func GetMsgBoxListUser(ctx *rpc.Context, req *qmsg.GetMsgBoxListUserReq) (*qmsg.GetMsgBoxListUserRsp, error) {
	var rsp qmsg.GetMsgBoxListUserRsp

	user, err := getUserCheck(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	db := ChoiceMsgBoxDb(MsgBox.NewList(req.ListOption).WhereCorpApp(user.CorpId, user.AppId).Where(map[string]interface{}{
		DbUid:      req.RobotUid,
		DbChatId:   req.ChatExtId,
		DbChatType: req.ChatType,
	}), user.CorpId)

	err = core.NewListOptionProcessor(req.ListOption).
		AddUint64(
			qmsg.GetMsgBoxListUserReq_ListOptionLessThanMsgSeq,
			func(val uint64) error {
				db.Where(DbMsgSeq, "<", val)
				return nil
			}).
		AddUint32(
			qmsg.GetMsgBoxListUserReq_ListOptionOrderBy,
			func(val uint32) error {
				if val == uint32(qmsg.GetMsgBoxListUserReq_OrderByMsgSeqDesc) {
					db.OrderDesc(DbMsgSeq)
				}
				return nil
			}).
		Process()

	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	rsp.Paginate, err = db.FindPaginate(ctx, &rsp.List)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	// 处理转介备注消息
	for i, item := range rsp.List {
		if item.MsgType == uint32(qmsg.MsgType_MsgTypeTransferChatRemark) {
			newMsg := *item
			newMsg.MsgType = uint32(qmsg.MsgType_MsgTypeSysNotify)
			newMsg.ChatMsgType = uint32(qmsg.ChatMsgType_ChatMsgTypeSystem)
			var msgContent qmsg.ChatTransferChatRemarkMsg
			err = json.Unmarshal([]byte(item.Msg), &msgContent)
			if err != nil || msgContent.Mark == "" {
				log.Errorf("err:%v", err)
				continue
			}

			if user.IsAssign {
				switch msgContent.TransferType {
				case uint32(qmsg.ChatTransferChatRemarkMsg_TransferTypeEnd):
					newMsg.Msg = fmt.Sprintf("%s %s由于%s已结束会话，现在可正常沟通", msgContent.Date, msgContent.TransferUsername, msgContent.Mark)
				case uint32(qmsg.ChatTransferChatRemarkMsg_TransferTypeAsset), uint32(qmsg.ChatTransferChatRemarkMsg_TransferTypeSystem):
					newMsg.Msg = fmt.Sprintf("%s 由于%s转给%s，目前无法操作会话", msgContent.Date, msgContent.Mark, msgContent.TransferUsername)
				default:
					continue
				}
			} else {
				switch msgContent.TransferType {
				case uint32(qmsg.ChatTransferChatRemarkMsg_TransferTypeNoAsset):
					transferUsername := msgContent.TransferUsername
					if msgContent.TransferUserId == fmt.Sprintf("%d", user.Id) {
						transferUsername = "你"
					}
					newMsg.Msg = fmt.Sprintf("%s %s由于%s，已将会话转介给%s", msgContent.Date, msgContent.Username, msgContent.Mark, transferUsername)
				case uint32(qmsg.ChatTransferChatRemarkMsg_TransferTypeAsset), uint32(qmsg.ChatTransferChatRemarkMsg_TransferTypeSystem):
					if msgContent.TransferUserId == fmt.Sprintf("%d", user.Id) {
						newMsg.Msg = fmt.Sprintf("%s %s由于%s，将会话转介给你", msgContent.Date, msgContent.Username, msgContent.Mark)
					} else {
						continue
					}
				default:
					continue
				}
			}
			rsp.List[i] = &newMsg
		}
	}
	return &rsp, nil
}
