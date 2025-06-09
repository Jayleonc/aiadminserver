package impl

import (
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/brick/utils"
	"git.pinquest.cn/qlb/qmsg"
	"git.pinquest.cn/qlb/qsm"
	"git.pinquest.cn/qlb/yc"
)

func AddGroupNoticeToMsgBoxSys(ctx *rpc.Context, req *qmsg.AddGroupNoticeToMsgBoxSysReq) (*qmsg.AddGroupNoticeToMsgBoxSysRsp, error) {
	var rsp qmsg.AddGroupNoticeToMsgBoxSysRsp
	var msgList []*yc.SendMessagesData
	msgList = append(msgList, &yc.SendMessagesData{
		MsgNum:        0,
		MsgType:       uint32(yc.MsgType_MsgTypeText),
		MsgContent:    req.Notice,
		IsGroupNotice: true,
	})
	_, err := YcGroupSendCallbackV2(ctx, &qsm.SendMessageCallbackReq{
		MsgId:   "group_notice_" + utils.GenRandomStr(),
		MsgNum:  0,
		ErrCode: 0,
		ErrMsg:  "",
		RobotSn: req.RobotSerialNo,
		BizContext: &qsm.BizContext{
			ModuleName: "qrobot_group_notice",
			Context:    req.StrGroupId,
			CorpId:     req.CorpId,
			AppId:      req.AppId,
			YcReq: &qsm.YcReq{
				GroupReq: &yc.SendMessagesToGroupReq{
					MerchantNo:    req.MerchantId,
					Data:          msgList,
					RobotSerialNo: req.RobotSerialNo,
					GroupSerialNo: req.StrGroupId,
				},
			},
			YcRsp: &qsm.YcRsp{
				MsgIndex: 0,
				GroupRsp: &yc.SendMessagesToGroupRsp{
					Data: &yc.SendMessagesToGroupRsp_Data{
						RelaSerialNo:    "",
						MsgNum:          0,
						ContactSerialNo: "",
						MsgId:           utils.GenRandomStr(),
						GroupSerialNo:   "",
					},
				},
			},
		},
	})
	if err != nil {
		log.Errorf("err:%v", err)
	}
	return &rsp, nil
}
