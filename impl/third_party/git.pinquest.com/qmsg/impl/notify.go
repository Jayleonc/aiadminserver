package impl

import (
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/quan"
	"git.pinquest.cn/qlb/wework"
)

const notifyMsgIdPrefix = "qmsg_notify"

func sendNotifyWithMsgId(ctx *rpc.Context, corpId, appId uint32, content string, msgId string, user *quan.ModelUser) {
	if !user.InAppScope {
		return
	}

	wUserId := user.WwUserId

	appIdent := &wework.AppIdent{
		AuthType: uint32(wework.AppIdent_AuthTypeNonAuth),
		CorpId:   corpId,
		AppId:    appId,
	}
	_, err := wework.NewMessage(appIdent).
		AppendText(wework.NewText().
			AppendText(content)).
		SendText2Touser(ctx, corpId, appId, wUserId, msgId)
	if err != nil {
		log.Errorf("err:%v", err)
		//warning.ReportMsg(ctx, fmt.Sprintf("send notify err:%v", err))
	} else {
		log.Infof("send to %s ret %+v", wUserId, content)
	}
}
