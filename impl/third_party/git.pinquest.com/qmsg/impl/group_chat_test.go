package impl

import (
	"git.pinquest.cn/qlb/core"
	"git.pinquest.cn/qlb/qmsg"
	"testing"
)

func TestGetGroupChatList(t *testing.T) {

	var corpId = uint32(2000772)
	var appId = uint32(2000273)
	ctx := core.NewInternalRpcCtx(corpId, appId, 52221)
	op := core.NewListOption()
	req := qmsg.GetGroupChatListReq{
		ListOption: op,
		RobotUid:   25317,
	}

	//群id
	//op.AddOpt(qmsg.GetGroupChatListReq_ListOptionIdList,25762)
	////群开关
	//op.AddOpt(qmsg.GetGroupChatListReq_ListOptionIsWatch,true)
	////群id
	//op.AddOpt(qmsg.GetGroupChatListReq_ListOptionIdList,0)
	////群分组id
	//op.AddOpt(qmsg.GetGroupChatListReq_ListOptionGroupCategoryId,0)
	////群标签id
	//op.AddOpt(qmsg.GetGroupChatListReq_ListOptionGroupTagId,0)
	//创建时间
	op.AddOpt(qmsg.GetGroupChatListReq_ListOptionGroupCreatedAt, "1619060939,1619060939")
	////群名字
	//op.AddOpt(qmsg.GetGroupChatListReq_ListOptionGroupName,"cyx")
	//机器人类型
	op.AddOpt(qmsg.GetGroupChatListReq_ListOptionAppType, 4)
	got, err := GetGroupChatList(ctx, &req)
	if err != nil {
		t.Logf("err :%v", err)
	}
	t.Logf("rsp :%v", got)

}
