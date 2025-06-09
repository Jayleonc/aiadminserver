package impl

import (
	"fmt"
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/json"
	"git.pinquest.cn/qlb/core"
	"git.pinquest.cn/qlb/qmsg"
	"reflect"
	"testing"
)

func TestBatchCloseChat(t *testing.T) {
	//SendChatMsg(nil, &qmsg.SendChatMsgReq{
	//	RobotUid:             0,
	//	CliMsgId:             "",
	//	ChatType:             0,
	//	ChatId:               0,
	//	ChatMsgType:          0,
	//	Msg:                  "",
	//	Href:                 "",
	//	Title:                "",
	//	Desc:                 "",
	//	VoiceTime:            0,
	//	AtList:               nil,
	//	MiniProgramCoverUrl:  "",
	//	MiniProgramName:      "",
	//	MiniProgramPath:      "",
	//	MiniProgramTitle:     "",
	//	SosObjectId:          "",
	//	MaterialId:           0,
	//	AssignChatId:         0,
	//	ChannelMsgTitle:      "",
	//	ChannelMsgCoverUrl:   "",
	//	ChannelMsgHref:       "",
	//	MsgSerialNo:          "",
	//	ChannelMsgName:       "",
	//	ChannelMsgIcon:       "",
	//})
	chatList := []*qmsg.ModelChat{
		{
			Id:          1,
			IsGroup:     true,
			UnreadCount: 0,
		},
		{
			Id:          2,
			IsGroup:     false,
			UnreadCount: 1,
		},
	}

	chatList = qmsg.ChatList(chatList).Filter(func(chat *qmsg.ModelChat) bool {
		return chat.UnreadCount == 0
	})

	log.Infof("%+v", chatList)
}

func TestRedis(t *testing.T) {
	InitTestEnv()
	ctx := core.NewInternalRpcCtx(2000772, 2000273, 0)
	err := updateUserSendRedis(ctx, 2000772, 2000273, 2, 2)
	if err != nil {
		log.Errorf("err:%v", err)
		return
	}

	err = updateUserSendRedis(ctx, 2000772, 2000273, 2, 2)
	if err != nil {
		log.Errorf("err:%v", err)
		return
	}

	list, err := getSendRedisList(fmt.Sprintf(UserLastSend, 1), 2, 1, 100)
	if err != nil {
		log.Errorf("err:%v", err)
		return
	}

	fmt.Println(list)
}

func TestRawSql(t *testing.T) {
	InitTestEnv()
	corpId, appId := 2000772, 2000273
	cidList := []uint64{16348, 16414, 1031}
	ctx := core.NewInternalRpcCtx(2000772, 2000273, 0)
	log.Infof(ctx.GetReqId())
	var lastAssignChatList []*qmsg.ModelAssignChat
	err := Db().FilterCorpAndApp(2000772, 2000273).Unscoped().QueryBySql(ctx, &lastAssignChatList, fmt.Sprintf("SELECT * FROM qmsg_assign_chat AS a RIGHT JOIN (SELECT MAX(created_at), cid FROM qmsg_assign_chat WHERE (`corp_id` = ?) AND (`app_id` = ?) AND cid IN (?) AND uid != 0 GROUP BY cid) AS b ON a.cid = b.cid AND (a.`corp_id` = ?) AND (a.`app_id` = ?) AND a.cid IN (?) AND a.uid != 0 GROUP BY a.cid ORDER BY NULL"), corpId, appId, cidList, corpId, appId, cidList).Error()
	if err != nil {
		log.Errorf("err:%v", err)
		return
	}
}

func TestDecode(t *testing.T) {
	ctx := core.NewInternalRpcCtx(2000772, 2000273, 0)
	log.Infof(ctx.GetReqId())
	var out interface{}
	out = &[]*qmsg.ModelAssignChat{}
	v := reflect.ValueOf(out)
	if v.Type().Kind() != reflect.Ptr {
		log.Infof("1")
		return
	}
	v = v.Elem()
	if v.Type().Kind() != reflect.Slice {
		log.Infof("2")
		return
	}
	// 取一下成完类型
	el := v.Type().Elem()
	if el.Kind() != reflect.Ptr {
		log.Infof("3")
		return
	}
	// 判断下是否 protobuf
	//if !el.Implements(reflect.TypeOf((*proto.Message)(nil)).Elem()) {
	//	return errors.New("element not protobuf message")
	//}
	log.Infof("ttt")
	text := `[{"MAX(created_at)":"1633752503","app_id":"2000273","chat_ext_id":"24083","cid":"16348","corp_id":"2000772","created_at":"1626344352","deleted_at":"1626353347","end_scene":"0","id":"16342","is_group":true,"origin_uid":"0","robot_uid":"231","start_scene":"0","uid":"40","updated_at":"1626353337"},{"MAX(created_at)":"1633752504","app_id":"2000273","chat_ext_id":"24114","cid":"16414","corp_id":"2000772","created_at":"1626969699","deleted_at":"1627027880","end_scene":"0","id":"18698","is_group":true,"origin_uid":"0","robot_uid":"231","start_scene":"0","uid":"22","updated_at":"1627027660"}]`
	if text != "" {
		var out = reflect.New(v.Type())
		err := json.Unmarshal([]byte(text), out.Interface())
		if err != nil {
			log.Errorf("err %v", err)
			return
		}
		v.Set(out.Elem())
	}
	log.Infof("%+v", out)
}
