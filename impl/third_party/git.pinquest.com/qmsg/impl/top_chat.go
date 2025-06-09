package impl

import (
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/brick/utils"
	"git.pinquest.cn/qlb/core"
	"git.pinquest.cn/qlb/qmsg"
)

func AddTopChat(ctx *rpc.Context, req *qmsg.AddTopChatReq) (*qmsg.AddTopChatRsp, error) {
	var rsp qmsg.AddTopChatRsp

	user, err := getUserCheck(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	m := req.TopChat
	m.Id = 0
	m.CorpId = user.CorpId
	m.AppId = user.AppId
	m.Uid = user.Id

	err = TopChat.Create(ctx, m)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	rsp.TopChat = m
	return &rsp, nil
}

func GetTopChatList(ctx *rpc.Context, req *qmsg.GetTopChatListReq) (*qmsg.GetTopChatListRsp, error) {
	var rsp qmsg.GetTopChatListRsp

	user, err := getUserCheck(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	db := TopChat.NewList(req.ListOption).WhereCorpApp(user.CorpId, user.AppId).Where(DbUid, user.Id)

	err = core.NewListOptionProcessor(req.ListOption).
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

	chatIdList := utils.PluckUint64(rsp.List, "Cid")

	if len(chatIdList) > 0 {
		var chatList []*qmsg.ModelChat
		err = Chat.WhereCorpApp(user.CorpId, user.AppId).WhereIn(DbId, chatIdList).Find(ctx, &chatList)
		if err != nil {
			log.Errorf("err %v", err)
			return nil, err
		}

		// 有资产才执行移除非本客服下的机器人会话
		if user.IsAssign {
			// 查询当前客服分配的机器人uid
			var robotList []*qmsg.ModelUserRobot
			err = UserRobot.Where(map[string]interface{}{
				DbCorpId: user.CorpId,
				DbAppId:  user.AppId,
				DbUid:    user.Id,
			}).Find(ctx, &robotList)
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, err
			}

			var uidListMap map[uint64]*qmsg.ModelUserRobot
			utils.KeyBy(robotList, "RobotUid", &uidListMap)
			// 移除掉已经删除的机器人
			for _, chat := range chatList {
				if _, ok := uidListMap[chat.RobotUid]; !ok {
					// 移除掉这个置顶会话
					_, err = TopChat.deleteByCid(ctx, user.CorpId, user.AppId, user.Id, chat.Id)
					if err != nil {
						log.Errorf("err:%v", err)
						return nil, err
					}
					continue
				}
				rsp.ChatList = append(rsp.ChatList, chat)
			}
		} else {
			rsp.ChatList = chatList
		}

	}

	return &rsp, nil
}

func DelTopChat(ctx *rpc.Context, req *qmsg.DelTopChatReq) (*qmsg.DelTopChatRsp, error) {
	var rsp qmsg.DelTopChatRsp

	user, err := getUserCheck(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	_, err = TopChat.deleteByCid(ctx, user.CorpId, user.AppId, user.Id, req.Cid)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	return &rsp, nil
}
