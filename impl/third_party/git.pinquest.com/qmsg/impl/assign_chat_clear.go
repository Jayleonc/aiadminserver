package impl

import (
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/core"
	"git.pinquest.cn/qlb/iquan"
	"git.pinquest.cn/qlb/qmsg"
	"time"
)

func ClearInvalidAssignChat(ctx *rpc.Context, startId uint64, corpId, limit uint32) error {
	for {
		log.Infof("clear batch: startId:%d,corpId:%d,limit:%d", startId, corpId, limit)
		assignChatList, err := getWaitingAssignChat(ctx, startId, corpId, limit)
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
		if len(assignChatList) == 0 {
			break
		}
		for _, assignChat := range assignChatList {
			err = checkAndCloseInvalidAssignChat(ctx, assignChat)
			if err != nil {
				log.Errorf("err:%v", err)
				return err
			}
		}
		startId = assignChatList[len(assignChatList)-1].Id
	}
	log.Infof("clear end.")
	return nil
}

func getWaitingAssignChat(ctx *rpc.Context, startId uint64, corpId, limit uint32) ([]*qmsg.ModelAssignChat, error) {
	var assignChatList []*qmsg.ModelAssignChat
	db := AssignChat.Where(DbId, ">", startId).Where(DbUid, 0)
	if corpId > 0 {
		db.Where(DbCorpId, corpId)
	}
	_, err := db.OrderAsc(DbId).SetLimit(limit).FindPaginate(ctx, &assignChatList)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	return assignChatList, nil
}

func checkAndCloseInvalidAssignChat(ctx *rpc.Context, assignChat *qmsg.ModelAssignChat) error {
	isRobot, err := checkRobot(ctx, assignChat.CorpId, assignChat.AppId, assignChat.RobotUid)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	// 机器人信息不存在，表示已注销
	if !isRobot {
		_, err = AssignChat.WhereCorpApp(assignChat.CorpId, assignChat.AppId).Where(DbId, assignChat.Id).Update(ctx, map[string]interface{}{
			DbDeletedAt: uint32(time.Now().Unix()),
			DbEndScene:  uint32(qmsg.ModelAssignChat_EndSceneUserExit),
		})
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
		log.Infof("close assign chat:%d", assignChat.Id)
	}
	return nil
}
func checkRobot(ctx *rpc.Context, corpId, appId uint32, uid uint64) (bool, error) {
	isQuanUser, err := checkIsQuanUser(ctx, corpId, appId, uid)
	if err != nil {
		log.Errorf("err:%v", err)
		return false, err
	}
	if !isQuanUser {
		return false, nil
	}
	isRobot, err := checkIsHostAccount(ctx, corpId, appId, uid)
	if err != nil {
		log.Errorf("err:%v", err)
		return isRobot, err
	}
	if !isRobot {
		isRobot, err = checkIsQrobot(ctx, corpId, appId, uid)
		if err != nil {
			log.Errorf("err:%v", err)
			return isRobot, err
		}
	}
	return isRobot, nil
}

func checkIsQuanUser(ctx *rpc.Context, corpId, appId uint32, uid uint64) (bool, error) {
	listRsp, err := iquan.GetUserList(ctx, &iquan.GetUserListReq{
		ListOption: core.NewListOption().AddOpt(iquan.GetUserListReq_ListOptionUidList, []uint64{uid}),
		CorpId:     corpId,
		AppId:      appId,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return false, err
	}
	if listRsp == nil || len(listRsp.List) == 0 {
		return false, nil
	}
	return true, nil
}

func checkIsHostAccount(ctx *rpc.Context, corpId, appId uint32, uid uint64) (bool, error) {
	qwhaleSrv := qwhaleService{}
	hostAccount, err := qwhaleSrv.getHostAccountByUid(ctx, corpId, appId, uid)
	if err != nil {
		log.Errorf("err:%v", err)
		return false, err
	}
	return hostAccount != nil, nil
}

func checkIsQrobot(ctx *rpc.Context, corpId, appId uint32, uid uint64) (bool, error) {
	qrobotSrv := qrobotService{}
	robot, err := qrobotSrv.getQRobotByUid(ctx, corpId, appId, uid)
	if err != nil {
		log.Errorf("err:%v", err)
		return false, err
	}
	return robot != nil, nil
}
