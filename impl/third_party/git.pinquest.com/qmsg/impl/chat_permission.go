package impl

import (
	"fmt"
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/brick/utils"
	"git.pinquest.cn/qlb/extrpkg/github.com/go-redis/redis"
	"git.pinquest.cn/qlb/featswitch"
	"git.pinquest.cn/qlb/qmsg"
	"strconv"
	"time"
)

func genBeSendKey(userId, robotUid, extAccountId uint64) string {
	return fmt.Sprintf("qmsg_be_send_expire_%d_%d_%d", userId, robotUid, extAccountId)
}

func getTodayDate() uint32 {
	today64, _ := strconv.ParseUint(time.Now().Format("20060102"), 10, 64)
	return uint32(today64)
}

func genSendKey(userId uint64) string {
	return fmt.Sprintf("qmsg_send_expire_%d_%d", userId, getTodayDate())
}

func checkChatPermission(ctx *rpc.Context, user *qmsg.ModelUser, robotUid, extAccountId uint64) error {

	//加开关，避免所有消息都进来怼一遍数据
	enable, err := featswitch.GetFeatureFlagEnabled(ctx, &featswitch.GetFeatureFlagEnabledReq{
		Key:    "QMSG_CHAT_PERMISSION_CHECK",
		CorpId: user.CorpId,
		AppId:  user.AppId,
	})
	if err != nil {
		log.Errorf("err %v", err)
		return err
	}

	//未打开开关不处理
	if !enable.Enabled {
		return nil
	}

	var info qmsg.ModelUserCategory
	err = UserCategory.WhereCorpApp(user.CorpId, user.AppId).Where(DbId, user.UserCategoryId).First(ctx, &info)
	if err != nil {
		if UserCategory.IsNotFoundErr(err) {
			return nil
		}

		log.Errorf("err %v", err)
		return err
	}

	if info.PermissionData == nil || info.PermissionData.ChatPermission == nil {
		return nil
	}

	chatPermission := info.PermissionData.ChatPermission
	//无限制 不计数，直接返回可用
	if !chatPermission.IsSetSendLimit {
		return nil
	}

	//检查是否 接到客户消息后发消息的额度
	beSendKey := genBeSendKey(user.Id, robotUid, extAccountId)
	valByte, err := s.RedisGroup.Get(beSendKey)
	if err != nil {
		if err != redis.Nil {
			log.Errorf("err %v", err)
			return err
		}
	} else {
		beSendUsableNum := uint64(9999999)
		beSendUsableNum, _ = strconv.ParseUint(string(valByte), 10, 64)

		//有可用额度，累计额度并返回
		if uint32(beSendUsableNum) < chatPermission.BeSendMsgNum {
			_, err = s.RedisGroup.IncrBy(beSendKey, 1)
			if err != nil {
				log.Errorf("err %v", err)
				return err
			}
			return nil
		}
	}

	//检查机器人主动发送额度

	sendKey := genSendKey(user.Id)

	redisMember := fmt.Sprintf("%d", extAccountId)

	score, err := s.RedisGroup.ZScore(sendKey, redisMember)
	if err != nil && err != redis.Nil {
		log.Errorf("err %v", err)
		return err
	}

	//没有找到记录，检查是否还有发送名额
	if err == redis.Nil {
		var cnt int64
		cnt, err = s.RedisGroup.ZCard(sendKey)
		if err != nil && err != redis.Nil {
			log.Errorf("err %v", err)
			return err
		}

		if uint32(cnt) >= chatPermission.ExtAccountNum {
			return rpc.CreateError(qmsg.ErrUserSendLimit)
		}

	}

	if uint32(score) >= chatPermission.SendMsgNum {
		return rpc.CreateError(qmsg.ErrUserSendLimit)
	}

	err = s.RedisGroup.ZIncrBy(sendKey, 1, redisMember)
	if err != nil && err != redis.Nil {
		log.Errorf("err %v", err)
		return err
	}

	//粗暴的刷新过期时间
	_ = s.RedisGroup.Expire(sendKey, 24*time.Hour)

	return nil
}

func checkChatPermission4Receive(ctx *rpc.Context, corpId, appId uint32, robotUid, extAccountId uint64) {

	//加开关，避免所有消息都进来怼一遍数据
	enable, err := featswitch.GetFeatureFlagEnabled(ctx, &featswitch.GetFeatureFlagEnabledReq{
		Key:    "QMSG_CHAT_PERMISSION_CHECK",
		CorpId: corpId,
		AppId:  appId,
	})
	if err != nil {
		log.Errorf("err %v", err)
		return
	}

	//未打开开关不处理
	if !enable.Enabled {
		return
	}

	var userRelaList []*qmsg.ModelUserRobot
	err = UserRobot.WhereCorpApp(corpId, appId).Where(DbRobotUid, robotUid).Find(ctx, &userRelaList)
	if err != nil {
		log.Errorf("err %v", err)
		return
	}

	if len(userRelaList) == 0 {
		return
	}

	userIdList := utils.PluckUint64(userRelaList, "Uid")

	var userList []*qmsg.ModelUser
	err = User.WhereCorpApp(corpId, appId).WhereIn(DbId, userIdList).Find(ctx, &userList)
	if err != nil {
		log.Errorf("err %v", err)
		return
	}

	if len(userList) == 0 {
		return
	}

	categoryIdList := utils.PluckUint64(userList, "UserCategoryId")

	userMap := make(map[uint64][]*qmsg.ModelUser)

	//同一个分组的客服塞一起
	for _, v := range userList {
		userMap[v.UserCategoryId] = append(userMap[v.UserCategoryId], v)
	}

	var cList []*qmsg.ModelUserCategory
	err = UserCategory.WhereCorpApp(corpId, appId).WhereIn(DbId, categoryIdList).Find(ctx, &cList)
	if err != nil {
		log.Errorf("err %v", err)
		return
	}

	for _, v := range cList {
		if v.PermissionData == nil || v.PermissionData.ChatPermission == nil || v.PermissionData.ChatPermission.IsSetSendLimit == false {
			continue
		}

		uList, ok := userMap[v.Id]
		if !ok {
			continue
		}

		for _, user := range uList {
			//重置客户发消息后可发送数量
			key := genBeSendKey(user.Id, robotUid, extAccountId)
			err = s.RedisGroup.Set(key, []byte(fmt.Sprintf("%d", 0)), time.Duration(v.PermissionData.ChatPermission.BeSendExpireHour)*time.Hour)
			if err != nil {
				log.Errorf("err %v", err)
				continue
			}
		}
	}
}
