package impl

import (
	"fmt"
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/extrpkg/github.com/go-redis/redis"
	"git.pinquest.cn/qlb/qmsg"
)

func getWsAndOnlineCount(ctx *rpc.Context, corpId, appId uint32) (uint32, error) {
	var userList []*qmsg.ModelUser
	err := User.WhereCorpApp(corpId, appId).Where(DbIsOnline, true).Where(DbIsDisable, false).Find(ctx, &userList)
	if err != nil {
		log.Errorf("err:%v", err)
		return 0, err
	}

	if len(userList) == 0 {
		return 0, nil
	}

	redisKey := genWsConnListRedisKey(corpId, appId)

	var result uint32

	for _, user := range userList {
		list, err := getWsConnList(corpId, appId, user.Id)
		if err != nil {
			log.Errorf("err:%v", err)
			return 0, err
		}

		log.Infof("list %+v", list.List)
		if err != nil {
			log.Errorf("err:%v", err)
		}
		var res []*WsConn
		err = s.RedisGroup.HGetJson(redisKey, fmt.Sprintf("%d", user.Id), &res)
		log.Infof("test %+v", res)
		if err != nil {
			if err == redis.Nil {
				continue
			} else {
				log.Errorf("err:%v", err)
				return 0, err
			}
		}

		if len(res) > 0 {
			result++
		}
	}

	return result, nil
}
