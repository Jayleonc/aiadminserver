package impl

import (
	"errors"
	"fmt"
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/qrt"
	"time"
)

var Frequency *qrt.Frequency

func InitFrequency() error {
	Frequency = qrt.NewFreq(s.RedisGroup)
	return nil
}

func FreqLimit(ctx *rpc.Context, key string, validSecond uint32) error {
	du, err := time.ParseDuration(fmt.Sprintf("%ds", validSecond))
	if err != nil {
		log.Errorf("err: %v", err)
		return err
	}
	expiredAt := time.Now().Add(du)
	_, isValid := Frequency.Check(key, 1, expiredAt)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	if !isValid {
		log.Warnf("超出频率限制")
		return errors.New(fmt.Sprintf("接口%d秒内只能调用一次", validSecond))
	}
	return nil
}
