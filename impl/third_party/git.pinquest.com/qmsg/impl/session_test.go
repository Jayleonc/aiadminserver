package impl

import (
	"git.pinquest.cn/base/log"
	"testing"
)

func TestSession(t *testing.T) {
	err1 := InitConfig()
	if err1 != nil {
		log.Errorf("err:%v", err1)
		return
	}
	err2 := InitState()
	if err2 != nil {
		log.Errorf("err:%v", err2)
		return
	}
	s, err := getSessionConfig(nil, 2000772, 2000273)
	if err != nil {
		log.Errorf("err:%v", err)
		return
	}

	log.Printf("%+v", s)
}
