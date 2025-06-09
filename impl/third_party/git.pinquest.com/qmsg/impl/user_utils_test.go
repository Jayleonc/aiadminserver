package impl

import (
	"encoding/base64"
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/utils"
	"testing"
)

func TestGetWsAndOnlineCount(t *testing.T) {
	s = NewState()

	err := InitStateRedis()
	if err != nil {
		return
	}

	var corpId uint32 = 2000772
	var appId uint32 = 2000273
	list, err := getWsConnList(corpId, appId, 22221)
	if err != nil {
		log.Errorf("err:%v", err)
		return
	}
	log.Infof("list %+v", list.List)
}

func TestReadImage2Base64(t *testing.T) {
	f, err := utils.FileRead("C:\\Users\\EDY\\Desktop\\images\\14984.mp3")
	if err != nil {
		log.Errorf("err:%v", err)
		return
	}
	b := []byte(f)
	base64Rst := base64.StdEncoding.EncodeToString(b)
	println("base64 result: [", base64Rst, "]")
}
