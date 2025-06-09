package main

import (
	"git.pinquest.cn/base/aiadmin"
	"git.pinquest.cn/base/aiadminserver/impl"
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/rpc"
	"os"
)

var BuildDate string

func init() {
	rpc.BuildDate = BuildDate
}

func main() {
	rpc.Version = "1.0.0"

	aiadmin.RegisterError()
	err := impl.InitConfig()
	if err == nil {
		err = impl.InitState()
		if err == nil {
			err = rpc.ServerRun(aiadmin.SvrName, CmdMap)
		}
	}

	if err != nil {
		log.Error(err)
		os.Exit(1)
	}
}
