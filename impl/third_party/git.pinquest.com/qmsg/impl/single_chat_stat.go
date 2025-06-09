package impl

import (
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/qmsg"
)

func GetSingleChatStat(ctx *rpc.Context, req *qmsg.GetSingleChatStatReq) (*qmsg.GetSingleChatStatRsp, error) {
	var rsp qmsg.GetSingleChatStatRsp
	return &rsp, nil
}

func GetServiceSingleChatStat(ctx *rpc.Context, req *qmsg.GetServiceSingleChatStatReq) (*qmsg.GetServiceSingleChatStatRsp, error) {
	var rsp qmsg.GetServiceSingleChatStatRsp
	return &rsp, nil
}

func GetRobotSingleChatStat(ctx *rpc.Context, req *qmsg.GetRobotSingleChatStatReq) (*qmsg.GetRobotSingleChatStatRsp, error) {
	var rsp qmsg.GetRobotSingleChatStatRsp
	return &rsp, nil
}

func ExportServiceSingleChatStat(ctx *rpc.Context, req *qmsg.ExportServiceSingleChatStatReq) (*qmsg.ExportServiceSingleChatStatRsp, error) {
	var rsp qmsg.ExportServiceSingleChatStatRsp
	return &rsp, nil
}

func ExportRobotSingleChatStat(ctx *rpc.Context, req *qmsg.ExportRobotSingleChatStatReq) (*qmsg.ExportRobotSingleChatStatRsp, error) {
	var rsp qmsg.ExportRobotSingleChatStatRsp
	return &rsp, nil
}
