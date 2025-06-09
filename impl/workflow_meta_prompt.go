package impl

import (
	"git.pinquest.cn/base/aiadmin"
	"git.pinquest.cn/qlb/brick/rpc"
)

func GetPromptTemplates(ctx *rpc.Context, req *aiadmin.GetPromptTemplatesReq) (*aiadmin.GetPromptTemplatesRsp, error) {
	var rsp aiadmin.GetPromptTemplatesRsp
	return &rsp, nil
}
