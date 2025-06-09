package impl

import (
	"errors"
	"git.pinquest.cn/base/aiadmin"
	"git.pinquest.cn/qlb/brick/rpc"
)

func GetWorkflowNodeTypes(ctx *rpc.Context, req *aiadmin.GetWorkflowNodeTypesReq) (*aiadmin.GetWorkflowNodeTypesRsp, error) {
	var rsp aiadmin.GetWorkflowNodeTypesRsp
	return &rsp, errors.New("not implement")
}
