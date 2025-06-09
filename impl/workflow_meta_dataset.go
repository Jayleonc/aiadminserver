package impl

import (
	"git.pinquest.cn/base/aiadmin"
	"git.pinquest.cn/qlb/brick/rpc"
)

func GetWorkflowDatasets(ctx *rpc.Context, req *aiadmin.GetWorkflowDatasetsReq) (*aiadmin.GetWorkflowDatasetsRsp, error) {
	var rsp aiadmin.GetWorkflowDatasetsRsp
	return &rsp, nil
}
