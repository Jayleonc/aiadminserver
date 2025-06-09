package impl

import (
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/core"
	"git.pinquest.cn/qlb/qmsg"
	"git.pinquest.cn/qlb/qrobot"
)

func GetGroupTagList(ctx *rpc.Context, req *qmsg.GetGroupTagListReq) (*qmsg.GetGroupTagListRsp, error) {
	var rsp qmsg.GetGroupTagListRsp
	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)

	groupTagListRsp, err := qrobot.GetGroupTagListSys(ctx, &qrobot.GetGroupTagListSysReq{
		ListOption: core.NewListOption(),
		CorpId:     corpId,
		AppId:      appId,
	})
	if err != nil {
		log.Errorf("err :%v", err)
		return nil, err
	}
	if len(groupTagListRsp.List) > 0 {

		for _, tag := range groupTagListRsp.List {

			groupTag := &qmsg.GetGroupTagListRsp_GroupTag{
				Id:   tag.Id,
				Name: tag.Name,
			}
			rsp.List = append(rsp.List, groupTag)
		}
	}
	return &rsp, nil
}

func GetGroupCategoryList(ctx *rpc.Context, req *qmsg.GetGroupCategoryListReq) (*qmsg.GetGroupCategoryListRsp, error) {
	var rsp qmsg.GetGroupCategoryListRsp

	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)

	groupCategoryListRsp, err := qrobot.GetGroupCategoryListSys(ctx, &qrobot.GetGroupCategoryListSysReq{
		ListOption: core.NewListOption(),
		CorpId:     corpId,
		AppId:      appId,
	})
	if err != nil {
		log.Errorf("err :%v", err)
		return nil, err
	}
	if len(groupCategoryListRsp.List) > 0 {
		for _, category := range groupCategoryListRsp.List {
			groupCategory := &qmsg.GetGroupCategoryListRsp_GroupCategory{
				Id:   category.Id,
				Name: category.Name,
			}
			rsp.List = append(rsp.List, groupCategory)

		}

	}
	return &rsp, nil
}
