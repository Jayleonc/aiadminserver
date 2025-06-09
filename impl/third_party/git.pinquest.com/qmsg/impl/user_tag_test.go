package impl

import (
	"fmt"
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/core"
	"git.pinquest.cn/qlb/qmsg"
	"git.pinquest.cn/qlb/quan"
	"testing"
)

func TestUserTag(t *testing.T) {
	corpId := uint32(2000772)
	appId := uint32(2000273)
	ctx := core.NewInternalRpcCtx(corpId, appId, 0)
	log.Infof("reqid %s", ctx.GetReqId())

	userId := uint64(64229)

	core.SetCorpAndApp(ctx, 2000772, 2000273)
	core.SetPinHeaderUId(ctx, userId)
	ctx.SetSpecHttpReqHeader(core.HeaderUid, userId)

	rsp, err := qmsg.ExtContactSearch(ctx, &qmsg.ExtContactSearchReq{
		ListOption: core.NewListOption().AddOpt(quan.GetExtContactFollowListReq_ListOptionSearchName, "林").
			AddOpt(quan.GetExtContactFollowListReq_ListOptionUidList, []uint64{1699, 1019,
				5229708078}).SetLimit(100),
	})
	if err != nil {
		log.Errorf("err %v", err)
		return
	}

	for _, v := range rsp.List {
		fmt.Println(v)
	}

	autRsp, err := qmsg.AddUserTag(ctx, &qmsg.AddUserTagReq{
		TagName: "第一个",
	})
	if err != nil {
		log.Errorf("err %v", err)
		return
	}

	log.Infof("%v", autRsp)

	gutRsp, err := qmsg.GetUserTagList(ctx, &qmsg.GetUserTagListReq{})
	if err != nil {
		log.Errorf("err %v", err)
		return
	}

	log.Infof("%v", gutRsp.TagList)

	if len(gutRsp.TagList) == 0 {
		log.Errorf("err not found ")
		return
	}

	_, err = qmsg.SetUserTag(ctx, &qmsg.SetUserTagReq{
		TagName: "第一个改",
		TagId:   gutRsp.TagList[0].TagId,
	})
	if err != nil {
		log.Errorf("err %v", err)
		return
	}

	gutRsp, err = qmsg.GetUserTagList(ctx, &qmsg.GetUserTagListReq{})
	if err != nil {
		log.Errorf("err %v", err)
		return
	}

	log.Infof("%v", gutRsp.TagList)

	_, err = qmsg.SetExtContactUserTag(ctx, &qmsg.SetExtContactUserTagReq{
		ExtAccountId: 762915,
		IsSet:        true,
		TagId:        gutRsp.TagList[0].TagId,
	})

	if err != nil {
		log.Errorf("err %v", err)
		return
	}

	ggRsp, err := qmsg.GetExtContactListByUserTag(ctx, &qmsg.GetExtContactListByUserTagReq{})
	if err != nil {
		log.Errorf("err %v", err)
		return
	}

	log.Infof("%v", ggRsp)

}
