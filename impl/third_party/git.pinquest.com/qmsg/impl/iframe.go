package impl

import (
	"fmt"
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/core"
	"git.pinquest.cn/qlb/qmsg"
	"git.pinquest.cn/qlb/quan"
)

func GetIframeList(ctx *rpc.Context, req *qmsg.GetIframeListReq) (*qmsg.GetIframeListRsp, error) {
	var rsp qmsg.GetIframeListRsp

	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)

	db := IframeSetting.WhereCorpApp(corpId, appId)

	if req.SettingType == uint32(qmsg.ModelIframeSetting_SettingTypeAccess) ||
		req.SettingType == uint32(qmsg.ModelIframeSetting_SettingTypeNil) {
		db.WhereIn(DbSettingType, []uint32{uint32(qmsg.ModelIframeSetting_SettingTypeAccess), uint32(qmsg.ModelIframeSetting_SettingTypeNil)})
	}

	if req.SettingType == uint32(qmsg.ModelIframeSetting_SettingTypeCustomize) {
		db.Where(DbSettingType, uint32(qmsg.ModelIframeSetting_SettingTypeCustomize))
	}

	var list []*qmsg.ModelIframeSetting
	err := db.Find(ctx, &list)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	rsp.List = list

	return &rsp, nil
}

func AddIframe(ctx *rpc.Context, req *qmsg.AddIframeReq) (*qmsg.AddIframeRsp, error) {
	var rsp qmsg.AddIframeRsp

	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)

	// 获取系统配置最大上限数量
	corpGroupConfig, err := quan.GetGroupNumConfigByCorpId(ctx, &quan.GetGroupNumConfigByCorpIdReq{CorpId: corpId, AppId: appId})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	db := IframeSetting.WhereCorpApp(corpId, appId)

	if req.Iframe.SettingType == uint32(qmsg.ModelIframeSetting_SettingTypeAccess) ||
		req.Iframe.SettingType == uint32(qmsg.ModelIframeSetting_SettingTypeNil) {
		db.WhereIn(DbSettingType, []uint32{uint32(qmsg.ModelIframeSetting_SettingTypeAccess), uint32(qmsg.ModelIframeSetting_SettingTypeNil)})
	}

	if req.Iframe.SettingType == uint32(qmsg.ModelIframeSetting_SettingTypeCustomize) {
		db.Where(DbSettingType, uint32(qmsg.ModelIframeSetting_SettingTypeCustomize))
	}

	// 统计数量
	count, err := db.Count(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	// 校验数量是不是达到上限了
	if count >= corpGroupConfig.MaxQmsgIframeNum {
		return nil, rpc.InvalidArg(fmt.Sprintf("最多只能创建%d个", corpGroupConfig.MaxQmsgIframeNum))
	}

	if req.Iframe.SettingType == uint32(qmsg.ModelIframeSetting_SettingTypeCustomize) && count > 1 {
		return nil, rpc.InvalidArg("最多只能创建一个")

	}

	iframe := req.Iframe
	iframe.CorpId = corpId
	iframe.AppId = appId

	err = IframeSetting.Create(ctx, iframe)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	rsp.Iframe = iframe

	return &rsp, nil
}

func SetIframe(ctx *rpc.Context, req *qmsg.SetIframeReq) (*qmsg.SetIframeRsp, error) {
	var rsp qmsg.SetIframeRsp

	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)

	iframe := req.Iframe
	if iframe.Id == 0 {
		return nil, rpc.InvalidArg("missed id")
	}
	iframe.CorpId = corpId
	iframe.AppId = appId

	// 兼容前端新旧版本交替默认没传这个字段的情况
	if iframe.RefreshType == 0 {
		iframe.RefreshType = uint32(qmsg.ModelIframeSetting_RefreshTypeAuto)
	}

	_, err := IframeSetting.WhereCorpApp(corpId, appId).Where(DbId, iframe.Id).Update(ctx, map[string]interface{}{
		DbTitle:          iframe.Title,
		DbUrl:            iframe.Url,
		DbIframeOpenType: iframe.IframeOpenType,
		DbIsPrivateShow:  iframe.IsPrivateShow,
		DbIsGroupShow:    iframe.IsGroupShow,
		DbIsEnable:       iframe.IsEnable,
		DbRefreshType:    iframe.RefreshType,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	if iframe.IsEnable {
		err = pushWsMsgToAll(ctx, corpId, appId, []*qmsg.WsMsgWrapper{
			{
				MsgType: uint32(qmsg.WsMsgWrapper_MsgTypeSidebarChange),
			},
		})
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
	}

	rsp.Iframe = iframe

	return &rsp, nil
}

func DeleteIframe(ctx *rpc.Context, req *qmsg.DeleteIframeReq) (*qmsg.DeleteIframeRsp, error) {
	var rsp qmsg.DeleteIframeRsp

	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)

	_, err := IframeSetting.WhereCorpApp(corpId, appId).Where(DbId, req.Id).Delete(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	err = pushWsMsgToAll(ctx, corpId, appId, []*qmsg.WsMsgWrapper{
		{
			MsgType: uint32(qmsg.WsMsgWrapper_MsgTypeSidebarChange),
		},
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	return &rsp, nil
}

func UpdateIframeStatus(ctx *rpc.Context, req *qmsg.UpdateIframeStatusReq) (*qmsg.UpdateIframeStatusRsp, error) {
	var rsp qmsg.UpdateIframeStatusRsp

	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)

	_, err := IframeSetting.WhereCorpApp(corpId, appId).Where(DbId, req.Id).Update(ctx, map[string]interface{}{
		DbIsEnable: req.IsEnable,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	err = pushWsMsgToAll(ctx, corpId, appId, []*qmsg.WsMsgWrapper{
		{
			MsgType: uint32(qmsg.WsMsgWrapper_MsgTypeSidebarChange),
		},
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	return &rsp, nil
}
