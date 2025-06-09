package impl

import (
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/core"
	"git.pinquest.cn/qlb/qmsg"
)

func GetSafeConfig(ctx *rpc.Context, req *qmsg.GetSafeConfigReq) (*qmsg.GetSafeConfigRsp, error) {
	var rsp qmsg.GetSafeConfigRsp

	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)

	config, err := getSafeConfig(ctx, corpId, appId)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	rsp.SafeConfig = config

	return &rsp, nil
}

func UpdateSafeConfig(ctx *rpc.Context, req *qmsg.UpdateSafeConfigReq) (*qmsg.UpdateSafeConfigRsp, error) {
	var rsp qmsg.UpdateSafeConfigRsp

	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)

	if req.SafeConfig == nil {
		return nil, rpc.InvalidArg("缺少参数")
	}

	updateData := map[string]interface{}{
		DbAllowAddContact:          req.SafeConfig.AllowAddContact,
		DbAllowDeleteContact:       req.SafeConfig.AllowDeleteContact,
		DbPhoneSensitiveType:       req.SafeConfig.PhoneSensitiveType,
		DbPhoneAllowEdit:           req.SafeConfig.PhoneAllowEdit,
		DbWatermarkComposeType:     req.SafeConfig.WatermarkComposeType,
		DbWatermarkComposeAutoText: req.SafeConfig.WatermarkComposeAutoText,
		DbIsHideAddress:            req.SafeConfig.IsHideAddress,
		DbAddressDisplay:           req.SafeConfig.AddressDisplay,
		DbDetail:                   req.SafeConfig.Detail,
	}

	if req.SafeConfig.Detail != nil {
		updateData[DbDetail] = req.SafeConfig.Detail
	}

	var config qmsg.ModelSafeConfig
	_, err := SafeConfig.UpdateOrCreate(ctx, map[string]interface{}{
		DbCorpId: corpId,
		DbAppId:  appId,
	}, updateData, &config)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	return &rsp, nil
}

func getSafeConfig(ctx *rpc.Context, corpId, appId uint32) (*qmsg.ModelSafeConfig, error) {
	var config qmsg.ModelSafeConfig
	_, err := SafeConfig.FirstOrCreate(ctx, map[string]interface{}{
		DbCorpId: corpId,
		DbAppId:  appId,
	}, map[string]interface{}{
		DbAllowAddContact:          false,
		DbAllowDeleteContact:       false,
		DbPhoneSensitiveType:       uint32(qmsg.PhoneSensitiveType_PhoneSensitiveTypeNil),
		DbPhoneAllowEdit:           true,
		DbWatermarkComposeType:     []uint32{},
		DbWatermarkComposeAutoText: "",
	}, &config)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	return &config, err
}
