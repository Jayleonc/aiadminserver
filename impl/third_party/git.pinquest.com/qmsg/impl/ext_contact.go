package impl

import (
	"fmt"
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/brick/utils"
	"git.pinquest.cn/qlb/core"
	"git.pinquest.cn/qlb/featswitch"
	"git.pinquest.cn/qlb/qcustomer"
	"git.pinquest.cn/qlb/qfc"
	"git.pinquest.cn/qlb/qmsg"
	"git.pinquest.cn/qlb/qrobot"
	"git.pinquest.cn/qlb/quan"
	"git.pinquest.cn/qlb/yc"
)

func SetExtContactAlias(ctx *rpc.Context, req *qmsg.SetExtContactAliasReq) (*qmsg.SetExtContactAliasRsp, error) {
	var rsp qmsg.SetExtContactAliasRsp
	user, err := getUserCheck(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	err = ensureHasRobotPerm(ctx, user, req.RobotUid)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	setRsp, err := qrobot.SetExtContactAliasSys(ctx, &qrobot.SetExtContactAliasSysReq{
		CorpId:    user.CorpId,
		AppId:     user.AppId,
		RobotUid:  req.RobotUid,
		AccountId: req.ExtAccountId,
		Alias:     req.Alias,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	rsp.TaskKey = setRsp.TaskKey
	return &rsp, nil
}

func DeleteContactSync(ctx *rpc.Context, req *qmsg.DeleteContactSyncReq) (*qmsg.DeleteContactSyncRsp, error) {
	var rsp qmsg.DeleteContactSyncRsp

	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)

	merchantNo, err := getMerchantId(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	qrobotSrv := qrobotService{}
	account, err := qrobotSrv.getAccountByRobotUid(ctx, corpId, appId, req.RobotUid)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	contact, err := qrobotSrv.getAccount(ctx, corpId, appId, req.ChatExtId)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	deleteRsp, err := qfc.DeleteContactSync(ctx, &qfc.DeleteContactSyncReq{
		YcReq: &yc.DeleteContactReq{
			MerchantNo:      merchantNo,
			RobotSerialNo:   account.YcSerialNo,
			ContactSerialNo: contact.YcSerialNo,
			RelaSerialNo:    "",
		},
		BizContext: getBizContext(corpId, appId, "DeleteContactSync"),
		Priority:   uint32(qfc.PriorityType_PriorityTypeHigh),
		MaxWaitMs:  8000,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	if !deleteRsp.YcRsp.Success {
		return &rsp, rpc.CreateError(qmsg.ErrYcOperateFail)
	}

	return &rsp, nil
}

func ConfirmAsSidedCustomerSync(ctx *rpc.Context, req *qmsg.ConfirmAsSidedCustomerSyncReq) (*qmsg.ConfirmAsSidedCustomerSyncRsp, error) {
	var rsp qmsg.ConfirmAsSidedCustomerSyncRsp

	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)

	merchantNo, err := getMerchantId(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	qrobotSrv := qrobotService{}
	account, err := qrobotSrv.getAccountByRobotUid(ctx, corpId, appId, req.RobotUid)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	contact, err := qrobotSrv.getAccount(ctx, corpId, appId, req.ChatExtId)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	confirmRsp, err := qfc.ConfirmAsSidedCustomerSync(ctx, &qfc.ConfirmAsSidedCustomerSyncReq{
		YcReq: &yc.ConfirmAsSidedCustomerReq{
			MerchantNo:       merchantNo,
			RobotSerialNo:    account.YcSerialNo,
			CustomerSerialNo: contact.YcSerialNo,
			RelaSerialNo:     "",
		},
		BizContext: getBizContext(corpId, appId, "ConfirmAsSidedCustomerSync"),
		Priority:   uint32(qfc.PriorityType_PriorityTypeHigh),
		MaxWaitMs:  8000,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	if !confirmRsp.YcRsp.Success {
		return &rsp, rpc.CreateError(qmsg.ErrYcOperateFail)
	}

	return &rsp, nil
}

func ChattingRetryAddCustomerSync(ctx *rpc.Context, req *qmsg.ChattingRetryAddCustomerSyncReq) (*qmsg.ChattingRetryAddCustomerSyncRsp, error) {
	var rsp qmsg.ChattingRetryAddCustomerSyncRsp

	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)

	merchantNo, err := getMerchantId(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	qrobotSrv := qrobotService{}
	account, err := qrobotSrv.getAccountByRobotUid(ctx, corpId, appId, req.RobotUid)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	confirmRsp, err := qfc.ChattingRetryAddCustomerSync(ctx, &qfc.ChattingRetryAddCustomerSyncReq{
		YcReq: &yc.ChattingRetryAddCustomerReq{
			MerchantNo:    merchantNo,
			RobotSerialNo: account.YcSerialNo,
			Message:       req.Message,
			SerialNo:      req.SerialNo,
			RelaSerialNo:  "",
		},
		BizContext: getBizContext(corpId, appId, "ChattingRetryAddCustomerSync"),
		Priority:   uint32(qfc.PriorityType_PriorityTypeHigh),
		MaxWaitMs:  8000,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	if !confirmRsp.YcRsp.Success {
		return &rsp, rpc.CreateError(qmsg.ErrYcOperateFail)
	}

	return &rsp, nil
}

// 问题1： 分页难
// 问题2： 外部客户名字需要用企微侧，否则引起歧义
func extContactSearch(ctx *rpc.Context, corpId, appId uint32, listOption *core.ListOption) (followInfoList []*qmsg.FollowInfo, hasMore bool, err error) {

	var corpTagIsUnion, rfmTagIsUnion bool
	followReq := qcustomer.GetExtContactFollowListReq{
		ListOption: core.NewListOption().SetLimit(listOption.Limit).SetOffset(listOption.Offset),
		CorpId:     corpId,
		AppId:      appId,
	}

	followReq.ListOption.AddOpt(qcustomer.FollowListOption_UserInScope, true)
	//数据不准，忽略这个
	//followReq.ListOption.AddOpt(qcustomer.FollowListOption_YcSerialNoIsExist, true)

	for _, o := range listOption.GetOptions() {
		if o.Type == int32(quan.GetExtContactFollowListReq_ListOptionCorpTagIsUnion) && o.Value == "true" {
			corpTagIsUnion = true
		}
	}
	for _, o := range listOption.GetOptions() {
		if o.Type == int32(quan.GetExtContactFollowListReq_ListOptionRfmTagIsUnion) && o.Value == "true" {
			rfmTagIsUnion = true
		}
	}
	// 处理列表查询条件
	err = core.NewListOptionProcessor(listOption).
		AddString(
			quan.GetExtContactFollowListReq_ListOptionSearchName,
			func(val string) error {
				const key = "QUAN_BLURRY_SEARCH_NAME"
				var enable *featswitch.GetFeatureFlagEnabledRsp
				enable, err = featswitch.GetFeatureFlagEnabled(ctx, &featswitch.GetFeatureFlagEnabledReq{Key: key, CorpId: corpId, AppId: appId})
				if err != nil {
					log.Errorf("err:%v", err)
					return err
				}

				if enable.Enabled {
					followReq.ListOption.AddOpt(qcustomer.FollowListOption_NameOrRemarkLike, val)
				} else {
					followReq.ListOption.AddOpt(qcustomer.FollowListOption_NameOrRemark, val)
				}
				return nil
			}).
		AddUint32(
			quan.GetExtContactFollowListReq_ListOptionGender,
			func(val uint32) error {
				followReq.ListOption.AddOpt(qcustomer.FollowListOption_Gender, val)
				return nil
			}).
		AddTimeStampRange(
			quan.GetExtContactFollowListReq_ListOptionAddTime,
			func(beginAt, endAt uint32) error {
				followReq.ListOption.AddOpt(qcustomer.FollowListOption_AddTimeRange, []uint32{beginAt, endAt})
				return nil
			}).
		AddUint64List(
			quan.GetExtContactFollowListReq_ListOptionCorpTag,
			func(valList []uint64) error {
				if corpTagIsUnion {
					followReq.ListOption.AddOpt(qcustomer.FollowListOption_TagOrList, valList)
				} else {
					followReq.ListOption.AddOpt(qcustomer.FollowListOption_TagAndList, valList)
				}
				return nil
			}).
		AddUint64List(
			quan.GetExtContactFollowListReq_ListOptionRfmTag,
			func(valList []uint64) error {
				if rfmTagIsUnion {
					followReq.ListOption.AddOpt(qcustomer.FollowListOption_RFMTagOrList, valList)
				} else {
					followReq.ListOption.AddOpt(qcustomer.FollowListOption_RFMTagAndList, valList)
				}
				return nil
			},
		).
		AddBool(
			quan.GetExtContactFollowListReq_ListOptionDeletedByStaff,
			func(val bool) error {
				followReq.ListOption.AddOpt(qcustomer.FollowListOption_DeletedByStaff, val)
				return nil
			}).
		AddBool(
			quan.GetExtContactFollowListReq_ListOptionDeletedByExtContact,
			func(val bool) error {
				followReq.ListOption.AddOpt(qcustomer.FollowListOption_DeletedByExtContact, val)
				return nil
			}).
		AddTimeStampRange(
			quan.GetExtContactFollowListReq_ListOptionUserDeletedAt,
			func(beginAt, endAt uint32) error {
				followReq.ListOption.AddOpt(qcustomer.FollowListOption_DeletedTimeRange, []uint32{beginAt, endAt})
				return nil
			}).
		AddUint64(
			quan.GetExtContactFollowListReq_ListOptionChannelId,
			func(val uint64) error {
				followReq.ListOption.AddOpt(qcustomer.FollowListOption_ChannelIdList, []uint64{val})
				return nil
			}).
		AddUint64List(
			quan.GetExtContactFollowListReq_ListOptionChannelIdList,
			func(valList []uint64) error {
				followReq.ListOption.AddOpt(qcustomer.FollowListOption_ChannelIdList, valList)
				return nil
			}).
		AddUint64List(
			quan.GetExtContactFollowListReq_ListOptionUidList,
			func(valList []uint64) error {

				//约定是常态10个，开特性30个，前端控制，兜底
				if len(valList) > 100 {
					return rpc.InvalidArg(fmt.Sprintf("选中机器人超过最大限制%d", 100))
				}

				if len(valList) == 0 {
					return rpc.InvalidArg("选中机器人为空")
				}
				followReq.ListOption.AddOpt(qcustomer.FollowListOption_UidList, valList)

				return nil
			}).
		AddUint32(quan.GetExtContactFollowListReq_ListOptionAddWay,
			func(val uint32) error {
				followReq.ListOption.AddOpt(qcustomer.FollowListOption_AddWayList, []uint32{val})
				return nil
			}).
		AddUint64List(
			quan.GetExtContactFollowListReq_ListOptionIdList,
			func(valList []uint64) error {
				followReq.ListOption.AddOpt(qcustomer.FollowListOption_IdList, valList)
				return nil
			}).
		AddUint64List(
			quan.GetExtContactFollowListReq_ListOptionExcludeCorpTag,
			func(valList []uint64) error {
				followReq.ListOption.AddOpt(qcustomer.FollowListOption_NotTagOrList, valList)
				return nil
			}).
		AddString(
			quan.GetExtContactFollowListReq_TimeTagValueAndList,
			func(val string) error {
				followReq.ListOption.AddOpt(qcustomer.FollowListOption_TimeTagValueAndList, val)
				return nil
			}).
		Process()
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, false, err
	}

	//followReq.SelectColumnList = []string{ "uid", "ext_uid", "add_at", "name", "avatar", "gender", "add_way"}

	qRsp, err := qcustomer.GetExtContactFollowList(ctx, &followReq)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, false, err
	}

	if len(qRsp.List) == 0 {
		return nil, false, err
	}

	var extUidList []uint64

	robotFollowListMap := make(map[uint64][]uint64)
	for _, v := range qRsp.List {
		extUidList = append(extUidList, v.ExtUid)
		robotFollowListMap[v.Uid] = append(robotFollowListMap[v.Uid], v.ExtUid)
	}

	accRsp, err := qrobot.GetAccountListSys(ctx, &qrobot.GetAccountListSysReq{
		ListOption: core.NewListOption().SetSkipCount().SetLimit(uint32(len(extUidList))).AddOpt(qrobot.GetAccountListSysReq_ListOptionExtUidList, extUidList),
		CorpId:     corpId,
		AppId:      appId,
	})
	if err != nil {
		log.Errorf("err %v", err)
		return nil, false, err
	}

	if len(accRsp.List) == 0 {
		return followInfoList, true, nil
	}

	extAccMap := make(map[uint64]*qrobot.ModelAccount) // key ext_uid
	utils.KeyBy(accRsp.List, "ExtUid", &extAccMap)

	accIdMap := make(map[uint64]*qrobot.ModelAccount)

	for robotUid, extList := range robotFollowListMap {
		var extAccountIdList []uint64
		for _, v := range extList {
			if val, ok := extAccMap[v]; ok {
				extAccountIdList = append(extAccountIdList, val.Id)
				accIdMap[val.Id] = val
			}
		}

		if len(extAccountIdList) == 0 {
			continue
		}

		var accFollowRsp *qrobot.GetAccountFollowListSysRsp
		accFollowRsp, err = qrobot.GetAccountFollowListSys(ctx, &qrobot.GetAccountFollowListSysReq{
			ListOption: core.NewListOption().AddOpt(qrobot.GetAccountFollowListSysReq_ListOptionExtAccountIdList, extAccountIdList),
			RobotUid:   robotUid,
			CorpId:     corpId,
			AppId:      appId,
		})
		if err != nil {
			log.Errorf("err %v", err)
			return nil, false, err
		}

		if len(accFollowRsp.List) == 0 {
			continue
		}

		for _, v := range accFollowRsp.FollowList {

			acc, ok := accIdMap[v.ExtAccountId]
			if !ok {
				log.Warnf("has result but not in request acc id list accId:%d followId:%d", v.ExtAccountId, v.Id)
				continue
			}

			followInfoList = append(followInfoList, &qmsg.FollowInfo{
				ExtAccountId: v.ExtAccountId,
				YcSerialNo:   acc.YcSerialNo,
				RobotUid:     robotUid,
				ExtUid:       acc.ExtUid,
				Name:         acc.Name,
				Avatar: func() string {
					if acc.Profile == nil {
						return ""
					}
					return acc.Profile.Avatar
				}(),
			})
		}
	}

	return followInfoList, true, nil
}

func ExtContactSearch(ctx *rpc.Context, req *qmsg.ExtContactSearchReq) (*qmsg.ExtContactSearchRsp, error) {
	var rsp qmsg.ExtContactSearchRsp
	var err error

	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)

	limit := req.ListOption.Limit
	offset := req.ListOption.Offset
	begin := req.ListOption.Offset
	maxFind := uint32(10000)

	for offset < (maxFind + begin) {
		var followInfoList []*qmsg.FollowInfo
		var hasMore bool
		followInfoList, hasMore, err = extContactSearch(ctx, corpId, appId, req.ListOption)
		if err != nil {
			log.Errorf("err %v", err)
			return nil, err
		}

		offset += req.ListOption.Limit
		req.ListOption.SetOffset(offset)

		rsp.List = append(rsp.List, followInfoList...)
		rsp.HasMore = hasMore
		rsp.Offset = offset

		if !hasMore || len(followInfoList) >= int(limit) {
			break
		}
	}

	return &rsp, nil
}
