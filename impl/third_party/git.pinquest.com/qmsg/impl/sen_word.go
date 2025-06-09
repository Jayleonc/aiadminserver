package impl

import (
	"fmt"
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/dbx"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/brick/utils"
	"git.pinquest.cn/qlb/core"
	"git.pinquest.cn/qlb/iquan"
	"git.pinquest.cn/qlb/qmsg"
	"git.pinquest.cn/qlb/qrobot"
	"git.pinquest.cn/qlb/quan"
	"strings"
	"unicode"
)

type SenWordChecker struct {
	ctx     *rpc.Context
	corpId  uint32
	appId   uint32
	content string
	record  *SenWordRecord
}
type SenWordRecord struct {
	robotUid uint64
	uid      uint64
	//isGroup   bool
	chatExtId uint64
	chatType  uint32
}

func SetSensitiveWordRule(ctx *rpc.Context, req *qmsg.SetSensitiveWordRuleReq) (*qmsg.SetSensitiveWordRuleRsp, error) {
	var rsp qmsg.SetSensitiveWordRuleRsp
	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)
	m := req.SensitiveWordRule
	m.CorpId = corpId
	m.AppId = appId
	if m.Rule != nil && len(m.Rule.WordList) > 0 {
		err := checkSenWordFormat(m.Rule.WordList)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
	}
	var rule qmsg.ModelSensitiveWordRule
	err := SenWordRule.WhereCorpApp(corpId, appId).Where(DbRuleType, m.RuleType).First(ctx, &rule)
	if err != nil && !SenWordRule.IsNotFoundErr(err) {
		log.Errorf("err:%v", err)
		return nil, err
	}
	if err != nil {
		err := SenWordRule.Create(ctx, m)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
	} else {
		m.Id = rule.Id
		_, err := SenWordRule.NewScope().WhereCorpApp(corpId, appId).Save(ctx, m)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
	}
	rsp.SensitiveWordRule = m
	return &rsp, nil
}
func checkSenWordFormat(words []string) error {
	if len(words) == 0 {
		return nil
	}
	for _, word := range words {
		for _, c := range word {
			if !unicode.Is(unicode.Han, c) && !unicode.IsLetter(c) && !unicode.IsDigit(c) {
				return rpc.CreateError(qmsg.ErrInvalidSensitiveWordFormat)
			}
		}
	}
	return nil
}
func GetSensitiveWordRule(ctx *rpc.Context, req *qmsg.GetSensitiveWordRuleReq) (*qmsg.GetSensitiveWordRuleRsp, error) {
	var rsp qmsg.GetSensitiveWordRuleRsp
	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)
	var rule qmsg.ModelSensitiveWordRule

	db := SenWordRule.WhereCorpApp(corpId, appId)
	if req.RuleType > 0 {
		db = db.Where(DbRuleType, req.RuleType)
	}

	err := db.First(ctx, &rule)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	rsp.SensitiveWordRule = &rule
	return &rsp, nil
}
func NewSenWordChecker(ctx *rpc.Context, corpId, appId uint32, content string, record *SenWordRecord) *SenWordChecker {
	return &SenWordChecker{
		ctx:     ctx,
		corpId:  corpId,
		appId:   appId,
		content: content,
		record:  record,
	}
}
func (p *SenWordChecker) check() ([]string, error) {
	// 要检测的发送内容为空白，无须校验
	if p.content == "" {
		return nil, nil
	}
	var rule qmsg.ModelSensitiveWordRule
	err := SenWordRule.WhereCorpApp(p.corpId, p.appId).Where(DbRuleType, uint32(qmsg.ModelSensitiveWordRule_User)).First(p.ctx, &rule)
	if err != nil && !SenWordRule.IsNotFoundErr(err) {
		log.Errorf("err:%v", err)
		return nil, err
	}
	// 未开启或没有敏感词规则，直接通过
	if err != nil || !rule.Enabled || rule.Rule == nil || len(rule.Rule.WordList) == 0 {
		return nil, nil
	}
	var words []string
	for _, word := range rule.Rule.WordList {
		if strings.Contains(p.content, word) {
			words = append(words, word)
		}
	}
	// 发送内容不包含任何敏感词
	if len(words) == 0 {
		return nil, nil
	}
	r := p.record
	if r == nil {
		r = &SenWordRecord{}
	}
	record := qmsg.ModelSensitiveOptRecord{
		Uid:               r.uid,
		CorpId:            p.corpId,
		AppId:             p.appId,
		IsGroup:           uint32(qmsg.ChatType_ChatTypeGroup) == r.chatType,
		Content:           p.content,
		ChatType:          r.chatType,
		RobotUid:          r.robotUid,
		ChatExtId:         r.chatExtId,
		SensitiveWordList: words,
	}
	err = SenOptRecord.Create(p.ctx, &record)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	return words, rpc.CreateError(qmsg.ErrTriggerCorpSensitiveWords)
}
func GetSensitiveOptRecordList(ctx *rpc.Context, req *qmsg.GetSensitiveOptRecordListReq) (*qmsg.GetSensitiveOptRecordListRsp, error) {
	var rsp qmsg.GetSensitiveOptRecordListRsp
	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)
	db := SenOptRecord.NewList(req.ListOption).WhereCorpApp(corpId, appId).OrderDesc(DbId)
	var isEmpty bool
	err := core.NewListOptionProcessor(req.ListOption).
		AddUint32List(
			qmsg.GetSensitiveOptRecordListReq_ListOptionCreateDateRange,
			func(valList []uint32) error {
				if len(valList) > 0 {
					db.Where(map[string]interface{}{
						"created_at >=": valList[0],
					})
				}
				if len(valList) > 1 {
					db.Where(map[string]interface{}{
						"created_at <": valList[1],
					})
				}
				return nil
			}).
		AddString(
			qmsg.GetSensitiveOptRecordListReq_ListOptionPlatformNameOrCustomerServiceName,
			func(val string) error {
				orCond := map[string]interface{}{}
				{
					var users []*qmsg.ModelUser
					err := User.WhereCorpApp(corpId, appId).WhereLike(DbUsername, fmt.Sprintf("%%%s%%", val)).Find(ctx, &users)
					if err != nil {
						log.Errorf("err:%v", err)
						return err
					}
					if len(users) > 0 {
						orCond[fmt.Sprintf("%s in", DbUid)] = utils.PluckUint64(users, "Id")
					}
				}
				{
					iRsp, err := iquan.GetUserList(ctx, &iquan.GetUserListReq{
						ListOption: core.NewListOption().SetSkipCount().AddOpt(iquan.GetUserListReq_ListOptionName, val),
						CorpId:     corpId,
						AppId:      appId,
					})
					if err != nil {
						log.Errorf("err:%v", err)
						return err
					}
					if len(iRsp.List) > 0 {
						orCond[fmt.Sprintf("%s in", DbRobotUid)] = utils.PluckUint64(iRsp.List, "Id")
					}
				}
				if len(orCond) > 0 {
					db.OrWhere(orCond)
				} else {
					isEmpty = true
				}
				return nil
			}).
		AddBool(qmsg.GetSensitiveOptRecordListReq_ListOptionIsAsc,
			func(val bool) error {
				if val {
					db.OrderAsc()
				} else {
					db.OrderDesc()
				}
				return nil
			}).
		Process()
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	if isEmpty {
		rsp.Paginate = &core.Paginate{
			Offset: req.ListOption.Offset,
			Limit:  req.ListOption.Limit,
		}
		return &rsp, nil
	}
	rsp.Paginate, err = db.FindPaginate(ctx, &rsp.List)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	if len(rsp.List) == 0 {
		return &rsp, nil
	}
	{
		uids := utils.PluckUint64(rsp.List, "Uid")
		var users []*qmsg.ModelUser
		err := User.WhereCorpApp(corpId, appId).WhereIn(DbId, uids).Find(ctx, &users)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
		utils.KeyBy(users, "Id", &rsp.UserMap)
	}
	{
		robotUids := utils.PluckUint64(rsp.List, "RobotUid")
		qRsp, err := qrobot.GetAccountListSys(ctx, &qrobot.GetAccountListSysReq{
			ListOption: core.NewListOption(qrobot.GetAccountListSysReq_ListOptionUidList, robotUids).SetSkipCount().SetLimit(uint32(len(robotUids))),
			CorpId:     corpId,
			AppId:      appId,
		})
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
		utils.KeyBy(qRsp.List, "Uid", &rsp.RobotMap)
	}
	{
		var accountIds, groupIds []uint64
		for _, record := range rsp.List {
			switch record.ChatType {
			case uint32(qmsg.ChatType_ChatTypeSingle):
				accountIds = append(accountIds, record.ChatExtId)
			case uint32(qmsg.ChatType_ChatTypeGroup):
				groupIds = append(groupIds, record.ChatExtId)
			default:
			}
		}
		if len(accountIds) > 0 {
			alo := core.NewListOption().SetSkipCount().SetLimit(uint32(len(accountIds)))
			alo.AddOpt(qrobot.GetAccountListSysReq_ListOptionIdList, accountIds)
			aRsp, err := qrobot.GetAccountListSys(ctx, &qrobot.GetAccountListSysReq{
				ListOption: alo,
				CorpId:     corpId,
				AppId:      appId,
			})
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, err
			}
			if len(aRsp.List) > 0 {
				extUids := make([]uint64, 0, len(aRsp.List))
				ext2acc := make(map[uint64]uint64, len(aRsp.List))
				for _, a := range aRsp.List {
					extUids = append(extUids, a.ExtUid)
					ext2acc[a.ExtUid] = a.Id
				}
				elo := core.NewListOption()
				elo.AddOpt(iquan.GetExtContactListReq_ListOptionExtUidList, extUids)
				extRsp, err := iquan.GetExtContactList(ctx, &iquan.GetExtContactListReq{
					ListOption: elo,
					AppId:      appId,
					CorpId:     corpId,
				})
				if err != nil {
					log.Errorf("err:%v", err)
					return nil, err
				}
				// key:qrobot.ModelAccount的id	value:quan.ModelExtContact
				extMap := make(map[uint64]*quan.ModelExtContact, len(extRsp.List))
				for _, ext := range extRsp.List {
					if _, ok := ext2acc[ext.Id]; ok {
						extMap[ext2acc[ext.Id]] = ext
					}
				}
				rsp.ExtMap = extMap
			}
		}
		if len(groupIds) > 0 {
			glo := core.NewListOption()
			glo.AddOpt(qrobot.GetGroupChatListSysReq_ListOptionGroupIdList, groupIds)
			gRsp, err := qrobot.GetGroupChatListSys(ctx, &qrobot.GetGroupChatListSysReq{
				ListOption: glo,
				CorpId:     corpId,
				AppId:      appId,
			})
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, err
			}
			utils.KeyBy(gRsp.List, "Id", &rsp.GroupMap)
		}
	}
	return &rsp, nil
}
func GetSensitiveOptRecord(ctx *rpc.Context, req *qmsg.GetSensitiveOptRecordReq) (*qmsg.GetSensitiveOptRecordRsp, error) {
	var rsp qmsg.GetSensitiveOptRecordRsp
	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)
	record, err := SenOptRecord.Get(ctx, corpId, appId, req.Id)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	{
		user, err := User.get(ctx, corpId, appId, record.Uid)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
		rsp.UserMap = map[uint64]*qmsg.ModelUser{user.Id: user}
	}
	{
		qlo := core.NewListOption()
		qlo.AddOpt(qrobot.GetQRobotListSysReq_ListOptionUidList, []uint64{record.RobotUid})
		qRsp, err := qrobot.GetQRobotListSys(ctx, &qrobot.GetQRobotListSysReq{
			ListOption: qlo,
			CorpId:     corpId,
			AppId:      appId,
		})
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
		utils.KeyBy(qRsp.List, "Uid", &rsp.RobotMap)
	}
	{
		if record.IsGroup {
			gRsp, err := qrobot.GetGroupChatSys(ctx, &qrobot.GetGroupChatSysReq{
				GroupChatId: record.ChatExtId,
				CorpId:      corpId,
				AppId:       appId,
			})
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, err
			}
			chat := gRsp.GroupChat
			rsp.GroupMap = map[uint64]*qrobot.ModelGroupChat{chat.Id: chat}
		} else {
			aRsp, err := qrobot.GetAccountSys(ctx, &qrobot.GetAccountSysReq{
				CorpId: corpId,
				AppId:  appId,
				Id:     record.ChatExtId,
			})
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, err
			}
			acc := aRsp.Account
			rsp.AccountMap = map[uint64]*qrobot.ModelAccount{acc.Id: acc}
		}
	}
	rsp.SensitiveOptRecord = record
	return &rsp, nil
}

func GetExtSensitiveOptRecordList(ctx *rpc.Context, req *qmsg.GetExtSensitiveOptRecordListReq) (*qmsg.GetExtSensitiveOptRecordListRsp, error) {
	var rsp qmsg.GetExtSensitiveOptRecordListRsp
	rsp.RobotAccountMap = make(map[uint64]*qrobot.ModelAccount)
	rsp.AccountMap = make(map[uint64]*qrobot.ModelAccount)
	rsp.GroupMap = make(map[uint64]*qrobot.ModelGroupChat)
	rsp.TagMap = make(map[uint64]*quan.ExtContactFollowTagList)

	var condMap = make(map[string]interface{})

	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)
	db := ExtSensitiveOptRecord.NewList(req.ListOption).WhereCorpApp(corpId, appId).OrderDesc(DbCreatedAt)
	var isEmpty bool
	err := core.NewListOptionProcessor(req.ListOption).
		AddTimeStampRange(
			qmsg.GetExtSensitiveOptRecordListReq_ListOptionCreatedAtRange,
			func(beginAt, endAt uint32) error {
				db.WhereBetween(DbCreatedAt, beginAt, endAt)

				condMap["beginAt"] = beginAt
				condMap["endAt"] = endAt

				return nil
			}).
		AddString(
			qmsg.GetExtSensitiveOptRecordListReq_ListOptionNameLike,
			func(val string) error {
				orCond := map[string]interface{}{}
				{
					iRsp, err := iquan.GetUserList(ctx, &iquan.GetUserListReq{
						ListOption: core.NewListOption().SetSkipCount().AddOpt(iquan.GetUserListReq_ListOptionName, val),
						CorpId:     corpId,
						AppId:      appId,
					})
					if err != nil {
						log.Errorf("err:%v", err)
						return err
					}
					if len(iRsp.List) > 0 {
						orCond[fmt.Sprintf("%s in", DbRobotUid)] = utils.PluckUint64(iRsp.List, "Id")
					}
				}
				if len(orCond) > 0 {
					db.OrWhere(orCond)

					condMap["nameLikeCond"] = orCond

				} else {
					isEmpty = true
				}
				return nil
			}).
		AddUint64List(
			qmsg.GetExtSensitiveOptRecordListReq_ListOptionCorpTag, func(valList []uint64) error {
				// 不能裸奔
				// 固定带时间范围

				beginAt, bok := condMap["beginAt"]
				endAt, eok := condMap["endAt"]
				if !bok || !eok {
					err := rpc.InvalidArg("Time range must be specified")
					log.Errorf("err:%v", err)
					return err
				}

				timeDiff := endAt.(uint32) - beginAt.(uint32)
				const maxTimeRange = 31 * 24 * 60 * 60 // 限制最大31天
				if timeDiff > maxTimeRange {
					err := rpc.InvalidArg("Time range too large")
					log.Errorf("err:%v", err)
					return err
				}

				if isEmpty || len(valList) == 0 {
					isEmpty = true

					return nil
				}

				condMap["tagIds"] = valList

				acctIds, err := transToExtSensitiveOptRecordAcctIds(ctx, corpId, appId, condMap)
				if err != nil {
					log.Errorf("err:%v", err)
					return err
				}

				if len(acctIds) <= 0 {
					isEmpty = true
					return nil
				}

				db.WhereIn(DbAccountId, acctIds)

				return nil
			}).
		Process()
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	if isEmpty {
		rsp.Paginate = &core.Paginate{
			Offset: req.ListOption.Offset,
			Limit:  req.ListOption.Limit,
		}
		return &rsp, nil
	}
	rsp.Paginate, err = db.FindPaginate(ctx, &rsp.List)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	if len(rsp.List) == 0 {
		return &rsp, nil
	}

	{
		robotUidList := utils.PluckUint64(rsp.List, "RobotUid")
		qRsp, err := qrobot.GetAccountListSys(ctx, &qrobot.GetAccountListSysReq{
			ListOption: core.NewListOption(qrobot.GetAccountListSysReq_ListOptionUidList, robotUidList).SetSkipCount().SetLimit(uint32(len(robotUidList))),
			CorpId:     corpId,
			AppId:      appId,
		})
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
		utils.KeyBy(qRsp.List, "Uid", &rsp.RobotAccountMap)
	}
	{
		var accountIds, groupIds []uint64
		for _, record := range rsp.List {
			if record.AccountId != 0 {
				accountIds = append(accountIds, record.AccountId)
			}
			if record.GroupId != 0 {
				groupIds = append(groupIds, record.GroupId)
			}
		}
		if len(accountIds) > 0 {
			alo := core.NewListOption().SetSkipCount().SetLimit(uint32(len(accountIds)))
			alo.AddOpt(qrobot.GetAccountListSysReq_ListOptionIdList, accountIds)
			aRsp, err := qrobot.GetAccountListSys(ctx, &qrobot.GetAccountListSysReq{
				ListOption: alo,
				CorpId:     corpId,
				AppId:      appId,
			})
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, err
			}

			for _, v := range aRsp.List {
				rsp.AccountMap[v.Id] = v
			}
			if len(aRsp.List) > 0 {
				extUidList := make([]uint64, 0, len(aRsp.List))
				ext2acc := make(map[uint64]uint64, len(aRsp.List))
				for _, a := range aRsp.List {
					extUidList = append(extUidList, a.ExtUid)
					ext2acc[a.ExtUid] = a.Id
				}
				elo := core.NewListOption()
				elo.AddOpt(iquan.GetExtContactListReq_ListOptionExtUidList, extUidList)
				extRsp, err := iquan.GetExtContactList(ctx, &iquan.GetExtContactListReq{
					ListOption: elo,
					AppId:      appId,
					CorpId:     corpId,
				})
				if err != nil {
					log.Errorf("err:%v", err)
					return nil, err
				}
				// key:qrobot.ModelAccount的id	value:quan.ModelExtContact
				extMap := make(map[uint64]*quan.ModelExtContact, len(extRsp.List))
				for _, ext := range extRsp.List {
					if _, ok := ext2acc[ext.Id]; ok {
						extMap[ext2acc[ext.Id]] = ext
					}
				}
				rsp.ExtMap = extMap

				//
				followList, err := iquan.GetExtContactFollowList(ctx, &iquan.GetExtContactFollowListReq{
					ListOption:         core.NewListOption().AddOpt(iquan.GetExtContactFollowListReq_ListOptionExtUidList, extUidList),
					CorpId:             corpId,
					AppId:              appId,
					CancelOrderByAddAt: true,
				})
				if err != nil {
					log.Errorf("err:%v", err)
					return nil, err
				}

				var tmpMap = make(map[uint64]map[uint64]*quan.ModelExtContactFollow_Tag)
				for _, follow := range followList.List {
					if _, ok := tmpMap[follow.ExtUid]; !ok {
						tmpMap[follow.ExtUid] = make(map[uint64]*quan.ModelExtContactFollow_Tag)
					}

					if follow.Info != nil && len(follow.Info.TagList) > 0 {
						for _, tag := range follow.Info.TagList {
							if _, ok := tmpMap[follow.ExtUid][tag.Id]; !ok {
								tmpMap[follow.ExtUid][tag.Id] = tag
							}
						}
					}
				}

				var tagMap = make(map[uint64]*quan.ExtContactFollowTagList)
				for extUid, tmpTagMap := range tmpMap {
					if _, ok := tagMap[extUid]; !ok {
						tagMap[extUid] = &quan.ExtContactFollowTagList{TagList: make([]*quan.ModelExtContactFollow_Tag, 0)}
					}

					if len(tmpTagMap) == 0 {
						continue
					}

					for _, tag1 := range tmpTagMap {
						tagMap[extUid].TagList = append(tagMap[extUid].TagList, tag1)
					}
				}

				rsp.TagMap = tagMap
			}
		}
		if len(groupIds) > 0 {
			glo := core.NewListOption()
			glo.AddOpt(qrobot.GetGroupChatListSysReq_ListOptionGroupIdList, groupIds)
			gRsp, err := qrobot.GetGroupChatListSys(ctx, &qrobot.GetGroupChatListSysReq{
				ListOption: glo,
				CorpId:     corpId,
				AppId:      appId,
			})
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, err
			}
			utils.KeyBy(gRsp.List, "Id", &rsp.GroupMap)
		}
	}
	return &rsp, nil
}

func transToExtSensitiveOptRecordAcctIds(ctx *rpc.Context, corpId, appId uint32, condMap map[string]interface{}) ([]uint64, error) {
	var acctIds []uint64

	tagIds := condMap["tagIds"]

	// base
	d := ExtSensitiveOptRecord.WhereCorpApp(corpId, appId).WhereBetween(DbCreatedAt, condMap["beginAt"], condMap["endAt"])

	if nameLikeCond, ok := condMap["nameLikeCond"]; ok {
		d.OrWhere(nameLikeCond)
	}

	d1 := new(dbx.Scope)
	*d1 = *d

	total, err := d1.DistinctCount(ctx, DbAccountId)
	if err != nil {
		log.Errorf("err:%v", err)
		return []uint64{}, err
	}

	if total <= 0 {
		err = rpc.InvalidArg("data empty")
		log.Errorf("err:%v", err)
		return []uint64{}, nil
	}

	const maxLen = 20000
	if total > maxLen {
		err = rpc.InvalidArg("data too large")
		log.Errorf("err:%v", err)
		return []uint64{}, err
	}

	var pageSize uint32 = 4000
	var tOffset uint32 = 0
	var fieldsForAcctList = fmt.Sprintf("%s,%s", iquan.DbId, iquan.DbExtUid)
	var fieldsForFollowList = fmt.Sprintf("%s,%s", iquan.DbId, iquan.DbExtUid)

	for ; tOffset < total; tOffset += pageSize {

		var extSensitiveOptRecordList []*qmsg.ModelExtSensitiveOptRecord

		d2 := new(dbx.Scope)
		*d2 = *d

		err = d2.SetOffset(tOffset).SetLimit(pageSize).Select(DbId, DbAccountId, fmt.Sprintf("MAX(%s) AS maxCreatedAt", DbCreatedAt)).Group(DbAccountId).OrderDesc("maxCreatedAt").Find(ctx, &extSensitiveOptRecordList)
		if err != nil {
			log.Errorf("err:%v", err)
			return []uint64{}, err
		}

		accountList, err := qrobot.GetAccountListSys(ctx, &qrobot.GetAccountListSysReq{
			ListOption: core.NewListOption().SetSkipCount().SetLimit(pageSize).
				AddOpt(qrobot.GetAccountListSysReq_ListOptionSelectFields, fieldsForAcctList).
				AddOpt(qrobot.GetAccountListSysReq_ListOptionIdList, utils.PluckUint64(extSensitiveOptRecordList, "AccountId")),
			CorpId: corpId,
			AppId:  appId,
		})
		if err != nil {
			log.Errorf("err:%v", err)
			return []uint64{}, err
		}

		if len(accountList.List) == 0 {
			continue
		}

		// @todo 客户可能未匹配，ext_uid = 0
		var acctMap = make(map[uint64]uint64)
		for _, acct := range accountList.List {
			if acct.ExtUid == 0 {
				continue
			}

			acctMap[acct.ExtUid] = acct.Id
		}

		followList, err := iquan.GetExtContactFollowList(ctx, &iquan.GetExtContactFollowListReq{
			ListOption: core.NewListOption().SetSkipCount().SetLimit(pageSize).
				AddOpt(iquan.GetExtContactFollowListReq_ListOptionSelectColumn, fieldsForFollowList).
				AddOpt(iquan.GetExtContactFollowListReq_ListOptionGroupByExtUid, true).
				AddOpt(iquan.GetExtContactFollowListReq_ListOptionCorpTagIdList, tagIds).
				AddOpt(iquan.GetExtContactFollowListReq_ListOptionExtUidList, utils.PluckUint64(accountList.List, "ExtUid")),
			CorpId:             corpId,
			AppId:              appId,
			CancelOrderByAddAt: true,
		})
		if err != nil {
			log.Errorf("err:%v", err)
			return []uint64{}, err
		}

		if len(followList.List) == 0 {
			continue
		}

		for _, follow := range followList.List {
			acctIds = append(acctIds, acctMap[follow.ExtUid])
		}
	}

	return acctIds, err
}
