package impl

import (
	"fmt"
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/core"
	"git.pinquest.cn/qlb/qmsg"
)

func SetSidebarSetting(ctx *rpc.Context, req *qmsg.SetSidebarSettingReq) (*qmsg.SetSidebarSettingRsp, error) {
	var rsp qmsg.SetSidebarSettingRsp

	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)

	_, err := SidebarSetting.set(ctx, corpId, appId, req.SidebarSetting.PrivateExceptModuleList, req.SidebarSetting.GroupExceptModuleList, &qmsg.ModelSidebarSetting{})
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

func GetSidebarSetting(ctx *rpc.Context, req *qmsg.GetSidebarSettingReq) (*qmsg.GetSidebarSettingRsp, error) {
	var rsp qmsg.GetSidebarSettingRsp

	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)

	var setting qmsg.ModelSidebarSetting
	_, err := SidebarSetting.get(ctx, corpId, appId, &setting)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	rsp.SidebarSetting = &setting

	return &rsp, nil
}

// 获取客服系统侧边栏tab排序列表
func GetSidebarTabSortList(ctx *rpc.Context, req *qmsg.GetSidebarTabSortListReq) (*qmsg.GetSidebarTabSortListRsp, error) {
	var rsp qmsg.GetSidebarTabSortListRsp

	// 写死每页数量
	req.ListOption.SetOffset(0)
	req.ListOption.SetLimit(200)

	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)
	db := SidebarTabSort.NewList(req.ListOption).
		WhereCorpApp(corpId, appId)
	err := core.NewListOptionProcessor(req.ListOption).
		AddUint64(qmsg.GetSidebarTabSortListReq_ListOptionUid, func(val uint64) error {
			db.Where(DbUid, val)
			return nil
		}).
		AddUint32List(qmsg.GetSidebarTabSortListReq_ListOptionSidebarTabType, func(val []uint32) error {
			db.WhereIn(DbTabType, val)
			return nil
		}).
		AddBool(qmsg.GetSidebarTabSortListReq_ListOptionOrderBySort, func(val bool) error {
			// 正序
			if val {
				db.OrderAsc(DbSort)
				return nil
			}
			// 倒序
			db.OrderDesc(DbSort)
			return nil
		}).
		Process()
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	// 默认是创建时间正序排序
	db.OrderAsc(DbCreatedAt)
	_, err = db.FindPaginate(ctx, &rsp.List)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	return &rsp, nil
}

// 设置客服系统侧边栏tab排序列表
func SetSidebarTabSortList(ctx *rpc.Context, req *qmsg.SetSidebarTabSortListReq) (*qmsg.SetSidebarTabSortListRsp, error) {
	var rsp qmsg.SetSidebarTabSortListRsp

	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)

	// 先查出当前客服所有排序记录
	var oldList []*qmsg.ModelSidebarTabSort
	err := SidebarTabSort.NewScope().
		WhereCorpApp(corpId, appId).
		Where(DbUid, req.Uid).Find(ctx, &oldList)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	// 串成KEY
	oldMap := map[string]*qmsg.ModelSidebarTabSort{}
	if len(oldList) > 0 {
		for _, oldSortItem := range oldList {
			key := fmt.Sprintf("%d_%s", oldSortItem.TabType, oldSortItem.TabKey)
			oldMap[key] = oldSortItem
		}
	}
	// 循环处理
	for _, sortItem := range req.List {
		// 检查记录是否已经存在
		uniqueKey := fmt.Sprintf("%d_%s", sortItem.TabType, sortItem.TabKey)
		oldItem, ok := oldMap[uniqueKey]
		if ok {
			// 已经存在的记录，直接更新
			oldItem.Sort = sortItem.Sort
			_, err := SidebarTabSort.NewScope().
				WhereCorpApp(corpId, appId).
				Save(ctx, oldItem)
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, err
			}
			// 把已更新的重置成nil，后面需要针对没匹配到的做删除处理
			oldMap[uniqueKey] = nil
			continue
		}
		// 新设置的排序记录
		err = SidebarTabSort.NewScope().Create(ctx, &qmsg.ModelSidebarTabSort{
			CorpId:  corpId,
			AppId:   appId,
			Uid:     req.Uid,
			TabType: sortItem.TabType,
			TabKey:  sortItem.TabKey,
			Sort:    sortItem.Sort,
		})
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
	}
	// 删除未被重新设置的项
	for _, oldSortItem := range oldMap {
		if oldSortItem != nil {
			// 删除
			_, err := SidebarTabSort.NewScope().
				WhereCorpApp(corpId, appId).
				Where(DbUid, oldSortItem.Uid).
				Where(DbId, oldSortItem.Id).
				Delete(ctx)
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, err
			}
		}
	}

	return &rsp, nil
}
