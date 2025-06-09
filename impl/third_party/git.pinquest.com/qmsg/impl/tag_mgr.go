package impl

import (
	"git.pinquest.cn/qlb/qcorptagmgr"
	"git.pinquest.cn/qlb/quan"
)

func tagMgr2QuanCorpTagGroupList(list []*qcorptagmgr.CorpTagGroup) []*quan.CorpTagGroup {
	if list == nil {
		return nil
	}
	var r []*quan.CorpTagGroup
	for _, tg := range list {
		var tagList []*quan.ModelCorpTag
		for _, tag := range tg.TagList {
			tagList = append(tagList, &quan.ModelCorpTag{
				Id:          tag.Id,
				CreatedAt:   tag.CreatedAt,
				UpdatedAt:   tag.UpdatedAt,
				CorpId:      tag.CorpId,
				AppId:       tag.AppId,
				GroupId:     tag.GroupId,
				Name:        tag.Name,
				WwCreatedAt: tag.WwCreatedAt,
				WwId:        tag.WwId,
				WwGroupId:   tag.WwGroupId,
				ViewOrder:   tag.ViewOrder,
			})
		}
		var m = &quan.CorpTagGroup{
			Group: &quan.ModelCorpTagGroup{
				Id:          tg.Group.Id,
				CreatedAt:   tg.Group.CreatedAt,
				UpdatedAt:   tg.Group.UpdatedAt,
				CorpId:      tg.Group.CorpId,
				AppId:       tg.Group.AppId,
				CreatorUid:  tg.Group.CreatorUid,
				Name:        tg.Group.Name,
				WwCreatedAt: tg.Group.WwCreatedAt,
				WwId:        tg.Group.WwId,
				ViewOrder:   tg.Group.ViewOrder,
			},
			TagList: tagList,
		}
		r = append(r, m)
	}
	return r
}

func quan2TagMgrCorpTagGroup(tg *quan.CorpTagGroup) *qcorptagmgr.CorpTagGroup {
	if tg == nil {
		return nil
	}
	var tagList []*qcorptagmgr.ModelCorpTag
	for _, tag := range tg.TagList {
		tagList = append(tagList, &qcorptagmgr.ModelCorpTag{
			Id:          tag.Id,
			CreatedAt:   tag.CreatedAt,
			UpdatedAt:   tag.UpdatedAt,
			CorpId:      tag.CorpId,
			AppId:       tag.AppId,
			GroupId:     tag.GroupId,
			Name:        tag.Name,
			WwCreatedAt: tag.WwCreatedAt,
			WwId:        tag.WwId,
			WwGroupId:   tag.WwGroupId,
			ViewOrder:   tag.ViewOrder,
		})
	}
	return &qcorptagmgr.CorpTagGroup{
		Group: &qcorptagmgr.ModelCorpTagGroup{
			Id:          tg.Group.Id,
			CreatedAt:   tg.Group.CreatedAt,
			UpdatedAt:   tg.Group.UpdatedAt,
			CorpId:      tg.Group.CorpId,
			AppId:       tg.Group.AppId,
			CreatorUid:  tg.Group.CreatorUid,
			Name:        tg.Group.Name,
			WwCreatedAt: tg.Group.WwCreatedAt,
			WwId:        tg.Group.WwId,
			ViewOrder:   tg.Group.ViewOrder,
		},
		TagList: tagList,
	}
}

func TagMgr2QuanCorpTagGroup(tg *qcorptagmgr.CorpTagGroup) *quan.CorpTagGroup {
	if tg == nil {
		return nil
	}
	var tagList []*quan.ModelCorpTag
	for _, tag := range tg.TagList {
		tagList = append(tagList, &quan.ModelCorpTag{
			Id:          tag.Id,
			CreatedAt:   tag.CreatedAt,
			UpdatedAt:   tag.UpdatedAt,
			CorpId:      tag.CorpId,
			AppId:       tag.AppId,
			GroupId:     tag.GroupId,
			Name:        tag.Name,
			WwCreatedAt: tag.WwCreatedAt,
			WwId:        tag.WwId,
			WwGroupId:   tag.WwGroupId,
			ViewOrder:   tag.ViewOrder,
		})
	}
	return &quan.CorpTagGroup{
		Group: &quan.ModelCorpTagGroup{
			Id:          tg.Group.Id,
			CreatedAt:   tg.Group.CreatedAt,
			UpdatedAt:   tg.Group.UpdatedAt,
			CorpId:      tg.Group.CorpId,
			AppId:       tg.Group.AppId,
			CreatorUid:  tg.Group.CreatorUid,
			Name:        tg.Group.Name,
			WwCreatedAt: tg.Group.WwCreatedAt,
			WwId:        tg.Group.WwId,
			ViewOrder:   tg.Group.ViewOrder,
		},
		TagList: tagList,
	}
}

func tagMgr2QuanTag(tm *qcorptagmgr.ModelCorpTag) *quan.ModelCorpTag {
	if tm == nil {
		return nil
	}
	return &quan.ModelCorpTag{
		Id:          tm.Id,
		CreatedAt:   tm.CreatedAt,
		UpdatedAt:   tm.UpdatedAt,
		CorpId:      tm.CorpId,
		AppId:       tm.AppId,
		GroupId:     tm.GroupId,
		Name:        tm.Name,
		WwCreatedAt: tm.WwCreatedAt,
		WwId:        tm.WwId,
		WwGroupId:   tm.WwGroupId,
		ViewOrder:   tm.ViewOrder,
	}
}

func mgrCorpGroup2QuanCorpGroup(group *qcorptagmgr.ModelCorpTagGroup) *quan.ModelCorpTagGroup {
	if group == nil {
		return nil
	}

	return &quan.ModelCorpTagGroup{
		Id:          group.Id,
		CreatedAt:   group.CreatedAt,
		UpdatedAt:   group.UpdatedAt,
		CorpId:      group.CorpId,
		AppId:       group.AppId,
		CreatorUid:  group.CreatorUid,
		Name:        group.Name,
		WwCreatedAt: group.WwCreatedAt,
		WwId:        group.WwId,
		ViewOrder:   group.ViewOrder,
	}
}
