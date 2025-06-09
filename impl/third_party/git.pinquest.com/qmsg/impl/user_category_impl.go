package impl

import (
	"fmt"
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/dbproxy"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/brick/utils"
	"git.pinquest.cn/qlb/core"
	"git.pinquest.cn/qlb/qmsg"
	"strings"
)

func GetUserCategoryList(ctx *rpc.Context, req *qmsg.GetUserCategoryListReq) (*qmsg.GetUserCategoryListRsp, error) {
	var rsp qmsg.GetUserCategoryListRsp
	user, err := getQUserCheck(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	list, paginate, err := getUserCategoryList(ctx, user.CorpId, user.AppId, req.ListOption)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	rsp.List = list
	rsp.Paginate = paginate
	return &rsp, nil
}

func AddUserCategory(ctx *rpc.Context, req *qmsg.AddUserCategoryReq) (*qmsg.AddUserCategoryRsp, error) {
	var rsp qmsg.AddUserCategoryRsp
	user, err := getQUserCheck(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	categoryId, err := addUserCategory(ctx, user.CorpId, user.AppId, req.Category)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	rsp.Id = categoryId
	return &rsp, nil
}

func SetUserCategory(ctx *rpc.Context, req *qmsg.SetUserCategoryReq) (*qmsg.SetUserCategoryRsp, error) {
	var rsp qmsg.SetUserCategoryRsp
	user, err := getQUserCheck(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	_, err = setUserCategory(ctx, user.CorpId, user.AppId, req.Category)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	return &rsp, nil
}

func BatchDelUserCategory(ctx *rpc.Context, req *qmsg.BatchDelUserCategoryReq) (*qmsg.BatchDelUserCategoryRsp, error) {
	var rsp qmsg.BatchDelUserCategoryRsp
	user, err := getQUserCheck(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	err = batchDeleteUserCategory(ctx, user.CorpId, user.AppId, req.IdList)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	return &rsp, nil
}

func BatchSetUserCategory(ctx *rpc.Context, req *qmsg.BatchSetUserCategoryReq) (*qmsg.BatchSetUserCategoryRsp, error) {
	var rsp qmsg.BatchSetUserCategoryRsp
	user, err := getQUserCheck(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	if req.Category.PermissionData == nil {
		req.Category.PermissionData = &qmsg.ModelUserCategory_PermissionData{}
	}
	_, err = UserCategory.WhereCorpApp(user.CorpId, user.AppId).WhereIn(DbId, req.IdList).Update(ctx, map[string]interface{}{
		DbPermissionType: req.Category.PermissionType,
		DbPermissionData: req.Category.PermissionData,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	return &rsp, nil
}

func SortUserCategoryList(ctx *rpc.Context, req *qmsg.SortUserCategoryListReq) (*qmsg.SortUserCategoryListRsp, error) {
	var rsp qmsg.SortUserCategoryListRsp
	user, err := getQUserCheck(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	cnt := len(req.CategoryIdList)
	var updateRawList []string
	for _, categoryId := range req.CategoryIdList {
		updateRawList = append(updateRawList, fmt.Sprintf("WHEN %d THEN %d", categoryId, cnt))
		cnt--
	}
	_, err = UserCategory.WhereCorpApp(user.CorpId, user.AppId).WhereIn(DbId, req.CategoryIdList).Update(ctx, map[string]interface{}{
		DbViewOrder: dbproxy.Expr(fmt.Sprintf("CASE %s %s END", DbId, strings.Join(updateRawList, " "))),
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	return &rsp, nil
}

func GetUserPermissionSettingSys(ctx *rpc.Context, req *qmsg.GetUserPermissionSettingSysReq) (*qmsg.GetUserPermissionSettingSysRsp, error) {
	var rsp qmsg.GetUserPermissionSettingSysRsp
	user, err := User.get(ctx, req.CorpId, req.AppId, req.Uid)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	userCategory, err := getUserCategory(ctx, user)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	if userCategory.PermissionType == uint32(qmsg.ModelUserCategory_PermissionTypeUser) {
		rsp.UseQmsgPermission = false
		return &rsp, nil
	}
	rsp.UseQmsgPermission = true
	if userCategory.PermissionData != nil {
		if userCategory.PermissionData.SelectAllMaterialCategory {
			rsp.SelectAllMaterialCategory = true
		} else {
			rsp.MaterialCategoryIdList = userCategory.PermissionData.MaterialCategoryIdList
		}
		if userCategory.PermissionData.SelectAllSpeechCraftCategory {
			rsp.SelectAllSpeechCraftCategory = true
		} else {
			rsp.SpeechCraftCategoryIdList = userCategory.PermissionData.SpeechCraftCategoryIdList
		}
	}
	return &rsp, nil
}

func GetUserCategoryListSys(ctx *rpc.Context, req *qmsg.GetUserCategoryListSysReq) (*qmsg.GetUserCategoryListSysRsp, error) {
	var rsp qmsg.GetUserCategoryListSysRsp
	list, paginate, err := getUserCategoryList(ctx, req.CorpId, req.AppId, req.ListOption)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	rsp.List = list
	rsp.Paginate = paginate
	return &rsp, nil
}

func AddUserCategorySys(ctx *rpc.Context, req *qmsg.AddUserCategorySysReq) (*qmsg.AddUserCategorySysRsp, error) {
	var rsp qmsg.AddUserCategorySysRsp
	categoryId, err := addUserCategory(ctx, req.CorpId, req.AppId, req.Category)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	rsp.Id = categoryId
	return &rsp, nil
}

func SetUserCategorySys(ctx *rpc.Context, req *qmsg.SetUserCategorySysReq) (*qmsg.SetUserCategorySysRsp, error) {
	var rsp qmsg.SetUserCategorySysRsp
	_, err := setUserCategory(ctx, req.CorpId, req.AppId, req.Category)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	return &rsp, nil
}

func BatchDelUserCategorySys(ctx *rpc.Context, req *qmsg.BatchDelUserCategorySysReq) (*qmsg.BatchDelUserCategorySysRsp, error) {
	var rsp qmsg.BatchDelUserCategorySysRsp
	err := batchDeleteUserCategory(ctx, req.CorpId, req.AppId, req.IdList)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	return &rsp, nil
}

func getUserCategoryList(ctx *rpc.Context, corpId, appId uint32, listOption *core.ListOption) ([]*qmsg.ModelUserCategory, *core.Paginate, error) {
	db := UserCategory.WhereCorpApp(corpId, appId).OrderDesc(DbViewOrder)
	if listOption != nil {
		db.SetLimit(listOption.Limit).SetOffset(listOption.Offset)
		err := core.NewListOptionProcessor(listOption).
			AddString(qmsg.GetUserCategoryListReq_ListOptionNameLike, func(val string) error {
				db.WhereLike(DbName, "%"+val+"%")
				return nil
			}).
			AddUint64List(qmsg.GetUserCategoryListReq_ListOptionIdList, func(valList []uint64) error {
				db.WhereIn(DbId, valList)
				return nil
			}).
			Process()
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, nil, err
		}

	}
	var list []*qmsg.ModelUserCategory
	paginate, err := db.FindPaginate(ctx, &list)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, nil, err
	}
	count, err := db.Count(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, nil, err
	}
	paginate.Total = count
	return list, paginate, nil
}

func addUserCategory(ctx *rpc.Context, corpId, appId uint32, category *qmsg.ModelUserCategory) (uint64, error) {
	category.CorpId = corpId
	category.AppId = appId
	category.ViewOrder = utils.Now()
	err := UserCategory.NewScope().Create(ctx, category)
	if err != nil {
		if utils.IsMysqlUniqueIndexConflictError(err) {
			return 0, rpc.CreateError(qmsg.ErrUserCategoryExists)
		}
		log.Errorf("err:%v", err)
		return 0, err
	}
	return category.Id, nil
}

func setUserCategory(ctx *rpc.Context, corpId, appId uint32, category *qmsg.ModelUserCategory) (uint64, error) {
	if category == nil || category.Id == 0 {
		return 0, rpc.InvalidArg("invalid data")
	}
	_, err := UserCategory.WhereCorpApp(corpId, appId).Where(DbId, category.Id).Omit(DbCorpId, DbAppId).Save(ctx, category)
	if err != nil {
		if utils.IsMysqlUniqueIndexConflictError(err) {
			return 0, rpc.CreateError(qmsg.ErrUserCategoryExists)
		}
		log.Errorf("err:%v", err)
		return 0, err
	}
	return category.Id, nil
}

func batchDeleteUserCategory(ctx *rpc.Context, corpId, appId uint32, idList []uint64) error {
	_, err := User.WhereCorpApp(corpId, appId).WhereIn(DbUserCategoryId, idList).Update(ctx, map[string]interface{}{
		DbUserCategoryId: 0,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	_, err = UserCategory.WhereCorpApp(corpId, appId).WhereIn(DbId, idList).Delete(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	return nil

}
