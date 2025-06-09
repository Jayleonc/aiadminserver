package impl

import (
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/dbx"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/qmsg"
)

type TUserCategory struct {
	TModel
}

var UserCategory = &TUserCategory{
	TModel: TModel{
		Model: dbx.NewModel(&dbx.ModelConfig{
			Type:            &qmsg.ModelUserCategory{},
			NotFoundErrCode: qmsg.ErrUserCategoryNotFound,
		}),
	},
}

func getUserCategory(ctx *rpc.Context, user *qmsg.ModelUser) (*qmsg.ModelUserCategory, error) {
	if user.UserCategoryId == 0 {
		return &qmsg.ModelUserCategory{
			CorpId:         user.CorpId,
			AppId:          user.AppId,
			Name:           "默认分组",
			PermissionType: uint32(qmsg.ModelUserCategory_PermissionTypeUser),
			PermissionData: &qmsg.ModelUserCategory_PermissionData{},
		}, nil
	}
	var userCategory qmsg.ModelUserCategory
	err := UserCategory.WhereCorpApp(user.CorpId, user.AppId).Where(DbId, user.UserCategoryId).First(ctx, &userCategory)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	return &userCategory, nil
}
