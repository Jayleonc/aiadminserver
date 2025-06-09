package impl

import (
	"fmt"
	"git.pinquest.cn/qlb/brick/dbx"
	"git.pinquest.cn/qlb/qmsg"
	"strings"
)

type TMsgStatTimeOut struct {
	TModel
}

type TMsgStatTimeScope struct {
	TModel
}

var MsgStatTimeScope = &TMsgStatTimeScope{
	TModel: TModel{
		Model: dbx.NewModel(&dbx.ModelConfig{
			Type:            &qmsg.ModelMsgStatTimeScope{},
			NotFoundErrCode: qmsg.ErrMsgStatTimeScopeNotFound,
		}),
	},
}

func convertTimeScopeToDb(db *dbx.Scope, date uint32, timeScope *qmsg.ModelMsgStatTimeScope) *dbx.Scope {
	db.Where(DbDate, date)
	if timeScope.Config.StatTimeScopeType == uint32(qmsg.StatTimeScopeType_StatTimeScopeTypeSpecified) {
		var sqlArr []string
		for _, item := range timeScope.Config.TimeScopeList {
			sqlArr = append(sqlArr, fmt.Sprintf("(%s = %d and %s = %d)", DbStartAt, item.StartAt, DbEndAt, item.EndAt))
		}
		if len(sqlArr) > 0 {
			db.WhereRaw(strings.Join(sqlArr, " or "))
		}
	}
	return db
}
