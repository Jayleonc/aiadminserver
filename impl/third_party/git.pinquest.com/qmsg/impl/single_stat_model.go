package impl

import (
	"git.pinquest.cn/qlb/brick/dbx"
	"git.pinquest.cn/qlb/qmsg"
)

type TRobotSingleStat struct {
	TModel
}
type TCustomerServiceSingleStat struct {
	TModel
}

var RobotSingleStat = &TRobotSingleStat{
	TModel: TModel{
		Model: dbx.NewModel(&dbx.ModelConfig{
			Type:            &qmsg.ModelRobotSingleStat{},
			NotFoundErrCode: qmsg.ErrRobotSingleStatNotFound,
		}),
	},
}
var CustomerServiceSingleStat = &TCustomerServiceSingleStat{
	TModel: TModel{
		Model: dbx.NewModel(&dbx.ModelConfig{
			Type:            &qmsg.ModelCustomerServiceSingleStat{},
			NotFoundErrCode: qmsg.ErrCustomerServiceSingleStatNotFound,
		}),
	},
}
