package impl

import (
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/core"
	"git.pinquest.cn/qlb/qmsg"
	"github.com/360EntSecGroup-Skylar/excelize/v2"
)

// MsgDataModel 用于会话质检-客服和成员的抽象方法
type MsgDataModel interface {
	// GetMsgInfoList pc-会话质检-数据明细
	GetMsgInfoList(ctx *rpc.Context, listOption *core.ListOption) ([]*qmsg.GetPersonMsgListRsp_PersonData, map[uint64]*qmsg.GetPersonMsgListRsp_PersonData, error)
	// GetMsgDetailsList pc-会话质检-数据明细-详情
	GetMsgDetailsList(ctx *rpc.Context, beginDate, endDate uint32, valUidList []uint64, fieldList []uint32) (map[uint64]*qmsg.GetPersonMsgListRsp_PersonData, []uint64, []uint64, []*qmsg.ModelRobotSingleStat, []*qmsg.ModelCustomerServiceSingleStat, error)
	// GetLineInfoList 获取折线图数据
	GetLineInfoList(ctx *rpc.Context, listOption *core.ListOption, msgStatTimeScope *qmsg.ModelMsgStatTimeScope) ([]*qmsg.GetLineChatDataListRsp_DataList, map[uint64]string, error)
	// ExportPersonMsg 导出数据
	ExportPersonMsg(ctx *rpc.Context, listOption *core.ListOption, uidList []uint64, f *excelize.File, titleList []interface{}) error
}

func GetMsgDataModelByType(ctx *rpc.Context, personType uint32, corpId, appId uint32) (MsgDataModel, error) {
	switch personType {
	case uint32(qmsg.GetLineChatDataListReq_PersonTypeRobot):
		return &MsgRobotModel{
			CorpId: corpId,
			AppId:  appId,
		}, nil
	case uint32(qmsg.GetLineChatDataListReq_PersonTypeCustomer):
		return &MsgCustomerModel{
			CorpId: corpId,
			AppId:  appId,
		}, nil
	default:
		return nil, rpc.InvalidArg("not a personType")
	}
}
