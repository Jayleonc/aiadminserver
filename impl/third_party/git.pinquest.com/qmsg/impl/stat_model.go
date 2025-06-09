package impl

import (
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/dbx"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/brick/utils"
	"git.pinquest.cn/qlb/qmsg"
	"time"
)

type TMsgStatMsgCache struct {
	TModel
}

var MsgStatMsgCache = &TMsgStatMsgCache{
	TModel: TModel{
		Model: dbx.NewModel(&dbx.ModelConfig{
			Type:            &qmsg.ModelMsgStatMsgCache{},
			NotFoundErrCode: qmsg.ErrMsgStatMsgCacheNotFound,
		}),
	},
}

type TMsgStatTimeOutDetail struct {
	TModel
}

var MsgStatTimeOutDetail = &TMsgStatTimeOutDetail{
	TModel: TModel{
		Model: dbx.NewModel(&dbx.ModelConfig{
			Type:            &qmsg.ModelMsgStatTimeOutDetail{},
			NotFoundErrCode: qmsg.ErrMsgStatTimeOutDetailNotFound,
		}),
	},
}

func (p *TMsgStatMsgCache) setCalTimeout(ctx *rpc.Context, corpId uint32, idList []uint64) (err error) {
	_, err = MsgStatMsgCache.WhereCorp(corpId).WhereIn(DbId, idList).Where(DbHasCalTimeout, false).Update(ctx, map[string]interface{}{
		DbHasCalTimeout:      true,
		DbHasCalFirstTimeout: true,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	return nil
}

func (p *TMsgStatMsgCache) setAssignModelCalTimeout(ctx *rpc.Context, corpId uint32, robotUid, customerAccountId, msgBoxId uint64) (err error) {
	_, err = MsgStatMsgCache.
		WhereCorp(corpId).
		Where(DbRobotUid, robotUid).
		Where(DbCustomerAccountId, customerAccountId).
		Where(DbMsgBoxId, msgBoxId).
		Where(DbHasCalTimeout, false).
		Update(ctx, map[string]interface{}{
			DbHasCalTimeout:      true,
			DbHasCalFirstTimeout: true,
		})
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	return nil
}

func (p *TMsgStatMsgCache) cleanOldData(ctx *rpc.Context) error {
	cleanTime := time.Now().AddDate(0, 0, -7).Unix()
	for {
		res, err := MsgStatMsgCache.NewScope().Lt(DbCreatedAt, cleanTime).SetLimit(1000).Delete(ctx)
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
		if res.RowsAffected == 0 {
			break
		}
	}
	return nil
}

func (p *TMsgStatTimeOutDetail) getTimeoutMsgBoxIdList(ctx *rpc.Context, corpId uint32, robotUid, customerAccountId uint64, msgBoxIdList []uint64) ([]uint64, error) {
	var list []*qmsg.ModelMsgStatTimeOutDetail
	scope := MsgStatTimeOutDetail.
		WhereCorp(corpId).
		Where(DbRobotUid, robotUid).
		Where(DbCustomerAccountId, customerAccountId)
	if len(msgBoxIdList) > 0 {
		scope.WhereIn(DbMsgBoxId, msgBoxIdList)
	}
	err := scope.Find(ctx, &list)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	return utils.PluckUint64(list, "MsgBoxId"), nil
}
