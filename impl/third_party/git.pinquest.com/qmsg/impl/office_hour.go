package impl

import (
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/brick/utils"
	"git.pinquest.cn/qlb/core"
	"git.pinquest.cn/qlb/qmsg"
)

func ResetOfficeHour(ctx *rpc.Context, req *qmsg.ResetOfficeHourReq) (*qmsg.ResetOfficeHourRsp, error) {
	var rsp qmsg.ResetOfficeHourRsp

	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)

	var saveIdList []uint64
	var batchCreate []*qmsg.ModelOfficeHour

	for _, hour := range req.OfficeHourList {
		if hour.Id == 0 {
			batchCreate = append(batchCreate, hour)
		} else {
			saveIdList = append(saveIdList, hour.Id)

			// 这里与前端协商好，为 1 才更新
			if hour.UpdatedAt == 1 {
				_, err := OfficeHour.WhereCorpApp(corpId, appId).Where(DbId, hour.Id).Update(ctx, map[string]interface{}{
					DbWeekList: hour.WeekList,
					DbStartAt:  hour.StartAt,
					DbEndAt:    hour.EndAt,
				})
				if err != nil {
					log.Errorf("err:%v", err)
					return nil, err
				}
			}
		}
	}

	// 查询本次操作将被删除的工作时间
	var deleteList []*qmsg.ModelOfficeHour
	err := OfficeHour.WhereCorpApp(corpId, appId).WhereNotIn(DbId, saveIdList).Find(ctx, &deleteList)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	deleteIdList := utils.PluckUint64(deleteList, "Id")
	// 删除工作时间
	_, err = OfficeHour.WhereCorpApp(corpId, appId).WhereIn(DbId, deleteIdList).Delete(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	// 删除被客服选择的工作时间
	_, err = UserOfficeHour.WhereCorpApp(corpId, appId).WhereIn(DbOfficeHourId, deleteIdList).Delete(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	// 保存新增的工作时间
	err = OfficeHour.SetCorpApp(corpId, appId).BatchCreate(ctx, batchCreate)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	return &rsp, nil
}

func GetOfficeHourList(ctx *rpc.Context, req *qmsg.GetOfficeHourListReq) (*qmsg.GetOfficeHourListRsp, error) {
	var rsp qmsg.GetOfficeHourListRsp

	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)

	err := OfficeHour.WhereCorpApp(corpId, appId).Find(ctx, &rsp.List)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	return &rsp, nil
}

func GetUserOfficeHourList(ctx *rpc.Context, req *qmsg.GetUserOfficeHourListReq) (*qmsg.GetUserOfficeHourListRsp, error) {
	var rsp qmsg.GetUserOfficeHourListRsp

	user, err := getUserCheck(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	err = UserOfficeHour.WhereCorpApp(user.CorpId, user.AppId).Where(DbUid, user.Id).Find(ctx, &rsp.UserOfficeHourList)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	var officeHourList []*qmsg.ModelOfficeHour
	err = OfficeHour.WithTrash().WhereCorpApp(user.CorpId, user.AppId).Find(ctx, &officeHourList)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	rsp.OfficeHourMap = map[uint64]*qmsg.ModelOfficeHour{}
	utils.KeyBy(officeHourList, "Id", &rsp.OfficeHourMap)

	return &rsp, nil
}

func ResetUserOfficeHour(ctx *rpc.Context, req *qmsg.ResetUserOfficeHourReq) (*qmsg.ResetUserOfficeHourRsp, error) {
	var rsp qmsg.ResetUserOfficeHourRsp

	user, err := getUserCheck(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	if req.IsUseOfficeHour {
		// 删除没有选择的时间配置
		_, err = UserOfficeHour.WhereCorpApp(user.CorpId, user.AppId).Where(DbUid, user.Id).WhereNotIn(DbOfficeHourId, req.OfficeHourIdList).Delete(ctx)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}

		for _, id := range req.OfficeHourIdList {
			_, err = UserOfficeHour.FirstOrCreate(ctx, map[string]interface{}{
				DbCorpId:       user.CorpId,
				DbAppId:        user.AppId,
				DbUid:          user.Id,
				DbOfficeHourId: id,
			}, map[string]interface{}{}, &qmsg.ModelUserOfficeHour{})
			if err != nil {
				log.Errorf("err:%v", err)
			}
		}
	} else {
		// 删除所有已选择的时间配置
		_, err := UserOfficeHour.WhereCorpApp(user.CorpId, user.AppId).Where(DbUid, user.Id).Delete(ctx)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
	}

	_, err = User.WhereCorpApp(user.CorpId, user.AppId).Where(DbId, user.Id).Update(ctx, map[string]interface{}{
		DbIsUseOfficeHour:       req.IsUseOfficeHour,
		DbIsAutoChangeWorkState: req.IsAutoChangeWorkState,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	return &rsp, nil
}
