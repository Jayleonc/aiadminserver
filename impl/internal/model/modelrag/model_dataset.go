package modelrag

import (
	"fmt"
	"git.pinquest.cn/base/aiadmin"
	"git.pinquest.cn/base/aiadminserver/impl/internal/model"
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/dbx"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/core"
	"time"
)

type TDataset struct {
	model.TModel
}

var ModelDataset = &TDataset{
	TModel: model.TModel{
		Model: dbx.NewModel(&dbx.ModelConfig{
			Type:            &aiadmin.ModelDataset{},
			NotFoundErrCode: aiadmin.ErrDatasetNotFound,
		}),
	},
}

func (m *TDataset) ListWithOption(ctx *rpc.Context, corpId uint32, req *aiadmin.GetDatasetListReq) ([]*aiadmin.ModelDataset, *core.Paginate, error) {
	option := req.ListOption
	db := m.WithTrash().NewList(option).WhereCorp(corpId)

	if req.OnlyMine {
		fmt.Println("only mine")
	}

	err := core.NewListOptionProcessor(option).
		AddString(int32(aiadmin.GetDatasetListReq_ListOptionNameLike), func(val string) error {
			val = fmt.Sprintf("%%%s%%", val)
			db.WhereLike("name", val)
			return nil
		}).
		AddBool(int32(aiadmin.GetDatasetListReq_ListOptionIsPublic), func(val bool) error {
			db.Where("is_public", val)
			return nil
		}).
		AddUint32(int32(aiadmin.GetDatasetListReq_ListOptionOrderBy), func(val uint32) error {
			switch val {
			case uint32(aiadmin.GetDatasetListReq_OrderByCreatedAtDesc):
				db.OrderDesc("created_at")
			case uint32(aiadmin.GetDatasetListReq_OrderByCreatedAtAsc):
				db.OrderAsc("created_at")
			}
			return nil
		}).
		Process()
	if err != nil {
		log.Errorf("Dataset ListWithOption processor error: %v", err)
		return nil, nil, err
	}

	var list []*aiadmin.ModelDataset
	paginate, err := db.FindPaginate(ctx, &list)
	if err != nil {
		log.Errorf("Dataset ListWithOption FindListPaginate error: %v", err)
		return nil, nil, err
	}
	return list, paginate, nil
}

// 获取单个 dataset
func (m *TDataset) GetById(ctx *rpc.Context, datasetId uint64) (*aiadmin.ModelDataset, error) {
	var dataset aiadmin.ModelDataset
	err := m.Where(map[string]interface{}{
		"id": datasetId,
	}).First(ctx, &dataset)
	if err != nil {
		log.Errorf("Get Dataset by ID error: %v", err)
	}
	return &dataset, err
}

// 根据 corp_id 和 dataset id 获取单个 dataset，确保数据属于当前企业
func (m *TDataset) GetByCorpIdAndId(ctx *rpc.Context, corpId uint32, datasetId uint64) (*aiadmin.ModelDataset, error) {
	var dataset aiadmin.ModelDataset
	err := m.WhereCorp(corpId).Where(map[string]interface{}{
		"id": datasetId,
	}).First(ctx, &dataset)
	if err != nil {
		if !m.IsNotFoundErr(err) {
			log.Errorf("Get Dataset by CorpId and ID error: %v", err)
		}
		return nil, err
	}
	return &dataset, nil
}

// 删除 dataset
//
//	func (m *TDataset) DeleteById(ctx *rpc.Context, corpId uint32, datasetId uint64) error {
//		_, err := m.NewScope().WhereCorp(corpId).Where(map[string]interface{}{
//			"id": datasetId,
//		}).Delete(ctx)
//		if err != nil {
//			log.Errorf("Delete Dataset error: %v", err)
//		}
//		return err
//	}
func (m *TDataset) DeleteById(ctx *rpc.Context, corpId uint32, datasetId uint64) error {
	_, err := m.NewScope().WhereCorp(corpId).Where(map[string]interface{}{
		"id": datasetId,
	}).Delete(ctx)
	if err != nil {
		log.Errorf("Delete Dataset error: %v", err)
	}
	return err
}

func (m *TDataset) DeleteByIdInTx(ctx *rpc.Context, tx *dbx.Tx, corpId uint32, datasetId uint64) error {
	_, err := m.Model.NewScope().Transaction(tx.GetTransactionId()).WhereCorp(corpId).
		Where("id = ?", datasetId).
		Delete(ctx)
	if err != nil {
		log.Errorf("Delete dataset %d in transaction failed: %v", datasetId, err)
	}
	return err
}

// 根据 corp_id 和 name 获取单个 dataset
func (m *TDataset) GetByCorpIdAndName(ctx *rpc.Context, corpId uint32, name string) (*aiadmin.ModelDataset, error) {
	var dataset aiadmin.ModelDataset
	err := m.Where(map[string]interface{}{
		"corp_id": corpId,
		"name":    name,
	}).First(ctx, &dataset)
	if err != nil {
		if !m.IsNotFoundErr(err) {
			log.Errorf("Get Dataset by CorpId and Name error: %v", err)
		}
		return nil, err
	}
	return &dataset, nil
}

// 创建 Dataset
func (m *TDataset) Create(ctx *rpc.Context, req *aiadmin.ModelDataset) (*aiadmin.ModelDataset, error) {
	now := time.Now().Unix()
	req.CreatedAt = uint32(now)
	req.UpdatedAt = uint32(now)

	err := m.Model.Create(ctx, req)
	if err != nil {
		log.Errorf("Create Dataset error: %v", err)
		return nil, err
	}

	return req, nil
}

// 根据 ID 更新 Dataset
func (m *TDataset) UpdateById(ctx *rpc.Context, datasetId uint64, data map[string]interface{}) (uint64, error) {
	data["updated_at"] = time.Now().Unix()
	rowsAffected, err := m.Where(map[string]interface{}{
		"id": datasetId,
	}).Update(ctx, data)
	if err != nil {
		log.Errorf("Update Dataset by ID error: %v", err)
	}
	return rowsAffected.RowsAffected, err
}

// 创建或更新 Dataset（根据 corp_id + name 唯一）
func (_ *TDataset) Edit(ctx *rpc.Context, req *aiadmin.ModelDataset) (*aiadmin.ModelDataset, error) {
	var record aiadmin.ModelDataset

	condition := map[string]interface{}{
		"corp_id": req.CorpId,
		"name":    req.Name,
	}

	data := map[string]interface{}{
		"robot_uid":                req.RobotUid,
		"name":                     req.Name,
		"description":              req.Description,
		"is_public":                req.IsPublic,
		"provider":                 req.Provider,
		"permission":               req.Permission,
		"data_source_type":         req.DataSourceType,
		"indexing_technique":       req.IndexingTechnique,
		"index_struct":             req.IndexStruct,
		"embedding_model":          req.EmbeddingModel,
		"embedding_model_provider": req.EmbeddingModelProvider,
		"retrieval_model":          req.RetrievalModel,
		"built_in_field_enabled":   req.BuiltInFieldEnabled,
		"collection_name":          req.CollectionName,
		"is_default":               req.IsDefault,
		"updated_at":               time.Now().Unix(),
	}

	_, err := ModelDataset.UpdateOrCreate(ctx, condition, data, &record)
	if err != nil {
		log.Errorf("UpdateOrCreate Dataset error: %v", err)
		return nil, err
	}
	return &record, nil
}

func (m *TDataset) CheckDefaultExists(ctx *rpc.Context, corpId uint32) (bool, error) {
	var dataset aiadmin.ModelDataset
	err := m.Where(map[string]interface{}{
		"corp_id":    corpId,
		"is_default": true,
	}).First(ctx, &dataset)
	if err != nil {
		if m.IsNotFoundErr(err) {
			// 没有找到符合条件的记录，即默认知识库不存在
			return false, nil
		}
		// 查询过程中发生其他错误
		log.Errorf("CheckDefaultExists Find error: %v", err)
		return false, err
	}
	// 如果没有发生错误，说明找到了至少一条符合条件的记录
	return true, nil
}
