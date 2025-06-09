package modelrag

import (
	"git.pinquest.cn/base/aiadminserver/impl/internal/model"
	"time"

	"git.pinquest.cn/base/aiadmin"
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/dbx"
	"git.pinquest.cn/qlb/brick/rpc"
)

type TFAQCategory struct {
	model.TModel
}

var ModelFAQCategory = &TFAQCategory{
	TModel: model.TModel{
		Model: dbx.NewModel(&dbx.ModelConfig{
			Type: &aiadmin.ModelFAQCategory{},
		}),
	},
}

// 根据名称查询分类
func (m *TFAQCategory) GetByName(ctx *rpc.Context, corpId uint32, datasetId uint64, name string) (*aiadmin.ModelFAQCategory, error) {
	var record aiadmin.ModelFAQCategory
	err := m.WhereCorp(corpId).
		Where("dataset_id = ?", datasetId).
		Where("name = ?", name).
		First(ctx, &record)
	if err != nil {
		log.Warnf("FAQCategory not found: %v", err)
		return nil, err
	}
	return &record, nil
}

// 创建分类
func (m *TFAQCategory) Insert(ctx *rpc.Context, item *aiadmin.ModelFAQCategory) (uint64, error) {
	err := m.Create(ctx, item)
	if err != nil {
		log.Errorf("Insert FAQCategory failed: %v", err)
	}
	return item.Id, err
}

// 如果没有则创建
func (m *TFAQCategory) GetOrCreate(ctx *rpc.Context, corpId, robotId uint32, datasetId uint64, name string) (*aiadmin.ModelFAQCategory, error) {
	if name == "" {
		return nil, nil
	}
	item, err := m.GetByName(ctx, corpId, datasetId, name)
	if err == nil && item != nil {
		return item, nil
	}

	item = &aiadmin.ModelFAQCategory{
		CorpId:    corpId,
		RobotUid:  robotId,
		DatasetId: datasetId,
		Name:      name,
		CreatedAt: uint32(time.Now().Unix()),
		UpdatedAt: uint32(time.Now().Unix()),
	}

	_, err = m.Insert(ctx, item)
	if err != nil {
		return nil, err
	}
	return item, nil
}

// 根据categoryId删除分类，需传corpId和datasetId
func (m *TFAQCategory) DeleteById(ctx *rpc.Context, corpId uint32, datasetId uint64, categoryId uint64) error {
	_, err := m.WhereCorp(corpId).
		Where("dataset_id = ?", datasetId).
		Where("id = ?", categoryId).
		Delete(ctx)
	if err != nil {
		log.Errorf("Delete FAQCategory failed: %v", err)
		return err
	}
	return nil
}

func (m *TFAQCategory) DeleteByIdInTx(ctx *rpc.Context, tx *dbx.Tx, corpId uint32, datasetId, categoryId uint64) error {
	_, err := m.NewScope().
		Transaction(tx.GetTransactionId()).
		WhereCorp(corpId).
		Where("dataset_id = ?", datasetId).Where("id = ?", categoryId).Delete(ctx)
	if err != nil {
		log.Errorf("Delete FAQ category in tx failed: %v", err)
	}
	return err
}

func (m *TFAQCategory) List(ctx *rpc.Context, corpId uint32, datasetId uint64) ([]*aiadmin.ModelFAQCategory, error) {
	var list []*aiadmin.ModelFAQCategory
	err := m.WithTrash().WhereCorp(corpId).Where("dataset_id", datasetId).Find(ctx, &list)
	if err != nil {
		return nil, err
	}
	return list, nil
}

func (m *TFAQCategory) UpdateById(ctx *rpc.Context, corpId uint32, Id uint64, data map[string]interface{}) error {
	_, err := m.WhereCorp(corpId).
		Where("id = ?", Id).
		Update(ctx, data)
	if err != nil {
		log.Errorf("Update FAQCategory failed: %v", err)
		return err
	}
	return nil
}
