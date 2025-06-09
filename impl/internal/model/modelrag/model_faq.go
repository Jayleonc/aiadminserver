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

type TFAQ struct {
	model.TModel
}

var ModelFAQ = &TFAQ{
	TModel: model.TModel{
		Model: dbx.NewModel(&dbx.ModelConfig{
			Type:            &aiadmin.ModelFAQ{},
			NotFoundErrCode: aiadmin.ErrFAQNotFound,
		}),
	},
}

// 根据 ID 获取 FAQ
func (m *TFAQ) GetById(ctx *rpc.Context, id uint64) (*aiadmin.ModelFAQ, error) {
	var record aiadmin.ModelFAQ
	err := m.Where(map[string]interface{}{
		"id": id,
	}).First(ctx, &record)
	if err != nil {
		log.Errorf("Get FAQ by ID error: %v", err)
	}
	return &record, err
}

// 根据 ID 获取 FAQ
func (m *TFAQ) GetByDatasetIdAndId(ctx *rpc.Context, corpId uint32, datasetId uint64, id uint64) (*aiadmin.ModelFAQ, error) {
	var record aiadmin.ModelFAQ
	err := m.WhereCorp(corpId).Where(map[string]interface{}{
		"id":         id,
		"dataset_id": datasetId,
	}).First(ctx, &record)
	if err != nil {
		log.Errorf("Get FAQ by ID error: %v", err)
	}
	return &record, err
}

// 批量插入 FAQ（每条 FAQ 必须填好 embedding_id、dataset_id 等）
func (m *TFAQ) InsertMulti(ctx *rpc.Context, faqs []*aiadmin.ModelFAQ) error {
	if len(faqs) == 0 {
		return nil
	}
	err := m.BatchCreate(ctx, faqs)
	if err != nil {
		log.Errorf("InsertMulti FAQ error: %v", err)
	}
	return err
}

func (m *TFAQ) InsertMultiInTx(ctx *rpc.Context, tx *dbx.Tx, faqs []*aiadmin.ModelFAQ) error {
	if len(faqs) == 0 {
		return nil
	}
	err := m.Model.NewScope().Transaction(tx.GetTransactionId()).BatchCreate(ctx, faqs)
	if err != nil {
		log.Errorf("InsertMultiInTx FAQ error: %v", err)
	}
	return err
}

// CreateInTx 在事务中创建 FAQ 记录
func (m *TFAQ) CreateInTx(ctx *rpc.Context, tx *dbx.Tx, data *aiadmin.ModelFAQ) error {
	now := uint32(time.Now().Unix())
	data.CreatedAt = now
	data.UpdatedAt = now

	if err := m.NewScope().Transaction(tx.GetTransactionId()).
		Create(ctx, data); err != nil {
		log.Errorf("CreateInTx FAQ error:%v", err)
		return err
	}
	return nil
}

// 更新某 FAQ 的状态（如 completed/failed）
func (m *TFAQ) UpdateStatus(ctx *rpc.Context, id uint64, status uint32, errMsg string) error {
	data := map[string]interface{}{
		"status":     status,
		"error_msg":  errMsg,
		"updated_at": time.Now().Unix(),
	}
	_, err := m.Where(map[string]interface{}{
		"id": id,
	}).Update(ctx, data)
	if err != nil {
		log.Errorf("UpdateStatus FAQ error: %v", err)
	}
	return err
}

// 查询指定数据集下所有 pending 的 FAQ（待向量化）
func (m *TFAQ) ListPendingFAQByDataset(ctx *rpc.Context, datasetId uint64) ([]*aiadmin.ModelFAQ, error) {
	var list []*aiadmin.ModelFAQ
	err := m.Where(map[string]interface{}{
		"dataset_id": datasetId,
		"status":     "pending",
		"enabled":    true,
	}).Find(ctx, &list)
	if err != nil {
		log.Errorf("ListPendingFAQByDataset error: %v", err)
	}
	return list, err
}

func (m *TFAQ) GetByContentHash(ctx *rpc.Context, corpId uint32, datasetId uint64, hash string) (*aiadmin.ModelFAQ, error) {
	var record aiadmin.ModelFAQ
	err := m.WhereCorp(corpId).
		Where("dataset_id = ?", datasetId).
		Where("content_hash = ?", hash).
		First(ctx, &record)
	if err != nil {
		return nil, err
	}
	return &record, nil
}

func (m *TFAQ) BatchUpdate(ctx *rpc.Context, faqList []*aiadmin.ModelFAQ) error {
	for _, item := range faqList {
		if item.Id == 0 {
			continue
		}

		updateMap := make(map[string]interface{})
		// 按需添加需要更新的字段，这里举例常见字段
		if item.Answer != "" {
			updateMap["answer"] = item.Answer
		}
		updateMap["updated_at"] = item.UpdatedAt

		if len(updateMap) == 0 {
			continue
		}

		_, err := m.Model.Where(map[string]interface{}{
			"id": item.Id,
		}).Update(ctx, updateMap)

		if err != nil {
			log.Errorf("update faq id=%d failed: %v", item.Id, err)
			return err
		}
	}
	return nil
}

func (m *TFAQ) BatchUpdateInTx(ctx *rpc.Context, tx *dbx.Tx, faqList []*aiadmin.ModelFAQ) error {
	for _, item := range faqList {
		if item.Id == 0 {
			continue
		}

		updateMap := make(map[string]interface{})
		if item.Answer != "" {
			updateMap["answer"] = item.Answer
		}
		updateMap["updated_at"] = item.UpdatedAt

		if len(updateMap) == 0 {
			continue
		}

		_, err := m.Model.NewScope().Transaction(tx.GetTransactionId()).
			Where(map[string]interface{}{"id": item.Id}).
			Update(ctx, updateMap)
		if err != nil {
			log.Errorf("BatchUpdateInTx update faq id=%d failed: %v", item.Id, err)
			return err
		}
	}
	return nil
}

func (m *TFAQ) DeleteById(ctx *rpc.Context, corpId uint32, faqId uint64) error {
	_, err := m.WhereCorp(corpId).
		Where("id = ?", faqId).
		Delete(ctx)
	if err != nil {
		log.Errorf("DeleteById FAQ error: %v", err)
	}
	return err
}

func (m *TFAQ) DeleteByIdInTx(ctx *rpc.Context, tx *dbx.Tx, corpId uint32, faqId uint64) error {
	_, err := m.NewScope().
		Transaction(tx.GetTransactionId()).
		WhereCorp(corpId).
		Where("id = ?", faqId).
		Delete(ctx)
	return err
}

// DeleteByDatasetIdInTx 在指定事务中删除某个数据集下的所有 FAQ
func (m *TFAQ) DeleteByDatasetIdInTx(ctx *rpc.Context, tx *dbx.Tx, corpId uint32, datasetId uint64) error {
	// 使用传入的 tx 获取事务ID，并绑定到 Scope
	_, err := m.NewScope().Transaction(tx.GetTransactionId()).WhereCorp(corpId).
		Where("dataset_id = ?", datasetId).
		Delete(ctx)
	if err != nil {
		log.Errorf("Delete FAQs for dataset %d in transaction failed: %v", datasetId, err)
	}
	return err
}

func (m *TFAQ) ListWithOption(ctx *rpc.Context, corpId uint32, req *aiadmin.GetFAQListReq) ([]*aiadmin.ModelFAQ, *core.Paginate, error) {
	option := req.ListOption
	db := m.WithTrash().NewList(option).WhereCorp(corpId).Where("dataset_id", req.DatasetId)

	err := core.NewListOptionProcessor(option).
		AddString(int32(aiadmin.GetFAQListReq_ListOptionKeyword), func(val string) error {
			val = fmt.Sprintf("%%%s%%", val)
			db.Where("(question LIKE ? OR answer LIKE ?)", val, val)
			return nil
		}).
		AddUint64(int32(aiadmin.GetFAQListReq_ListOptionCategoryId), func(val uint64) error {
			db.Where("category_id", val)
			return nil
		}).
		AddBool(int32(aiadmin.GetFAQListReq_ListOptionEnabled), func(val bool) error {
			db.Where("enabled", val)
			return nil
		}).
		AddUint32(int32(aiadmin.GetFAQListReq_ListOptionOrderBy), func(val uint32) error {
			switch val {
			case uint32(aiadmin.GetFAQListReq_OrderByCreatedAtDesc):
				db.OrderDesc("created_at")
			case uint32(aiadmin.GetFAQListReq_OrderByCreatedAtAsc):
				db.OrderAsc("created_at")
			case uint32(aiadmin.GetFAQListReq_OrderByUpdatedAtDesc):
				db.OrderDesc("updated_at")
			case uint32(aiadmin.GetFAQListReq_OrderByUpdatedAtAsc):
				db.OrderAsc("updated_at")
			}
			return nil
		}).
		Process()

	if err != nil {
		return nil, nil, err
	}

	var list []*aiadmin.ModelFAQ
	paginate, err := db.FindPaginate(ctx, &list)
	if err != nil {
		return nil, nil, err
	}
	return list, paginate, nil
}

func (m *TFAQ) DeleteByDatasetId(ctx *rpc.Context, corpId uint32, datasetId uint64) error {
	_, err := m.WhereCorp(corpId).
		Where("dataset_id = ?", datasetId).
		Delete(ctx)
	if err != nil {
		log.Errorf("DeleteByDatasetId FAQ error: %v", err)
	}
	return err
}

// 根据categoryId删除分类，需传corpId和datasetId
func (m *TFAQ) DeleteByCategoryId(ctx *rpc.Context, corpId uint32, datasetId uint64, categoryId uint64) error {
	_, err := m.WhereCorp(corpId).
		Where("dataset_id = ?", datasetId).
		Where("category_id = ?", categoryId).
		Delete(ctx)
	if err != nil {
		log.Errorf("Delete FAQCategory failed: %v", err)
		return err
	}
	return nil
}

func (m *TFAQ) DeleteByCategoryIdInTx(ctx *rpc.Context, tx *dbx.Tx, corpId uint32, datasetId, categoryId uint64) error {
	_, err := m.NewScope().
		Transaction(tx.GetTransactionId()).
		WhereCorp(corpId).
		Where("dataset_id = ? AND category_id = ?", datasetId, categoryId).
		Delete(ctx)
	if err != nil {
		log.Errorf("Delete FAQs in tx failed: %v", err)
	}
	return err
}

func (m *TFAQ) ListByDatasetId(ctx *rpc.Context, corpId uint32, datasetId uint64) ([]*aiadmin.ModelFAQ, error) {
	var list []*aiadmin.ModelFAQ
	err := m.WhereCorp(corpId).Where("dataset_id = ?", datasetId).Find(ctx, &list)
	if err != nil {
		log.Errorf("ListByDatasetId FAQ error: %v", err)
	}
	return list, err
}

func (m *TFAQ) ListByCategoryId(ctx *rpc.Context, corpId uint32, datasetId uint64, categoryId uint64) ([]*aiadmin.ModelFAQ, error) {
	var list []*aiadmin.ModelFAQ
	err := m.WhereCorp(corpId).Where("dataset_id = ?", datasetId).Where("category_id = ?", categoryId).Find(ctx, &list)
	if err != nil {
		log.Errorf("ListByCategoryId FAQ error: %v", err)
	}
	return list, err
}

func (m *TFAQ) UpdateById(ctx *rpc.Context, corpId uint32, id uint64, updateMap map[string]interface{}) error {
	_, err := m.WhereCorp(corpId).
		Where("id = ?", id).
		Update(ctx, updateMap)
	if err != nil {
		log.Errorf("UpdateById FAQ error: %v", err)
		return nil
	}
	return nil
}

func (m *TFAQ) UpdateByIdInTx(ctx *rpc.Context, tx *dbx.Tx, corpId uint32, id uint64, updateMap map[string]interface{}) error {
	_, err := m.NewScope().
		Transaction(tx.GetTransactionId()).
		WhereCorp(corpId).
		Where("id = ?", id).
		Update(ctx, updateMap)
	if err != nil {
		log.Errorf("UpdateByIdInTx FAQ error: %v", err)
		return err
	}
	return nil
}
