package modelrag

import (
	"git.pinquest.cn/base/aiadmin"
	"git.pinquest.cn/base/aiadminserver/impl/internal/model"
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/dbx"
	"git.pinquest.cn/qlb/brick/rpc"
	"time"
)

// TFaqManualMedia model
type TFaqManualMedia struct {
	model.TModel
}

// ModelFaqManualMedia 实例
var ModelFaqManualMedia = &TFaqManualMedia{
	TModel: model.TModel{
		Model: dbx.NewModel(&dbx.ModelConfig{
			Type:            &aiadmin.ModelFAQManualMedia{},
			NotFoundErrCode: -1, // todo
		}),
	},
}

// Create 创建 FAQ 手动关联记录
func (m *TFaqManualMedia) Create(ctx *rpc.Context, req *aiadmin.ModelFAQManualMedia) error {
	now := time.Now().Unix()
	req.CreatedAt = uint32(now)
	req.UpdatedAt = uint32(now)

	err := m.Model.Create(ctx, req)
	if err != nil {
		log.Errorf("Create FAQ manual media error: %v", err)
		return err
	}
	return nil
}

func (m *TFaqManualMedia) CreateInTx(ctx *rpc.Context, tx *dbx.Tx, req *aiadmin.ModelFAQManualMedia) error {
	err := m.NewScope().Transaction(tx.GetTransactionId()).Create(ctx, req)
	if err != nil {
		log.Errorf("CreateInTx faq_manual_media error: %v", err)
		return err
	}
	return nil
}

// DeleteByFaqID 根据 FAQ ID 删除关联记录
func (m *TFaqManualMedia) DeleteByFaqID(ctx *rpc.Context, faqID uint64) error {
	_, err := m.Where(map[string]interface{}{
		"faq_id": faqID,
	}).Delete(ctx)
	if err != nil {
		log.Errorf("Delete FAQ manual media by faq_id error: %v", err)
		return err
	}
	return nil
}

func (m *TFaqManualMedia) DeleteByFaqIDInTx(ctx *rpc.Context, tx *dbx.Tx, faqId uint64) error {
	_, err := m.NewScope().
		Transaction(tx.GetTransactionId()).
		Where("faq_id = ?", faqId).Delete(ctx)
	return err
}

// ListByFaqID 根据 FAQ ID 获取关联的素材 IDs
func (m *TFaqManualMedia) ListByFaqID(ctx *rpc.Context, faqID uint64) ([]*aiadmin.ModelFAQManualMedia, error) {
	var list []*aiadmin.ModelFAQManualMedia
	err := m.Where(map[string]interface{}{
		"faq_id": faqID,
	}).Find(ctx, &list)
	if err != nil {
		log.Errorf("List FAQ manual media by faq_id error: %v", err)
		return nil, err
	}
	return list, nil
}

// TManualMaterial model
type TManualMaterial struct {
	model.TModel
}

// ModelManualMaterial 实例
var ModelManualMaterial = &TManualMaterial{
	TModel: model.TModel{
		Model: dbx.NewModel(&dbx.ModelConfig{
			Type:            &aiadmin.ModelManualMaterial{},
			NotFoundErrCode: -1, // todo
		}),
	},
}

// Create 创建手动上传素材记录
func (m *TManualMaterial) Create(ctx *rpc.Context, req *aiadmin.ModelManualMaterial) error {
	now := time.Now().Unix()
	req.CreatedAt = uint32(now)
	req.UpdatedAt = uint32(now)

	err := m.Model.Create(ctx, req)
	if err != nil {
		log.Errorf("Create manual material error: %v", err)
		return err
	}
	return nil
}

func (m *TManualMaterial) CreateInTx(ctx *rpc.Context, tx *dbx.Tx, req *aiadmin.ModelManualMaterial) error {
	err := m.NewScope().Transaction(tx.GetTransactionId()).Create(ctx, req)
	if err != nil {
		log.Errorf("CreateInTx manual material error: %v", err)
		return err
	}
	return nil
}

// GetByID 根据 ID 获取手动上传素材
func (m *TManualMaterial) GetByID(ctx *rpc.Context, id uint64) (*aiadmin.ModelManualMaterial, error) {
	var record aiadmin.ModelManualMaterial
	err := m.Where(map[string]interface{}{
		"id": id,
	}).First(ctx, &record)
	if err != nil {
		if !m.IsNotFoundErr(err) {
			log.Errorf("Get manual material by id error: %v", err)
		}
		return nil, err
	}
	return &record, nil
}

// DeleteByID 根据 ID 删除手动上传素材
func (m *TManualMaterial) DeleteByID(ctx *rpc.Context, id uint64) (uint64, error) {
	result, err := m.Where(map[string]interface{}{
		"id": id,
	}).Delete(ctx)
	if err != nil {
		log.Errorf("Delete manual material by id error: %v", err)
		return 0, err
	}
	return result.RowsAffected, nil
}
