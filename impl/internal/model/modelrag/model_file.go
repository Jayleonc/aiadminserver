package modelrag

import (
	"git.pinquest.cn/base/aiadmin"
	"git.pinquest.cn/base/aiadminserver/impl/internal/model"
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/dbx"
	"git.pinquest.cn/qlb/brick/rpc"
	"time"
)

// === RagFile 表 ===

type TFile struct {
	model.TModel
}

var ModelFile = &TFile{
	TModel: model.TModel{
		Model: dbx.NewModel(&dbx.ModelConfig{
			Type:            &aiadmin.ModelFile{},
			NotFoundErrCode: 666,
		}),
	},
}

func (m *TFile) Insert(ctx *rpc.Context, file *aiadmin.ModelFile) error {
	file.CreatedAt = uint32(time.Now().Unix())
	file.UpdatedAt = file.CreatedAt
	return m.Model.Create(ctx, file)
}

func (m *TFile) UpdateStatus(ctx *rpc.Context, fileId uint64, status uint32, errorMsg string) error {
	_, err := m.Model.Where(map[string]interface{}{
		"id": fileId,
	}).Update(ctx, map[string]interface{}{
		"status":     status,
		"error_msg":  errorMsg,
		"updated_at": time.Now().Unix(),
	})
	if err != nil {
		return err
	}
	return nil
}

func (m *TFile) GetById(ctx *rpc.Context, fileId uint64) (*aiadmin.ModelFile, error) {
	var record aiadmin.ModelFile
	err := m.Where(map[string]interface{}{
		"id": fileId,
	}).First(ctx, &record)
	return &record, err
}

func (m *TFile) UpdateData(ctx *rpc.Context, fileId uint64, data map[string]interface{}) error {
	// 默认加更新时间字段
	if data != nil {
		data["updated_at"] = time.Now().Unix()
	}

	_, err := m.Model.Where(map[string]interface{}{
		"id": fileId,
	}).Update(ctx, data)

	return err
}

func (m *TFile) UpdateDataInTx(ctx *rpc.Context, tx *dbx.Tx, fileId uint64, data map[string]interface{}) error {
	if data != nil {
		data["updated_at"] = time.Now().Unix()
	}

	_, err := m.Model.NewScope().Transaction(tx.GetTransactionId()).
		Where(map[string]interface{}{"id": fileId}).
		Update(ctx, data)
	if err != nil {
		log.Errorf("UpdateDataInTx file id=%d failed: %v", fileId, err)
	}
	return err
}

func (m *TFile) DeleteByDatasetId(ctx *rpc.Context, datasetId uint64) error {
	_, err := m.Where(map[string]interface{}{
		"used_in_dataset_id": datasetId,
	}).Delete(ctx)
	return err
}

// DeleteByDatasetIdInTx 在指定事务中删除某个数据集下的所有文件
func (m *TFile) DeleteByDatasetIdInTx(ctx *rpc.Context, tx *dbx.Tx, corpId uint32, datasetId uint64) error {
	_, err := m.NewScope().Transaction(tx.GetTransactionId()).WhereCorp(corpId).
		Where("used_in_dataset_id = ?", datasetId).
		Delete(ctx)
	if err != nil {
		log.Errorf("Delete files for dataset %d in transaction failed: %v", datasetId, err)
	}
	return err
}
