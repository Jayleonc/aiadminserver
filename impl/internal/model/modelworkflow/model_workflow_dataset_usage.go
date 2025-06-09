package modelworkflow

import (
	"git.pinquest.cn/base/aiadmin"
	"git.pinquest.cn/base/aiadminserver/impl/internal/model"
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/dbx"
	"git.pinquest.cn/qlb/brick/rpc"
)

// TWorkflowDatasetUsage 工作流数据集使用关系模型
type TWorkflowDatasetUsage struct {
	model.TModel
}

// ModelWorkflowDatasetUsage 工作流数据集使用关系模型实例
var ModelWorkflowDatasetUsage = &TWorkflowDatasetUsage{
	TModel: model.TModel{
		Model: dbx.NewModel(&dbx.ModelConfig{
			Type: &aiadmin.ModelWorkflowDatasetUsage{},
			//NotFoundErrCode: aiadmin.ErrWorkflowDatasetUsageNotFound,
		}),
	},
}

// CreateInTx 在事务中创建工作流数据集使用关系记录
func (m *TWorkflowDatasetUsage) CreateInTx(ctx *rpc.Context, tx *dbx.Tx, usage *aiadmin.ModelWorkflowDatasetUsage) error {
	err := m.NewScope().Transaction(tx.GetTransactionId()).Create(ctx, usage)
	if err != nil {
		log.Errorf("Create WorkflowDatasetUsage in transaction failed: %v", err)
	}
	return err
}

// BatchCreateInTx 在事务中批量创建工作流数据集使用关系记录
func (m *TWorkflowDatasetUsage) BatchCreateInTx(ctx *rpc.Context, tx *dbx.Tx, usages []*aiadmin.ModelWorkflowDatasetUsage) error {
	if len(usages) == 0 {
		return nil
	}

	err := m.NewScope().Transaction(tx.GetTransactionId()).BatchCreate(ctx, usages)
	if err != nil {
		log.Errorf("Batch create WorkflowDatasetUsage in transaction failed: %v", err)
	}
	return err
}

// ListByDatasetId 根据数据集ID查询使用该数据集的工作流列表
func (m *TWorkflowDatasetUsage) ListByDatasetId(ctx *rpc.Context, datasetId uint64) ([]*aiadmin.ModelWorkflowDatasetUsage, error) {
	var usages []*aiadmin.ModelWorkflowDatasetUsage
	err := m.Where("dataset_id = ?", datasetId).Find(ctx, &usages)
	if err != nil {
		if !m.IsNotFoundErr(err) {
			log.Errorf("List WorkflowDatasetUsage by dataset_id %d failed: %v", datasetId, err)
		}
		return nil, err
	}
	return usages, nil
}

// DeleteByWorkflowIdInTx 在事务中删除指定工作流ID的所有数据集使用关系
func (m *TWorkflowDatasetUsage) DeleteByWorkflowIdInTx(ctx *rpc.Context, tx *dbx.Tx, workflowId string) error {
	_, err := m.NewScope().Transaction(tx.GetTransactionId()).
		Where("workflow_id = ?", workflowId).
		Delete(ctx)
	if err != nil {
		log.Errorf("Delete WorkflowDatasetUsage by workflow_id %s in transaction failed: %v", workflowId, err)
	}
	return err
}

// ListByWorkflowId 根据工作流ID查询该工作流使用的所有数据集
func (m *TWorkflowDatasetUsage) ListByWorkflowId(ctx *rpc.Context, workflowId string) ([]*aiadmin.ModelWorkflowDatasetUsage, error) {
	var usages []*aiadmin.ModelWorkflowDatasetUsage
	err := m.Where("workflow_id = ?", workflowId).Find(ctx, &usages)
	if err != nil {
		if !m.IsNotFoundErr(err) {
			log.Errorf("List WorkflowDatasetUsage by workflow_id %s failed: %v", workflowId, err)
		}
		return nil, err
	}
	return usages, nil
}
