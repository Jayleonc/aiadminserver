// impl/internal/model/modelworkflow/model_workflow_version.go
package modelworkflow

import (
	"git.pinquest.cn/base/aiadmin"
	"git.pinquest.cn/base/aiadminserver/impl/internal/model"
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/dbx"
	"git.pinquest.cn/qlb/brick/rpc"
)

type TWorkflowVersion struct {
	model.TModel
}

var ModelWorkflowVersion = &TWorkflowVersion{
	TModel: model.TModel{
		Model: dbx.NewModel(&dbx.ModelConfig{
			Type:            &aiadmin.ModelWorkflowVersion{},
			NotFoundErrCode: aiadmin.ErrWorkflowNotFound, // 可以定义一个新的错误码 ErrWorkflowVersionNotFound
		}),
	},
}

func (m *TWorkflowVersion) CreateInTx(ctx *rpc.Context, tx *dbx.Tx, version *aiadmin.ModelWorkflowVersion) error {
	return m.NewScope().Transaction(tx.GetTransactionId()).Create(ctx, version)
}

func (m *TWorkflowVersion) GetById(ctx *rpc.Context, versionId string) (*aiadmin.ModelWorkflowVersion, error) {
	var record aiadmin.ModelWorkflowVersion
	err := m.Where("id = ?", versionId).First(ctx, &record)
	if err != nil {
		if !m.IsNotFoundErr(err) {
			log.Errorf("Get WorkflowVersion by ID error: %v", err)
		}
		return nil, err
	}
	return &record, nil
}

// GetLatestVersionNumberByWorkflowId 获取指定工作流的最新版本号
func (m *TWorkflowVersion) GetLatestVersionNumberByWorkflowId(ctx *rpc.Context, tx *dbx.Tx, workflowId string) (int32, error) {
	var version aiadmin.ModelWorkflowVersion
	// 如果 tx 不为 nil，则在事务中查询
	scope := m.NewScope()
	if tx != nil {
		scope.Transaction(tx.GetTransactionId())
	}

	err := scope.Where("workflow_id = ?", workflowId).OrderDesc("version_number").First(ctx, &version)
	if err != nil {
		if m.IsNotFoundErr(err) {
			// 如果没有找到任何版本，可以认为是版本号0，或者根据业务返回特定错误/值
			return 0, nil
		}
		log.Errorf("GetLatestVersionNumberByWorkflowId for workflow %s error: %v", workflowId, err)
		return 0, err
	}
	return version.VersionNumber, nil
}

// 你可能还需要其他方法，比如 ListByWorkflowId 等
func (m *TWorkflowVersion) ListByWorkflowId(ctx *rpc.Context, workflowId string) ([]*aiadmin.ModelWorkflowVersion, error) {
	var list []*aiadmin.ModelWorkflowVersion
	err := m.Where("workflow_id = ?", workflowId).OrderDesc("version_number").Find(ctx, &list)
	if err != nil {
		log.Errorf("List WorkflowVersions by WorkflowId %s error: %v", workflowId, err)
		return nil, err
	}
	return list, nil
}

// DeleteByWorkflowIdInTx 删除指定工作流的所有版本记录
func (m *TWorkflowVersion) DeleteByWorkflowIdInTx(ctx *rpc.Context, tx *dbx.Tx, workflowId string) error {
	_, err := m.NewScope().Transaction(tx.GetTransactionId()).
		Where("workflow_id = ?", workflowId).
		Delete(ctx)
	if err != nil {
		log.Errorf("Delete versions by workflow_id %s in transaction failed: %v", workflowId, err)
	}
	return err
}

// ListByIds 根据一组版本ID批量获取版本信息
func (m *TWorkflowVersion) ListByIds(ctx *rpc.Context, ids []string) ([]*aiadmin.ModelWorkflowVersion, error) {
	var list []*aiadmin.ModelWorkflowVersion
	if len(ids) == 0 {
		return list, nil
	}
	err := m.Where("id IN (?)", ids).Find(ctx, &list) // 假设你的 dbx 库支持 IN 查询的这种占位符
	// 如果不支持，可能需要构建 "id = ? OR id = ? ..." 或者使用 dbx 提供的特定 IN 方法
	// 例如: err := m.In("id", ids...).Find(ctx, &list)
	if err != nil {
		log.Errorf("List WorkflowVersions by IDs error: %v, ids: %v", err, ids)
		return nil, err
	}
	return list, nil
}
