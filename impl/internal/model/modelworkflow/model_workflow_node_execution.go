package modelworkflow

import (
	"git.pinquest.cn/base/aiadmin"
	"git.pinquest.cn/base/aiadminserver/impl/internal/model"
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/dbx"
	"git.pinquest.cn/qlb/brick/rpc"
	"time"
)

type TWorkflowNodeExecution struct {
	model.TModel
}

var ModelWorkflowNodeExecution = &TWorkflowNodeExecution{
	TModel: model.TModel{
		Model: dbx.NewModel(&dbx.ModelConfig{
			Type: &aiadmin.ModelWorkflowNodeExecution{},
			// NotFoundErrCode: aiadmin.ErrWorkflowNodeExecutionNotFound, // 定义一个新错误码
		}),
	},
}

// CreateInTx 使用事务创建 ModelWorkflowNodeExecution 记录
func (m *TWorkflowNodeExecution) CreateInTx(rpcCtx *rpc.Context, tx *dbx.Tx, exec *aiadmin.ModelWorkflowNodeExecution) error {
	now := uint32(time.Now().Unix())
	if exec.CreatedAtTs == 0 {
		exec.CreatedAtTs = now
	}
	if exec.UpdatedAtTs == 0 {
		exec.UpdatedAtTs = now
	}
	// exec.Id 应由 logger 提前生成
	err := m.NewScope().Transaction(tx.GetTransactionId()).Create(rpcCtx, exec)
	if err != nil {
		log.Errorf("TWorkflowNodeExecution.CreateInTx: failed to create ModelWorkflowNodeExecution with ID %s: %v", exec.Id, err)
	}
	return err
}

// UpdateInTx 使用事务根据 ID 更新 ModelWorkflowNodeExecution 记录的指定字段
func (m *TWorkflowNodeExecution) UpdateInTx(rpcCtx *rpc.Context, tx *dbx.Tx, id string, updates map[string]interface{}) error {
	if _, ok := updates["updated_at_ts"]; !ok {
		updates["updated_at_ts"] = uint32(time.Now().Unix())
	}
	_, err := m.NewScope().Transaction(tx.GetTransactionId()).Where("id = ?", id).Update(rpcCtx, updates)
	if err != nil {
		log.Errorf("TWorkflowNodeExecution.UpdateInTx: failed to update ModelWorkflowNodeExecution with ID %s: %v", id, err)
	}
	return err
}

// ListByWorkflowRunId 获取某次运行的所有节点执行记录 (按 index 升序)
func (m *TWorkflowNodeExecution) ListByWorkflowRunId(rpcCtx *rpc.Context, workflowRunId string) ([]*aiadmin.ModelWorkflowNodeExecution, error) {
	var list []*aiadmin.ModelWorkflowNodeExecution
	// corpId 等筛选条件已经在 workflow_run_id 中隐含，这里直接用 workflow_run_id 查询
	err := m.Where("workflow_run_id = ?", workflowRunId).OrderAsc("`index`").Find(rpcCtx, &list)
	if err != nil {
		log.Errorf("ModelWorkflowNodeExecution.ListByWorkflowRunId: Find error for run_id %s: %v", workflowRunId, err)
		return nil, err
	}
	return list, nil
}
