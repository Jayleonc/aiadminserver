package modelworkflow

import (
	"git.pinquest.cn/base/aiadmin"
	"git.pinquest.cn/base/aiadminserver/impl/internal/model"
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/dbx"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/core"
	"time"
)

type TWorkflowRun struct {
	model.TModel
}

var ModelWorkflowRun = &TWorkflowRun{
	TModel: model.TModel{
		Model: dbx.NewModel(&dbx.ModelConfig{
			Type:            &aiadmin.ModelWorkflowRun{},
			NotFoundErrCode: aiadmin.ErrWorkflowNotFound, // 可以定义一个更具体的 ErrWorkflowRunNotFound
		}),
	},
}

// CreateInTx 使用事务创建 ModelWorkflowRun 记录
// 注意：rpc.Context 参数主要用于传递上层信息，实际的事务绑定是通过 tx *dbx.Tx 实现的。
// Model 方法应避免直接依赖 rpc.Context 来获取事务，而是明确接收 *dbx.Tx。
func (m *TWorkflowRun) CreateInTx(rpcCtx *rpc.Context, tx *dbx.Tx, run *aiadmin.ModelWorkflowRun) error {
	// 确保 created_at_ts 和 updated_at_ts 被设置
	// started_at 和其他时间戳字段应由调用方（logger）设置
	now := uint32(time.Now().Unix())
	if run.CreatedAtTs == 0 {
		run.CreatedAtTs = now
	}
	if run.UpdatedAtTs == 0 {
		run.UpdatedAtTs = now
	}
	// 在 GORM 中，如果你在 Create 时没有指定主键，并且主键是数据库自增的，GORM 会自动处理。
	// 但如果主键是像 UUID 这样的由应用生成的字符串，你需要确保在调用 Create 之前它已经被填充。
	// 在我们的设计中，run.Id (WorkflowRunID) 是由 logger.LogContext 提前生成的。
	err := m.NewScope().Transaction(tx.GetTransactionId()).Create(rpcCtx, run) // rpcCtx 仅用于可能的拦截器或日志
	if err != nil {
		log.Errorf("TWorkflowRun.CreateInTx: failed to create ModelWorkflowRun with ID %s: %v", run.Id, err)
	}
	return err
}

// GetByIdInTx 使用事务根据 ID 获取 ModelWorkflowRun 记录
func (m *TWorkflowRun) GetByIdInTx(rpcCtx *rpc.Context, tx *dbx.Tx, id string) (*aiadmin.ModelWorkflowRun, error) {
	var record aiadmin.ModelWorkflowRun
	err := m.NewScope().Transaction(tx.GetTransactionId()).Where("id = ?", id).First(rpcCtx, &record)
	if err != nil {
		if !m.IsNotFoundErr(err) {
			log.Errorf("TWorkflowRun.GetByIdInTx: failed to get ModelWorkflowRun by ID %s: %v", id, err)
		}
		return nil, err
	}
	return &record, nil
}

// UpdateInTx 使用事务根据 ID 更新 ModelWorkflowRun 记录的指定字段
func (m *TWorkflowRun) UpdateInTx(rpcCtx *rpc.Context, tx *dbx.Tx, id string, updates map[string]interface{}) error {
	if _, ok := updates["updated_at_ts"]; !ok {
		updates["updated_at_ts"] = uint32(time.Now().Unix())
	}
	_, err := m.NewScope().Transaction(tx.GetTransactionId()).Where("id = ?", id).Update(rpcCtx, updates)
	if err != nil {
		log.Errorf("TWorkflowRun.UpdateInTx: failed to update ModelWorkflowRun with ID %s: %v", id, err)
	}
	return err
}

// ListWithOption 用于 GetWorkflowRunList 接口的实现 todo 还没写好
func (m *TWorkflowRun) ListWithOption(rpcCtx *rpc.Context, req *aiadmin.GetWorkflowRunListReq) ([]*aiadmin.ModelWorkflowRun, *core.Paginate, error) {
	// 确保从 rpcCtx 或 req 中获取 corpId
	corpId := req.CorpId // 假设 req 中有 CorpId，或者从 rpcCtx 获取
	// if corpId == 0 && rpcCtx != nil {
	//     corpId = core.MustGetPinHeaderCorpId2Uint32(rpcCtx) // 示例
	// }

	db := m.NewList(req.ListOption).Where("corp_id = ?", corpId)

	if req.AppId != "" { // app_id 在 proto 中是 string，但通常是 uuid，这里按 string 处理
		db.Where("app_id = ?", req.AppId)
	}
	if req.WorkflowId != "" {
		db.Where("workflow_id = ?", req.WorkflowId)
	}
	if req.Status != uint32(aiadmin.WorkflowRunStatus_WORKFLOW_RUN_STATUS_UNKNOWN) {
		db.Where("status = ?", req.Status)
	}
	if req.TriggeredFrom != "" {
		db.Where("triggered_from = ?", req.TriggeredFrom)
	}
	if req.Keyword != "" {
		keywordLike := "%" + req.Keyword + "%"
		// 注意：JSON 字段的搜索在 MySQL 中性能可能不佳，需要谨慎使用或考虑其他方案
		// inputs, outputs, error 字段是 JSON 字符串或 TEXT
		db.Where("(inputs LIKE ? OR outputs LIKE ? OR error LIKE ? OR created_by LIKE ?)", keywordLike, keywordLike, keywordLike, keywordLike)
	}
	if req.TimeStartMs > 0 {
		db.Where("started_at >= ?", req.TimeStartMs)
	}
	if req.TimeEndMs > 0 {
		db.Where("started_at <= ?", req.TimeEndMs)
	}

	// 默认按开始时间降序
	db.OrderDesc("started_at")

	var list []*aiadmin.ModelWorkflowRun
	paginate, err := db.FindPaginate(rpcCtx, &list)
	if err != nil {
		log.Errorf("ModelWorkflowRun.ListWithOption: FindPaginate error: %v", err)
		return nil, nil, err
	}
	return list, paginate, nil
}

// GetById 获取单个运行记录 (非事务)
func (m *TWorkflowRun) GetById(rpcCtx *rpc.Context, id string) (*aiadmin.ModelWorkflowRun, error) {
	var record aiadmin.ModelWorkflowRun
	err := m.Where("id = ?", id).First(rpcCtx, &record)
	if err != nil {
		if !m.IsNotFoundErr(err) {
			log.Errorf("TWorkflowRun.GetById: failed for ID %s: %v", id, err)
		}
		return nil, err
	}
	return &record, nil
}

func (m *TWorkflowRun) GetByWorkflowRunId(rpcCtx *rpc.Context, id string) (*aiadmin.ModelWorkflowRun, error) {
	var record aiadmin.ModelWorkflowRun
	err := m.Where("workflow_run_id = ?", id).First(rpcCtx, &record)
	if err != nil {
		if !m.IsNotFoundErr(err) {
			log.Errorf("TWorkflowRun.GetByWorkflowRunId: failed for ID %s: %v", id, err)
		}
		return nil, err
	}
	return &record, nil
}

func (m *TWorkflowRun) Create(rpcCtx *rpc.Context, run *aiadmin.ModelWorkflowRun) error {
	now := uint32(time.Now().Unix())
	if run.CreatedAtTs == 0 {
		run.CreatedAtTs = now
	}
	if run.UpdatedAtTs == 0 {
		run.UpdatedAtTs = now
	}

	// m.Model (即 dbx.Model) 的 Create 方法会检查 rpcCtx 是否已关联事务
	return m.Model.Create(rpcCtx, run)
}

func (m *TWorkflowRun) UpdateById(rpcCtx *rpc.Context, id string, updates map[string]interface{}) error {
	if _, ok := updates["updated_at_ts"]; !ok {
		updates["updated_at_ts"] = uint32(time.Now().Unix())
	}
	// m.Model 的 Where(...).Update(...) 也会检查 rpcCtx 是否已关联事务
	_, err := m.Model.Where("id = ?", id).Update(rpcCtx, updates)
	return err
}
