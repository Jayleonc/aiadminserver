package modelworkflow

import (
	"fmt"
	"git.pinquest.cn/base/aiadmin"
	"git.pinquest.cn/base/aiadminserver/impl/internal/model"
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/dbx"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/core"
)

type TWorkflow struct {
	model.TModel
}

var ModelWorkflow = &TWorkflow{
	TModel: model.TModel{
		Model: dbx.NewModel(&dbx.ModelConfig{
			Type:            &aiadmin.ModelWorkflow{},
			NotFoundErrCode: aiadmin.ErrWorkflowNotFound,
		}),
	},
}

func (m *TWorkflow) GetById(ctx *rpc.Context, id string) (*aiadmin.ModelWorkflow, error) {
	var record aiadmin.ModelWorkflow
	err := m.Where("id = ?", id).First(ctx, &record)
	if err != nil {
		if !m.IsNotFoundErr(err) {
			log.Errorf("Get Workflow by ID error: %v", err)
		}
		return nil, err
	}
	return &record, nil
}

func (m *TWorkflow) CreateInTx(ctx *rpc.Context, tx *dbx.Tx, wf *aiadmin.ModelWorkflow) error {
	return m.NewScope().Transaction(tx.GetTransactionId()).Create(ctx, wf)
}

// ListWithOption 仿照 GetFAQListReq 的最佳实践，处理工作流列表的查询和筛选
func (m *TWorkflow) ListWithOption(ctx *rpc.Context, corpId uint32, uid uint64, req *aiadmin.GetWorkflowListReq) ([]*aiadmin.ModelWorkflow, *core.Paginate, error) {
	// 1. 构建基础查询，带上分页和企业ID
	db := m.NewList(req.ListOption).WhereCorp(corpId)

	// 2. 处理顶层的 "only_mine" 筛选条件
	if req.OnlyMine {
		db.Where("created_by", uid)
	}

	// 3. 使用 ListOptionProcessor 处理来自 req.ListOption 的动态筛选条件
	err := core.NewListOptionProcessor(req.ListOption).
		// 处理名称模糊查询
		AddString(int32(aiadmin.GetWorkflowListReq_ListOptionNameLike), func(val string) error {
			if val != "" {
				db.WhereLike("name", "%"+val+"%")
			}
			return nil
		}).
		// 处理状态筛选
		AddUint32(int32(aiadmin.GetWorkflowListReq_ListOptionStatus), func(val uint32) error {
			// 假设 0 或未定义的枚举值代表“全部状态”，不进行筛选
			if val > 0 {
				db.Where("status", val)
			}
			return nil
		}).
		Process() // 执行处理器

	if err != nil {
		return nil, nil, err
	}

	// 4. 设置默认排序规则 (如果 ListOption 中没有指定排序)
	// GetWorkflowListReq 中没有定义排序选项，所以我们在这里加一个默认排序
	db.OrderDesc("updated_at")

	// 5. 执行查询并返回结果
	var list []*aiadmin.ModelWorkflow
	paginate, err := db.FindPaginate(ctx, &list)
	if err != nil {
		return nil, nil, err
	}
	return list, paginate, nil
}

// DeleteByIdInTx 在事务中删除主工作流记录
func (m *TWorkflow) DeleteByIdInTx(ctx *rpc.Context, tx *dbx.Tx, workflowId string) error {
	_, err := m.NewScope().Transaction(tx.GetTransactionId()).
		Where("id = ?", workflowId).
		Delete(ctx)
	if err != nil {
		log.Errorf("Delete Workflow by id %s in transaction failed: %v", workflowId, err)
	}
	return err
}

// UpdateByIdInTx 在事务中更新主工作流记录的指定字段
func (m *TWorkflow) UpdateByIdInTx(ctx *rpc.Context, tx *dbx.Tx, id string, data map[string]interface{}) error {
	// 确保 Update 操作在传入的事务中执行
	_, err := m.NewScope().Transaction(tx.GetTransactionId()).
		Where("id = ?", id).
		Update(ctx, data)
	if err != nil {
		log.Errorf("Update Workflow by id %s in transaction failed: %v, data: %v", id, err, data)
	}
	return err
}

// UpdateById 更新主工作流记录的指定字段
func (m *TWorkflow) UpdateById(ctx *rpc.Context, id string, data map[string]interface{}) (*dbx.UpdateResult, error) {
	// 基础模型通常已经有 Update 方法，这里确保签名和错误处理符合需要
	// 如果 TModel 中没有 Update 方法，可以这样实现:
	result, err := m.NewScope().Where("id = ?", id).Update(ctx, data)
	if err != nil {
		log.Errorf("Update Workflow by id %s failed: %v", id, err)
		return nil, err
	}
	return &result, nil
}

// CountEnabledWorkflowsByIds 计算给定的 workflow ID 列表中有多少是启用状态的
// tx *dbx.Tx 如果不为 nil，则在此事务中查询
func (m *TWorkflow) CountEnabledWorkflowsByIds(ctx *rpc.Context, tx *dbx.Tx, workflowIds []string) (int64, error) {
	if len(workflowIds) == 0 {
		return 0, nil
	}

	scope := m.NewScope()
	if tx != nil {
		scope.Transaction(tx.GetTransactionId())
	}

	count, err := scope.Where("id IN (?) AND status = ?", workflowIds, aiadmin.WorkflowStatus_WorkflowStatusEnabled).
		Count(ctx)

	if err != nil {
		log.Errorf("CountEnabledWorkflowsByIds: Failed to count enabled workflows for ids %v: %v", workflowIds, err)
		return 0, fmt.Errorf("查询启用工作流数量失败: %w", err)
	}
	return int64(count), nil
}
