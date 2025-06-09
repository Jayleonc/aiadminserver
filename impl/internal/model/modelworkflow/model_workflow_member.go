// impl/internal/model/modelworkflow/model_workflow_member.go
package modelworkflow

import (
	"fmt"
	"git.pinquest.cn/base/aiadmin"
	"git.pinquest.cn/base/aiadminserver/impl/internal/model"
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/dbx"
	"git.pinquest.cn/qlb/brick/rpc"
)

type TWorkflowMember struct {
	model.TModel
}

var ModelWorkflowMember = &TWorkflowMember{
	TModel: model.TModel{
		Model: dbx.NewModel(&dbx.ModelConfig{
			Type: &aiadmin.ModelWorkflowMember{},
			//NotFoundErrCode: ... , // Define if needed
		}),
	},
}

func (m *TWorkflowMember) BatchCreateInTx(ctx *rpc.Context, tx *dbx.Tx, members []*aiadmin.ModelWorkflowMember) error {
	if len(members) == 0 {
		return nil
	}
	return m.NewScope().Transaction(tx.GetTransactionId()).BatchCreate(ctx, members)
}

func (m *TWorkflowMember) DeleteByWorkflowIdInTx(ctx *rpc.Context, tx *dbx.Tx, workflowId string) error {
	_, err := m.NewScope().Transaction(tx.GetTransactionId()).
		Where("workflow_id = ?", workflowId).
		Delete(ctx)
	if err != nil {
		log.Errorf("Delete members by workflow_id %s in transaction failed: %v", workflowId, err)
	}
	return err
}

func (m *TWorkflowMember) ListByRobotUid(ctx *rpc.Context, corpId uint32, robotUid uint64) ([]*aiadmin.ModelWorkflowMember, error) {
	var list []*aiadmin.ModelWorkflowMember
	err := m.Where("corp_id = ? AND robot_uid = ?", corpId, robotUid).Find(ctx, &list)
	if err != nil {
		log.Errorf("ListByRobotUid for corp %d, robot %d failed: %v", corpId, robotUid, err)
	}
	return list, err
}

// Add other necessary methods like ListByWorkflowId, etc.

// ListByWorkflowId 获取指定工作流的所有成员
func (m *TWorkflowMember) ListByWorkflowId(ctx *rpc.Context, workflowId string) ([]*aiadmin.ModelWorkflowMember, error) {
	var list []*aiadmin.ModelWorkflowMember
	err := m.Where("workflow_id = ?", workflowId).Find(ctx, &list)
	if err != nil {
		log.Errorf("List WorkflowMembers by WorkflowId %s error: %v", workflowId, err)
		return nil, err
	}
	return list, nil
}

// GetOtherWorkflowIdsByRobotUidInTx 获取指定 robotUid 关联的、非 currentWorkflowIdToExclude 的所有 workflow_id
// tx *dbx.Tx 如果不为 nil，则所有查询都在该事务下执行
func (m *TWorkflowMember) GetOtherWorkflowIdsByRobotUid(ctx *rpc.Context, tx *dbx.Tx, corpId uint32, robotUid uint64, currentWorkflowIdToExclude string) ([]string, error) {
	var members []*aiadmin.ModelWorkflowMember
	memberScope := m.NewScope()
	if tx != nil {
		memberScope.Transaction(tx.GetTransactionId())
	}

	// 查询 robot_uid 关联的所有 workflow_id，排除当前正在操作的 workflow_id
	err := memberScope.Where("corp_id = ? AND robot_uid = ? AND workflow_id != ?", corpId, robotUid, currentWorkflowIdToExclude).
		Select("workflow_id").
		Find(ctx, &members)

	if err != nil {
		log.Errorf("GetOtherWorkflowIdsByRobotUid: Failed for robot_uid %s (excluding %s): %v", robotUid, currentWorkflowIdToExclude, err)
		return nil, fmt.Errorf("查询成员关联的其他工作流ID失败: %w", err)
	}

	if len(members) == 0 {
		return []string{}, nil // 没有关联其他工作流
	}

	otherWorkflowIds := make([]string, 0, len(members))
	for _, mem := range members {
		otherWorkflowIds = append(otherWorkflowIds, mem.WorkflowId)
	}
	return otherWorkflowIds, nil
}

// ListByWorkflowIds 根据一组工作流ID批量获取成员列表
func (m *TWorkflowMember) ListByWorkflowIds(ctx *rpc.Context, workflowIds []string) ([]*aiadmin.ModelWorkflowMember, error) {
	var list []*aiadmin.ModelWorkflowMember
	if len(workflowIds) == 0 {
		return list, nil
	}

	err := m.Where("workflow_id IN (?)", workflowIds).Find(ctx, &list)
	if err != nil {
		log.Errorf("List WorkflowMembers by WorkflowIDs error: %v, ids: %v", err, workflowIds)
		return nil, err
	}
	return list, nil
}
