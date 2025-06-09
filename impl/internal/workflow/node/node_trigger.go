// file: impl/internal/workflow/node/node_trigger.go
package node

import (
	"context"
	"encoding/json"
	"fmt"
	"git.pinquest.cn/base/aiadminserver/impl/internal/workflow/types"
	"time" // 需要导入 time

	"git.pinquest.cn/base/aiadmin"
	"git.pinquest.cn/base/aiadminserver/impl/internal/workflow/common"
	"git.pinquest.cn/base/aiadminserver/impl/internal/workflow/logger" // 引入 logger
	baselog "git.pinquest.cn/base/log"                                 // 项目标准日志
	"github.com/cloudwego/eino/compose"
	"github.com/cloudwego/eino/schema"
)

// TriggerNode 代表工作流中的触发器节点
type TriggerNode struct {
	nodeDef *aiadmin.WorkflowNodeDef   // 存储完整的节点定义
	config  *aiadmin.TriggerNodeConfig // 解析后的配置
}

// NewTriggerNodeBuilder 创建触发器节点的构建器
// 【修改】接收 *aiadmin.WorkflowNodeDef
func NewTriggerNodeBuilder(nodeDef *aiadmin.WorkflowNodeDef) (common.NodeBuilder, error) {
	cfg := &aiadmin.TriggerNodeConfig{}
	if err := json.Unmarshal([]byte(nodeDef.NodeConfigJson), cfg); err != nil {
		return nil, fmt.Errorf("反序列化触发器节点 '%s' 配置失败: %w", nodeDef.Id, err)
	}
	return &TriggerNode{
		nodeDef: nodeDef,
		config:  cfg,
	}, nil
}

// Build 创建 Eino 组件。
// 【修改】接收 LogContext，并在 Lambda 中使用
// Build 创建 Eino 组件。
// 由于 TriggerNode 的输入是 *schema.Message，而非 *types.WorkflowPayload，
// 其 Build 方法返回 `any`（实际是 `*compose.Lambda`），并在内部自行管理日志。
func (n *TriggerNode) Build(lc *logger.LogContext) (any, error) {
	return compose.InvokableLambda(
		func(stdLibCtx context.Context, input *schema.Message) (*types.WorkflowPayload, error) {
			// ---- 日志记录 (TriggerNode 内部处理) ----
			nodeStartTime := time.Now()
			var nodeInnerErr error
			var nodeExecID string
			var startLogErr error

			tempPayloadForInputLog := types.NewWorkflowPayload(input)

			if lc != nil {
				nodeExecID, startLogErr = lc.StartNodeExecution(n.nodeDef, tempPayloadForInputLog)
				if startLogErr != nil {
					baselog.Errorf("[WorkflowRun:%s][Node:%s] StartNodeExecution 记录失败: %v. 节点仍将尝试执行。", lc.WorkflowRunID, n.nodeDef.Id, startLogErr)
				}
			} else {
				baselog.Warnf("[TriggerNode:%s] LogContext is nil, skipping DB logging.", n.nodeDef.Id)
			}

			// Capture final payload and error in defer for EndNodeExecution
			var finalPayloadForEndLog *types.WorkflowPayload // Declared here to be accessible in defer

			defer func() {
				if lc == nil {
					return
				}
				elapsedTimeMs := time.Since(nodeStartTime).Milliseconds()
				if !(nodeExecID == "" && startLogErr == nil) { // Only attempt EndNodeExecution if StartNodeExecution was (or seemed) successful
					if endLogErr := lc.EndNodeExecution(nodeExecID, n.nodeDef, finalPayloadForEndLog, nodeInnerErr, elapsedTimeMs); endLogErr != nil {
						baselog.Errorf("[WorkflowRun:%s][Node:%s][ExecID:%s] EndNodeExecution (manual) 记录失败: %v.", lc.WorkflowRunID, n.nodeDef.Id, nodeExecID, endLogErr)
					}
				}
			}()

			// ---- 实际节点逻辑 ----
			baselog.Debugf("[TriggerNode:%s][RunID:%s] 已触发，创建工作流载荷. 原始输入: %s", n.nodeDef.Id, lc.WorkflowRunID, input.Content)

			payload := types.NewWorkflowPayload(input)
			if execCtx, ok := input.Extra[types.WorkflowExecutionCtxKey].(*types.WorkflowExecutionContext); ok {
				if execCtx != nil && execCtx.InitialTask != nil {
					payload.RecentHistory = execCtx.InitialTask.GetRecentHistory()
				}
			}

			finalPayloadForEndLog = payload // Assign the result to the variable captured by defer

			return payload, nodeInnerErr // nodeInnerErr 此时应为 nil
		},
	), nil
}
