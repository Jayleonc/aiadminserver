package buffer

import (
	"encoding/json"
	"fmt"
	"time"

	"git.pinquest.cn/base/aiadmin"
	"git.pinquest.cn/base/aiadminserver/impl/internal/model/modelworkflow"
	"git.pinquest.cn/qlb/brick/rpc"
)

const (
	DefaultMinDebounceWindow   = 1 * time.Second  // 默认最小防抖时间
	DefaultMaxDebounceWindow   = 5 * time.Second  // 默认最大防抖时间（用作主防抖窗口）
	MaxSupportedDebounceWindow = 60 * time.Second // 支持的最大防抖窗口上限（安全考虑）
)

// WorkflowBufferConfig 保存工作流生效的缓冲配置。
type WorkflowBufferConfig struct {
	WorkflowID      string        // 工作流 ID
	MinDebounceTime time.Duration // 最小防抖时间 (来自配置)
	MaxDebounceTime time.Duration // 最大防抖时间 (来自配置, 作为 time.AfterFunc 的延迟)
}

// GetWorkflowBufferConfig 获取并解析指定工作流版本的触发器配置。
func GetWorkflowBufferConfig(rpcCtx *rpc.Context, workflowID string, activeVersionID string) (*WorkflowBufferConfig, error) {
	if workflowID == "" || activeVersionID == "" {
		return nil, fmt.Errorf("工作流ID (workflowID) 和活跃版本ID (activeVersionID) 不能为空")
	}

	version, err := modelworkflow.ModelWorkflowVersion.GetById(rpcCtx, activeVersionID)
	if err != nil {
		return nil, fmt.Errorf("获取工作流 %s 的版本 %s 失败: %w", workflowID, activeVersionID, err)
	}
	if version == nil {
		return nil, fmt.Errorf("未找到工作流 %s 的版本 %s", workflowID, activeVersionID)
	}

	// 设置默认值
	cfg := &WorkflowBufferConfig{
		WorkflowID:      workflowID,
		MinDebounceTime: DefaultMinDebounceWindow,
		MaxDebounceTime: DefaultMaxDebounceWindow,
	}

	if version.TriggerConfig != "" {
		var triggerNodeCfg aiadmin.TriggerNodeConfig
		if err := json.Unmarshal([]byte(version.TriggerConfig), &triggerNodeCfg); err == nil {
			if triggerNodeCfg.MinIntervalSeconds > 0 { //
				cfg.MinDebounceTime = time.Duration(triggerNodeCfg.MinIntervalSeconds) * time.Second
			}
			if triggerNodeCfg.MaxIntervalSeconds > 0 { //
				// 配置中的 MaxIntervalSeconds 用作主要的防抖窗口时间
				cfg.MaxDebounceTime = time.Duration(triggerNodeCfg.MaxIntervalSeconds) * time.Second
			}
			// 确保 MaxDebounceTime 至少等于 MinDebounceTime
			if cfg.MaxDebounceTime < cfg.MinDebounceTime {
				cfg.MaxDebounceTime = cfg.MinDebounceTime
			}
			// 安全上限检查
			if cfg.MaxDebounceTime > MaxSupportedDebounceWindow {
				cfg.MaxDebounceTime = MaxSupportedDebounceWindow
			}
			if cfg.MinDebounceTime > MaxSupportedDebounceWindow {
				cfg.MinDebounceTime = MaxSupportedDebounceWindow
			}
		} else {
			return nil, fmt.Errorf("解析工作流版本 %s 的 trigger_config 失败: %w", activeVersionID, err)
		}
	}
	// 如果 MaxIntervalSeconds 未设置或无效，确保使用有效的默认值或 MinDebounceTime
	if cfg.MaxDebounceTime <= 0 {
		if cfg.MinDebounceTime > 0 {
			cfg.MaxDebounceTime = cfg.MinDebounceTime
		} else {
			cfg.MaxDebounceTime = DefaultMaxDebounceWindow
		}
	}
	return cfg, nil
}
