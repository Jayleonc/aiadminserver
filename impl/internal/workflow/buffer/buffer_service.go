// file: impl/internal/workflow/buffer/buffer_service.go
package buffer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"git.pinquest.cn/base/aiadminserver/impl/internal/workflow/types"
	"strings"
	"sync"
	"time"

	"git.pinquest.cn/base/aiadmin"
	"git.pinquest.cn/base/aiadminserver/impl/internal/model/modelworkflow"
	wf "git.pinquest.cn/base/aiadminserver/impl/internal/workflow"
	"git.pinquest.cn/base/aiadminserver/impl/internal/workflow/logger"
	baselog "git.pinquest.cn/base/log" // 项目标准日志
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/core"
	"github.com/cloudwego/eino/schema"
	"strconv"

	"git.pinquest.cn/qlb/brick/redisgroup"
)

var activeTimers = sync.Map{}

const (
	RedisFieldStatus      = "status"
	RedisFieldDebounceMs  = "debounce_window_ms"
	RedisFieldLastMsgTs   = "last_msg_ts"
	RedisFieldAggregating = "aggregating" // 标记是否正在聚合处理中
	StatusWaiting         = "WAITING"
	debugObservationTTL   = 10 * time.Minute
)

type Service struct {
	redisGroup *redisgroup.RedisGroup
}

func NewService(rg *redisgroup.RedisGroup) *Service {
	return &Service{redisGroup: rg}
}

func (s *Service) HandleIncomingEvent(rpcCtx *rpc.Context, event *aiadmin.AIWorkflowTriggerEventWithHistory) error {
	if event == nil {
		return fmt.Errorf("事件 (event) 不能为空")
	}
	baselog.Debugf("BufferService: 开始处理事件, WorkflowID: %s, ChatID: %d, CorpID: %d", event.WorkflowId, event.ChatId, event.CorpId)

	// 1. 获取工作流和缓冲配置
	workflowDef, err := modelworkflow.ModelWorkflow.GetById(rpcCtx, event.WorkflowId)
	if err != nil {
		return fmt.Errorf("获取工作流 %s 失败: %w", event.WorkflowId, err)
	}
	if workflowDef == nil {
		return fmt.Errorf("未找到工作流 %s", event.WorkflowId)
	}
	if workflowDef.ActiveVersionId == "" {
		return fmt.Errorf("工作流 %s 没有活跃版本", event.WorkflowId)
	}

	bufferCfg, err := GetWorkflowBufferConfig(rpcCtx, event.WorkflowId, workflowDef.ActiveVersionId)
	if err != nil {
		return fmt.Errorf("获取工作流 %s 的缓冲配置失败: %w", event.WorkflowId, err)
	}
	baselog.Debugf("BufferService: 工作流 %s, 防抖窗口: %v", event.WorkflowId, bufferCfg.MaxDebounceTime)

	stateKey := GetStateKey(event.CorpId, event.ChatId, event.WorkflowId)
	msgsKey := GetMessagesKey(event.CorpId, event.ChatId, event.WorkflowId)

	eventJSONBytes, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("序列化事件失败: %w", err)
	}
	eventJSONStr := string(eventJSONBytes)
	nowUnixMilli := time.Now().UnixMilli()

	// 2. Redis 操作
	if _, err := s.redisGroup.RPush(msgsKey, eventJSONStr); err != nil {
		return fmt.Errorf("Redis RPush 操作失败 (键: %s): %w", msgsKey, err)
	}
	stateFields := map[string]interface{}{
		RedisFieldLastMsgTs:   strconv.FormatInt(nowUnixMilli, 10),
		RedisFieldStatus:      StatusWaiting,
		RedisFieldDebounceMs:  strconv.FormatInt(bufferCfg.MaxDebounceTime.Milliseconds(), 10),
		RedisFieldAggregating: "0",
	}
	if err := s.redisGroup.HMSet(stateKey, stateFields); err != nil {
		baselog.Errorf("Redis HMSet 操作失败 (键: %s): %v。考虑回滚 RPUSH", stateKey, err)
		return fmt.Errorf("Redis HMSet 操作失败 (键: %s): %w", stateKey, err)
	}
	ttlSeconds := int(bufferCfg.MaxDebounceTime.Seconds()) + 60
	s.redisGroup.Expire(msgsKey, time.Duration(ttlSeconds)*time.Second)
	s.redisGroup.Expire(stateKey, time.Duration(ttlSeconds)*time.Second)
	baselog.Debugf("BufferService: 事件已推送到 %s, 状态 %s 已更新, last_msg_ts: %d.", msgsKey, stateKey, nowUnixMilli)

	// 3. 定时器管理
	if t, ok := activeTimers.Load(stateKey); ok {
		if timer, okTimer := t.(*time.Timer); okTimer {
			timer.Stop()
			baselog.Debugf("BufferService: 已停止 %s 的现有定时器", stateKey)
		}
	}
	newTimer := time.AfterFunc(bufferCfg.MaxDebounceTime, func() {
		baselog.Infof("BufferService: %s 的定时器已触发。尝试执行聚合操作。", stateKey)
		activeTimers.Delete(stateKey)

		// 为 ProcessAggregation 创建一个新的 rpc.Context
		detachedRpcCtx := &rpc.Context{}
		core.SetPinHeaderCorpId(detachedRpcCtx, event.CorpId)
		core.SetPinHeaderAppId(detachedRpcCtx, event.AppId)  // 确保 event 中有 AppId
		core.SetPinHeaderUId(detachedRpcCtx, event.RobotUid) // 确保 event 中有 RobotUid

		if err := s.ProcessAggregation(detachedRpcCtx, event.CorpId, event.ChatId, event.WorkflowId); err != nil {
			baselog.Errorf("BufferService: 处理 %s 的聚合操作失败: %v", stateKey, err)
		}
	})
	activeTimers.Store(stateKey, newTimer)
	baselog.Debugf("BufferService: 已为 %s 安排新定时器，将在 %v 后触发", stateKey, bufferCfg.MaxDebounceTime)
	return nil
}

// ProcessAggregation 由定时器调用
func (s *Service) ProcessAggregation(rpcCtx *rpc.Context, corpID uint32, chatID uint64, workflowID string) error {
	stateKey := GetStateKey(corpID, chatID, workflowID)
	msgsKey := GetMessagesKey(corpID, chatID, workflowID)
	baselog.Infof("BufferService.ProcessAggregation: 开始处理聚合 for %s", stateKey)

	// 1. 聚合锁 (HGet + HSet 模拟 HSetNX) - todo 后续改成 HSetNx
	currentAggregatingVal, errGetLock := s.redisGroup.HGet(stateKey, RedisFieldAggregating)
	if errGetLock != nil && !s.redisGroup.IsNotFound(errGetLock) && !errors.Is(errGetLock, redisgroup.ErrRedisException) {
		return errGetLock
	}
	if currentAggregatingVal == "1" {
		return nil
	}
	if errSetLock := s.redisGroup.HSet(stateKey, RedisFieldAggregating, "1"); errSetLock != nil {
		return errSetLock
	}
	defer func() {
		if _, errDel := s.redisGroup.HDel(stateKey, RedisFieldAggregating); errDel != nil {
			baselog.Errorf("BufferService: 清理 %s 的聚合标记失败: %v", stateKey, errDel)
		} else {
			baselog.Debugf("BufferService: %s 的聚合标记已清理。", stateKey)
		}
	}()
	baselog.Infof("BufferService: %s 的聚合锁已获取。", stateKey)

	// 2. 状态检查 - 这部分逻辑保持不变
	stateDataMap, err := s.redisGroup.HGetAll(stateKey)
	if err != nil || len(stateDataMap) == 0 {
		return errors.New(fmt.Sprintf("BufferService: 状态键 %s 检查时消失或错误: %v", stateKey, err))
	}
	lastMsgTsStr, okTs := stateDataMap[RedisFieldLastMsgTs]
	debounceMsStr, okDebounce := stateDataMap[RedisFieldDebounceMs]
	if !okTs || !okDebounce { /* ... 清理并返回 ... */
		return nil
	}
	lastMsgTs, _ := strconv.ParseInt(lastMsgTsStr, 10, 64)
	debounceMs, _ := strconv.ParseInt(debounceMsStr, 10, 64)
	if lastMsgTs == 0 || debounceMs == 0 { /* ... 清理并返回 ... */
		return nil
	}
	if time.Now().UnixMilli() < lastMsgTs+debounceMs { /* ... 取消当前聚合 ... */
		return nil
	}

	// 3. 获取并聚合消息 - 这部分逻辑保持不变
	msgJSONs, err := s.redisGroup.LRange(msgsKey, 0, -1)
	if err != nil {
		return fmt.Errorf("为 %s 获取消息失败: %w", msgsKey, err)
	}
	if len(msgJSONs) == 0 { /* ... 清理并返回 ... */
		return nil
	}
	var aggregatedKeyword strings.Builder
	var latestEvent *aiadmin.AIWorkflowTriggerEventWithHistory
	for i, msgJSON := range msgJSONs { /* ... 聚合逻辑 ... */
		var event aiadmin.AIWorkflowTriggerEventWithHistory
		if err := json.Unmarshal([]byte(msgJSON), &event); err != nil {
			baselog.Errorf("BufferService: 反序列化 %s 的消息失败 (索引 %d): %v", msgsKey, i, err)
			continue
		}
		if aggregatedKeyword.Len() > 0 {
			aggregatedKeyword.WriteString("\n")
		}
		aggregatedKeyword.WriteString(event.SingleMessageKeyword)
		if latestEvent == nil || event.SingleMessageTimestamp > latestEvent.SingleMessageTimestamp {
			latestEvent = &event
		}
	}
	if latestEvent == nil { /* ... 清理并返回 ... */
		return nil
	}
	finalTask := &aiadmin.AIWorkflowMsqTask{
		WorkflowId: workflowID, CorpId: corpID, AppId: latestEvent.AppId, RobotUid: latestEvent.RobotUid,
		ChatId: chatID, ChatType: latestEvent.ChatType, SendAccountId: latestEvent.SendAccountId,
		Keyword: aggregatedKeyword.String(), CliMsgId: latestEvent.SingleMessageCliMsgId,
		RecentHistory: latestEvent.RecentHistory,
	}
	baselog.Infof("BufferService: 已为工作流 %s, ChatID %d 构造聚合任务。关键词: [%s]", finalTask.WorkflowId, finalTask.ChatId, finalTask.Keyword)

	// 4. 执行工作流（日志记录的事务由 LogContext 内部方法管理）
	var overallWorkflowError error

	// 4.1 加载元数据 (不包裹在 logger 的事务中，因为这些是前置只读操作)
	wfDef, dbErr := modelworkflow.ModelWorkflow.GetById(rpcCtx, workflowID)
	if dbErr != nil || wfDef == nil {
		overallWorkflowError = fmt.Errorf("ProcessAggregation: 获取工作流 %s 定义失败: %w", workflowID, dbErr)
		baselog.Error(overallWorkflowError.Error())
		s.cleanupRedisAfterError(stateKey, msgsKey)
		return overallWorkflowError
	}
	if wfDef.Status != uint32(aiadmin.WorkflowStatus_WorkflowStatusEnabled) {
		baselog.Warnf("ProcessAggregation: 工作流 '%s' (ID: %s) 未启用，跳过执行。", wfDef.Name, wfDef.Id)
		s.cleanupRedisAfterSuccess(stateKey, msgsKey) // 视为一种“成功”的空操作
		return nil
	}
	activeVersion, dbErrVer := modelworkflow.ModelWorkflowVersion.GetById(rpcCtx, wfDef.ActiveVersionId)
	if dbErrVer != nil || activeVersion == nil {
		overallWorkflowError = fmt.Errorf("ProcessAggregation: 获取工作流 %s 的活跃版本 %s 失败: %w", workflowID, wfDef.ActiveVersionId, dbErrVer)
		baselog.Error(overallWorkflowError.Error())
		s.cleanupRedisAfterError(stateKey, msgsKey)
		return overallWorkflowError
	}

	// 4.2 创建 WorkflowExecutionContext 和 LogContext (不含长事务)
	// 【修改】使用 common.WorkflowExecutionContext
	wfExecCtx := &types.WorkflowExecutionContext{
		InitialTask: finalTask,
		RunMode:     types.RunModeProduction, // 【修改】使用 common.RunModeProduction
		RequestId:   rpcCtx.GetReqId(),       // 使用传入的 rpcCtx 的请求ID
		CorpId:      finalTask.CorpId, AppId: finalTask.AppId, RobotUid: finalTask.RobotUid,
		ChatId: finalTask.ChatId, ChatType: finalTask.ChatType, RecentHistory: finalTask.RecentHistory,
	}
	// 【修改】NewLogContext 不再接收 Tx
	logCtx := logger.NewLogContext(rpcCtx, wfExecCtx)

	// 4.3 日志：工作流开始（内部会处理自己的短事务）
	if err := logCtx.StartWorkflowRun(wfDef, activeVersion, finalTask); err != nil {
		overallWorkflowError = fmt.Errorf("ProcessAggregation: 记录聚合工作流开始日志失败: %w", err)
		baselog.Error(overallWorkflowError.Error())
		// 即使开始日志失败，也应尝试执行工作流，但最终的错误需要被记录和返回
	}

	// 4.4 编译工作流，传入不带长事务的 LogContext
	// 【修改】将 logCtx 传递给 CompileWorkflow
	runnable, compileErr := wf.CompileWorkflow(wfDef, activeVersion, context.Background(), logCtx)
	if compileErr != nil {
		overallWorkflowError = fmt.Errorf("ProcessAggregation: 编译聚合工作流 %s 失败: %w", workflowID, compileErr)
		baselog.Errorf(overallWorkflowError.Error())
		// 编译失败，尝试记录最终的 WorkflowRun 状态
		logCtx.EndWorkflowRun(nil, overallWorkflowError) // EndWorkflowRun 内部处理事务
		s.cleanupRedisAfterError(stateKey, msgsKey)
		return overallWorkflowError
	}

	// 4.5 构造输入并执行
	userInput := schema.Message{Role: schema.User, Content: finalTask.Keyword} // Eino 需要 *schema.Message
	userInput.Extra = map[string]interface{}{
		types.WorkflowExecutionCtxKey: wfExecCtx, // 【修改】使用 common.WorkflowExecutionCtxKey
	}
	finalPayloadFromInvoke, invokeErr := runnable.Invoke(context.Background(), &userInput) // 传递指针

	// 4.6 确定最终执行错误
	if invokeErr != nil {
		if overallWorkflowError == nil {
			overallWorkflowError = invokeErr
		} // 保留第一个主要错误
		baselog.Errorf("ProcessAggregation: 工作流 Invoke 失败 for %s: %v", stateKey, invokeErr)
	} else if finalPayloadFromInvoke != nil && finalPayloadFromInvoke.Error != nil {
		if overallWorkflowError == nil {
			overallWorkflowError = finalPayloadFromInvoke.Error
		}
		baselog.Errorf("ProcessAggregation: 工作流执行后 Payload 中包含错误 for %s: %v", stateKey, finalPayloadFromInvoke.Error)
	} else if finalPayloadFromInvoke == nil && invokeErr == nil { // 成功执行但无返回
		baselog.Warnf("ProcessAggregation: 工作流 %s 为 ChatID %d 执行后返回的 finalPayload 为 nil", workflowID, chatID)
	} else if invokeErr == nil && finalPayloadFromInvoke != nil && finalPayloadFromInvoke.Error == nil {
		baselog.Infof("ProcessAggregation: 工作流 %s 为 ChatID %d 成功执行。最终答案: %s", workflowID, chatID, finalPayloadFromInvoke.FinalAnswer)
	}

	// 4.7 日志：工作流结束（内部会处理自己的短事务）
	if err := logCtx.EndWorkflowRun(finalPayloadFromInvoke, overallWorkflowError); err != nil {
		baselog.Warnf("ProcessAggregation: 记录聚合工作流结束日志时发生错误: %v", err)
		if overallWorkflowError == nil { // 如果主流程没出错，但日志结束出错了
			overallWorkflowError = fmt.Errorf("记录工作流结束日志失败: %w", err)
		}
	}

	// 5. Redis 清理
	if overallWorkflowError == nil {
		s.cleanupRedisAfterSuccess(stateKey, msgsKey)
	} else {
		s.cleanupRedisAfterError(stateKey, msgsKey)
	}

	return overallWorkflowError
}

func (s *Service) cleanupRedisAfterSuccess(stateKey, msgsKey string) {
	s.redisGroup.Del(msgsKey)
	baselog.Infof("BufferService: 聚合任务 for %s 成功完成。状态键将保留 %v。", stateKey, 5*time.Minute)
	s.redisGroup.Expire(stateKey, 5*time.Minute) // 成功后保留一段时间供观察
}

func (s *Service) cleanupRedisAfterError(stateKey, msgsKey string) {
	s.redisGroup.Del(msgsKey)
	baselog.Errorf("BufferService: 聚合任务 for %s 失败. 消息已从Redis队列移除。状态键将保留 %v。", stateKey, debugObservationTTL)
	s.redisGroup.Expire(stateKey, debugObservationTTL) // 失败后保留更长时间供调试
}
