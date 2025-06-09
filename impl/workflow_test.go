package impl

import (
	"encoding/json"
	"git.pinquest.cn/base/aiadmin"
	buffer2 "git.pinquest.cn/base/aiadminserver/impl/internal/workflow/buffer"
	"git.pinquest.cn/base/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

// TestTriggerWorkflowBuffering 测试工作流缓冲机制是否能正确接收事件并更新Redis
func TestTriggerWorkflowBuffering(t *testing.T) {
	// --- 初始化环境 (与 TestWorkflow 类似，确保 S 和 workflowBufferService 已初始化) ---
	// 此处假设 InitTestEnvWithCorp 已经初始化了 S，并且 InitWorkflowBufferService(S.RedisGroup) 已被调用
	// 或者在测试的 Setup/Teardown 中处理。为简化，直接调用：
	InitTestEnv()                           // 确保 S.RedisGroup 等被初始化
	InitWorkflowBufferService(S.RedisGroup) // 初始化 workflowBufferService

	// 使用一个固定的 rpc.Context 进行测试
	ctx := InitTestEnvWithCorp(2000772, 2000273, 1699) // 示例 CorpId, AppId, Uid
	corpId := uint32(2000772)
	// appId := uint32(2000273) // appId 暂未在 buffer key 中使用，但 future proof
	uid := uint64(1699) // 模拟AI机器人UID

	// --- 测试参数 ---
	testWorkflowId := "wf_5647b0ce-f5be-4b1f-a238-436018d4f90d" // 假设的测试工作流ID
	testChatId := uint64(12345)                                 // 假设的聊天ID
	testUserId := uint64(67890)                                 // 模拟的用户ID (SendAccountId)// --- 模拟工作流版本和触发配置 ---
	// 在真实的集成测试中，这些数据应该存在于数据库中，或者被mock掉。
	// 这里为了简化，我们假设 GetWorkflowBufferConfig 能够获取到配置。
	// 你可以修改 GetWorkflowBufferConfig 让其在特定测试 workflowId 时返回固定配置，
	// 或者确保数据库中有对应的测试数据。
	// 为了让测试独立运行，我们直接提供 bufferCfg
	// 假设已有的 ModelWorkflow 和 ModelWorkflowVersion 记录
	// 你需要确保数据库中有 WorkflowId="wf_test_agg_detail_002" 及其 ActiveVersionId
	// 并且该 ActiveVersion 的 TriggerConfig 是 {"max_interval_seconds": 2}
	// 如果无法操作数据库，你需要mock `modelworkflow.ModelWorkflow.GetById` 和 `modelworkflow.ModelWorkflowVersion.GetById`
	// 这里我们使用一个固定的 bufferCfg，但实际测试中应依赖 GetWorkflowBufferConfig
	bufferCfg := &buffer2.WorkflowBufferConfig{
		WorkflowID:      testWorkflowId,
		MinDebounceTime: 1 * time.Second,
		MaxDebounceTime: 3 * time.Second, // 防抖窗口3秒
	}
	log.Infof("测试使用的防抖窗口: %v", bufferCfg.MaxDebounceTime)

	// 清理可能存在的旧测试数据
	stateKey := buffer2.GetStateKey(corpId, testChatId, testWorkflowId)
	msgsKey := buffer2.GetMessagesKey(corpId, testChatId, testWorkflowId)
	S.RedisGroup.Del(stateKey)
	S.RedisGroup.Del(msgsKey)
	defer func() { // 测试结束时再次清理
		S.RedisGroup.Del(stateKey)
		S.RedisGroup.Del(msgsKey)
	}()

	// --- 模拟连续发送的事件 ---
	eventTimestampBase := time.Now()

	event1 := &aiadmin.AIWorkflowTriggerEventWithHistory{
		WorkflowId: testWorkflowId,
		CorpId:     corpId, AppId: 0, RobotUid: uid, ChatId: testChatId, ChatType: 1, SendAccountId: testUserId,
		SingleMessageKeyword:   "第一条消息，你好啊！",
		SingleMessageCliMsgId:  "cli_msg_agg_001",
		SingleMessageTimestamp: eventTimestampBase.UnixMilli(),
		RecentHistory: []*aiadmin.ChatMessage{
			{Role: string(aiadmin.RoleTypeUser), Content: "之前的对话1"},
		},
	}

	event2 := &aiadmin.AIWorkflowTriggerEventWithHistory{
		WorkflowId: testWorkflowId,
		CorpId:     corpId, AppId: 0, RobotUid: uid, ChatId: testChatId, ChatType: 1, SendAccountId: testUserId,
		SingleMessageKeyword:   "这是第二条消息，天气不错。",
		SingleMessageCliMsgId:  "cli_msg_agg_002",
		SingleMessageTimestamp: eventTimestampBase.Add(500 * time.Millisecond).UnixMilli(), // 0.5秒后
		RecentHistory: []*aiadmin.ChatMessage{ // 历史记录通常是包含上一条消息的
			{Role: string(aiadmin.RoleTypeUser), Content: "之前的对话1"},
			{Role: string(aiadmin.RoleTypeUser), Content: "第一条消息，你好啊！"}, // event1 的内容进入了历史
		},
	}

	event3 := &aiadmin.AIWorkflowTriggerEventWithHistory{
		WorkflowId: testWorkflowId,
		CorpId:     corpId, AppId: 0, RobotUid: uid, ChatId: testChatId, ChatType: 1, SendAccountId: testUserId,
		SingleMessageKeyword:   "然后是第三条，我想问个问题。",
		SingleMessageCliMsgId:  "cli_msg_agg_003",
		SingleMessageTimestamp: eventTimestampBase.Add(1200 * time.Millisecond).UnixMilli(), // 再过 0.7 秒 (总计1.2秒)
		RecentHistory: []*aiadmin.ChatMessage{ // 最新历史，包含了前两条
			{Role: string(aiadmin.RoleTypeUser), Content: "之前的对话1"},
			{Role: string(aiadmin.RoleTypeUser), Content: "第一条消息，你好啊！"},
			{Role: string(aiadmin.RoleTypeAssistant), Content: "你好！"}, // 假设AI针对第一条有回复
			{Role: string(aiadmin.RoleTypeUser), Content: "这是第二条消息，天气不错。"},
		},
	}

	// --- 调用 HandleIncomingEvent 发送事件 ---
	log.Infof("TestTriggerWorkflowBufferingAndAggregationDetail: 发送事件1...")
	err := workflowBufferService.HandleIncomingEvent(ctx, event1)
	require.NoError(t, err, "处理事件1失败")

	log.Infof("TestTriggerWorkflowBufferingAndAggregationDetail: 发送事件2...")
	err = workflowBufferService.HandleIncomingEvent(ctx, event2)
	require.NoError(t, err, "处理事件2失败")

	log.Infof("TestTriggerWorkflowBufferingAndAggregationDetail: 发送事件3...")
	err = workflowBufferService.HandleIncomingEvent(ctx, event3) // 这是最后一条消息
	require.NoError(t, err, "处理事件3失败")

	// --- 验证 Redis 中的消息列表 ---
	msgList, err := S.RedisGroup.LRange(msgsKey, 0, -1)
	require.NoError(t, err, "从Redis获取消息列表失败")
	require.Len(t, msgList, 3, "消息列表应包含3条消息")

	// 验证消息顺序和内容 (可选，但有助于确认存储正确)
	var decodedEv1, decodedEv2, decodedEv3 aiadmin.AIWorkflowTriggerEventWithHistory
	json.Unmarshal([]byte(msgList[0]), &decodedEv1)
	json.Unmarshal([]byte(msgList[1]), &decodedEv2)
	json.Unmarshal([]byte(msgList[2]), &decodedEv3)
	assert.Equal(t, event1.SingleMessageKeyword, decodedEv1.SingleMessageKeyword)
	assert.Equal(t, event2.SingleMessageKeyword, decodedEv2.SingleMessageKeyword)
	assert.Equal(t, event3.SingleMessageKeyword, decodedEv3.SingleMessageKeyword)
	log.Info("Redis中消息列表内容初步符合预期。")

	// --- 等待 ProcessAggregation 被触发 ---
	// 等待时间应该是最后一条消息的时间戳 + MaxDebounceTime + 一个小的缓冲时间
	// event3 是最后一条消息，它的时间戳是 eventTimestampBase.Add(1200 * time.Millisecond)
	// MaxDebounceTime 是 3 秒
	// 所以聚合应该在 event3 之后大约 3 秒触发
	waitDuration := bufferCfg.MaxDebounceTime + (2 * time.Second) // 额外2秒缓冲
	log.Infof("TestTriggerWorkflowBufferingAndAggregationDetail: 等待 %v 以便 ProcessAggregation 执行...", waitDuration)
	time.Sleep(waitDuration)

	// --- 检查 ProcessAggregation 执行后的情况 ---
	// 此时，我们主要通过 ProcessAggregation 内部增强的日志来观察聚合结果。
	// 同时，我们也可以检查 Redis 的键是否按照预期被设置了调试TTL。
	log.Info("TestTriggerWorkflowBufferingAndAggregationDetail: ProcessAggregation 应已执行，请检查下面的日志输出中 Keyword 和 RecentHistory 的聚合情况。")

	exists, err := S.RedisGroup.Exists(stateKey)
	require.NoError(t, err)
	if !exists {
		// 如果 stateKey 不存在，可能是 ProcessAggregation 成功执行并清理了（如果逻辑是删除而不是设TTL）
		// 或者 ProcessAggregation 根本没执行（定时器问题、锁问题等）
		// 或者 ProcessAggregation 在设置TTL前就出错了
		log.Warnf("stateKey %s 在 ProcessAggregation 后不存在，可能已被删除或未成功执行。", stateKey)
	} else {
		ttlState, errTtl := S.RedisGroup.Ttl(stateKey)
		require.NoError(t, errTtl)
		//assert.LessOrEqual(t, int64(ttlState.Seconds()), int64((5 * time.Minute).Seconds()), "stateKey 的 TTL 应该是调试TTL (5分钟)")
		assert.Greater(t, int64(ttlState.Seconds()), int64(0), "stateKey 的 TTL 应该大于0")
		log.Infof("stateKey %s 存在，TTL: %v", stateKey, ttlState)
	}

	existsMsgs, err := S.RedisGroup.Exists(msgsKey)
	require.NoError(t, err)
	if !existsMsgs {
		log.Warnf("msgsKey %s 在 ProcessAggregation 后不存在。", msgsKey)
	} else {
		ttlMsgs, errTtl := S.RedisGroup.Ttl(msgsKey)
		require.NoError(t, errTtl)
		//assert.LessOrEqual(t, int64(ttlMsgs.Seconds()), int64((5*time.Minute).Seconds()), "msgsKey 的 TTL 应该是调试TTL (5分钟)")
		assert.Greater(t, int64(ttlMsgs.Seconds()), int64(0), "msgsKey 的 TTL 应该大于0")
		log.Infof("msgsKey %s 存在，TTL: %v", msgsKey, ttlMsgs)
	}
	// 在这个测试的日志输出中，你应该能看到类似这样的日志行 (来自 ProcessAggregation):
	// "BufferService: 已为工作流 wf_test_agg_detail_002, ChatID 12345 聚合任务。"
	// "  => 聚合后关键词 (finalTask.Keyword): [第一条消息，你好啊！\n这是第二条消息，天气不错。\n然后是第三条，我想问个问题。]"
	// "  => 使用的历史记录 (finalTask.RecentHistory) 来源: 最新一条原始事件附带的历史。条数: 4"
	// "     历史 1: Role=USER, Content=[之前的对话1]"
	// "     历史 2: Role=USER, Content=[第一条消息，你好啊！]"
	// "     历史 3: Role=ASSISTANT, Content=[你好！]"
	// "     历史 4: Role=USER, Content=[这是第二条消息，天气不错。]"
	// (注意：上面日志中的历史记录内容和条数是基于 event3.RecentHistory 的示例)

	// 由于我们无法直接从这个测试中断言 ProcessAggregation 内部构造的 finalTask，
	// 所以主要依赖日志输出来确认聚合逻辑是否符合预期。
	// 如果要进行自动化断言，需要按之前讨论的方案修改 ProcessAggregation 以暴露结果。
}
