package buffer

import "fmt"

const (
	redisKeyPrefix         = "aiadmin:workflow_buffer" // Redis 键前缀
	redisKeyStateSuffix    = "state"                   // 状态键后缀
	redisKeyMessagesSuffix = "msgs"                    // 消息列表键后缀
	redisKeyLockSuffix     = "lock"                    // 分布式锁键后缀
	redisKeyTimerMapSuffix = "timers"                  // 用于管理定时器引用的键后缀
)

// GetStateKey 返回用于存储会话缓冲状态的 Redis 键。
// 格式: aiadmin:workflow_buffer:state:<CorpID>:<ChatID>:<WorkflowID>
func GetStateKey(corpID uint32, chatID uint64, workflowID string) string {
	return fmt.Sprintf("%s:%s:%d:%d:%s", redisKeyPrefix, redisKeyStateSuffix, corpID, chatID, workflowID)
}

// GetMessagesKey 返回用于存储会话缓冲消息列表的 Redis 键。
// 格式: aiadmin:workflow_buffer:msgs:<CorpID>:<ChatID>:<WorkflowID>
func GetMessagesKey(corpID uint32, chatID uint64, workflowID string) string {
	return fmt.Sprintf("%s:%s:%d:%d:%s", redisKeyPrefix, redisKeyMessagesSuffix, corpID, chatID, workflowID)
}

// GetLockKey 返回用于会话聚合处理的分布式锁的 Redis 键。
// 格式: aiadmin:workflow_buffer:lock:<CorpID>:<ChatID>:<WorkflowID>
func GetLockKey(corpID uint32, chatID uint64, workflowID string) string {
	return fmt.Sprintf("%s:%s:%d:%d:%s", redisKeyPrefix, redisKeyLockSuffix, corpID, chatID, workflowID)
}

// GetTimersMapKey 返回一个哈希表的 Redis 键，用于存储某个工作流和聊天的活动定时器引用。
// 这个实现会更复杂，如果定时器需要通过 Redis 在多实例间管理。
// 对于简单的单实例 `time.AfterFunc`，这个键可能不会直接在 Redis 中使用，更多是内存中的概念。
func GetTimersMapKey(workflowID string, chatID uint64) string {
	return fmt.Sprintf("%s:%s:%s:%d", redisKeyPrefix, redisKeyTimerMapSuffix, workflowID, chatID)
}
