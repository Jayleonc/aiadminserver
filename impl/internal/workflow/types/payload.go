// impl/internal/workflow/types/payload.go
package types

import (
	"git.pinquest.cn/base/aiadmin"
	"github.com/cloudwego/eino/schema"
)

// WorkflowPayload 是在工作流各节点间传递的统一数据载体。
// 它携带了从开始到当前节点的所有关键信息和状态。
type WorkflowPayload struct {
	// 原始输入，始终不变
	InitialQuery *schema.Message

	// 当前处理的上下文信息，可能会被中间节点修改
	CurrentUserQuery string

	RecentHistory []*aiadmin.ChatMessage

	RetrievedContext string // 知识库检索出的上下文
	Classification   string // 问题分类器的分类结果

	// 新增：用于存放中间步骤（如LLM节点）生成的、可能还会被后续节点修改的答案
	DraftAnswer string

	// 最终结果
	FinalAnswer string
	FinalExtra  []*aiadmin.MaterialInfo // 最终要发送的素材

	// 流程控制与状态
	Error             error              // 记录流程中发生的错误
	FlowControl       map[PayloadKey]any // 【修改】用于驱动分支等逻辑，key 的类型变为 PayloadKey
	CustomData        map[string]any     // 一个灵活的容器，用于节点间传递非标准数据
	MessageSentByNode bool
}

// NewWorkflowPayload 是 WorkflowPayload 的构造函数，确保内部 map 被初始化。
func NewWorkflowPayload(initialQuery *schema.Message) *WorkflowPayload {
	return &WorkflowPayload{
		InitialQuery:      initialQuery,
		CurrentUserQuery:  initialQuery.Content, // 默认使用原始查询内容
		FlowControl:       make(map[PayloadKey]any),
		CustomData:        make(map[string]any),
		MessageSentByNode: false, // 默认未发送
	}
}

// RunMode 定义了工作流的运行模式
type RunMode int

const (
	RunModeProduction RunMode = iota // 0: 生产模式 (默认)
	RunModeTest                      // 1: 测试模式
)

// WorkflowExecutionCtxKey 是用于在 schema.Message.Extra 中存储 WorkflowExecutionContext 的键。
const WorkflowExecutionCtxKey = "workflow_execution_context"

// WorkflowExecutionContext 封装了工作流执行时需要在节点间传递的核心上下文信息。
type WorkflowExecutionContext struct {
	InitialTask   *aiadmin.AIWorkflowMsqTask // 原始的 MQ 任务
	RequestId     string                     // 从 MQ 消费者 rpc.Context 获取的请求 ID，用于链路追踪
	RunMode       RunMode                    // 运行模式 (测试或生产)
	CorpId        uint32
	AppId         uint32
	RobotUid      uint64
	ChatId        uint64
	ChatType      uint32 // 使用原始类型，如 task.GetChatType() 返回的 uint32
	RecentHistory []*aiadmin.ChatMessage
}
