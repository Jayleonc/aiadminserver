// impl/internal/workflow/common/constants.go
package types

// PayloadKey 定义了在 WorkflowPayload 的 map 类型字段中使用的 key 的常量
type PayloadKey string

const (
	// FlowControlNextCondition 用于在 FlowControl map 中设置驱动分支逻辑的条件值。
	// 例如，问题分类器会将分类结果（如 "faq_query", "else"）设置给这个 key。
	// 编译器中的分支逻辑会读取这个 key 的值来决定流程的下一个走向。
	FlowControlNextCondition PayloadKey = "next_condition"

	// ConditionQueryKnowledgeFound 定义了当 query_knowledge 节点找到知识时，
	// 将设置到 payload.FlowControl[types.FlowControlNextCondition] 的标准字符串值。
	// 前端在连接“找到知识”的路径时，对应边的 "condition" 字段应设置为此确切值。
	ConditionQueryKnowledgeFound = "if_query_knowledge_found"

	// InternalQKElseTrigger 是一个内部使用的值，当 query_knowledge 节点未找到知识，
	// 并且我们希望流程走向 "else" 分支时，节点会将此值设置到 next_condition。
	// 这个值不应该被任何边的 "condition" 显式使用（"else" 除外）。
	InternalQKElseTrigger = "__qk_internal_else_trigger_for_not_found__"
)
