package utils

import (
	"fmt"
	"github.com/google/uuid"
)

// NewIdWithPrefix 生成带前缀的 UUID，例如：wf_xxx、node_xxx
func NewIdWithPrefix(prefix string) string {
	return fmt.Sprintf("%s_%s", prefix, uuid.NewString())
}

func NewWorkflowId() string {
	return NewIdWithPrefix("wf")
}

func NewNodeId() string {
	return NewIdWithPrefix("node")
}

func NewEdgeId() string {
	return NewIdWithPrefix("edge")
}

func NewCliMsgId() string {
	return fmt.Sprintf("ai_wfn_cli_%s", uuid.NewString())
}
