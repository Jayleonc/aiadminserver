package node

import "git.pinquest.cn/base/aiadminserver/impl/internal/workflow/common"

// BuilderWrapper 是一个包装器，用于在编译器中同时持有节点的构建器和已构建的组件实例。
type BuilderWrapper struct {
	Builder   common.NodeBuilder
	Component any
}
