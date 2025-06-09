// file: internal/workflow/node/node_query_knowledge.go
package node

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"git.pinquest.cn/base/aiadmin"
	"git.pinquest.cn/base/aiadminserver/impl/internal/model/modelrag"
	"git.pinquest.cn/base/aiadminserver/impl/internal/rag/retriever"
	"git.pinquest.cn/base/aiadminserver/impl/internal/workflow/common"
	"git.pinquest.cn/base/aiadminserver/impl/internal/workflow/logger"
	"git.pinquest.cn/base/aiadminserver/impl/internal/workflow/types"
	baselog "git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/rpc"
	einoRetriever "github.com/cloudwego/eino/components/retriever"
	"github.com/cloudwego/eino/schema"
)

// QueryKnowledgeNode 持有节点的状态
type QueryKnowledgeNode struct {
	nodeDef *aiadmin.WorkflowNodeDef          // 存储完整的节点定义
	config  *aiadmin.QueryKnowledgeNodeConfig // 解析后的配置
}

// NewQueryKnowledgeNodeBuilder 创建节点构建器实例
// 【修改】接收 *aiadmin.WorkflowNodeDef
func NewQueryKnowledgeNodeBuilder(nodeDef *aiadmin.WorkflowNodeDef) (common.NodeBuilder, error) {
	cfg := &aiadmin.QueryKnowledgeNodeConfig{}
	if err := json.Unmarshal([]byte(nodeDef.NodeConfigJson), cfg); err != nil {
		return nil, fmt.Errorf("反序列化查询知识库节点 '%s' 配置失败: %w", nodeDef.Id, err)
	}
	if len(cfg.DatasetIds) == 0 {
		return nil, fmt.Errorf("知识库节点 '%s' 配置中必须至少指定一个知识库ID (datasetIds)", nodeDef.Id)
	}
	if cfg.ReplyStrategy == 0 { // 如果未指定回复策略，默认为动态回复
		cfg.ReplyStrategy = int32(aiadmin.ReplyStrategy_ReplyStrategyDynamic)
	}
	return &QueryKnowledgeNode{
		nodeDef: nodeDef,
		config:  cfg,
	}, nil
}

// Build 方法修改
// 【修改】接收 LogContext，并在 Lambda 中使用
// Build 方法修改：现在返回一个 common.NodeLogicFunc，移除日志记录部分
func (n *QueryKnowledgeNode) Build(lc *logger.LogContext) (any, error) { // 修改返回类型
	manager := retriever.GetRetrieverManager()
	if manager == nil {
		return nil, fmt.Errorf("RetrieverManager尚未初始化 for node %s", n.nodeDef.Id)
	}

	// 返回节点的纯业务逻辑函数
	var nodeLogic common.NodeLogicFunc = func(stdLibCtx context.Context, p *types.WorkflowPayload) (*types.WorkflowPayload, error) {
		// ---- 实际节点逻辑 ----
		if p == nil {
			err := fmt.Errorf("知识库查询节点 [%s] 接受到的 payload 为 nil", n.nodeDef.Id)
			p = &types.WorkflowPayload{} // 创建一个空的、带错误的payload
			p.Error = err
			return p, nil
		}

		// baselog.Infof("[QueryKnowledgeNode:%s][RunID:%s] Input: UserQuery='%s', HistoryCount=%d, InitialQueryContent='%s'", // lc 不再直接在业务逻辑中使用
		baselog.Infof("[QueryKnowledgeNode:%s] Input: UserQuery='%s', HistoryCount=%d, InitialQueryContent='%s'",
			n.nodeDef.Id, p.CurrentUserQuery, len(p.RecentHistory), p.InitialQuery.Content)

		userQuery := p.CurrentUserQuery
		if userQuery == "" && p.InitialQuery != nil {
			userQuery = p.InitialQuery.Content
		}
		if userQuery == "" {
			err := fmt.Errorf("知识库查询节点 [%s] 无法获取有效的用户输入", n.nodeDef.Id)
			p.Error = err
			return p, nil
		}

		baselog.Debugf("[QueryKnowledgeNode:%s] 使用查询: '%s' 进行检索", n.nodeDef.Id, userQuery)

		// rpcCtxForDBQuery 仍然需要，但是应该从 payload 或 stdLibCtx 中获取，或者通过某种全局服务访问，而不是直接依赖 lc.RpcCtx
		// 考虑到 rpcCtxForDBQuery 主要是用于 DB 操作，可以从外部传入或在更早的阶段绑定到 WorkflowExecutionContext
		// 这里简化为直接使用传入的 lc.RpcCtx (如果 lc 非空)
		var rpcCtxForDBQuery *rpc.Context
		if lc != nil { // 这里的 lc 是 Build 方法的参数，在 nodeLogic 闭包中可用
			rpcCtxForDBQuery = lc.RpcCtx
		} else {
			// 如果 lc 为 nil，创建一个临时的，但这可能导致无法正确路由数据库或缺少追踪信息
			rpcCtxForDBQuery = &rpc.Context{}
			baselog.Warnf("[QueryKnowledgeNode:%s] lc is nil, using new rpc.Context for DB query. Tracing/DB sharding might be affected.", n.nodeDef.Id)
		}

		var allDocs []*schema.Document
		var retrievedDocsMetadata = make([]map[string]interface{}, 4)

		baselog.Debugf("[QueryKnowledgeNode:%s] 准备从 %d 个知识库中检索: %v", n.nodeDef.Id, len(n.config.DatasetIds), n.config.DatasetIds)

		for _, datasetID := range n.config.DatasetIds {
			dataset, errDb := modelrag.ModelDataset.GetById(rpcCtxForDBQuery, datasetID)
			if errDb != nil {
				baselog.Errorf("[QueryKnowledgeNode:%s] 查询知识库(ID: %d)信息失败: %v. 跳过此知识库.", n.nodeDef.Id, datasetID, errDb)
				continue
			}
			if dataset.CollectionName == "" {
				baselog.Errorf("[QueryKnowledgeNode:%s] 知识库(ID: %d) 未关联向量集合(CollectionName is empty). 跳过此知识库.", n.nodeDef.Id, datasetID)
				continue
			}
			baselog.Debugf("    -> 正在检索知识库: %s (ID: %d), 向量集: %s", dataset.Name, datasetID, dataset.CollectionName)

			retrieverInstance, errRetriever := manager.GetRetriever(dataset.CollectionName)
			if errRetriever != nil {
				baselog.Errorf("[QueryKnowledgeNode:%s] 获取 Retriever 实例失败 for collection '%s': %v. 跳过此知识库.", n.nodeDef.Id, dataset.CollectionName, errRetriever)
				continue
			}
			topK := int(n.config.TopK)
			if topK == 0 {
				topK = 3 // 如果 topK 为 0，默认为 3
			}
			runtimeOpts := []einoRetriever.Option{einoRetriever.WithTopK(topK)}
			docs, errSearch := retrieverInstance.GetRelevantDocuments(stdLibCtx, userQuery, runtimeOpts...)
			if errSearch != nil {
				baselog.Errorf("[QueryKnowledgeNode:%s] 从集合 '%s' 检索失败: %v. 跳过此知识库.", n.nodeDef.Id, dataset.CollectionName, errSearch)
				continue
			}
			for _, doc := range docs {
				allDocs = append(allDocs, doc)
				retrievedDocsMetadata = append(retrievedDocsMetadata, map[string]interface{}{
					"doc_id":          doc.ID,
					"score":           doc.Score(),
					"content_snippet": truncateString(doc.Content, 100),
					"dataset_id":      datasetID,
					"collection_name": dataset.CollectionName,
				})
			}
		}

		sort.SliceStable(allDocs, func(i, j int) bool {
			return allDocs[i].Score() > allDocs[j].Score()
		})

		finalTopK := int(n.config.TopK)
		if finalTopK == 0 {
			finalTopK = 3 // 如果topK为0，默认为3
		}
		if len(allDocs) > finalTopK {
			allDocs = allDocs[:finalTopK]
			if len(retrievedDocsMetadata) > finalTopK {
				retrievedDocsMetadata = retrievedDocsMetadata[:finalTopK]
			}
		}

		// 将收集到的文档元数据（含分数）放入 additionalLogData
		if p.CustomData == nil {
			p.CustomData = make(map[string]any)
		}
		if len(retrievedDocsMetadata) > 0 {
			p.CustomData[fmt.Sprintf("log_addon_output_for_node_%s", n.nodeDef.Id)] = map[string]interface{}{
				"retrieved_documents_details":             retrievedDocsMetadata,
				"total_documents_found_before_final_topk": len(allDocs), // 在最终裁剪前的数量
				"final_documents_count_after_topk":        len(allDocs), // 实际用于后续步骤的文档数
			}
		} else {
			p.CustomData[fmt.Sprintf("log_addon_output_for_node_%s", n.nodeDef.Id)] = map[string]interface{}{
				"total_documents_found_before_final_topk": len(allDocs), // 在最终裁剪前的数量
				"final_documents_count_after_topk":        len(allDocs), // 实际用于后续步骤的文档数
			}
		}

		switch n.config.ReplyStrategy {
		case int32(aiadmin.ReplyStrategy_ReplyStrategyStandard): // 标准回复
			baselog.Debugf("[QueryKnowledgeNode:%s] Executing ReplyStrategy: Standard", n.nodeDef.Id)
			if p.FlowControl == nil {
				p.FlowControl = make(map[types.PayloadKey]any)
			}
			if p.CustomData == nil {
				p.CustomData = make(map[string]any)
			}

			if len(allDocs) > 0 {
				// 1. FinalAnswer/DraftAnswer 取最相关的
				p.FinalAnswer = allDocs[0].Content
				p.DraftAnswer = allDocs[0].Content

				// 2. 拼接 RetrievedContext
				var contextBuilder strings.Builder
				numCandidatesToProvide := len(allDocs)
				// 可选：限制最多3个候选
				// if numCandidatesToProvide > 3 { numCandidatesToProvide = 3 }
				for i := 0; i < numCandidatesToProvide; i++ {
					doc := allDocs[i]
					contextBuilder.WriteString(fmt.Sprintf("【标准回复候选 %d】(来源文档ID: %s, 相关度评分: %.4f):\n%s\n\n", i+1, doc.ID, doc.Score(), doc.Content))
				}
				p.RetrievedContext = strings.TrimSpace(contextBuilder.String())

				// 3. CustomData 标记
				p.CustomData["__query_knowledge_strategy__"] = "standard_reply_found_candidates"
				p.CustomData["__query_knowledge_direct_answer__"] = p.FinalAnswer

				p.FlowControl[types.FlowControlNextCondition] = types.ConditionQueryKnowledgeFound
				baselog.Debugf("[QueryKnowledgeNode:%s] Standard Reply: Knowledge found. FinalAnswer set. RetrievedContext prepared with %d candidates. Setting next_condition to '%s'.", n.nodeDef.Id, numCandidatesToProvide, types.ConditionQueryKnowledgeFound)
			} else {
				p.FinalAnswer = "抱歉，知识库中没有找到与您问题相关的直接答案。"
				p.DraftAnswer = p.FinalAnswer
				p.RetrievedContext = "没有找到相关的标准回复候选。"
				p.CustomData["__query_knowledge_strategy__"] = "standard_reply_not_found"
				p.FlowControl[types.FlowControlNextCondition] = types.InternalQKElseTrigger
				baselog.Debugf("[QueryKnowledgeNode:%s] Standard Reply: Knowledge not found. Setting default FinalAnswer. Setting next_condition to trigger 'else'.", n.nodeDef.Id)
			}
			break
		case int32(aiadmin.ReplyStrategy_ReplyStrategyDynamic): // 动态回复
			baselog.Debugf("[QueryKnowledgeNode:%s] Executing ReplyStrategy: Dynamic", n.nodeDef.Id)
			if p.FlowControl == nil {
				p.FlowControl = make(map[types.PayloadKey]any)
			}
			if p.CustomData == nil {
				p.CustomData = make(map[string]any)
			}
			if len(allDocs) > 0 {
				var contextBuilder strings.Builder
				for i, doc := range allDocs {
					contextBuilder.WriteString(fmt.Sprintf("参考信息%d (来源文档ID %s, 相关度评分: %.4f):\n%s\n\n", i+1, doc.ID, doc.Score(), doc.Content))
				}
				p.RetrievedContext = strings.TrimSpace(contextBuilder.String())
				p.FinalAnswer = ""
				p.DraftAnswer = ""
				p.CustomData["__query_knowledge_strategy__"] = "dynamic_reply_found_context"
				p.FlowControl[types.FlowControlNextCondition] = types.ConditionQueryKnowledgeFound
				baselog.Debugf("[QueryKnowledgeNode:%s] Dynamic Reply: Knowledge found. RetrievedContext prepared. Setting next_condition to '%s'. Context length: %d", n.nodeDef.Id, types.ConditionQueryKnowledgeFound, len(p.RetrievedContext))
			} else {
				p.RetrievedContext = "关于您的问题，系统没有检索到相关参考信息。"
				p.FinalAnswer = ""
				p.DraftAnswer = ""
				p.CustomData["__query_knowledge_strategy__"] = "dynamic_reply_not_found"
				p.FlowControl[types.FlowControlNextCondition] = types.InternalQKElseTrigger
				baselog.Debugf("[QueryKnowledgeNode:%s] Dynamic Reply: Knowledge not found. Setting next_condition to trigger 'else'.", n.nodeDef.Id)
			}
			break
		default:
			baselog.Debugf("[QueryKnowledgeNode:%s] 使用【动态回复】策略", n.nodeDef.Id)
			var contextBuilder strings.Builder
			if len(allDocs) > 0 {
				for i, doc := range allDocs {
					contextBuilder.WriteString(fmt.Sprintf("参考信息%d: %s\n", i+1, doc.Content))
				}
			} else {
				contextBuilder.WriteString("没有找到相关的参考信息。")
			}
			p.RetrievedContext = contextBuilder.String()
		}

		if p.FlowControl == nil {
			p.FlowControl = make(map[types.PayloadKey]any)
		}

		if len(allDocs) > 0 { // 知识已找到
			p.FlowControl[types.FlowControlNextCondition] = types.ConditionQueryKnowledgeFound
			baselog.Debugf("[QueryKnowledgeNode:%s] Knowledge found. Setting next_condition to '%s'.", n.nodeDef.Id, types.ConditionQueryKnowledgeFound)
		} else { // 知识未找到
			p.FlowControl[types.FlowControlNextCondition] = types.InternalQKElseTrigger
			baselog.Debugf("[QueryKnowledgeNode:%s] Knowledge not found. Setting next_condition to '%s' to trigger 'else' branch.", n.nodeDef.Id, types.InternalQKElseTrigger)
		}

		return p, nil
	}
	return nodeLogic, nil // 返回 NodeLogicFunc
}
