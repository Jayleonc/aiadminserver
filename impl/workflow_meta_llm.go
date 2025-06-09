package impl

import (
	"git.pinquest.cn/base/aiadmin"
	"git.pinquest.cn/base/aiadminserver/impl/pkg/llm"
	"git.pinquest.cn/qlb/brick/rpc"
)

func GetLLMModels(ctx *rpc.Context, req *aiadmin.GetLLMModelsReq) (*aiadmin.GetLLMModelsRsp, error) {
	var rsp aiadmin.GetLLMModelsRsp
	rsp.List = []*aiadmin.LLMModelMeta{}

	codeCatalog := llm.GetModelCatalog()

	configuredAccounts := S.Conf.LLMProviders

	for providerType := range configuredAccounts {
		if modelsFromProvider, providerExists := codeCatalog[providerType]; providerExists {
			for _, model := range modelsFromProvider {
				// 检查请求中是否有类型过滤
				if req.Type != "" && req.Type != model.Type {
					continue
				}

				rsp.List = append(rsp.List, &aiadmin.LLMModelMeta{
					Model:       model.Model,
					Type:        model.Type,
					Provider:    model.Provider,
					Description: model.Description,
				})
			}
		}
	}
	return &rsp, nil
}
