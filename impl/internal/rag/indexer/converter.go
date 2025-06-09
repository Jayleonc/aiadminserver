package indexer

import (
	"encoding/json"
	"git.pinquest.cn/base/aiadmin"
	"git.pinquest.cn/base/log"
	einoschema "github.com/cloudwego/eino/schema"
)

// ConvertFAQToEinoDoc 将数据库中的 FAQ 记录转为向量库使用的 eino.Document
func ConvertFAQToEinoDoc(f *aiadmin.ModelFAQ) *einoschema.Document {
	var content string
	if f.Question != "" {
		content += f.Question + "\n"
	}
	if f.Answer != "" {
		content += f.Answer
	}

	metaData := FAQMetaData{
		FaqId:                  f.Id,
		Question:               f.Question,
		Answer:                 f.Answer,
		FaqType:                f.FaqType,
		SourceType:             f.SourceType,
		SourceId:               f.SourceId,
		DatasetId:              f.DatasetId,
		CategoryId:             f.CategoryId,
		SpeechcraftMaterialIds: f.SpeechcraftMaterialIds,
		HasManualMedia:         f.HasManualMedia,
	}

	var metaDataMap map[string]any
	metaDataBytes, err := json.Marshal(metaData)
	if err != nil {
		log.Printf("Error marshaling FAQMetaData: %v", err)
		metaDataMap = make(map[string]any)
	} else {
		if err := json.Unmarshal(metaDataBytes, &metaDataMap); err != nil {
			log.Printf("Error unmarshaling FAQMetaData to map[string]any: %v", err)
			metaDataMap = make(map[string]any)
		}
	}

	return &einoschema.Document{
		ID:       f.EmbeddingId, // 也可通过 embedding Id 回溯 FAQ
		Content:  content,
		MetaData: metaDataMap,
	}

}
