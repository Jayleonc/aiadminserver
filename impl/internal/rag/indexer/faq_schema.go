// internal/modelrag/schema/faq_schema.go
package indexer

import (
	"bytes"
	"encoding/binary"
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
)

const (
	FieldID       = "id"
	FieldVector   = "vector"
	FieldContent  = "content"  // 问答摘要（合并内容）
	FieldMetadata = "metadata" // JSON 格式，包含所有业务字段
	VectorDim     = 1536       // 默认维度（bge-small）
)

func BuildMilvusFAQSchema() []*entity.Field {
	return []*entity.Field{
		entity.NewField().
			WithName(FieldID).
			WithIsPrimaryKey(true).
			WithIsAutoID(false).
			WithDataType(entity.FieldTypeVarChar).
			WithMaxLength(64),

		entity.NewField().
			WithName(FieldVector).
			WithDataType(entity.FieldTypeFloatVector).
			WithDim(VectorDim),

		entity.NewField().
			WithName(FieldContent).
			WithDataType(entity.FieldTypeVarChar).
			WithMaxLength(2048),

		entity.NewField().
			WithName(FieldMetadata).
			WithDataType(entity.FieldTypeJSON),
	}
}

type FAQMetaData struct {
	FaqId                  uint64 `json:"faq_id"` // 将 FAQ 的 ID 存储在 metadata 中，方便后续操作
	Question               string `json:"question"`
	Answer                 string `json:"answer"`
	FaqType                uint32 `json:"faq_type"`
	SourceType             string `json:"source_type"`
	SourceId               uint64 `json:"source_id"`
	DatasetId              uint64 `json:"dataset_id"`
	CategoryId             uint64 `json:"category_id"`
	SpeechcraftMaterialIds string `json:"speechcraft_material_ids"` // 话术库的ids 直接去 quan_material 表查询，quan 应该有对应方法
	HasManualMedia         bool   `json:"has_manual_media"`         // 如果这个为 true，表示有手动上传的素材，先去 rag_faq_manual_media 表中查询 material_id，再去 rag_manual_material 表中查询素材信息
}

// internal/modelrag/schema/faq_schema.go 中定义
func vector2Bytes(vec []float64) []byte {
	buf := new(bytes.Buffer)
	for _, f := range vec {
		_ = binary.Write(buf, binary.LittleEndian, float32(f))
	}
	return buf.Bytes()
}
