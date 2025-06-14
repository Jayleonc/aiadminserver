package milvus

import (
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
)

const (
	ConsistencyLevelStrong     ConsistencyLevel = 1
	ConsistencyLevelSession    ConsistencyLevel = 2
	ConsistencyLevelBounded    ConsistencyLevel = 3
	ConsistencyLevelEventually ConsistencyLevel = 4
	ConsistencyLevelCustomized ConsistencyLevel = 5

	HAMMING = MetricType(entity.HAMMING)
	JACCARD = MetricType(entity.JACCARD)

	L2     = MetricType(entity.L2)
	IP     = MetricType(entity.IP)
	COSINE = MetricType(entity.COSINE)
)

// defaultSchema is the default schema for milvus by eino
type defaultSchema struct {
	ID       string    `json:"id" milvus:"name:id"`
	Content  string    `json:"content" milvus:"name:content"`
	Vector   []float32 `json:"vector" milvus:"name:vector"`
	Metadata []byte    `json:"metadata" milvus:"name:metadata"`
}

func getDefaultFields() []*entity.Field {
	return []*entity.Field{
		entity.NewField().
			WithName(defaultCollectionID).
			WithDescription(defaultCollectionIDDesc).
			WithIsPrimaryKey(true).
			WithDataType(entity.FieldTypeVarChar).
			WithMaxLength(255),
		entity.NewField().
			WithName(defaultCollectionVector).
			WithDescription(defaultCollectionVectorDesc).
			WithIsPrimaryKey(false).
			WithDataType(entity.FieldTypeFloatVector).
			WithDim(defaultDim),
		entity.NewField().
			WithName(defaultCollectionContent).
			WithDescription(defaultCollectionContentDesc).
			WithIsPrimaryKey(false).
			WithDataType(entity.FieldTypeVarChar).
			WithMaxLength(1024),
		entity.NewField().
			WithName(defaultCollectionMetadata).
			WithDescription(defaultCollectionMetadataDesc).
			WithIsPrimaryKey(false).
			WithDataType(entity.FieldTypeJSON),
	}
}

type ConsistencyLevel entity.ConsistencyLevel

func (c *ConsistencyLevel) getConsistencyLevel() entity.ConsistencyLevel {
	return entity.ConsistencyLevel(*c - 1)
}

// MetricType is the metric type for vector by eino
type MetricType entity.MetricType

// getMetricType returns the metric type
func (t *MetricType) getMetricType() entity.MetricType {
	return entity.MetricType(*t)
}
