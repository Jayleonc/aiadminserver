package config

var DefaultDatasetConfig = struct {
	EmbeddingModel         string
	EmbeddingModelProvider string
	IndexingTechnique      string
	IndexStruct            string
	RetrievalModel         string
	BuiltInFieldEnabled    bool
}{
	EmbeddingModel:         "text-embedding-v2",
	EmbeddingModelProvider: "tongyi",
	IndexingTechnique:      "HNSW", // todo 记得研究下，这个是什么玩意。
	IndexStruct:            "flat",
	RetrievalModel:         "default",
	BuiltInFieldEnabled:    true,
}
