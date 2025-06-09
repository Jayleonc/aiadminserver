package utils

import (
	"fmt"
	"math/rand"
	"time"
)

func GenerateMilvusCollectionName(corpId uint32) string {
	return fmt.Sprintf("corp%d_index_%s", corpId, time.Now().Format("20060102150405"))
}

func GetEmbeddingId(corpId uint32) string {
	return fmt.Sprintf("embed_%d_%s_%d", corpId, time.Now().Format("20060102150405"), rand.Intn(5000))
}
