package service

import (
	"context"
	"errors"
	"fmt"
	"git.pinquest.cn/base/aiadminserver/impl/internal/rag/indexer"
	"git.pinquest.cn/base/aiadminserver/impl/pkg/utils/retry"
	einoschema "github.com/cloudwego/eino/schema"
	"time"
)

// DeleteMilvusVectors 删除 Milvus 向量，并附带重试机制
func DeleteMilvusVectors(ctx context.Context, collectionName string, embeddingIds []string) error {
	op := func() error {
		idx, err := indexer.GetIndexer().GetOrInitIndexer(ctx, collectionName)
		if err != nil {
			return err
		}
		if idx == nil {
			return errors.New("indexer is nil")
		}
		return idx.Delete(ctx, embeddingIds)
	}

	return retry.Do(op,
		retry.WithRetryTimes(5),
		retry.WithInterval(time.Second*3),
		retry.WithLogPrefix(fmt.Sprintf("Milvus.DeleteVectors.%s", collectionName)),
	)

}

// StoreMilvusVectors 存储 Milvus 向量，并附带重试机制
func StoreMilvusVectors(ctx context.Context, collectionName string, docs []*einoschema.Document) ([]string, error) {
	var storedIds []string
	op := func() error {
		idx, err := indexer.GetIndexer().GetOrInitIndexer(ctx, collectionName)
		if err != nil {
			return err
		}
		if idx == nil {
			return errors.New("indexer is nil")
		}

		ids, storeErr := idx.Store(ctx, docs)
		if storeErr == nil {
			storedIds = ids
		}

		return storeErr
	}

	err := retry.Do(op,
		retry.WithRetryTimes(5),
		retry.WithInterval(time.Second*3),
		retry.WithLogPrefix(fmt.Sprintf("Milvus.StoreVectors.%s", collectionName)),
	)
	return storedIds, err
}

func DeleteMilvusCollection(ctx context.Context, collectionName string) error {
	if collectionName == "" {
		return errors.New("collection name is empty")
	}

	op := func() error {
		idx, err := indexer.GetIndexer().GetOrInitIndexer(ctx, collectionName)
		if err != nil {
			return err
		}
		if idx == nil {
			return errors.New("indexer is nil")
		}
		return idx.DeleteCollection(ctx, collectionName)
	}

	return retry.Do(op,
		retry.WithRetryTimes(5),
		retry.WithInterval(time.Second*3),
		retry.WithLogPrefix(fmt.Sprintf("Milvus.DeleteCollection.%s", collectionName)),
	)
}
