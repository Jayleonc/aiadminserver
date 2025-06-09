// internal/modelrag/docstore/csv_parser.go
package docstore

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"git.pinquest.cn/base/log"
	einoschema "github.com/cloudwego/eino/schema"
	"github.com/tmc/langchaingo/schema"
	"github.com/tmc/langchaingo/textsplitter"
	"io"
	"slices"
	"strings"
)

type CSVWithHeaderRow struct {
	r              io.Reader
	headerRowIndex int
	columns        []string
}

func NewCSVWithHeaderRow(r io.Reader, headerRowIndex int, columns ...string) CSVWithHeaderRow {
	return CSVWithHeaderRow{
		r:              r,
		headerRowIndex: headerRowIndex,
		columns:        columns,
	}
}

func (c CSVWithHeaderRow) Load(_ context.Context) ([]schema.Document, error) {
	reader := csv.NewReader(c.r)
	// *** 关键修改：设置 FieldsPerRecord 为 -1，表示不检查每条记录的字段数量是否一致 ***
	reader.FieldsPerRecord = -1

	var header []string
	var docs []schema.Document
	var rown int

	// 读取头部行
	for i := 0; i <= c.headerRowIndex; i++ {
		row, err := reader.Read()
		if err != nil {
			return nil, fmt.Errorf("读取第 %d 行失败: %w", i+1, err)
		}
		if i == c.headerRowIndex {
			header = row
		}
	}

	// 读取数据行
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			// 对于 CSV 解析错误，打印详细信息以便调试
			var parseErr *csv.ParseError
			if errors.As(err, &parseErr) {
				return nil, fmt.Errorf("CSV 解析错误在行 %d, 列 %d: %s, 原始错误: %w", parseErr.Line, parseErr.Column, parseErr.Err.Error(), err)
			}
			return nil, fmt.Errorf("读取 CSV 行失败: %w", err)
		}

		var content []string

		for i, value := range row {
			// *** 安全检查：确保当前字段索引 i 不会超出 header 的范围 ***
			if i >= len(header) {
				// 如果当前行的字段比头部多，忽略多余的字段
				// 比如头是 问题内容,回复内容,分类
				// 可内容是 我的账户无法登录,请重置密码,账户类,多余内容
				// 也就是说，row 的长度是 4，超出了 header 的长度。
				log.Warnf("第 %d 行字段过多，字段值: %v 被忽略", rown, value)
				break // 退出当前行的字段遍历
			}

			// 如果设置了 filterCols 且当前列不在过滤列表中，则跳过
			if len(c.columns) > 0 && !slices.Contains(c.columns, header[i]) {
				continue
			}
			content = append(content, fmt.Sprintf("%s: %s", header[i], value))
		}
		rown++

		// 构建文档
		doc := schema.Document{
			PageContent: strings.Join(content, "\n"),
			Metadata:    make(map[string]any),
		}
		doc.Metadata["row"] = rown // 添加行号到元数据

		// 将所有列的值添加到元数据，即使是空的
		for i, value := range row {
			if i < len(header) { // 确保索引不会超出 header 的范围
				doc.Metadata[header[i]] = value
			}
		}
		docs = append(docs, doc)
	}

	return docs, nil
}

func (c CSVWithHeaderRow) LoadAndSplit(ctx context.Context, splitter textsplitter.TextSplitter) ([]schema.Document, error) {
	docs, err := c.Load(ctx)
	if err != nil {
		return nil, err
	}
	return textsplitter.SplitDocuments(splitter, docs)
}

// LoadAndSplitCSVWithHeaderRow 使用自定义 CSVWithHeaderRow 加载器
func LoadAndSplitCSVWithHeaderRow(
	r io.Reader,
	chunkSize, chunkOverlap int,
	headerRowIndex int,
	filterCols ...string,
) ([]*einoschema.Document, error) {
	loader := NewCSVWithHeaderRow(r, headerRowIndex, filterCols...)
	splitter := textsplitter.NewRecursiveCharacter(textsplitter.WithSeparators([]string{"\n\n"}))
	splitter.ChunkSize = chunkSize
	splitter.ChunkOverlap = chunkOverlap

	docs, err := loader.LoadAndSplit(context.Background(), splitter)
	if err != nil {
		return nil, err
	}
	return LangchaingoToEinoDocs(docs), nil
}

// LoadAndSplitCSV 从 CSV 加载并切割成 eino.Document
func LoadAndSplitCSV(r io.Reader, chunkSize, chunkOverlap int, filterCols ...string) ([]*einoschema.Document, error) {
	return LoadAndSplitCSVWithHeaderRow(r, chunkSize, chunkOverlap, 0, filterCols...)
}

//func LoadAndSplitCSV(r io.Reader, chunkSize, chunkOverlap int, filterCols ...string) ([]*einoschema.Document, error) {
//	loader := documentloaders.NewCSV(r, filterCols...)
//	splitter := textsplitter.NewRecursiveCharacter(textsplitter.WithSeparators([]string{"\n\n"}))
//	splitter.ChunkSize = chunkSize
//	splitter.ChunkOverlap = chunkOverlap
//
//	docs, err := loader.LoadAndSplit(context.Background(), splitter)
//	if err != nil {
//		return nil, err
//	}
//	return LangchaingoToEinoDocs(docs), nil
//}
