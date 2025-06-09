// internal/modelrag/docstore/converter.go
package docstore

import (
	"git.pinquest.cn/base/aiadminserver/impl/pkg/utils"
	einoschema "github.com/cloudwego/eino/schema"
	langschema "github.com/tmc/langchaingo/schema"
	"strings"
)

// LangchaingoToEinoDocs 改进版：保留原始字段名（如 问题内容、回复内容、分类）
func LangchaingoToEinoDocs(docs []langschema.Document) []*einoschema.Document {
	res := make([]*einoschema.Document, 0, len(docs))
	for _, d := range docs {
		fields, err := parseQAFromContent(d.PageContent)
		if err != nil {
			continue
		}

		q := strings.TrimSpace(fields["question"])
		a := strings.TrimSpace(fields["answer"])

		if q == "" && a == "" {
			continue
		}

		e := &einoschema.Document{
			Content:  q + "\n" + a,
			MetaData: utils.StringMapToAnyMap(fields), // 直接传回字段 map，已是英文
		}
		res = append(res, e)
	}
	return res
}

// parseQAFromContent 将 PageContent 解析为字段名 -> 值的 map，保留原始字段名
func parseQAFromContent(content string) (map[string]string, error) {
	lines := strings.Split(content, "\n")
	data := map[string]string{}
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}
		field := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])
		if field != "" {
			data[field] = value
		}
	}
	return data, nil
}
