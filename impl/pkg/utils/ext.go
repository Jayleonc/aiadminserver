package utils

import (
	"encoding/csv"
	"fmt"
	"github.com/xuri/excelize/v2"
	"os"
	"path/filepath"
	"strings"
)

var headerMap = map[string]string{
	"问题内容":    "question",
	"回复内容":    "answer",
	"分类（非必填）": "category",
}

// XlsxToCsv 自动将 xlsx 文件转换为同目录下的 csv 文件
func XlsxToCsv(xlsxPath string) (string, error) {
	// 打开 xlsx 文件
	f, err := excelize.OpenFile(xlsxPath)
	if err != nil {
		return "", fmt.Errorf("打开 xlsx 文件失败: %w", err)
	}
	defer f.Close()

	// 获取第一个 sheet
	sheetName := f.GetSheetName(0)
	if sheetName == "" {
		return "", fmt.Errorf("找不到任何 sheet")
	}

	rows, err := f.GetRows(sheetName)
	if err != nil {
		return "", fmt.Errorf("读取 sheet 内容失败: %w", err)
	}

	// 自动生成 csv 文件路径
	csvPath := strings.TrimSuffix(xlsxPath, filepath.Ext(xlsxPath)) + ".csv"

	// 创建 csv 文件
	csvFile, err := os.Create(csvPath)
	if err != nil {
		return "", fmt.Errorf("创建 csv 文件失败: %w", err)
	}
	defer csvFile.Close()

	writer := csv.NewWriter(csvFile)
	defer writer.Flush()

	// 写入每一行
	for i, row := range rows {
		if i == 0 {
			continue // 跳过第一行
		}
		if i == 1 {
			// 替换标题行
			for j, col := range row {
				row[j] = strings.TrimSpace(headerMap[strings.TrimSpace(col)])
			}
		}
		if err := writer.Write(row); err != nil {
			return "", fmt.Errorf("写入 csv 文件失败: %w", err)
		}
	}

	return csvPath, nil
}

func XlsxToCsvIfNeeded(xlsxPath string) (csvPath string, alreadyExists bool, err error) {
	csvPath = strings.TrimSuffix(xlsxPath, filepath.Ext(xlsxPath)) + ".csv"

	// 如果 csv 文件已存在，并且 xlsx 没有比它更新，就跳过
	xlsxInfo, err := os.Stat(xlsxPath)
	if err != nil {
		return "", false, fmt.Errorf("无法获取 xlsx 文件信息: %w", err)
	}

	// 获取 csv 文件信息
	csvInfo, err := os.Stat(csvPath)
	if err == nil {
		// 如果 csv 文件存在，并且比 xlsx 文件新，就跳过
		if csvInfo.ModTime().After(xlsxInfo.ModTime()) {
			return csvPath, true, nil
		}
	} else if !os.IsNotExist(err) {
		// 其他错误，比如权限问题，直接报错
		return "", false, fmt.Errorf("无法获取 csv 文件信息: %w", err)
	}

	// 否则执行转换
	csvPath, err = XlsxToCsv(xlsxPath)
	if err != nil {
		return "", false, err
	}
	return csvPath, false, nil
}
