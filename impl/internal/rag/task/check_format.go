package task

import (
	"encoding/json"
	"errors"
	"fmt"
	"git.pinquest.cn/base/aiadminserver/impl/internal/rag/docstore"
	"git.pinquest.cn/base/xls"
	"git.pinquest.cn/qlb/core"
	"strings"

	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/commonv2"
	"github.com/xuri/excelize/v2"
)

func CheckFAQFormatTask(ctx *rpc.Context) (*commonv2.AsyncTaskResult, error) {
	result := &commonv2.AsyncTaskResult{}
	result.State = uint32(commonv2.AsyncTaskState_AsyncTaskStateFailure) // 默认失败
	corpId := core.MustGetPinHeaderCorpId2Uint32(ctx)

	fileUrl := ctx.GetReqHeader("file_url")
	fileName := ctx.GetReqHeader("file_name")

	requiredHeaders := []string{"问题内容", "回复内容", "分类（非必填）"}

	// Step 1: 下载文件
	localPath, err := docstore.DownloadToLocal(ctx, corpId, fileUrl, fileName)
	if err != nil {
		result.Error = &commonv2.ErrMsg{
			ErrCode: rpc.KSystemError,
			ErrMsg:  "文件下载失败，请稍后重试",
		}
		return result, fmt.Errorf("下载文件失败: %w", err)
	}

	// Step 2: 判断扩展名并调用对应解析函数
	lowerName := strings.ToLower(fileName)
	if strings.HasSuffix(lowerName, ".xls") {
		return checkXlsFile(localPath, fileUrl, requiredHeaders)
	}
	if strings.HasSuffix(lowerName, ".xlsx") {
		return checkXlsxFile(localPath, fileUrl, requiredHeaders)
	}

	result.Error = &commonv2.ErrMsg{
		ErrCode: rpc.KErrRequestBodyReadFail,
		ErrMsg:  "不支持的文件格式，请上传 .xls 或 .xlsx 文件",
	}
	return result, fmt.Errorf("不支持的文件格式，请上传 .xls 或 .xlsx 文件")
}

func checkXlsxFile(localPath, fileUrl string, requiredHeaders []string) (*commonv2.AsyncTaskResult, error) {
	result := &commonv2.AsyncTaskResult{}
	result.State = uint32(commonv2.AsyncTaskState_AsyncTaskStateFailure) // 默认失败
	f, err := excelize.OpenFile(localPath)
	if err != nil {
		result.Error = &commonv2.ErrMsg{
			ErrCode: rpc.KErrRequestBodyReadFail,
			ErrMsg:  "Excel 文件不匹配模板，请使用系统提供的模版进行上传",
		}
		return result, fmt.Errorf("无法打开 .xlsx 文件: %w", err)
	}
	defer f.Close()

	sheetList := f.GetSheetList()
	if len(sheetList) == 0 {
		result.Error = &commonv2.ErrMsg{
			ErrCode: rpc.KErrRequestBodyReadFail,
			ErrMsg:  "Excel 文件中没有任何工作表，请检查文件内容",
		}
		return result, fmt.Errorf("excel 文件中没有任何工作表，请检查文件内容")
	}
	sheetName := sheetList[0]
	rows, err := f.GetRows(sheetName)
	if err != nil || len(rows) < 2 {
		result.Error = &commonv2.ErrMsg{
			ErrCode: rpc.KErrRequestBodyReadFail,
			ErrMsg:  "Excel 文件格式错误或数据为空",
		}
		return result, fmt.Errorf("excel 文件格式错误或数据为空")
	}
	if len(rows)-2 > 2000 {
		result.Error = &commonv2.ErrMsg{
			ErrCode: rpc.KErrRequestBodyReadFail,
			ErrMsg:  fmt.Sprintf("Excel 数据行数超过上限（2000条），当前为 %d 条", len(rows)-2),
		}
		return result, errors.New("数据行过多")
	}

	headerRow := rows[1]
	return checkHeaderFields(localPath, fileUrl, sheetName, headerRow, requiredHeaders)
}

func checkXlsFile(localPath, fileUrl string, requiredHeaders []string) (*commonv2.AsyncTaskResult, error) {
	result := &commonv2.AsyncTaskResult{}
	result.State = uint32(commonv2.AsyncTaskState_AsyncTaskStateFailure) // 默认失败
	wb, err := xls.Open(localPath, "utf-8")
	if err != nil || wb == nil {
		result.Error = &commonv2.ErrMsg{
			ErrCode: rpc.KErrRequestBodyReadFail,
			ErrMsg:  "Excel 文件不匹配模板，请使用系统提供的模版进行上传",
		}
		return result, fmt.Errorf("无法打开 .xls 文件: %w", err)
	}
	sheet := wb.GetSheet(0)
	if sheet == nil || sheet.MaxRow < 2 {
		result.Error = &commonv2.ErrMsg{
			ErrCode: rpc.KErrRequestBodyReadFail,
			ErrMsg:  "Excel 文件中没有有效数据或表头",
		}
		return result, fmt.Errorf("excel 文件中没有有效数据或表头")
	}
	if sheet.MaxRow-2 > 2000 {
		result.Error = &commonv2.ErrMsg{
			ErrCode: rpc.KErrRequestBodyReadFail,
			ErrMsg:  fmt.Sprintf("Excel 数据行数超过上限（2000条），当前为 %d 条", sheet.MaxRow-2),
		}
		return result, fmt.Errorf("数据行过多")
	}

	// 第 1 行为标题，第 2 行为表头（下标从 0 开始）
	var headerRow []string
	header, _ := sheet.Row(1)
	for i := 0; i < header.LastCol(); i++ {
		headerRow = append(headerRow, strings.TrimSpace(header.Col(i)))
	}

	return checkHeaderFields(localPath, fileUrl, sheet.Name, headerRow, requiredHeaders)
}

func checkHeaderFields(localPath, fileUrl, sheetName string, headerRow, requiredHeaders []string) (*commonv2.AsyncTaskResult, error) {
	result := &commonv2.AsyncTaskResult{}
	result.State = uint32(commonv2.AsyncTaskState_AsyncTaskStateFailure) // 默认失败
	headerMap := make(map[string]bool)
	for _, h := range headerRow {
		headerMap[strings.TrimSpace(h)] = true
	}

	var missing []string
	for _, h := range requiredHeaders {
		if !headerMap[h] {
			missing = append(missing, h)
		}
	}
	if len(missing) > 0 {
		result.Error = &commonv2.ErrMsg{
			ErrCode: rpc.KErrRequestBodyReadFail,
			ErrMsg:  "Excel 文件不匹配模板，请使用系统提供的模版进行上传",
		}
		return result, fmt.Errorf("缺少字段: %s", strings.Join(missing, ", "))
	}

	info := map[string]interface{}{
		"local_path":       localPath,
		"file_url":         fileUrl,
		"sheet_name":       sheetName,
		"required_headers": requiredHeaders,
		"actual_headers":   headerRow,
	}
	buf, _ := json.Marshal(info)

	result.State = uint32(commonv2.AsyncTaskState_AsyncTaskStateSuccess) // 成功
	result.Buf = string(buf)
	return result, nil
}
