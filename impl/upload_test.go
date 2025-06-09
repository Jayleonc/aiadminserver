package impl

import (
	"fmt"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/storage"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestUpload(t *testing.T) {
	// 1. 读取本地文件，模拟上传
	path := "test.xls"
	fileContent, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("failed to read file: %v", err)
	}

	ext := filepath.Ext(path)

	// 2. 构造上传请求
	req := &storage.UploadV2Req{
		CorpId:       2000772,
		ResourceType: "quan_pub",
		FileData:     fileContent,
		FileSuffix:   ext,
		FileName:     filepath.Base(path),
	}

	// 3. 执行上传
	resp, err := storage.UploadV2(&rpc.Context{}, req)
	if err != nil {
		t.Fatalf("upload failed: %v", err)
	}

	// 4. 打印返回结果
	t.Logf("upload success: %+v", resp)
}

func TestDownloadUploadedFile(t *testing.T) {
	// 1. 上传后返回的 file_url（带 token）
	url1 := "https://quan00-1259287960.cos.ap-guangzhou.myqcloud.com/2000772/aa0c6d72aa8f45feb60b9d63f7ee6505.xlsx"
	u, err := url.Parse(url1)
	if err != nil {
		t.Fatalf("failed to parse url: %v", err)
	}

	// 去掉路径前导的 "/"
	cleanPath := strings.TrimPrefix(u.Path, "/")
	fmt.Println("clean path:", cleanPath)

	download, err := storage.Download(&rpc.Context{}, &storage.DownloadReq{
		CorpId: 2000772,
		Path:   cleanPath,
	})
	if err != nil {
		t.Fatalf("download failed: %v", err)
	}

	t.Logf("download success: %+v", download.GetBuf())

}
