package docstore

import (
	"fmt"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/storage"
	"net/url"
	"os"
	"path/filepath"
)

func DownloadToLocal(ctx *rpc.Context, corpId uint32, fileUrl string, fileName string) (string, error) {
	u, err := url.Parse(fileUrl)
	if err != nil {
		return "", fmt.Errorf("invalid file_url: %v", err)
	}
	cleanPath := u.Path[1:]

	res, err := storage.Download(ctx, &storage.DownloadReq{
		CorpId: corpId,
		Path:   cleanPath,
	})
	if err != nil {
		return "", fmt.Errorf("download failed: %v", err)
	}

	localPath := fmt.Sprintf("/tmp/faq_downloads/%d_%s", corpId, fileName)
	if err := os.MkdirAll(filepath.Dir(localPath), 0755); err != nil {
		return "", err
	}
	if err := os.WriteFile(localPath, res.GetBuf(), 0644); err != nil {
		return "", err
	}
	return localPath, nil
}
