package impl

import (
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/qmsg"
	"git.pinquest.cn/qlb/storage"
	"net/url"
	"strings"
)

func getVideoVoiceTime(ctx *rpc.Context, videoUrl string, corpId uint32) (uint32, error) {
	URL, err := url.Parse(videoUrl)
	if err != nil {
		log.Errorf("err:%v", err)
		return 0, rpc.InvalidArg("parse video failed")
	}
	filePath := strings.TrimLeft(URL.Path, "/")
	//视频的时候必须要获取时长
	metaInfo, err := storage.GetMediaMetaInfo(ctx, &storage.GetMediaMetaInfoReq{
		ResourceType: s.Conf.ResourceType4CI,
		FileName:     filePath,
		CorpId:       corpId,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return 0, rpc.InvalidArg("video not found")
	}
	if metaInfo.Video == nil || metaInfo.Video.Duration == 0 {
		return 0, rpc.InvalidArg("parse video duration failed")
	}
	log.Infof("%+v", metaInfo)
	return uint32(metaInfo.Video.Duration), nil
}

func getVoiceTime(ctx *rpc.Context, videoUrl string, corpId uint32) (uint32, error) {
	URL, err := url.Parse(videoUrl)
	if err != nil {
		log.Errorf("err:%v", err)
		return 0, rpc.InvalidArg("parse video failed")
	}
	filePath := strings.TrimLeft(URL.Path, "/")
	//视频的时候必须要获取时长
	metaInfo, err := storage.GetMediaMetaInfo(ctx, &storage.GetMediaMetaInfoReq{
		ResourceType: s.Conf.ResourceType4CI,
		FileName:     filePath,
		CorpId:       corpId,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return 0, rpc.InvalidArg("video not found")
	}
	if metaInfo.AudioDuration == 0 {
		return 0, rpc.InvalidArg("parse audio duration failed")
	}

	return uint32(metaInfo.AudioDuration), nil
}

func getGetMediaSnapshotRsp(ctx *rpc.Context, videoUrl string, frame, corpId uint32) (string, error) {
	URL, err := url.Parse(videoUrl)
	if err != nil {
		log.Errorf("err:%v", err)
		return "", rpc.InvalidArg("parse video failed")
	}
	filePath := strings.TrimLeft(URL.Path, "/")
	//视频的时候必须要获取时长
	res, err := storage.GetMediaSnapshot(ctx, &storage.GetMediaSnapshotReq{
		ResourceType: s.Conf.ResourceType4CI,
		FileName:     filePath,
		Time:         frame,
		CorpId:       corpId,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return "", rpc.InvalidArg("file not found")
	}
	return res.SnapshotUrl, nil
}
func GetMediaInfo(ctx *rpc.Context, req *qmsg.GetMediaInfoReq) (*qmsg.GetMediaInfoRsp, error) {
	var rsp qmsg.GetMediaInfoRsp
	user, err := getUserCheck(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	if req.MediaType == uint32(qmsg.GetMediaInfoReq_MediaTypeAudio) ||
		strings.Contains(req.MediaUrl, ".amr") ||
		strings.Contains(req.MediaUrl, ".wav") {
		var duration uint32
		duration, err = getVoiceTime(ctx, req.MediaUrl, user.CorpId)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
		rsp.MediaDuration = duration
	} else {
		r, err := getMediaInfo(ctx, req.MediaUrl, user.CorpId)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
		rsp = *r
	}

	return &rsp, nil
}
func getMediaInfo(ctx *rpc.Context, mediaUrl string, corpId uint32) (*qmsg.GetMediaInfoRsp, error) {
	var rsp qmsg.GetMediaInfoRsp
	duration, err := getVideoVoiceTime(ctx, mediaUrl, corpId)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	//封面
	snapshot, err := getGetMediaSnapshotRsp(ctx, mediaUrl, 1, corpId)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	rsp.MediaDuration = duration
	rsp.MediaCoverUrl = snapshot
	return &rsp, nil
}
