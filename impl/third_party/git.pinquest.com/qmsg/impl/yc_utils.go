package impl

import (
	"encoding/base64"
	"encoding/json"
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/brick/sos"
	"git.pinquest.cn/qlb/brick/utils"
	"git.pinquest.cn/qlb/core"
	"git.pinquest.cn/qlb/featswitch"
	"git.pinquest.cn/qlb/iquan"
	"git.pinquest.cn/qlb/qmsg"
	"git.pinquest.cn/qlb/qorder"
	"git.pinquest.cn/qlb/qrobot"
	"git.pinquest.cn/qlb/qrt"
	"git.pinquest.cn/qlb/quan"
	"git.pinquest.cn/qlb/qwhale"
	"git.pinquest.cn/qlb/yc"
	"git.pinquest.cn/qlb/yc/ycchatmsg"
	"regexp"
	"strings"
)

type YcMerchant struct {
	key string
	ts  uint32
	m   *yc.ModelMerchant
}

var ycMerchant *YcMerchant

func NewYcMerchant(key string) *YcMerchant {
	return &YcMerchant{key: key}
}
func (p *YcMerchant) GetModel(ctx *rpc.Context) (*yc.ModelMerchant, error) {
	if p.m != nil {
		now := utils.Now()
		if p.ts+60 >= now {
			return p.m, nil
		}
	}
	rsp, err := yc.GetMerchantByKey(ctx, &yc.GetMerchantByKeyReq{Key: p.key})
	if err != nil {
		log.Errorf("err:%v", err)
		//warning.ReportMsg(ctx, "get merchant key for %v err:%v", p.key, err)
		if rpc.GetErrCode(err) < 0 && p.m != nil {
			return p.m, nil
		}
		return nil, err
	}
	p.m = rsp.Merchant
	p.ts = utils.Now()
	return p.m, nil
}
func getMerchant(ctx *rpc.Context) (*yc.ModelMerchant, error) {
	if ycMerchant == nil {
		ycMerchant = NewYcMerchant(s.Conf.MerchantKey)
	}
	return ycMerchant.GetModel(ctx)
}
func getMerchantId(ctx *rpc.Context) (string, error) {
	merchant, err := getMerchant(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return "", err
	}
	return merchant.StrMerchantId, nil
}
func coverChatMsg2YcMsg(ctx *rpc.Context, corpId, appId uint32, robotUid uint64, msgList []*qrobot.ChatMsg) ([]*yc.SendMessagesData, error) {
	ycChatMsg := ycchatmsg.NewChatMsgList()
	for _, x := range msgList {
		switch x.ChatMsgType {
		case uint32(qmsg.ChatMsgType_ChatMsgTypeText):
			ycChatMsg.AddText(x.Msg)
		case uint32(qmsg.ChatMsgType_ChatMsgTypeImage):
			ycChatMsg.AddImage(x.Msg)

		case uint32(qmsg.ChatMsgType_ChatMsgTypeVoice):
			var tmp qmsg.ChatVoiceMsg
			err := utils.Json2Pb(x.Msg, &tmp)
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, err
			}

			duration := tmp.VoiceTime
			if duration == 0 {
				var d uint32
				d, err = getVoiceTime(ctx, tmp.VoiceUrl, corpId)
				if err != nil {
					log.Errorf("err:%v", err)
					return nil, err
				}
				duration = uint64(d)
			}

			ycChatMsg.AddVoiceV3(tmp.VoiceUrl, tmp.VoiceSilkUrl, duration)

		case uint32(qmsg.ChatMsgType_ChatMsgTypeVideo):
			ycChatMsg.AddVideoV2(x.Msg, uint64(x.VoiceTime), x.Href)
		case uint32(qmsg.ChatMsgType_ChatMsgTypeWeb):
			var web qmsg.ChatWebMsg
			err := utils.Json2Pb(x.Msg, &web)
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, err
			}
			ycChatMsg.AddImageAndLink(web.IconUrl, web.Href, web.Title, web.Desc)
		//case uint32(qmsg.ChatMsgType_ChatMsgTypeFriendCard):
		//	ycChatMsg.AddFriendCard(x.Msg)
		case uint32(qmsg.ChatMsgType_ChatMsgTypeFile):
			ycChatMsg.AddFile(x.Msg, x.Title, x.Desc)
		case uint32(qmsg.ChatMsgType_ChatMsgTypeWxApp):
			corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)
			byteContent, icon, err := genWxAppContent(ctx, corpId, appId, robotUid, x.MaterialId, x.SosObjectId, x.MiniProgramTitle, x.MiniProgramPath, x.MiniProgramReplacePath)
			if err != nil {
				log.Errorf("err: %v", err)
				return nil, err
			}
			if byteContent == nil {
				log.Warnf("get wx app content empty")
				return nil, err
			}
			var base64Body = base64.StdEncoding.EncodeToString(byteContent)
			ycChatMsg.AddWxAppV2(base64Body, x.SosObjectId, x.MiniProgramName, x.MiniProgramTitle, x.MiniProgramPath, x.MiniProgramCoverUrl, icon, x.MaterialId)
		case uint32(qmsg.ChatMsgType_ChatMsgTypeRichTxt):
			if len(x.AtList) > 0 {
				runes := []rune(x.Msg)
				var ats []*yc.SendMessagesData_AtsItem
				last := 0
				for _, item := range x.AtList {
					loc := int(item.LocationAt)
					if len(runes) < loc {
						loc = len(runes)
					}
					if loc > last {
						text := string(runes[last:loc])
						ats = append(ats, &yc.SendMessagesData_AtsItem{Type: 0, Value: text})
					}
					ats = append(ats, &yc.SendMessagesData_AtsItem{Type: 1, Value: item.StrMemberId})
					last = loc
				}
				if last < len(runes) {
					ats = append(ats, &yc.SendMessagesData_AtsItem{Type: 0, Value: string(runes[last:])})
				}
				ycChatMsg.AddRichTextV2(x.Msg, ats)
			} else {
				ycChatMsg.AddRichText(x.Msg, ycchatmsg.AtType(x.At), x.AtContactSerialNos, ycchatmsg.AtLocation(x.AtLocation))
			}
		case uint32(qmsg.ChatMsgType_ChatMsgTypeChannelMsg):
			channelMsg := ycchatmsg.ChannelMsg{
				ChannelMsgName:     x.ChannelMsgName,
				ChannelMsgIcon:     x.ChannelMsgIcon,
				ChannelMsgTitle:    x.ChannelMsgTitle,
				ChannelMsgCoverUrl: x.ChannelMsgCoverUrl,
				ChannelMsgHref:     x.ChannelMsgHref,
				MsgSerialNo:        x.MsgSerialNo,
			}
			ycChatMsg.AddChannelMsgChat(channelMsg)
		case uint32(qmsg.ChatMsgType_ChatMsgTypeChannelLiveMsg):
			channelMsg := ycchatmsg.ChannelMsg{
				ChannelMsgName:     x.ChannelMsgName,
				ChannelMsgIcon:     x.ChannelMsgIcon,
				ChannelMsgTitle:    x.ChannelMsgTitle,
				ChannelMsgCoverUrl: x.ChannelMsgCoverUrl,
				ChannelMsgHref:     x.ChannelMsgHref,
				MsgSerialNo:        x.MsgSerialNo,
			}
			ycChatMsg.AddChannelLiveMsgChat(channelMsg)
		case uint32(qmsg.ChatMsgType_ChatMsgTypeStoreProduct):
			corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)
			productRsp, err := qorder.GetStoreProductListSys(ctx, &qorder.GetStoreProductListSysReq{
				ListOption: core.NewListOption().SetSkipCount().SetLimit(1).AddOpt(qorder.GetStoreProductListSysReq_ListOptionStoreProductIds, x.StoreProductId),
				CorpId:     corpId,
				AppId:      appId,
			})
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, err
			}

			if len(productRsp.List) == 0 {
				return nil, rpc.InvalidArg("product not found")
			}

			product := productRsp.List[0]

			x.MiniProgramPath = strings.ReplaceAll(x.MiniProgramPath, "{{ .ProductId }}", product.Pid)

			byteContent, icon, err := genWxAppContent(ctx, corpId, appId, robotUid, x.MaterialId, x.SosObjectId, x.MiniProgramTitle, x.MiniProgramPath, x.MiniProgramReplacePath)
			if err != nil {
				log.Errorf("err: %v", err)
				return nil, err
			}
			if byteContent == nil {
				log.Warnf("get wx app content empty")
				return nil, err
			}
			var base64Body = base64.StdEncoding.EncodeToString(byteContent)
			ycChatMsg.AddWxAppV2(base64Body, x.SosObjectId, x.MiniProgramName, x.MiniProgramTitle, x.MiniProgramPath, x.MiniProgramCoverUrl, icon, x.MaterialId)
		}
	}
	return ycChatMsg.Convert2YcMsg(), nil
}
func genWxAppContent(ctx *rpc.Context, corpId, appId uint32, robotUid, materialId uint64, sosObjectId, title, path string, replaceAppPath bool) ([]byte, string, error) {
	var byt []byte
	var icon string

	if sosObjectId == "" {
		mRsp, err := iquan.GetMaterialSys(ctx, &iquan.GetMaterialSysReq{
			Id:     materialId,
			CorpId: corpId,
			AppId:  appId,
		})
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, "", err
		}
		byt = []byte(mRsp.Material.Detail.YcEncode)
	} else {
		sosRsp, err := sos.GetObject(ctx, sosObjectId)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, "", err
		}
		byt = sosRsp.Object.Buf
	}
	if byt == nil {
		log.Warnf("get wx app content empty")
		return nil, "", nil
	}
	decode, err := base64.StdEncoding.DecodeString(string(byt))
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, "", err
	}
	var wxApp map[string]interface{}
	var wxAppStruct yc.MsgWxApp
	err = json.Unmarshal(decode, &wxApp)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, "", err
	}
	err = json.Unmarshal(decode, &wxAppStruct)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, "", err
	}

	if replaceAppPath {
		var belleApi qrt.BelleApi
		err = belleApi.Init(s.RedisGroup)
		if err != nil {
			log.Errorf("err %v", err)
			return nil, "", err
		}

		qRsp, err2 := quan.GetUser(ctx, &quan.GetUserReq{
			Id:     robotUid,
			CorpId: corpId,
			AppId:  appId,
		})
		if err2 != nil {
			log.Errorf("err %v", err)
			return nil, "", err
		}

		if qRsp.User == nil {
			log.Errorf("err %v", err)
			return nil, "", rpc.CreateError(qwhale.ErrUserNotFound)
		}
		var newPath string

		//TODO: kill this
		log.Infof("belle replace user :%s appId:%s path:%s", qRsp.User.WwUserId, wxAppStruct.Appid, path)
		//
		newPath, err = belleApi.GetAppPath(ctx, corpId, qRsp.User.WwUserId, wxAppStruct.Appid, path)
		if err != nil {
			log.Errorf("err %v", err2)
			return nil, "", err
		}

		if newPath == "" {
			return nil, "", rpc.InvalidArg("get belle path failed")
		}

		//TODO: kill this
		log.Infof("belle replace old :%s new:%s", wxAppStruct.Appid, newPath)

		path = newPath
	}

	icon = wxAppStruct.WeappIconUrl
	wxApp["title"] = title
	wxApp["pagepath"] = path
	var newCtx = core.NewInternalRpcCtx(corpId, appId, 0)
	// 对path做处理 注入一下 .html后缀
	const key = "QROBOT_WXAPP_INJECT_PATH"
	featureFlagRsp, err := featswitch.GetFeatureFlagEnabled(newCtx, &featswitch.GetFeatureFlagEnabledReq{Key: key, CorpId: corpId, AppId: appId})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, "", err
	}
	if featureFlagRsp.Enabled {
		log.Infof("need inject wx app path")
		wxApp["pagepath"] = checkAndInjectWxAppPath(path)
	}
	b, err := json.Marshal(wxApp)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, "", err
	}
	return b, icon, nil
}

// 给小程序注入.html后缀
func checkAndInjectWxAppPath(path string) string {
	if path == "" {
		return path
	}
	reg := regexp.MustCompile(`^[^?]+\?{0,1}`)
	subMatch := reg.FindAllStringSubmatch(path, -1)
	// 没匹配到 直接原样返回
	if len(subMatch) == 0 {
		return path
	}
	// 这里的临时变量命名随意一点吧
	subSubMatch := subMatch[0]
	if len(subSubMatch) == 0 {
		return path
	}
	splitPath := strings.Split(subSubMatch[0], "?")
	patten := splitPath[0]
	pathEndIdx := len(patten)
	log.Infof("get patten: %v", patten)
	if !strings.HasSuffix(patten, ".html") {
		// 注入一个.html后缀
		path = path[:pathEndIdx] + ".html" + path[pathEndIdx:]
	}
	log.Infof("wx app link: %v", path)
	return path
}

func getBizContext(corpId, appId uint32, context string) *yc.BizContext {
	return &yc.BizContext{
		CorpId:  corpId,
		AppId:   appId,
		Module:  "qmsg",
		Context: context,
	}
}
