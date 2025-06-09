package impl

import (
	"encoding/base64"
	"encoding/csv"
	"fmt"
	"git.pinquest.cn/qlb/extrpkg/github.com/go-redis/redis"
	"git.pinquest.cn/qlb/featswitch"
	"git.pinquest.cn/qlb/qopen"
	"git.pinquest.cn/qlb/qris"
	"regexp"
	"strconv"
	"strings"
	"time"

	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/dbproxy"
	"git.pinquest.cn/qlb/brick/json"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/brick/smq"
	"git.pinquest.cn/qlb/brick/utils"
	"git.pinquest.cn/qlb/brick/utils/routine"
	"git.pinquest.cn/qlb/commonv2"
	"git.pinquest.cn/qlb/core"
	"git.pinquest.cn/qlb/extrpkg/github.com/elliotchance/pie/pie"
	"git.pinquest.cn/qlb/extrpkg/github.com/jinzhu/gorm"
	"git.pinquest.cn/qlb/iquan"
	"git.pinquest.cn/qlb/qadapter"
	"git.pinquest.cn/qlb/qmsg"
	"git.pinquest.cn/qlb/qrc"
	"git.pinquest.cn/qlb/qrobot"
	"git.pinquest.cn/qlb/qrt"
	"git.pinquest.cn/qlb/qsm"
	"git.pinquest.cn/qlb/quan"
	"git.pinquest.cn/qlb/yc"
	utils2 "github.com/darabuchi/utils"
)

// processSendChatMsg 异步处理发送消息
func processSendChatMsg(ctx *rpc.Context, mqReq *smq.ConsumeReq, data interface{}) error {
	d := data.(*qmsg.SendChatMsgMqReq)
	user := d.User
	req := d.SendReq

	cnt, err := SendRecord.
		WhereCorpApp(user.CorpId, user.AppId).
		Where(DbUid, user.Id).
		Where(DbRobotUid, req.RobotUid).
		Where(DbCliMsgId, req.CliMsgId).
		Count(ctx)
	if err != nil {
		log.Errorf("err %v", err)
		return err
	}

	err = SendRecord.Create(ctx, &qmsg.ModelSendRecord{
		CorpId:   user.CorpId,
		AppId:    user.AppId,
		Uid:      user.Id,
		RobotUid: req.RobotUid,
		CliMsgId: req.CliMsgId,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	//有历史记录，msg id加后缀，避免底层放重不做处理
	if cnt > 1 {
		req.CliMsgId = fmt.Sprintf("%s.qmsg_retry_%s", req.CliMsgId, utils.GetRandomString(4, utils.RandomStringModNumberPlusLetter))
	}

	var chatExtId uint64
	var isGroup bool

	err = func() error {
		robot, err := getRobotByUid(ctx, user.CorpId, user.AppId, req.RobotUid)
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
		chatMsg := qrobot.ChatMsg{
			ChatMsgType:            req.ChatMsgType,
			Msg:                    req.Msg,
			Href:                   req.Href,
			Desc:                   req.Desc,
			VoiceTime:              uint32(req.VoiceTime),
			Title:                  req.Title,
			MaterialId:             req.MaterialId,
			MiniProgramCoverUrl:    req.MiniProgramCoverUrl,
			MiniProgramName:        req.MiniProgramName,
			MiniProgramPath:        req.MiniProgramPath,
			MiniProgramTitle:       req.MiniProgramTitle,
			MiniProgramReplacePath: req.MiniProgramReplacePath,
			SosObjectId:            req.SosObjectId,
			ChannelMsgName:         req.ChannelMsgName,
			ChannelMsgIcon:         req.ChannelMsgIcon,
			ChannelMsgTitle:        req.ChannelMsgTitle,
			ChannelMsgCoverUrl:     req.ChannelMsgCoverUrl,
			ChannelMsgHref:         req.ChannelMsgHref,
			MsgSerialNo:            req.MsgSerialNo,
			StoreProductId:         req.StoreProductId,
		}
		var atContactSerialNos []string
		var hasAtAll bool
		if req.ChatMsgType == uint32(qmsg.ChatMsgType_ChatMsgTypeRichTxt) {
			for _, v := range req.AtList {
				chatMsg.AtList = append(chatMsg.AtList, &qrobot.ChatMsgAt{
					StrMemberId: v.StrMemberId,
					AtAll:       v.AtAll,
					Uid:         v.Uid,
					Name:        v.Name,
					LocationAt:  v.LocationAt,
				})
				//if v.AtAll {
				//	hasAtAll = true
				//	atContactSerialNos = append(atContactSerialNos, v.StrMemberId)
				//	break
				//}
				//atContactSerialNos = append(atContactSerialNos, v.StrMemberId)
			}
			chatMsg.AtLocation = uint32(qrobot.ChatMsg_AtLocationFirst)
			if hasAtAll {
				chatMsg.At = uint32(qrobot.ChatMsg_AtTypeAll)
			} else {
				chatMsg.At = uint32(qrobot.ChatMsg_AtTypeGroupMember)
			}
			chatMsg.AtContactSerialNos = atContactSerialNos
		}

		// 1. 推送给由创
		var msgList []*qrobot.ChatMsg
		log.Debugf("chatMsg-old:%v", chatMsg.Msg)
		isReplaced, err := qrt.ChatMsgReplaceWxUrl(ctx, user.CorpId, user.AppId, &chatMsg, s.Conf.ResourceType)
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
		if isReplaced {
			log.Infof("chatMsg-replaced:%v", chatMsg.Msg)
		}
		msgList = append(msgList, &chatMsg)
		core.SetCorpAndApp(ctx, user.CorpId, user.AppId)
		chatMsgList, err := coverChatMsg2YcMsg(ctx, user.CorpId, user.AppId, req.RobotUid, msgList)
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}

		merchantId, err := getMerchantId(ctx)
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}

		switch req.ChatType {
		case uint32(qmsg.ChatType_ChatTypeSingle):
			accRsp, err := qrobot.GetAccount(ctx, &qrobot.GetAccountReq{
				Id: req.ChatId,
			})
			if err != nil {
				log.Errorf("err:%v", err)
				return err
			}
			acc := accRsp.Account
			chatExtId = acc.Id
			_, err = qsm.YcSendMessagesToMyCustomer(ctx, &qsm.YcSendMessagesToMyCustomerReq{
				MerchantId: merchantId,
				YcReq: &yc.SendMessagesToMyCustomerReq{
					MerchantNo:      merchantId,
					RobotSerialNo:   robot.YcSerialNo,
					ContactSerialNo: acc.YcSerialNo,
					Data:            chatMsgList,
					RelaSerialNo:    fmt.Sprintf("qmsg_%v", req.CliMsgId),
				},
				MsgId: req.CliMsgId,
				BizContext: &qsm.BizContext{
					ModuleName: "qmsg",
					CorpId:     robot.CorpId,
					AppId:      robot.AppId,
					Context:    req.CliMsgId,
				},
				RpcCallback: &qsm.RpcCallback{
					ServiceName: qmsg.ServiceName,
					ServicePath: qmsg.YcPrivateSendCallbackV2CMDPath,
				},
				ControlInfo: &qsm.ControlInfo{
					CallbackWithReqAndRsp: true,
				},
			})
			if err != nil {
				switch rpc.GetErrCode(err) {
				case qsm.ErrMsgSendDup:
					err = addSingleMsg4Send(ctx, req.CliMsgId, robot, acc, chatMsgList[0], "", true)
					if err != nil {
						log.Errorf("err:%v", err)

						return err
					}
				case yc.ErrRobotNotBelongMerchant, yc.ErrYcRobotDisconnect:
					return rpc.CreateErrorWithMsg(yc.ErrYcRobotDisconnect, "当前账号不在线")
				default:
					log.Errorf("err:%v", err)
					return err
				}
			}
		case uint32(qmsg.ChatType_ChatTypeGroup):
			groupRsp, err := qrobot.GetGroupChatListSys(ctx, &qrobot.GetGroupChatListSysReq{
				ListOption: core.NewListOption().
					AddOpt(uint32(qrobot.GetGroupChatListSysReq_ListOptionGroupIdList), req.ChatId),
				CorpId: user.CorpId,
				AppId:  user.AppId,
			})
			if err != nil {
				log.Errorf("err:%v", err)
				return err
			}
			if len(groupRsp.List) == 0 {
				log.Warnf("not found group")
				return rpc.InvalidArg("not found group")
			}
			group := groupRsp.List[0]
			chatExtId = group.Id
			isGroup = true
			_, err = qsm.YcSendMessagesToGroup(ctx, &qsm.YcSendMessagesToGroupReq{
				MerchantId: merchantId,
				YcReq: &yc.SendMessagesToGroupReq{
					MerchantNo:    merchantId,
					RobotSerialNo: robot.YcSerialNo,
					GroupSerialNo: groupRsp.List[0].StrGroupId,
					Data:          chatMsgList,
					RelaSerialNo:  fmt.Sprintf("qmsg_%v", req.CliMsgId),
				},
				MsgId: req.CliMsgId,
				BizContext: &qsm.BizContext{
					ModuleName: "qmsg",
					CorpId:     robot.CorpId,
					AppId:      robot.AppId,
					Context:    req.CliMsgId,
				},
				RpcCallback: &qsm.RpcCallback{
					ServiceName: qmsg.ServiceName,
					ServicePath: qmsg.YcGroupSendCallbackV2CMDPath,
				},
				ControlInfo: &qsm.ControlInfo{
					CallbackWithReqAndRsp: true,
				},
			})
			if err != nil {
				switch rpc.GetErrCode(err) {
				case qrc.ErrMsgSendDup:
					err = addGroupMsg4Send(ctx, req.CliMsgId, robot, group, chatMsgList[0], "", 0, true)
					if err != nil {
						log.Errorf("err:%v", err)
						return err
					}
				case yc.ErrRobotNotBelongMerchant, yc.ErrYcRobotDisconnect:
					return rpc.CreateErrorWithMsg(yc.ErrYcRobotDisconnect, "当前账号不在线")
				default:
					log.Errorf("err:%v", err)
					return err
				}
			}
		default:
			return rpc.InvalidArg("unsupported chat type")
		}
		return nil
	}()
	if err != nil {
		log.Errorf("err:%v", err)
		errCode := rpc.GetErrCode(err)
		if errCode > 0 {
			var errMsg string
			if e, ok := err.(*rpc.ErrMsg); ok {
				errMsg = e.ErrMsg
			}
			wrapper := &qmsg.WsMsgWrapper{
				MsgType: uint32(qmsg.WsMsgWrapper_MsgTypeSendChatMsgFail),
				SendChatMsgFail: &qmsg.WsMsgWrapper_SendChatMsgFail{
					ErrCode:  int32(errCode),
					ErrMsg:   errMsg,
					CliMsgId: req.CliMsgId,
					RobotUid: req.RobotUid,
				},
			}
			err = pushWsMsgAssignPriority(ctx, user.CorpId, user.AppId, req.RobotUid, chatExtId, isGroup, []*qmsg.WsMsgWrapper{wrapper})
			if err != nil {
				log.Errorf("err:%v", err)
				return err
			}
		}
		return err
	}

	LogStream.LogStreamSubType(ctx, SendMsgSubType, &SendMsgStream{
		CorpId:   user.CorpId,
		Uid:      user.Id,
		ChatType: req.ChatType,
	})

	return nil
}
func validateChatMsg(ctx *rpc.Context, typ uint32, msgReq *qmsg.SendChatMsgReq, user *qmsg.ModelUser, checkSensitive bool) (*validateSendChatMsgRsp, error) {
	var rsp validateSendChatMsgRsp
	msg := msgReq.Msg
	switch typ {
	case uint32(qmsg.ChatMsgType_ChatMsgTypeText), uint32(qmsg.ChatMsgType_ChatMsgTypeRichTxt):
		// 控制文本长度，前端控制1000字，后端松多10字
		const length = 1010
		runeMsg := []rune(msg)
		if len(runeMsg) > length || (len(runeMsg) <= 0 && typ != uint32(qmsg.ChatMsgType_ChatMsgTypeRichTxt)) {
			return &rsp, rpc.InvalidArg("message too long or cannot be empty")
		}
		if len(runeMsg) > 0 {
			// 校验企业敏感词
			r := SenWordRecord{
				robotUid:  msgReq.RobotUid,
				uid:       user.Id,
				chatExtId: msgReq.ChatId,
				chatType:  msgReq.ChatType,
			}
			r.chatType = msgReq.ChatType
			checker := NewSenWordChecker(nil, user.CorpId, user.AppId, msg, &r)
			if words, err := checker.check(); err != nil {
				rsp.TriggerSysSensitiveWordList = words
				log.Errorf("err:%v", err)
				return &rsp, err
			}
			// 只有圈客宝校验yc明感词
			if checkSensitive {
				// 校验系统敏感词
				qRsp, err := qrobot.FindSensitiveWordAll(ctx, &qrobot.FindSensitiveWordAllReq{
					Text: msg,
				})
				if err != nil {
					log.Errorf("err:%v", err)
					return &rsp, err
				}
				rsp.TriggerSysSensitiveWordList = qRsp.TriggerSensitiveWordList
				if len(rsp.TriggerSysSensitiveWordList) != 0 {
					return &rsp, rpc.CreateError(qmsg.ErrTriggerSysSensitiveWords)
				}
			}

		}
	case uint32(qmsg.ChatMsgType_ChatMsgTypeImage),
		uint32(qmsg.ChatMsgType_ChatMsgTypeWeb),
		uint32(qmsg.ChatMsgType_ChatMsgTypeFile):
		// 类型为web的，只校验链接
		if typ == uint32(qmsg.ChatMsgType_ChatMsgTypeWeb) {
			var webParams qmsg.ChatWebMsg
			err := utils.Json2Pb(msg, &webParams)
			if err != nil {
				log.Errorf("err:%v", err)
				return &rsp, err
			}
			msg = webParams.Href
		}
		// 资源相关的消息 只做格式检查
		re := regexp.MustCompile("^((https|http)://)[^(/+_)]+(.*)")
		if re.MatchString(msg) {
			return &rsp, nil
		}
		return &rsp, rpc.InvalidArg("URL format error")

	case uint32(qmsg.ChatMsgType_ChatMsgTypeVoice):
		var params qmsg.ChatVoiceMsg
		err := utils.Json2Pb(msg, &params)
		if err != nil {
			log.Errorf("err:%v", err)
			return &rsp, err
		}

		re := regexp.MustCompile("^((https|http)://)[^(/+_)]+(.*)")
		if !re.MatchString(params.VoiceSilkUrl) {
			return &rsp, rpc.InvalidArg("URL format error")
		}
		//if msgReq.VoiceTime == 0 {
		//	return &rsp, rpc.InvalidArg("video duration empty")
		//}
	case uint32(qmsg.ChatMsgType_ChatMsgTypeVideo):
		re := regexp.MustCompile("^((https|http)://)[^(/+_)]+(.*)")
		if !re.MatchString(msg) {
			return &rsp, rpc.InvalidArg("URL format error")
		}
		if msgReq.VoiceTime == 0 {
			return &rsp, rpc.InvalidArg("video duration empty")
		}
		if msgReq.Href == "" {
			return &rsp, rpc.InvalidArg("video cover empty")
		}
	// case uint32(qmsg.ChatMsgType_ChatMsgTypeFriendCard):
	//	// 好友名片的好友序列号长度不为64
	//	if len(msg) != 64 {
	//		return &rsp, rpc.InvalidArg("customer_serial_no error")
	//	}
	//
	case uint32(qmsg.ChatMsgType_ChatMsgTypeWxApp):
	case uint32(qmsg.ChatMsgType_ChatMsgTypeChannelMsg):
	case uint32(qmsg.ChatMsgType_ChatMsgTypeChannelLiveMsg):
	// _, err := base64.StdEncoding.DecodeString(msg)
	//	if err != nil {
	//		return &rsp, rpc.InvalidArg("wx app info error")
	//	}
	case uint32(qmsg.ChatMsgType_ChatMsgTypeStoreProduct):
	default:
		return &rsp, rpc.InvalidArg("invalid chat msg type %d", typ)
	}
	return &rsp, nil
}
func validateSendChatMsgReq(ctx *rpc.Context, corpId uint32, appId uint32, req *qmsg.SendChatMsgReq, user *qmsg.ModelUser) (*validateSendChatMsgRsp, error) {
	var rsp validateSendChatMsgRsp

	// 扫码号不检查敏感词
	qwhaleSrv := qwhaleService{}
	hostAccount, err := qwhaleSrv.getHostAccountByUid(ctx, corpId, appId, req.RobotUid)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	vRsp, err := validateChatMsg(ctx, req.ChatMsgType, req, user, hostAccount == nil)
	if vRsp != nil {
		rsp = *vRsp
	}
	if err != nil {
		log.Errorf("err:%v", err)
		return &rsp, err
	}

	switch req.ChatType {
	case uint32(qmsg.ChatType_ChatTypeSingle):
		// 查下 account_id 是否我的客户，不是则报错
		qrobotRsp, err := qrobot.GetAccountFollowSys(ctx, &qrobot.GetAccountFollowSysReq{
			CorpId:    corpId,
			AppId:     appId,
			RobotUid:  req.RobotUid,
			AccountId: req.ChatId,
		})
		if err != nil {
			if rpc.GetErrCode(err) == qrobot.ErrAccountNotFound {
				LogStream.LogStreamSubType(ctx, SendFailSubType, &SendFailStream{
					CorpId:    corpId,
					RobotUid:  req.RobotUid,
					ExtChatId: req.ChatId,
					ErrCode:   uint32(qmsg.ErrAccountFollowNotFound),
				})
			}
			log.Errorf("err:%v", err)
			return &rsp, rpc.CreateError(qmsg.ErrAccountFollowNotFound)
		}
		acc := qrobotRsp.Account
		// NOTE qrobot_account表中ext_uid>0的，且account_type=0的有将近4万条记录，所以这边不能加上account_type条件判断
		if acc.ExtUid > 0 {
			followRsp, err := iquan.GetExtContactFollow(ctx, &iquan.GetExtContactFollowReq{
				CorpId: acc.CorpId,
				AppId:  acc.AppId,
				ExtUid: acc.ExtUid,
				Uid:    req.RobotUid,
			})
			if err != nil {
				if rpc.GetErrCode(err) == quan.ErrExtContactFollowNotFound {
					// TODO 把之前的account对应的extuid重置为0重新匹配
				} else {
					LogStream.LogStreamSubType(ctx, SendFailSubType, &SendFailStream{
						CorpId:    corpId,
						RobotUid:  req.RobotUid,
						ExtChatId: req.ChatId,
						ErrCode:   uint32(rpc.GetErrCode(err)),
					})
					log.Errorf("err:%v", err)
					return nil, err
				}
			} else {
				follow := followRsp.Follow
				if follow.DeletedByStaff {
					LogStream.LogStreamSubType(ctx, SendFailSubType, &SendFailStream{
						CorpId:    corpId,
						RobotUid:  req.RobotUid,
						ExtChatId: req.ChatId,
						ErrCode:   uint32(qmsg.ErrHasDeletedByStaff),
					})
					return &rsp, rpc.CreateError(qmsg.ErrHasDeletedByStaff)
				}
				if follow.DeletedByExtContact {
					// return &rsp, nil
					LogStream.LogStreamSubType(ctx, SendFailSubType, &SendFailStream{
						CorpId:    corpId,
						RobotUid:  req.RobotUid,
						ExtChatId: req.ChatId,
						ErrCode:   uint32(qmsg.ErrHasDeletedByExtContact),
					})
					return &rsp, rpc.CreateError(qmsg.ErrHasDeletedByExtContact)
				}
			}
		}
	case uint32(qmsg.ChatType_ChatTypeGroup):
		_, err = qrobot.CheckRobotInGroupChat(ctx, &qrobot.CheckRobotInGroupChatReq{
			RobotUid:    req.RobotUid,
			GroupChatId: req.ChatId,
			CorpId:      corpId,
			AppId:       appId,
		})
		if err != nil {
			log.Errorf("err:%v", err)
			if rpc.GetErrCode(err) == qrobot.ErrRobotIsNotWatchThisGroup {
				err = rpc.CreateError(qmsg.ErrGroupNotOpenWatch)
			}
			return &rsp, err
		}
	default:
		return &rsp, rpc.InvalidArg("invalid chat type %d", req.ChatType)
	}
	return &rsp, nil
}

type validateSendChatMsgRsp struct {
	TriggerSysSensitiveWordList []string
	OneSideFriendErrCode        int32
}

func SendChatMsg(ctx *rpc.Context, req *qmsg.SendChatMsgReq) (*qmsg.SendChatMsgRsp, error) {
	var rsp qmsg.SendChatMsgRsp
	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)
	user, err := getUserCheck(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	if req.AssignChatId == 0 {
		// 拉下我有没有这个平台号的管理权限
		err = ensureHasRobotPerm(ctx, user, req.RobotUid)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
	} else {
		assignChat, err := AssignChat.get(ctx, corpId, appId, req.AssignChatId)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}

		if assignChat.Uid != user.Id {
			return nil, rpc.CreateError(qmsg.ErrNotHasRobotPerm)
		}
	}

	// 客服操作机器人发给客户的消息，只要客服发了，不管最终有没有发送成功，都参与客服质检
	if req.ChatType == uint32(qmsg.ChatType_ChatTypeSingle) {
		pubToRobotToCustomerMsgStatMq(ctx, &qmsg.RobotToCustomerMsgStatMqReq{
			CorpId:             user.CorpId,
			AppId:              user.AppId,
			RobotUid:           req.RobotUid,
			ReceiveAccountId:   req.ChatId,
			CustomerServiceUid: user.Id,
		})
	}

	vRsp, err := validateSendChatMsgReq(ctx, corpId, appId, req, user)
	if vRsp != nil {
		rsp.TriggerSysSensitiveWordList = vRsp.TriggerSysSensitiveWordList
	}
	if err != nil {
		log.Errorf("err:%v", err)
		return &rsp, err
	}
	// 看下这条消息是不是收到过了
	err = checkCliMsgId(ctx, user.CorpId, user.AppId, req.RobotUid, req.CliMsgId)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	if req.ChatType == uint32(qmsg.ChatType_ChatTypeSingle) {
		//检查聊天权限
		err = checkChatPermission(ctx, user, req.RobotUid, req.ChatId)
		if err != nil {
			log.Errorf("err %v", err)
			return nil, err
		}
	}

	// 视频语音类型,去storage获取相关时长信息
	pubRsp := qmsg.MqSendChatMsg.PubOrExecInRoutine(
		ctx, user.CorpId, user.AppId,
		&qmsg.SendChatMsgMqReq{
			SendReq: req,
			User:    user,
			SendAt:  utils.Now(),
		}, processSendChatMsg)
	if pubRsp != nil {
		log.Infof("pub rsp %s", pubRsp.MsgId)
	}

	var chatInfo qmsg.ModelChat
	err = Chat.WhereCorpApp(user.CorpId, user.AppId).
		Where(DbChatExtId, req.ChatId).
		Where(DbRobotUid, req.RobotUid).
		Where(DbIsGroup, req.ChatType == uint32(qmsg.ChatType_ChatTypeGroup)).First(ctx, &chatInfo)
	if err != nil {
		log.Errorf("err %v", err)
		return nil, err
	}

	// 变更回复状态
	_, err = Chat.WhereCorpApp(user.CorpId, user.AppId).
		Where(DbChatExtId, req.ChatId).
		Where(DbRobotUid, req.RobotUid).
		Where(DbIsGroup, req.ChatType == uint32(qmsg.ChatType_ChatTypeGroup)).
		Where(DbIsReply, false).
		Update(ctx, map[string]interface{}{
			// 已回复
			DbIsReply: true,
		})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	var tmp qmsg.ModelLastChat
	_, err = LastChat.UpdateOrCreate(ctx, map[string]interface{}{
		DbCorpId:       user.CorpId,
		DbAppId:        user.AppId,
		DbUid:          user.Id,
		DbCid:          chatInfo.Id,
		DbLastChatType: uint32(qmsg.GetLastChatListReq_LastChatTypeReply),
	}, map[string]interface{}{DbLastTimeAt: utils.Now()}, &tmp)

	if err != nil && !LastChat.IsUniqueIndexConflictErr(err) {
		log.Errorf("err:%v", err)
		return nil, err
	}

	return &rsp, nil
}
func GetChatList(ctx *rpc.Context, req *qmsg.GetChatListReq) (*qmsg.GetChatListRsp, error) {
	var rsp qmsg.GetChatListRsp

	// 临时保护数据库，避免翻页太大
	if req.ListOption.GetOffset() > 10000 {
		// 当做没数据处理
		return &rsp, nil
	}

	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)
	db := Chat.NewList(req.ListOption).WhereCorpApp(corpId, appId).Where(DbIsGroup, req.IsGroup).
		OrderDesc(DbLastSentAt)
	err := core.NewListOptionProcessor(req.ListOption).
		AddString(
			qmsg.GetChatListReq_ListOptionChatName,
			func(val string) error {
				if req.IsGroup {
					groupRsp, err := qrobot.GetGroupChatListSys(ctx, &qrobot.GetGroupChatListSysReq{
						CorpId:     corpId,
						AppId:      appId,
						ListOption: core.NewListOption(qrobot.GetGroupChatListSysReq_ListOptionName, val),
					})
					if err != nil {
						log.Errorf("err:%v", err)
						return err
					}
					if len(groupRsp.List) == 0 {
						return rpc.CreateError(qmsg.ErrLenZero)
					}
					db.WhereIn(DbChatExtId, utils.PluckUint64(groupRsp.List, "Id"))
				} else {
					var robotUidList []uint64
					for _, option := range req.ListOption.Options {
						if option.Type == int32(qmsg.GetChatListReq_ListOptionUidList) {
							if option.Value != "" {
								list := strings.Split(option.Value, ",")
								for _, item := range list {
									x, err := strconv.ParseUint(item, 10, 64)
									if err != nil {
										return rpc.CreateErrorWithMsg(
											int32(rpc.InvalidArgErrCode),
											fmt.Sprintf("invalid option value with type %d, expected uint64[]", option.Type))
									}
									robotUidList = append(robotUidList, x)
								}
							}
							break
						}
					}
					qrobotSrv := qrobotService{}
					accountList, robotUidList, err := qrobotSrv.getAccountListByNameOrRemark(ctx, corpId, appId, val, robotUidList)
					if err != nil {
						log.Errorf("err:%v", err)
						return err
					}
					if len(accountList) == 0 || len(robotUidList) == 0 {
						return rpc.CreateError(qmsg.ErrLenZero)
					}
					db.WhereIn(DbRobotUid, robotUidList)
					db.WhereIn(DbChatExtId, utils.PluckUint64(accountList, "Id"))
				}
				return nil
			}).
		AddTimeStampRange(
			qmsg.GetChatListReq_ListOptionLastSentAt,
			func(beginAt, endAt uint32) error {
				db.Where(fmt.Sprintf("? < %s AND %s < ?", DbLastSentAt, DbLastSentAt), beginAt, endAt)
				return nil
			}).
		AddString(
			qmsg.GetChatListReq_ListOptionRobotName,
			func(val string) error {
				userRsp, err := quan.GetUserList(ctx, &quan.GetUserListReq{
					ListOption:   core.NewListOption(quan.GetUserListReq_ListOptionName, val),
					OnlyUserList: true,
				})
				if err != nil {
					log.Errorf("err:%v", err)
					return err
				}
				if len(userRsp.List) == 0 {
					return rpc.CreateError(qmsg.ErrLenZero)
				}
				db.WhereIn(DbRobotUid, utils.PluckUint64(userRsp.List, "Id"))
				return nil
			}).
		AddBool(
			qmsg.GetChatListReq_ListOptionOrderAsc,
			func(val bool) error {
				if val {
					db.ResetOrderAsc(DbLastSentAt)
				} else {
					db.ResetOrderDesc(DbLastSentAt)
				}
				return nil
			}).
		AddUint64List(
			qmsg.GetChatListReq_ListOptionUidList,
			func(valList []uint64) error {
				db.WhereIn(DbRobotUid, valList)
				return nil
			}).
		Process()
	if err != nil {
		if rpc.GetErrCode(err) == qmsg.ErrLenZero {
			return &rsp, nil
		}
		log.Errorf("err:%v", err)
		return nil, err
	}
	rsp.Paginate, err = db.FindPaginate(ctx, &rsp.List)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	if len(rsp.List) > 0 {
		robotUidList := utils.PluckUint64(rsp.List, "RobotUid")
		userRsp, err := quan.GetUserList(ctx, &quan.GetUserListReq{
			ListOption:   core.NewListOption().AddOpt(quan.GetUserListReq_ListOptionUidList, robotUidList),
			OnlyUserList: true,
		})
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
		var userMap map[uint64]*quan.ModelUser
		utils.KeyBy(userRsp.List, "Id", &userMap)
		rsp.UserMap = userMap
		idList := utils.PluckUint64(rsp.List, "ChatExtId")
		if req.IsGroup {
			groupRsp, err := qrobot.GetGroupChatListSys(ctx, &qrobot.GetGroupChatListSysReq{
				ListOption: core.NewListOption().AddOpt(qrobot.GetGroupChatListSysReq_ListOptionGroupIdList, idList),
				CorpId:     corpId,
				AppId:      appId,
			})
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, err
			}
			var m map[uint64]*qrobot.ModelGroupChat
			utils.KeyBy(groupRsp.List, "Id", &m)
			rsp.GroupMap = m
		} else {
			accountRsp, err := qrobot.GetAccountListSys(ctx, &qrobot.GetAccountListSysReq{
				ListOption: core.NewListOption().SetSkipCount().SetLimit(uint32(len(idList))).AddOpt(qrobot.GetAccountListSysReq_ListOptionIdList, idList),
				CorpId:     corpId,
				AppId:      appId,
			})
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, err
			}
			var m map[uint64]*qrobot.ModelAccount
			utils.KeyBy(accountRsp.List, "Id", &m)
			rsp.AccountMap = m
			extUidList := utils.PluckUint64(accountRsp.List, "ExtUid")
			extContactRsp, err := iquan.GetExtContactList(ctx, &iquan.GetExtContactListReq{
				ListOption: core.NewListOption(iquan.GetExtContactListReq_ListOptionExtUidList, extUidList),
				AppId:      appId,
				CorpId:     corpId,
			})
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, err
			}
			var extMap map[uint64]*quan.ModelExtContact
			utils.KeyBy(extContactRsp.List, "Id", &extMap)
			rsp.ExtContactMap = extMap

			err = fillRemark(ctx, corpId, appId, robotUidList, extUidList, &rsp)
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, err
			}
		}
	}
	return &rsp, nil
}
func updateOrCreateChat(ctx *rpc.Context, msg *qmsg.ModelMsgBox) (*qmsg.ModelChat, uint64) {
	var chat qmsg.ModelChat
	var err error
	var lastSeq uint64

	if msg.ChatId == 0 {
		return nil, 0
	}

	var needCreate bool

	err = Chat.WhereCorpApp(msg.CorpId, msg.AppId).Where(map[string]interface{}{
		DbRobotUid:  msg.Uid,
		DbIsGroup:   msg.ChatType == uint32(qmsg.ChatType_ChatTypeGroup),
		DbChatExtId: msg.ChatId,
	}).First(ctx, &chat)
	if err != nil {
		if Chat.IsNotFoundErr(err) {
			needCreate = true
		} else {
			log.Errorf("err:%v", err)
			return nil, 0
		}
	}

	// 系统消息，则不处理
	if msg.ChatMsgType == 0 {
		return &chat, 0
	}

	lastSeq = chat.MsgSeq

	// 开始复杂的更新逻辑
	updateMap := map[string]interface{}{
		DbMsgSeq:          msg.MsgSeq,
		DbSenderAccountId: msg.SenderAccountId,
	}

	if msg.MsgType == uint32(qmsg.MsgType_MsgTypeChat) {
		updateMap[DbMsg] = utils.SubString(msg.Msg, 100)
		updateMap[DbChatMsgType] = msg.ChatMsgType
		updateMap[DbMsgUpdatedAt] = msg.SentAt

		isRobotSent := false
		if msg.ChatType == uint32(qmsg.ChatType_ChatTypeSingle) { // 私聊
			// 机器人自己发的
			if msg.SenderAccountId != msg.ChatId {
				isRobotSent = true
			}
		} else { // 群聊
			accountRsp, err := qrobot.GetAccountSys(ctx, &qrobot.GetAccountSysReq{
				CorpId: msg.CorpId,
				AppId:  msg.AppId,
				Id:     msg.SenderAccountId,
			})
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, 0
			}
			// 群聊机器人发的
			if accountRsp.Account != nil && accountRsp.Account.Uid == msg.Uid {
				isRobotSent = true
			}
		}
		if isRobotSent {
			updateMap[DbLastSentAt] = msg.SentAt
		} else {
			updateMap[DbLastReceiveAt] = msg.SentAt
			updateMap[DbUnreadCount] = dbproxy.Expr(fmt.Sprintf("%s + %d", DbUnreadCount, 1))
			if chat.UnreadCount == 0 {
				updateMap[DbFirstUnreadAt] = uint32(time.Now().Unix())
			}
			updateMap[DbIsReply] = false
		}
	}

	if needCreate {
		// 特性判断是否需要把创建会话的第一条消息设置为已回复（有些客户不希望第一条通过好友的消息出现在已读未回复中）
		flagEnabled, err := featswitch.GetFeatureFlagEnabled(ctx, &featswitch.GetFeatureFlagEnabledReq{
			Key: "QMSG_FIRST_MESSAGE_NO_REPLY", CorpId: msg.CorpId, AppId: msg.AppId,
		})
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, 0
		}
		if flagEnabled.Enabled {
			updateMap[DbIsReply] = true
		}
		createMap := updateMap
		createMap[DbCorpId] = msg.CorpId
		createMap[DbAppId] = msg.AppId
		createMap[DbRobotUid] = msg.Uid
		createMap[DbIsGroup] = msg.ChatType == uint32(qmsg.ChatType_ChatTypeGroup)
		createMap[DbMsg] = utils.SubString(msg.Msg, 100)
		createMap[DbChatExtId] = msg.ChatId
		createMap[DbChatMsgType] = msg.ChatMsgType
		err = Chat.Create(ctx, &createMap)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, 0
		}

		// 再查一次用于返回上层
		err = Chat.WhereCorpApp(msg.CorpId, msg.AppId).Where(map[string]interface{}{
			DbRobotUid:  msg.Uid,
			DbIsGroup:   msg.ChatType == uint32(qmsg.ChatType_ChatTypeGroup),
			DbChatExtId: msg.ChatId,
		}).First(ctx, &chat)

		return &chat, lastSeq
	} else {
		_, err = Chat.WhereCorpApp(msg.CorpId, msg.AppId).Where(map[string]interface{}{
			DbRobotUid:  msg.Uid,
			DbIsGroup:   msg.ChatType == uint32(qmsg.ChatType_ChatTypeGroup),
			DbChatExtId: msg.ChatId,
		}).Update(ctx, updateMap)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, 0
		}
	}

	err = Chat.WhereCorpApp(msg.CorpId, msg.AppId).Where(map[string]interface{}{
		DbRobotUid:  msg.Uid,
		DbIsGroup:   msg.ChatType == uint32(qmsg.ChatType_ChatTypeGroup),
		DbChatExtId: msg.ChatId,
		DbId:        chat.Id,
	}).First(ctx, &chat)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, 0
	}

	// 记录发送相差时间
	if msg.MsgType == uint32(qmsg.MsgType_MsgTypeChat) && msg.ChatType == uint32(qmsg.ChatType_ChatTypeSingle) {
		// 如果是客服自己发的
		if msg.SenderAccountId != msg.ChatId {
			err = updateUserSendRedis(ctx, msg.CorpId, msg.AppId, msg.SentAt, chat.Id)
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, 0
			}
		} else {
			err = updateContactSendRedis(ctx, msg.CorpId, msg.AppId, msg.SentAt, chat.Id)
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, 0
			}
		}
	}

	return &chat, lastSeq
}

func BatchSendChatMsg(ctx *rpc.Context, req *qmsg.BatchSendChatMsgReq) (*qmsg.BatchSendChatMsgRsp, error) {
	var rsp qmsg.BatchSendChatMsgRsp

	var chatMsgList []*qrobot.ChatMsg
	for _, x := range req.ChatMsgList {
		chatMsgList = append(chatMsgList, &qrobot.ChatMsg{
			ChatMsgType: x.ChatMsgType,
			Msg:         x.Msg,
			Href:        x.Href,
			Desc:        x.Desc,
		})
	}
	_, err := qrobot.BatchSendChatMsg(ctx, &qrobot.BatchSendChatMsgReq{
		ChatType:         req.ChatType,
		RobotSerialNo:    req.RobotSerialNo,
		ReceiverSerialNo: req.ReceiverSerialNo,
		ChatMsgList:      chatMsgList,
		MsgId:            req.MsgId,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	return &rsp, nil
}
func GetMsgBoxList(ctx *rpc.Context, req *qmsg.GetMsgBoxListReq) (*qmsg.GetMsgBoxListRsp, error) {
	var rsp qmsg.GetMsgBoxListRsp
	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)
	db := MsgBox.NewList(req.ListOption).WhereCorpApp(corpId, appId).
		Where(DbChatId, req.ChatId).Where(DbChatType, req.ChatType).Where(DbUid, req.RobotUid).
		Where(DbMsgType, qmsg.MsgType_MsgTypeChat).OrderDesc(DbSentAt)
	db = ChoiceMsgBoxDb(db, corpId)
	err := core.NewListOptionProcessor(req.ListOption).
		AddTimeStampRange(
			qmsg.GetMsgBoxListReq_ListOptionSentAt,
			func(beginAt, endAt uint32) error {
				db.Where(fmt.Sprintf("? < %s AND %s < ?", DbSentAt, DbSentAt), beginAt, endAt)
				return nil
			}).
		AddString(
			qmsg.GetMsgBoxListReq_ListOptionMsg,
			func(val string) error {
				db.WhereLike(DbMsg, fmt.Sprintf("%%%s%%", val)).Where(DbChatMsgType, qmsg.ChatMsgType_ChatMsgTypeText)
				return nil
			}).
		AddBool(
			qmsg.GetMsgBoxListReq_ListOptionOrderByDesc,
			func(val bool) error {
				if val {
					db.ResetOrderDesc(DbSentAt)
				} else {
					db.ResetOrderAsc(DbSentAt)
				}
				return nil
			}).
		AddUint64(qmsg.GetMsgBoxListReq_ListOptionGteMsgSeq,
			func(val uint64) error {
				db.Gte(DbMsgSeq, val)
				return nil
			}).
		AddUint64(qmsg.GetMsgBoxListReq_ListOptionLteMsgSeq,
			func(val uint64) error {
				db.Lte(DbMsgSeq, val)
				return nil
			}).
		AddBool(
			qmsg.GetMsgBoxListReq_ListOptionSelectTimeoutMsgBoxList,
			func(val bool) error {
				if val {
					if req.ChatType == uint32(qmsg.ChatType_ChatTypeSingle) {
						timeoutMsgBoxIdList, err := MsgStatTimeOutDetail.getTimeoutMsgBoxIdList(ctx, corpId, req.RobotUid, req.ChatId, []uint64{})
						if err != nil {
							log.Errorf("err:%v", err)
							return err
						}
						if len(timeoutMsgBoxIdList) == 0 {
							return gorm.ErrRecordNotFound
						}
						db.WhereIn(DbId, timeoutMsgBoxIdList)
					} else {
						return gorm.ErrRecordNotFound
					}
				}
				return nil
			}).
		Process()
	if err != nil {
		if err != gorm.ErrRecordNotFound {
			log.Errorf("err:%v", err)
			return nil, err
		}
		return &rsp, nil
	}
	rsp.Paginate, err = db.FindPaginate(ctx, &rsp.List)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	if len(rsp.List) > 0 {
		accountIdList := utils.PluckUint64(rsp.List, "SenderAccountId")
		accountRsp, err := qrobot.GetAccountList(ctx, &qrobot.GetAccountListReq{
			ListOption: core.NewListOption(qrobot.GetAccountListReq_ListOptionIdList, accountIdList),
		})
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
		var accountMap map[uint64]*qrobot.ModelAccount
		utils.KeyBy(accountRsp.List, "Id", &accountMap)
		rsp.AccountMap = accountMap
		robotUidList := utils.PluckUint64(accountRsp.List, "Uid")
		userRsp, err := quan.GetUserList(ctx, &quan.GetUserListReq{
			ListOption:   core.NewListOption().AddOpt(quan.GetUserListReq_ListOptionUidList, robotUidList),
			OnlyUserList: true,
		})
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
		var userMap map[uint64]*quan.ModelUser
		utils.KeyBy(userRsp.List, "Id", &userMap)
		rsp.UserMap = userMap
		extUidList := utils.PluckUint64(accountRsp.List, "ExtUid")
		extContactRsp, err := iquan.GetExtContactList(ctx, &iquan.GetExtContactListReq{
			ListOption: core.NewListOption(iquan.GetExtContactListReq_ListOptionExtUidList, extUidList),
			AppId:      appId,
			CorpId:     corpId,
		})
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
		var extMap map[uint64]*quan.ModelExtContact
		utils.KeyBy(extContactRsp.List, "Id", &extMap)
		rsp.ExtContactMap = extMap
		if req.ChatType == uint32(qmsg.ChatType_ChatTypeSingle) {
			rsp.TimeoutMsgBoxIdList, err = MsgStatTimeOutDetail.getTimeoutMsgBoxIdList(ctx, corpId, req.RobotUid, req.ChatId, utils.PluckUint64(rsp.List, "Id"))
		}
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
		// 获取客服发消息关联信息
		rsp.MsgToCustomerMap = map[uint64]*qmsg.GetMsgBoxListRsp_MsgToCustomerMap{}
		msgBoxIdListMap := map[uint64][]uint64{}
		for _, msgItem := range rsp.List {
			msgBoxIdList, ok := msgBoxIdListMap[msgItem.Uid]
			if !ok {
				// 初始化
				msgBoxIdList = []uint64{}
			}
			msgBoxIdList = append(msgBoxIdList, msgItem.Id)
			msgBoxIdListMap[msgItem.Uid] = msgBoxIdList
		}
		var userIdList []uint64
		for robotUid, msgBoxIdList := range msgBoxIdListMap {
			// 按机器人批量查询发送消息记录的关系
			var sendRecordList []*qmsg.ModelSendRecord
			err = SendRecord.WhereCorpApp(corpId, appId).
				Select(DbUid, DbMsgBoxId).
				Where(DbRobotUid, robotUid).
				WhereIn(DbMsgBoxId, msgBoxIdList).
				Find(ctx, &sendRecordList)
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, err
			}
			for _, sendRecord := range sendRecordList {
				if sendRecord.Uid <= 0 || sendRecord.MsgBoxId <= 0 {
					continue
				}
				msgToCustomerList, ok := rsp.MsgToCustomerMap[sendRecord.Uid]
				if !ok {
					msgToCustomerList = &qmsg.GetMsgBoxListRsp_MsgToCustomerMap{
						MsgBoxId: []uint64{},
					}
				}
				msgToCustomerList.MsgBoxId = append(msgToCustomerList.MsgBoxId, sendRecord.MsgBoxId)
				rsp.MsgToCustomerMap[sendRecord.Uid] = msgToCustomerList
				if !utils.InSliceUint64(sendRecord.Uid, userIdList) {
					userIdList = append(userIdList, sendRecord.Uid)
				}
			}
		}
		// 获取对应的客服账号
		if len(userIdList) > 0 {
			var customerUserList []*qmsg.ModelUser
			err = User.WhereCorpApp(corpId, appId).
				WhereIn(DbId, userIdList).
				Find(ctx, &customerUserList)
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, err
			}
			utils.KeyBy(customerUserList, "Id", &rsp.CustomerUserMap)
		}

		var detailList []*qmsg.ModelAiEntertainDetail

		tmpIdList := utils.PluckUint64(rsp.List, "Id")
		err = aiEntertainDetail.WhereCorpApp(corpId, appId).WhereIn(DbMsgBoxId, tmpIdList).Find(ctx, &detailList)
		if err != nil {
			log.Errorf("err %v", err)
			return nil, err
		}
		rsp.AiSendMsgBoxIdList = utils.PluckUint64(detailList, "MsgBoxId")
	}
	return &rsp, nil
}
func ExportChatList(ctx *rpc.Context, req *qmsg.ExportChatListReq) (*qmsg.ExportChatListRsp, error) {
	var rsp qmsg.ExportChatListRsp
	var fileName string
	var fieldName string
	now := time.Now()
	if req.IsGroup {
		fileName = "客服工作台群聊记录"
		fieldName = "群聊名称"
	} else {
		fileName = "客服工作台私聊记录"
		fieldName = "好友名称"
	}
	fileName += fmt.Sprintf("%d%d%d%d%d", now.Year(), now.Month(), now.Day(), now.Hour(), now.Month())
	var err error
	rsp.TaskKey, err = commonv2.ExportExcel(ctx, s.Conf.ResourceType, fileName+".xlsx", func(ctx *rpc.Context, w *csv.Writer) error {
		// 表头数据
		if err = w.Write([]string{
			fieldName, "所属号", "上次对话时间",
		}); err != nil {
			log.Errorf("err: %v", err)
			return err
		}
		const (
			Limit = 1000
		)
		offset := uint32(0)
		for {
			chatRsp, err := GetChatList(ctx, &qmsg.GetChatListReq{
				ListOption: req.ListOption.SetLimit(Limit).SetOffset(offset),
				IsGroup:    req.IsGroup,
			})
			if err != nil {
				log.Errorf("err:%v", err)
				return err
			}
			if len(chatRsp.List) == 0 {
				break
			}
			offset += Limit
			for _, chat := range chatRsp.List {
				var record []string
				if _, ok := chatRsp.UserMap[chat.RobotUid]; !ok {
					continue
				}
				if req.IsGroup {
					if g, ok := chatRsp.GroupMap[chat.ChatExtId]; ok {
						record = append(record, g.Name)
					} else {
						record = append(record, "群聊")
					}
				} else {
					if a, ok := chatRsp.AccountMap[chat.ChatExtId]; ok {
						record = append(record, a.Name)
					} else {
						record = append(record, "")
					}
				}
				record = append(record, chatRsp.UserMap[chat.RobotUid].WwUserName, time.Unix(int64(chat.LastSentAt), 0).Format("2006-01-02 15:04:05"))
				if err = w.Write(record); err != nil {
					log.Errorf("err:%v", err)
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	return &rsp, nil
}
func YcGroupSendCallback(ctx *rpc.Context, req *qrc.YcSendMessagesToGroupCallbackReq) (*qrc.YcSendMessagesToGroupCallbackRsp, error) {
	var rsp qrc.YcSendMessagesToGroupCallbackRsp
	var err error
	ycRsp := req.YcRsp
	ycReq := req.YcReq
	if ycRsp.Data == nil {
		return nil, rpc.InvalidArg("yc rsp data nil")
	}
	if !ycRsp.Success {
		log.Warnf("send fail, %+v, skip", ycRsp)
		return &rsp, nil
	}
	msgIdx := yc.SendMessagesDataList(ycReq.Data).
		FindFirstUsing(func(value *yc.SendMessagesData) bool {
			return ycRsp.Data.MsgNum == value.MsgNum
		})
	if msgIdx < 0 {
		return nil, rpc.InvalidArg("msg idx out of range, len %d, idx %d", len(ycReq.Data), msgIdx)
	}
	ycMsg := ycReq.Data[msgIdx]
	// 拉下机器人信息
	robot, err := getRobotBySn(ctx, req.BizContext.CorpId, req.BizContext.AppId, ycReq.RobotSerialNo)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	// 拉下群
	groupRsp, err := qrobot.GetGroupChatListSys(ctx, &qrobot.GetGroupChatListSysReq{
		ListOption: core.NewListOption().SetSkipCount().SetLimit(1).
			AddOpt(qrobot.GetGroupChatListSysReq_ListOptionStrGroupId, ycReq.GroupSerialNo),
		CorpId: robot.CorpId,
		AppId:  robot.AppId,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	if len(groupRsp.List) == 0 {
		return nil, rpc.InvalidArg("group not found")
	}
	group := groupRsp.List[0]
	cliMsgId := req.MsgId
	if len(req.YcReq.Data) > 1 {
		cliMsgId = fmt.Sprintf("%s_%d", cliMsgId, msgIdx)
	}
	err = addGroupMsg4Send(ctx, cliMsgId, robot, group, ycMsg, req.MsgId, 0, req.BizContext.ModuleName == "qmsg")
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	setMsgDeDupCache(robot.Uid, ycRsp.Data.MsgId)
	return &rsp, nil
}
func YcPrivateSendCallback(ctx *rpc.Context, req *qrc.YcSendMessagesToMyCustomerCallbackReq) (*qrc.YcSendMessagesToMyCustomerCallbackRsp, error) {
	var rsp qrc.YcSendMessagesToMyCustomerCallbackRsp
	var err error
	ycRsp := req.YcRsp
	ycReq := req.YcReq
	if ycRsp.Data == nil {
		return nil, rpc.InvalidArg("yc rsp data nil")
	}
	// TODO WS
	msgIdx := yc.SendMessagesDataList(ycReq.Data).
		FindFirstUsing(func(value *yc.SendMessagesData) bool {
			return ycRsp.Data.MsgNum == value.MsgNum
		})
	if msgIdx < 0 {
		return nil, rpc.InvalidArg("msg idx out of range, len %d, idx %d", len(ycReq.Data), msgIdx)
	}
	ycMsg := ycReq.Data[msgIdx]
	// 拉下机器人信息
	robot, err := getRobotBySn(ctx, req.BizContext.CorpId, req.BizContext.AppId, ycReq.RobotSerialNo)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	if !ycRsp.Success {
		log.Warnf("send fail, %+v, skip", ycRsp)
		msg := &qmsg.WsMsgWrapper_SendChatMsgFail{
			ErrCode:  int32(yc.ErrApiBizErr),
			ErrMsg:   ycRsp.Message,
			CliMsgId: req.MsgId,
			RobotUid: robot.Uid,
		}
		err = yc.ExecMessage(ctx, ycReq.RobotSerialNo, ycRsp.Message)
		if err != nil {
			if x, ok := err.(*rpc.ErrMsg); ok {
				msg.ErrCode = x.ErrCode
				msg.ErrMsg = x.ErrMsg
			} else {
				msg.ErrCode = -1
				msg.ErrMsg = err.Error()
			}
		}

		var msgBox qmsg.ModelMsgBox
		err = MsgBox.getByUidCliMsgId(ctx, robot.CorpId, robot.AppId, robot.Uid, req.MsgId, &msgBox)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}

		wrapper := &qmsg.WsMsgWrapper{
			MsgType:         uint32(qmsg.WsMsgWrapper_MsgTypeSendChatMsgFail),
			SendChatMsgFail: msg,
		}
		err = pushWsMsgAssignPriority(ctx, robot.CorpId, robot.AppId, robot.Uid, msgBox.ChatId, msgBox.ChatType == uint32(qmsg.ChatType_ChatTypeGroup), []*qmsg.WsMsgWrapper{wrapper})
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
		return &rsp, nil
	}
	// 拉下人
	accRsp, err := qrobot.GetAccountByYcSerialNo(ctx, &qrobot.GetAccountByYcSerialNoReq{
		CorpId:     robot.CorpId,
		AppId:      robot.AppId,
		YcSerialNo: ycReq.ContactSerialNo,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	acc := accRsp.Account
	cliMsgId := req.MsgId
	if len(req.YcReq.Data) > 1 {
		cliMsgId = fmt.Sprintf("%s_%d", cliMsgId, msgIdx)
	}
	err = addSingleMsg4Send(ctx, cliMsgId, robot, acc, ycMsg, req.MsgId, req.BizContext.ModuleName == "qmsg")
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	setMsgDeDupCache(robot.Uid, ycRsp.Data.MsgId)
	return &rsp, nil
}

func genMsgDeDupCacheKey(robotUid uint64, msgId string) string {
	return fmt.Sprintf("msg_dedup_%d_%s", robotUid, msgId)
}

func checkMsgDeDupCache(robotUid uint64, msgId string) error {
	key := genMsgDeDupCacheKey(robotUid, msgId)
	exists, err := s.RedisGroup.Exists(key)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	if exists {
		return rpc.CreateError(int32(qmsg.ErrCode_ErrCliMsgIdExisted))
	}
	return nil
}

func setMsgDeDupCache(robotUid uint64, msgId string) {
	key := genMsgDeDupCacheKey(robotUid, msgId)
	if err := s.RedisGroup.Set(key, []byte{'1'}, time.Minute*5); err != nil {
		log.Errorf("err:%v", err)
	}
}

func checkAiMsgDeDupCache(content string) (isDup bool, err error) {
	key := fmt.Sprintf("qmsg_ai_msg_dedup_%s", utils.StrMd5(content))
	exists, err := s.RedisGroup.Exists(key)
	if err != nil {
		log.Errorf("err:%v", err)
		return false, err
	}

	if exists {
		return true, nil
	}

	if err = s.RedisGroup.Set(key, []byte("0"), time.Minute*5); err != nil {
		log.Errorf("err:%v", err)
		return false, err
	}

	return
}

func checkNotifyDeDupCache(content string, uid uint64) (isDup bool, err error) {
	key := fmt.Sprintf("qmsg_notify_dedup_%d_%s", uid, utils.StrMd5(content))
	exists, err := s.RedisGroup.Exists(key)
	if err != nil {
		log.Errorf("err:%v", err)
		return false, err
	}

	if exists {
		return true, nil
	}

	if err = s.RedisGroup.Set(key, []byte("0"), time.Minute*5); err != nil {
		log.Errorf("err:%v", err)
		return false, err
	}

	return
}

func coverYcSendMsg2ChatMsg(ctx *rpc.Context, corpId, appId uint32, groupId uint64, ycChatMsg *yc.SendMessagesData, ycFileId uint64) (uint32, string, error) {
	var msgType uint32
	var msg string
	var err error
	switch ycChatMsg.MsgType {
	case uint32(yc.MsgType_MsgTypeText):
		switch ycChatMsg.At {
		case 0:
			msgType = uint32(qmsg.ChatMsgType_ChatMsgTypeText)
			msg = ycChatMsg.MsgContent
		case 1:
			msgType = uint32(qmsg.ChatMsgType_ChatMsgTypeRichTxt)
			chatMsg := &qmsg.ChatMsg{
				ChatMsgType: uint32(qmsg.ChatMsgType_ChatMsgTypeRichTxt),
				Msg:         ycChatMsg.MsgContent,
				AtList: []*qmsg.ChatMsgAt{
					{
						LocationAt:  ycChatMsg.AtLocation,
						AtAll:       true,
						Name:        "所有人",
						StrMemberId: "0",
					},
				},
			}
			msg, err = utils.Pb2JsonSkipDefaults(chatMsg)
			if err != nil {
				log.Errorf("err:%v", err)
				return 0, "", err
			}
		case 2:
			msgType = uint32(qmsg.ChatMsgType_ChatMsgTypeRichTxt)
			chatMsg := &qmsg.ChatMsg{
				ChatMsgType: uint32(qmsg.ChatMsgType_ChatMsgTypeRichTxt),
				Msg:         ycChatMsg.MsgContent,
				AtList:      []*qmsg.ChatMsgAt{},
			}
			memberMap := map[string]string{}
			groupMemberListRsp, err := qrobot.GetGroupChatMemberListSys(nil, &qrobot.GetGroupChatMemberListSysReq{
				CorpId: corpId,
				AppId:  appId,
				ListOption: core.NewListOption().
					AddOpt(uint32(qrobot.GetGroupChatMemberListSysReq_ListOptionGroupIdList), groupId).
					AddOpt(uint32(qrobot.GetGroupChatMemberListSysReq_ListOptionStrMemberIdList),
						ycChatMsg.AtContactSerialNos),
			})
			if err != nil {
				log.Errorf("err:%v", err)
			} else {
				qrobot.GroupChatMemberList(groupMemberListRsp.List).
					Each(func(member *qrobot.ModelGroupChatMember) {
						memberMap[member.StrMemberId] = member.MemberName
					})
			}
			pie.Strings(ycChatMsg.AtContactSerialNos).
				Each(func(sn string) {
					chatMsg.AtList = append(chatMsg.AtList, &qmsg.ChatMsgAt{
						LocationAt:  ycChatMsg.AtLocation,
						AtAll:       false,
						Name:        memberMap[sn],
						StrMemberId: sn,
					})
				})
			msg, err = utils.Pb2JsonSkipDefaults(chatMsg)
			if err != nil {
				log.Errorf("err:%v", err)
				return 0, "", err
			}
		default:
			return 0, "", rpc.InvalidArg("invalid at type")
		}
	case uint32(yc.MsgType_MsgTypeImage):
		msgType = uint32(qmsg.ChatMsgType_ChatMsgTypeImage)
		msg = ycChatMsg.MsgContent
	case uint32(yc.MsgType_MsgTypeVoice):
		msgType = uint32(qmsg.ChatMsgType_ChatMsgTypeVoice)
		chatMsg := &qmsg.ChatVoiceMsg{
			VoiceUrl:     ycChatMsg.Href,
			VoiceTime:    ycChatMsg.VoiceTime,
			YcFileId:     ycFileId,
			VoiceSilkUrl: ycChatMsg.MsgContent,
		}
		msg, err = utils.Pb2JsonSkipDefaults(chatMsg)
		if err != nil {
			log.Errorf("err:%v", err)
			return 0, "", err
		}
	case uint32(yc.MsgType_MsgTypeVideo):
		msgType = uint32(qmsg.ChatMsgType_ChatMsgTypeVideo)
		chatMsg := &qmsg.ChatVideoMsg{
			VideoUrl:  ycChatMsg.Href,
			VideoTime: ycChatMsg.VoiceTime,
		}
		infoRsp, err := getMediaInfo(nil, ycChatMsg.Href, corpId)
		if err != nil {
			log.Errorf("err:%v", err)
		} else {
			chatMsg.VideoTime = uint64(infoRsp.MediaDuration)
			chatMsg.CoverUrl = infoRsp.MediaCoverUrl
		}
		// 优先使用客户传的封面链接
		if ycChatMsg.MsgContent != "" && (strings.Contains(ycChatMsg.MsgContent, "https://") || strings.Contains(ycChatMsg.MsgContent, "http://")) {
			chatMsg.CoverUrl = ycChatMsg.MsgContent
		}
		msg, err = utils.Pb2JsonSkipDefaults(chatMsg)
		if err != nil {
			log.Errorf("err:%v", err)
			return 0, "", err
		}
	case uint32(yc.MsgType_MsgTypeImageAndLink):
		msgType = uint32(qmsg.ChatMsgType_ChatMsgTypeWeb)
		chatMsg := &qmsg.ChatWebMsg{
			Title:   ycChatMsg.Title,
			Href:    ycChatMsg.Href,
			Desc:    ycChatMsg.Desc,
			IconUrl: ycChatMsg.MsgContent,
		}
		msg, err = utils.Pb2JsonSkipDefaults(chatMsg)
		if err != nil {
			log.Errorf("err:%v", err)
			return 0, "", err
		}
	case uint32(yc.MsgType_MsgTypeFriendCard):
		msgType = uint32(qmsg.ChatMsgType_ChatMsgTypeFriendCard)
	case uint32(yc.MsgType_MsgTypeFile):
		msgType = uint32(qmsg.ChatMsgType_ChatMsgTypeFile)
		chatMsg := &qmsg.ChatFileMsg{
			FileName: ycChatMsg.Title,
			FileUrl:  ycChatMsg.Href,
			Size:     ycChatMsg.Desc,
		}
		msg, err = utils.Pb2JsonSkipDefaults(chatMsg)
		if err != nil {
			log.Errorf("err:%v", err)
			return 0, "", err
		}
	case uint32(yc.MsgType_MsgTypeWxApp):
		msgType = uint32(qmsg.ChatMsgType_ChatMsgTypeWxApp)
		var chatMsg qmsg.ChatWxAppMsg
		if ycChatMsg.GetMiniProgramIconUrl() != "" && ycChatMsg.GetMiniProgramName() != "" &&
			ycChatMsg.GetMiniProgramTitle() != "" {
			chatMsg.Title = ycChatMsg.GetMiniProgramTitle()
			chatMsg.AppName = ycChatMsg.GetMiniProgramName()
			chatMsg.AppId = ycChatMsg.GetMiniProgramAppId()
			chatMsg.Path = ycChatMsg.GetMiniProgramPath()
			chatMsg.IconUrl = ycChatMsg.GetMiniProgramIconUrl()
			chatMsg.CoverUrl = ycChatMsg.GetMiniProgramCoverUrl()
			chatMsg.SosObjectId = ycChatMsg.GetSosObjectId()
		} else if ycChatMsg.GetMsgContent() != "" {
			decode, err := base64.StdEncoding.DecodeString(ycChatMsg.MsgContent)
			if err != nil {
				log.Errorf("err:%v", err)
				return 0, "", err
			}
			var wxApp yc.MsgWxApp
			err = json.Unmarshal(decode, &wxApp)
			if err != nil {
				log.Errorf("err:%v", err)
				return 0, "", err
			}
			chatMsg.Title = wxApp.Title
			chatMsg.AppName = wxApp.Des_1
			if wxApp.AppName != "" { // 兼容圈客宝
				chatMsg.AppName = wxApp.AppName
			}
			if wxApp.Appid != "" { // 兼容圈客宝
				chatMsg.AppId = wxApp.Appid
			}
			chatMsg.IconUrl = wxApp.WeappIconUrl
			chatMsg.CoverUrl = ycChatMsg.MiniProgramCoverUrl
			chatMsg.Path = ycChatMsg.MiniProgramPath
			chatMsg.SosObjectId = ycChatMsg.SosObjectId
			// 如果没有封面，取Href
			if chatMsg.CoverUrl == "" {
				chatMsg.CoverUrl = ycChatMsg.Href
			}
		} else {
			log.Errorf("err:%v", err)
			return 0, "", rpc.InvalidArg("invalid mp msg")
		}

		msg, err = utils.Pb2JsonSkipDefaults(&chatMsg)
		if err != nil {
			log.Errorf("err:%v", err)
			return 0, "", err
		}
	case uint32(yc.MsgType_MsgTypeChannelMsg):
		msgType = uint32(qmsg.ChatMsgType_ChatMsgTypeChannelMsg)
		m, err := utils2.NewMapWithJson([]byte(ycChatMsg.MsgContent))
		if err != nil {
			log.Errorf("err:%v", err)
			return 0, "", err
		}
		channelMsg := yc.ChannelMsg{
			Avatar:     m.GetString("avatar"),
			CoverUrl:   m.GetString("cover_url"),
			Desc:       m.GetString("desc"),
			FeedType:   m.GetUint32("feed_type"),
			Nickname:   m.GetString("nickname"),
			Url:        m.GetString("url"),
			Thumburl_1: m.GetString("thumb_url"),
			Title:      m.GetString("title"),
			Des:        m.GetString("desc"),
		}
		chatMsg := &qmsg.ChatChannelMsg{
			Title:       channelMsg.Title,
			CoverUrl:    channelMsg.CoverUrl,
			Href:        channelMsg.Url,
			MsgSerialNo: ycChatMsg.MsgSerialNo,
			Name:        channelMsg.Nickname,
			Icon:        channelMsg.Avatar,
		}
		msg, err = utils.Pb2JsonSkipDefaults(chatMsg)
		if err != nil {
			log.Errorf("err:%v", err)
			return 0, "", err
		}
	case uint32(yc.MsgType_MsgTypeChannelLiveMsg):
		msgType = uint32(qmsg.ChatMsgType_ChatMsgTypeChannelLiveMsg)
		m, err := utils2.NewMapWithJson([]byte(ycChatMsg.MsgContent))
		if err != nil {
			log.Errorf("err:%v", err)
			return 0, "", err
		}
		channelMsg := yc.ChannelMsg{
			Avatar:     m.GetString("avatar"),
			CoverUrl:   m.GetString("cover_url"),
			Desc:       m.GetString("desc"),
			FeedType:   m.GetUint32("feed_type"),
			Nickname:   m.GetString("nickname"),
			Url:        m.GetString("url"),
			Thumburl_1: m.GetString("thumb_url"),
			Title:      m.GetString("title"),
			Des:        m.GetString("desc"),
		}
		chatMsg := &qmsg.ChatChannelMsg{
			Title:       channelMsg.Title,
			CoverUrl:    channelMsg.CoverUrl,
			Href:        channelMsg.Url,
			MsgSerialNo: ycChatMsg.MsgSerialNo,
			Name:        channelMsg.Nickname,
			Icon:        channelMsg.Avatar,
		}
		msg, err = utils.Pb2JsonSkipDefaults(chatMsg)
		if err != nil {
			log.Errorf("err:%v", err)
			return 0, "", err
		}
	case uint32(yc.MsgType_MsgTypeAtText):
		msgType = uint32(qmsg.ChatMsgType_ChatMsgTypeRichTxt)
		chatMsg := &qmsg.ChatMsg{
			ChatMsgType: uint32(qmsg.ChatMsgType_ChatMsgTypeRichTxt),
			Msg:         ycChatMsg.MsgContent,
			AtList:      []*qmsg.ChatMsgAt{},
		}
		text := ""
		for _, at := range ycChatMsg.Ats {
			if at.Type == uint32(qopen.MessagesData_AtsItem_AtContact) {
				ycChatMsg.AtContactSerialNos = append(ycChatMsg.AtContactSerialNos, at.Value)
			}
		}
		ycChatMsg.AtContactSerialNos = pie.Strings(ycChatMsg.AtContactSerialNos).Unique()
		// 查询出所有的at人的信息
		memberMap := map[string]string{}
		groupMemberListRsp, err := qrobot.GetGroupChatMemberListSys(nil, &qrobot.GetGroupChatMemberListSysReq{
			CorpId: corpId,
			AppId:  appId,
			ListOption: core.NewListOption().
				AddOpt(uint32(qrobot.GetGroupChatMemberListSysReq_ListOptionGroupIdList), groupId).
				AddOpt(uint32(qrobot.GetGroupChatMemberListSysReq_ListOptionStrMemberIdList),
					ycChatMsg.AtContactSerialNos),
		})
		if err != nil {
			log.Errorf("err:%v", err)
		} else {
			qrobot.GroupChatMemberList(groupMemberListRsp.List).
				Each(func(member *qrobot.ModelGroupChatMember) {
					memberMap[member.StrMemberId] = member.MemberName
				})
		}
		atLocation := 0
		for _, at := range ycChatMsg.Ats {
			switch at.Type {
			case uint32(qopen.MessagesData_AtsItem_Text):
				text += at.Value
				atLocation += len([]rune(at.Value))
			case uint32(qopen.MessagesData_AtsItem_AtContact):
				chatMsg.AtList = append(chatMsg.AtList, &qmsg.ChatMsgAt{
					LocationAt:  uint32(atLocation),
					AtAll:       false,
					Name:        memberMap[at.Value],
					StrMemberId: at.Value,
				})
			default:
				return 0, "", rpc.InvalidArg("invalid at type")
			}
		}
		chatMsg.Msg = text
		msg, err = utils.Pb2JsonSkipDefaults(chatMsg)
		if err != nil {
			log.Errorf("err:%v", err)
			return 0, "", err
		}
	case uint32(yc.MsgType_MsgTypeProductMsg):
		msgType = uint32(qmsg.ChatMsgType_ChatMsgTypeProductMsg)
		chatMsg := &qmsg.ChatProductMsg{
			Title:           ycChatMsg.ProductMsgProductTitle,
			CoverUrl:        ycChatMsg.ProductMsgCoverUrl,
			MarketPrice:     ycChatMsg.ProductMsgMarketPrice,
			SellingPrice:    ycChatMsg.ProductMsgSellingPrice,
			PagePath:        ycChatMsg.ProductMsgPagePath,
			ProductId:       ycChatMsg.ProductMsgProductId,
			AppId:           ycChatMsg.ProductMsgAppId,
			PlatformName:    ycChatMsg.ProductMsgPlatformName,
			PlatformHeadImg: ycChatMsg.ProductMsgPlatformHeadImg,
			MsgSerialNo:     ycChatMsg.MsgSerialNo,
		}

		msg, err = utils.Pb2JsonSkipDefaults(chatMsg)
		if err != nil {
			log.Errorf("err:%v", err)
			return 0, "", err
		}
	}
	return msgType, msg, nil
}

func addGroupMsg4Send(ctx *rpc.Context, cliMsgId string, robot *qrobot.ModelAccount, group *qrobot.ModelGroupChat, ycMsg *yc.SendMessagesData, msgId string, ycFileId uint64, skipWait bool) error {
	// 2. 本地插入消息
	msg := &qmsg.ModelMsgBox{
		Uid:             robot.Uid,
		CorpId:          robot.CorpId,
		AppId:           robot.AppId,
		SentAt:          utils.Now(),
		MsgType:         uint32(qmsg.MsgType_MsgTypeChat),
		CliMsgId:        cliMsgId,
		ChatType:        uint32(qmsg.ChatType_ChatTypeGroup),
		SenderAccountId: robot.Id,
		ChatId:          group.Id,
	}
	var err error
	msg.ChatMsgType, msg.Msg, err = coverYcSendMsg2ChatMsg(ctx, robot.CorpId, robot.AppId, group.Id, ycMsg, ycFileId)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	chat, lastSeq, err := addMsg(ctx, msg, skipWait)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	if strings.HasPrefix(cliMsgId, aiMsgPrefix) {
		// 记录智能回复
		err = aiEntertainDetail.Create(ctx, &qmsg.ModelAiEntertainDetail{
			CorpId:   robot.CorpId,
			AppId:    robot.AppId,
			RobotUid: robot.Uid,
			CliMsgId: cliMsgId,
			YcMsgId:  msgId,
			SentAt:   msg.SentAt,
			MsgBoxId: msg.Id,
		})
		if err != nil {
			log.Errorf("err %v", err)
			return err
		}
	}

	err = SendRecord.updateAndCheckDelay(ctx, robot.CorpId, robot.AppId, map[string]interface{}{
		DbCorpId:   robot.CorpId,
		DbAppId:    robot.AppId,
		DbCliMsgId: cliMsgId,
		DbRobotUid: robot.Uid,
	}, map[string]interface{}{
		DbYcMsgId:  msgId,
		DbSentAt:   msg.SentAt,
		DbMsgBoxId: msg.Id,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	wrapperList := []*qmsg.WsMsgWrapper{
		{
			MsgType: uint32(qmsg.WsMsgWrapper_MsgTypeMsgBox),
			MsgBox:  msg,
			Chat:    chat,
			MsgId:   msgId,
			LastSeq: lastSeq,
		},
		// 兼容 1.0 ws ，日后废弃
		{
			MsgType: uint32(qmsg.WsMsgWrapper_MsgTypeMsgSeqChanged),
			MsgSeqChanged: &qmsg.WsMsgWrapper_MsgSeqChanged{
				RobotUid:     msg.Uid,
				RemoteMsgSeq: msg.MsgSeq,
			},
		},
	}
	err = pushWsMsgByChat(ctx, chat, nil, wrapperList)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	err = logInterval(ctx, msg.CorpId, msg.Uid, msgId, "addGroupMsg4Send")
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	return nil
}
func addSingleMsg4Send(ctx *rpc.Context, cliMsgId string, robot *qrobot.ModelAccount, acc *qrobot.ModelAccount, ycMsg *yc.SendMessagesData, msgId string, skipWait bool) error {
	// 2. 本地插入消息
	msg := &qmsg.ModelMsgBox{
		Uid:             robot.Uid,
		CorpId:          robot.CorpId,
		AppId:           robot.AppId,
		SentAt:          utils.Now(),
		MsgType:         uint32(qmsg.MsgType_MsgTypeChat),
		CliMsgId:        cliMsgId,
		ChatType:        uint32(qmsg.ChatType_ChatTypeSingle),
		SenderAccountId: robot.Id,
		ChatId:          acc.Id,
	}
	var err error
	msg.ChatMsgType, msg.Msg, err = coverYcSendMsg2ChatMsg(ctx, msg.CorpId, msg.AppId, msg.ChatId, ycMsg, 0)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	chat, lastSeq, err := addMsg(ctx, msg, skipWait)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	if strings.HasPrefix(cliMsgId, aiMsgPrefix) {
		err = aiEntertainDetail.Create(ctx, &qmsg.ModelAiEntertainDetail{
			CorpId:   robot.CorpId,
			AppId:    robot.AppId,
			RobotUid: robot.Uid,
			CliMsgId: cliMsgId,
			YcMsgId:  msgId,
			SentAt:   msg.SentAt,
			MsgBoxId: msg.Id,
		})
		if err != nil {
			log.Errorf("err %v", err)
			return err
		}
	}

	err = SendRecord.updateAndCheckDelay(ctx, robot.CorpId, robot.AppId, map[string]interface{}{
		DbCorpId:   robot.CorpId,
		DbAppId:    robot.AppId,
		DbCliMsgId: cliMsgId,
		DbRobotUid: robot.Uid,
	}, map[string]interface{}{
		DbYcMsgId:  msgId,
		DbSentAt:   msg.SentAt,
		DbMsgBoxId: msg.Id,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	wrapperList := []*qmsg.WsMsgWrapper{
		{
			MsgType: uint32(qmsg.WsMsgWrapper_MsgTypeMsgBox),
			MsgBox:  msg,
			Chat:    chat,
			MsgId:   msgId,
			LastSeq: lastSeq,
		},
		// 兼容 1.0 ws ，日后废弃
		{
			MsgType: uint32(qmsg.WsMsgWrapper_MsgTypeMsgSeqChanged),
			MsgSeqChanged: &qmsg.WsMsgWrapper_MsgSeqChanged{
				RobotUid:     msg.Uid,
				RemoteMsgSeq: msg.MsgSeq,
			},
		},
	}
	err = pushWsMsgByChat(ctx, chat, nil, wrapperList)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	err = logInterval(ctx, msg.CorpId, msg.Uid, msgId, "addSingleMsg4Send")
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	return nil
}

func YcGroupSendCallbackV2(ctx *rpc.Context, req *qsm.SendMessageCallbackReq) (*qsm.SendMessageCallbackRsp, error) {
	var rsp qsm.SendMessageCallbackRsp
	var err error
	if req.GetErrCode() != 0 {
		log.Warnf("send fail, code:%+v,msg:%+v skip", req.ErrCode, req.ErrMsg)
		return &rsp, nil
	}
	if req.BizContext == nil || req.BizContext.YcReq == nil || req.BizContext.YcRsp == nil || (req.BizContext.YcReq != nil && req.BizContext.YcReq.GroupReq == nil) || (req.BizContext.YcRsp != nil && req.BizContext.YcRsp.GroupRsp == nil) {
		log.Warn("BizContext YcReq Or YcRsp is nil ")
		return &rsp, err
	}
	log.Debugf("YcGroupSendCallbackV2.BizContext :%v", req.BizContext)
	core.SetCorpAndApp(ctx, req.BizContext.CorpId, req.BizContext.AppId)
	// 拉下机器人信息
	robot, err := getRobotBySn(ctx, req.BizContext.CorpId, req.BizContext.AppId, req.RobotSn)
	if err != nil {
		if rpc.GetErrCode(err) != qrobot.ErrAccountNotFound {
			log.Errorf("err:%v", err)
			return nil, err
		} else {
			return &rsp, nil
		}
	}

	ycReq := req.BizContext.YcReq.GroupReq
	msgIdx := req.BizContext.YcRsp.MsgIndex
	if msgIdx < 0 || msgIdx > uint32(len(ycReq.Data))-1 {
		return nil, rpc.InvalidArg("msg idx out of range, len %d, idx %d", len(ycReq.Data), msgIdx)
	}
	ycMsg := ycReq.Data[msgIdx]

	// 拉下群
	groupRsp, err := qrobot.GetGroupChatListSys(ctx, &qrobot.GetGroupChatListSysReq{
		ListOption: core.NewListOption().SetSkipCount().SetLimit(1).
			AddOpt(qrobot.GetGroupChatListSysReq_ListOptionStrGroupId, ycReq.GroupSerialNo),
		CorpId: robot.CorpId,
		AppId:  robot.AppId,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	if len(groupRsp.List) == 0 {
		return nil, rpc.InvalidArg("group not found")
	}

	group := groupRsp.List[0]
	isWatch := false
	for _, uid := range group.WatchRobotIdList {
		if uid == robot.Uid {
			isWatch = true
			break
		}
	}
	for _, uid := range group.WatchHostAccountUidList {
		if uid == robot.Uid {
			isWatch = true
			break
		}
	}

	if !isWatch {
		return &rsp, nil
	}

	cliMsgId := req.MsgId

	if strings.Contains(cliMsgId, ".qmsg_retry") {

		tmpList := strings.Split(cliMsgId, ".")
		var tmpMsgId string
		for _, v := range tmpList {
			if strings.Contains(v, "qmsg_retry") {
				continue
			}
			tmpMsgId += v
		}
		log.Infof("split msg id [%s] [%s]", cliMsgId, tmpMsgId)
		cliMsgId = tmpMsgId
	}

	if len(ycReq.Data) > 1 {
		cliMsgId = fmt.Sprintf("%s_%d", cliMsgId, msgIdx)
	}

	ycRspMsgId := ""
	if req.BizContext.YcRsp.GroupRsp.Data != nil {
		ycRspMsgId = req.BizContext.YcRsp.GroupRsp.Data.MsgId
	}
	err = addGroupMsg4Send(ctx, cliMsgId, robot, group, ycMsg, ycRspMsgId, req.YcFileId, req.BizContext.ModuleName == "qmsg")
	if err != nil {
		if rpc.GetErrCode(err) == rpc.InvalidArgErrCode {
			return &rsp, nil
		}
		log.Errorf("err:%v", err)
		return nil, err
	}
	return &rsp, nil
}

func YcPrivateSendCallbackV2(ctx *rpc.Context, req *qsm.SendMessageCallbackReq) (*qsm.SendMessageCallbackRsp, error) {
	var rsp qsm.SendMessageCallbackRsp
	var err error
	core.SetCorpAndApp(ctx, req.BizContext.CorpId, req.BizContext.AppId)
	// 拉下机器人信息
	robot, err := getRobotBySn(ctx, req.BizContext.CorpId, req.BizContext.AppId, req.RobotSn)
	if err != nil {
		if rpc.GetErrCode(err) != qrobot.ErrAccountNotFound {
			log.Errorf("err:%v", err)
			return nil, err
		} else {
			return &rsp, nil
		}
	}

	var ycReq *yc.SendMessagesToMyCustomerReq
	if req.BizContext != nil && req.BizContext.YcReq != nil {
		ycReq = req.BizContext.YcReq.PrivateReq
	}

	if req.ErrCode != 0 {
		log.Warnf("send fail, code:%+v,msg:%+v skip", req.ErrCode, req.ErrMsg)
		msg := &qmsg.WsMsgWrapper_SendChatMsgFail{
			ErrCode:  int32(yc.ErrApiBizErr),
			ErrMsg:   req.ErrMsg,
			CliMsgId: req.MsgId,
			RobotUid: robot.Uid,
		}
		err = yc.ExecMessage(ctx, req.RobotSn, req.ErrMsg)
		if err != nil {
			if x, ok := err.(*rpc.ErrMsg); ok {
				msg.ErrCode = x.ErrCode
				msg.ErrMsg = x.ErrMsg
			} else {
				msg.ErrCode = -1
				msg.ErrMsg = err.Error()
			}
		}

		var sendRecord qmsg.ModelSendRecord
		err = SendRecord.WhereCorpApp(robot.CorpId, robot.AppId).Where(DbRobotUid, robot.Uid).Where(DbCliMsgId, req.MsgId).First(ctx, &sendRecord)
		if err != nil {
			if SendRecord.IsNotFoundErr(err) {
				return &rsp, nil
			}
			log.Errorf("err %v", err)
			return nil, err
		}

		LogStream.LogStreamSubType(ctx, SendFailSubType, &SendFailStream{
			CorpId:   req.BizContext.CorpId,
			RobotUid: robot.Uid,
			ErrCode:  uint32(msg.ErrCode),
		})

		// 拉下人
		accRsp, err := qrobot.GetAccountByYcSerialNo(ctx, &qrobot.GetAccountByYcSerialNoReq{
			CorpId:     robot.CorpId,
			AppId:      robot.AppId,
			YcSerialNo: ycReq.ContactSerialNo,
		})
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}

		wrapper := &qmsg.WsMsgWrapper{
			MsgType:         uint32(qmsg.WsMsgWrapper_MsgTypeSendChatMsgFail),
			SendChatMsgFail: msg,
		}
		err = pushWsMsgAssignPriority(ctx, robot.CorpId, robot.AppId, robot.Uid, accRsp.Account.Id, false, []*qmsg.WsMsgWrapper{wrapper})
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
		return &rsp, nil
	}

	// 由创报错会导致这里painc
	if ycReq == nil || req.BizContext.YcRsp == nil {
		return nil, rpc.InvalidArg("YcReq or YcRsp nil.")
	}

	// TODO WS
	msgIdx := req.BizContext.YcRsp.MsgIndex
	if msgIdx < 0 {
		return nil, rpc.InvalidArg("msg idx out of range, len %d, idx %d", len(ycReq.Data), msgIdx)
	}
	ycMsg := ycReq.Data[msgIdx]

	// 拉下人
	accRsp, err := qrobot.GetAccountByYcSerialNo(ctx, &qrobot.GetAccountByYcSerialNoReq{
		CorpId:     robot.CorpId,
		AppId:      robot.AppId,
		YcSerialNo: ycReq.ContactSerialNo,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	acc := accRsp.Account
	cliMsgId := req.MsgId

	if strings.Contains(cliMsgId, ".qmsg_retry") {

		tmpList := strings.Split(cliMsgId, ".")
		var tmpMsgId string
		for _, v := range tmpList {
			if strings.Contains(v, "qmsg_retry") {
				continue
			}
			tmpMsgId += v
		}
		log.Infof("split msg id [%s] [%s]", cliMsgId, tmpMsgId)
		cliMsgId = tmpMsgId
	}

	if len(ycReq.Data) > 1 {
		cliMsgId = fmt.Sprintf("%s_%d", cliMsgId, msgIdx)
	}
	err = addSingleMsg4Send(ctx, cliMsgId, robot, acc, ycMsg, req.BizContext.YcRsp.PrivateRsp.Data.MsgId, req.BizContext.ModuleName == "qmsg")
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	return &rsp, nil
}

// 最纯粹的会话列表
func GetChatListSys(ctx *rpc.Context, req *qmsg.GetChatListSysReq) (*qmsg.GetChatListSysRsp, error) {
	var rsp qmsg.GetChatListSysRsp

	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)

	var (
		groupRspMap  = false
		assignRspMap = false
	)

	db := Chat.NewList(req.ListOption).WhereCorpApp(corpId, appId)

	err := core.NewListOptionProcessor(req.ListOption).
		AddUint64List(qmsg.GetChatListSysReq_ListOptionIdList, func(valList []uint64) error {
			db.WhereIn(DbId, valList)
			return nil
		}).
		AddBool(qmsg.GetChatListSysReq_ListOptionOnlyUnRead, func(val bool) error {
			db.Where(DbUnreadCount, ">", 0)
			return nil
		}).
		AddBool(qmsg.GetChatListSysReq_ListOptionGroupRspMap, func(val bool) error {
			groupRspMap = true
			return nil
		}).
		AddBool(qmsg.GetChatListSysReq_ListOptionAssignRspMap, func(val bool) error {
			assignRspMap = true
			return nil
		}).
		AddUint32(qmsg.GetChatListSysReq_ListOptionOrderBy, func(val uint32) error {
			if val == uint32(qmsg.GetChatListSysReq_OrderByMsgSeqDesc) {
				db.OrderDesc(DbMsgSeq)
			} else if val == uint32(qmsg.GetChatListSysReq_OrderByMsgSeqAsc) {
				db.OrderAsc(DbMsgSeq)
			}
			return nil
		}).
		AddUint32(qmsg.GetChatListSysReq_ListOptionNextMsgSeq, func(val uint32) error {
			db.Where(DbMsgSeq, "<", val)
			return nil
		}).
		AddUint32(qmsg.GetChatListSysReq_ListOptionGtMsgSeq, func(val uint32) error {
			db.Where(DbMsgSeq, ">", val)
			return nil
		}).
		AddUint64(qmsg.GetChatListSysReq_ListOptionRobotUid, func(val uint64) error {
			db.Where(DbRobotUid, val)
			return nil
		}).
		AddUint64List(qmsg.GetChatListSysReq_ListOptionRobotUidList, func(valList []uint64) error {
			db.WhereIn(DbRobotUid, valList)
			return nil
		}).
		AddUint64List(qmsg.GetChatListSysReq_ListOptionChatExtIdList, func(valList []uint64) error {
			db.WhereIn(DbChatExtId, valList)
			return nil
		}).
		Process()
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	rsp.Paginate, err = db.FindPaginate(ctx, &rsp.List)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	if groupRspMap {
		var groupIdList []uint64
		for _, chat := range rsp.List {
			if chat.IsGroup {
				groupIdList = append(groupIdList, chat.ChatExtId)
			}
		}

		if len(groupIdList) > 0 {
			groupRsp, err := qrobot.GetGroupChatListSys(ctx, &qrobot.GetGroupChatListSysReq{
				ListOption: core.NewListOption().SetLimit(uint32(len(groupIdList))).SetSkipCount().AddOpt(qrobot.GetGroupChatListSysReq_ListOptionGroupIdList, groupIdList),
				CorpId:     corpId,
				AppId:      appId,
			})
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, err
			}
			var groupMap map[uint64]*qrobot.ModelGroupChat
			utils.KeyBy(groupRsp.List, "Id", &groupMap)
			rsp.GroupMap = groupMap
		}
	}

	if assignRspMap {
		chatIdList := utils.PluckUint64(rsp.List, "Id")
		if len(chatIdList) > 0 {
			assignChatList, err := AssignChat.getListByCid(ctx, corpId, appId, chatIdList)
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, err
			}

			var assignChatMap map[uint64]*qmsg.ModelAssignChat
			utils.KeyBy(assignChatList, "Id", &assignChatMap)
			rsp.AssignChatMap = assignChatMap
		}

	}

	for _, chat := range rsp.List {
		if chat.LastReceiveAt == 0 {
			chat.LastReceiveAt = chat.CreatedAt
		}
	}

	return &rsp, nil
}

func GetUserChatList(ctx *rpc.Context, req *qmsg.GetUserChatListReq) (*qmsg.GetUserChatListRsp, error) {
	var rsp qmsg.GetUserChatListRsp

	u, err := getUserCheck(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	if !u.IsAssign {
		log.Errorf("[%d]该账号为非分配资产", u.Id)
		return nil, rpc.CreateError(int32(qmsg.ErrCode_ErrUserAssignType))
	}

	if u.ContactFilter != nil {
		if u.ContactFilter.FilterExtContact != nil {
			if u.ContactFilter.FilterExtContact.ExtContactCondition != uint32(quan.AdvanceExtContactFilter_ExtContactConditionAll) {
				log.Errorf("非全部资产账号: %v", u)
				return nil, rpc.CreateError(int32(qmsg.ErrCode_ErrUserAssignType))
			}
		}
	}

	if u.GroupFilter != nil {
		if u.GroupFilter.SelectType != uint32(qmsg.GroupFilter_SelectTypeAll) {
			log.Errorf("非全部资产账号: %v", u)
			return nil, rpc.CreateError(int32(qmsg.ErrCode_ErrUserAssignType))
		}
	}

	if req.AdvanceReq == nil {
		_, err = UserRobot.getByFull(ctx, u.CorpId, u.AppId, u.Id, req.RobotUid)
		if err != nil {
			if UserRobot.IsNotFoundErr(err) {
				return nil, rpc.CreateError(int32(qmsg.ERRNoAuthorization))
			}
			log.Errorf("err:%v", err)
			return nil, err

		}

		listRsp, err := GetChatListSys(ctx, &qmsg.GetChatListSysReq{
			ListOption: req.ListOption.AddOpt(qmsg.GetChatListSysReq_ListOptionRobotUid, req.RobotUid),
		})
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
		rsp.List = listRsp.List
		rsp.Paginate = listRsp.Paginate
		rsp.GroupMap = listRsp.GroupMap
		rsp.AccountMap = listRsp.AccountMap
		rsp.AssignChatMap = listRsp.AssignChatMap
	} else {
		advanceReq := req.AdvanceReq
		listRsp, err := GetAdvanceChatList(ctx, &qmsg.GetAdvanceChatListReq{
			ResultKey: advanceReq.ResultKey,
			Start:     req.ListOption.Offset,
			Limit:     req.ListOption.Limit,
			UidList:   advanceReq.UidList,
		})
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
		rsp.List = listRsp.ChatList
		rsp.GroupMap = listRsp.GroupMap
		rsp.AccountMap = listRsp.AccountMap
		rsp.AssignChatMap = listRsp.AssignChatMap

		//{
		//	// -----------------TODO： 增加qmsg account 的返回，后续需要跟前端约定统一调整，废弃原有的account map------------------------//
		//	var accList []*qrobot.ModelAccount
		//	for _, v := range listRsp.AccountMap {
		//		accList = append(accList, v)
		//	}
		//
		//	if len(accList) > 0 {
		//		tmpList, err := refreshLatestProfile(ctx, u.CorpId, u.AppId, req.RobotUid, accList)
		//		if err != nil {
		//			log.Errorf("err:%v", err)
		//			return nil, err
		//		}
		//
		//		rsp.QmsgAccountMap = make(map[uint64]*qmsg.Account)
		//		for _, v := range tmpList {
		//			rsp.QmsgAccountMap[v.Id] = v
		//		}
		//	}
		//}
	}

	return &rsp, nil
}

func GetChatIdList(ctx *rpc.Context, req *qmsg.GetChatIdListReq) (*qmsg.GetChatIdListRsp, error) {
	var rsp qmsg.GetChatIdListRsp

	u, err := getUserCheck(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	var chatIdList []uint64

	if u.IsAssign {

		if u.ContactFilter != nil {
			if u.ContactFilter.FilterExtContact != nil {
				filter := u.ContactFilter.FilterExtContact

				if filter.ExtContactCondition == uint32(quan.AdvanceExtContactFilter_ExtContactConditionAll) {
					return nil, rpc.InvalidArg("非法请求")
				}

				if u.ContactFilter.FilterExtContact.ExtContactCondition == uint32(quan.AdvanceExtContactFilter_ExtContactConditionAssign) {
					cidList, err := getChatIdList(ctx, u.CorpId, u.AppId, u.Id, false, "", filter.ExtUidList)
					if err != nil {
						log.Errorf("err:%v", err)
						return nil, err
					}

					if len(cidList) > 0 {
						chatIdList = append(chatIdList, cidList...)
					}

				} else if u.ContactFilter.FilterExtContact.ExtContactCondition == uint32(quan.AdvanceExtContactFilter_ExtContactConditionFilter) {
					// 先不管
				}
			}
		}

		if u.GroupFilter != nil {
			if u.GroupFilter.SelectType == uint32(qmsg.GroupFilter_SelectTypeAll) {
				return nil, rpc.InvalidArg("非法请求")
			} else if u.GroupFilter.SelectType == uint32(qmsg.GroupFilter_SelectTypeAssign) {
				cidList, err := getChatIdList(ctx, u.CorpId, u.AppId, u.Id, true, DbChatId, u.GroupFilter.GroupIdList)
				if err != nil {
					log.Errorf("err:%v", err)
					return nil, err
				}

				chatIdList = append(chatIdList, cidList...)
			} else if u.GroupFilter.SelectType == uint32(qmsg.GroupFilter_SelectTypeCategory) {
				groupRsp, err := qrobot.GetGroupChatListSys(ctx, &qrobot.GetGroupChatListSysReq{
					ListOption: core.NewListOption().AddOpt(qrobot.GetGroupChatListSysReq_ListOptionGroupIdList, u.GroupFilter.GroupCategoryIdList),
					CorpId:     u.CorpId,
					AppId:      u.AppId,
					Unscoped:   false,
				})
				if err != nil {
					log.Errorf("err:%v", err)
					return nil, err
				}

				if len(groupRsp.List) != 0 {
					idList := utils.PluckUint64(groupRsp.List, "Id")

					cidList, err := getChatIdList(ctx, u.CorpId, u.AppId, u.Id, true, DbChatId, idList)
					if err != nil {
						log.Errorf("err:%v", err)
						return nil, err
					}

					if len(cidList) > 0 {
						chatIdList = append(chatIdList, cidList...)
					}

				}
			}
		}

		rsp.ChatIdList = chatIdList

	} else {
		var assignChatList []*qmsg.ModelAssignChat
		err = AssignChat.WhereCorpApp(u.CorpId, u.AppId).Where(DbUid, u.Id).Find(ctx, &assignChatList)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}

		rsp.ChatIdList = utils.PluckUint64(assignChatList, "ChatId")
	}

	return &rsp, nil
}

func getChatIdList(ctx *rpc.Context, corpId, appId uint32, uid uint64, isGroup bool, InScope string, idList []uint64) ([]uint64, error) {
	var assignList []*qmsg.ModelAssignChat
	err := AssignChat.WhereCorpApp(corpId, appId).Where(DbUid, uid).Where(DbIsGroup, isGroup).
		WhereIn(InScope, idList).Find(ctx, &assignList)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	if len(assignList) != 0 {
		idList := utils.PluckUint64(assignList, "Cid")

		return idList, nil
	}

	return []uint64{}, nil
}

func ResetChatUnread(ctx *rpc.Context, req *qmsg.ResetChatUnreadReq) (*qmsg.ResetChatUnreadRsp, error) {
	var rsp qmsg.ResetChatUnreadRsp

	u, err := getUserCheck(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	var chat *qmsg.ModelChat
	if req.ChatId != 0 {
		chat, err = Chat.get(ctx, u.CorpId, u.AppId, req.ChatId)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
	} else {
		chat, err = Chat.getByRobotAndExt(ctx, u.CorpId, u.AppId, req.RobotUid, req.ChatExtId, req.IsGroup)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
	}

	readType := uint32(qsm.MarkAsReadType_TypeUnread)

	if req.Count == 0 {
		readType = uint32(qsm.MarkAsReadType_TypeRead)
	}

	_, err = Chat.WhereCorpApp(u.CorpId, u.AppId).Where(DbId, req.ChatId).Update(ctx, map[string]interface{}{
		DbUnreadCount: req.Count,
	})
	if err != nil {
		//warning.ReportMsg(ctx, fmt.Sprintf("err: %v", err))
		log.Errorf("err:%v", err)
		return nil, err
	}

	//记录最后已读
	var tmp qmsg.ModelLastChat
	_, err = LastChat.UpdateOrCreate(ctx, map[string]interface{}{
		DbCorpId:       u.CorpId,
		DbAppId:        u.AppId,
		DbUid:          u.Id,
		DbCid:          chat.Id,
		DbLastChatType: uint32(qmsg.GetLastChatListReq_LastChatTypeRead),
	}, map[string]interface{}{
		DbLastTimeAt: utils.Now(),
	}, &tmp)

	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	// 需要告警数据的corp列表
	var NeedAlertCorpList = []uint32{2000772, 26697, 26474, 26317, 23788, 2033}
	if utils.InSliceUint32(u.CorpId, NeedAlertCorpList) {
		_ = ResetChatUnreadRecord.Create(ctx, &qmsg.ModelResetChatUnreadRecord{
			CorpId:    u.CorpId,
			AppId:     u.AppId,
			Uid:       u.Id,
			Cid:       chat.Id,
			MsgSeq:    req.MsgSeq,
			RobotUid:  chat.RobotUid,
			ChatExtId: chat.ChatExtId,
			ClientIp:  ctx.GetUserIp(),
			Sid:       ctx.GetReqHeader("X-Device-Sid"),
			BaseReqId: ctx.GetBaseReqId(),
		})
	}

	qwhaleSrv := qwhaleService{}
	hostAccount, err := qwhaleSrv.getHostAccountByUid(ctx, chat.CorpId, chat.AppId, chat.RobotUid)
	if err == nil && hostAccount == nil {
		log.Warnf("warn:由创已读接口只支持扫码号:%v", chat)
		return &rsp, nil
	}

	_, err = qsm.MarkAsReadForIm(ctx, &qsm.MarkAsReadForImReq{
		MsgSeq:   chat.MsgSeq,
		Type:     readType,
		RobotUid: chat.RobotUid,
		ExtId:    chat.ChatExtId,
		IsGroup:  chat.IsGroup,
	})
	if err != nil {
		if rpc.GetErrCode(err) == qrobot.ErrGroupChatNotFound {
			return &rsp, nil
		}
		log.Errorf("err:%v", err)
		return nil, err
	}

	return &rsp, nil
}

func BatchResetChatUnread(ctx *rpc.Context, req *qmsg.BatchResetChatUnreadReq) (*qmsg.BatchResetChatUnreadRsp, error) {
	var rsp qmsg.BatchResetChatUnreadRsp

	user, err := getUserCheck(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	db := Chat.WhereCorpApp(user.CorpId, user.AppId).Where(DbUnreadCount, ">", 0)

	if !user.IsAssign {
		assignChatList, err := AssignChat.getListByUid(ctx, user.CorpId, user.AppId, user.Id)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}

		cidList := utils.PluckUint64(assignChatList, "Cid")
		db.WhereIn(DbId, cidList)
	}

	err = core.NewListOptionProcessor(req.ListOption).
		AddUint64List(qmsg.BatchResetChatUnreadReq_ListOptionRobotUidList, func(valList []uint64) error {
			if user.IsAssign {
				db.WhereIn(DbRobotUid, valList)
			}
			return nil
		}).
		AddBool(qmsg.BatchResetChatUnreadReq_ListOptionIsGroup, func(val bool) error {
			db.Where(DbIsGroup, val)
			return nil
		}).
		Process()
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	var chatList []*qmsg.ModelChat
	err = db.Find(ctx, &chatList)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	chatIdList := utils.PluckUint64(chatList, "Id")
	// 得到企业成员的信息
	chatIdList.Chunk(500, func(chunk pie.Uint64s) bool {

		_, err = Chat.WhereCorpApp(user.CorpId, user.AppId).Gt(DbUnreadCount, 0).
			WhereIn(DbId, chunk).Update(ctx, map[string]interface{}{
			DbUnreadCount: 0,
		})
		if err != nil {
			log.Errorf("err:%v", err)
			return true
		}
		return false
	})

	if err != nil {
		//warning.ReportMsg(ctx, fmt.Sprintf("err: %v", err))
		log.Errorf("err:%v", err)
		return nil, err
	}

	routine.Go(ctx, func(ctx *rpc.Context) error {
		qwhaleSrv := qwhaleService{}
		for _, chat := range chatList {

			hostAccount, err := qwhaleSrv.getHostAccountByUid(ctx, chat.CorpId, chat.AppId, chat.RobotUid)
			if err == nil && hostAccount == nil {
				log.Warnf("warn:由创已读接口只支持扫码号:%v", chat)
				continue
			}

			_, err = qsm.MarkAsReadForIm(ctx, &qsm.MarkAsReadForImReq{
				MsgSeq:   chat.MsgSeq,
				Type:     uint32(qsm.MarkAsReadType_TypeRead),
				RobotUid: chat.RobotUid,
				ExtId:    chat.ChatExtId,
				IsGroup:  chat.IsGroup,
			})
			if err != nil {
				log.Errorf("err:%v", err)
				return err
			}
		}
		return nil
	})

	return &rsp, nil
}

func GetChat(ctx *rpc.Context, req *qmsg.GetChatReq) (*qmsg.GetChatRsp, error) {
	var rsp qmsg.GetChatRsp

	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)

	chat, assignChat, err := getChat(ctx, corpId, appId, req.RobotUid, req.ChatExtId, req.IsGroup, true)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	if chat.LastReceiveAt == 0 {
		chat.LastReceiveAt = chat.CreatedAt
	}

	rsp.Chat = chat
	rsp.AssignChat = assignChat
	// 无需回复设置
	handler := &StatHandler{
		CorpId:            chat.CorpId,
		AppId:             chat.AppId,
		MsgTime:           utils.Now(),
		RobotUid:          chat.RobotUid,
		CustomerAccountId: chat.ChatExtId,
		ChatId:            chat.Id,
	}
	matchStateRule, err := handler.CheckIsMatchStatRule(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	if matchStateRule {
		err = handler.Init(ctx)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
		rsp.EnableNoRequiredReply = handler.GetEnableNoRequiredReply()
		if rsp.EnableNoRequiredReply {
			rsp.EnableDoNoRequiredReply = handler.GetEnableDoNoRequiredReply()
		}
	}

	if !req.IsGroup {
		user, err := getUserCheck(ctx)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}

		var relaList []*qmsg.ModelUserTagRelation
		err = UserTagRelation.WhereCorpApp(corpId, appId).Where(DbUid, user.Id).Where(DbAccountId, chat.ChatExtId).Find(ctx, &relaList)
		if err != nil {
			log.Errorf("err %v", err)
			return nil, err
		}

		tagIdList := utils.PluckUint64(relaList, "Id")

		if len(tagIdList) > 0 {
			var tagList []*qmsg.ModelUserTag
			err = UserTag.WhereCorpApp(corpId, appId).Where(DbUid, user.Id).WhereIn(DbId, tagIdList).Find(ctx, &tagList)
			if err != nil {
				log.Errorf("err %v", err)
				return nil, err
			}
			rsp.TagMap = make(map[uint64]string)
			for _, v := range tagList {
				rsp.TagMap[v.Id] = v.TagName
			}
		}

	}

	return &rsp, nil
}

func GetAdvanceChatList(ctx *rpc.Context, req *qmsg.GetAdvanceChatListReq) (*qmsg.GetAdvanceChatListRsp, error) {
	var rsp qmsg.GetAdvanceChatListRsp

	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)

	listRsp, err := iquan.GetAdvanceExtContactList(ctx, &iquan.GetAdvanceExtContactListReq{
		ResultKey: req.ResultKey,
		Start:     req.Start,
		Limit:     req.Limit,
		CorpId:    corpId,
		AppId:     appId,
		GroupBy:   uint32(iquan.GetAdvanceExtContactListReq_GroupByFollow),
		UidList:   req.UidList,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	extUidList := utils.PluckUint64(listRsp.FollowList, "ExtUid")
	uidList := utils.PluckUint64(listRsp.FollowList, "Uid")

	if len(uidList) == 0 || len(extUidList) == 0 {
		return &rsp, nil
	}

	accListRsp, err := qrobot.GetAccountListSys(ctx, &qrobot.GetAccountListSysReq{
		ListOption: core.NewListOption().SetSkipCount().SetLimit(uint32(len(extUidList))).
			AddOpt(qrobot.GetAccountListSysReq_ListOptionExtUidList, extUidList),
		CorpId: corpId,
		AppId:  appId,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	if len(listRsp.FollowList) > len(accListRsp.List) {
		//warning.ReportMsg(ctx, "follow larger than account %d", len(listRsp.FollowList)-len(accListRsp.List))
	}

	accountExtUidMap := map[uint64]*qrobot.ModelAccount{}
	accountMap := map[uint64]*qrobot.ModelAccount{}
	chatMap := map[string]struct{}{}

	utils.KeyBy(accListRsp.List, "ExtUid", &accountExtUidMap)
	accIdList := utils.PluckUint64(accListRsp.List, "Id")

	// 查询会话信息
	chatListRsp, err := GetChatListSys(ctx, &qmsg.GetChatListSysReq{
		ListOption: core.NewListOption().
			AddOpt(qmsg.GetChatListSysReq_ListOptionChatExtIdList, accIdList).
			AddOpt(qmsg.GetChatListSysReq_ListOptionRobotUidList, uidList).
			AddOpt(qmsg.GetChatListSysReq_ListOptionAssignRspMap, true),
	})

	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	if len(chatListRsp.List) == 0 {
		return &rsp, nil
	}

	for _, chat := range chatListRsp.List {
		key := fmt.Sprintf("%d-%d", chat.RobotUid, chat.ChatExtId)
		chatMap[key] = struct{}{}
	}

	for _, follow := range listRsp.FollowList {
		if account, ok := accountExtUidMap[follow.ExtUid]; ok {
			if _, ok := chatMap[fmt.Sprintf("%d-%d", follow.Uid, account.Id)]; !ok {
				// 创建会话
				chat := &qmsg.ModelChat{}
				chat.RobotUid = follow.Uid
				chat.ChatExtId = account.Id
				chat.IsGroup = false
				chat.CorpId = corpId
				chat.AppId = appId
				err = Chat.Create(ctx, &chat)
				if err != nil {
					log.Errorf("err:%v", err)
					return nil, err
				}
				chatListRsp.List = append(chatListRsp.List, chat)
			}

			accountMap[account.Id] = account
			if account.Profile == nil {
				account.Profile = &qrobot.ModelAccount_Profile{}
			}
			account.Profile.Avatar = follow.Avatar
			account.Profile.Name = follow.Name
			account.Profile.RemarkName = follow.Info.Remark
		}
	}

	rsp.AccountMap = accountMap
	rsp.ChatList = chatListRsp.List
	rsp.AssignChatMap = chatListRsp.AssignChatMap

	return &rsp, nil
}

func SetChatNotReply(ctx *rpc.Context, req *qmsg.SetChatNotReplyReq) (*qmsg.SetChatNotReplyRsp, error) {
	var rsp qmsg.SetChatNotReplyRsp

	user, err := getUserCheck(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	chat, _, err := getChat(ctx, user.CorpId, user.AppId, req.RobotUid, req.ChatExtId, false, true)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	handler := &StatHandler{
		CorpId:            chat.CorpId,
		AppId:             chat.AppId,
		MsgTime:           utils.Now(),
		RobotUid:          chat.RobotUid,
		CustomerAccountId: chat.ChatExtId,
		ChatId:            chat.Id,
	}
	matchStatRule, err := handler.CheckIsMatchStatRule(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	if matchStatRule {
		err = handler.Init(ctx)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
		if handler.GetEnableNoRequiredReply() {
			ok, err := handler.AddNoRequiredReplyIngCache()
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, err
			}
			if ok {
				msg := qmsg.ModelMsgBox{
					CorpId:   user.CorpId,
					AppId:    user.AppId,
					Uid:      chat.RobotUid,
					CliMsgId: "set_chat_not_reply_" + utils.GenRandomStr(),
					MsgType:  uint32(qmsg.MsgType_MsgTypeSetNotReply),
					ChatType: uint32(qmsg.ChatType_ChatTypeSingle),
					ChatId:   chat.ChatExtId,
				}
				chat, lastSeq, err := addMsg(ctx, &msg, true)
				if err != nil {
					log.Errorf("err:%v", err)
					return nil, err
				}
				wrapperList := []*qmsg.WsMsgWrapper{
					{
						MsgType: uint32(qmsg.WsMsgWrapper_MsgTypeSetNotReply),
						Chat:    chat,
						LastSeq: lastSeq,
					},
				}
				err = pushWsMsgByChat(ctx, chat, nil, wrapperList)
				if err != nil {
					log.Errorf("err:%v", err)
					return nil, err
				}
				pubToRobotToCustomerMsgStatMq(ctx, &qmsg.RobotToCustomerMsgStatMqReq{
					CorpId:             chat.CorpId,
					AppId:              chat.AppId,
					RobotUid:           chat.RobotUid,
					ReceiveAccountId:   chat.ChatExtId,
					CustomerServiceUid: user.Id,
					IsSetNotReplyMsg:   true,
				})
			} else {
				log.Warnf("add ing cache fail")
			}
			return &rsp, nil
		} else {
			log.Warnf("not found no required reply cache")
		}
	} else {
		log.Warnf("not match stat rule")
	}
	return nil, rpc.CreateError(qmsg.ErrSingleChatHasReplied)
}

func Audio2Text(ctx *rpc.Context, req *qmsg.Audio2TextReq) (*qmsg.Audio2TextRsp, error) {
	var rsp qmsg.Audio2TextRsp

	corpId, appId := core.GetPinHeaderCorpIdAndAppId2Int(ctx)
	user, err := getUserCheck(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	if req.AssignChatId == 0 {
		//拉下我有没有这个平台号的管理权限
		//err = ensureHasRobotPerm(ctx, user, req.RobotUid)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
	} else {
		assignChat, err := AssignChat.get(ctx, corpId, appId, req.AssignChatId)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}

		if assignChat.Uid != user.Id {
			return nil, rpc.CreateError(qmsg.ErrNotHasRobotPerm)
		}
	}

	db := MsgBox.WhereCorpApp(corpId, appId).
		Where(DbUid, req.RobotUid).
		Where(DbChatType, req.ChatType).
		Where(DbChatId, req.ChatId).Where(DbCliMsgId, req.MsgId)

	var msgDetail qmsg.ModelMsgBox
	err = ChoiceMsgBoxDb(db, corpId).First(ctx, &msgDetail)
	if err != nil {
		log.Errorf("err:%v", err)
		if MsgBox.IsNotFoundErr(err) {
			return nil, rpc.CreateError(qmsg.ErrMsgBoxNotFound)
		}
		return nil, err
	}

	if msgDetail.ChatMsgType != uint32(qmsg.ChatMsgType_ChatMsgTypeVoice) {
		return nil, rpc.InvalidArg("非语音消息，不支持转换")
	}

	var voiceMsg qmsg.ChatVoiceMsg
	err = json.Unmarshal([]byte(msgDetail.Msg), &voiceMsg)
	if err != nil {
		log.Errorf("err %v", err)
		return nil, err
	}

	if voiceMsg.Text != "" {
		rsp.Text = voiceMsg.Text
		return &rsp, nil
	}

	if msgDetail.CreatedAt+24*3600 < utils.Now() {
		return nil, rpc.InvalidArg("转文字失败，仅支持24小时以内的语音消息")
	}

	lRsp, err := qadapter.ListChannelSys(ctx, &qadapter.ListChannelSysReq{
		CorpIdList: []uint32{user.CorpId},
		RobotType:  uint32(qadapter.RobotType_CodeScan),
	})
	if err != nil {
		log.Errorf("err %v", err)
		return nil, err
	}

	if val, ok := lRsp.ChannelMap[user.CorpId]; !ok {
		return nil, rpc.InvalidArg("暂不支持")
	} else {
		switch val {
		case uint32(qadapter.ChannelType_Weg):
			// 支持
		case uint32(qadapter.ChannelType_Bns):
			// 系统后台自动转
			return nil, rpc.InvalidArg("转换中，请稍后再试")
		default:
			return nil, rpc.InvalidArg("暂不支持")
		}
	}

	merchantId, err := qrt.GetMerchantId(ctx)
	if err != nil {
		log.Errorf("err %v", err)
		return nil, err
	}

	robot, err := getRobotByUid(ctx, user.CorpId, user.AppId, req.RobotUid)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	ycRsp, err := yc.Audio2Text(ctx, merchantId, &yc.Audio2TextReq{
		MerchantNo:    merchantId,
		RobotSerialNo: robot.YcSerialNo,
		MsgId:         req.MsgId,
	}, &yc.BizContext{
		CorpId:  user.CorpId,
		AppId:   user.AppId,
		Module:  "qmsg",
		Context: "audio2text",
	})
	if err != nil {
		log.Errorf("err %v", err)
		return nil, rpc.InvalidArg("转换失败")
	}

	if !ycRsp.Success || ycRsp.Data == nil {
		log.Errorf("err:%s", ycRsp.Message)
		return nil, rpc.InvalidArg("转换失败")
	}

	rsp.Text = ycRsp.Data.MsgContent

	voiceMsg.Text = ycRsp.Data.MsgContent

	tmpByte, err := json.Marshal(voiceMsg)
	if err != nil {
		log.Errorf("err %v", err)
		return nil, err
	}

	log.Debugf("msg text %s", string(tmpByte))

	_, err = ChoiceMsgBoxDb(MsgBox.WhereCorpApp(corpId, appId).Where(DbId, msgDetail.Id), corpId).Update(ctx, map[string]interface{}{
		DbMsg: string(tmpByte),
	})
	if err != nil {
		log.Errorf("err %v", err)
	}

	return &rsp, nil
}

func autoAudio2TextCacheKey(relaSn string) string {
	return fmt.Sprintf("qmsg_audio2text_%s", relaSn)
}

func Audio2TextCbMq(ctx *rpc.Context, req *smq.ConsumeReq) (*smq.ConsumeRsp, error) {
	process := func(ctx *rpc.Context, mqReq *smq.ConsumeReq, data interface{}) (*smq.ConsumeRsp, error) {
		var rsp smq.ConsumeRsp
		d := data.(*yc.Audio2TextCbReq)
		log.Debugf("audio2text q_msg %v", d)
		if d.Success == false || d.Data == nil {
			return &rsp, nil
		}

		var key string
		//if d.RelaSerialNo != "" {
		//	// 通过主动调用语音转文本的
		//	key = d.RelaSerialNo
		//} else if d.Data != nil && d.Data.MsgId != "" {
		// 通过开启企微自动语音转文字，直接回调的
		key = fmt.Sprintf("%s_%s", d.RobotSerialNo, d.Data.MsgId)
		//}
		b, err := s.RedisGroup.Get(autoAudio2TextCacheKey(key))
		if err != nil {
			if err == redis.Nil {
				// 语音可能比消息先到
				rsp.Retry = true
				return &rsp, nil
			}
			log.Errorf("err %v", err)
			return &rsp, err
		}
		boxId, err := strconv.ParseUint(string(b), 10, 64)
		if err != nil {
			log.Errorf("err %v", err)
			return &rsp, err
		}
		if boxId == 0 {
			log.Errorf("not found msg box id")
			return &rsp, nil
		}

		qRsp, err := qris.GetRobotItemBySn(ctx, &qris.GetRobotItemBySnReq{
			RobotSn: d.RobotSerialNo,
		})
		if err != nil {
			log.Errorf("err %v", err)
			return &rsp, err
		}

		if qRsp.RobotItem == nil {
			return &rsp, nil
		}

		robot := qRsp.RobotItem

		db := MsgBox.WhereCorpApp(robot.CorpId, robot.AppId).
			Where(DbId, boxId).Where(DbUid, robot.Uid).Where(DbChatMsgType, uint32(qmsg.ChatMsgType_ChatMsgTypeVoice))

		var msgDetail qmsg.ModelMsgBox
		err = ChoiceMsgBoxDb(db, robot.CorpId).First(ctx, &msgDetail)
		if err != nil {
			log.Errorf("err:%v", err)
			if MsgBox.IsNotFoundErr(err) {
				return &rsp, nil
			}
			return &rsp, err
		}

		var voiceMsg qmsg.ChatVoiceMsg

		err = json.Unmarshal([]byte(msgDetail.Msg), &voiceMsg)
		if err != nil {
			log.Errorf("err %v", err)
			return &rsp, err
		}

		if voiceMsg.Text != "" {

			log.Debugf("已经转换过了，不推了 %s:", voiceMsg.Text)
			return &rsp, nil
		}

		voiceMsg.Text = d.Data.MsgContent
		tmpByte, err := json.Marshal(voiceMsg)
		if err != nil {
			log.Errorf("err %v", err)
			return &rsp, err
		}

		log.Debugf("msg text %s", string(tmpByte))

		_, err = ChoiceMsgBoxDb(MsgBox.WhereCorpApp(robot.CorpId, robot.AppId).Where(DbId, msgDetail.Id), robot.CorpId).Update(ctx, map[string]interface{}{
			DbMsg: string(tmpByte),
		})
		if err != nil {
			log.Errorf("err %v", err)
		}

		msgDetail.Msg = string(tmpByte)

		var chat *qmsg.ModelChat
		var tmp qmsg.ModelChat
		err = Chat.WhereCorpApp(robot.CorpId, robot.AppId).Where(map[string]interface{}{
			DbRobotUid:  robot.Uid,
			DbIsGroup:   msgDetail.ChatType == uint32(qmsg.ChatType_ChatTypeGroup),
			DbChatExtId: msgDetail.ChatId,
		}).First(ctx, &tmp)
		if err != nil && !Chat.IsNotFoundErr(err) {

			log.Errorf("err:%v", err)
			return &rsp, nil
		}

		if err == nil {
			chat = &tmp
		}

		// 非智能助手处理
		wrapperList := []*qmsg.WsMsgWrapper{
			{
				MsgType: uint32(qmsg.WsMsgWrapper_MsgTypeVoiceHaveText),
				MsgBox:  &msgDetail,
				Chat:    chat,
				MsgId:   msgDetail.CliMsgId,
			},
		}

		//里面会查assignChat
		err = pushWsMsgByChatV2(ctx, chat, nil, wrapperList)
		if err != nil {
			log.Errorf("err %v", err)
		}

		return &rsp, nil
	}
	return MqAudio2Text.ProcessV2(ctx, req, process)
}

func GetLastChatList(ctx *rpc.Context, req *qmsg.GetLastChatListReq) (*qmsg.GetLastChatListRsp, error) {
	var rsp qmsg.GetLastChatListRsp
	//rsp.AccountMap = make(map[uint64]*qrobot.ModelAccount)
	//rsp.GroupMap = make(map[uint64]*qrobot.ModelGroupChat)

	u, err := getUserCheck(ctx)
	if err != nil {
		log.Errorf("err:%v", err)

	}

	if !u.IsAssign {
		log.Errorf("[%d]该账号为非分配资产", u.Id)
		return nil, rpc.CreateError(int32(qmsg.ErrCode_ErrUserAssignType))
	}

	db := LastChat.NewList(req.ListOption).WhereCorpApp(u.CorpId, u.AppId)

	//只读取最近30天
	db.Gte(DbLastTimeAt, utils.Now()-30*24*3600)
	db.Where(DbUid, u.Id)

	if req.LastChatType != 0 {
		db.Where(DbLastChatType, req.LastChatType)
	}

	err = core.NewListOptionProcessor(req.ListOption).
		AddUint32(qmsg.GetLastChatListReq_ListOptionOrderBy, func(val uint32) error {
			if val == uint32(qmsg.GetLastChatListReq_OrderByLastTimeAtDesc) {
				db.OrderDesc(DbLastTimeAt)
			}

			if val == uint32(qmsg.GetLastChatListReq_OrderByLastTimeAtAsc) {
				db.OrderAsc(DbLastTimeAt)
			}
			return nil
		}).Process()
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	var chatIdList pie.Uint64s

	var listChat []*qmsg.ModelLastChat
	rsp.Paginate, err = db.FindPaginate(ctx, &listChat)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	chatIdList = utils.PluckUint64(listChat, "Cid")

	if len(chatIdList) == 0 {
		return &rsp, nil
	}

	var list []*qmsg.ModelChat
	err = Chat.WhereCorpApp(u.CorpId, u.AppId).WhereIn(DbId, chatIdList).Find(ctx, &list)
	if err != nil {
		log.Errorf("err %v", err)
		return nil, err
	}

	rsp.List = list

	//var accountIdList []uint64
	//var groupIdList []uint64
	//for _, v := range list {
	//	if v.IsGroup {
	//		groupIdList = append(groupIdList, v.ChatExtId)
	//	} else {
	//		accountIdList = append(accountIdList, v.ChatExtId)
	//	}
	//}
	//
	//if len(accountIdList) > 0 {
	//
	//	listRsp, err := qrobot.GetAccountListSys(ctx, &qrobot.GetAccountListSysReq{
	//		ListOption: core.NewListOption().SetSkipCount().AddOpt(qrobot.GetAccountListSysReq_ListOptionIdList, accountIdList),
	//		CorpId:     u.CorpId,
	//		AppId:      u.AppId,
	//	})
	//	if err != nil {
	//		log.Errorf("err:%v", err)
	//		return nil, err
	//	}
	//
	//	for _, v := range listRsp.List {
	//		rsp.AccountMap[v.Id] = v
	//	}
	//
	//}
	//
	//if len(groupIdList) > 0 {
	//
	//	groupListRsp, err := qrobot.GetGroupChatListSys(ctx, &qrobot.GetGroupChatListSysReq{
	//		ListOption: core.NewListOption().AddOpt(qrobot.GetGroupChatListSysReq_ListOptionGroupIdList, groupIdList),
	//		CorpId:     u.CorpId,
	//		AppId:      u.AppId,
	//		Unscoped:   true,
	//	})
	//	if err != nil {
	//		log.Errorf("err:%v", err)
	//		return nil, err
	//	}
	//
	//	for _, v := range groupListRsp.List {
	//		rsp.GroupMap[v.Id] = v
	//	}
	//
	//}

	return &rsp, nil
}
