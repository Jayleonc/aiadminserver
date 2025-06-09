package impl

import (
	"git.pinquest.cn/base/aiadmin"
	"time"

	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/brick/smq"
	"git.pinquest.cn/qlb/brick/utils"
	"git.pinquest.cn/qlb/core"
	"git.pinquest.cn/qlb/iquan"
	"git.pinquest.cn/qlb/qmsg"
	"git.pinquest.cn/qlb/qrobot"
	"git.pinquest.cn/qlb/quan"
	"git.pinquest.cn/qlb/qwhale"
	"git.pinquest.cn/qlb/yc"
)

var MqAddFriendInGroup = smq.NewJsonMsg("qmsg_add_friend_group", &qmsg.AddFriendInGroupReqMq{})
var MqAddFriendByPhone = smq.NewJsonMsg("qmsg_add_friend_phone", &qmsg.AddFriendByPhoneReqMq{})
var MqSyncSingleChatStat = smq.NewJsonMsg("qmsg_sync_single_chat_stat", &qmsg.SyncSingleChatStatMessageMqReq{})
var MqMsgBoxStat = smq.NewJsonMsg("qmsg_msg_box_stat", &qmsg.MsgBoxStatMqReq{})
var MqRobotToCustomerMsgStat = smq.NewJsonMsg("qmsg_robot_to_customer_msg_stat", &qmsg.RobotToCustomerMsgStatMqReq{})
var MqAfterAssignChatChanged = smq.NewJsonMsg("qmsg_after_assign_chat_changed", &qmsg.AfterAssignChatChangedMqReq{})
var MqRecallMessage = smq.NewJsonMsg("yc_recall_cb", &yc.RecalledMsgRsp{})
var MqInviteJoinGroup = smq.NewJsonMsg("yc_receive_join_group", &yc.ReceiveInviterJoinGroupReq{})
var MqOneSideFriend = smq.NewJsonMsg("yc_side_friend_cb", &yc.BeOneSideFriendReq{})
var MqBecomeFriend = smq.NewJsonMsg("yc_become_friend", &yc.BecomeFriendReq{})
var MqMsgAiEntertain = smq.NewJsonMsg("qmsg_msg_ai_entertain", &qmsg.MsgAiEntertainMqData{})
var MqAudio2Text = smq.NewJsonMsg("audio_2_text_cb", &yc.Audio2TextCbReq{})
var MqSingleRecvChatSlow = smq.NewJsonMsg("single_recv_chat_slow", &qwhale.RecvSingleChatMsgReq{})
var MqdeleteChatMsg = smq.NewJsonMsg("qmsg_delete_chat_msg", &qmsg.DeleteChatMsgReq{})

// 2. 声明一个新的MQ Topic
var MqAIWorkflowTrigger = smq.NewJsonMsg("ai_workflow_trigger", &aiadmin.AIWorkflowMsqTask{})

// MqAIWorkflowBufferEvent 定义了用于接收需要缓冲的原始AI工作流事件的MQ实例
var MqAIWorkflowBufferEvent = smq.NewJsonMsg("ai_workflow_buffer_event_v1", &aiadmin.AIWorkflowTriggerEventWithHistory{})

func initMq() error {
	// 1. 向SMQ系统注册我们的Topic及其消费者配置
	_, err := smq.AddTopic(nil, &smq.AddTopicReq{
		Topic: &smq.Topic{
			Name: MqAIWorkflowTrigger.TopicName,
			SubConfig: &smq.SubConfig{
				ConcurrentCount:    50,
				MaxRetryCount:      5,
				MaxExecTimeSeconds: 1800,
				ServiceName:        aiadmin.ServiceName,
				ServicePath:        aiadmin.HandleAIWorkflowTaskCMDPath,
			},
		},
	})
	if err != nil {
		log.Fatalf("向SMQ注册MQ Topic (%s) 失败: %v", MqAIWorkflowTrigger.TopicName, err)
		return err
	}

	// --------------------- 消息类 ------------------------------
	// 发消息
	_, err = smq.AddTopic(nil, &smq.AddTopicReq{
		Topic: &smq.Topic{
			Name: qmsg.MqSendChatMsg.TopicName,
			SubConfig: &smq.SubConfig{
				ConcurrentCount:    200,
				MaxRetryCount:      5,
				MaxExecTimeSeconds: 1800,
				ServiceName:        qmsg.ServiceName,
				ServicePath:        qmsg.SendChatMsgMqCMDPath,
			},
		},
	})
	if err != nil {
		log.Errorf("err:%v", err)
	}

	_, err = smq.AddChannel(nil, &smq.AddChannelReq{
		TopicName: MqAudio2Text.TopicName,
		Channel: &smq.Channel{
			Name: MqAudio2Text.TopicName + "_channel",
			SubConfig: &smq.SubConfig{
				ConcurrentCount:    10,
				MaxRetryCount:      5,
				MaxExecTimeSeconds: 20,
				ServiceName:        qmsg.ServiceName,
				ServicePath:        qmsg.Audio2TextCbMqCMDPath,
			},
		},
	})
	if err != nil {
		log.Errorf("err:%v", err)
	}

	// 圈客宝收消息
	_, err = smq.AddChannel(nil, &smq.AddChannelReq{
		TopicName: qrobot.MqRecvChat.TopicName,
		Channel: &smq.Channel{
			Name: "qmsg_recv_msg",
			SubConfig: &smq.SubConfig{
				ConcurrentCount:    100,
				MaxRetryCount:      5,
				MaxExecTimeSeconds: 300,
				ServiceName:        qmsg.ServiceName,
				ServicePath:        qmsg.RecvYcChatMsgMqCMDPath,
				BarrierMode:        true,
			},
		},
	})
	if err != nil {
		log.Errorf("err:%v", err)
	}

	// 企客宝收群聊消息
	_, err = smq.AddChannel(nil, &smq.AddChannelReq{
		TopicName: qwhale.MqGroupRecvChat.TopicName,
		Channel: &smq.Channel{
			Name: "qmsg_recv_group_msg",
			SubConfig: &smq.SubConfig{
				ConcurrentCount:    100,
				MaxRetryCount:      5,
				MaxExecTimeSeconds: 300,
				ServiceName:        qmsg.ServiceName,
				ServicePath:        qmsg.RecvGroupChatMqCMDPath,
				BarrierMode:        true,
			},
		},
	})
	if err != nil {
		log.Errorf("err:%v", err)
	}

	// 企客宝收私聊消息
	_, err = smq.AddChannel(nil, &smq.AddChannelReq{
		TopicName: qwhale.MqSingleRecvChat.TopicName,
		Channel: &smq.Channel{
			Name: "qmsg_recv_single_msg",
			SubConfig: &smq.SubConfig{
				ConcurrentCount:    300,
				MaxRetryCount:      5,
				MaxExecTimeSeconds: 300,
				ServiceName:        qmsg.ServiceName,
				ServicePath:        qmsg.RecvSingleChatMqCMDPath,
				BarrierMode:        true,
			},
		},
	})
	if err != nil {
		log.Errorf("err:%v", err)
	}

	_, err = smq.AddTopic(nil, &smq.AddTopicReq{
		Topic: &smq.Topic{
			Name: MqSingleRecvChatSlow.TopicName,
			SubConfig: &smq.SubConfig{
				ConcurrentCount:    5,
				MaxRetryCount:      0,
				MaxExecTimeSeconds: 300,
				ServiceName:        qmsg.ServiceName,
				ServicePath:        qmsg.SlowRecvSingleChatMqCMDPath,
				BarrierMode:        true,
			},
		},
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	// --------------------- 成员客户信息变动类 ------------------------------
	// 成员删除
	_, err = smq.AddChannel(nil, &smq.AddChannelReq{
		TopicName: quan.MqUserDeleted.TopicName,
		Channel: &smq.Channel{
			Name: "qmsg_user_deleted",
			SubConfig: &smq.SubConfig{
				ConcurrentCount:    50,
				MaxRetryCount:      5,
				MaxExecTimeSeconds: 10,
				ServiceName:        qmsg.ServiceName,
				ServicePath:        qmsg.RecvUserDeleteMsgMqCMDPath,
				BarrierMode:        true,
			},
		},
	})
	if err != nil {
		log.Errorf("err:%v", err)
	}

	// 被客户删除
	_, err = smq.AddChannel(nil, &smq.AddChannelReq{
		TopicName: quan.MqExtContactDelete.TopicName,
		Channel: &smq.Channel{
			Name: "qmsg_ext_contact_delete",
			SubConfig: &smq.SubConfig{
				ConcurrentCount:    50,
				MaxRetryCount:      5,
				MaxExecTimeSeconds: 10,
				ServiceName:        qmsg.ServiceName,
				ServicePath:        qmsg.ExtContactDeleteMqCMDPath,
			},
		},
	})
	if err != nil {
		log.Errorf("err:%v", err)
	}

	// 客户信息变动
	_, err = smq.AddChannel(nil, &smq.AddChannelReq{
		TopicName: quan.MqExtContactUpdate.TopicName,
		Channel: &smq.Channel{
			Name: "qmsg_ext_contact_update",
			SubConfig: &smq.SubConfig{
				ConcurrentCount:    3,
				MaxRetryCount:      5,
				MaxExecTimeSeconds: 10,
				ServiceName:        qmsg.ServiceName,
				ServicePath:        qmsg.ExtContactUpdateMqCMDPath,
			},
		},
	})
	if err != nil {
		log.Errorf("err:%v", err)
	}

	// 添加客户
	_, err = smq.AddChannel(nil, &smq.AddChannelReq{
		TopicName: quan.MqExtContactAdd.TopicName,
		Channel: &smq.Channel{
			Name: "qmsg_ext_contact_add",
			SubConfig: &smq.SubConfig{
				ConcurrentCount:    50,
				MaxRetryCount:      5,
				MaxExecTimeSeconds: 10,
				ServiceName:        qmsg.ServiceName,
				ServicePath:        qmsg.ExtContactUpdateMqCMDPath,
			},
		},
	})
	if err != nil {
		log.Errorf("err:%v", err)
	}

	// 成员信息变动
	_, err = smq.AddChannel(nil, &smq.AddChannelReq{
		TopicName: quan.MqUserUpdated.TopicName,
		Channel: &smq.Channel{
			Name: "qmsg_user_update",
			SubConfig: &smq.SubConfig{
				ConcurrentCount:    50,
				MaxRetryCount:      5,
				MaxExecTimeSeconds: 10,
				ServiceName:        qmsg.ServiceName,
				ServicePath:        qmsg.ExtContactUpdateMqCMDPath,
			},
		},
	})
	if err != nil {
		log.Errorf("err:%v", err)
	}

	// 单边好友
	_, err = smq.AddChannel(nil, &smq.AddChannelReq{
		TopicName: MqOneSideFriend.TopicName,
		Channel: &smq.Channel{
			Name: "qmsg_one_side_friend",
			SubConfig: &smq.SubConfig{
				ConcurrentCount:    50,
				MaxRetryCount:      5,
				MaxExecTimeSeconds: 10,
				ServiceName:        qmsg.ServiceName,
				ServicePath:        qmsg.OneSideFriendMqCMDPath,
			},
		},
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	// 成为好友
	_, err = smq.AddChannel(nil, &smq.AddChannelReq{
		TopicName: MqBecomeFriend.TopicName,
		Channel: &smq.Channel{
			Name: "qmsg_become_friend",
			SubConfig: &smq.SubConfig{
				ConcurrentCount:    50,
				MaxRetryCount:      5,
				MaxExecTimeSeconds: 5,
				ServiceName:        qmsg.ServiceName,
				ServicePath:        qmsg.BecomeFriendMqCMDPath,
			},
		},
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	// --------------------- 行为回调 ------------------------------
	// 群内加好友
	_, err = smq.AddTopic(nil, &smq.AddTopicReq{
		Topic: &smq.Topic{
			Name: MqAddFriendInGroup.TopicName,
			SubConfig: &smq.SubConfig{
				ConcurrentCount:    50,
				MaxRetryCount:      5,
				MaxExecTimeSeconds: 1800,
				ServiceName:        qmsg.ServiceName,
				ServicePath:        qmsg.AddFriendInGroupMqCMDPath,
			},
		},
	})
	if err != nil {
		log.Errorf("err:%v", err)
	}

	// 手机号加好友
	_, err = smq.AddTopic(nil, &smq.AddTopicReq{
		Topic: &smq.Topic{
			Name: MqAddFriendByPhone.TopicName,
			SubConfig: &smq.SubConfig{
				ConcurrentCount:    50,
				MaxRetryCount:      5,
				MaxExecTimeSeconds: 1800,
				ServiceName:        qmsg.ServiceName,
				ServicePath:        qmsg.AddFriendByPhoneMqCMDPath,
			},
		},
	})
	if err != nil {
		log.Errorf("err:%v", err)
	}

	// 定时任务统计mq消费
	_, err = smq.AddTopic(nil, &smq.AddTopicReq{
		Topic: &smq.Topic{
			Name: MqSyncSingleChatStat.TopicName,
			SubConfig: &smq.SubConfig{
				ConcurrentCount:    100,
				MaxRetryCount:      5,
				MaxExecTimeSeconds: 300,
				ServiceName:        qmsg.ServiceName,
				ServicePath:        qmsg.HandleSyncSingleChatStatMessageMqCMDPath,
				BarrierMode:        true,
			},
		},
	})
	if err != nil {
		log.Errorf("err:%v", err)
	}

	// 接收到客户消息统计
	_, err = smq.AddTopic(nil, &smq.AddTopicReq{
		Topic: &smq.Topic{
			Name: MqMsgBoxStat.TopicName,
			SubConfig: &smq.SubConfig{
				ConcurrentCount:    100,
				MaxRetryCount:      5,
				MaxExecTimeSeconds: 300,
				ServiceName:        qmsg.ServiceName,
				ServicePath:        qmsg.HandleMsgBoxStatMqCMDPath,
				BarrierMode:        true,
			},
		},
	})
	if err != nil {
		log.Errorf("err:%v", err)
	}

	// 客服发给客户消息统计
	_, err = smq.AddTopic(nil, &smq.AddTopicReq{
		Topic: &smq.Topic{
			Name: MqRobotToCustomerMsgStat.TopicName,
			SubConfig: &smq.SubConfig{
				ConcurrentCount:    100,
				MaxRetryCount:      5,
				MaxExecTimeSeconds: 300,
				ServiceName:        qmsg.ServiceName,
				ServicePath:        qmsg.HandleRobotToCustomerMsgStatMqCMDPath,
				BarrierMode:        true,
			},
		},
	})
	if err != nil {
		log.Errorf("err:%v", err)
	}

	// 客服发给客户消息统计
	_, err = smq.AddTopic(nil, &smq.AddTopicReq{
		Topic: &smq.Topic{
			Name: MqAfterAssignChatChanged.TopicName,
			SubConfig: &smq.SubConfig{
				ConcurrentCount:    100,
				MaxRetryCount:      5,
				MaxExecTimeSeconds: 300,
				ServiceName:        qmsg.ServiceName,
				ServicePath:        qmsg.AfterAssignChatChangedMqCMDPath,
				BarrierMode:        true,
			},
		},
	})
	if err != nil {
		log.Errorf("err:%v", err)
	}

	// 会话结束回调
	_, err = smq.AddTopic(nil, &smq.AddTopicReq{
		Topic: &smq.Topic{
			Name: qmsg.MqAssignChatChange.TopicName,
			SubConfig: &smq.SubConfig{
				ConcurrentCount:    100,
				MaxRetryCount:      5,
				MaxExecTimeSeconds: 300,
				ServiceName:        qmsg.ServiceName,
				ServicePath:        qmsg.AssignChatChangeMqCMDPath,
				BarrierMode:        true,
			},
		},
	})
	if err != nil {
		log.Errorf("err:%v", err)
	}

	// 撤回的回调结果
	_, err = smq.AddChannel(nil, &smq.AddChannelReq{
		TopicName: MqRecallMessage.TopicName,
		Channel: &smq.Channel{
			Name: MqRecallMessage.TopicName,
			SubConfig: &smq.SubConfig{
				ConcurrentCount:    100,
				MaxRetryCount:      5,
				MaxExecTimeSeconds: 10,
				ServiceName:        qmsg.ServiceName,
				ServicePath:        qmsg.RecallMsgCallbackMqCMDPath,
			},
		},
	})
	if err != nil {
		log.Errorf("err:%v", err)
	}

	// 扫码号注销
	_, err = smq.AddChannel(nil, &smq.AddChannelReq{
		TopicName: qmsg.MqUnbindHostAccountMsg.TopicName,
		Channel: &smq.Channel{
			Name: "qmsg_host_account_unbind",
			SubConfig: &smq.SubConfig{
				ConcurrentCount:    300,
				MaxRetryCount:      5,
				MaxExecTimeSeconds: 300,
				ServiceName:        qmsg.ServiceName,
				ServicePath:        qmsg.UnbindHostAccountMqCMDPath,
				BarrierMode:        true,
			},
		},
	})
	if err != nil {
		log.Errorf("err:%v", err)
	}

	// 清理机器人会话队列
	_, err = smq.AddTopic(nil, &smq.AddTopicReq{
		Topic: &smq.Topic{
			Name: MqdeleteChatMsg.TopicName,
			SubConfig: &smq.SubConfig{
				ConcurrentCount:    5,
				MaxRetryCount:      3,
				MaxExecTimeSeconds: 1800,
				ServiceName:        qmsg.ServiceName,
				ServicePath:        qmsg.DeleteChatMsgMqCMDPath,
				RetryIntervalMin:   2,
				RetryIntervalMax:   15,
				RetryIntervalStep:  2,
				BarrierMode:        true,
			},
		},
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	return nil
}

func ExtContactRemarkUpdateMq(ctx *rpc.Context, req *smq.ConsumeReq) (*smq.ConsumeRsp, error) {
	return quan.MqExtContactRemarkUpdated.ProcessV2(ctx, req, func(ctx *rpc.Context, req *smq.ConsumeReq, data interface{}) (*smq.ConsumeRsp, error) {
		var rsp smq.ConsumeRsp

		remarkData := data.(*quan.ExtContactRemarkUpdatedMqMsg)

		followRsp, err := iquan.GetExtContactFollow(ctx, &iquan.GetExtContactFollowReq{
			CorpId: req.CorpId,
			AppId:  req.AppId,
			ExtUid: remarkData.ExtUid,
			Uid:    remarkData.UserId,
		})
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}

		follow := followRsp.Follow

		listRsp, err := qrobot.GetAccountListSys(ctx, &qrobot.GetAccountListSysReq{
			CorpId: req.CorpId,
			AppId:  req.AppId,
			ListOption: core.NewListOption().
				AddOpt(qrobot.GetAccountListSysReq_ListOptionExtUidList, []uint64{remarkData.ExtUid}).
				SetLimit(1).SetSkipCount(),
		})
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}

		if len(listRsp.List) == 0 {
			return &rsp, nil
		}

		account := listRsp.List[0]

		msg := qmsg.ModelMsgBox{
			CorpId:   req.CorpId,
			AppId:    req.AppId,
			Uid:      follow.Uid,
			CliMsgId: "quan_ext_contact_remark_update_" + utils.GenRandomStr(),
			MsgType:  uint32(qmsg.MsgType_MsgTypeExtContactProfileChanged),
			ChatType: uint32(qmsg.ChatType_ChatTypeSingle),
			ChatId:   account.Id,
		}
		chat, lastSeq, err := addMsg(ctx, &msg, false)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}

		_, err = Chat.updateFromFollow(ctx, req.CorpId, req.AppId, chat, follow)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}

		err = pushWsMsgByRobotUid(ctx, req.CorpId, req.AppId, remarkData.UserId, []*qmsg.WsMsgWrapper{
			{
				MsgType: uint32(qmsg.WsMsgWrapper_MsgTypeExtContactChange),
				Chat:    chat,
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
		})
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}

		return &rsp, nil
	})
}

func ExtContactUpdateMq(ctx *rpc.Context, req *smq.ConsumeReq) (*smq.ConsumeRsp, error) {
	return quan.MqExtContactUpdate.ProcessV2(ctx, req, func(ctx *rpc.Context, req *smq.ConsumeReq, data interface{}) (*smq.ConsumeRsp, error) {
		var rsp smq.ConsumeRsp

		updateData := data.(*quan.ExtContactUpdateMqMsg)

		follow := updateData.Follow

		listRsp, err := qrobot.GetAccountListSys(ctx, &qrobot.GetAccountListSysReq{
			CorpId: req.CorpId,
			AppId:  req.AppId,
			ListOption: core.NewListOption().
				AddOpt(qrobot.GetAccountListSysReq_ListOptionExtUidList, []uint64{follow.ExtUid}).
				SetLimit(1).SetSkipCount(),
		})
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}

		if len(listRsp.List) == 0 {
			return &rsp, nil
		}

		account := listRsp.List[0]

		msg := qmsg.ModelMsgBox{
			CorpId:   req.CorpId,
			AppId:    req.AppId,
			Uid:      follow.Uid,
			CliMsgId: "quan_ext_contact_update_" + utils.GenRandomStr(),
			MsgType:  uint32(qmsg.MsgType_MsgTypeExtContactProfileChanged),
			ChatType: uint32(qmsg.ChatType_ChatTypeSingle),
			ChatId:   account.Id,
		}
		chat, lastSeq, err := addMsg(ctx, &msg, false)
		if err != nil {
			log.Errorf("err:%v", err)

			if rpc.GetErrCode(err) < 0 && req.CreatedAt+300 > utils.Now() {
				rsp.Retry = true
				rsp.SkipIncRetryCount = true
			}

			return &rsp, err
		}

		_, err = Chat.updateFromFollow(ctx, req.CorpId, req.AppId, chat, follow)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}

		err = pushWsMsgByRobotUid(ctx, req.CorpId, req.AppId, follow.Uid, []*qmsg.WsMsgWrapper{
			{
				MsgType: uint32(qmsg.WsMsgWrapper_MsgTypeExtContactChange),
				Chat:    chat,
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
		})
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}

		return &rsp, nil
	})
}

func UserUpdateMq(ctx *rpc.Context, req *smq.ConsumeReq) (*smq.ConsumeRsp, error) {
	return quan.MqExtContactUpdate.ProcessV2(ctx, req, func(ctx *rpc.Context, req *smq.ConsumeReq, data interface{}) (*smq.ConsumeRsp, error) {
		var rsp smq.ConsumeRsp

		updateData := data.(*quan.UserUpdatedMqMsg)

		if updateData.UpdateSource == uint32(quan.UserUpdatedMqMsg_UpdateSourceWeworkAuthApp) {
			return &rsp, nil
		}

		list, err := UserRobot.getListByRobotUid(ctx, req.CorpId, req.AppId, updateData.User.Id)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}

		if len(list) == 0 {
			return &rsp, nil
		}

		msg := qmsg.ModelMsgBox{
			CorpId:   req.CorpId,
			AppId:    req.AppId,
			Uid:      updateData.User.Id,
			CliMsgId: "quan_user_update_" + utils.GenRandomStr(),
			MsgType:  uint32(qmsg.MsgType_MsgTypeRobotProfileChanged),
			ChatType: uint32(qmsg.ChatType_ChatTypeSingle),
		}
		_, _, err = addMsg(ctx, &msg, false)
		if err != nil {
			log.Errorf("err:%v", err)

			if rpc.GetErrCode(err) < 0 && req.CreatedAt+300 > utils.Now() {
				rsp.Retry = true
				rsp.SkipIncRetryCount = true
			}

			return &rsp, err
		}

		for _, userRobot := range list {
			err = pushWsMsg(ctx, req.CorpId, req.AppId, userRobot.Uid, []*qmsg.WsMsgWrapper{
				{
					MsgType:   uint32(qmsg.WsMsgWrapper_MsgTypeRobotChange),
					RobotUser: updateData.User,
				},
				// 兼容 1.0 ws ，日后废弃
				{
					MsgType: uint32(qmsg.WsMsgWrapper_MsgTypeMsgSeqChanged),
					MsgSeqChanged: &qmsg.WsMsgWrapper_MsgSeqChanged{
						RobotUid:     msg.Uid,
						RemoteMsgSeq: msg.MsgSeq,
					},
				},
			})
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, err
			}
		}

		return &rsp, nil
	})
}

func ExtContactAddMq(ctx *rpc.Context, req *smq.ConsumeReq) (*smq.ConsumeRsp, error) {
	return quan.MqExtContactAdd.ProcessV2(ctx, req, func(ctx *rpc.Context, req *smq.ConsumeReq, data interface{}) (*smq.ConsumeRsp, error) {
		var rsp smq.ConsumeRsp

		dataMsg := data.(*quan.ExtContactAddMqMsg)

		list, err := UserRobot.getListByRobotUid(ctx, req.CorpId, req.AppId, dataMsg.Follow.Uid)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}

		if len(list) == 0 {
			return &rsp, nil
		}

		listRsp, err := qrobot.GetAccountListSys(ctx, &qrobot.GetAccountListSysReq{
			CorpId: req.CorpId,
			AppId:  req.AppId,
			ListOption: core.NewListOption().
				AddOpt(qrobot.GetAccountListSysReq_ListOptionExtUidList, []uint64{dataMsg.Follow.ExtUid}).
				SetLimit(1).SetSkipCount(),
		})
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}

		if len(listRsp.List) == 0 {
			return &rsp, nil
		}

		account := listRsp.List[0]

		msg := qmsg.ModelMsgBox{
			CorpId:   req.CorpId,
			AppId:    req.AppId,
			Uid:      dataMsg.Follow.Uid,
			CliMsgId: "quan_ext_contact_add_" + utils.GenRandomStr(),
			MsgType:  uint32(qmsg.MsgType_MsgTypeExtContactProfileChanged),
			ChatType: uint32(qmsg.ChatType_ChatTypeSingle),
			ChatId:   account.Id,
		}
		chat, lastSeq, err := addMsg(ctx, &msg, false)
		if err != nil {
			log.Errorf("err:%v", err)

			if rpc.GetErrCode(err) < 0 && req.CreatedAt+300 > utils.Now() {
				rsp.Retry = true
				rsp.SkipIncRetryCount = true
			}

			return &rsp, err
		}

		_, err = Chat.updateFromFollow(ctx, req.CorpId, req.AppId, chat, dataMsg.Follow)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}

		for _, userRobot := range list {
			err = pushWsMsg(ctx, req.CorpId, req.AppId, userRobot.Uid, []*qmsg.WsMsgWrapper{
				{
					MsgType: uint32(qmsg.WsMsgWrapper_MsgTypeExtContactChange),
					Chat:    chat,
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
			})
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, err
			}
		}

		return &rsp, nil
	})
}

func OneSideFriendMq(ctx *rpc.Context, req *smq.ConsumeReq) (*smq.ConsumeRsp, error) {
	return MqOneSideFriend.ProcessV2(ctx, req, func(ctx *rpc.Context, req *smq.ConsumeReq, data interface{}) (*smq.ConsumeRsp, error) {
		var rsp smq.ConsumeRsp

		mqMsg := data.(*yc.BeOneSideFriendReq)
		if mqMsg.Data == nil {
			log.Warnf("data is nil")
			return &rsp, nil
		}

		var corpId, appId uint32
		var robotUid uint64

		// 先查找是不是扫码号，如果不是扫码号，再查找是不是平台号
		qwhaleSrv := qwhaleService{}
		hostAccount, err := qwhaleSrv.getHostAccountBySn(ctx, mqMsg.RobotSerialNo)
		if err != nil {
			if rpc.GetErrCode(err) != int(qwhale.ErrCode_ErrHostAccountNotFound) {
				log.Errorf("err:%v", err)
				return nil, err
			}
		}
		if hostAccount != nil {
			corpId = hostAccount.CorpId
			appId = hostAccount.AppId
			robotUid = hostAccount.Uid
		} else {
			qrobotSrv := qrobotService{}
			qrobotRobot, err := qrobotSrv.getQRobotByStrOpenId(ctx, mqMsg.GetRobotSerialNo())
			if err != nil {
				log.Errorf("err:%v", err)
				return nil, err
			}
			if qrobotRobot == nil {
				log.Errorf("err:%v", err)
				return nil, rpc.CreateError(qmsg.ErrUserRobotNotFound)
			}
			corpId = qrobotRobot.CorpId
			appId = qrobotRobot.AppId
			robotUid = qrobotRobot.Uid
		}

		listRsp, err := qrobot.GetAccountListSys(ctx, &qrobot.GetAccountListSysReq{
			ListOption: core.NewListOption().SetLimit(1).SetSkipCount().
				AddOpt(qrobot.GetAccountListSysReq_ListOptionStrRobotId, mqMsg.Data.ContactSerialNo),
			CorpId: corpId,
			AppId:  appId,
		})
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}

		if len(listRsp.List) == 0 {
			return &rsp, nil
		}

		account := listRsp.List[0]

		chat, assignChat, err := getChat(ctx, corpId, appId, robotUid, account.Id, false, false)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}

		if chat.Detail == nil {
			chat.Detail = &qmsg.ModelChat_Detail{}
		}

		// 单边好友，所以其中一边数据为 0
		if mqMsg.Type == uint32(qmsg.ModelChat_Detail_OneSideTypeConfirm) {
			chat.Detail.DeletedByStaffAt = uint32(time.Now().Unix())
			chat.Detail.DeletedByExtContactAt = 0
		} else {
			chat.Detail.DeletedByExtContactAt = uint32(time.Now().Unix())
			chat.Detail.DeletedByStaffAt = 0
		}

		chat.Detail.SerialNo = mqMsg.SerialNo
		chat.Detail.YcOneSideType = mqMsg.Data.Type

		_, err = Chat.WhereCorpApp(corpId, appId).Where(DbId, chat.Id).Update(ctx, map[string]interface{}{
			DbDetail: chat.Detail,
		})
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}

		err = pushWsMsgByChat(ctx, chat, assignChat, []*qmsg.WsMsgWrapper{
			{
				MsgType: uint32(qmsg.WsMsgWrapper_MsgTypeOneSideFriend),
				Chat:    chat,
			},
		})
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
		return &rsp, nil
	})
}

func BecomeFriendMq(ctx *rpc.Context, req *smq.ConsumeReq) (*smq.ConsumeRsp, error) {
	return MqBecomeFriend.ProcessV2(ctx, req, func(ctx *rpc.Context, req *smq.ConsumeReq, data interface{}) (*smq.ConsumeRsp, error) {
		var rsp smq.ConsumeRsp

		mqMsg := data.(*yc.BecomeFriendReq)
		if mqMsg.Data == nil {
			log.Warnf("empty data")
			return &rsp, nil
		}
		// 只处理扫码号
		qwhaleSrv := qwhaleService{}
		hostAccount, err := qwhaleSrv.getHostAccountBySn(ctx, mqMsg.RobotSerialNo)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}

		listRsp, err := qrobot.GetAccountListSys(ctx, &qrobot.GetAccountListSysReq{
			ListOption: core.NewListOption().SetLimit(2).SetSkipCount().
				AddOpt(qrobot.GetAccountListSysReq_ListOptionStrRobotIdList, []string{mqMsg.Data.ContactSerialNo, mqMsg.RobotSerialNo}),
			CorpId: hostAccount.CorpId,
			AppId:  hostAccount.AppId,
		})
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}

		if len(listRsp.List) != 2 {
			return &rsp, nil
		}

		var robotUid, chatExtId uint64

		for _, account := range listRsp.List {
			if account.Uid != 0 {
				robotUid = account.Uid
			} else {
				chatExtId = account.Id
			}
		}

		if robotUid == 0 || chatExtId == 0 {
			return &rsp, nil
		}

		chat, err := Chat.getByRobotAndExt(ctx, hostAccount.CorpId, hostAccount.AppId, robotUid, chatExtId, false)
		if err != nil {
			if Chat.IsNotFoundErr(err) {
				return &rsp, nil
			} else {
				log.Errorf("err:%v", err)
				return nil, err
			}
		}

		if chat.Detail == nil {
			return &rsp, nil
		}

		chat.Detail.DeletedByExtContactAt = 0
		chat.Detail.DeletedByStaffAt = 0
		chat.Detail.SerialNo = ""
		chat.Detail.YcOneSideType = 0

		_, err = Chat.updateMap(ctx, hostAccount.CorpId, hostAccount.AppId, chat.Id, map[string]interface{}{
			DbDetail: chat.Detail,
		})
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}

		return &rsp, nil
	})
}
