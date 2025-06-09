package impl

import (
	"fmt"
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/brick/smq"
	"git.pinquest.cn/qlb/brick/utils"
	"git.pinquest.cn/qlb/brick/utils/routine"
	"git.pinquest.cn/qlb/extrpkg/github.com/go-redis/redis"
	"git.pinquest.cn/qlb/qmsg"
	"git.pinquest.cn/qlb/quan"
	"git.pinquest.cn/qlb/schedulerv2"
	"time"
)

// SyncSingleChatStatSched 客服质检企业维度数据统计
func SyncSingleChatStatSched(ctx *rpc.Context, req *schedulerv2.SchedulerCallbackReq) (*schedulerv2.SchedulerCallbackRsp, error) {
	var rsp schedulerv2.SchedulerCallbackRsp
	schedulerv2.Async(ctx, req.Task, func(ctx *rpc.Context) error {
		return DoSyncSingleChatStatSched(ctx)
	})
	rsp.IsAsync = true
	return &rsp, nil
}

func DoSyncSingleChatStatSched(ctx *rpc.Context) error {
	rules, err := MsgQualityStatRule.GetNeedRunSchedRuleList(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	if len(rules) == 0 {
		log.Warnf("empty rules")
		return nil
	}
	//清理历史数据
	routine.Go(ctx, func(ctx *rpc.Context) error {
		err = MsgStatMsgCache.cleanOldData(ctx)
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
		return nil
	})
	for _, rule := range rules {
		_, err = MqSyncSingleChatStat.PubV2(ctx, &smq.PubReq{
			CorpId: rule.CorpId,
			AppId:  rule.AppId,
		}, &qmsg.SyncSingleChatStatMessageMqReq{
			CorpId: rule.CorpId,
			AppId:  rule.AppId,
			Rule:   rule,
		})
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
	}
	return nil
}

func ChatTimeoutSched(ctx *rpc.Context, req *schedulerv2.SchedulerCallbackReq) (*schedulerv2.SchedulerCallbackRsp, error) {
	var rsp schedulerv2.SchedulerCallbackRsp
	schedulerv2.Async(ctx, req.Task, func(ctx *rpc.Context) error {
		var configList []*qmsg.ModelSessionConfig
		var offset, limit int64
		now := uint32(time.Now().Unix())

		err := SessionConfig.Where(1, 1).Find(ctx, &configList)
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}

		for _, config := range configList {
			if config.SessionSetting != nil {
				if config.SessionSetting.AllowCloseBySys {
					min := config.SessionSetting.CloseBySysMinutes
					if min != 0 {
						offset = 0
						limit = 500
						for {
							// 从客服最后发送时间队列获取超时会话id列表
							list, err := getSendRedisList(fmt.Sprintf(UserLastSend, config.CorpId), now-min*60, offset, limit)
							if err != nil {
								log.Errorf("err:%v", err)
								return err
							}
							if len(list) == 0 {
								break
							}
							err = closeAssignChat4Sys(ctx, config, list)
							if err != nil {
								log.Errorf("err:%v", err)
								return err
							}

							if len(list) < int(limit) {
								break
							} else {
								offset += limit
							}
						}
					}
				} else {
					// 删除队列(原代码没有对配置做校验，全部放入到队列中，并已上线产生数据，这里对历史数据做清理，二次上线后删除此处代码)
					err = deleteUserSendRedis(config.CorpId)
					if err != nil {
						log.Errorf("err:%v", err)
						return err
					}
				}
				if config.SessionSetting.AllowAutoReassignUnanswered {
					min := config.SessionSetting.AutoReassignUnansweredMinutes
					if min != 0 {
						offset = 0
						limit = 500
						for {
							list, err := getSendRedisList(fmt.Sprintf(ContactLastSend, config.CorpId), now-min*60, offset, limit)
							if err != nil {
								log.Errorf("err:%v", err)
								return err
							}
							if len(list) == 0 {
								break
							}

							for _, cid := range list {
								err = reAssign4Sys(ctx, config, cid)
								if err != nil {
									log.Errorf("err:%v", err)
									return err
								}
							}

							if len(list) < int(limit) {
								break
							} else {
								offset += limit
							}
						}
					}
				} else {
					// 删除队列(原代码没有对配置做校验，全部放入到队列中，并已上线产生数据，这里对历史数据做清理，二次上线后删除此处代码)
					err = deleteContactSendRedis(config.CorpId)
					if err != nil {
						log.Errorf("err:%v", err)
						return err
					}
				}
			}
		}

		return nil
	})

	return &rsp, nil
}

// @desc: 用户超时未回复，系统自动关闭会话
func closeAssignChat4Sys(ctx *rpc.Context, config *qmsg.ModelSessionConfig, list []string) error {
	// 获取分配会话记录
	var assignChatList []*qmsg.ModelAssignChat
	err := AssignChat.WhereCorpApp(config.CorpId, config.AppId).Where(DbUid, "!=", 0).WhereIn(DbCid, list).Find(ctx, &assignChatList)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	assignChatIdList := utils.PluckUint64(assignChatList, "Id")
	// 关闭分配会话
	err = AssignChat.setClosed(ctx, config.CorpId, config.AppId, map[string]interface{}{
		DbEndScene: qmsg.ModelAssignChat_EndSceneContactAutoClose,
	}, assignChatIdList...)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	// 加入最近联系人
	err = LastContactChat.batchCreateOrUpdate(ctx, config.CorpId, config.AppId, assignChatIdList)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	// 关闭会话，通知客服
	for _, assignChat := range assignChatList {
		err = pushWsMsg(ctx, assignChat.CorpId, assignChat.AppId, assignChat.Uid,
			[]*qmsg.WsMsgWrapper{
				{
					MsgType:      uint32(qmsg.WsMsgWrapper_MsgTypeRemoveAssignChat),
					AssignChatId: assignChat.Id,
				},
			})
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
	}

	err = pushWsMsgByClose(ctx, config.CorpId, config.AppId, assignChatList, false)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	// 从最后发送队列中移除
	err = removeUserSendRedis(config.CorpId, list)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	return nil
}

// @desc: 系统自动重新分配
func reAssign4Sys(ctx *rpc.Context, config *qmsg.ModelSessionConfig, cid string) error {
	var assignChat qmsg.ModelAssignChat
	err := AssignChat.WhereCorpApp(config.CorpId, config.AppId).Where(DbCid, cid).Where(DbUid, "!=", 0).First(ctx, &assignChat)
	if err != nil {
		if AssignChat.IsNotFoundErr(err) {

			_, err = s.RedisGroup.ZRem(fmt.Sprintf(ContactLastSend, config.CorpId), cid)
			if err != nil {
				log.Errorf("err:%v", err)
			}

			return nil
		}
		log.Errorf("err:%v", err)
		return err
	}

	// 先结束
	err = AssignChat.setClosed(ctx, config.CorpId, config.AppId, map[string]interface{}{
		DbEndScene: qmsg.ModelAssignChat_EndSceneUserAutoClose,
	}, assignChat.Id)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	_, err = LastContactChat.createOrUpdateByAssignChatId(ctx, config.CorpId, config.AppId, assignChat.Id)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	assignUid, scene, err := findAssignUser(ctx, config.CorpId, config.AppId, assignChat.RobotUid, assignChat.ChatExtId, assignChat.IsGroup, assignChat.Uid)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	// 分配失败，撤销结束
	if assignUid == 0 {
		_, err = AssignChat.WithTrash().WhereCorpApp(config.CorpId, config.AppId).Where(DbId, assignChat.Id).Update(ctx,
			map[string]interface{}{
				DbDeletedAt: 0,
				DbEndScene:  0,
			})
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
		_, err = LastContactChat.delete(ctx, config.CorpId, config.AppId, assignChat.Uid, assignChat.Cid)
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
		// 回滚之后不发ws消息给前端
		return nil
	} else {
		_, err = assignUser(ctx, config.CorpId, config.AppId, assignChat.RobotUid, assignChat.ChatExtId, assignUid, assignChat.IsGroup, scene)
		if err != nil {
			log.Errorf("err %v", err)
			return err
		}
	}

	err = pushWsMsg(ctx, assignChat.CorpId, assignChat.AppId, assignChat.Uid,
		[]*qmsg.WsMsgWrapper{
			{
				MsgType:      uint32(qmsg.WsMsgWrapper_MsgTypeRemoveAssignChat),
				AssignChatId: assignChat.Id,
			},
		})
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	return nil
}

const OnWorkEarlyWarningKey = "qmsg_on_work_early_warning_%d"
const OffWorkEarlyWarningKey = "qmsg_off_work_early_warning_%d"

func ChatCountEarlyWarningSched(ctx *rpc.Context, req *schedulerv2.SchedulerCallbackReq) (*schedulerv2.SchedulerCallbackRsp, error) {
	var rsp schedulerv2.SchedulerCallbackRsp
	schedulerv2.Async(ctx, req.Task, func(ctx *rpc.Context) error {
		var configList []*qmsg.ModelSessionConfig
		err := SessionConfig.Where(1, 1).Find(ctx, &configList)
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}

		for _, config := range configList {
			if config.OfficeHourSetting != nil {
				onWork, err := getIsOnWork(ctx, config.CorpId, config.AppId)
				if err != nil {
					log.Errorf("err:%v", err)
					return err
				}

				if onWork && config.OfficeHourSetting.AllowOnWorkNotify {
					_, err = s.RedisGroup.Get(fmt.Sprintf(OnWorkEarlyWarningKey, config.CorpId))
					if err != nil {
						if err == redis.Nil {
							c, err := AssignChat.WhereCorpApp(config.CorpId, config.AppId).Where(DbUid, 0).Count(ctx)
							if err != nil {
								log.Errorf("err:%v", err)
								return err
							}

							if c >= config.OfficeHourSetting.OnWorkChatCount {
								_, err = quan.SendNotifySys(ctx, &quan.SendNotifySysReq{
									CorpId:  config.CorpId,
									AppId:   config.AppId,
									UidList: config.OfficeHourSetting.OnWorkUidList,
									Content: fmt.Sprintf("当前排队会话数已达【%d】，请及时处理。", c),
								})
								if err != nil {
									log.Errorf("err:%v", err)
									return err
								}

								_, err = s.RedisGroup.SetNX(fmt.Sprintf(OnWorkEarlyWarningKey, config.CorpId), []byte(""), time.Hour)
								if err != nil {
									log.Errorf("err:%v", err)
									return err
								}
							}
						} else {
							log.Errorf("err:%v", err)
							return err
						}
					}
				}
				if !onWork && config.OfficeHourSetting.AllowOffWorkNotify {
					_, err = s.RedisGroup.Get(fmt.Sprintf(OffWorkEarlyWarningKey, config.CorpId))
					if err != nil {
						if err == redis.Nil {
							c, err := AssignChat.WhereCorpApp(config.CorpId, config.AppId).Where(DbUid, 0).Count(ctx)
							if err != nil {
								log.Errorf("err:%v", err)
								return err
							}

							if c >= config.OfficeHourSetting.OffWorkChatCount {
								_, err = quan.SendNotifySys(ctx, &quan.SendNotifySysReq{
									CorpId:  config.CorpId,
									AppId:   config.AppId,
									UidList: config.OfficeHourSetting.OffWorkUidList,
									Content: fmt.Sprintf("当前排队会话数已达【%d】，请及时处理。", c),
								})
								if err != nil {
									log.Errorf("err:%v", err)
									return err
								}

								_, err = s.RedisGroup.SetNX(fmt.Sprintf(OffWorkEarlyWarningKey, config.CorpId), []byte(""), time.Hour)
								if err != nil {
									log.Errorf("err:%v", err)
									return err
								}
							}
						} else {
							log.Errorf("err:%v", err)
							return err
						}
					}
				}
			}
		}
		return nil
	})
	return &rsp, nil
}

func ChangeUserOnlineSched(ctx *rpc.Context, req *schedulerv2.SchedulerCallbackReq) (*schedulerv2.SchedulerCallbackRsp, error) {
	var rsp schedulerv2.SchedulerCallbackRsp

	schedulerv2.Async(ctx, req.Task, func(ctx *rpc.Context) error {
		var offset uint32 = 0
		var limit uint32 = 100
		for {
			var userList []*qmsg.ModelUser
			_, err := User.Where(DbIsDisable, false).
				//Where(DbIsUseOfficeHour, true).
				//Where(DbIsAutoChangeWorkState, true).
				Where(DbIsAssign, false).
				SetOffset(offset).SetLimit(limit).FindPaginate(ctx,
				&userList)
			if err != nil {
				log.Errorf("err:%v", err)
				return err
			}
			if len(userList) == 0 {
				break
			}

			corpUserList := make(map[uint32][]*qmsg.ModelUser)

			for _, v := range userList {
				corpUserList[v.CorpId] = append(corpUserList[v.CorpId], v)
			}

			for _, list := range corpUserList {

				if len(list) == 0 {
					continue
				}

				var sessionConf qmsg.ModelSessionConfig
				err = SessionConfig.WhereCorpApp(list[0].CorpId, list[0].AppId).First(ctx, &sessionConf)
				if err != nil && !SessionConfig.IsNotFoundErr(err) {
					log.Errorf("err %v", err)
					return err
				}

				for _, user := range list {
					if sessionConf.OfficeHourSetting != nil && sessionConf.OfficeHourSetting.WorkSetType ==
						uint32(qmsg.ModelSessionConfig_OfficeHourSetting_NotAllowManualSetWork) {
						if !user.IsOnline {
							err = autoSetUserWorkState(ctx, user, true)
							if err != nil {
								log.Errorf("err:%v", err)
							}
						}
						continue
					}

					err = changeUserWorkState(ctx, user)
					if err != nil {
						log.Errorf("err:%v", err)
						return err
					}
				}
			}

			offset += limit
		}

		return nil
	})
	return &rsp, nil
}

func changeUserWorkState(ctx *rpc.Context, user *qmsg.ModelUser) error {
	if !user.IsAutoChangeWorkState {
		return nil
	}
	var userOfficeHourList []*qmsg.ModelUserOfficeHour
	err := UserOfficeHour.WhereCorpApp(user.CorpId, user.AppId).Where(DbUid, user.Id).Find(ctx, &userOfficeHourList)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	operateTime, err := getUserWorkStateOperateTime(user)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	for _, userOfficeHour := range userOfficeHourList {
		var officeHour *qmsg.ModelOfficeHour
		err := OfficeHour.WhereCorpApp(user.CorpId, user.AppId).Where(DbId, userOfficeHour.OfficeHourId).
			Where(fmt.Sprintf("JSON_CONTAINS(convert(week_list using utf8mb4), '[%d]')", time.Now().Weekday())).
			First(ctx, &officeHour)
		if err != nil && !OfficeHour.IsNotFoundErr(err) {
			log.Errorf("err:%v", err)
			return err
		}
		if officeHour == nil {
			continue
		}
		nowMinutes := getNowMinutes()
		if int(officeHour.StartAt) <= nowMinutes && int(officeHour.EndAt) >= nowMinutes {
			isUserOperateInCurrOfficeHour, err := checkUserOperate4OfficeHour(user, officeHour, operateTime)
			if err != nil {
				log.Errorf("err:%v", err)
				return err
			}

			// 在上班时间，且当前时间段内，用户没有进行过主动操作，将状态修改为上班
			if !isUserOperateInCurrOfficeHour && !user.IsOnline {
				err = autoSetUserWorkState(ctx, user, true)
				if err != nil {
					log.Errorf("err:%v", err)
					return err
				}
			}
			return nil
		}
	}

	// 以下为不在上班时间范围的逻辑
	isUserOperateInAllOfficeHour, err := checkUserOperate4OutOfficeHour(ctx, user, operateTime)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	// 用户操作是在工作时间范围内，不影响非工作时间范围的系统操作
	if isUserOperateInAllOfficeHour {
		operateTime = 0
	}
	// 非工作时间范围内有过自主操作，系统不更新用户上下班状态
	if operateTime > 0 {
		return nil
	}
	// 不在上班时间，且当前状态为上班，将状态修改为下班
	if user.IsOnline {
		err = autoSetUserWorkState(ctx, user, false)
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
	}
	return nil
}

// @desc: 工作时间范围内的用户操作时间检测
func checkUserOperate4OfficeHour(user *qmsg.ModelUser, officeHour *qmsg.ModelOfficeHour, operateTime int) (bool, error) {
	if operateTime == 0 {
		return false, nil
	}
	if int(officeHour.StartAt) <= operateTime && int(officeHour.EndAt) >= operateTime {
		return true, nil
	} else {
		// 用户操作时间在当前工作时间范围之外，删除此次操作时间(重新计数)
		err := delUserWorkStateOperateTime(user)
		if err != nil {
			log.Errorf("err:%v", err)
			return false, err
		}
		return false, nil
	}
}

// @desc: 非工作时间范围内的用户操作时间检测
func checkUserOperate4OutOfficeHour(ctx *rpc.Context, user *qmsg.ModelUser, operateTime int) (bool, error) {
	if operateTime == 0 {
		return false, nil
	}
	var officeHour *qmsg.ModelOfficeHour
	err := OfficeHour.WhereCorpApp(user.CorpId, user.AppId).
		Where(fmt.Sprintf("JSON_CONTAINS(convert(week_list using utf8mb4), '[%d]')", time.Now().Weekday())).
		Where(DbStartAt, "<=", operateTime).
		Where(DbEndAt, ">=", operateTime).
		First(ctx, &officeHour)
	if err != nil && !OfficeHour.IsNotFoundErr(err) {
		log.Errorf("err:%v", err)
		return false, err
	}
	if officeHour == nil {
		return false, nil
	} else {
		// 删除用户在工作时间范围内的操作
		err = delUserWorkStateOperateTime(user)
		if err != nil {
			log.Errorf("err:%v", err)
			return false, err
		}
		return true, nil
	}

}

// 系统自动设置工作状态
func autoSetUserWorkState(ctx *rpc.Context, user *qmsg.ModelUser, isOnline bool) error {
	_, err := User.WhereCorpApp(user.CorpId, user.AppId).Where(DbId, user.Id).Update(ctx, map[string]interface{}{
		DbIsOnline: isOnline,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	var logType qmsg.UserLog_LogType
	if isOnline {
		logType = qmsg.UserLog_LogTypeOnline
	} else {
		logType = qmsg.UserLog_LogTypeOffline
	}

	err = pushWsMsg(ctx, user.CorpId, user.AppId, user.Id, []*qmsg.WsMsgWrapper{
		{
			MsgType:  uint32(qmsg.WsMsgWrapper_MsgTypeIsOnlineChange),
			Uid:      user.Id,
			IsOnline: isOnline,
		},
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	_, err = OperatorLog.create(ctx, user.CorpId, user.AppId, user.Id, logType)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	//新增客服上下班回调
	err = callbackKefuStateChange(ctx, user.CorpId, user.AppId, user.LoginId, user.StrOpenId, isOnline)
	if err != nil {
		log.Errorf("err %v", err)
	}

	return nil
}

// MsgStatYesterdayReportSched h5-会话质检-每日定时日报提醒
func MsgStatYesterdayReportSched(ctx *rpc.Context, req *schedulerv2.SchedulerCallbackReq) (*schedulerv2.SchedulerCallbackRsp, error) {
	var rsp schedulerv2.SchedulerCallbackRsp
	log.Debugf("start msg_stat_yesterday_report_sched ")
	schedulerv2.Async(ctx, req.Task, func(ctx *rpc.Context) error {
		log.Debugf("start run async task msg_stat_yesterday_report_sched ")
		rules, err := MsgQualityStatRule.GetNeedRunSchedRuleList(ctx)
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
		log.Debugf("have %d corp need notify", len(rules))
		for _, rule := range rules {
			err := StatSingleCorpData(ctx, rule)
			if err != nil {
				log.Errorf("err:%v", err)
				continue
			}
		}
		log.Debugf("end run async task msg_stat_yesterday_report_sched")
		return nil
	})
	rsp.IsAsync = true
	return &rsp, nil
}

func SingleChatTimeoutStatSched(ctx *rpc.Context, req *schedulerv2.SchedulerCallbackReq) (*schedulerv2.SchedulerCallbackRsp, error) {
	var rsp schedulerv2.SchedulerCallbackRsp
	schedulerv2.Async(ctx, req.Task, func(ctx *rpc.Context) error {
		log.Debugf("start run async task SingleChatTimeoutStatSched")
		err := RunSingleChatTimeoutStat(ctx)
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
		return nil
	})
	rsp.IsAsync = true
	return &rsp, nil
}

func RunSingleChatTimeoutStat(ctx *rpc.Context) error {
	rules, err := MsgQualityStatRule.GetNeedRunSchedRuleList(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}
	lastDay := getYesterday()
	for _, rule := range rules {
		err = HandleTimeoutByStatRule(ctx, rule, lastDay)
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
	}
	return nil
}
