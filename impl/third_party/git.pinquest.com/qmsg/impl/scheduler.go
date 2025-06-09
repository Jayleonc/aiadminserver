package impl

import (
	"git.pinquest.cn/qlb/pinscheduler"
	"git.pinquest.cn/qlb/qmsg"
	"git.pinquest.cn/qlb/schedulerv2"
)

var SchedulerTaskList []*schedulerv2.AddTaskReq

func init() {
	SchedulerTaskList = append(SchedulerTaskList,
		// 客服质检企业维度数据统计
		&schedulerv2.AddTaskReq{
			Task: &schedulerv2.Task{
				BizCallbackService: qmsg.SvrName,
				BizCallbackPath:    qmsg.SyncSingleChatStatSchedCMDPath,
				BizScene:           pinscheduler.BizQmsgSyncSingleChatStatSched,
				BizId:              "qmsg_single_chat_stat",
				TimeSpec:           "0 0 1 * * *",
			},
		},
		// 会话过期检查
		&schedulerv2.AddTaskReq{
			Task: &schedulerv2.Task{
				BizCallbackService: qmsg.SvrName,
				BizCallbackPath:    qmsg.ChatTimeoutSchedCMDPath,
				BizScene:           pinscheduler.BizQmsgChatTimeout,
				BizId:              "qmsg_chat_timeout",
				TimeSpec:           "0 0/1 * * * *",
			},
		},
		// 会话预警检查
		&schedulerv2.AddTaskReq{
			Task: &schedulerv2.Task{
				BizCallbackService: qmsg.SvrName,
				BizCallbackPath:    qmsg.ChatCountEarlyWarningSchedCMDPath,
				BizScene:           pinscheduler.BizQmsgChatCountEarlyWarning,
				BizId:              "qmsg_chat_count_early_warning",
				TimeSpec:           "0 0/5 * * * *",
			},
		},
		// 客服上下班状态变更
		&schedulerv2.AddTaskReq{
			Task: &schedulerv2.Task{
				BizCallbackService: qmsg.SvrName,
				BizCallbackPath:    qmsg.ChangeUserOnlineSchedCMDPath,
				BizScene:           pinscheduler.BizQmsgChangeUserOnlineSched,
				BizId:              "qmsg_chane_user_online",
				TimeSpec:           "0 0/1 * * * *",
			},
		},
		// 客服质检每日统计
		&schedulerv2.AddTaskReq{
			Task: &schedulerv2.Task{
				BizCallbackService: qmsg.SvrName,
				BizCallbackPath:    qmsg.MsgStatYesterdayReportSchedCMDPath,
				BizScene:           pinscheduler.BizMsgStatYesterdayReportSched,
				BizId:              "qmsg_msg_stat_yesterday_report",
				TimeSpec:           "0 0 9 * * *",
				//TimeSpec: "0 */1 * * * *", // test 每分钟执行一次
			},
		},
		// 客服质检每日超时统计
		&schedulerv2.AddTaskReq{
			Task: &schedulerv2.Task{
				BizCallbackService: qmsg.SvrName,
				BizCallbackPath:    qmsg.SingleChatTimeoutStatSchedCMDPath,
				BizScene:           pinscheduler.BizQmsgSingleChatTimeoutStatSched,
				BizId:              "qmsg_single_chat_timeout_stat_sched",
				TimeSpec:           "2 0 0 * * *",
			},
		},
		// 定时删除聊天记录
		&schedulerv2.AddTaskReq{
			Task: &schedulerv2.Task{
				BizCallbackService: qmsg.SvrName,
				BizCallbackPath:    qmsg.DelHistoryMsgBoxSchedulerCMDPath,
				BizScene:           pinscheduler.BizQmsgDelHistoryMsgBoxScheduler,
				BizId:              "qmsg_del_history_msg_box_scheduler",
				TimeSpec:           "20 10 3 * * *",
			},
		},
	)
}
