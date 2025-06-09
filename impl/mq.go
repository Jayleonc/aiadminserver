// aiadmin/impl/mq.go
package impl

import (
	"git.pinquest.cn/base/aiadmin"
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/smq"
)

// MqAIWorkflowTrigger 定义了用于触发AI工作流的MQ实例
var MqAIWorkflowTrigger = smq.NewJsonMsg("ai_workflow_trigger", &aiadmin.AIWorkflowMsqTask{}) //

// MqAIWorkflowBufferEvent 定义了用于接收需要缓冲的原始AI工作流事件的MQ实例
var MqAIWorkflowBufferEvent = smq.NewJsonMsg("ai_workflow_buffer_event_v1", &aiadmin.AIWorkflowTriggerEventWithHistory{})

// InitMq 负责初始化 aiadmin 服务的 MQ Topic 声明。
// 这个函数应该在aiadmin服务启动时被调用。
func InitMq() error { // rpc.Context 可以作为参数传入，或者如果 AddTopic 允许nil，则直接用nil
	log.Infof("开始初始化 aiadmin MQ Topic 声明...")

	// 1. 向SMQ系统注册我们的Topic及其消费者配置
	_, err := smq.AddTopic(nil, &smq.AddTopicReq{ // 假设 AddTopic 允许第一个参数为 nil rpc.Context
		Topic: &smq.Topic{
			Name: MqAIWorkflowTrigger.TopicName, //
			SubConfig: &smq.SubConfig{
				ConcurrentCount:    50,
				MaxRetryCount:      5,
				MaxExecTimeSeconds: 1800,
				ServiceName:        aiadmin.ServiceName,                 // 使用您定义的常量
				ServicePath:        aiadmin.HandleAIWorkflowTaskCMDPath, // 使用您定义的常量
			},
		},
	})
	if err != nil {
		log.Fatalf("向SMQ注册MQ Topic (%s) 失败: %v", MqAIWorkflowTrigger.TopicName, err)
		return err
	}
	log.Infof("MQ Topic (%s) 声明成功，消费者路径: %s", MqAIWorkflowTrigger.TopicName, aiadmin.HandleAIWorkflowTaskCMDPath) //

	_, err = smq.AddTopic(nil, &smq.AddTopicReq{
		Topic: &smq.Topic{
			Name: MqAIWorkflowBufferEvent.TopicName,
			SubConfig: &smq.SubConfig{
				ConcurrentCount:    100, // 缓冲处理应该很快，可以适当调高并发
				MaxRetryCount:      3,   // 缓冲失败重试次数不宜过多
				MaxExecTimeSeconds: 60,  // 单个缓冲事件处理超时时间
				ServiceName:        aiadmin.ServiceName,
				ServicePath:        aiadmin.HandleAIWorkflowBufferEventCMDPath,
			},
		},
	})
	if err != nil {
		log.Fatalf("向SMQ注册MQ Topic (%s) 失败: %v", MqAIWorkflowBufferEvent.TopicName, err)
		return err
	}
	log.Infof("MQ Topic (%s) 声明成功，消费者路径: %s", MqAIWorkflowBufferEvent.TopicName, aiadmin.HandleAIWorkflowBufferEventCMDPath)

	return nil
}
