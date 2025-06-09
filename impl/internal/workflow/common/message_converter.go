// file: impl/internal/workflow/common/message_converter.go
package common

import (
	"git.pinquest.cn/base/aiadmin"
	"github.com/cloudwego/eino/schema"
	"github.com/darabuchi/log"
)

// ToEinoMessage 将 aiadmin.ChatMessage 转换为 eino.schema.Message
// 这个函数将处理角色映射。
func ToEinoMessage(chatMsg *aiadmin.ChatMessage) *schema.Message {
	if chatMsg == nil {
		return nil
	}

	content := chatMsg.GetContent()

	// chatMsg.GetRole() 返回的是 string 类型，其值应与 aiadmin.RoleType 的常量值（小写）对应
	// 例如，如果 qmsg 端设置 Role 为 "user"，这里 GetRole() 就会得到 "user"
	roleFromMsg := chatMsg.GetRole()

	switch aiadmin.RoleType(roleFromMsg) {
	case aiadmin.RoleTypeUser: // "user"
		return schema.UserMessage(content)
	case aiadmin.RoleTypeAssistant: // "assistant"
		return schema.AssistantMessage(content, nil)
	case aiadmin.RoleTypeSystem: // "system"
		return schema.SystemMessage(content)
	case aiadmin.RoleTypeTool: // "tool"
		return schema.ToolMessage(content, "")
	default:
		log.Warnf("ToEinoMessage: 未知角色 '%s'，内容将作为 UserMessage 处理。", roleFromMsg)
		// 即使角色未知，仍然可以尝试将其视为用户消息，或者根据业务需求返回nil或错误
		return schema.UserMessage(content)
	}
}

// ToEinoMessages 批量转换 (这个函数逻辑不变)
func ToEinoMessages(chatMsgs []*aiadmin.ChatMessage) []*schema.Message {
	if len(chatMsgs) == 0 {
		return nil
	}
	einoMsgs := make([]*schema.Message, 0, len(chatMsgs))
	for _, msg := range chatMsgs {
		if einoMsg := ToEinoMessage(msg); einoMsg != nil {
			einoMsgs = append(einoMsgs, einoMsg)
		}
	}
	return einoMsgs
}
