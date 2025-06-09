package impl

import (
	"fmt"
	"testing"

	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/qmsg"
)

func TestGenerateAiReply(t *testing.T) {
	ctx := InitTestEnvWithCorp(2000772, 2000273, 5229718142)
	type args struct {
		ctx *rpc.Context
		req *qmsg.GenerateAiReplyReq
	}
	tests := []struct {
		name    string
		args    args
		want    *qmsg.GenerateAiReplyRsp
		wantErr bool
	}{
		{
			name: "normal",
			args: args{
				ctx: ctx,
				req: &qmsg.GenerateAiReplyReq{
					RobotUid: 5229718142,
					ChatId:   16355932,
					ChatType: 1,
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GenerateAiReply(tt.args.ctx, tt.args.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("GenerateAiReply() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != nil {
				fmt.Printf("%s\n", got.Reply)
				fmt.Println("==================================================================")
				fmt.Printf("多风格[1]：%v\n", got.Replies[0])
				fmt.Println("==================================================================")
				if len(got.Replies) > 1 {
					fmt.Printf("多风格[2]：%v\n", got.Replies[1])
				}
			}
		})
	}
}

func TestGenerateAiReply2(t *testing.T) {
	ctx := InitTestEnvWithCorp(2000772, 2000273, 1699)
	type args struct {
		ctx *rpc.Context
		req *qmsg.GenerateAiReplyReq
	}
	tests := []struct {
		name string
		args args
		want *qmsg.GenerateAiReplyRsp

		wantErr bool
	}{
		{
			name: "normal",
			args: args{
				ctx: ctx,
				req: &qmsg.GenerateAiReplyReq{
					RobotUid: 1699,
					ChatId:   16355932,
					ChatType: 1,
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GenerateAiReply(tt.args.ctx, tt.args.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("GenerateAiReply() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != nil {
				fmt.Printf("%s\n", got.Reply)
				fmt.Println("==================================================================")
				fmt.Printf("多风格[1]：%v\n", got.Replies[0])
				fmt.Println("==================================================================")
				if len(got.Replies) > 1 {
					fmt.Printf("多风格[2]：%v\n", got.Replies[1])
				}
			}
		})
	}
}
