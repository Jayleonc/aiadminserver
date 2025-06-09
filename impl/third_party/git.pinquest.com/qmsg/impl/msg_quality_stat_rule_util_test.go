package impl

import (
	"git.pinquest.cn/qlb/brick/rpc"
	"testing"
)

func Test_getMsgQualityStatRule(t *testing.T) {
	InitTestEnv()
	type args struct {
		ctx    *rpc.Context
		corpId uint32
		appId  uint32
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "数据库有值",
			args: args{
				ctx:    nil,
				corpId: 2000772,
				appId:  2000273,
			},
			wantErr: false,
		},
		{
			name: "数据库没有值",
			args: args{
				ctx:    nil,
				corpId: 2000773,
				appId:  2000278,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getMsgQualityStatRule(tt.args.ctx, tt.args.corpId, tt.args.appId)
			if (err != nil) != tt.wantErr {
				if !MsgQualityStatRule.IsNotFoundErr(err) {
					t.Errorf("getMsgQualityStatRule() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
			}
			t.Log(got)
		})
	}
}
