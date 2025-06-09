package impl

import (
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/core"
	"git.pinquest.cn/qlb/qmsg"
	"reflect"
	"testing"
)

func TestGetMsgBoxList(t *testing.T) {
	ctx := InitTestEnvWithCorp(2000772, 2000273, 1257)
	type args struct {
		ctx *rpc.Context
		req *qmsg.GetMsgBoxListReq
	}
	tests := []struct {
		name    string
		args    args
		want    *qmsg.GetMsgBoxListRsp
		wantErr bool
	}{
		{
			name: "test",
			args: args{ctx, &qmsg.GetMsgBoxListReq{
				ListOption: core.NewListOption(),
				ChatType:   1,
				ChatId:     16315415,
				RobotUid:   5229683071,
			}},
			want:    &qmsg.GetMsgBoxListRsp{},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GetMsgBoxList(tt.args.ctx, tt.args.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetMsgBoxList() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetMsgBoxList() got = %v, want %v", got, tt.want)
			}
		})
	}
}
