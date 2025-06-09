package impl

import (
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/qmsg"
	"reflect"
	"testing"
)

func TestCheckChannelMsgRecruit(t *testing.T) {
	ctx := InitTestEnvWithCorp(2000772, 2000273, 1527)
	type args struct {
		ctx *rpc.Context
		req *qmsg.CheckChannelMsgRecruitReq
	}
	tests := []struct {
		name    string
		args    args
		want    *qmsg.CheckChannelMsgRecruitRsp
		wantErr bool
	}{
		{
			name: "test",
			args: args{ctx, &qmsg.CheckChannelMsgRecruitReq{
				RobotSerialNo: "ACC0B766EA70F105F521713D5DFEBADD5194F219CF554649F1C4F9C615435A82",
				MsgSerialNo:   "6D6443A646600C953FE4E6D843964A1B",
			}},
			want:    &qmsg.CheckChannelMsgRecruitRsp{},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := CheckChannelMsgRecruit(tt.args.ctx, tt.args.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("CheckChannelMsgRecruit() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("CheckChannelMsgRecruit() got = %v, want %v", got, tt.want)
			}
		})
	}
}
