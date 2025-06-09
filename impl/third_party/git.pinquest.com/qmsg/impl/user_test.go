package impl

import (
	"fmt"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/extrpkg/github.com/elliotchance/pie/pie"
	"git.pinquest.cn/qlb/qmsg"
	"reflect"
	"testing"
)

func TestDiff(t *testing.T) {
	origin := pie.Uint64s{1, 2, 3, 4, 5}
	update := pie.Uint64s{1, 3, 5, 6, 7}
	add, delete := origin.Diff(update)
	//left := origin.Intersect(update)
	//fmt.Println(left.Append(add...))
	fmt.Println(add)
	fmt.Println(delete)
}

func TestGetUserByRobotUidList(t *testing.T) {
	ctx := InitTestEnvWithCorp(2000772, 2000273, 1527)
	type args struct {
		ctx *rpc.Context
		req *qmsg.GetUserByRobotUidListReq
	}
	tests := []struct {
		name    string
		args    args
		want    *qmsg.GetUserByRobotUidListRsp
		wantErr bool
	}{
		{
			name: "test",
			args: args{ctx, &qmsg.GetUserByRobotUidListReq{
				RobotUidList: []uint64{
					1308,
					1428,
					1637,
					1642,
					1640,
					1646,
					1263},
			}},
			want:    &qmsg.GetUserByRobotUidListRsp{},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GetUserByRobotUidList(tt.args.ctx, tt.args.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetUserByRobotUidList() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetUserByRobotUidList() got = %v, want %v", got, tt.want)
			}
		})
	}
}
