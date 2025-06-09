package impl

import (
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/core"
	"git.pinquest.cn/qlb/qmsg"
	"reflect"
	"testing"
)

func TestSetSidebarTabSortList(t *testing.T) {
	ctx := InitTestEnvWithCorp(2000772, 2000273, 1527)
	type args struct {
		ctx *rpc.Context
		req *qmsg.SetSidebarTabSortListReq
	}
	tests := []struct {
		name    string
		args    args
		want    *qmsg.SetSidebarTabSortListRsp
		wantErr bool
	}{
		{
			name: "test",
			args: args{ctx, &qmsg.SetSidebarTabSortListReq{
				Uid: 1527,
				List: []*qmsg.SetSidebarTabSortListReq_SidebarTabSort{
					{
						TabType: uint32(qmsg.SidebarTabType_SidebarTabTypeSysGroup),
						TabKey:  "1",
						Sort:    230,
					},
					{
						TabType: uint32(qmsg.SidebarTabType_SidebarTabTypeSysGroup),
						TabKey:  "2",
						Sort:    220,
					},
					{
						TabType: uint32(qmsg.SidebarTabType_SidebarTabTypeSysSingle),
						TabKey:  "1",
						Sort:    220,
					},
				},
			}},
			want:    &qmsg.SetSidebarTabSortListRsp{},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := SetSidebarTabSortList(tt.args.ctx, tt.args.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("SetSidebarTabSortList() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("SetSidebarTabSortList() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetSidebarTabSortList(t *testing.T) {
	ctx := InitTestEnvWithCorp(2000772, 2000273, 1527)
	type args struct {
		ctx *rpc.Context
		req *qmsg.GetSidebarTabSortListReq
	}
	tests := []struct {
		name    string
		args    args
		want    *qmsg.GetSidebarTabSortListRsp
		wantErr bool
	}{
		{
			name: "test",
			args: args{ctx, &qmsg.GetSidebarTabSortListReq{
				ListOption: core.NewListOption().
					SetSkipCount().
					AddOpt(uint32(qmsg.GetSidebarTabSortListReq_ListOptionUid), 1527).
					AddOpt(uint32(qmsg.GetSidebarTabSortListReq_ListOptionSidebarTabType), "1,2,3,4").
					AddOpt(uint32(qmsg.GetSidebarTabSortListReq_ListOptionOrderBySort), "true"),
			}},
			want:    &qmsg.GetSidebarTabSortListRsp{},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GetSidebarTabSortList(tt.args.ctx, tt.args.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetSidebarTabSortList() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetSidebarTabSortList() got = %v, want %v", got, tt.want)
			}
		})
	}
}
