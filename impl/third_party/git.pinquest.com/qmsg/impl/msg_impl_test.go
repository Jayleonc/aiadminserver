package impl

import (
	"fmt"
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/dbproxy"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/core"
	"git.pinquest.cn/qlb/qmsg"
	"github.com/golang/protobuf/proto"
	"reflect"
	"testing"
)

func Test_addMsg(t *testing.T) {
	m := &qmsg.ModelMsgBox{
		CorpId:      1,
		AppId:       2,
		Uid:         3,
		MsgSeq:      6,
		CliMsgId:    "xxxx3",
		MsgType:     5,
		ChatType:    6,
		ChatId:      7,
		ChatMsgType: 8,
		Msg:         "9",
	}
	err := MsgBox.Where(map[string]interface{}{
		DbCorpId:     m.CorpId,
		DbAppId:      m.AppId,
		"uid":        m.Uid,
		"msg_seq >=": m.MsgSeq,
	}).CondCreate(nil, uint32(dbproxy.CondInsertModelReq_MethodNotExists), m)
	if err != nil {
		log.Errorf("err:%v", err)
		return
	}
	log.Infof("create success %+v", m)
}
func Test_genWxAppContent(t *testing.T) {
	//log.Info(genWxAppContent(nil, 2000772, 2000273, 0,
	//	"CgwIZBABGDMg+9/L4Ao=", "luckincoffee瑞幸咖啡",
	//	"https://quan-1259287960.cos.ap-guangzhou.myqcloud.com/0/5039baf90c2c4c2389bf600c4c97f2da.jpg"))
}

func TestPb(t *testing.T) {
	var a qmsg.UidMsgIdAnalyze
	_ = proto.Unmarshal([]byte("\b\xf7\xb3\xf0\xf9\xb0/\x12\x11addSingleMsg4Send"), &a)
	fmt.Println(a)
	return
	var sessionConfig qmsg.ModelSessionConfig
	_ = proto.Unmarshal([]byte("2\x02\x10c"), &sessionConfig)
	fmt.Printf("%+v", sessionConfig)
	fmt.Println()
	fmt.Printf("%+v", sessionConfig.AssignSetting)
	fmt.Println()
	fmt.Printf("%+v", sessionConfig.AssignSetting.MaxChatNum)
	fmt.Println()
	fmt.Printf("%+v", sessionConfig.AssignSetting.MaxChatNum-uint32(11))
	fmt.Println()
	fmt.Printf("%+v", sessionConfig.AssignSetting.MaxChatNum-uint32(11))

}

func TestGetChatSessionByIframe(t *testing.T) {

	ctx := core.NewInternalRpcCtx(2000772, 2000273, 5229318951)

	type args struct {
		ctx *rpc.Context
		req *qmsg.GetChatSessionByIframeReq
	}
	tests := []struct {
		name    string
		args    args
		want    *qmsg.GetChatSessionByIframeRsp
		wantErr bool
	}{
		// TODO: Add test cases.
		{
			name: "test001",
			args: args{
				ctx: ctx,
				req: &qmsg.GetChatSessionByIframeReq{
					GroupId:   0,
					ExtUserId: "wmfd4MDAAA8ZNLJjO9jwrSYbRccjcFwA",
					RobotUid:  5229318970,
					StrOpenId: "",
				},
			},
			want: &qmsg.GetChatSessionByIframeRsp{
				Chat: &qmsg.ModelChat{
					Id:            7225833105,
					CreatedAt:     1627544982,
					UpdatedAt:     1638153589,
					CorpId:        2000772,
					AppId:         2000273,
					RobotUid:      5229318970,
					MsgSeq:        2669,
					Msg:           "123",
					ChatExtId:     5407306,
					LastSentAt:    1633039576,
					ChatMsgType:   1,
					MsgUpdatedAt:  1633039576,
					FirstUnreadAt: 1627544982,
					Detail: &qmsg.ModelChat_Detail{
						Name:      "Heley",
						Avatar:    "http://wx.qlogo.cn/mmhead/XCopLcwfzefIkMYzVXCWr2b4spyh4FtAbynT0st2EhvaNuVHQCkheQ/0",
						Remark:    "Heley备注321",
						UpdatedAt: 1638153589,
						UserType:  1,
					},
				},
			},
			wantErr: false,
		},
		{
			name: "test001",
			args: args{
				ctx: ctx,
				req: &qmsg.GetChatSessionByIframeReq{
					GroupId:   0,
					ExtUserId: "wmfd4MDAAA8ZNLJjO9jwrSYbRccjcFwA",
					RobotUid:  0,
					StrOpenId: "accCN8VrTCP2MpeI4QgwI4e",
				},
			},
			want: &qmsg.GetChatSessionByIframeRsp{
				Chat: &qmsg.ModelChat{
					Id:            7225833105,
					CreatedAt:     1627544982,
					UpdatedAt:     1638153589,
					CorpId:        2000772,
					AppId:         2000273,
					RobotUid:      5229318970,
					MsgSeq:        2669,
					Msg:           "123",
					ChatExtId:     5407306,
					LastSentAt:    1633039576,
					ChatMsgType:   1,
					MsgUpdatedAt:  1633039576,
					FirstUnreadAt: 1627544982,
					Detail: &qmsg.ModelChat_Detail{
						Name:      "Heley",
						Avatar:    "http://wx.qlogo.cn/mmhead/XCopLcwfzefIkMYzVXCWr2b4spyh4FtAbynT0st2EhvaNuVHQCkheQ/0",
						Remark:    "Heley备注321",
						UpdatedAt: 1638153589,
						UserType:  1,
					},
				},
			},
			wantErr: false,
		},
		{
			name: "test001",
			args: args{
				ctx: ctx,
				req: &qmsg.GetChatSessionByIframeReq{
					GroupId:   0,
					ExtUserId: "wmfd4MDAAA8ZNLJjO9jwrSYbRccjcFwA",
					RobotUid:  0,
					StrOpenId: "accCN8VrTCP2MpeI4QgwI4e",
				},
			},
			want: &qmsg.GetChatSessionByIframeRsp{
				Chat: &qmsg.ModelChat{
					Id:            7225833105,
					CreatedAt:     1627544982,
					UpdatedAt:     1638153589,
					CorpId:        2000772,
					AppId:         2000273,
					RobotUid:      5229318970,
					MsgSeq:        2669,
					Msg:           "123",
					ChatExtId:     5407306,
					LastSentAt:    1633039576,
					ChatMsgType:   1,
					MsgUpdatedAt:  1633039576,
					FirstUnreadAt: 1627544982,
					Detail: &qmsg.ModelChat_Detail{
						Name:      "Heley",
						Avatar:    "http://wx.qlogo.cn/mmhead/XCopLcwfzefIkMYzVXCWr2b4spyh4FtAbynT0st2EhvaNuVHQCkheQ/0",
						Remark:    "Heley备注321",
						UpdatedAt: 1638153589,
						UserType:  1,
					},
				},
			},
			wantErr: false,
		},
		{
			name: "test001",
			args: args{
				ctx: ctx,
				req: &qmsg.GetChatSessionByIframeReq{
					GroupId:   0,
					ExtUserId: "wmfd4MDAAA8ZNLJjO9jwrSYbRccjcFwA",
					RobotUid:  5229318970,
					StrOpenId: "",
				},
			},
			want: &qmsg.GetChatSessionByIframeRsp{
				Chat: &qmsg.ModelChat{
					Id:            7225833105,
					CreatedAt:     1627544982,
					UpdatedAt:     1638153589,
					CorpId:        2000772,
					AppId:         2000273,
					RobotUid:      5229318970,
					MsgSeq:        2669,
					Msg:           "123",
					ChatExtId:     5407306,
					LastSentAt:    1633039576,
					ChatMsgType:   1,
					MsgUpdatedAt:  1633039576,
					FirstUnreadAt: 1627544982,
					Detail: &qmsg.ModelChat_Detail{
						Name:      "Heley",
						Avatar:    "http://wx.qlogo.cn/mmhead/XCopLcwfzefIkMYzVXCWr2b4spyh4FtAbynT0st2EhvaNuVHQCkheQ/0",
						Remark:    "Heley备注321",
						UpdatedAt: 1638153589,
						UserType:  1,
					},
				},
			},
			wantErr: false,
		},
		{
			name: "test001",
			args: args{
				ctx: ctx,
				req: &qmsg.GetChatSessionByIframeReq{
					GroupId:   71345,
					ExtUserId: "",
					RobotUid:  5229318970,
					StrOpenId: "",
				},
			},
			want: &qmsg.GetChatSessionByIframeRsp{
				Chat: &qmsg.ModelChat{
					Id:              7225840120,
					CreatedAt:       1632290775,
					UpdatedAt:       1637747462,
					CorpId:          2000772,
					AppId:           2000273,
					RobotUid:        5229318970,
					IsGroup:         true,
					MsgSeq:          2703,
					Msg:             ";ljaslkdj",
					ChatExtId:       71345,
					ChatMsgType:     1,
					SenderAccountId: 5418396,
					MsgUpdatedAt:    1637747461,
					FirstUnreadAt:   1634701748,
				},
			},
			wantErr: false,
		},
		{
			name: "test001",
			args: args{
				ctx: ctx,
				req: &qmsg.GetChatSessionByIframeReq{
					GroupId:   71345,
					ExtUserId: "",
					RobotUid:  0,
					StrOpenId: "accCN8VrTCP2MpeI4QgwI4e",
				},
			},
			want: &qmsg.GetChatSessionByIframeRsp{
				Chat: &qmsg.ModelChat{
					Id:              7225840120,
					CreatedAt:       1632290775,
					UpdatedAt:       1637747462,
					CorpId:          2000772,
					AppId:           2000273,
					RobotUid:        5229318970,
					IsGroup:         true,
					MsgSeq:          2703,
					Msg:             ";ljaslkdj",
					ChatExtId:       71345,
					ChatMsgType:     1,
					SenderAccountId: 5418396,
					MsgUpdatedAt:    1637747461,
					FirstUnreadAt:   1634701748,
				},
			},
			wantErr: false,
		},
		{
			name: "test001",
			args: args{
				ctx: ctx,
				req: &qmsg.GetChatSessionByIframeReq{
					GroupId:   0,
					ExtUserId: "",
					RobotUid:  5229318970,
					StrOpenId: "",
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "test001",
			args: args{
				ctx: ctx,
				req: &qmsg.GetChatSessionByIframeReq{
					GroupId:   0,
					ExtUserId: "wmfd4MDAAA8ZNLJjO9jwrSYbRccjcFwA",
					RobotUid:  0,
					StrOpenId: "",
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "test001",
			args: args{
				ctx: ctx,
				req: &qmsg.GetChatSessionByIframeReq{
					GroupId:   0,
					ExtUserId: "",
					RobotUid:  0,
					StrOpenId: "accCN8VrTCP2MpeI4QgwI4e",
				},
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GetChatSessionByIframe(tt.args.ctx, tt.args.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetChatSessionByIframe() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetChatSessionByIframe() got = %v, want %v", got, tt.want)
			}
		})
	}
}

//
//func TestNewSeqGen(t *testing.T) {
//	s := NewSeqGen()
//
//	var w sync.WaitGroup
//	run := func(base int) {
//		defer w.Done()
//		for i := 0; i < 10; i++ {
//			for j := 0; j < 100; j++ {
//				_, err := s.GetNextSeq(nil, uint64(base+i))
//				if err != nil && err != ErrGenSeqTimeout {
//					log.Errorf("err:%v", err)
//				}
//			}
//		}
//	}
//
//	for i := 0; i < 10; i++ {
//		w.Add(1)
//		go run(10)
//	}
//
//	w.Wait()
//	time.Sleep(time.Minute * 5)
//	log.Warn("need release all")
//	time.Sleep(time.Minute)
//}
