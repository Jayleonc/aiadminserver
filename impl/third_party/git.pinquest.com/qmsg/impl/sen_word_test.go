package impl

import (
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/core"
	"testing"
)

func TestSenWordChecker_check(t *testing.T) {
	type fields struct {
		ctx     *rpc.Context
		corpId  uint32
		appId   uint32
		content string
		record  *SenWordRecord
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name: "test",
			fields: fields{
				ctx:     core.NewInternalRpcCtx(2000757, 2000255, 112),
				corpId:  2000757,
				appId:   2000255,
				content: "I'm ok",
				record: &SenWordRecord{
					robotUid:  862,
					uid:       30,
					chatExtId: 10,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewSenWordChecker(tt.fields.ctx, tt.fields.corpId, tt.fields.appId, tt.fields.content, tt.fields.record)
			if _, err := p.check(); (err != nil) != tt.wantErr {
				t.Errorf("check() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
