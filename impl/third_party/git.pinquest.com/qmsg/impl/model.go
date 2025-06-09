package impl

import (
	"fmt"
	"git.pinquest.cn/base/log"
	"git.pinquest.cn/qlb/brick/dbx"
	"git.pinquest.cn/qlb/brick/rpc"
	"git.pinquest.cn/qlb/brick/utils"
	"git.pinquest.cn/qlb/extrpkg/github.com/elliotchance/pie/pie"
	"git.pinquest.cn/qlb/qmsg"
	"git.pinquest.cn/qlb/quan"
	"time"
)

func (p *TMsgBox) WU(user *qmsg.ModelUser, whereCond ...interface{}) *dbx.Scope {
	return p.Where(map[string]interface{}{
		DbCorpId: user.CorpId,
		DbAppId:  user.AppId,
		DbUid:    user.Id,
	}).Where(whereCond...)
}
func (p *TMsgBox) W(corpId, appId uint32, uid uint64, whereCond ...interface{}) *dbx.Scope {
	return p.Where(map[string]interface{}{
		DbCorpId: corpId,
		DbAppId:  appId,
		DbUid:    uid,
	}).Where(whereCond...)
}
func (p *TUser) get(ctx *rpc.Context, corpId, appId uint32, uid uint64) (*qmsg.ModelUser, error) {
	var u qmsg.ModelUser
	err := p.Where(map[string]interface{}{
		DbCorpId: corpId,
		DbAppId:  appId,
		DbId:     uid,
	}).First(ctx, &u)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	return &u, nil
}
func (p *TUserRobot) GetRobotList(ctx *rpc.Context, corpId, appId uint32, uid uint64) (qmsg.UserRobotList, error) {
	var list []*qmsg.ModelUserRobot
	err := p.Where(map[string]interface{}{
		DbCorpId: corpId,
		DbAppId:  appId,
		DbUid:    uid,
	}).Select().Find(ctx, &list)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	return list, nil
}
func (p *TUserRobot) GetAllRobotList(ctx *rpc.Context, corpId, appId uint32) ([]*qmsg.ModelUserRobot, error) {
	var list []*qmsg.ModelUserRobot
	err := p.Where(map[string]interface{}{
		DbCorpId: corpId,
		DbAppId:  appId,
	}).Find(ctx, &list)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	return list, nil
}
func (p *TUserRobot) GetRobotUidList(ctx *rpc.Context, corpId, appId uint32, uid uint64) ([]uint64, error) {
	var list []*qmsg.ModelUserRobot
	err := p.Where(map[string]interface{}{
		DbCorpId: corpId,
		DbAppId:  appId,
		DbUid:    uid,
		DbRobotType + " IN": []uint32{
			0,
			uint32(qmsg.RobotType_RobotTypePlatform),
		},
	}).Find(ctx, &list)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	return utils.PluckUint64(list, "RobotUid"), nil
}
func (p *TUserRobot) GetHostAccountUidList(ctx *rpc.Context, corpId, appId uint32, uid uint64) ([]uint64, error) {
	var list []*qmsg.ModelUserRobot
	err := p.Where(map[string]interface{}{
		DbCorpId:    corpId,
		DbAppId:     appId,
		DbUid:       uid,
		DbRobotType: uint32(qmsg.RobotType_RobotTypeHostAccount),
	}).Find(ctx, &list)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	return utils.PluckUint64(list, "RobotUid"), nil
}
func (p *TSenOptRecord) Get(ctx *rpc.Context, corpId, appId uint32, id uint64) (*qmsg.ModelSensitiveOptRecord, error) {
	var record qmsg.ModelSensitiveOptRecord
	err := p.Where(map[string]interface{}{
		DbCorpId: corpId,
		DbAppId:  appId,
		DbId:     id,
	}).First(ctx, &record)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	return &record, nil
}

type TModel struct {
	*dbx.Model
}
type TMsgBox struct {
	TModel
}
type TCommonModel struct {
	TModel
}
type TMsgQualityStatRule struct {
	TModel
}

var MsgQualityStatRule = &TMsgQualityStatRule{
	TModel: TModel{
		Model: dbx.NewModel(&dbx.ModelConfig{
			Type:            &qmsg.ModelMsgQualityStatRule{},
			NotFoundErrCode: qmsg.ErrMsgQualityStatRuleNotFound,
		}),
	},
}

var MsgBox = &TMsgBox{
	TModel: TModel{
		Model: dbx.NewModel(&dbx.ModelConfig{
			Type:            &qmsg.ModelMsgBox{},
			NotFoundErrCode: qmsg.ErrMsgBoxNotFound,
		}),
	},
}

var ExtSensitiveOptRecord = dbx.NewModel(&dbx.ModelConfig{
	Type:            &qmsg.ModelExtSensitiveOptRecord{},
	NotFoundErrCode: qmsg.ErrExtSensitiveOptRecordNotFound,
})

func (_ *TMsgBox) getByUidCliMsgId(ctx *rpc.Context, corpId, appId uint32, uid uint64, cliMsgId string, msg *qmsg.ModelMsgBox) error {
	err := ChoiceMsgBoxDb(MsgBox.WhereCorpApp(corpId, appId).Where(map[string]interface{}{
		DbUid:      uid,
		DbCliMsgId: cliMsgId,
	}), corpId).First(ctx, &msg)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	return nil
}

func (_ *TMsgBox) getBySendRecord(ctx *rpc.Context, corpId, appId uint32, uid uint64, ycMsgId string) (*qmsg.ModelMsgBox, error) {
	// 走这里可能是因为工作台发，手机端撤回
	var record qmsg.ModelSendRecord
	err := SendRecord.WhereCorpApp(corpId, appId).Where(map[string]interface{}{
		DbRobotUid: uid,
		DbYcMsgId:  ycMsgId,
	}).First(ctx, &record)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	var msgBox qmsg.ModelMsgBox
	err = ChoiceMsgBoxDb(MsgBox.W(corpId, appId, uid), corpId).Where(DbCliMsgId, record.CliMsgId).First(ctx, &msgBox)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	return &msgBox, nil
}

type TUser struct {
	TModel
}

var User = &TUser{
	TModel: TModel{
		Model: dbx.NewModel(&dbx.ModelConfig{
			Type:            &qmsg.ModelUser{},
			NotFoundErrCode: qmsg.ErrUserNotFound,
		}),
	},
}

func (_ *TUser) getUserById(ctx *rpc.Context, corpId, appId uint32, id uint64) (*qmsg.ModelUser, error) {
	var user qmsg.ModelUser
	err := User.WhereCorpApp(corpId, appId).Where(DbId, id).First(ctx, &user)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	return &user, err
}

func (_ *TUser) updateUser(ctx *rpc.Context, corpId, appId uint32, id uint64, updateMap map[string]interface{}) (*dbx.UpdateResult, error) {
	res, err := User.WhereCorpApp(corpId, appId).Where(DbId, id).Update(ctx, updateMap)
	if err != nil {
		log.Errorf("err:%v", err)
		return &dbx.UpdateResult{}, err
	}

	return &res, err
}

type TUserRobot struct {
	TModel
}

var UserRobot = &TUserRobot{
	TModel: TModel{
		Model: dbx.NewModel(&dbx.ModelConfig{
			Type:            &qmsg.ModelUserRobot{},
			NotFoundErrCode: qmsg.ErrUserRobotNotFound,
		}),
	},
}

func (_ *TUserRobot) batchCreate(ctx *rpc.Context, corpId, appId uint32, list []*qmsg.ModelUserRobot, robotUidList []uint64) error {
	err := UserRobot.BatchCreate(ctx, list)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	assignChatList, err := AssignChat.getListByRobotUidList(ctx, corpId, appId, robotUidList)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	if len(assignChatList) > 0 {
		uidList := utils.PluckUint64(assignChatList, "Uid")

		err = deleteAssignChatByRobotUid(ctx, corpId, appId, robotUidList, qmsg.ModelAssignChat_EndSceneRobotBind)
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}

		for _, uid := range uidList {
			err = pushWsMsg(ctx, corpId, appId, uid, []*qmsg.WsMsgWrapper{
				{
					MsgType: uint32(qmsg.WsMsgWrapper_MsgTypeRobotAssign),
				},
			})
			if err != nil {
				log.Errorf("err:%v", err)
				return err
			}
		}
	}

	return err
}

func (_ *TUserRobot) getByFull(ctx *rpc.Context, corpId, appId uint32, uid, robotUid uint64) (*qmsg.ModelUserRobot, error) {
	var userRobot qmsg.ModelUserRobot
	err := UserRobot.WhereCorpApp(corpId, appId).Where(DbUid, uid).Where(DbRobotUid, robotUid).First(ctx, &userRobot)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	return &userRobot, nil
}

func (_ *TUserRobot) getListByUid(ctx *rpc.Context, corpId, appId uint32, uid uint64) ([]*qmsg.ModelUserRobot, error) {
	var userRobotList []*qmsg.ModelUserRobot
	err := UserRobot.WhereCorpApp(corpId, appId).Where(DbUid, uid).Find(ctx, &userRobotList)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	return userRobotList, nil
}

func (_ *TUserRobot) getListByRobotUid(ctx *rpc.Context, corpId, appId uint32, robotUid uint64) ([]*qmsg.ModelUserRobot, error) {
	var userRobotList []*qmsg.ModelUserRobot
	err := UserRobot.WhereCorpApp(corpId, appId).Where(DbRobotUid, robotUid).Find(ctx, &userRobotList)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	return userRobotList, nil
}

func (_ *TUserRobot) getListByRobotUidList(ctx *rpc.Context, corpId, appId uint32, uid uint64, robotUidList []uint64) ([]*qmsg.ModelUserRobot, error) {
	var userRobotList []*qmsg.ModelUserRobot
	err := UserRobot.WhereCorpApp(corpId, appId).Where(DbUid, uid).WhereIn(DbRobotUid, robotUidList).Find(ctx, &userRobotList)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	return userRobotList, nil
}

type TSeq struct {
	TModel
}

var Seq = &TSeq{
	TModel: TModel{
		Model: dbx.NewModel(&dbx.ModelConfig{
			Type:            &qmsg.ModelSeq{},
			NotFoundErrCode: qmsg.ErrSeqNotFound,
		}),
	},
}

type TChat struct {
	TModel
}

var Chat = &TChat{
	TModel: TModel{
		Model: dbx.NewModel(&dbx.ModelConfig{
			Type:            &qmsg.ModelChat{},
			NotFoundErrCode: qmsg.ErrChatNotFound,
		}),
	},
}

var LastChat = dbx.NewModel(&dbx.ModelConfig{
	Type: &qmsg.ModelLastChat{},
})

func (_ *TChat) get(ctx *rpc.Context, corpId, appId uint32, id uint64) (*qmsg.ModelChat, error) {
	var chat qmsg.ModelChat
	err := Chat.WhereCorpApp(corpId, appId).Where(DbId, id).First(ctx, &chat)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	return &chat, nil
}

func (_ *TChat) getByRobotAndExt(ctx *rpc.Context, corpId, appId uint32, robotUid, chatExtId uint64, isGroup bool) (*qmsg.ModelChat, error) {
	var chat qmsg.ModelChat
	err := Chat.WhereCorpApp(corpId, appId).Where(DbRobotUid, robotUid).Where(DbIsGroup, isGroup).Where(DbChatExtId, chatExtId).First(ctx, &chat)
	if err != nil {
		if !Chat.IsNotFoundErr(err) {
			log.Errorf("err:%v", err)
		}
		return nil, err
	}

	return &chat, nil
}

func (_ *TChat) getListById(ctx *rpc.Context, corpId, appId uint32, idList []uint64) ([]*qmsg.ModelChat, error) {
	var chatList []*qmsg.ModelChat
	err := Chat.WhereCorpApp(corpId, appId).WhereIn(DbId, idList).Find(ctx, &chatList)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	return chatList, nil
}

func (_ *TChat) updateFromFollow(ctx *rpc.Context, corpId, appId uint32, chat *qmsg.ModelChat, follow *quan.ModelExtContactFollow) (*dbx.UpdateResult, error) {
	if chat == nil {
		return &dbx.UpdateResult{}, nil
	}

	updateChatFormFollow(chat, follow)

	res, err := Chat.WhereCorpApp(corpId, appId).Where(DbId, chat.Id).Update(ctx, map[string]interface{}{
		DbDetail: chat.Detail,
	})
	if err != nil {
		log.Errorf("err:%v", err)
		return &dbx.UpdateResult{}, err
	}

	return &res, nil
}

func (_ *TChat) updateMap(ctx *rpc.Context, corpId, appId uint32, id uint64, updateMap map[string]interface{}) (*dbx.UpdateResult, error) {
	res, err := Chat.WhereCorpApp(corpId, appId).Where(DbId, id).Update(ctx, updateMap)
	if err != nil {
		log.Errorf("err:%v", err)
		return &dbx.UpdateResult{}, err
	}

	return &res, nil
}

type TSenWordRule struct {
	TModel
}

var SenWordRule = &TSenWordRule{
	TModel: TModel{
		Model: dbx.NewModel(&dbx.ModelConfig{
			Type:            &qmsg.ModelSensitiveWordRule{},
			NotFoundErrCode: qmsg.ErrSensitiveWordRuleNotFound,
		}),
	},
}

type TSenOptRecord struct {
	TModel
}

var SenOptRecord = &TSenOptRecord{
	TModel: TModel{
		Model: dbx.NewModel(&dbx.ModelConfig{
			Type:            &qmsg.ModelSensitiveOptRecord{},
			NotFoundErrCode: qmsg.ErrSensitiveOptRecordNotFound,
		}),
	},
}

type TSessionConfig struct {
	TModel
}

var SessionConfig = &TSessionConfig{
	TModel: TModel{
		Model: dbx.NewModel(&dbx.ModelConfig{
			Type:            &qmsg.ModelSessionConfig{},
			NotFoundErrCode: qmsg.ErrSessionConfigNotFound,
		}),
	},
}

type TSidebarSetting struct {
	TModel
}

var SidebarSetting = &TSidebarSetting{
	TModel: TModel{
		Model: dbx.NewModel(&dbx.ModelConfig{
			Type: &qmsg.ModelSidebarSetting{},
		}),
	},
}

func (_ *TSidebarSetting) get(ctx *rpc.Context, corpId, appId uint32, setting *qmsg.ModelSidebarSetting) (*dbx.FirstOrCreateResult, error) {
	res, err := SidebarSetting.FirstOrCreate(ctx, map[string]interface{}{
		DbCorpId: corpId,
		DbAppId:  appId,
	}, map[string]interface{}{}, &setting)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	return &res, nil
}

func (_ *TSidebarSetting) set(ctx *rpc.Context, corpId, appId uint32, privateModuleIdList, groupModuleIdList []uint32, setting *qmsg.ModelSidebarSetting) (*dbx.UpdateOrCreateResult, error) {
	res, err := SidebarSetting.UpdateOrCreate(ctx, map[string]interface{}{
		DbCorpId: corpId,
		DbAppId:  appId,
	}, map[string]interface{}{
		DbPrivateExceptModuleList: privateModuleIdList,
		DbGroupExceptModuleList:   groupModuleIdList,
	}, &setting)
	if err != nil {
		log.Errorf("err:%v", err)
		return &dbx.UpdateOrCreateResult{}, err
	}
	return &res, nil
}

type TIframeSetting struct {
	TModel
}

var IframeSetting = &TIframeSetting{
	TModel: TModel{
		Model: dbx.NewModel(&dbx.ModelConfig{
			Type: &qmsg.ModelIframeSetting{},
		}),
	},
}

func (_ *TIframeSetting) getList(ctx *rpc.Context, corpId, appId uint32) ([]*qmsg.ModelIframeSetting, error) {
	var list []*qmsg.ModelIframeSetting
	err := IframeSetting.WhereCorpApp(corpId, appId).Find(ctx, &list)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}
	return list, nil
}

type TAssignChat struct {
	TModel
}

var AssignChat = &TAssignChat{
	TModel: TModel{
		Model: dbx.NewModel(&dbx.ModelConfig{
			Type:            &qmsg.ModelAssignChat{},
			NotFoundErrCode: qmsg.ErrAssignChatNotFound,
		}),
	},
}

func (_ *TAssignChat) create(ctx *rpc.Context, corpId, appId uint32, uid, originUid, robotUid, chatExtId uint64, isGroup bool, chat *qmsg.ModelChat, startScene qmsg.ModelAssignChat_StartScene) (*qmsg.ModelAssignChat, error) {
	if chat == nil {
		dbChat, err := Chat.getByRobotAndExt(ctx, corpId, appId, robotUid, chatExtId, isGroup)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}

		chat = dbChat
	}
	assignChatType := qmsg.ModelAssignChat_AssignChatTypeNil
	if !isGroup {
		// 先设置为旧好友会话,接收到成为好友的消息回调时(qmsg.MsgType.MsgTypeExtContactAdded),修改为新好友会话
		assignChatType = qmsg.ModelAssignChat_AssignChatTypeOldFriend
	}
	attrs := map[string]interface{}{
		DbCorpId: corpId,
		DbAppId:  appId,
		DbCid:    chat.Id,
	}
	values := map[string]interface{}{
		DbUid:            uid,
		DbRobotUid:       robotUid,
		DbIsGroup:        isGroup,
		DbChatExtId:      chatExtId,
		DbOriginUid:      originUid,
		DbStartScene:     uint32(startScene),
		DbAssignChatType: uint32(assignChatType),
	}
	var assignChat qmsg.ModelAssignChat
	_, err := AssignChat.FirstOrCreate(ctx, attrs, values, &assignChat)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	// 分配会话，将最近联系人删除
	if assignChat.Uid != 0 {
		_, err := LastContactChat.delete(ctx, corpId, appId, uid, chat.Id)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
	}

	// 非群组会话，更新最近联系时间
	if !assignChat.IsGroup {
		err = updateContactSendAt(ctx, corpId, appId, uint32(time.Now().Unix()), assignChat.Cid)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
	}

	pubToAssignChatChangedMq(ctx, corpId, appId, []uint64{assignChat.Id})
	return &assignChat, nil
}

func (_ *TAssignChat) getByCid(ctx *rpc.Context, corpId, appId uint32, cid uint64) (*qmsg.ModelAssignChat, error) {
	var assignChat qmsg.ModelAssignChat
	err := AssignChat.WhereCorpApp(corpId, appId).Where(DbCid, cid).First(ctx, &assignChat)
	if err != nil {
		if !AssignChat.IsNotFoundErr(err) {
			log.Errorf("err:%v", err)
		}
		return nil, err
	}

	return &assignChat, nil
}

func (_ *TAssignChat) get(ctx *rpc.Context, corpId, appId uint32, id uint64) (*qmsg.ModelAssignChat, error) {
	var assignChat qmsg.ModelAssignChat
	err := AssignChat.WhereCorpApp(corpId, appId).Where(DbId, id).First(ctx, &assignChat)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	return &assignChat, nil
}

func (_ *TAssignChat) getWithTrash(ctx *rpc.Context, corpId, appId uint32, id uint64) (*qmsg.ModelAssignChat, error) {
	var assignChat qmsg.ModelAssignChat
	err := AssignChat.WithTrash().WhereCorpApp(corpId, appId).Where(DbId, id).First(ctx, &assignChat)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	return &assignChat, nil
}

func (_ *TAssignChat) getListByCid(ctx *rpc.Context, corpId, appId uint32, cidList []uint64) ([]*qmsg.ModelAssignChat, error) {
	var assignChatList []*qmsg.ModelAssignChat
	err := AssignChat.WhereCorpApp(corpId, appId).WhereIn(DbCid, cidList).Find(ctx, &assignChatList)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	return assignChatList, nil
}

func (_ *TAssignChat) getListWithTrash(ctx *rpc.Context, corpId, appId uint32,
	idList []uint64) ([]*qmsg.ModelAssignChat, error) {
	var assignChatList []*qmsg.ModelAssignChat
	err := AssignChat.WithTrash().WhereCorpApp(corpId, appId).WhereIn(DbId, idList).Find(ctx, &assignChatList)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	return assignChatList, nil
}

func (_ *TAssignChat) getByFull(ctx *rpc.Context, corpId, appId uint32, robotUid, chatExtId uint64, isGroup bool) (*qmsg.ModelAssignChat, error) {
	var assignChat qmsg.ModelAssignChat
	err := AssignChat.WhereCorpApp(corpId, appId).Where(map[string]interface{}{
		DbRobotUid:  robotUid,
		DbIsGroup:   isGroup,
		DbChatExtId: chatExtId,
	}).First(ctx, &assignChat)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	return &assignChat, nil
}

func (_ *TAssignChat) getListByUidListWithGroup(ctx *rpc.Context, corpId, appId uint32, uid uint64, robotUidList []uint64) ([]*qmsg.ModelAssignChat, error) {
	var assignChatList []*qmsg.ModelAssignChat
	err := AssignChat.WhereCorpApp(corpId, appId).Where(DbUid, uid).WhereIn(DbRobotUid, robotUidList).Group(DbRobotUid).Find(ctx, &assignChatList)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	return assignChatList, nil
}

func (_ *TAssignChat) getListByUid(ctx *rpc.Context, corpId, appId uint32, uid uint64) ([]*qmsg.ModelAssignChat, error) {
	var assignChatList []*qmsg.ModelAssignChat
	err := AssignChat.WhereCorpApp(corpId, appId).Where(DbUid, uid).Find(ctx, &assignChatList)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	return assignChatList, nil
}

func (_ *TAssignChat) getListByRobotUidList(ctx *rpc.Context, corpId, appId uint32, robotUidList []uint64) ([]*qmsg.ModelAssignChat, error) {
	var assignChatList []*qmsg.ModelAssignChat
	err := AssignChat.WhereCorpApp(corpId, appId).WhereIn(DbRobotUid, robotUidList).Find(ctx, &assignChatList)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	return assignChatList, nil
}

// 关闭会话都在这统一处理
func (_ *TAssignChat) setClosed(ctx *rpc.Context, corpId, appId uint32, updateParams map[string]interface{}, assignIdList ...uint64) error {
	if len(assignIdList) == 0 {
		return nil
	}
	updateParams[DbDeletedAt] = uint32(time.Now().Unix())

	pie.Uint64s(assignIdList).Chunk(500, func(chunk pie.Uint64s) bool {
		_, err := AssignChat.WhereCorpApp(corpId, appId).WhereIn(DbId, chunk).Update(ctx, updateParams)
		if err != nil {
			log.Errorf("err:%v", err)
			return true
		}
		pubToAssignChatChangedMq(ctx, corpId, appId, chunk)

		return false
	})

	return nil
}

// 分配待分配的会话都在这统一处理
func (_ *TAssignChat) assignUnAssignedChat(ctx *rpc.Context, corpId, appId uint32, uid uint64, startScene qmsg.ModelAssignChat_StartScene, assignIdList ...uint64) (uint64, error) {
	if len(assignIdList) == 0 {
		return 0, nil
	}
	var rowsAffected uint64
	var outErr error
	pie.Uint64s(assignIdList).Chunk(500, func(chunk pie.Uint64s) bool {
		res, err := AssignChat.WhereCorpApp(corpId, appId).Where(DbUid, 0).WhereIn(DbId, chunk).Update(ctx, map[string]interface{}{
			DbUid:        uid,
			DbStartScene: startScene,
		})
		if err != nil {
			log.Errorf("err:%v", err)
			outErr = err
			return true
		}

		var assignChatList []*qmsg.ModelAssignChat
		err = AssignChat.WhereCorpApp(corpId, appId).WhereIn(DbId, chunk).Find(ctx, &assignChatList)
		if err != nil {
			log.Errorf("err:%v", err)
			outErr = err
			return true
		}

		if len(assignChatList) > 0 {
			chatIdList := utils.PluckUint64(assignChatList, "Cid")
			err = batchUpdateContactSendAt(ctx, corpId, appId, uint32(time.Now().Unix()), chatIdList)
			if err != nil {
				log.Errorf("err:%v", err)
				outErr = err
				return true
			}

			// 会话会被分配，删除最近联系人记录
			_, err = LastContactChat.deleteByCidList(ctx, corpId, appId, uid, chatIdList)
			if err != nil {
				log.Errorf("err:%v", err)
				outErr = err
				return true
			}
		}

		pubToAssignChatChangedMq(ctx, corpId, appId, chunk)
		rowsAffected += res.RowsAffected
		return false
	})

	return rowsAffected, outErr
}

func (_ *TAssignChat) assignChatListByUid(ctx *rpc.Context, corpId, appId uint32, uid uint64) (uint64, error) {
	config, err := getSessionConfig(ctx, corpId, appId)
	if err != nil {
		log.Errorf("err:%v", err)
		return 0, err
	}

	if config.AssignSetting.OnlyOnline {
		user, err := User.get(ctx, corpId, appId, uid)
		if err != nil {
			log.Errorf("err:%v", err)
			return 0, err
		}

		if !user.IsOnline {
			return 0, err
		}
	}

	count, err := AssignChat.WhereCorpApp(corpId, appId).Where(DbUid, uid).Count(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return 0, err
	}

	if count >= config.AssignSetting.MaxChatNum {
		return 0, nil
	}

	needAssignCount := config.AssignSetting.MaxChatNum - count

	var assignChatList []*qmsg.ModelAssignChat
	err = AssignChat.WhereCorpApp(corpId, appId).Where(DbUid, 0).SetLimit(needAssignCount).Find(ctx, &assignChatList)
	if err != nil {
		log.Errorf("err:%v", err)
		return 0, err
	}

	assignIdList := utils.PluckUint64(assignChatList, "Id")

	rowsAffected, err := AssignChat.assignUnAssignedChat(ctx, corpId, appId, uid, qmsg.ModelAssignChat_StartSceneSys, assignIdList...)
	if err != nil {
		log.Errorf("err:%v", err)
		return 0, err
	}

	var wrapperList []*qmsg.WsMsgWrapper
	for _, assignChat := range assignChatList {
		chat, err := Chat.get(ctx, corpId, appId, assignChat.Cid)
		if err != nil {
			log.Errorf("err:%v", err)
			return 0, err
		}

		wrapperList = append(wrapperList, &qmsg.WsMsgWrapper{
			MsgType:    uint32(qmsg.WsMsgWrapper_MsgTypeAssignChat),
			AssignChat: assignChat,
			Chat:       chat,
		})
	}
	err = pushWsMsg(ctx, corpId, appId, uid, wrapperList)
	if err != nil {
		log.Errorf("err:%v", err)
		return 0, err
	}
	return rowsAffected, nil
}

func (_ *TAssignChat) countWaiting(ctx *rpc.Context, corpId, appId uint32) (uint32, error) {
	c, err := AssignChat.WhereCorpApp(corpId, appId).Where(DbUid, 0).Count(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return 0, err
	}

	info := assignInfo{Count: c}
	err = s.RedisGroup.SetJson(fmt.Sprintf(assignInfoPrefix, corpId, appId), info, time.Minute)
	if err != nil {
		log.Errorf("err:%v", err)
		return c, err
	}

	return c, nil
}

var RobotSingleChatStat = &TCommonModel{
	TModel: TModel{
		Model: dbx.NewModel(&dbx.ModelConfig{
			Type: &qmsg.ModelRobotSingleChatStat{},
		}),
	},
}

var ServiceSingleChatStat = &TCommonModel{
	TModel: TModel{
		Model: dbx.NewModel(&dbx.ModelConfig{
			Type: &qmsg.ModelServiceSingleChatStat{},
		}),
	},
}

var SingleChatStat = &TCommonModel{
	TModel: TModel{
		Model: dbx.NewModel(&dbx.ModelConfig{
			Type: &qmsg.ModelSingleChatStat{},
		}),
	},
}

type TOperatorLog struct {
	TModel
}

var OperatorLog = &TOperatorLog{
	TModel: TModel{
		Model: dbx.NewModel(&dbx.ModelConfig{
			Type:            &qmsg.ModelOperatorLog{},
			NotFoundErrCode: qmsg.ErrOperatorLogNotFound,
		}),
	},
}

func (_ *TOperatorLog) create(ctx *rpc.Context, corpId, appId uint32, uid uint64, logType qmsg.UserLog_LogType) (*qmsg.ModelOperatorLog, error) {
	realIp := ctx.GetReqHeader("X-Forwarded-For")
	if realIp == "" {
		realIp = ctx.GetReqHeader("X-Real-IP")
	}
	deviceSid := ctx.GetReqHeader("X-Device-Sid")
	operatorLog := &qmsg.ModelOperatorLog{
		CorpId:    corpId,
		AppId:     appId,
		Uid:       uid,
		LogType:   uint32(logType),
		RealIp:    realIp,
		DeviceSid: deviceSid,
	}
	err := OperatorLog.Create(ctx, operatorLog)
	if err != nil {
		//log.Error("err:%v", err)
		return nil, err
	}

	return operatorLog, nil
}

type TTopChat struct {
	TModel
}

var TopChat = &TTopChat{
	TModel: TModel{
		Model: dbx.NewModel(&dbx.ModelConfig{
			Type:            &qmsg.ModelTopChat{},
			NotFoundErrCode: qmsg.ErrTopChatNotFound,
		}),
	},
}

func (_ *TTopChat) deleteByUidList(ctx *rpc.Context, corpId, appId uint32, uidList []uint64) (dbx.DeleteResult, error) {
	res, err := TopChat.WhereCorpApp(corpId, appId).WhereIn(DbUid, uidList).Delete(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return res, err
	}

	return res, nil
}

func (_ *TTopChat) deleteByCid(ctx *rpc.Context, corpId, appId uint32, uid, cid uint64) (dbx.DeleteResult, error) {
	res, err := TopChat.WhereCorpApp(corpId, appId).Where(DbUid, uid).Where(DbCid, cid).Delete(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return res, err
	}

	return res, nil
}

func (_ *TTopChat) deleteByUidCidList(ctx *rpc.Context, corpId, appId uint32, uid uint64, cidList []uint64) (dbx.DeleteResult, error) {
	res, err := TopChat.WhereCorpApp(corpId, appId).Where(DbUid, uid).WhereIn(DbCid, cidList).Delete(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return res, err
	}

	return res, nil
}

func (_ *TTopChat) deleteByCidList(ctx *rpc.Context, corpId, appId uint32, cidList []uint64) (dbx.DeleteResult, error) {
	res, err := TopChat.WhereCorpApp(corpId, appId).WhereIn(DbCid, cidList).Delete(ctx)
	if err != nil {
		log.Errorf("err:%v", err)
		return res, err
	}

	return res, nil
}

type TAtRecord struct {
	TModel
}

var AtRecord = &TAtRecord{
	TModel: TModel{
		Model: dbx.NewModel(&dbx.ModelConfig{
			Type:            &qmsg.ModelAtRecord{},
			NotFoundErrCode: qmsg.ErrAtRecordNotFound,
		}),
	},
}

type TSafeConfig struct {
	TModel
}

var SafeConfig = &TSafeConfig{
	TModel: TModel{
		Model: dbx.NewModel(&dbx.ModelConfig{
			Type:            &qmsg.ModelSafeConfig{},
			NotFoundErrCode: qmsg.ErrSafeConfigNotFound,
		}),
	},
}

type TSendRecord struct {
	TModel
}

var SendRecord = &TSendRecord{
	TModel: TModel{
		Model: dbx.NewModel(&dbx.ModelConfig{
			Type:            &qmsg.ModelSendRecord{},
			NotFoundErrCode: qmsg.ErrSendRecordNotFound,
		}),
	},
}

func (_ *TSendRecord) updateAndCheckDelay(ctx *rpc.Context, corpId, appId uint32, whereMap, updateMap map[string]interface{}) error {
	var record qmsg.ModelSendRecord
	err := SendRecord.Where(whereMap).First(ctx, &record)
	if err != nil {
		if SendRecord.IsNotFoundErr(err) {
			return nil
		}
		log.Errorf("err:%v", err)
		return err
	}

	_, err = SendRecord.WhereCorpApp(corpId, appId).Where(DbId, record.Id).Update(ctx, updateMap)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	now := uint32(time.Now().Unix())

	LogStream.LogStreamSubType(ctx, SendDelaySubType, &SendDelayStream{
		CorpId:   record.CorpId,
		RobotUid: record.RobotUid,
		Delay:    now - record.CreatedAt,
	})

	return nil
}

type TOfficeHour struct {
	TModel
}

var OfficeHour = &TOfficeHour{
	TModel: TModel{
		Model: dbx.NewModel(&dbx.ModelConfig{
			Type:            &qmsg.ModelOfficeHour{},
			NotFoundErrCode: qmsg.ErrOfficeHourNotFound,
		}),
	},
}

type TUserOfficeHour struct {
	TModel
}

var UserOfficeHour = &TUserOfficeHour{
	TModel: TModel{
		Model: dbx.NewModel(&dbx.ModelConfig{
			Type:            &qmsg.ModelUserOfficeHour{},
			NotFoundErrCode: qmsg.ErrUserOfficeHourNotFound,
		}),
	},
}

type TLastContactChat struct {
	TModel
}

var LastContactChat = &TLastContactChat{
	TModel: TModel{
		Model: dbx.NewModel(&dbx.ModelConfig{
			Type:            &qmsg.ModelLastContactChat{},
			NotFoundErrCode: qmsg.ErrLastContactChatNotFound,
		}),
	},
}

func (p *TLastContactChat) IsNotFoundErr(err error) bool {
	return err == p.GetNotFoundErr()
}

type TSidebarTabSort struct {
	TModel
}

var SidebarTabSort = &TSidebarTabSort{
	TModel: TModel{
		Model: dbx.NewModel(&dbx.ModelConfig{
			Type:            &qmsg.ModelSidebarTabSort{},
			NotFoundErrCode: qmsg.ErrSidebarTabSortNotFound,
		}),
	},
}

var SystemMsgRecord = dbx.NewModel(&dbx.ModelConfig{
	Type:            &qmsg.ModelSystemMsgRecord{},
	NotFoundErrCode: qmsg.ErrSystemMsgRecordNotFound,
})

type TResetChatUnreadRecord struct {
	TModel
}

var ResetChatUnreadRecord = &TResetChatUnreadRecord{
	TModel: TModel{
		Model: dbx.NewModel(&dbx.ModelConfig{
			Type: &qmsg.ModelResetChatUnreadRecord{},
		}),
	},
}

type TTransferLog struct {
	TModel
}

var TransferLog = &TTransferLog{
	TModel: TModel{
		Model: dbx.NewModel(&dbx.ModelConfig{
			Type: &qmsg.ModelTransferLog{},
		}),
	},
}

func (_ *TTransferLog) updateOrCreate(ctx *rpc.Context, req *qmsg.ModelTransferLog) (*qmsg.ModelTransferLog, error) {
	var record qmsg.ModelTransferLog
	_, err := TransferLog.UpdateOrCreate(ctx, map[string]interface{}{
		DbCorpId:         req.CorpId,
		DbAppId:          req.AppId,
		"assign_chat_id": req.AssignChatId,
	}, map[string]interface{}{
		DbUid:            req.Uid,
		DbChatId:         req.ChatId,
		DbCliMsgId:       req.CliMsgId,
		DbChatType:       req.ChatType,
		"assign_chat_id": req.AssignChatId,
		"mark":           req.Mark,
	}, &record)
	if err != nil {
		return nil, err
	}
	return &record, nil
}
