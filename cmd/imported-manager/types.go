// Copyright (c) 2026 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package main

import (
	"context"
	"time"

	"go.mau.fi/whatsmeow/baileysauth"
	"go.mau.fi/whatsmeow/importedclient"
	"go.mau.fi/whatsmeow/types"
)

// Config 控制 Manager 使用的持久化存储、代理和账号生命周期行为。
// 这个结构是 demo 级别的配置，正式业务可以根据自己的配置系统重新组织。
type Config struct {
	// StoreDialect 是 whatsmeow sqlstore 的数据库类型，这里通常使用 postgres。
	StoreDialect string
	// StoreDSN 是 SQL store 的连接串。
	// 导入账号、扫码账号、app-state key、prekey 等都会持久化到这个 store。
	StoreDSN string
	// StoreMaxIdleConns 控制数据库连接池里的空闲连接数。
	StoreMaxIdleConns int
	// StoreMaxOpenConns 控制数据库最大连接数。
	StoreMaxOpenConns int

	// ProxyScheme 用于把 ProxyHost/ProxyPort 这种拆分字段拼成 URL，默认 socks5。
	ProxyScheme string
	// ProxyURL 是默认 websocket 代理。
	// 如果单个 ImportRequest/LoginRequest 没有传代理，就使用这里的代理。
	ProxyURL string
	// MediaProxyURL 是媒体上传代理，可选。
	// 图片/视频上传到 WhatsApp CDN 时可以走单独代理，避免 websocket 代理不支持大流量上传。
	MediaProxyURL string
	// MediaDirect 为 true 时，媒体上传不走 ProxyURL。
	MediaDirect bool
	// TransportTimeout 作用于 websocket 和媒体 HTTP transport。
	TransportTimeout time.Duration

	// ConnectTimeout 是单次 Connect 等待认证成功的时间。
	// ConnectRetries/ConnectRetryDelay 用来处理代理抖动、TLS 超时等临时连接问题。
	ConnectTimeout    time.Duration
	ConnectRetries    int
	ConnectRetryDelay time.Duration

	// RecoverAppState 为 true 时，账号上线后会尝试恢复 app-state key。
	// app-state key 是添加联系人、删除聊天框、仅本机删除消息等 app-state 操作的基础。
	// RequireAppState 为 true 时，如果恢复失败则认为账号不可用；为 false 时只记录错误。
	RecoverAppState bool
	RequireAppState bool
	AppStateKeyWait time.Duration

	// AccountDBLock 使用 Postgres advisory lock 防止多个进程同时连接同一个设备。
	// 如果同一个 JID 被两个进程同时连接，WhatsApp 通常会让其中一个连接 stream_replaced。
	AccountDBLock bool
	// ImportOptions 透传给 baileysauth，用于控制 JSON 导入细节。
	ImportOptions *baileysauth.ImportOptions
}

// AccountStatus 是 Manager 内部使用的详细生命周期状态。
// 业务层一般不需要直接依赖它，建议只消费 BusinessAccountStatus + OfflineReason。
type AccountStatus string

const (
	AccountStatusImporting    AccountStatus = "importing"
	AccountStatusImported     AccountStatus = "imported"
	AccountStatusConnecting   AccountStatus = "connecting"
	AccountStatusOnline       AccountStatus = "online"
	AccountStatusDisconnected AccountStatus = "disconnected"
	AccountStatusLoggedOut    AccountStatus = "logged_out"
	AccountStatusDeleted      AccountStatus = "deleted_device"
	AccountStatusReplaced     AccountStatus = "stream_replaced"
	AccountStatusConnectFail  AccountStatus = "connect_failed"
	AccountStatusTemporaryBan AccountStatus = "temporary_ban"
	AccountStatusFailed       AccountStatus = "failed"
	AccountStatusClosed       AccountStatus = "closed"
)

type BusinessAccountStatus string

const (
	// BusinessStatusOnline 表示 websocket 已认证成功，账号当前可用。
	BusinessStatusOnline BusinessAccountStatus = "online"
	// BusinessStatusOffline 表示账号当前没有可用连接。
	BusinessStatusOffline BusinessAccountStatus = "offline"
	// BusinessStatusBanned 表示 WhatsApp 返回临时封禁/限制事件。
	BusinessStatusBanned BusinessAccountStatus = "banned"
)

// AccountDecision 是 demo 给业务层的处理建议。
// 它不是 WhatsApp 协议状态，而是根据 business status 和 offline reason 推导出来的业务动作。
type AccountDecision string

const (
	// AccountDecisionUsable 表示账号当前可用，或者只是业务主动关闭，后续仍可再次登录。
	AccountDecisionUsable AccountDecision = "usable"
	// AccountDecisionUnavailable 表示账号凭据/linked device 已失效，不建议继续自动重试。
	AccountDecisionUnavailable AccountDecision = "unavailable"
	// AccountDecisionRetryLater 表示更像网络、代理或临时连接问题，业务层可以稍后重试。
	AccountDecisionRetryLater AccountDecision = "retry_later"
	// AccountDecisionOccupied 表示同一设备可能在其他进程或机器上线，需要排查重复登录。
	AccountDecisionOccupied AccountDecision = "occupied"
	// AccountDecisionPause 表示账号触发风控或临时封禁，应暂停使用一段时间。
	AccountDecisionPause AccountDecision = "pause"
	// AccountDecisionUnknown 表示暂时无法给出明确建议，业务层应保留错误文本并人工排查。
	AccountDecisionUnknown AccountDecision = "unknown"
)

// OfflineReason 是给业务表和 API 使用的稳定离线原因。
// 它屏蔽了 whatsmeow 的底层事件类型，业务只需要根据这些字符串判断是否重试、换代理、换号或人工处理。
type OfflineReason string

const (
	// OfflineReasonNone 表示没有离线原因。
	// 通常只在账号 online，或账号刚创建但还没有发生离线事件时使用。
	OfflineReasonNone OfflineReason = ""
	// OfflineReasonDisconnected 表示 websocket 普通断开。
	// 这类情况可能是网络、代理、服务端主动断开或本地连接抖动，业务层一般可以重连。
	OfflineReasonDisconnected OfflineReason = "disconnected"
	// OfflineReasonLoggedOut 表示 WhatsApp 返回 logged out，常见于 401。
	// 一般说明这个 linked device 已经从手机端移除、被其他登录替换，或凭据失效，业务层应标记账号不可用。
	OfflineReasonLoggedOut OfflineReason = "logged_out"
	// OfflineReasonDeletedDevice 表示当前设备已被 WhatsApp 判定为 deleted device。
	// 和 logged_out 类似，通常需要业务层停止重试，等待重新提供账号或重新关联。
	OfflineReasonDeletedDevice OfflineReason = "deleted_device"
	// OfflineReasonStreamReplaced 表示同一个 linked device 在其他进程或机器上线，当前连接被替换。
	// 业务层通常应标记为 occupied/online_elsewhere，可稍后重试或检查是否有重复任务。
	OfflineReasonStreamReplaced OfflineReason = "stream_replaced"
	// OfflineReasonConnectFailed 表示连接阶段失败。
	// 常见原因包括代理不可用、TLS/WebSocket 握手超时、网络断开等，业务层可换代理或延迟重试。
	OfflineReasonConnectFailed OfflineReason = "connect_failed"
	// OfflineReasonClientOutdated 表示 WhatsApp 认为当前客户端版本过旧。
	// 业务层通常不能靠换代理解决，需要升级 whatsmeow/协议参数后再重试。
	OfflineReasonClientOutdated OfflineReason = "client_outdated"
	// OfflineReasonCATRefresh 表示客户端认证 token 刷新失败。
	// 可能是网络问题，也可能是账号状态异常；业务层可先重试，连续失败再人工排查。
	OfflineReasonCATRefresh OfflineReason = "cat_refresh_failed"
	// OfflineReasonTemporaryBan 表示 WhatsApp 返回临时封禁或风控限制。
	// 业务层应降低频率或暂停该账号，避免继续操作导致更严重限制。
	OfflineReasonTemporaryBan OfflineReason = "temporary_ban"
	// OfflineReasonClosed 表示业务主动关闭本地连接。
	// 例如调用 CancelAccount 或 Manager.Close，这不是账号异常，也不会删除数据库里的设备。
	OfflineReasonClosed OfflineReason = "closed"
	// OfflineReasonImportFailed 表示 JSON 导入或初始化阶段失败。
	// 常见于 JSON 格式不对、关键字段缺失、写入 store 失败等，业务层应记录导入失败原因。
	OfflineReasonImportFailed OfflineReason = "import_failed"
	// OfflineReasonUnknown 表示错误未能归类到已知原因。
	// 业务层应保留原始 Error 文本，方便后续补充分类。
	OfflineReasonUnknown OfflineReason = "unknown"
)

// ImportRequest 表示一次外部 Baileys JSON 账号导入请求。
// 调用方可以直接传 JSON 字节，不一定要把文件路径交给 Manager。
type ImportRequest struct {
	// ID 可以映射业务账号表主键。
	ID int64
	// Phone 是不带 "+" 的完整手机号。
	// 如果为空，会从 JSON 里的设备 JID 推导。
	Phone string
	// Label 只用于日志和 snapshot，方便批量测试时区分账号。
	Label string

	// CredsJSON 是推荐入口：业务层自己读取/解密/校验文件后，把 JSON 字节传进来。
	// Creds 是字符串形式，主要为了 demo 和 CLI 方便。
	CredsJSON []byte
	Creds     string

	// ProxyURL 可以直接传完整代理 URL。
	// 也可以传 ProxyHost/ProxyPort/ProxyUser/ProxyPass，由 BuildProxyURL 组装。
	ProxyURL  string
	ProxyHost string
	ProxyPort string
	ProxyUser string
	ProxyPass string

	ImportOptions *baileysauth.ImportOptions
}

// LoginRequest 表示直接登录已存在于 whatsmeow SQL store 的设备。
// 这种模式不需要再传 JSON，适合账号第一次导入成功后，服务重启再恢复上线。
type LoginRequest struct {
	ID    int64
	Phone string
	JID   string
	Label string

	// 这些代理字段只覆盖当前这个账号的连接代理。
	ProxyURL  string
	ProxyHost string
	ProxyPort string
	ProxyUser string
	ProxyPass string
}

// ImportResponse 是导入/登录成功后的最小返回值。
type ImportResponse struct {
	ID     int64         `json:"id"`
	Phone  string        `json:"phone"`
	JID    string        `json:"jid"`
	LID    string        `json:"lid"`
	Status AccountStatus `json:"status"`
}

// AccountStatusSnapshot 是账号当前状态的只读快照。
// 它可以返回给 API，也可以由业务层存入自己的账号状态表。
type AccountStatusSnapshot struct {
	ID             int64                 `json:"id"`
	Phone          string                `json:"phone"`
	Key            string                `json:"key"`
	Label          string                `json:"label"`
	JID            string                `json:"jid"`
	LID            string                `json:"lid"`
	Status         AccountStatus         `json:"status"`
	BusinessStatus BusinessAccountStatus `json:"business_status"`
	OfflineReason  OfflineReason         `json:"offline_reason"`
	Decision       AccountDecision       `json:"decision"`
	Error          string                `json:"error"`
	CreatedAt      time.Time             `json:"created_at"`
	UpdatedAt      time.Time             `json:"updated_at"`
	OnlineAt       time.Time             `json:"online_at"`
	ClosedAt       time.Time             `json:"closed_at"`

	AppStateKeyID      string `json:"app_state_key_id"`
	AppStateReady      bool   `json:"app_state_ready"`
	AppStateKeysBefore int    `json:"app_state_keys_before"`
	AppStateKeysAfter  int    `json:"app_state_keys_after"`
	AppStateError      string `json:"app_state_error"`
	ExistingDevice     bool   `json:"existing_device"`
	StateReset         bool   `json:"state_reset"`
	StateResetReason   string `json:"state_reset_reason"`
}

// ImportSuccessResult 会在账号成功上线后回调给业务层。
// import 模式和 login 模式都会走这个结果。
type ImportSuccessResult struct {
	ID                 int64
	Phone              string
	JID                string
	LID                string
	ExistingDevice     bool
	StateReset         bool
	StateResetReason   string
	AppStateKeysBefore int
	AppStateKeysAfter  int
	AppStateReady      bool
	Account            *ManagedAccount
}

// ImportFailureResult 会在账号导入或登录失败时回调给业务层。
// 例如 401、设备已删除、代理不可用、重复连接等。
type ImportFailureResult struct {
	ID    int64
	Phone string
	JID   string
	Error string
}

// BusinessStatusChange 是业务层最推荐消费的状态变化事件。
// 它把底层复杂事件统一成：
// - NewStatus：online/offline/banned；
// - NewOfflineReason：离线原因；
// - Error：底层错误文本。
// 业务层可以在这里更新账号表，而不用直接解析 whatsmeow events。
type BusinessStatusChange struct {
	ID       int64
	Phone    string
	JID      string
	LID      string
	Account  *ManagedAccount
	Snapshot AccountStatusSnapshot

	OldStatus        BusinessAccountStatus
	NewStatus        BusinessAccountStatus
	OldOfflineReason OfflineReason
	NewOfflineReason OfflineReason
	Decision         AccountDecision
	Error            string
}

// ImportSuccessHandler / ImportFailureHandler 用来让 demo 或业务层持久化导入/登录结果。
type ImportSuccessHandler func(ctx context.Context, result ImportSuccessResult) error
type ImportFailureHandler func(ctx context.Context, result ImportFailureResult) error

// AccountEventHandler 暴露原始 whatsmeow 事件，主要用于调试或特殊业务逻辑。
// 普通账号状态同步建议使用 BusinessStatusHandler。
type AccountEventHandler func(ctx context.Context, account *ManagedAccount, evt any) error

// BusinessStatusHandler 是业务状态变化回调。
// 正式服务里通常在这里更新自己的账号状态、离线原因、错误信息和最后在线时间。
type BusinessStatusHandler func(ctx context.Context, change BusinessStatusChange) error

// ManagedAccount 保存一个活跃账号在当前 Manager 进程里的生命周期状态。
// 注意：它是内存对象，不等同于业务账号表；业务层是否移除、重试、标记禁用，需要自己决定。
type ManagedAccount struct {
	Key   string
	ID    int64
	Phone string
	Label string

	Status         AccountStatus
	BusinessStatus BusinessAccountStatus
	OfflineReason  OfflineReason
	Error          string

	JID types.JID
	LID types.JID

	// App-state 字段用于判断联系人、聊天框、消息本机删除等 app-state 能力是否可用。
	// 健康账号通常应该至少有一个 app-state key 持久化到数据库。
	AppStateKeyID      string
	AppStateKeysBefore int
	AppStateKeysAfter  int
	AppStateError      string
	ExistingDevice     bool
	StateReset         bool
	StateResetReason   string

	Account *importedclient.Account
	Cancel  context.CancelFunc

	// handlerID 用于关闭账号时注销 whatsmeow 事件处理器。
	// terminal 用于在连接或 app-state 恢复过程中收到 401/stream_replaced 等致命事件时，打断等待流程。
	handlerID uint32
	lock      *accountLock
	terminal  chan error

	CreatedAt time.Time
	UpdatedAt time.Time
	OnlineAt  time.Time
	ClosedAt  time.Time
}

func (acc *ManagedAccount) Snapshot() AccountStatusSnapshot {
	if acc == nil {
		return AccountStatusSnapshot{}
	}
	return AccountStatusSnapshot{
		ID:                 acc.ID,
		Phone:              acc.Phone,
		Key:                acc.Key,
		Label:              acc.Label,
		JID:                acc.JID.String(),
		LID:                acc.LID.String(),
		Status:             acc.Status,
		BusinessStatus:     acc.BusinessStatus,
		OfflineReason:      acc.OfflineReason,
		Decision:           decisionForStatus(acc.BusinessStatus, acc.OfflineReason),
		Error:              acc.Error,
		CreatedAt:          acc.CreatedAt,
		UpdatedAt:          acc.UpdatedAt,
		OnlineAt:           acc.OnlineAt,
		ClosedAt:           acc.ClosedAt,
		AppStateKeyID:      acc.AppStateKeyID,
		AppStateReady:      acc.AppStateReady(),
		AppStateKeysBefore: acc.AppStateKeysBefore,
		AppStateKeysAfter:  acc.AppStateKeysAfter,
		AppStateError:      acc.AppStateError,
		ExistingDevice:     acc.ExistingDevice,
		StateReset:         acc.StateReset,
		StateResetReason:   acc.StateResetReason,
	}
}

func (acc *ManagedAccount) AppStateReady() bool {
	return acc != nil && (acc.AppStateKeysBefore > 0 || acc.AppStateKeysAfter > 0)
}
