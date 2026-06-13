// Copyright (c) 2026 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"hash/fnv"
	"strings"
	"sync"
	"time"

	"github.com/lib/pq"

	"go.mau.fi/whatsmeow/store/sqlstore"
	waLog "go.mau.fi/whatsmeow/util/log"
)

const (
	defaultStoreDialect      = "postgres"
	defaultProxyScheme       = "socks5"
	defaultConnectTimeout    = 30 * time.Second
	defaultConnectRetries    = 3
	defaultConnectRetryDelay = 3 * time.Second
	defaultAppStateKeyWait   = 60 * time.Second
)

var (
	ErrManagerClosed   = errors.New("imported account manager is closed")
	ErrAccountNotFound = errors.New("imported account not found")
	ErrAccountExists   = errors.New("imported account already active")
	ErrAccountLocked   = errors.New("imported account is locked by another manager")
)

// Manager 持有 whatsmeow SQL store，以及当前进程内正在运行的账号连接。
//
// 这里刻意只做“协议账号生命周期”：
// - 导入或读取账号；
// - 建立 websocket；
// - 监听底层事件；
// - 输出业务状态回调；
// - 主动关闭连接。
//
// 账号表、任务队列、失败重试、代理池、封禁策略这些业务规则不写死在这里，
// 正式项目可以通过回调和 snapshot 自己维护。
type Manager struct {
	cfg       Config
	container *sqlstore.Container
	db        *sql.DB
	log       waLog.Logger

	mu       sync.RWMutex
	accounts map[string]*ManagedAccount
	jidIndex map[string]string
	closed   bool

	onSuccess        ImportSuccessHandler
	onFailure        ImportFailureHandler
	onEvent          AccountEventHandler
	onBusinessStatus BusinessStatusHandler
}

func NewManager(ctx context.Context, cfg Config, log waLog.Logger) (*Manager, error) {
	cfg = normalizeConfig(cfg)
	if log == nil {
		log = waLog.Noop
	}
	if cfg.StoreDialect == "postgres" {
		sqlstore.PostgresArrayWrapper = pq.Array
	}
	db, err := sql.Open(cfg.StoreDialect, cfg.StoreDSN)
	if err != nil {
		return nil, fmt.Errorf("open whatsmeow store: %w", err)
	}
	if cfg.StoreMaxIdleConns > 0 {
		db.SetMaxIdleConns(cfg.StoreMaxIdleConns)
	}
	if cfg.StoreMaxOpenConns > 0 {
		db.SetMaxOpenConns(cfg.StoreMaxOpenConns)
	}
	container := sqlstore.NewWithDB(db, cfg.StoreDialect, log.Sub("Store"))
	if err = container.Upgrade(ctx); err != nil {
		_ = container.Close()
		return nil, fmt.Errorf("upgrade whatsmeow store: %w", err)
	}
	return &Manager{
		cfg:       cfg,
		container: container,
		db:        db,
		log:       log,
		accounts:  make(map[string]*ManagedAccount),
		jidIndex:  make(map[string]string),
	}, nil
}

// New 是一个简化构造函数；如果调用方不需要自定义日志，可以直接用它。
func New(ctx context.Context, cfg Config) (*Manager, error) {
	return NewManager(ctx, cfg, waLog.Noop)
}

// normalizeConfig 填充 demo 可用的默认值。
// 业务相关的配置选择，例如是否按账号分配代理、失败后是否禁用账号，不在这里处理。
func normalizeConfig(cfg Config) Config {
	cfg.StoreDialect = strings.TrimSpace(cfg.StoreDialect)
	cfg.StoreDSN = strings.TrimSpace(cfg.StoreDSN)
	cfg.ProxyScheme = strings.TrimSpace(cfg.ProxyScheme)
	cfg.ProxyURL = strings.TrimSpace(cfg.ProxyURL)
	cfg.MediaProxyURL = strings.TrimSpace(cfg.MediaProxyURL)
	if cfg.StoreDialect == "" {
		cfg.StoreDialect = defaultStoreDialect
	}
	if cfg.ProxyScheme == "" {
		cfg.ProxyScheme = defaultProxyScheme
	}
	if cfg.ConnectTimeout <= 0 {
		cfg.ConnectTimeout = defaultConnectTimeout
	}
	if cfg.ConnectRetries <= 0 {
		cfg.ConnectRetries = defaultConnectRetries
	}
	if cfg.ConnectRetryDelay <= 0 {
		cfg.ConnectRetryDelay = defaultConnectRetryDelay
	}
	if cfg.AppStateKeyWait <= 0 {
		cfg.AppStateKeyWait = defaultAppStateKeyWait
	}
	return cfg
}

// AccountKey 是这个 demo 的内存账号 key。
// 正式业务可以替换成自己的账号主键，例如 account_id 或 tenant_id + account_id。
func AccountKey(phone string, id int64) string {
	phone = strings.TrimPrefix(strings.TrimSpace(phone), "+")
	return fmt.Sprintf("%s:%d", phone, id)
}

// SetHandlers 注册导入/登录成功和失败的粗粒度回调。
// 如果业务只需要知道最终结果，可以使用这两个回调。
func (m *Manager) SetHandlers(onSuccess ImportSuccessHandler, onFailure ImportFailureHandler) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onSuccess = onSuccess
	m.onFailure = onFailure
}

// SetAccountEventHandler 暴露原始 whatsmeow 事件，主要用于调试。
// 这些事件比较底层，业务状态同步建议用 SetBusinessStatusHandler。
func (m *Manager) SetAccountEventHandler(handler AccountEventHandler) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onEvent = handler
}

// SetBusinessStatusHandler 注册业务层最关心的状态回调。
// 回调里只需要处理 online/offline/banned 和离线原因，不需要解析底层事件。
func (m *Manager) SetBusinessStatusHandler(handler BusinessStatusHandler) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onBusinessStatus = handler
}

// Config 返回已经填充默认值后的 Manager 配置。
func (m *Manager) Config() Config {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.cfg
}

// Container 暴露底层 SQL store。
// demo 的 login 模式会用它枚举或查找已持久化设备；正式业务可以包一层自己的 DAO。
func (m *Manager) Container() *sqlstore.Container {
	if m == nil {
		return nil
	}
	return m.container
}

// GetStatus 返回一个当前进程内活跃账号的状态快照。
// 如果账号已经被 CancelAccount 移除，这里会返回 false。
func (m *Manager) GetStatus(phone string, id int64) (AccountStatusSnapshot, bool) {
	if m == nil {
		return AccountStatusSnapshot{}, false
	}
	key := AccountKey(phone, id)
	m.mu.RLock()
	defer m.mu.RUnlock()
	account, ok := m.accounts[key]
	if !ok {
		return AccountStatusSnapshot{}, false
	}
	return account.Snapshot(), true
}

// ListAccounts 返回当前进程内所有活跃账号的状态快照。
func (m *Manager) ListAccounts() []AccountStatusSnapshot {
	if m == nil {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]AccountStatusSnapshot, 0, len(m.accounts))
	for _, account := range m.accounts {
		out = append(out, account.Snapshot())
	}
	return out
}

// addAccount 把账号登记为当前进程内活跃账号。
// 这里会拒绝重复业务 key，也会拒绝重复 WhatsApp device JID，避免同设备多连接。
func (m *Manager) addAccount(account *ManagedAccount) error {
	if m == nil {
		return ErrManagerClosed
	}
	if account == nil {
		return errors.New("managed account is nil")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrManagerClosed
	}
	if _, exists := m.accounts[account.Key]; exists {
		return ErrAccountExists
	}
	if jid := account.JID.String(); jid != "" {
		if _, exists := m.jidIndex[jid]; exists {
			return fmt.Errorf("%w: jid %s", ErrAccountExists, jid)
		}
		m.jidIndex[jid] = account.Key
	}
	now := time.Now()
	if account.CreatedAt.IsZero() {
		account.CreatedAt = now
	}
	account.UpdatedAt = now
	m.accounts[account.Key] = account
	return nil
}

// updateAccount 串行更新内存账号状态，并刷新 UpdatedAt。
func (m *Manager) updateAccount(key string, fn func(account *ManagedAccount)) bool {
	if m == nil {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	account, ok := m.accounts[key]
	if !ok {
		return false
	}
	fn(account)
	account.UpdatedAt = time.Now()
	return true
}

// removeAccount 只从活跃账号 map 里移除账号。
// 它不会自动断开 websocket，调用方需要在移除后调用 closeManagedAccount。
func (m *Manager) removeAccount(key string) (*ManagedAccount, bool) {
	if m == nil {
		return nil, false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	account, ok := m.accounts[key]
	if !ok {
		return nil, false
	}
	delete(m.accounts, key)
	if jid := account.JID.String(); jid != "" {
		delete(m.jidIndex, jid)
	}
	return account, true
}

// CancelAccount 是业务主动触发的本地关闭。
// 它只关闭当前 Manager 里的连接，不会删除 WhatsApp linked device，也不会删除 SQL 中的设备数据。
func (m *Manager) CancelAccount(phone string, id int64) error {
	key := AccountKey(phone, id)
	account, ok := m.removeAccount(key)
	if !ok {
		return ErrAccountNotFound
	}
	closeManagedAccount(account)
	m.setBusinessStatus(account, BusinessStatusOffline, OfflineReasonClosed, nil)
	return nil
}

// Close 关闭当前 Manager 下所有活跃账号，并关闭 SQL store。
func (m *Manager) Close() error {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	accounts := m.accounts
	m.accounts = make(map[string]*ManagedAccount)
	m.jidIndex = make(map[string]string)
	m.mu.Unlock()

	for _, account := range accounts {
		closeManagedAccount(account)
		m.setBusinessStatus(account, BusinessStatusOffline, OfflineReasonClosed, nil)
	}
	if m.container != nil {
		return m.container.Close()
	}
	return nil
}

// notifyImportSuccess 在账号成功建立认证 websocket 后调用业务成功回调。
func (m *Manager) notifyImportSuccess(account *ManagedAccount) {
	if m == nil || account == nil {
		return
	}
	m.mu.RLock()
	handler := m.onSuccess
	m.mu.RUnlock()
	if handler == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_ = handler(ctx, ImportSuccessResult{
		ID:                 account.ID,
		Phone:              account.Phone,
		JID:                account.JID.String(),
		LID:                account.LID.String(),
		ExistingDevice:     account.ExistingDevice,
		StateReset:         account.StateReset,
		StateResetReason:   account.StateResetReason,
		AppStateKeysBefore: account.AppStateKeysBefore,
		AppStateKeysAfter:  account.AppStateKeysAfter,
		AppStateReady:      account.AppStateReady(),
		Account:            account,
	})
}

// notifyImportFailure 在导入 JSON 或数据库直接登录失败时调用业务失败回调。
func (m *Manager) notifyImportFailure(account *ManagedAccount, err error) {
	if m == nil || account == nil || err == nil {
		return
	}
	m.mu.RLock()
	handler := m.onFailure
	m.mu.RUnlock()
	if handler == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_ = handler(ctx, ImportFailureResult{
		ID:    account.ID,
		Phone: account.Phone,
		JID:   account.JID.String(),
		Error: err.Error(),
	})
}

// notifyAccountEvent 转发原始 whatsmeow 事件。
// 回调在 Manager 锁之外执行，避免业务逻辑阻塞账号生命周期更新。
func (m *Manager) notifyAccountEvent(account *ManagedAccount, evt any) {
	if m == nil || account == nil || evt == nil {
		return
	}
	m.mu.RLock()
	handler := m.onEvent
	m.mu.RUnlock()
	if handler == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := handler(ctx, account, evt); err != nil {
		m.log.Warnf("account event handler failed for %s: %v", account.JID, err)
	}
}

// setBusinessStatus 更新简化后的业务状态。
// 只有状态、离线原因或错误文本变化时才会触发回调，避免业务层重复写状态表。
func (m *Manager) setBusinessStatus(account *ManagedAccount, status BusinessAccountStatus, reason OfflineReason, err error) {
	if m == nil || account == nil {
		return
	}
	if status == "" {
		status = BusinessStatusOffline
	}
	var errText string
	if err != nil {
		errText = err.Error()
	}

	var change *BusinessStatusChange
	m.mu.Lock()
	oldStatus := account.BusinessStatus
	oldReason := account.OfflineReason
	if oldStatus != status || oldReason != reason || (errText != "" && account.Error != errText) {
		account.BusinessStatus = status
		account.OfflineReason = reason
		if errText != "" {
			account.Error = errText
		}
		account.UpdatedAt = time.Now()
		snapshot := account.Snapshot()
		change = &BusinessStatusChange{
			ID:               account.ID,
			Phone:            account.Phone,
			JID:              account.JID.String(),
			LID:              account.LID.String(),
			Account:          account,
			Snapshot:         snapshot,
			OldStatus:        oldStatus,
			NewStatus:        status,
			OldOfflineReason: oldReason,
			NewOfflineReason: reason,
			Decision:         decisionForStatus(status, reason),
			Error:            errText,
		}
	}
	handler := m.onBusinessStatus
	m.mu.Unlock()

	if change == nil || handler == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if handlerErr := handler(ctx, *change); handlerErr != nil {
		m.log.Warnf("business status handler failed for %s: %v", account.JID, handlerErr)
	}
}

// accountLock 保存一个 WhatsApp device JID 对应的 Postgres advisory lock。
type accountLock struct {
	conn *sql.Conn
	key  int64
}

// acquireAccountLock 防止多个 Manager 进程同时连接同一个 linked device。
// 如果没有这个锁，同一个 JSON/设备被重复上线时，WhatsApp 通常会返回 stream_replaced。
func (m *Manager) acquireAccountLock(ctx context.Context, jid string) (*accountLock, error) {
	if !m.cfg.AccountDBLock || m.cfg.StoreDialect != "postgres" {
		return nil, nil
	}
	key := accountLockKey(jid)
	conn, err := m.db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire account lock connection for %s: %w", jid, err)
	}
	var locked bool
	if err = conn.QueryRowContext(ctx, `SELECT pg_try_advisory_lock($1)`, key).Scan(&locked); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("acquire account lock for %s: %w", jid, err)
	}
	if !locked {
		_ = conn.Close()
		return nil, fmt.Errorf("%w: %s", ErrAccountLocked, jid)
	}
	return &accountLock{conn: conn, key: key}, nil
}

func (lock *accountLock) release() {
	if lock == nil || lock.conn == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _ = lock.conn.ExecContext(ctx, `SELECT pg_advisory_unlock($1)`, lock.key)
	_ = lock.conn.Close()
}

// accountLockKey 把设备 JID 映射成稳定的 advisory-lock 整数。
func accountLockKey(jid string) int64 {
	hash := fnv.New64a()
	_, _ = hash.Write([]byte("whatsmeow-imported-account:"))
	_, _ = hash.Write([]byte(jid))
	return int64(hash.Sum64())
}

// closeManagedAccount 注销事件处理器、断开 websocket，并释放进程级锁。
// 它只改变本地 Manager 状态，不会删除 WhatsApp 设备或 SQL store 中的账号数据。
func closeManagedAccount(account *ManagedAccount) {
	if account == nil {
		return
	}
	if account.Cancel != nil {
		account.Cancel()
	}
	if account.Account != nil && account.Account.Client != nil {
		if account.handlerID != 0 {
			account.Account.Client.RemoveEventHandler(account.handlerID)
		}
		account.Account.Client.Disconnect()
	}
	if account.lock != nil {
		account.lock.release()
		account.lock = nil
	}
	account.Status = AccountStatusClosed
	account.ClosedAt = time.Now()
	account.UpdatedAt = account.ClosedAt
}
