// Copyright (c) 2026 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package main

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"go.mau.fi/whatsmeow/baileysauth"
	"go.mau.fi/whatsmeow/importedclient"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types/events"
)

var errCredentialsRequired = errors.New("credentials JSON is required")

// ImportAndConnect 导入一个外部 Baileys JSON 账号并连接上线。
// 成功后账号会保留在 Manager 的内存 accounts 里，直到业务调用 CancelAccount 或 Manager.Close。
// 这个方法适合“首次拿到 JSON 账号”时使用；后续服务重启可以走 ConnectStored，避免重复传 JSON。
func (m *Manager) ImportAndConnect(ctx context.Context, req ImportRequest) (*ImportResponse, error) {
	if m == nil {
		return nil, ErrManagerClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	credsJSON := req.CredsJSON
	if len(credsJSON) == 0 && strings.TrimSpace(req.Creds) != "" {
		credsJSON = []byte(req.Creds)
	}
	if len(credsJSON) == 0 {
		return nil, errCredentialsRequired
	}

	parsed, err := baileysauth.Parse(credsJSON)
	if err != nil {
		return nil, fmt.Errorf("parse credentials: %w", err)
	}
	phone := normalizePhone(req.Phone)
	if phone == "" {
		phone = phoneFromJID(parsed.Device.GetJID().User)
	}
	key := AccountKey(phone, req.ID)
	// accountCtx 故意不直接使用请求 ctx。
	// 原因是导入方法返回后，账号连接仍然应该继续在线，由 Manager 的 Cancel/Close 控制生命周期。
	accountCtx, cancel := context.WithCancel(context.Background())
	account := &ManagedAccount{
		Key:            key,
		ID:             req.ID,
		Phone:          phone,
		Label:          firstNonEmpty(req.Label, key),
		Status:         AccountStatusImporting,
		BusinessStatus: BusinessStatusOffline,
		OfflineReason:  OfflineReasonNone,
		JID:            parsed.Device.GetJID(),
		LID:            parsed.Device.GetLID(),
		Cancel:         cancel,
		terminal:       make(chan error, 1),
		CreatedAt:      time.Now(),
	}

	if err = m.addAccount(account); err != nil {
		cancel()
		return nil, err
	}

	keepAccount := false
	defer func() {
		if keepAccount {
			return
		}
		// 导入失败时只移除当前进程内的活跃账号。
		// SQL store 里是否已经写入了部分设备数据，取决于 baileysauth/importedclient 执行到哪一步。
		// 业务层如果要清理失败账号，可以在失败回调里决定是否删除业务记录或数据库设备。
		m.removeAccount(key)
		closeManagedAccount(account)
	}()

	if err = m.openImportedAccount(accountCtx, account, req, credsJSON); err != nil {
		m.failAccount(account, err)
		return nil, err
	}
	if err = m.connectImportedAccount(accountCtx, account); err != nil {
		m.failAccount(account, err)
		return nil, err
	}
	if m.cfg.RecoverAppState {
		// app-state key 是联系人、聊天框、仅发送方删除消息等 app-state 操作的前提。
		// 一旦收到 key，whatsmeow 会把它持久化到 SQL store，后续 ConnectStored 可以直接复用。
		appStateErr := m.recoverAppState(accountCtx, account)
		if appStateErr != nil {
			m.updateAccount(account.Key, func(account *ManagedAccount) {
				account.AppStateError = appStateErr.Error()
			})
			if m.cfg.RequireAppState {
				m.failAccount(account, appStateErr)
				return nil, appStateErr
			}
			m.log.Warnf("app-state recovery failed for %s: %v", account.JID, appStateErr)
		}
	}
	m.updateAccount(account.Key, func(account *ManagedAccount) {
		account.Status = AccountStatusOnline
		account.Error = ""
		account.OnlineAt = time.Now()
		account.AppStateKeysAfter = countAppStateKeys(context.Background(), account)
	})
	m.setBusinessStatus(account, BusinessStatusOnline, OfflineReasonNone, nil)
	m.notifyImportSuccess(account)

	keepAccount = true
	return &ImportResponse{
		ID:     account.ID,
		Phone:  account.Phone,
		JID:    account.JID.String(),
		LID:    account.LID.String(),
		Status: AccountStatusOnline,
	}, nil
}

// openImportedAccount 负责把外部 JSON 凭据导入到 whatsmeow SQL store。
// 导入完成后会创建 importedclient.Account，后续发送消息、app-state 恢复、事件监听都依赖它。
func (m *Manager) openImportedAccount(ctx context.Context, account *ManagedAccount, req ImportRequest, credsJSON []byte) error {
	proxyURL, err := BuildProxyURL(req, m.cfg.ProxyScheme)
	if err != nil {
		return err
	}
	if proxyURL == "" {
		proxyURL = m.cfg.ProxyURL
	}
	importOptions := req.ImportOptions
	if importOptions == nil {
		importOptions = m.cfg.ImportOptions
	}
	lock, err := m.acquireAccountLock(ctx, account.JID.String())
	if err != nil {
		return err
	}
	account.lock = lock

	importedAccount, err := importedclient.Open(ctx, importedclient.Config{
		CredsJSON:        credsJSON,
		Container:        m.container,
		ImportOptions:    importOptions,
		Logger:           m.log.Sub(safeLoggerName(account.Label)),
		Proxy:            proxyURL,
		MediaProxy:       m.cfg.MediaProxyURL,
		MediaDirect:      m.cfg.MediaDirect,
		TransportTimeout: m.cfg.TransportTimeout,
		AppStateKeyWait:  m.cfg.AppStateKeyWait,
	})
	if err != nil {
		return fmt.Errorf("import credentials: %w", err)
	}
	account.Account = importedAccount
	account.JID = importedAccount.Device.GetJID()
	account.LID = importedAccount.Device.GetLID()
	account.AppStateKeyID = printableKeyID(importedAccount.Imported.MyAppStateKeyID)
	account.ExistingDevice = importedAccount.Imported.ExistingDevice
	account.StateReset = importedAccount.Imported.StateReset
	account.StateResetReason = importedAccount.Imported.StateResetReason
	account.AppStateKeysBefore = countAppStateKeys(ctx, account)
	account.handlerID = importedAccount.Client.AddEventHandler(func(evt any) {
		m.handleAccountEvent(account, evt)
	})
	m.updateAccount(account.Key, func(account *ManagedAccount) {
		account.Status = AccountStatusImported
	})
	return nil
}

// connectImportedAccount 负责建立 websocket 连接，并做短重试。
// 如果 WhatsApp 在连接过程中返回 401、stream_replaced、deleted device 等致命事件，
// handleAccountEvent 会把错误写入 account.terminal，从而立即中断重试。
func (m *Manager) connectImportedAccount(ctx context.Context, account *ManagedAccount) error {
	m.updateAccount(account.Key, func(account *ManagedAccount) {
		account.Status = AccountStatusConnecting
	})
	var lastErr error
	for attempt := 1; attempt <= m.cfg.ConnectRetries; attempt++ {
		lastErr = account.Account.Connect()
		if lastErr == nil {
			if account.Account.Client.WaitForConnection(m.cfg.ConnectTimeout) {
				return nil
			}
			lastErr = fmt.Errorf("timed out waiting %s for authenticated connection", m.cfg.ConnectTimeout)
		}
		if err := pollTerminal(account); err != nil {
			return err
		}
		if errors.Is(lastErr, store.ErrDeviceDeleted) || isTerminalStatus(account.Status) {
			return lastErr
		}
		if account.Account != nil && account.Account.Client != nil {
			account.Account.Client.Disconnect()
		}
		if attempt == m.cfg.ConnectRetries {
			break
		}
		if err := sleepContext(ctx, m.cfg.ConnectRetryDelay); err != nil {
			return err
		}
		m.updateAccount(account.Key, func(account *ManagedAccount) {
			account.Status = AccountStatusConnecting
		})
	}
	return lastErr
}

// recoverAppState 尝试恢复 app-state key。
// 它会让 whatsmeow 拉取 app-state snapshot，并在缺 key 时向主设备/其他伴随设备请求 key share。
// 默认是 best effort：失败只记录错误；如果 Config.RequireAppState=true，则恢复失败会让账号登录失败。
func (m *Manager) recoverAppState(ctx context.Context, account *ManagedAccount) error {
	if account == nil || account.Account == nil || account.Account.Client == nil {
		return errors.New("managed account is not initialized")
	}
	if account.AppStateKeysBefore > 0 {
		return nil
	}
	var keyID []byte
	if account.Account.Imported != nil {
		keyID = account.Account.Imported.MyAppStateKeyID
	}
	recoveryCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	terminalErrCh := make(chan error, 1)
	go func() {
		select {
		case err := <-account.terminal:
			terminalErrCh <- err
			cancel()
		case <-recoveryCtx.Done():
		}
	}()
	err := account.Account.Client.RecoverAppStateKeys(recoveryCtx, keyID, m.cfg.AppStateKeyWait)
	select {
	case terminalErr := <-terminalErrCh:
		if terminalErr != nil {
			return terminalErr
		}
	default:
	}
	return err
}

// handleAccountEvent 把 whatsmeow 的底层事件转换成 Manager 内部状态和业务状态回调。
// 业务层一般不用直接处理这些原始事件，只需要订阅 BusinessStatusHandler。
func (m *Manager) handleAccountEvent(account *ManagedAccount, evt any) {
	m.notifyAccountEvent(account, evt)

	switch event := evt.(type) {
	case *events.Connected:
		m.updateAccount(account.Key, func(account *ManagedAccount) {
			account.Status = AccountStatusOnline
			account.OnlineAt = time.Now()
			account.Error = ""
		})
		m.setBusinessStatus(account, BusinessStatusOnline, OfflineReasonNone, nil)
	case *events.Disconnected:
		m.updateAccount(account.Key, func(account *ManagedAccount) {
			account.Status = AccountStatusDisconnected
		})
		m.setBusinessStatus(account, BusinessStatusOffline, OfflineReasonDisconnected, nil)
	case *events.LoggedOut:
		m.terminalAccount(account, AccountStatusLoggedOut, OfflineReasonLoggedOut, fmt.Errorf("logged out: on_connect=%t reason=%s", event.OnConnect, event.Reason.String()))
	case *events.StreamReplaced:
		m.terminalAccount(account, AccountStatusReplaced, OfflineReasonStreamReplaced, errors.New("stream replaced: same imported credentials connected elsewhere"))
	case *events.ConnectFailure:
		m.terminalAccount(account, AccountStatusConnectFail, OfflineReasonConnectFailed, fmt.Errorf("connect failure: reason=%s message=%s", event.Reason.String(), event.Message))
	case *events.ClientOutdated:
		m.terminalAccount(account, AccountStatusConnectFail, OfflineReasonClientOutdated, errors.New("whatsapp client outdated"))
	case *events.CATRefreshError:
		m.terminalAccount(account, AccountStatusConnectFail, OfflineReasonCATRefresh, fmt.Errorf("CAT refresh failed: %w", event.Error))
	case *events.TemporaryBan:
		m.terminalAccount(account, AccountStatusTemporaryBan, OfflineReasonTemporaryBan, errors.New(event.String()))
	}
}

// terminalAccount 记录致命账号状态，并唤醒正在等待连接成功或 app-state 恢复完成的流程。
// 例如 401、stream_replaced、temporary_ban 都会走这里。
func (m *Manager) terminalAccount(account *ManagedAccount, status AccountStatus, reason OfflineReason, err error) {
	m.updateAccount(account.Key, func(account *ManagedAccount) {
		account.Status = status
		account.Error = err.Error()
	})
	m.setBusinessStatus(account, businessStatusForAccountStatus(status), reason, err)
	select {
	case account.terminal <- err:
	default:
	}
}

// failAccount 处理同步阶段返回的错误。
// 例如导入失败、代理配置错误、连接超时，可能还没有走到具体的 whatsmeow fatal event。
func (m *Manager) failAccount(account *ManagedAccount, err error) {
	status := classifyStatus(err)
	m.updateAccount(account.Key, func(account *ManagedAccount) {
		account.Status = status
		account.Error = err.Error()
	})
	m.setBusinessStatus(account, businessStatusForAccountStatus(status), offlineReasonForAccountStatus(status), err)
	m.notifyImportFailure(account, err)
}

// pollTerminal 检查连接等待期间是否已经收到 fatal event。
// 这样可以避免 Connect 还在重试，但账号其实已经被 401 或 stream_replaced 判死。
func pollTerminal(account *ManagedAccount) error {
	select {
	case err := <-account.terminal:
		return err
	default:
		return nil
	}
}

// isTerminalStatus 标记哪些状态应该停止当前账号的连接重试。
func isTerminalStatus(status AccountStatus) bool {
	switch status {
	case AccountStatusLoggedOut, AccountStatusDeleted, AccountStatusReplaced, AccountStatusConnectFail, AccountStatusTemporaryBan, AccountStatusFailed, AccountStatusClosed:
		return true
	default:
		return false
	}
}

// classifyStatus 把同步返回的错误归类成 Manager 内部详细状态。
// 业务层看到的 offline reason 会再由 offlineReasonForAccountStatus 转换。
func classifyStatus(err error) AccountStatus {
	if err == nil {
		return ""
	}
	if errors.Is(err, store.ErrDeviceDeleted) {
		return AccountStatusDeleted
	}
	text := strings.ToLower(err.Error())
	switch {
	case strings.Contains(text, "logged out"), strings.Contains(text, "reason=401"):
		return AccountStatusLoggedOut
	case strings.Contains(text, "invalid use of deleted device"):
		return AccountStatusDeleted
	case strings.Contains(text, "stream replaced"):
		return AccountStatusReplaced
	case strings.Contains(text, "temporary ban"):
		return AccountStatusTemporaryBan
	case strings.Contains(text, "connect"):
		return AccountStatusConnectFail
	default:
		return AccountStatusFailed
	}
}

// businessStatusForAccountStatus 把内部详细状态降维成业务只关心的三态：
// online/offline/banned。这里接收的通常是失败或终止状态，所以默认返回 offline。
func businessStatusForAccountStatus(status AccountStatus) BusinessAccountStatus {
	if status == AccountStatusTemporaryBan {
		return BusinessStatusBanned
	}
	return BusinessStatusOffline
}

// offlineReasonForAccountStatus 把内部状态映射成稳定的业务离线原因。
// 业务层可以直接把这个值写到自己的账号表。
func offlineReasonForAccountStatus(status AccountStatus) OfflineReason {
	switch status {
	case AccountStatusDisconnected:
		return OfflineReasonDisconnected
	case AccountStatusLoggedOut:
		return OfflineReasonLoggedOut
	case AccountStatusDeleted:
		return OfflineReasonDeletedDevice
	case AccountStatusReplaced:
		return OfflineReasonStreamReplaced
	case AccountStatusConnectFail:
		return OfflineReasonConnectFailed
	case AccountStatusTemporaryBan:
		return OfflineReasonTemporaryBan
	case AccountStatusClosed:
		return OfflineReasonClosed
	case AccountStatusFailed:
		return OfflineReasonImportFailed
	default:
		return OfflineReasonUnknown
	}
}

// decisionForStatus 给业务层一个直接处理建议。
// Manager 只负责分类和回调，不会根据这个建议自动重试或删除账号。
func decisionForStatus(status BusinessAccountStatus, reason OfflineReason) AccountDecision {
	if status == BusinessStatusOnline {
		return AccountDecisionUsable
	}
	if status == BusinessStatusBanned {
		return AccountDecisionPause
	}
	switch reason {
	case OfflineReasonNone, OfflineReasonClosed:
		return AccountDecisionUsable
	case OfflineReasonLoggedOut, OfflineReasonDeletedDevice:
		return AccountDecisionUnavailable
	case OfflineReasonStreamReplaced:
		return AccountDecisionOccupied
	case OfflineReasonDisconnected, OfflineReasonConnectFailed, OfflineReasonCATRefresh:
		return AccountDecisionRetryLater
	case OfflineReasonTemporaryBan:
		return AccountDecisionPause
	case OfflineReasonClientOutdated, OfflineReasonImportFailed, OfflineReasonUnknown:
		return AccountDecisionUnknown
	default:
		return AccountDecisionUnknown
	}
}

// countAppStateKeys 只用于诊断和 summary 展示。
// 读取失败时按 0 处理，因为真正的导入/连接错误会在外层单独处理。
func countAppStateKeys(ctx context.Context, account *ManagedAccount) int {
	if account == nil || account.Account == nil || account.Account.Client == nil {
		return 0
	}
	count, err := account.Account.Client.AppStateKeyCount(ctx)
	if err != nil {
		return 0
	}
	return count
}

func normalizePhone(phone string) string {
	phone = strings.TrimSpace(phone)
	phone = strings.TrimPrefix(phone, "+")
	return phone
}

func phoneFromJID(user string) string {
	if idx := strings.IndexByte(user, ':'); idx >= 0 {
		return user[:idx]
	}
	return user
}

func sleepContext(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func printableKeyID(keyID []byte) string {
	if len(keyID) == 0 {
		return ""
	}
	return strings.ToUpper(hex.EncodeToString(keyID))
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			return value
		}
	}
	return ""
}

func safeLoggerName(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "Account"
	}
	replacer := strings.NewReplacer("/", "_", "\\", "_", " ", "_", ":", "_", "@", "_")
	return replacer.Replace(value)
}
