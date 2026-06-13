// Copyright (c) 2026 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/importedclient"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
)

var errStoredAccountRequired = errors.New("stored account phone or JID is required")

// ConnectStored 直接连接 SQL store 中已经存在的设备。
// 这个方法适合账号第一次 ImportAndConnect 成功后，服务重启或进程恢复时使用。
// 它不会重新导入 JSON，只会复用数据库里已有的 device、prekey、app-state key 等数据。
func (m *Manager) ConnectStored(ctx context.Context, req LoginRequest) (*ImportResponse, error) {
	if m == nil {
		return nil, ErrManagerClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	device, err := m.findStoredDevice(ctx, req)
	if err != nil {
		return nil, err
	}
	if device == nil || device.ID == nil {
		return nil, ErrAccountNotFound
	}
	phone := normalizePhone(req.Phone)
	if phone == "" {
		phone = phoneFromJID(device.ID.User)
	}
	key := AccountKey(phone, req.ID)
	// 数据库直接登录不需要再传 JSON。
	// 这里复用 whatsmeow SQL store 中已经持久化的设备信息和 app-state key。
	accountCtx, cancel := context.WithCancel(context.Background())
	account := &ManagedAccount{
		Key:            key,
		ID:             req.ID,
		Phone:          phone,
		Label:          firstNonEmpty(req.Label, key),
		Status:         AccountStatusImported,
		BusinessStatus: BusinessStatusOffline,
		OfflineReason:  OfflineReasonNone,
		JID:            device.GetJID(),
		LID:            device.GetLID(),
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
		// 登录失败时只移除当前 Manager 的内存账号。
		// SQL store 中的设备数据不在这里删除，是否清理由业务层根据失败原因决定。
		m.removeAccount(key)
		closeManagedAccount(account)
	}()

	lock, err := m.acquireAccountLock(ctx, account.JID.String())
	if err != nil {
		m.failAccount(account, err)
		return nil, err
	}
	account.lock = lock

	client := whatsmeow.NewClient(device, m.log.Sub(safeLoggerName(account.Label)))
	proxyURL, err := BuildProxyURL(ImportRequest{
		ProxyURL:  req.ProxyURL,
		ProxyHost: req.ProxyHost,
		ProxyPort: req.ProxyPort,
		ProxyUser: req.ProxyUser,
		ProxyPass: req.ProxyPass,
	}, m.cfg.ProxyScheme)
	if err != nil {
		m.failAccount(account, err)
		return nil, err
	}
	if proxyURL == "" {
		proxyURL = m.cfg.ProxyURL
	}
	if proxyURL != "" {
		if err = client.SetProxyAddress(proxyURL); err != nil {
			m.failAccount(account, err)
			return nil, err
		}
	}
	account.Account = &importedclient.Account{
		Client:    client,
		Device:    device,
		Container: m.container,
	}
	// 对于已经导入过或扫码登录过的账号，app-state key 理论上应该已经在 SQL store 里。
	// 这里记录数量，方便排查“能上线但无法做 app-state 操作”的情况。
	account.AppStateKeysBefore = countAppStateKeys(ctx, account)
	account.handlerID = client.AddEventHandler(func(evt any) {
		m.handleAccountEvent(account, evt)
	})

	if err = m.connectImportedAccount(accountCtx, account); err != nil {
		m.failAccount(account, err)
		return nil, err
	}
	if m.cfg.RecoverAppState {
		appStateErr := m.recoverAppState(accountCtx, account)
		if appStateErr != nil {
			m.updateAccount(account.Key, func(account *ManagedAccount) {
				account.AppStateError = appStateErr.Error()
			})
			if m.cfg.RequireAppState {
				m.failAccount(account, appStateErr)
				return nil, appStateErr
			}
			m.log.Warnf("app-state recovery failed for stored account %s: %v", account.JID, appStateErr)
		}
		account.AppStateKeysAfter = countAppStateKeys(ctx, account)
	} else {
		account.AppStateKeysAfter = account.AppStateKeysBefore
	}
	m.updateAccount(account.Key, func(account *ManagedAccount) {
		account.Status = AccountStatusOnline
		account.Error = ""
		account.OnlineAt = time.Now()
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

// findStoredDevice 从 SQL store 中查找设备。
// 优先使用完整 JID，因为同一个手机号可能存在多个 device id；
// phone 查询只是为了 demo 方便，正式业务建议保存并使用完整 JID。
func (m *Manager) findStoredDevice(ctx context.Context, req LoginRequest) (*store.Device, error) {
	if req.JID != "" {
		jid, err := types.ParseJID(req.JID)
		if err != nil {
			return nil, fmt.Errorf("parse stored jid: %w", err)
		}
		return m.container.GetDevice(ctx, jid)
	}
	phone := normalizePhone(req.Phone)
	if phone == "" {
		return nil, errStoredAccountRequired
	}
	devices, err := m.container.GetAllDevices(ctx)
	if err != nil {
		return nil, err
	}
	for _, device := range devices {
		if device == nil || device.ID == nil {
			continue
		}
		if phoneFromJID(device.ID.User) == phone {
			return device, nil
		}
	}
	return nil, ErrAccountNotFound
}
