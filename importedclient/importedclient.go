// Copyright (c) 2026 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

// Package importedclient wraps imported-account lifecycle operations into a
// higher-level API for applications that use external Baileys JSON credentials.
package importedclient

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/lib/pq"
	"golang.org/x/net/proxy"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/baileysauth"
	"go.mau.fi/whatsmeow/messagebuilder"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	waLog "go.mau.fi/whatsmeow/util/log"
)

// Config configures an imported account lifecycle client.
type Config struct {
	// CredsJSON contains the Baileys creds JSON bytes. This is the primary
	// import input for production callers.
	CredsJSON []byte
	// Creds contains the Baileys creds JSON string. Used only when CredsJSON is empty.
	Creds string

	// Container can be set by callers that already manage a whatsmeow store.
	// When Container is nil, DBDialect and DBURI are used to create an SQL store.
	Container store.DeviceContainer
	// ImportOptions controls repeated imports of the same JID. When nil, the
	// default importer behavior is used: reset device-specific state only when
	// imported identity material differs from the existing device.
	ImportOptions *baileysauth.ImportOptions
	DBDialect     string
	DBURI         string

	DBMaxIdleConns int
	DBMaxOpenConns int

	Logger waLog.Logger

	Proxy            string
	MediaProxy       string
	MediaDirect      bool
	TransportTimeout time.Duration

	// AppStateKeyWait controls EnsureReady app-state key recovery.
	// Defaults to 30 seconds.
	AppStateKeyWait time.Duration
}

// Account is a ready-to-use wrapper around an imported whatsmeow client.
type Account struct {
	Client    *whatsmeow.Client
	Device    *store.Device
	Imported  *baileysauth.ImportedDevice
	Container store.DeviceContainer

	appStateKeyWait time.Duration
	sqlContainer    *sqlstore.Container
}

// SentMessage contains the metadata applications should persist for later
// delete/revoke operations.
type SentMessage struct {
	Chat      types.JID
	ID        types.MessageID
	Timestamp time.Time
	FromMe    bool
	Response  whatsmeow.SendResponse
}

// Open imports Baileys JSON credentials, creates a whatsmeow client, and applies
// configured transports. It does not connect automatically; call EnsureReady or Connect.
func Open(ctx context.Context, cfg Config) (*Account, error) {
	credsJSON := cfg.CredsJSON
	if len(credsJSON) == 0 && cfg.Creds != "" {
		credsJSON = []byte(cfg.Creds)
	}
	if len(credsJSON) == 0 {
		return nil, errors.New("imported credentials JSON is empty")
	}
	if cfg.MediaDirect && cfg.MediaProxy != "" {
		return nil, errors.New("media direct and media proxy cannot both be set")
	}

	container, sqlContainer, err := openContainer(ctx, cfg)
	if err != nil {
		return nil, err
	}
	closeSQLOnError := sqlContainer != nil
	defer func() {
		if closeSQLOnError {
			_ = sqlContainer.Close()
		}
	}()

	var imported *baileysauth.ImportedDevice
	if cfg.ImportOptions != nil {
		imported, err = baileysauth.ImportIntoContainerWithOptions(ctx, container, credsJSON, *cfg.ImportOptions)
	} else {
		imported, err = baileysauth.ImportIntoContainer(ctx, container, credsJSON)
	}
	if err != nil {
		return nil, fmt.Errorf("import credentials: %w", err)
	}
	client := whatsmeow.NewClient(imported.Device, cfg.Logger)
	if err = configureTransports(client, cfg); err != nil {
		return nil, err
	}

	closeSQLOnError = false
	return &Account{
		Client:          client,
		Device:          imported.Device,
		Imported:        imported,
		Container:       container,
		appStateKeyWait: cfg.AppStateKeyWait,
		sqlContainer:    sqlContainer,
	}, nil
}

// OpenFile is a convenience helper for local tools. Production callers should
// normally read or decrypt the file themselves and pass Config.CredsJSON to Open.
func OpenFile(ctx context.Context, cfg Config, path string) (*Account, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	cfg.CredsJSON = data
	cfg.Creds = ""
	return Open(ctx, cfg)
}

// Connect connects the underlying whatsmeow client.
func (acc *Account) Connect() error {
	if acc == nil || acc.Client == nil {
		return whatsmeow.ErrClientIsNil
	}
	if acc.Client.IsConnected() && acc.Client.IsLoggedIn() {
		return nil
	}
	return acc.Client.Connect()
}

// EnsureReady connects the account and recovers app-state keys when needed.
func (acc *Account) EnsureReady(ctx context.Context) error {
	if err := acc.Connect(); err != nil {
		return err
	}
	wait := acc.appStateKeyWait
	if wait <= 0 {
		wait = 30 * time.Second
	}
	if acc != nil && acc.Imported != nil {
		if err := acc.Client.RecoverAppStateKeys(ctx, acc.Imported.MyAppStateKeyID, wait); err != nil {
			return err
		}
	}
	return nil
}

// Close disconnects the client and closes the SQL container if Open created it.
func (acc *Account) Close() error {
	if acc == nil {
		return nil
	}
	if acc.Client != nil {
		acc.Client.Disconnect()
	}
	if acc.sqlContainer != nil {
		return acc.sqlContainer.Close()
	}
	return nil
}

// AddContact adds a phone number or JID to the current account's contacts.
func (acc *Account) AddContact(ctx context.Context, recipient, fullName string) (types.JID, error) {
	target, err := ParseRecipient(recipient)
	if err != nil {
		return types.EmptyJID, err
	}
	if err = acc.Client.AddContact(ctx, target, fullName); err != nil {
		return types.EmptyJID, err
	}
	return target, nil
}

// Send builds and sends a message to a phone number or JID.
func (acc *Account) Send(ctx context.Context, recipient string, opts messagebuilder.Options) (SentMessage, error) {
	target, err := ParseRecipient(recipient)
	if err != nil {
		return SentMessage{}, err
	}
	built, err := messagebuilder.Build(ctx, acc.Client, opts)
	if err != nil {
		return SentMessage{}, err
	}
	resp, err := acc.Client.SendMessage(ctx, target, built.Message, built.SendRequestExtra())
	if err != nil {
		return SentMessage{Chat: target, ID: resp.ID, Timestamp: resp.Timestamp, FromMe: true, Response: resp}, err
	}
	return SentMessage{
		Chat:      target,
		ID:        resp.ID,
		Timestamp: resp.Timestamp,
		FromMe:    true,
		Response:  resp,
	}, nil
}

// SendBuiltMessage sends a prebuilt messagebuilder payload to a phone number or JID.
func (acc *Account) SendBuiltMessage(ctx context.Context, recipient string, msg messagebuilder.BuiltMessage) (SentMessage, error) {
	if msg.Message == nil {
		return SentMessage{}, errors.New("message is nil")
	}
	target, err := ParseRecipient(recipient)
	if err != nil {
		return SentMessage{}, err
	}
	resp, err := acc.Client.SendMessage(ctx, target, msg.Message, msg.SendRequestExtra())
	if err != nil {
		return SentMessage{Chat: target, ID: resp.ID, Timestamp: resp.Timestamp, FromMe: true, Response: resp}, err
	}
	return SentMessage{Chat: target, ID: resp.ID, Timestamp: resp.Timestamp, FromMe: true, Response: resp}, nil
}

// SendMessage sends a prebuilt protobuf message to a phone number or JID.
func (acc *Account) SendMessage(ctx context.Context, recipient string, msg *messagebuilder.BuiltMessage) (SentMessage, error) {
	if msg == nil {
		return SentMessage{}, errors.New("message is nil")
	}
	return acc.SendBuiltMessage(ctx, recipient, *msg)
}

// DeleteMessageForMe deletes a sent message only from the current account.
func (acc *Account) DeleteMessageForMe(ctx context.Context, sent SentMessage, deleteMedia bool) error {
	if sent.Chat.IsEmpty() || sent.ID == "" {
		return errors.New("sent message chat or ID is empty")
	}
	return acc.Client.DeleteMessageForMe(ctx, sent.Chat, types.EmptyJID, sent.ID, sent.FromMe, sent.Timestamp, deleteMedia)
}

// DeleteMessageForMeAfter waits for delay, then deletes a sent message only from
// the current account.
func (acc *Account) DeleteMessageForMeAfter(ctx context.Context, sent SentMessage, delay time.Duration, deleteMedia bool) error {
	if delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}
	return acc.DeleteMessageForMe(ctx, sent, deleteMedia)
}

// DeleteChatForMe deletes the chat only from the current account.
func (acc *Account) DeleteChatForMe(ctx context.Context, sent SentMessage, deleteMedia bool) error {
	if sent.Chat.IsEmpty() {
		return errors.New("sent message chat is empty")
	}
	lastKey := acc.Client.BuildMessageKey(sent.Chat, types.EmptyJID, sent.ID)
	return acc.Client.DeleteChatForMe(ctx, sent.Chat, sent.Timestamp, lastKey, deleteMedia)
}

// RevokeForEveryone revokes a previously sent outgoing message.
func (acc *Account) RevokeForEveryone(ctx context.Context, sent SentMessage) (whatsmeow.SendResponse, error) {
	if sent.Chat.IsEmpty() || sent.ID == "" {
		return whatsmeow.SendResponse{}, errors.New("sent message chat or ID is empty")
	}
	return acc.Client.SendMessage(ctx, sent.Chat, acc.Client.BuildRevoke(sent.Chat, types.EmptyJID, sent.ID))
}

// ParseRecipient converts a phone number or JID string to a WhatsApp JID.
func ParseRecipient(recipient string) (types.JID, error) {
	recipient = strings.TrimSpace(recipient)
	if recipient == "" {
		return types.EmptyJID, errors.New("recipient is empty")
	}
	if strings.Contains(recipient, "@") {
		return types.ParseJID(recipient)
	}
	user := strings.NewReplacer("+", "", " ", "", "-", "", "(", "", ")", "").Replace(recipient)
	if user == "" {
		return types.EmptyJID, errors.New("recipient phone number is empty")
	}
	return types.NewJID(user, types.DefaultUserServer), nil
}

func openContainer(ctx context.Context, cfg Config) (store.DeviceContainer, *sqlstore.Container, error) {
	if cfg.Container != nil {
		return cfg.Container, nil, nil
	}
	if cfg.DBDialect == "" || cfg.DBURI == "" {
		return nil, nil, errors.New("DB dialect and URI are required when no container is provided")
	}
	if cfg.DBDialect == "postgres" {
		sqlstore.PostgresArrayWrapper = pq.Array
	}
	db, err := sql.Open(cfg.DBDialect, cfg.DBURI)
	if err != nil {
		return nil, nil, fmt.Errorf("open database: %w", err)
	}
	if cfg.DBMaxIdleConns > 0 {
		db.SetMaxIdleConns(cfg.DBMaxIdleConns)
	}
	if cfg.DBMaxOpenConns > 0 {
		db.SetMaxOpenConns(cfg.DBMaxOpenConns)
	}
	container := sqlstore.NewWithDB(db, cfg.DBDialect, cfg.Logger)
	if err = container.Upgrade(ctx); err != nil {
		_ = container.Close()
		return nil, nil, fmt.Errorf("upgrade database: %w", err)
	}
	return container, container, nil
}

func configureTransports(client *whatsmeow.Client, cfg Config) error {
	if cfg.Proxy != "" {
		opts := whatsmeow.SetProxyOptions{
			NoMedia: cfg.MediaDirect || cfg.MediaProxy != "",
		}
		if err := setProxyAddressWithTimeout(client, cfg.Proxy, cfg.TransportTimeout, opts); err != nil {
			return fmt.Errorf("set proxy: %w", err)
		}
	}
	if cfg.MediaProxy != "" {
		if err := setProxyAddressWithTimeout(client, cfg.MediaProxy, cfg.TransportTimeout, whatsmeow.SetProxyOptions{NoWebsocket: true}); err != nil {
			return fmt.Errorf("set media proxy: %w", err)
		}
	} else if cfg.MediaDirect {
		setDirectMediaTransport(client, cfg.TransportTimeout)
	}
	return nil
}

func setProxyAddressWithTimeout(client *whatsmeow.Client, addr string, timeout time.Duration, opts whatsmeow.SetProxyOptions) error {
	if timeout <= 0 {
		return client.SetProxyAddress(addr, opts)
	}
	parsed, err := url.Parse(addr)
	if err != nil {
		return err
	}
	transport := (http.DefaultTransport.(*http.Transport)).Clone()
	transport.TLSHandshakeTimeout = timeout
	transport.ResponseHeaderTimeout = timeout
	dialer := &net.Dialer{
		Timeout:   timeout,
		KeepAlive: 30 * time.Second,
	}
	switch parsed.Scheme {
	case "http", "https":
		transport.Proxy = http.ProxyURL(parsed)
		transport.DialContext = dialer.DialContext
	case "socks5":
		px, err := proxy.FromURL(parsed, dialer)
		if err != nil {
			return err
		}
		pxc, ok := px.(proxy.ContextDialer)
		if !ok {
			return errors.New("socks5 proxy does not support context dialer")
		}
		transport.DialContext = pxc.DialContext
	default:
		return fmt.Errorf("unsupported proxy scheme %q", parsed.Scheme)
	}
	client.DangerousInternals().SetTransport(transport, opts)
	return nil
}

func setDirectMediaTransport(client *whatsmeow.Client, timeout time.Duration) {
	transport := (http.DefaultTransport.(*http.Transport)).Clone()
	transport.Proxy = nil
	if timeout > 0 {
		transport.TLSHandshakeTimeout = timeout
		transport.ResponseHeaderTimeout = timeout
		transport.DialContext = (&net.Dialer{
			Timeout:   timeout,
			KeepAlive: 30 * time.Second,
		}).DialContext
	}
	client.DangerousInternals().SetTransport(transport, whatsmeow.SetProxyOptions{NoWebsocket: true})
}
