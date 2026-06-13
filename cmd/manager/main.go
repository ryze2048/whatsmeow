// Copyright (c) 2026 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package main

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"hash/fnv"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/lib/pq"

	"go.mau.fi/whatsmeow/baileysauth"
	"go.mau.fi/whatsmeow/importedclient"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
)

type managerStatus string

const (
	statusStarting     managerStatus = "starting"
	statusImported     managerStatus = "imported"
	statusConnecting   managerStatus = "connecting"
	statusOnline       managerStatus = "online"
	statusDisconnected managerStatus = "disconnected"
	statusLoggedOut    managerStatus = "logged_out"
	statusDeleted      managerStatus = "deleted_device"
	statusReplaced     managerStatus = "stream_replaced"
	statusLocked       managerStatus = "account_locked"
	statusConnectFail  managerStatus = "connect_failed"
	statusTempBan      managerStatus = "temporary_ban"
	statusClosed       managerStatus = "closed"
)

const (
	defaultManagerCredsDir         = "docs/keypair"
	defaultManagerLimit            = 10
	defaultManagerConcurrency      = 10
	defaultManagerDBURI            = "postgres://admin:admin123@127.0.0.1:5432/app?sslmode=disable"
	defaultManagerDBDialect        = "postgres"
	defaultManagerDBMaxIdleConns   = 10
	defaultManagerDBMaxOpenConns   = 100
	defaultManagerProxy            = "socks5://wefanvip1_1:MKLP123456@proxyus.rola.vip:2000"
	defaultManagerTimeout          = 5 * time.Minute
	defaultManagerConnectRetries   = 5
	defaultManagerConnectReadyWait = 30 * time.Second
	defaultManagerStayFor          = 60 * time.Second
	defaultManagerRecoverAppState  = true
	defaultManagerAccountDBLock    = true
	defaultManagerDebug            = true
)

type repeatedStrings []string

func (values *repeatedStrings) String() string {
	return strings.Join(*values, ",")
}

func (values *repeatedStrings) Set(value string) error {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	*values = append(*values, value)
	return nil
}

type managerConfig struct {
	creds          repeatedStrings
	credsList      string
	credsDir       string
	limit          int
	concurrency    int
	dbURI          string
	dbDialect      string
	dbMaxIdleConns int
	dbMaxOpenConns int
	proxy          string

	timeout          time.Duration
	connectRetries   int
	connectRetryWait time.Duration
	connectReadyWait time.Duration
	transportTimeout time.Duration
	stayFor          time.Duration

	recoverAppState bool
	requireAppState bool
	accountDBLock   bool
	appStateKeyWait time.Duration
	debug           bool
}

type accountJob struct {
	Index int
	Path  string
	JSON  []byte
	JID   string
	LID   string
}

type accountResult struct {
	Index   int
	Path    string
	JID     string
	LID     string
	Status  managerStatus
	Started time.Time
	Online  time.Time
	Offline time.Time
	Err     error

	AppStateKeyID      string
	AppStateKeysBefore int
	AppStateKeysAfter  int
	AppStateErr        error
	ExistingDevice     bool
	StateReset         bool
	StateResetReason   string
}

type lifecycleTracker struct {
	label    string
	terminal chan error

	mu     sync.RWMutex
	status managerStatus
}

func newLifecycleTracker(label string) *lifecycleTracker {
	return &lifecycleTracker{
		label:    label,
		terminal: make(chan error, 1),
		status:   statusStarting,
	}
}

func (tracker *lifecycleTracker) setStatus(status managerStatus) {
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	if tracker.status == status {
		return
	}
	tracker.status = status
	fmt.Printf("[%s] status=%s\n", tracker.label, status)
}

func (tracker *lifecycleTracker) statusValue() managerStatus {
	tracker.mu.RLock()
	defer tracker.mu.RUnlock()
	return tracker.status
}

func (tracker *lifecycleTracker) terminalError(err error) {
	select {
	case tracker.terminal <- err:
	default:
	}
}

func (tracker *lifecycleTracker) pollTerminal() error {
	select {
	case err := <-tracker.terminal:
		return err
	default:
		return nil
	}
}

func (tracker *lifecycleTracker) handleEvent(evt any) {
	switch event := evt.(type) {
	case *events.Connected:
		tracker.setStatus(statusOnline)
	case *events.Disconnected:
		tracker.setStatus(statusDisconnected)
	case *events.LoggedOut:
		tracker.setStatus(statusLoggedOut)
		tracker.terminalError(fmt.Errorf("logged out: on_connect=%t reason=%s", event.OnConnect, event.Reason.String()))
	case *events.StreamReplaced:
		tracker.setStatus(statusReplaced)
		tracker.terminalError(errors.New("stream replaced: same imported credentials connected elsewhere"))
	case *events.ConnectFailure:
		tracker.setStatus(statusConnectFail)
		tracker.terminalError(fmt.Errorf("connect failure: reason=%s message=%s", event.Reason.String(), event.Message))
	case *events.ClientOutdated:
		tracker.terminalError(errors.New("client outdated"))
	case *events.CATRefreshError:
		tracker.terminalError(fmt.Errorf("CAT refresh failed: %w", event.Error))
	case *events.TemporaryBan:
		tracker.setStatus(statusTempBan)
		tracker.terminalError(fmt.Errorf("temporary ban: %s", event.String()))
	case *events.StreamError:
		fmt.Printf("[%s] stream error: code=%s\n", tracker.label, event.Code)
	case *events.KeepAliveTimeout:
		fmt.Printf("[%s] keepalive timeout: count=%d last_success=%s\n", tracker.label, event.ErrorCount, event.LastSuccess.Format(time.RFC3339))
	case *events.KeepAliveRestored:
		fmt.Printf("[%s] keepalive restored\n", tracker.label)
	}
}

func main() {
	cfg := parseFlags()
	if err := run(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "manager demo failed: %v\n", err)
		os.Exit(1)
	}
}

func parseFlags() managerConfig {
	var cfg managerConfig
	flag.Var(&cfg.creds, "creds", "path to one Baileys JSON credentials file; repeat for multiple accounts")
	flag.StringVar(&cfg.credsList, "creds-list", "", "comma-separated Baileys JSON credential paths")
	flag.StringVar(&cfg.credsDir, "creds-dir", defaultManagerCredsDir, "directory containing Baileys JSON credential files")
	flag.IntVar(&cfg.limit, "limit", defaultManagerLimit, "maximum number of accounts to load; 0 means all")
	flag.IntVar(&cfg.concurrency, "concurrency", defaultManagerConcurrency, "number of accounts to connect concurrently")
	flag.StringVar(&cfg.dbURI, "db", defaultManagerDBURI, "persistent database URI")
	flag.StringVar(&cfg.dbDialect, "db-dialect", defaultManagerDBDialect, "database dialect")
	flag.IntVar(&cfg.dbMaxIdleConns, "db-max-idle-conns", defaultManagerDBMaxIdleConns, "database max idle connections")
	flag.IntVar(&cfg.dbMaxOpenConns, "db-max-open-conns", defaultManagerDBMaxOpenConns, "database max open connections")
	flag.StringVar(&cfg.proxy, "proxy", defaultManagerProxy, "websocket proxy URL")
	flag.DurationVar(&cfg.timeout, "timeout", defaultManagerTimeout, "overall batch timeout")
	flag.IntVar(&cfg.connectRetries, "connect-retries", defaultManagerConnectRetries, "connect retry attempts per account")
	flag.DurationVar(&cfg.connectRetryWait, "connect-retry-delay", 3*time.Second, "delay between connect retries")
	flag.DurationVar(&cfg.connectReadyWait, "connect-ready-wait", defaultManagerConnectReadyWait, "wait for authentication after websocket connect")
	flag.DurationVar(&cfg.transportTimeout, "transport-timeout", 90*time.Second, "proxy transport timeout")
	flag.DurationVar(&cfg.stayFor, "stay-for", defaultManagerStayFor, "how long to keep connected accounts online before disconnecting")
	flag.BoolVar(&cfg.recoverAppState, "recover-app-state", defaultManagerRecoverAppState, "recover app-state keys after connecting")
	flag.BoolVar(&cfg.requireAppState, "require-app-state", false, "mark account failed if app-state key recovery does not store a key")
	flag.BoolVar(&cfg.accountDBLock, "account-db-lock", defaultManagerAccountDBLock, "use a Postgres advisory lock to avoid connecting the same device JID from multiple manager processes")
	flag.DurationVar(&cfg.appStateKeyWait, "app-state-key-wait", 60*time.Second, "app-state key recovery wait")
	flag.BoolVar(&cfg.debug, "debug", defaultManagerDebug, "enable debug logs")
	flag.Parse()
	return cfg
}

func run(cfg managerConfig) error {
	if cfg.dbURI == "" {
		return errors.New("-db is required for persistent lifecycle testing")
	}
	paths, err := collectCredentialPaths(cfg)
	if err != nil {
		return err
	}
	if len(paths) == 0 {
		return errors.New("no credential files found")
	}
	baseCtx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()
	ctx, cancel := context.WithTimeout(baseCtx, cfg.timeout)
	defer cancel()

	logger := waLog.Noop
	if cfg.debug {
		logger = waLog.Stdout("Manager", "DEBUG", true)
	}

	container, db, err := openSharedContainer(ctx, cfg, logger)
	if err != nil {
		return err
	}
	defer container.Close()

	jobs, err := loadAccountJobs(paths)
	if err != nil {
		return err
	}
	if len(jobs) == 0 {
		return errors.New("no unique credential accounts loaded")
	}
	if cfg.concurrency < 1 {
		cfg.concurrency = 1
	}
	if cfg.concurrency > len(jobs) {
		cfg.concurrency = len(jobs)
	}
	fmt.Printf("using persistent store: %s\n", redactURI(cfg.dbURI))
	fmt.Printf("loading accounts: files=%d unique_accounts=%d concurrency=%d stay_for=%s recover_app_state=%t require_app_state=%t account_db_lock=%t\n", len(paths), len(jobs), cfg.concurrency, cfg.stayFor, cfg.recoverAppState, cfg.requireAppState, cfg.accountDBLock)
	results := runBatch(ctx, cfg, container, db, jobs)
	printSummary(results)
	return nil
}

func collectCredentialPaths(cfg managerConfig) ([]string, error) {
	seen := make(map[string]struct{})
	var paths []string
	addPath := func(path string) {
		path = strings.TrimSpace(path)
		if path == "" {
			return
		}
		if _, ok := seen[path]; ok {
			return
		}
		seen[path] = struct{}{}
		paths = append(paths, path)
	}
	for _, path := range cfg.creds {
		addPath(path)
	}
	for _, path := range strings.Split(cfg.credsList, ",") {
		addPath(path)
	}
	if cfg.credsDir != "" {
		credsDir, err := resolveRelativePath(cfg.credsDir)
		if err != nil {
			return nil, err
		}
		entries, err := os.ReadDir(credsDir)
		if err != nil {
			return nil, fmt.Errorf("read creds dir: %w", err)
		}
		for _, entry := range entries {
			if entry.IsDir() || !strings.EqualFold(filepath.Ext(entry.Name()), ".json") {
				continue
			}
			addPath(filepath.Join(credsDir, entry.Name()))
		}
	}
	sort.Strings(paths)
	if cfg.limit > 0 && len(paths) > cfg.limit {
		paths = paths[:cfg.limit]
	}
	return paths, nil
}

func resolveRelativePath(path string) (string, error) {
	if filepath.IsAbs(path) {
		return path, nil
	}
	if _, err := os.Stat(path); err == nil {
		return path, nil
	}
	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		candidate := filepath.Join(wd, path)
		if _, err = os.Stat(candidate); err == nil {
			return candidate, nil
		}
		parent := filepath.Dir(wd)
		if parent == wd {
			break
		}
		wd = parent
	}
	return "", fmt.Errorf("path %q not found from current directory or parent directories", path)
}

func loadAccountJobs(paths []string) ([]accountJob, error) {
	jobs := make([]accountJob, 0, len(paths))
	seenJID := make(map[string]accountJob, len(paths))
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read creds %s: %w", path, err)
		}
		imported, err := baileysauth.Parse(data)
		if err != nil {
			return nil, fmt.Errorf("parse creds %s: %w", path, err)
		}
		jid := imported.Device.GetJID().String()
		if previous, ok := seenJID[jid]; ok {
			fmt.Printf("skipping duplicate account jid=%s path=%s duplicate_of=%s\n", jid, path, previous.Path)
			continue
		}
		job := accountJob{
			Index: len(jobs) + 1,
			Path:  path,
			JSON:  data,
			JID:   jid,
			LID:   imported.Device.GetLID().String(),
		}
		seenJID[jid] = job
		jobs = append(jobs, job)
	}
	return jobs, nil
}

func openSharedContainer(ctx context.Context, cfg managerConfig, logger waLog.Logger) (*sqlstore.Container, *sql.DB, error) {
	if cfg.dbDialect == "postgres" {
		sqlstore.PostgresArrayWrapper = pq.Array
	}
	db, err := sql.Open(cfg.dbDialect, cfg.dbURI)
	if err != nil {
		return nil, nil, fmt.Errorf("open database: %w", err)
	}
	if cfg.dbMaxIdleConns > 0 {
		db.SetMaxIdleConns(cfg.dbMaxIdleConns)
	}
	if cfg.dbMaxOpenConns > 0 {
		db.SetMaxOpenConns(cfg.dbMaxOpenConns)
	}
	container := sqlstore.NewWithDB(db, cfg.dbDialect, logger)
	if err = container.Upgrade(ctx); err != nil {
		_ = container.Close()
		return nil, nil, fmt.Errorf("upgrade database: %w", err)
	}
	return container, db, nil
}

func runBatch(ctx context.Context, cfg managerConfig, container *sqlstore.Container, db *sql.DB, jobs []accountJob) []accountResult {
	jobCh := make(chan accountJob)
	resultCh := make(chan accountResult, len(jobs))

	var workers sync.WaitGroup
	for workerID := 0; workerID < cfg.concurrency; workerID++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for job := range jobCh {
				resultCh <- runAccount(ctx, cfg, container, db, job)
			}
		}()
	}

	go func() {
		defer close(jobCh)
		for _, job := range jobs {
			select {
			case <-ctx.Done():
				return
			case jobCh <- job:
			}
		}
	}()

	go func() {
		workers.Wait()
		close(resultCh)
	}()

	results := make([]accountResult, 0, len(jobs))
	for result := range resultCh {
		results = append(results, result)
	}
	sort.Slice(results, func(i, j int) bool {
		return results[i].Index < results[j].Index
	})
	return results
}

type accountLock struct {
	conn *sql.Conn
	key  int64
	jid  string
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

func acquireAccountLock(ctx context.Context, cfg managerConfig, db *sql.DB, job accountJob) (*accountLock, error) {
	if !cfg.accountDBLock || cfg.dbDialect != "postgres" {
		return nil, nil
	}
	key := accountLockKey(job.JID)
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire account DB lock connection for %s: %w", job.JID, err)
	}
	var locked bool
	if err = conn.QueryRowContext(ctx, `SELECT pg_try_advisory_lock($1)`, key).Scan(&locked); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("acquire account DB lock for %s: %w", job.JID, err)
	}
	if !locked {
		_ = conn.Close()
		return nil, fmt.Errorf("account %s is already locked by another manager process", job.JID)
	}
	return &accountLock{conn: conn, key: key, jid: job.JID}, nil
}

func accountLockKey(jid string) int64 {
	hash := fnv.New64a()
	_, _ = hash.Write([]byte("whatsmeow-manager-account:"))
	_, _ = hash.Write([]byte(jid))
	return int64(hash.Sum64())
}

func runAccount(ctx context.Context, cfg managerConfig, container *sqlstore.Container, db *sql.DB, job accountJob) (result accountResult) {
	result = accountResult{
		Index:   job.Index,
		Path:    job.Path,
		JID:     job.JID,
		LID:     job.LID,
		Started: time.Now(),
		Status:  statusStarting,
	}
	label := fmt.Sprintf("%02d %s", job.Index, filepath.Base(job.Path))
	logger := waLog.Noop
	if cfg.debug {
		logger = waLog.Stdout("Manager/"+fmt.Sprintf("%02d", job.Index), "DEBUG", true)
	}

	lock, err := acquireAccountLock(ctx, cfg, db, job)
	if err != nil {
		result.Status = statusLocked
		result.Err = err
		return result
	}
	defer lock.release()

	account, err := importedclient.Open(ctx, importedclient.Config{
		CredsJSON:        job.JSON,
		Container:        container,
		Logger:           logger,
		Proxy:            cfg.proxy,
		TransportTimeout: cfg.transportTimeout,
		AppStateKeyWait:  cfg.appStateKeyWait,
	})
	if err != nil {
		result.Err = err
		return result
	}
	result.JID = account.Device.ID.String()
	result.LID = account.Device.LID.String()
	result.AppStateKeyID = printableKeyID(account.Imported.MyAppStateKeyID)
	result.ExistingDevice = account.Imported.ExistingDevice
	result.StateReset = account.Imported.StateReset
	result.StateResetReason = account.Imported.StateResetReason
	result.Status = statusImported
	result.AppStateKeysBefore = countAppStateKeys(ctx, account)

	tracker := newLifecycleTracker(label)
	handlerID := account.Client.AddEventHandler(tracker.handleEvent)
	defer account.Client.RemoveEventHandler(handlerID)
	defer func() {
		account.Close()
		result.AppStateKeysAfter = countAppStateKeys(ctx, account)
		if result.Err == nil {
			tracker.setStatus(statusClosed)
		}
		result.Status = tracker.statusValue()
		result.Offline = time.Now()
	}()

	fmt.Printf("[%s] imported jid=%s lid=%s existing=%t state_reset=%t reason=%s app_state_keys=%d latest_key_id=%s\n", label, result.JID, result.LID, result.ExistingDevice, result.StateReset, result.StateResetReason, result.AppStateKeysBefore, result.AppStateKeyID)
	tracker.setStatus(statusConnecting)
	if err = connectWithRetry(ctx, cfg, account, tracker); err != nil {
		result.Err = err
		return result
	}
	result.Online = time.Now()
	fmt.Printf("[%s] online jid=%s\n", label, result.JID)
	if err = tracker.pollTerminal(); err != nil {
		result.Err = err
		return result
	}

	if cfg.recoverAppState {
		result.AppStateErr = recoverAppStateKeys(ctx, cfg, account, tracker)
		result.AppStateKeysAfter = countAppStateKeys(ctx, account)
		if result.AppStateErr != nil {
			fmt.Printf("[%s] warning: app-state key recovery failed: keys=%d err=%v\n", label, result.AppStateKeysAfter, result.AppStateErr)
			if isTerminalStatus(tracker.statusValue()) || !account.Client.IsConnected() || !account.Client.IsLoggedIn() {
				result.Err = result.AppStateErr
				return result
			}
			if cfg.requireAppState {
				result.Err = result.AppStateErr
				return result
			}
		} else {
			fmt.Printf("[%s] app-state keys ready: before=%d after=%d\n", label, result.AppStateKeysBefore, result.AppStateKeysAfter)
		}
	} else {
		result.AppStateKeysAfter = countAppStateKeys(ctx, account)
	}

	if err = holdOnline(ctx, tracker, cfg.stayFor); err != nil {
		result.Err = err
		return result
	}
	fmt.Printf("[%s] disconnecting jid=%s\n", label, result.JID)
	return result
}

func connectWithRetry(ctx context.Context, cfg managerConfig, account *importedclient.Account, tracker *lifecycleTracker) error {
	retries := cfg.connectRetries
	if retries < 1 {
		retries = 1
	}
	var lastErr error
	for attempt := 1; attempt <= retries; attempt++ {
		lastErr = account.Connect()
		if lastErr == nil {
			if err := waitForAccountReady(ctx, account, tracker, cfg.connectReadyWait); err != nil {
				lastErr = err
			} else {
				return nil
			}
		}
		if err := tracker.pollTerminal(); err != nil {
			lastErr = err
		}
		if errors.Is(lastErr, store.ErrDeviceDeleted) {
			if tracker.statusValue() == statusConnecting {
				tracker.setStatus(statusDeleted)
			}
			return lastErr
		}
		if isTerminalStatus(tracker.statusValue()) {
			return lastErr
		}
		account.Client.Disconnect()
		if attempt == retries {
			break
		}
		fmt.Printf("[%s] connect attempt %d/%d failed: %v; retrying in %s\n", tracker.label, attempt, retries, lastErr, cfg.connectRetryWait)
		if err := sleepContext(ctx, cfg.connectRetryWait); err != nil {
			return err
		}
		tracker.setStatus(statusConnecting)
	}
	return lastErr
}

func waitForAccountReady(ctx context.Context, account *importedclient.Account, tracker *lifecycleTracker, wait time.Duration) error {
	if wait <= 0 {
		wait = defaultManagerConnectReadyWait
	}
	readyCh := make(chan bool, 1)
	go func() {
		readyCh <- account.Client.WaitForConnection(wait)
	}()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-tracker.terminal:
			return err
		case ready := <-readyCh:
			if ready {
				return nil
			}
			if err := tracker.pollTerminal(); err != nil {
				return err
			}
			if account.Client.IsConnected() && account.Client.IsLoggedIn() {
				return nil
			}
			return fmt.Errorf("timed out waiting %s for authenticated connection", wait)
		}
	}
}

func recoverAppStateKeys(ctx context.Context, cfg managerConfig, account *importedclient.Account, tracker *lifecycleTracker) error {
	if account == nil || account.Client == nil || account.Imported == nil {
		return errors.New("account is not initialized")
	}
	before := countAppStateKeys(ctx, account)
	if before > 0 {
		fmt.Printf("[%s] app-state keys already persisted: count=%d\n", tracker.label, before)
		return nil
	}
	if err := tracker.pollTerminal(); err != nil {
		return err
	}
	recoveryCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	terminalErrCh := make(chan error, 1)
	go func() {
		select {
		case err := <-tracker.terminal:
			terminalErrCh <- err
			cancel()
		case <-recoveryCtx.Done():
		}
	}()
	fmt.Printf("[%s] recovering app-state keys: latest_key_id=%s wait=%s\n", tracker.label, printableKeyID(account.Imported.MyAppStateKeyID), cfg.appStateKeyWait)
	err := account.Client.RecoverAppStateKeys(recoveryCtx, account.Imported.MyAppStateKeyID, cfg.appStateKeyWait)
	select {
	case terminalErr := <-terminalErrCh:
		if terminalErr != nil {
			return terminalErr
		}
	default:
	}
	return err
}

func isTerminalStatus(status managerStatus) bool {
	switch status {
	case statusLoggedOut, statusDeleted, statusReplaced, statusConnectFail, statusTempBan:
		return true
	default:
		return false
	}
}

func countAppStateKeys(ctx context.Context, account *importedclient.Account) int {
	if account == nil || account.Client == nil {
		return 0
	}
	count, err := account.Client.AppStateKeyCount(ctx)
	if err != nil {
		return 0
	}
	return count
}

func holdOnline(ctx context.Context, tracker *lifecycleTracker, stayFor time.Duration) error {
	if stayFor <= 0 {
		return nil
	}
	timer := time.NewTimer(stayFor)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-tracker.terminal:
		return err
	case <-timer.C:
		return nil
	}
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

func printSummary(results []accountResult) {
	var okCount int
	fmt.Println("account summary:")
	for _, result := range results {
		if result.Err == nil {
			okCount++
			fmt.Printf(
				"  [%02d] ok status=%s jid=%s existing=%t state_reset=%t app_state_keys=%d->%d app_state_err=%v online_at=%s path=%s\n",
				result.Index,
				result.Status,
				result.JID,
				result.ExistingDevice,
				result.StateReset,
				result.AppStateKeysBefore,
				result.AppStateKeysAfter,
				result.AppStateErr,
				formatTime(result.Online),
				result.Path,
			)
			continue
		}
		fmt.Printf(
			"  [%02d] failed status=%s jid=%s existing=%t state_reset=%t app_state_keys=%d->%d app_state_err=%v err=%v path=%s\n",
			result.Index,
			result.Status,
			result.JID,
			result.ExistingDevice,
			result.StateReset,
			result.AppStateKeysBefore,
			result.AppStateKeysAfter,
			result.AppStateErr,
			result.Err,
			result.Path,
		)
	}
	fmt.Printf("summary: total=%d ok=%d failed=%d\n", len(results), okCount, len(results)-okCount)
}

func printableKeyID(keyID []byte) string {
	if len(keyID) == 0 {
		return ""
	}
	return strings.ToUpper(hex.EncodeToString(keyID))
}

func formatTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.Format(time.RFC3339)
}

func redactURI(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.User == nil {
		return raw
	}
	if username := parsed.User.Username(); username != "" {
		parsed.User = url.UserPassword(username, "redacted")
	}
	return parsed.String()
}
