// Copyright (c) 2026 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"go.mau.fi/whatsmeow/baileysauth"
	waLog "go.mau.fi/whatsmeow/util/log"
)

const (
	// 这个命令是一个“业务 Manager 骨架 demo”，不是要放进 whatsmeow 包里的通用 Manager。
	// 这里把数据库和代理写死，是为了快速测试导入、登录、上线、下线、状态回调这些主链路。
	// 真正迁移到业务项目时，可以把这些值改成从配置文件、环境变量或业务数据库读取。
	defaultCredsDir       = "docs/keypair"
	defaultLimit          = 10
	defaultConcurrency    = 10
	demoStoreDSN          = "postgres://admin:admin123@127.0.0.1:5432/app?sslmode=disable"
	demoStoreDialect      = "postgres"
	demoStoreMaxIdleConns = 10
	demoStoreMaxOpenConns = 100
	demoProxy             = "socks5://wefanvip1_1:MKLP123456@proxyus.rola.vip:2000"
)

// config 只保存这个 demo 需要的命令行参数。
// 在正式服务里，这些值通常不从 flag 读取，而是由业务配置、账号表、代理池等模块提供。
type config struct {
	mode              string
	credsDir          string
	jid               string
	phone             string
	limit             int
	concurrency       int
	timeout           time.Duration
	stayFor           time.Duration
	connectRetries    int
	connectRetryDelay time.Duration
	requireAppState   bool
	appStateKeyWait   time.Duration
	debug             bool
}

// job 表示一个账号任务。
// - import 模式：json 字段会包含外部 Baileys JSON 账号内容；
// - login 模式：json 为空，Manager 会根据 jid 或 phone 从 Postgres 中读取已持久化的设备。
type job struct {
	index int
	path  string
	json  []byte
	jid   string
	phone string
}

// result 是单个账号任务的最终结果，用来在命令行最后打印汇总。
// 这里保留 snapshot，是为了即使账号失败，也能看到失败时的状态、离线原因、app-state key 数量等信息。
type result struct {
	index    int
	path     string
	snapshot AccountStatusSnapshot
	err      error
}

func main() {
	cfg := parseFlags()
	if err := run(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "imported manager demo failed: %v\n", err)
		os.Exit(1)
	}
}

func parseFlags() config {
	var cfg config
	flag.StringVar(&cfg.mode, "mode", "import", "run mode: import reads JSON credentials; login connects stored DB accounts")
	flag.StringVar(&cfg.credsDir, "creds-dir", defaultCredsDir, "directory containing Baileys JSON credential files")
	flag.StringVar(&cfg.jid, "jid", "", "stored account JID for -mode login")
	flag.StringVar(&cfg.phone, "phone", "", "stored account phone for -mode login")
	flag.IntVar(&cfg.limit, "limit", defaultLimit, "maximum number of unique accounts to load; 0 means all")
	flag.IntVar(&cfg.concurrency, "concurrency", defaultConcurrency, "number of accounts to connect concurrently")
	flag.DurationVar(&cfg.timeout, "timeout", 0, "overall demo timeout; 0 means no timeout")
	flag.DurationVar(&cfg.stayFor, "stay-for", 0, "how long each connected account stays online before disconnecting; 0 means until Ctrl+C")
	flag.IntVar(&cfg.connectRetries, "connect-retries", 5, "connect retry attempts per account")
	flag.DurationVar(&cfg.connectRetryDelay, "connect-retry-delay", 3*time.Second, "delay between connect retries")
	flag.BoolVar(&cfg.requireAppState, "require-app-state", false, "fail accounts that cannot recover app-state keys")
	flag.DurationVar(&cfg.appStateKeyWait, "app-state-wait", 60*time.Second, "how long to wait for app-state key recovery")
	flag.BoolVar(&cfg.debug, "debug", true, "enable whatsmeow debug logs")
	flag.Parse()
	return cfg
}

func run(cfg config) error {
	baseCtx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()
	ctx := baseCtx
	cancel := func() {}
	if cfg.timeout > 0 {
		ctx, cancel = context.WithTimeout(baseCtx, cfg.timeout)
	}
	defer cancel()

	logger := waLog.Noop
	if cfg.debug {
		logger = waLog.Stdout("ImportedManager", "DEBUG", true)
	}
	manager, err := NewManager(ctx, Config{
		StoreDialect:      demoStoreDialect,
		StoreDSN:          demoStoreDSN,
		StoreMaxIdleConns: demoStoreMaxIdleConns,
		StoreMaxOpenConns: demoStoreMaxOpenConns,
		ProxyURL:          demoProxy,
		TransportTimeout:  90 * time.Second,
		ConnectRetries:    cfg.connectRetries,
		ConnectRetryDelay: cfg.connectRetryDelay,
		ConnectTimeout:    30 * time.Second,
		RecoverAppState:   true,
		RequireAppState:   cfg.requireAppState,
		AppStateKeyWait:   cfg.appStateKeyWait,
		AccountDBLock:     true,
	}, logger)
	if err != nil {
		return err
	}
	defer manager.Close()

	// 原始事件回调主要用于调试。
	// whatsmeow 的底层事件比较细，例如 Connected、Disconnected、LoggedOut、StreamReplaced 等。
	// 生产业务如果只关心账号状态，一般不需要直接处理这些事件，只保留下面的业务状态回调即可。
	manager.SetAccountEventHandler(func(ctx context.Context, account *ManagedAccount, evt any) error {
		fmt.Printf("[%s] event=%T\n", account.Label, evt)
		return nil
	})
	// 业务状态回调是业务服务最应该接的回调。
	// 它把复杂的协议事件统一成三种主状态：
	// - online：账号已上线，可用；
	// - offline：账号离线，同时带离线原因；
	// - banned：账号被 WhatsApp 临时限制。
	// 业务层可以在这里更新自己的 account 表，例如 status、offline_reason、last_online_at、last_error。
	manager.SetBusinessStatusHandler(func(ctx context.Context, change BusinessStatusChange) error {
		fmt.Printf(
			"[%s] business_status=%s reason=%s decision=%s old=%s/%s err=%s\n",
			change.Snapshot.Label,
			change.NewStatus,
			change.NewOfflineReason,
			change.Decision,
			change.OldStatus,
			change.OldOfflineReason,
			change.Error,
		)
		return nil
	})

	jobs, err := loadJobs(ctx, manager, cfg)
	if err != nil {
		return err
	}
	if len(jobs) == 0 {
		return errors.New("no accounts found")
	}
	if cfg.concurrency < 1 {
		cfg.concurrency = 1
	}
	if cfg.concurrency > len(jobs) {
		cfg.concurrency = len(jobs)
	}

	fmt.Printf("using persistent store: %s\n", redactURI(demoStoreDSN))
	fmt.Printf("loading accounts: mode=%s unique_accounts=%d concurrency=%d stay_for=%s require_app_state=%t proxy=%s\n", cfg.mode, len(jobs), cfg.concurrency, formatStayFor(cfg.stayFor), cfg.requireAppState, redactURI(demoProxy))
	results := runBatch(ctx, manager, cfg, jobs)
	printSummary(results)
	return nil
}

func loadJobs(ctx context.Context, manager *Manager, cfg config) ([]job, error) {
	switch strings.ToLower(strings.TrimSpace(cfg.mode)) {
	case "", "import":
		return loadImportJobs(cfg)
	case "login":
		return loadStoredJobs(ctx, manager, cfg)
	default:
		return nil, fmt.Errorf("unsupported -mode %q", cfg.mode)
	}
}

// loadImportJobs 扫描 JSON 文件，并按设备 JID 去重。
// 同一个 WhatsApp linked device 只能同时保持一个 websocket 连接。
// 如果同一份 JSON 被并发导入/登录，多数情况下会出现 stream_replaced，所以这里先在任务层去重。
func loadImportJobs(cfg config) ([]job, error) {
	dir, err := resolveRelativePath(cfg.credsDir)
	if err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read creds dir: %w", err)
	}
	paths := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.EqualFold(filepath.Ext(entry.Name()), ".json") {
			continue
		}
		paths = append(paths, filepath.Join(dir, entry.Name()))
	}
	sort.Strings(paths)

	jobs := make([]job, 0, len(paths))
	seen := make(map[string]string, len(paths))
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			fmt.Printf("skipping unreadable creds path=%s err=%v\n", path, err)
			continue
		}
		imported, err := baileysauth.Parse(data)
		if err != nil {
			fmt.Printf("skipping invalid creds path=%s err=%v\n", path, err)
			continue
		}
		jid := imported.Device.GetJID().String()
		if previous := seen[jid]; previous != "" {
			fmt.Printf("skipping duplicate account jid=%s path=%s duplicate_of=%s\n", jid, path, previous)
			continue
		}
		seen[jid] = path
		jobs = append(jobs, job{
			index: len(jobs) + 1,
			path:  path,
			json:  data,
			jid:   jid,
			phone: phoneFromJID(imported.Device.GetJID().User),
		})
		if cfg.limit > 0 && len(jobs) >= cfg.limit {
			break
		}
	}
	return jobs, nil
}

// loadStoredJobs 是“直接登录数据库里已有账号”的任务加载逻辑。
// 如果传入 -jid 或 -phone，只登录指定账号；
// 如果都不传，则从 Postgres 中加载所有已持久化设备，再结合 -limit 控制数量。
func loadStoredJobs(ctx context.Context, manager *Manager, cfg config) ([]job, error) {
	if cfg.jid != "" || cfg.phone != "" {
		return []job{{
			index: 1,
			jid:   strings.TrimSpace(cfg.jid),
			phone: normalizePhone(cfg.phone),
			path:  "postgres",
		}}, nil
	}
	devices, err := manager.Container().GetAllDevices(ctx)
	if err != nil {
		return nil, err
	}
	sort.Slice(devices, func(i, j int) bool {
		if devices[i] == nil || devices[i].ID == nil {
			return false
		}
		if devices[j] == nil || devices[j].ID == nil {
			return true
		}
		return devices[i].ID.String() < devices[j].ID.String()
	})
	jobs := make([]job, 0, len(devices))
	for _, device := range devices {
		if device == nil || device.ID == nil {
			continue
		}
		jobs = append(jobs, job{
			index: len(jobs) + 1,
			jid:   device.ID.String(),
			phone: phoneFromJID(device.ID.User),
			path:  "postgres",
		})
		if cfg.limit > 0 && len(jobs) >= cfg.limit {
			break
		}
	}
	return jobs, nil
}

// runBatch 并发执行多个账号任务。
// 这个函数模拟正式业务里的 AccountManager 批量上线行为，但不写业务表，也不做业务重试策略。
// 业务项目迁移时，可以把这里替换成自己的调度器、账号队列、代理分配逻辑。
func runBatch(ctx context.Context, manager *Manager, cfg config, jobs []job) []result {
	jobCh := make(chan job)
	resultCh := make(chan result, len(jobs))

	var wg sync.WaitGroup
	for worker := 0; worker < cfg.concurrency; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for item := range jobCh {
				resultCh <- runOne(ctx, manager, cfg, item)
			}
		}()
	}
	go func() {
		defer close(jobCh)
		for _, item := range jobs {
			select {
			case <-ctx.Done():
				return
			case jobCh <- item:
			}
		}
	}()
	go func() {
		wg.Wait()
		close(resultCh)
	}()

	results := make([]result, 0, len(jobs))
	for item := range resultCh {
		results = append(results, item)
	}
	sort.Slice(results, func(i, j int) bool {
		return results[i].index < results[j].index
	})
	return results
}

// runOne 处理单个账号：
// 1. import 模式下导入 JSON 并连接；
// 2. login 模式下直接从 Postgres 读取设备并连接；
// 3. 上线后保持 stayFor 时间；如果 stayFor<=0，则一直在线直到 Ctrl+C；
// 4. 时间到或收到退出信号后调用 CancelAccount 做本地断开。
// CancelAccount 只是关闭当前 Manager 里的连接，不会删除 WhatsApp 设备，也不会删除数据库里的账号。
func runOne(ctx context.Context, manager *Manager, cfg config, item job) result {
	label := fmt.Sprintf("%02d/%s", item.index, filepath.Base(item.path))
	var resp *ImportResponse
	var err error
	if strings.EqualFold(cfg.mode, "login") {
		resp, err = manager.ConnectStored(ctx, LoginRequest{
			ID:    int64(item.index),
			Phone: item.phone,
			JID:   item.jid,
			Label: label,
		})
	} else {
		resp, err = manager.ImportAndConnect(ctx, ImportRequest{
			ID:        int64(item.index),
			Phone:     item.phone,
			Label:     label,
			CredsJSON: item.json,
		})
	}
	if err != nil {
		snapshot := failedSnapshot(item, err)
		if resp != nil {
			if current, ok := manager.GetStatus(resp.Phone, resp.ID); ok {
				snapshot = current
			}
		}
		return result{index: item.index, path: item.path, snapshot: snapshot, err: err}
	}
	snapshot, _ := manager.GetStatus(resp.Phone, resp.ID)
	fmt.Printf("[%02d] online jid=%s status=%s app_state_keys=%d->%d\n", item.index, snapshot.JID, snapshot.Status, snapshot.AppStateKeysBefore, snapshot.AppStateKeysAfter)
	waitUntilDisconnect(ctx, cfg.stayFor)
	if err = manager.CancelAccount(resp.Phone, resp.ID); err != nil {
		current, _ := manager.GetStatus(resp.Phone, resp.ID)
		return result{index: item.index, path: item.path, snapshot: current, err: err}
	}
	snapshot.Status = AccountStatusClosed
	snapshot.BusinessStatus = BusinessStatusOffline
	snapshot.OfflineReason = OfflineReasonClosed
	snapshot.Decision = decisionForStatus(snapshot.BusinessStatus, snapshot.OfflineReason)
	snapshot.ClosedAt = time.Now()
	return result{index: item.index, path: item.path, snapshot: snapshot}
}

func failedSnapshot(item job, err error) AccountStatusSnapshot {
	status := classifyStatus(err)
	if status == "" {
		status = AccountStatusFailed
	}
	reason := offlineReasonForAccountStatus(status)
	return AccountStatusSnapshot{
		ID:             int64(item.index),
		Phone:          item.phone,
		Key:            AccountKey(item.phone, int64(item.index)),
		Label:          fmt.Sprintf("%02d/%s", item.index, filepath.Base(item.path)),
		JID:            item.jid,
		Status:         status,
		BusinessStatus: businessStatusForAccountStatus(status),
		OfflineReason:  reason,
		Decision:       decisionForStatus(businessStatusForAccountStatus(status), reason),
		Error:          err.Error(),
		UpdatedAt:      time.Now(),
	}
}

func printSummary(results []result) {
	var okCount int
	fmt.Println("account summary:")
	for _, item := range results {
		if item.err == nil {
			okCount++
			fmt.Printf(
				"  [%02d] ok status=%s business=%s reason=%s decision=%s jid=%s existing=%t state_reset=%t app_state_ready=%t app_state_keys=%d->%d app_state_err=%s online_at=%s path=%s\n",
				item.index,
				item.snapshot.Status,
				item.snapshot.BusinessStatus,
				item.snapshot.OfflineReason,
				item.snapshot.Decision,
				item.snapshot.JID,
				item.snapshot.ExistingDevice,
				item.snapshot.StateReset,
				item.snapshot.AppStateReady,
				item.snapshot.AppStateKeysBefore,
				item.snapshot.AppStateKeysAfter,
				item.snapshot.AppStateError,
				formatTime(item.snapshot.OnlineAt),
				item.path,
			)
			continue
		}
		fmt.Printf(
			"  [%02d] failed status=%s business=%s reason=%s decision=%s jid=%s existing=%t state_reset=%t app_state_ready=%t app_state_keys=%d->%d app_state_err=%s err=%v path=%s\n",
			item.index,
			item.snapshot.Status,
			item.snapshot.BusinessStatus,
			item.snapshot.OfflineReason,
			item.snapshot.Decision,
			item.snapshot.JID,
			item.snapshot.ExistingDevice,
			item.snapshot.StateReset,
			item.snapshot.AppStateReady,
			item.snapshot.AppStateKeysBefore,
			item.snapshot.AppStateKeysAfter,
			item.snapshot.AppStateError,
			item.err,
			item.path,
		)
	}
	fmt.Printf("summary: total=%d ok=%d failed=%d\n", len(results), okCount, len(results)-okCount)
}

func waitUntilDisconnect(ctx context.Context, stayFor time.Duration) {
	if stayFor <= 0 {
		<-ctx.Done()
		return
	}
	_ = sleepContext(ctx, stayFor)
}

func formatStayFor(stayFor time.Duration) string {
	if stayFor <= 0 {
		return "until_ctrl_c"
	}
	return stayFor.String()
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
