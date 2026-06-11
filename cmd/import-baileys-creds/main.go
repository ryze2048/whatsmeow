package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"image"
	_ "image/jpeg"
	_ "image/png"
	"mime"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/lib/pq"
	"golang.org/x/net/proxy"
	"google.golang.org/protobuf/proto"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/appstate"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/proto/waAdv"
	waE2E "go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waSyncAction"
	"go.mau.fi/whatsmeow/proto/waWa6"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"go.mau.fi/whatsmeow/util/keys"
	waLog "go.mau.fi/whatsmeow/util/log"
)

type baileysBytes struct {
	raw json.RawMessage
}

func (bb *baileysBytes) UnmarshalJSON(data []byte) error {
	bb.raw = append(bb.raw[:0], data...)
	return nil
}

func (bb baileysBytes) Bytes() ([]byte, error) {
	return decodeFlexibleBytes(bb.raw)
}

type baileysKeyPair struct {
	Private baileysBytes `json:"private"`
	Public  baileysBytes `json:"public"`
}

type baileysCreds struct {
	Phone   string `json:"Phone"`
	Account struct {
		AccountSignature    baileysBytes `json:"accountSignature"`
		AccountSignatureKey baileysBytes `json:"accountSignatureKey"`
		Details             baileysBytes `json:"details"`
		DeviceSignature     baileysBytes `json:"deviceSignature"`
	} `json:"account"`
	AdvSecretKey            baileysBytes    `json:"advSecretKey"`
	FirstUnuploadedPreKeyID uint32          `json:"firstUnuploadedPreKeyId"`
	Me                      baileysIdentity `json:"me"`
	MyAppStateKeyID         string          `json:"myAppStateKeyId"`
	NextPreKeyID            uint32          `json:"nextPreKeyId"`
	NoiseKey                baileysKeyPair  `json:"noiseKey"`
	Platform                string          `json:"platform"`
	RegistrationID          uint32          `json:"registrationId"`
	SignedIdentityKey       baileysKeyPair  `json:"signedIdentityKey"`
	SignedPreKey            struct {
		KeyID     uint32         `json:"keyId"`
		KeyPair   baileysKeyPair `json:"keyPair"`
		Signature baileysBytes   `json:"signature"`
	} `json:"signedPreKey"`
}

type baileysAppStateSyncKeyData struct {
	KeyData     baileysBytes                   `json:"keyData"`
	Fingerprint *baileysAppStateKeyFingerprint `json:"fingerprint"`
	Timestamp   json.RawMessage                `json:"timestamp"`
}

type baileysAppStateKeyFingerprint struct {
	RawID         *uint32  `json:"rawId"`
	RawIDAlt      *uint32  `json:"rawID"`
	CurrentIndex  *uint32  `json:"currentIndex"`
	DeviceIndexes []uint32 `json:"deviceIndexes"`
}

type baileysIdentity struct {
	ID   string `json:"id"`
	LID  string `json:"lid"`
	Name string `json:"name"`
}

type importedDevice struct {
	device          *store.Device
	myAppStateKeyID []byte
	warnings        []string
}

type memoryStore struct {
	store.NoopStore

	mu sync.Mutex

	device *store.Device

	nextPreKeyID            uint32
	preKeys                 map[uint32]*keys.PreKey
	uploadedUpTo            uint32
	uploadedPreKeyCountHint int

	identities map[string][32]byte
	sessions   map[string][]byte
	senderKeys map[string][]byte

	appStateKeys map[string]store.AppStateSyncKey
	appStateMACs map[string]map[string][]byte
	appStateVers map[string]appStateVersion

	lidToPN map[string]types.JID
	pnToLID map[string]types.JID
}

type appStateVersion struct {
	version uint64
	hash    [128]byte
}

func newMemoryStore() *memoryStore {
	return &memoryStore{
		nextPreKeyID: 1,
		preKeys:      make(map[uint32]*keys.PreKey),
		identities:   make(map[string][32]byte),
		sessions:     make(map[string][]byte),
		senderKeys:   make(map[string][]byte),
		appStateKeys: make(map[string]store.AppStateSyncKey),
		appStateMACs: make(map[string]map[string][]byte),
		appStateVers: make(map[string]appStateVersion),
		lidToPN:      make(map[string]types.JID),
		pnToLID:      make(map[string]types.JID),
	}
}

func (ms *memoryStore) PutDevice(ctx context.Context, device *store.Device) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.device = device
	return nil
}

func (ms *memoryStore) DeleteDevice(ctx context.Context, device *store.Device) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if ms.device == device {
		ms.device = nil
	}
	return nil
}

func (ms *memoryStore) PutIdentity(ctx context.Context, address string, key [32]byte) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.identities[address] = key
	return nil
}

func (ms *memoryStore) DeleteAllIdentities(ctx context.Context, phone string) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	for address := range ms.identities {
		if len(address) >= len(phone) && address[:len(phone)] == phone {
			delete(ms.identities, address)
		}
	}
	return nil
}

func (ms *memoryStore) DeleteIdentity(ctx context.Context, address string) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	delete(ms.identities, address)
	return nil
}

func (ms *memoryStore) IsTrustedIdentity(ctx context.Context, address string, key [32]byte) (bool, error) {
	return true, nil
}

func (ms *memoryStore) GetSession(ctx context.Context, address string) ([]byte, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	return cloneBytes(ms.sessions[address]), nil
}

func (ms *memoryStore) HasSession(ctx context.Context, address string) (bool, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	_, ok := ms.sessions[address]
	return ok, nil
}

func (ms *memoryStore) GetManySessions(ctx context.Context, addresses []string) (map[string][]byte, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	out := make(map[string][]byte, len(addresses))
	for _, address := range addresses {
		if session, ok := ms.sessions[address]; ok {
			out[address] = cloneBytes(session)
		} else {
			out[address] = nil
		}
	}
	return out, nil
}

func (ms *memoryStore) PutSession(ctx context.Context, address string, session []byte) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.sessions[address] = cloneBytes(session)
	return nil
}

func (ms *memoryStore) PutManySessions(ctx context.Context, sessions map[string][]byte) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	for address, session := range sessions {
		ms.sessions[address] = cloneBytes(session)
	}
	return nil
}

func (ms *memoryStore) DeleteAllSessions(ctx context.Context, phone string) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	for address := range ms.sessions {
		if len(address) >= len(phone) && address[:len(phone)] == phone {
			delete(ms.sessions, address)
		}
	}
	return nil
}

func (ms *memoryStore) DeleteSession(ctx context.Context, address string) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	delete(ms.sessions, address)
	return nil
}

func (ms *memoryStore) GenOnePreKey(ctx context.Context) (*keys.PreKey, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	key := keys.NewPreKey(ms.nextPreKeyID)
	ms.preKeys[key.KeyID] = key
	ms.nextPreKeyID++
	return key, nil
}

func (ms *memoryStore) GetOrGenPreKeys(ctx context.Context, count uint32) ([]*keys.PreKey, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	out := make([]*keys.PreKey, 0, count)
	for _, key := range ms.preKeys {
		if key.KeyID > ms.uploadedUpTo {
			out = append(out, key)
			if uint32(len(out)) == count {
				return out, nil
			}
		}
	}
	for uint32(len(out)) < count {
		key := keys.NewPreKey(ms.nextPreKeyID)
		ms.preKeys[key.KeyID] = key
		ms.nextPreKeyID++
		out = append(out, key)
	}
	return out, nil
}

func (ms *memoryStore) GetPreKey(ctx context.Context, id uint32) (*keys.PreKey, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	return ms.preKeys[id], nil
}

func (ms *memoryStore) RemovePreKey(ctx context.Context, id uint32) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	delete(ms.preKeys, id)
	return nil
}

func (ms *memoryStore) MarkPreKeysAsUploaded(ctx context.Context, upToID uint32) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if upToID > ms.uploadedUpTo {
		ms.uploadedUpTo = upToID
	}
	return nil
}

func (ms *memoryStore) UploadedPreKeyCount(ctx context.Context) (int, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	count := 0
	for id := range ms.preKeys {
		if id <= ms.uploadedUpTo {
			count++
		}
	}
	if count == 0 && ms.uploadedPreKeyCountHint > 0 {
		return ms.uploadedPreKeyCountHint, nil
	}
	return count, nil
}

func (ms *memoryStore) PutSenderKey(ctx context.Context, group, user string, session []byte) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.senderKeys[group+"\x00"+user] = cloneBytes(session)
	return nil
}

func (ms *memoryStore) GetSenderKey(ctx context.Context, group, user string) ([]byte, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	return cloneBytes(ms.senderKeys[group+"\x00"+user]), nil
}

func (ms *memoryStore) PutAppStateSyncKey(ctx context.Context, id []byte, key store.AppStateSyncKey) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.appStateKeys[string(id)] = store.AppStateSyncKey{
		Data:        cloneBytes(key.Data),
		Fingerprint: cloneBytes(key.Fingerprint),
		Timestamp:   key.Timestamp,
	}
	return nil
}

func (ms *memoryStore) GetAppStateSyncKey(ctx context.Context, id []byte) (*store.AppStateSyncKey, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	key, ok := ms.appStateKeys[string(id)]
	if !ok {
		return nil, nil
	}
	return &store.AppStateSyncKey{
		Data:        cloneBytes(key.Data),
		Fingerprint: cloneBytes(key.Fingerprint),
		Timestamp:   key.Timestamp,
	}, nil
}

func (ms *memoryStore) GetLatestAppStateSyncKeyID(ctx context.Context) ([]byte, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	var latestID []byte
	var latestTS int64
	for id, key := range ms.appStateKeys {
		if latestID == nil || key.Timestamp > latestTS {
			latestID = []byte(id)
			latestTS = key.Timestamp
		}
	}
	return cloneBytes(latestID), nil
}

func (ms *memoryStore) GetAllAppStateSyncKeys(ctx context.Context) ([]*store.AppStateSyncKey, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	out := make([]*store.AppStateSyncKey, 0, len(ms.appStateKeys))
	for _, key := range ms.appStateKeys {
		out = append(out, &store.AppStateSyncKey{
			Data:        cloneBytes(key.Data),
			Fingerprint: cloneBytes(key.Fingerprint),
			Timestamp:   key.Timestamp,
		})
	}
	return out, nil
}

func (ms *memoryStore) AppStateKeyCount() int {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	return len(ms.appStateKeys)
}

func (ms *memoryStore) PutAppStateVersion(ctx context.Context, name string, version uint64, hash [128]byte) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.appStateVers[name] = appStateVersion{version: version, hash: hash}
	return nil
}

func (ms *memoryStore) GetAppStateVersion(ctx context.Context, name string) (uint64, [128]byte, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	version := ms.appStateVers[name]
	return version.version, version.hash, nil
}

func (ms *memoryStore) DeleteAppStateVersion(ctx context.Context, name string) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	delete(ms.appStateVers, name)
	return nil
}

func (ms *memoryStore) PutAppStateMutationMACs(ctx context.Context, name string, version uint64, mutations []store.AppStateMutationMAC) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if ms.appStateMACs[name] == nil {
		ms.appStateMACs[name] = make(map[string][]byte)
	}
	for _, mutation := range mutations {
		ms.appStateMACs[name][string(mutation.IndexMAC)] = cloneBytes(mutation.ValueMAC)
	}
	return nil
}

func (ms *memoryStore) DeleteAppStateMutationMACs(ctx context.Context, name string, indexMACs [][]byte) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	for _, indexMAC := range indexMACs {
		delete(ms.appStateMACs[name], string(indexMAC))
	}
	return nil
}

func (ms *memoryStore) GetAppStateMutationMAC(ctx context.Context, name string, indexMAC []byte) ([]byte, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	return cloneBytes(ms.appStateMACs[name][string(indexMAC)]), nil
}

func (ms *memoryStore) PutManyLIDMappings(ctx context.Context, mappings []store.LIDMapping) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	for _, mapping := range mappings {
		ms.lidToPN[mapping.LID.User] = mapping.PN
		ms.pnToLID[mapping.PN.User] = mapping.LID
	}
	return nil
}

func (ms *memoryStore) PutLIDMapping(ctx context.Context, lid, jid types.JID) error {
	return ms.PutManyLIDMappings(ctx, []store.LIDMapping{{LID: lid, PN: jid}})
}

func (ms *memoryStore) GetPNForLID(ctx context.Context, lid types.JID) (types.JID, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if pn, ok := ms.lidToPN[lid.User]; ok {
		pn.Device = lid.Device
		return pn, nil
	}
	return types.EmptyJID, nil
}

func (ms *memoryStore) GetLIDForPN(ctx context.Context, pn types.JID) (types.JID, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if lid, ok := ms.pnToLID[pn.User]; ok {
		lid.Device = pn.Device
		return lid, nil
	}
	return types.EmptyJID, nil
}

func (ms *memoryStore) GetManyLIDsForPNs(ctx context.Context, pns []types.JID) (map[types.JID]types.JID, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	out := make(map[types.JID]types.JID, len(pns))
	for _, pn := range pns {
		if lid, ok := ms.pnToLID[pn.User]; ok {
			lid.Device = pn.Device
			out[pn] = lid
		}
	}
	return out, nil
}

/*
	env GOCACHE=/private/tmp/whatsmeow-gocache go run . \
	    -creds '../../docs/keypair/244975542278.json' \
	    -connect \
	    -timeout 60s \
	    -proxy 'socks5://wefanvip_1:MKLP123456@proxyus.rola.vip:2000' \
	    -debug
*/

/*
	env GOCACHE=/private/tmp/whatsmeow-gocache go run . \
	  -creds '../../docs/keypair/21698290649.json' \
	  -connect \
	  -timeout 90s \
	   -proxy 'socks5://wefanvip_1:MKLP123456@proxyus.rola.vip:2000' \
	  -send-to '27651432974' \
	  -add-contact \
	  -contact-name 'Test Contact' \
	  -message 'hello from imported whatsmeow creds'
*/

/*
	CREDS='/Users/ryze/GolandProjects/whatsmeow/docs/keypair/244975542278.json' \
	 AUTH_DIR='/private/tmp/baileys-auth-smoke' \
	 PROXY='socks5://wefanvip_1:MKLP123456@proxyus.rola.vip:2000' \
	 WAIT_MS=45000 \
	 yarn tsx smoke-appstate.ts
*/
/*

proxyus.rola.vip:2000:wefanvip1_1:MKLP123456

 env GOCACHE=/private/tmp/whatsmeow-gocache go run . \
    -creds '../../docs/keypair/919904526514.json' \
    -connect \
    -timeout 120s \
    -connect-retries 5 \
    -connect-retry-delay 3s \
    -send-timeout 240s \
    -transport-timeout 90s \
    -proxy 'socks5://wefanvip1_1:MKLP123456@proxyus.rola.vip:2000' \
    -media-proxy 'socks5://wefanvip1_1:MKLP123456@proxyus.rola.vip:2000' \
    -send-to '919948667476' \
    -message-kind template-url \
    -message 'hello world' \
    -media-path '../../docs/image/2026-06-01_180749_007.jpg' \
    -preview-title 'Link' \
    -preview-body 'hello world' \
    -preview-url 'https://example.com/' \
    -preview-button 'click me auto' \
    -control-message 'plain text delivery test' \
    -control-delay 2s \
    -post-send-wait 30s



  删除刚才的消息：

  env GOCACHE=/private/tmp/whatsmeow-gocache go run . \
    -creds '../../docs/keypair/919904526514.json' \
    -connect \
    -timeout 120s \
    -connect-retries 5 \
    -connect-retry-delay 3s \
    -send-timeout 120s \
    -transport-timeout 90s \
    -proxy 'socks5://wefanvip1_1:MKLP123456@proxyus.rola.vip:2000' \
    -send-to '919948667476' \
    -revoke-message-id '3EB05CF6CF890C1365817F' \
    -debug

// 单删除
env GOCACHE=/private/tmp/whatsmeow-gocache go run . \
    -creds '../../docs/keypair/916201537103.json' \
    -db 'postgres://admin:admin123@127.0.0.1:5432/app?sslmode=disable' \
    -db-dialect postgres \
    -db-max-idle-conns 10 \
    -db-max-open-conns 100 \
    -connect \
    -timeout 120s \
    -connect-retries 5 \
    -connect-retry-delay 3s \
    -send-timeout 180s \
    -transport-timeout 90s \
    -proxy 'socks5://wefanvip1_1:MKLP123456@proxyus.rola.vip:2000' \
    -send-to '916267994957' \
    -message 'hello after 5s delete for me' \
    -post-send-wait 0 \
    -delete-for-me \
    -delete-delay 5s \
    -debug


env GOCACHE=/private/tmp/whatsmeow-gocache go run . \
    -creds '../../docs/keypair/916201537103.json' \
    -db 'postgres://admin:admin123@127.0.0.1:5432/app?sslmode=disable' \
    -db-dialect postgres \
    -db-max-idle-conns 10 \
    -db-max-open-conns 100 \
    -connect \
    -timeout 120s \
    -connect-retries 5 \
    -connect-retry-delay 3s \
    -send-timeout 180s \
    -transport-timeout 90s \
    -proxy 'socks5://wefanvip1_1:MKLP123456@proxyus.rola.vip:2000' \
    -send-to '916267994957' \
    -message 'hello after 5s delete chat' \
    -post-send-wait 0 \
    -delete-for-me \
    -delete-delay 5s \
    -delete-chat-for-me \
    -debug

│  >_ OpenAI Codex (v0.137.0)                                                     │
│                                                                                 │
│ Visit https://chatgpt.com/codex/settings/usage for up-to-date                   │
│ information on rate limits and credits                                          │
│                                                                                 │
│  Model:                gpt-5.5 (reasoning xhigh, summaries auto)                │
│  Directory:            ~/GolandProjects/whatsmeow                               │
│  Permissions:          Workspace (Ask for approval)                             │
│  Agents.md:            <none>                                                   │
│  Account:              hierimonrocu@mail.com (Plus)                             │
│  Collaboration mode:   Default                                                  │
│  Session:              019ea4ca-e6a6-7a31-8df6-30e6261f85c7                     │
│                                                                                 │
│  Context window:       27% left (191K used / 258K)                              │
│  5h limit:             [░░░░░░░░░░░░░░░░░░░░] 0% left (resets 19:28)            │
│  Weekly limit:         [██████░░░░░░░░░░░░░░] 31% left (resets 08:30 on 11 Jun) │
│  premium limit:
*/
func main() {
	var (
		credsPath         = flag.String("creds", "", "path to a Baileys-style creds JSON file")
		authDir           = flag.String("auth-dir", "", "optional Baileys multi-file auth directory to import app-state sync keys from")
		dbDSN             = flag.String("db", "", "optional persistent SQL store DSN, e.g. postgres://user:pass@127.0.0.1:5432/app?sslmode=disable")
		dbDialect         = flag.String("db-dialect", "postgres", "SQL store dialect for -db: postgres or sqlite3")
		dbMaxIdleConns    = flag.Int("db-max-idle-conns", 0, "max idle SQL connections for -db; 0 keeps database/sql default")
		dbMaxOpenConns    = flag.Int("db-max-open-conns", 0, "max open SQL connections for -db; 0 keeps database/sql default")
		connect           = flag.Bool("connect", false, "attempt to connect to WhatsApp with the imported device")
		proxyAddr         = flag.String("proxy", "", "optional proxy URL for WhatsApp websocket/media, e.g. http://127.0.0.1:7890 or socks5://127.0.0.1:1080")
		mediaProxyAddr    = flag.String("media-proxy", "", "optional separate proxy URL for media uploads/downloads")
		mediaDirect       = flag.Bool("media-direct", false, "use a direct connection for media uploads/downloads while -proxy is used for WhatsApp websocket traffic")
		sendTo            = flag.String("send-to", "", "optional WhatsApp phone number or JID to send a smoke-test text to after connecting")
		message           = flag.String("message", "whatsmeow imported creds smoke test", "smoke-test text for -send-to")
		messageKind       = flag.String("message-kind", "text", "message type for -send-to: text, image, external-ad, link-preview, interactive-url, native-flow-url, template-url, or buttons-url")
		mediaPath         = flag.String("media-path", "", "local image/media path for image messages, preview thumbnails, or URL card headers")
		previewTitle      = flag.String("preview-title", "Link", "title for external-ad, link-preview, or URL card messages")
		previewBody       = flag.String("preview-body", "", "body/description for external-ad, link-preview, or URL card messages; defaults to -message")
		previewURL        = flag.String("preview-url", "https://example.com/", "URL for external-ad, link-preview, or URL card messages")
		previewButton     = flag.String("preview-button", "click me auto", "CTA button title for external-ad or URL card messages")
		externalAdAction  = flag.Bool("external-ad-action-link", false, "include actionLink/CTA fields in external-ad messages")
		addContact        = flag.Bool("add-contact", false, "add -send-to to the WhatsApp contact list before sending the smoke-test text")
		addContacts       = flag.String("add-contacts", "", "comma/space/newline-separated phone numbers or JIDs to add after connecting")
		addContactMode    = flag.String("add-contact-method", "usync", "contact add method for -add-contact: usync or appstate")
		addContactBatch   = flag.Int("add-contact-batch-size", 20, "number of contacts per usync add-contact request")
		addContactPause   = flag.Duration("add-contact-pause", 2*time.Second, "pause between usync add-contact batches")
		contactName       = flag.String("contact-name", "Whatsmeow Smoke Test", "full name to use with -add-contact")
		forceLIDSend      = flag.Bool("force-lid-send", false, "send the smoke-test message to the LID returned by user info instead of the phone-number JID")
		lidDBMigrated     = flag.Bool("lid-db-migrated", false, "set login payload lidDbMigrated; Baileys currently logs false for imported creds")
		appStateKeyWait   = flag.Duration("app-state-key-wait", 20*time.Second, "time to wait for app-state key recovery before -add-contact; 0 disables recovery")
		recoverKeys       = flag.Bool("recover-app-state-keys", false, "request app-state sync keys from own linked devices after connecting and persist any received keys")
		transportWait     = flag.Duration("transport-timeout", 60*time.Second, "dial/TLS/header timeout for WhatsApp websocket proxy transport")
		timeout           = flag.Duration("timeout", 30*time.Second, "connect timeout")
		connectRetries    = flag.Int("connect-retries", 3, "number of websocket connection attempts for transient proxy/network failures")
		connectRetryWait  = flag.Duration("connect-retry-delay", 3*time.Second, "initial delay between websocket connection attempts")
		sendTimeout       = flag.Duration("send-timeout", 180*time.Second, "target lookup/contact/media upload/send timeout after connecting; 0 disables the post-connect timeout")
		postSendWait      = flag.Duration("post-send-wait", 20*time.Second, "time to remain connected after sending while waiting for delivery/retry receipts; 0 disables")
		controlMessage    = flag.String("control-message", "", "optional plain-text control message sent after the primary message on the same connection")
		controlDelay      = flag.Duration("control-delay", 2*time.Second, "delay before sending -control-message")
		deleteForEveryone = flag.Bool("delete-for-everyone", false, "revoke the primary message for both sender and recipient after it is sent")
		deleteForMe       = flag.Bool("delete-for-me", false, "delete the primary message only from the current account using app-state after it is sent")
		deleteDelay       = flag.Duration("delete-delay", 2*time.Second, "delay before deleting the primary message with -delete-for-everyone or -delete-for-me")
		revokeMessageID   = flag.String("revoke-message-id", "", "revoke an existing outgoing message ID in -send-to instead of sending a new message")
		deleteForMeID     = flag.String("delete-for-me-message-id", "", "delete an existing message ID only from the current account in -send-to instead of sending a new message")
		deleteForMeTS     = flag.Int64("delete-for-me-message-timestamp", 0, "Unix seconds timestamp for -delete-for-me-message-id; defaults to current time when omitted")
		deleteForMeFromMe = flag.Bool("delete-for-me-from-me", true, "whether -delete-for-me-message-id refers to an outgoing message from this account")
		deleteMediaForMe  = flag.Bool("delete-media-for-me", false, "also remove local media metadata when deleting a message only from the current account")
		deleteChatForMe   = flag.Bool("delete-chat-for-me", false, "delete the -send-to chat only from the current account using app-state after the message/delete operation")
		deleteChatMedia   = flag.Bool("delete-chat-media", false, "also remove local media metadata when deleting the current account chat")
		debug             = flag.Bool("debug", false, "enable verbose whatsmeow logs")
	)
	flag.Parse()

	if *credsPath == "" {
		fmt.Fprintln(os.Stderr, "missing -creds")
		os.Exit(2)
	}
	if *sendTo != "" && !*connect {
		fmt.Fprintln(os.Stderr, "-send-to requires -connect")
		os.Exit(2)
	}
	if *revokeMessageID != "" && *sendTo == "" {
		fmt.Fprintln(os.Stderr, "-revoke-message-id requires -send-to")
		os.Exit(2)
	}
	if *deleteForMe && *sendTo == "" {
		fmt.Fprintln(os.Stderr, "-delete-for-me requires -send-to")
		os.Exit(2)
	}
	if *deleteForMeID != "" && *sendTo == "" {
		fmt.Fprintln(os.Stderr, "-delete-for-me-message-id requires -send-to")
		os.Exit(2)
	}
	if *deleteChatForMe && *sendTo == "" {
		fmt.Fprintln(os.Stderr, "-delete-chat-for-me requires -send-to")
		os.Exit(2)
	}
	if *deleteForEveryone && *deleteForMe {
		fmt.Fprintln(os.Stderr, "-delete-for-everyone and -delete-for-me cannot be used together")
		os.Exit(2)
	}
	if *revokeMessageID != "" && *deleteForMeID != "" {
		fmt.Fprintln(os.Stderr, "-revoke-message-id and -delete-for-me-message-id cannot be used together")
		os.Exit(2)
	}
	if *addContacts != "" && !*connect {
		fmt.Fprintln(os.Stderr, "-add-contacts requires -connect")
		os.Exit(2)
	}
	if *addContact && *sendTo == "" {
		fmt.Fprintln(os.Stderr, "-add-contact requires -send-to")
		os.Exit(2)
	}
	if (*addContact || *addContacts != "") && *addContactMode != "usync" && *addContactMode != "appstate" {
		fmt.Fprintln(os.Stderr, "-add-contact-method must be usync or appstate")
		os.Exit(2)
	}
	if *addContactBatch <= 0 {
		fmt.Fprintln(os.Stderr, "-add-contact-batch-size must be greater than 0")
		os.Exit(2)
	}
	if *connectRetries <= 0 {
		fmt.Fprintln(os.Stderr, "-connect-retries must be greater than 0")
		os.Exit(2)
	}
	if *dbMaxIdleConns < 0 {
		fmt.Fprintln(os.Stderr, "-db-max-idle-conns must be greater than or equal to 0")
		os.Exit(2)
	}
	if *dbMaxOpenConns < 0 {
		fmt.Fprintln(os.Stderr, "-db-max-open-conns must be greater than or equal to 0")
		os.Exit(2)
	}
	if *mediaDirect && *mediaProxyAddr != "" {
		fmt.Fprintln(os.Stderr, "-media-direct and -media-proxy cannot be used together")
		os.Exit(2)
	}
	if *messageKind != "text" && *messageKind != "image" && *messageKind != "external-ad" && *messageKind != "link-preview" && *messageKind != "interactive-url" && *messageKind != "native-flow-url" && *messageKind != "template-url" && *messageKind != "buttons-url" {
		fmt.Fprintln(os.Stderr, "-message-kind must be text, image, external-ad, link-preview, interactive-url, native-flow-url, template-url, or buttons-url")
		os.Exit(2)
	}

	imported, err := loadBaileysCreds(*credsPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to import creds: %v\n", err)
		os.Exit(1)
	}
	if *dbDSN != "" {
		container, err := usePersistentSQLStore(context.Background(), imported.device, *dbDialect, *dbDSN, *dbMaxIdleConns, *dbMaxOpenConns)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to initialize persistent SQL store: %v\n", err)
			os.Exit(1)
		}
		defer container.Close()
		fmt.Printf("using persistent %s store: %s\n", *dbDialect, redactSQLAddress(*dbDSN))
	}
	if *authDir != "" {
		count, keyID, warnings, err := importBaileysAuthDir(imported.device, *authDir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to import auth dir: %v\n", err)
			os.Exit(1)
		}
		if len(keyID) > 0 {
			imported.myAppStateKeyID = keyID
		}
		fmt.Printf("imported baileys auth dir: app_state_keys=%d latest_key_id=%s\n", count, printableKeyID(keyID))
		for _, warning := range warnings {
			fmt.Printf("warning: %s\n", warning)
		}
	}

	fmt.Printf("imported device: jid=%s lid=%s platform=%q registration_id=%d\n",
		imported.device.GetJID(), imported.device.GetLID(), imported.device.Platform, imported.device.RegistrationID)
	if len(imported.warnings) > 0 {
		for _, warning := range imported.warnings {
			fmt.Printf("warning: %s\n", warning)
		}
	}
	if !*connect {
		fmt.Println("dry run complete; pass -connect to attempt a live WhatsApp connection")
		return
	}

	msgOpts := smokeMessageOptions{
		Kind:           *messageKind,
		Text:           *message,
		MediaPath:      *mediaPath,
		PreviewTitle:   *previewTitle,
		PreviewBody:    *previewBody,
		PreviewURL:     *previewURL,
		PreviewButton:  *previewButton,
		ExternalAction: *externalAdAction,
	}
	if err = attemptConnect(*timeout, *transportWait, *sendTimeout, *postSendWait, *controlDelay, *deleteDelay, *connectRetries, *connectRetryWait, *debug, imported.device, imported.myAppStateKeyID, *proxyAddr, *mediaProxyAddr, *mediaDirect, *sendTo, msgOpts, *controlMessage, *deleteForEveryone, *deleteForMe, *deleteChatForMe, *revokeMessageID, *deleteForMeID, *deleteForMeTS, *deleteForMeFromMe, *deleteMediaForMe, *deleteChatMedia, *addContact, *addContacts, *addContactMode, *addContactBatch, *addContactPause, *contactName, *forceLIDSend, *lidDBMigrated, *appStateKeyWait, *recoverKeys); err != nil {
		fmt.Fprintf(os.Stderr, "connect test failed: %v\n", err)
		os.Exit(1)
	}
}

func loadBaileysCreds(path string) (*importedDevice, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var creds baileysCreds
	if err = json.Unmarshal(data, &creds); err != nil {
		return nil, err
	}

	id, err := types.ParseJID(creds.Me.ID)
	if err != nil {
		return nil, fmt.Errorf("parse me.id: %w", err)
	}
	var lid types.JID
	if creds.Me.LID != "" {
		lid, err = types.ParseJID(creds.Me.LID)
		if err != nil {
			return nil, fmt.Errorf("parse me.lid: %w", err)
		}
	}

	noiseKey, noiseWarnings, err := parseKeyPair("noiseKey", creds.NoiseKey)
	if err != nil {
		return nil, err
	}
	identityKey, identityWarnings, err := parseKeyPair("signedIdentityKey", creds.SignedIdentityKey)
	if err != nil {
		return nil, err
	}
	preKeyPair, preKeyWarnings, err := parseKeyPair("signedPreKey.keyPair", creds.SignedPreKey.KeyPair)
	if err != nil {
		return nil, err
	}
	preKeySignature, err := bytesOfLen("signedPreKey.signature", creds.SignedPreKey.Signature, 64)
	if err != nil {
		return nil, err
	}
	advSecretKey, err := bytesOfLen("advSecretKey", creds.AdvSecretKey, 32)
	if err != nil {
		return nil, err
	}
	accountDetails, err := creds.Account.Details.Bytes()
	if err != nil {
		return nil, fmt.Errorf("account.details: %w", err)
	}
	accountSignature, err := bytesOfLen("account.accountSignature", creds.Account.AccountSignature, 64)
	if err != nil {
		return nil, err
	}
	accountSignatureKey, err := bytesOfLen("account.accountSignatureKey", creds.Account.AccountSignatureKey, 32)
	if err != nil {
		return nil, err
	}
	deviceSignature, err := bytesOfLen("account.deviceSignature", creds.Account.DeviceSignature, 64)
	if err != nil {
		return nil, err
	}
	if creds.RegistrationID == 0 {
		return nil, errors.New("registrationId is zero or missing")
	}
	if creds.SignedPreKey.KeyID == 0 {
		return nil, errors.New("signedPreKey.keyId is zero or missing")
	}
	var myAppStateKeyID []byte
	var warnings []string
	if creds.MyAppStateKeyID != "" {
		myAppStateKeyID, err = decodeBase64String(creds.MyAppStateKeyID)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("myAppStateKeyId is present but could not be decoded: %v", err))
		}
	}

	mem := newMemoryStore()
	if creds.NextPreKeyID > 0 {
		mem.nextPreKeyID = creds.NextPreKeyID
	} else if creds.SignedPreKey.KeyID >= mem.nextPreKeyID {
		mem.nextPreKeyID = creds.SignedPreKey.KeyID + 1
	}
	if creds.FirstUnuploadedPreKeyID > 0 {
		mem.uploadedUpTo = creds.FirstUnuploadedPreKeyID - 1
		mem.uploadedPreKeyCountHint = int(creds.FirstUnuploadedPreKeyID - 1)
	}
	device := &store.Device{
		ID:             &id,
		LID:            lid,
		NoiseKey:       noiseKey,
		IdentityKey:    identityKey,
		SignedPreKey:   &keys.PreKey{KeyPair: *preKeyPair, KeyID: creds.SignedPreKey.KeyID, Signature: (*[64]byte)(preKeySignature)},
		RegistrationID: creds.RegistrationID,
		AdvSecretKey:   advSecretKey,
		Account: &waAdv.ADVSignedDeviceIdentity{
			Details:             accountDetails,
			AccountSignature:    accountSignature,
			AccountSignatureKey: accountSignatureKey,
			DeviceSignature:     deviceSignature,
		},
		Platform:    creds.Platform,
		PushName:    creds.Me.Name,
		Container:   mem,
		LIDs:        mem,
		Initialized: true,
	}
	device.SetAllStores(mem)
	mem.device = device
	if !id.IsEmpty() && !lid.IsEmpty() {
		mem.lidToPN[lid.User] = id.ToNonAD()
		mem.pnToLID[id.User] = lid.ToNonAD()
	}

	warnings = append(warnings, noiseWarnings...)
	warnings = append(warnings, identityWarnings...)
	warnings = append(warnings, preKeyWarnings...)
	if creds.Phone == "" {
		warnings = append(warnings, "Phone is empty")
	}
	return &importedDevice{device: device, myAppStateKeyID: myAppStateKeyID, warnings: warnings}, nil
}

func importBaileysAuthDir(device *store.Device, dir string) (int, []byte, []string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, nil, nil, err
	}

	var warnings []string
	latestKeyID, err := readBaileysAuthDirAppStateKeyID(dir)
	if err != nil {
		warnings = append(warnings, fmt.Sprintf("failed to read auth dir creds.json myAppStateKeyId: %v", err))
	}

	count := 0
	ctx := context.Background()
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasPrefix(name, "app-state-sync-key-") || !strings.HasSuffix(name, ".json") {
			continue
		}
		keyIDText := strings.TrimSuffix(strings.TrimPrefix(name, "app-state-sync-key-"), ".json")
		keyIDText = strings.ReplaceAll(keyIDText, "__", "/")
		keyID, err := decodeBase64String(keyIDText)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("skipping %s: invalid key id: %v", name, err))
			continue
		}
		key, err := readBaileysAppStateSyncKey(filepath.Join(dir, name))
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("skipping %s: %v", name, err))
			continue
		}
		if err = device.AppStateKeys.PutAppStateSyncKey(ctx, keyID, key); err != nil {
			warnings = append(warnings, fmt.Sprintf("skipping %s: failed to store key: %v", name, err))
			continue
		}
		count++
		if len(latestKeyID) == 0 {
			latestKeyID = keyID
		}
	}
	return count, latestKeyID, warnings, nil
}

func readBaileysAuthDirAppStateKeyID(dir string) ([]byte, error) {
	data, err := os.ReadFile(filepath.Join(dir, "creds.json"))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	var creds struct {
		MyAppStateKeyID string `json:"myAppStateKeyId"`
	}
	if err = json.Unmarshal(data, &creds); err != nil {
		return nil, err
	}
	if creds.MyAppStateKeyID == "" {
		return nil, nil
	}
	return decodeBase64String(creds.MyAppStateKeyID)
}

func readBaileysAppStateSyncKey(path string) (store.AppStateSyncKey, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return store.AppStateSyncKey{}, err
	}
	var raw baileysAppStateSyncKeyData
	if err = json.Unmarshal(data, &raw); err != nil {
		return store.AppStateSyncKey{}, err
	}
	keyData, err := bytesOfLen("keyData", raw.KeyData, 32)
	if err != nil {
		return store.AppStateSyncKey{}, err
	}
	fingerprint, err := marshalBaileysAppStateFingerprint(raw.Fingerprint)
	if err != nil {
		return store.AppStateSyncKey{}, err
	}
	timestamp, err := parseFlexibleInt64(raw.Timestamp)
	if err != nil {
		return store.AppStateSyncKey{}, fmt.Errorf("timestamp: %w", err)
	}
	return store.AppStateSyncKey{
		Data:        keyData,
		Fingerprint: fingerprint,
		Timestamp:   timestamp,
	}, nil
}

func marshalBaileysAppStateFingerprint(raw *baileysAppStateKeyFingerprint) ([]byte, error) {
	if raw == nil {
		return nil, nil
	}
	fp := &waE2E.AppStateSyncKeyFingerprint{
		CurrentIndex:  raw.CurrentIndex,
		DeviceIndexes: raw.DeviceIndexes,
	}
	if raw.RawID != nil {
		fp.RawID = raw.RawID
	} else {
		fp.RawID = raw.RawIDAlt
	}
	return proto.Marshal(fp)
}

func parseFlexibleInt64(raw json.RawMessage) (int64, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return 0, nil
	}
	var num int64
	if err := json.Unmarshal(raw, &num); err == nil {
		return num, nil
	}
	var floatNum float64
	if err := json.Unmarshal(raw, &floatNum); err == nil {
		return int64(floatNum), nil
	}
	var str string
	if err := json.Unmarshal(raw, &str); err == nil {
		if str == "" {
			return 0, nil
		}
		return strconv.ParseInt(str, 10, 64)
	}
	return 0, errors.New("unsupported integer encoding")
}

func printableKeyID(keyID []byte) string {
	if len(keyID) == 0 {
		return ""
	}
	return base64.StdEncoding.EncodeToString(keyID)
}

func parseKeyPair(name string, pair baileysKeyPair) (*keys.KeyPair, []string, error) {
	privateKey, err := bytesOfLen(name+".private", pair.Private, 32)
	if err != nil {
		return nil, nil, err
	}
	publicKey, err := pair.Public.Bytes()
	if err != nil {
		return nil, nil, fmt.Errorf("%s.public: %w", name, err)
	}
	keyPair := keys.NewKeyPairFromPrivateKey(*(*[32]byte)(privateKey))
	var warnings []string
	if len(publicKey) != 32 {
		warnings = append(warnings, fmt.Sprintf("%s.public has %d bytes, expected 32", name, len(publicKey)))
	} else if !bytes.Equal(keyPair.Pub[:], publicKey) {
		warnings = append(warnings, fmt.Sprintf("%s.public does not match the public key derived from private", name))
	}
	return keyPair, warnings, nil
}

func bytesOfLen(name string, value baileysBytes, expected int) ([]byte, error) {
	data, err := value.Bytes()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", name, err)
	}
	if len(data) != expected {
		return nil, fmt.Errorf("%s has %d bytes, expected %d", name, len(data), expected)
	}
	return data, nil
}

func decodeFlexibleBytes(raw json.RawMessage) ([]byte, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return nil, errors.New("empty byte value")
	}

	var str string
	if err := json.Unmarshal(raw, &str); err == nil {
		return decodeBase64String(str)
	}

	var wrapper struct {
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(raw, &wrapper); err == nil && len(wrapper.Data) > 0 {
		return decodeFlexibleBytes(wrapper.Data)
	}

	var ints []int
	if err := json.Unmarshal(raw, &ints); err == nil {
		out := make([]byte, len(ints))
		for i, val := range ints {
			if val < 0 || val > 255 {
				return nil, fmt.Errorf("byte array element %d is outside 0..255", i)
			}
			out[i] = byte(val)
		}
		return out, nil
	}

	return nil, errors.New("unsupported byte encoding")
}

func decodeBase64String(input string) ([]byte, error) {
	if input == "" {
		return nil, errors.New("empty base64 string")
	}
	if out, err := base64.StdEncoding.DecodeString(input); err == nil {
		return out, nil
	}
	if out, err := base64.RawStdEncoding.DecodeString(input); err == nil {
		return out, nil
	}
	if out, err := base64.URLEncoding.DecodeString(input); err == nil {
		return out, nil
	}
	if out, err := base64.RawURLEncoding.DecodeString(input); err == nil {
		return out, nil
	}
	return nil, errors.New("invalid base64 string")
}

type smokeMessageOptions struct {
	Kind           string
	Text           string
	MediaPath      string
	PreviewTitle   string
	PreviewBody    string
	PreviewURL     string
	PreviewButton  string
	ExternalAction bool
}

func attemptConnect(timeout, transportWait, sendTimeout, postSendWait, controlDelay, deleteDelay time.Duration, connectRetries int, connectRetryWait time.Duration, debug bool, device *store.Device, myAppStateKeyID []byte, proxyAddr, mediaProxyAddr string, mediaDirect bool, sendTo string, msgOpts smokeMessageOptions, controlMessage string, deleteForEveryone, deleteForMe, deleteChatForMe bool, revokeMessageID, deleteForMeID string, deleteForMeTS int64, deleteForMeFromMe, deleteMediaForMe, deleteChatMedia bool, addContact bool, addContacts, addContactMode string, addContactBatch int, addContactPause time.Duration, contactName string, forceLIDSend bool, lidDBMigrated bool, appStateKeyWait time.Duration, recoverKeys bool) error {
	connCtx, cancelConn := context.WithCancel(context.Background())
	defer cancelConn()
	sessionCtx, cancelSession := context.WithCancelCause(context.Background())
	defer cancelSession(nil)

	log := waLog.Noop
	if debug {
		log = waLog.Stdout("BaileysImport", "DEBUG", true)
	}
	client := whatsmeow.NewClient(device, log)
	client.EnableAutoReconnect = true
	client.SendReportingTokens = true
	client.GetClientPayload = func() *waWa6.ClientPayload {
		payload := device.GetClientPayload()
		payload.LidDbMigrated = proto.Bool(lidDBMigrated)
		return payload
	}
	if proxyAddr != "" {
		proxyOpts := whatsmeow.SetProxyOptions{
			NoMedia: mediaDirect || mediaProxyAddr != "",
		}
		if err := setProxyAddressWithTimeout(client, proxyAddr, transportWait, proxyOpts); err != nil {
			return fmt.Errorf("set proxy: %w", err)
		}
	}
	if mediaProxyAddr != "" {
		if err := setProxyAddressWithTimeout(client, mediaProxyAddr, transportWait, whatsmeow.SetProxyOptions{NoWebsocket: true}); err != nil {
			return fmt.Errorf("set media proxy: %w", err)
		}
	} else if mediaDirect {
		setDirectMediaTransport(client, transportWait)
	}
	defer client.Disconnect()

	result := make(chan error, 1)
	receipts := make(chan events.Receipt, 32)
	appStateSyncs := make(chan appstate.WAPatchName, len(appstate.AllPatchNames)*2)
	signalSessionFailure := func(err error) {
		cancelSession(err)
		select {
		case result <- err:
		default:
		}
	}
	client.AddEventHandler(func(rawEvt any) {
		switch evt := rawEvt.(type) {
		case *events.Connected:
			select {
			case result <- nil:
			default:
			}
		case *events.LoggedOut:
			signalSessionFailure(fmt.Errorf("logged out: on_connect=%t reason=%s", evt.OnConnect, evt.Reason.String()))
		case *events.ConnectFailure:
			signalSessionFailure(fmt.Errorf("connect failure: %s (%s)", evt.Reason.String(), evt.Message))
		case *events.ClientOutdated:
			signalSessionFailure(errors.New("client outdated"))
		case *events.TemporaryBan:
			signalSessionFailure(fmt.Errorf("temporary ban: %s", evt.String()))
		case *events.StreamReplaced:
			signalSessionFailure(errors.New("stream replaced: the same imported device credentials connected from another process or machine"))
		case *events.Disconnected:
			fmt.Println("warning: websocket disconnected; waiting for automatic reconnect")
		case *events.Receipt:
			receipt := *evt
			receipt.MessageIDs = append([]types.MessageID(nil), evt.MessageIDs...)
			select {
			case receipts <- receipt:
			default:
			}
		case *events.AppStateSyncComplete:
			select {
			case appStateSyncs <- evt.Name:
			default:
			}
		}
	})

	var connectErr error
	for attempt := 1; attempt <= connectRetries; attempt++ {
		connectErr = client.ConnectContext(connCtx)
		if connectErr == nil {
			break
		}
		if attempt == connectRetries || connCtx.Err() != nil {
			return connectErr
		}
		delay := time.Duration(attempt) * connectRetryWait
		fmt.Printf("warning: websocket connect attempt %d/%d failed: %v; retrying in %s\n", attempt, connectRetries, connectErr, delay)
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-connCtx.Done():
			timer.Stop()
			return context.Cause(connCtx)
		}
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case err := <-result:
		if err != nil {
			return err
		}
	case <-timer.C:
		return fmt.Errorf("timed out waiting for authenticated connection after %s", timeout)
	}

	opCtx := sessionCtx
	cancelOp := func() {}
	if sendTimeout > 0 {
		opCtx, cancelOp = context.WithTimeout(opCtx, sendTimeout)
	}
	defer cancelOp()

	fmt.Printf("connected as %s lid_db_migrated=%t\n", client.Store.GetJID(), lidDBMigrated)
	if recoverKeys {
		beforeCount := countAppStateKeys(device)
		fmt.Printf("app-state sync keys before recovery: count=%d latest_key_id=%s wait=%s\n", beforeCount, printableKeyID(myAppStateKeyID), appStateKeyWait)
		if appStateKeyWait <= 0 {
			fmt.Println("warning: -recover-app-state-keys requested but -app-state-key-wait is 0; skipping recovery wait")
		} else if err := recoverAppStateKeys(opCtx, client, device, myAppStateKeyID, appStateKeyWait); err != nil {
			fmt.Printf("warning: app-state key recovery did not complete: %v\n", err)
		}
		afterCount := countAppStateKeys(device)
		fmt.Printf("app-state sync keys after recovery: count=%d\n", afterCount)
		if beforeCount == 0 && afterCount > 0 {
			fmt.Printf("waiting up to %s for initial app-state sync to complete\n", appStateKeyWait)
			if err := waitForInitialAppStateSync(opCtx, appStateSyncs, appStateKeyWait); err != nil {
				fmt.Printf("warning: initial app-state sync did not complete before shutdown: %v\n", err)
			}
		}
	}
	batchTargets, err := parseTargetJIDList(addContacts)
	if err != nil {
		return err
	}
	if len(batchTargets) > 0 {
		if err = addContactsBeforeSend(opCtx, client, device, batchTargets, addContactMode, myAppStateKeyID, appStateKeyWait, addContactBatch, addContactPause); err != nil {
			fmt.Printf("warning: batch add contacts failed: %v\n", err)
		}
	}
	if sendTo == "" {
		return nil
	}

	target, err := parseTargetJID(sendTo)
	if err != nil {
		return err
	}

	checkCtx, cancelCheck := context.WithTimeout(opCtx, 20*time.Second)
	defer cancelCheck()
	userInfo, err := client.GetUserInfo(checkCtx, []types.JID{target})
	if err != nil {
		return fmt.Errorf("check target registration: %w", err)
	}
	info, ok := userInfo[target]
	if !ok || len(info.Devices) == 0 {
		return fmt.Errorf("target %s is not registered or has no visible devices", target)
	}
	fmt.Printf("target ready: jid=%s devices=%d lid=%s\n", target, len(info.Devices), info.LID)
	sendTarget := target
	if forceLIDSend {
		if info.LID.IsEmpty() {
			return fmt.Errorf("target %s has no LID to use with -force-lid-send", target)
		}
		sendTarget = info.LID
		fmt.Printf("forcing LID send target: %s -> %s\n", target, sendTarget)
	}
	if revokeMessageID = strings.TrimSpace(revokeMessageID); revokeMessageID != "" {
		fmt.Printf("revoking existing message for everyone: chat=%s message_id=%s\n", sendTarget, revokeMessageID)
		revokeResp, revokeErr := sendMessageWithReconnect(
			opCtx,
			client,
			sendTarget,
			client.BuildRevoke(sendTarget, types.EmptyJID, types.MessageID(revokeMessageID)),
			"revoke",
			connectRetries,
			timeout,
		)
		if revokeErr != nil {
			return fmt.Errorf("revoke existing message for everyone: %w", revokeErr)
		}
		fmt.Printf("revoked message for everyone: original_id=%s revoke_id=%s timestamp=%s\n",
			revokeMessageID, revokeResp.ID, revokeResp.Timestamp.Format(time.RFC3339))
		if deleteChatForMe {
			if err = deleteChatForMeLocal(opCtx, client, device, sendTarget, revokeResp.ID, true, revokeResp.Timestamp, deleteChatMedia, myAppStateKeyID, appStateKeyWait); err != nil {
				return fmt.Errorf("delete chat for current account after revoke: %w", err)
			}
		}
		return nil
	}
	if deleteForMeID = strings.TrimSpace(deleteForMeID); deleteForMeID != "" {
		messageTimestamp := time.Now()
		if deleteForMeTS > 0 {
			messageTimestamp = time.Unix(deleteForMeTS, 0)
		}
		fmt.Printf("deleting existing message for current account: chat=%s message_id=%s from_me=%t message_timestamp=%s delete_media=%t\n",
			sendTarget, deleteForMeID, deleteForMeFromMe, messageTimestamp.Format(time.RFC3339), deleteMediaForMe)
		if err = deleteMessageForMe(opCtx, client, device, sendTarget, types.MessageID(deleteForMeID), deleteForMeFromMe, messageTimestamp, deleteMediaForMe, myAppStateKeyID, appStateKeyWait); err != nil {
			return fmt.Errorf("delete existing message for current account: %w", err)
		}
		fmt.Printf("deleted existing message for current account: chat=%s message_id=%s\n", sendTarget, deleteForMeID)
		if deleteChatForMe {
			if err = deleteChatForMeLocal(opCtx, client, device, sendTarget, types.MessageID(deleteForMeID), deleteForMeFromMe, messageTimestamp, deleteChatMedia, myAppStateKeyID, appStateKeyWait); err != nil {
				return fmt.Errorf("delete chat for current account after delete-for-me: %w", err)
			}
		}
		return nil
	}

	if addContact {
		if err = addContactBeforeSend(opCtx, client, device, target, contactName, addContactMode, myAppStateKeyID, appStateKeyWait); err != nil {
			fmt.Printf("warning: add contact failed, continuing to send message: %v\n", err)
		}
	}
	msg, err := buildSmokeMessage(opCtx, client, msgOpts)
	if err != nil {
		if cause := context.Cause(opCtx); cause != nil && !errors.Is(cause, context.DeadlineExceeded) {
			return cause
		}
		return err
	}
	fmt.Printf("sending message: kind=%s chat=%s own_jid=%s own_lid=%s\n", msgOpts.Kind, sendTarget, client.Store.GetJID(), client.Store.GetLID())
	resp, err := sendMessageWithReconnect(opCtx, client, sendTarget, msg, msgOpts.Kind, connectRetries, timeout)
	if err != nil {
		return fmt.Errorf("send smoke-test message: %w", err)
	}
	fmt.Printf("sent smoke-test message id=%s timestamp=%s\n", resp.ID, resp.Timestamp.Format(time.RFC3339))
	lastMessageID := resp.ID
	lastMessageTimestamp := resp.Timestamp
	lastMessageFromMe := true
	expectedReceipts := []expectedMessageReceipt{{
		ID:    resp.ID,
		Label: msgOpts.Kind,
	}}
	if controlMessage = strings.TrimSpace(controlMessage); controlMessage != "" {
		if err = waitContext(opCtx, controlDelay); err != nil {
			return err
		}
		controlResp, controlErr := sendMessageWithReconnect(opCtx, client, sendTarget, &waE2E.Message{
			Conversation: proto.String(controlMessage),
		}, "control-text", connectRetries, timeout)
		if controlErr != nil {
			return fmt.Errorf("send plain-text control message: %w", controlErr)
		}
		fmt.Printf("sent plain-text control message id=%s timestamp=%s\n", controlResp.ID, controlResp.Timestamp.Format(time.RFC3339))
		lastMessageID = controlResp.ID
		lastMessageTimestamp = controlResp.Timestamp
		lastMessageFromMe = true
		expectedReceipts = append(expectedReceipts, expectedMessageReceipt{
			ID:    controlResp.ID,
			Label: "control-text",
		})
	}
	if err = waitForMessageReceipts(opCtx, receipts, expectedReceipts, postSendWait); err != nil {
		return err
	}
	if deleteForEveryone {
		if err = waitContext(opCtx, deleteDelay); err != nil {
			return err
		}
		fmt.Printf("revoking primary message for everyone: chat=%s message_id=%s\n", sendTarget, resp.ID)
		revokeResp, revokeErr := sendMessageWithReconnect(
			opCtx,
			client,
			sendTarget,
			client.BuildRevoke(sendTarget, types.EmptyJID, resp.ID),
			"revoke",
			connectRetries,
			timeout,
		)
		if revokeErr != nil {
			return fmt.Errorf("revoke primary message for everyone: %w", revokeErr)
		}
		fmt.Printf("revoked primary message for everyone: original_id=%s revoke_id=%s timestamp=%s\n",
			resp.ID, revokeResp.ID, revokeResp.Timestamp.Format(time.RFC3339))
		lastMessageID = revokeResp.ID
		lastMessageTimestamp = revokeResp.Timestamp
		lastMessageFromMe = true
	}
	if deleteForMe {
		if err = waitContext(opCtx, deleteDelay); err != nil {
			return err
		}
		fmt.Printf("deleting primary message for current account: chat=%s message_id=%s delete_media=%t\n",
			sendTarget, resp.ID, deleteMediaForMe)
		if err = deleteMessageForMe(opCtx, client, device, sendTarget, resp.ID, true, resp.Timestamp, deleteMediaForMe, myAppStateKeyID, appStateKeyWait); err != nil {
			return fmt.Errorf("delete primary message for current account: %w", err)
		}
		fmt.Printf("deleted primary message for current account: chat=%s message_id=%s\n", sendTarget, resp.ID)
	}
	if deleteChatForMe {
		if err = deleteChatForMeLocal(opCtx, client, device, sendTarget, lastMessageID, lastMessageFromMe, lastMessageTimestamp, deleteChatMedia, myAppStateKeyID, appStateKeyWait); err != nil {
			return fmt.Errorf("delete chat for current account: %w", err)
		}
	}
	return nil
}

func sendMessageWithReconnect(ctx context.Context, client *whatsmeow.Client, target types.JID, message *waE2E.Message, label string, attempts int, reconnectWait time.Duration) (whatsmeow.SendResponse, error) {
	messageID := client.GenerateMessageID()
	additionalNodes, err := additionalNodesForMessage(label)
	if err != nil {
		return whatsmeow.SendResponse{ID: messageID}, err
	}
	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		if attempt > 1 {
			wait := boundedWait(ctx, reconnectWait)
			fmt.Printf("waiting up to %s for reconnect before retrying %s message %s (%d/%d)\n", wait, label, messageID, attempt, attempts)
			if wait <= 0 || !client.WaitForConnection(wait) {
				if cause := context.Cause(ctx); cause != nil {
					return whatsmeow.SendResponse{}, cause
				}
				lastErr = errors.New("websocket did not reconnect before retry timeout")
				continue
			}
		}
		response, err := client.SendMessage(ctx, target, message, whatsmeow.SendRequestExtra{
			ID:              messageID,
			AdditionalNodes: additionalNodes,
		})
		if err == nil {
			return response, nil
		}
		lastErr = err
		if !isRetryableSendDisconnect(err) || attempt == attempts {
			break
		}
		fmt.Printf("warning: %s message %s send interrupted by disconnect: %v\n", label, messageID, err)
	}
	return whatsmeow.SendResponse{ID: messageID}, lastErr
}

func additionalNodesForMessage(kind string) (*[]waBinary.Node, error) {
	if kind != "native-flow-url" {
		return nil, nil
	}
	decisionID, err := secureRandomBytes(20)
	if err != nil {
		return nil, fmt.Errorf("generate native flow decision ID: %w", err)
	}
	nodes := []waBinary.Node{{
		Tag:   "biz",
		Attrs: waBinary.Attrs{},
		Content: []waBinary.Node{
			{
				Tag:   "interactive",
				Attrs: waBinary.Attrs{"type": "native_flow", "v": "1"},
				Content: []waBinary.Node{{
					Tag:   "native_flow",
					Attrs: waBinary.Attrs{"v": "9", "name": "mixed"},
				}},
			},
			{
				Tag: "quality_control",
				Attrs: waBinary.Attrs{
					"decision_id": hex.EncodeToString(decisionID),
					"source_type": "third_party",
				},
				Content: []waBinary.Node{{
					Tag:   "decision_source",
					Attrs: waBinary.Attrs{"value": "df"},
				}},
			},
		},
	}}
	return &nodes, nil
}

func secureRandomBytes(size int) ([]byte, error) {
	data := make([]byte, size)
	_, err := rand.Read(data)
	return data, err
}

func isRetryableSendDisconnect(err error) bool {
	var disconnectedErr *whatsmeow.DisconnectedError
	return errors.Is(err, whatsmeow.ErrNotConnected) || errors.As(err, &disconnectedErr)
}

func boundedWait(ctx context.Context, requested time.Duration) time.Duration {
	if requested <= 0 {
		requested = 30 * time.Second
	}
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining < requested {
			return remaining
		}
	}
	return requested
}

func waitContext(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

type expectedMessageReceipt struct {
	ID        types.MessageID
	Label     string
	Delivered bool
	SawRetry  bool
}

func waitForMessageReceipts(ctx context.Context, receipts <-chan events.Receipt, expected []expectedMessageReceipt, wait time.Duration) error {
	if wait <= 0 {
		return nil
	}
	fmt.Printf("waiting up to %s for delivery/retry receipts for %d message(s)\n", wait, len(expected))
	timer := time.NewTimer(wait)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-timer.C:
			for _, message := range expected {
				if message.Delivered {
					continue
				}
				if message.SawRetry {
					fmt.Printf("warning: %s message %s received a retry receipt but no later delivery receipt before timeout\n", message.Label, message.ID)
				} else {
					fmt.Printf("warning: %s message %s got a server ack but no delivery receipt before timeout\n", message.Label, message.ID)
				}
			}
			return nil
		case receipt := <-receipts:
			for index := range expected {
				message := &expected[index]
				if !containsMessageID(receipt.MessageIDs, message.ID) {
					continue
				}
				fmt.Printf("message receipt: label=%s id=%s type=%s chat=%s sender=%s timestamp=%s\n",
					message.Label, message.ID, printableReceiptType(receipt.Type), receipt.Chat, receipt.Sender, receipt.Timestamp.Format(time.RFC3339))
				switch receipt.Type {
				case types.ReceiptTypeRetry:
					message.SawRetry = true
				case types.ReceiptTypeDelivered, types.ReceiptTypeRead, types.ReceiptTypeReadSelf, types.ReceiptTypePlayed:
					message.Delivered = true
				}
			}
			if allMessagesDelivered(expected) {
				return nil
			}
		}
	}
}

func allMessagesDelivered(expected []expectedMessageReceipt) bool {
	for _, message := range expected {
		if !message.Delivered {
			return false
		}
	}
	return true
}

func containsMessageID(ids []types.MessageID, target types.MessageID) bool {
	for _, id := range ids {
		if id == target {
			return true
		}
	}
	return false
}

func printableReceiptType(receiptType types.ReceiptType) string {
	if receiptType == types.ReceiptTypeDelivered {
		return "delivered"
	}
	return string(receiptType)
}

func deleteMessageForMe(ctx context.Context, client *whatsmeow.Client, device *store.Device, target types.JID, messageID types.MessageID, fromMe bool, messageTimestamp time.Time, deleteMedia bool, myAppStateKeyID []byte, appStateKeyWait time.Duration) error {
	if err := prepareRegularHighAppState(ctx, client, device, myAppStateKeyID, appStateKeyWait, "delete-for-me"); err != nil {
		return err
	}

	return client.SendAppState(ctx, appstate.BuildDeleteMessageForMe(target, types.EmptyJID, messageID, fromMe, messageTimestamp, deleteMedia))
}

func deleteChatForMeLocal(ctx context.Context, client *whatsmeow.Client, device *store.Device, target types.JID, lastMessageID types.MessageID, lastMessageFromMe bool, lastMessageTimestamp time.Time, deleteMedia bool, myAppStateKeyID []byte, appStateKeyWait time.Duration) error {
	if err := prepareRegularHighAppState(ctx, client, device, myAppStateKeyID, appStateKeyWait, "delete-chat-for-me"); err != nil {
		return err
	}

	sender := types.EmptyJID
	if !lastMessageFromMe {
		sender = target
	}
	lastMessageKey := client.BuildMessageKey(target, sender, lastMessageID)
	fmt.Printf("deleting chat for current account: chat=%s last_message_id=%s delete_media=%t\n", target, lastMessageID, deleteMedia)
	if err := client.SendAppState(ctx, appstate.BuildDeleteChat(target, lastMessageTimestamp, lastMessageKey, deleteMedia)); err != nil {
		return err
	}
	fmt.Printf("deleted chat for current account: chat=%s\n", target)
	return nil
}

func prepareRegularHighAppState(ctx context.Context, client *whatsmeow.Client, device *store.Device, myAppStateKeyID []byte, appStateKeyWait time.Duration, action string) error {
	if countAppStateKeys(device) == 0 && appStateKeyWait > 0 {
		fmt.Printf("no app-state sync keys loaded; requesting key share and waiting up to %s\n", appStateKeyWait)
		if err := recoverAppStateKeys(ctx, client, device, myAppStateKeyID, appStateKeyWait); err != nil {
			fmt.Printf("warning: app-state key recovery did not complete: %v\n", err)
		} else {
			fmt.Printf("recovered app-state sync keys count=%d\n", countAppStateKeys(device))
		}
	}
	keyCount := countAppStateKeys(device)
	if keyCount == 0 {
		return errors.New("no app state keys found; use -db with a persistent store and run -recover-app-state-keys before deleting messages for the current account")
	}

	fmt.Printf("syncing regular_high app-state before %s: keys=%d\n", action, keyCount)
	syncCtx := ctx
	var cancelSync context.CancelFunc
	if appStateKeyWait > 0 {
		syncCtx, cancelSync = context.WithTimeout(ctx, appStateKeyWait)
	}
	if err := client.FetchAppState(syncCtx, appstate.WAPatchRegularHigh, true, false); err != nil {
		if cancelSync != nil {
			cancelSync()
		}
		return fmt.Errorf("sync regular_high app-state before delete-for-me: %w", err)
	}
	if cancelSync != nil {
		cancelSync()
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

func buildSmokeMessage(ctx context.Context, client *whatsmeow.Client, opts smokeMessageOptions) (*waE2E.Message, error) {
	if strings.TrimSpace(opts.Text) == "" {
		opts.Text = "whatsmeow imported creds smoke test"
	}
	switch opts.Kind {
	case "text":
		return &waE2E.Message{Conversation: proto.String(opts.Text)}, nil
	case "image":
		return buildImageMessage(ctx, client, opts)
	case "external-ad":
		return buildExternalAdMessage(opts)
	case "link-preview":
		return buildLinkPreviewMessage(ctx, client, opts)
	case "interactive-url":
		return buildInteractiveURLMessage(ctx, client, opts)
	case "native-flow-url":
		return buildNativeFlowURLMessage(ctx, client, opts)
	case "template-url":
		return buildTemplateURLMessage(ctx, client, opts)
	case "buttons-url":
		return buildButtonsURLMessage(ctx, client, opts)
	default:
		return nil, fmt.Errorf("unknown message kind %q", opts.Kind)
	}
}

func buildImageMessage(ctx context.Context, client *whatsmeow.Client, opts smokeMessageOptions) (*waE2E.Message, error) {
	if opts.MediaPath == "" {
		return nil, errors.New("-message-kind image requires -media-path")
	}
	imageMessage, err := buildUploadedImageMessage(ctx, client, opts.MediaPath, opts.Text)
	if err != nil {
		return nil, err
	}
	return &waE2E.Message{ImageMessage: imageMessage}, nil
}

func buildUploadedImageMessage(ctx context.Context, client *whatsmeow.Client, path, caption string) (*waE2E.ImageMessage, error) {
	data, mimetype, err := readMediaFile(path)
	if err != nil {
		return nil, err
	}
	upload, err := client.Upload(ctx, data, whatsmeow.MediaImage)
	if err != nil {
		return nil, fmt.Errorf("upload image media: %w", err)
	}
	width, height := imageDimensions(data)
	imageMessage := &waE2E.ImageMessage{
		Mimetype:          proto.String(mimetype),
		URL:               proto.String(upload.URL),
		DirectPath:        proto.String(upload.DirectPath),
		MediaKey:          upload.MediaKey,
		FileEncSHA256:     upload.FileEncSHA256,
		FileSHA256:        upload.FileSHA256,
		FileLength:        proto.Uint64(upload.FileLength),
		MediaKeyTimestamp: proto.Int64(time.Now().Unix()),
		JPEGThumbnail:     thumbnailOrNil(data, mimetype),
	}
	if strings.TrimSpace(caption) != "" {
		imageMessage.Caption = proto.String(caption)
	}
	if width > 0 {
		imageMessage.Width = proto.Uint32(width)
	}
	if height > 0 {
		imageMessage.Height = proto.Uint32(height)
	}
	imageMessage.ViewOnce = proto.Bool(false)
	imageMessage.ContextInfo = &waE2E.ContextInfo{}
	return imageMessage, nil
}

func buildInteractiveURLMessage(ctx context.Context, client *whatsmeow.Client, opts smokeMessageOptions) (*waE2E.Message, error) {
	_, params, err := nativeFlowURLButtonParams(opts)
	if err != nil {
		return nil, err
	}

	header := &waE2E.InteractiveMessage_Header{
		HasMediaAttachment: proto.Bool(false),
	}
	if opts.MediaPath != "" {
		imageMessage, err := buildUploadedImageMessage(ctx, client, opts.MediaPath, "")
		if err != nil {
			return nil, err
		}
		// Match the known working raw-protocol payload: interactive header media
		// omits the thumbnail and media key timestamp fields.
		imageMessage.JPEGThumbnail = nil
		imageMessage.MediaKeyTimestamp = nil
		header.Media = &waE2E.InteractiveMessage_Header_ImageMessage{ImageMessage: imageMessage}
		header.HasMediaAttachment = proto.Bool(true)
	}

	interactive := &waE2E.InteractiveMessage{
		Header: header,
		Body:   &waE2E.InteractiveMessage_Body{Text: proto.String(urlCardText(opts))},
		InteractiveMessage: &waE2E.InteractiveMessage_NativeFlowMessage_{
			NativeFlowMessage: &waE2E.InteractiveMessage_NativeFlowMessage{
				Buttons: []*waE2E.InteractiveMessage_NativeFlowMessage_NativeFlowButton{{
					Name:             proto.String("cta_url"),
					ButtonParamsJSON: proto.String(params),
				}},
				MessageVersion: proto.Int32(0),
			},
		},
		ContextInfo: &waE2E.ContextInfo{},
	}
	return &waE2E.Message{
		ViewOnceMessage: &waE2E.FutureProofMessage{
			Message: &waE2E.Message{
				InteractiveMessage: interactive,
			},
		},
	}, nil
}

func buildNativeFlowURLMessage(ctx context.Context, client *whatsmeow.Client, opts smokeMessageOptions) (*waE2E.Message, error) {
	_, params, err := nativeFlowURLButtonParams(opts)
	if err != nil {
		return nil, err
	}
	messageSecret, err := secureRandomBytes(32)
	if err != nil {
		return nil, fmt.Errorf("generate native flow message secret: %w", err)
	}

	header := &waE2E.InteractiveMessage_Header{
		Title:              proto.String(strings.TrimSpace(opts.PreviewTitle)),
		HasMediaAttachment: proto.Bool(false),
	}
	if opts.MediaPath != "" {
		imageMessage, err := buildUploadedImageMessage(ctx, client, opts.MediaPath, "")
		if err != nil {
			return nil, err
		}
		header.Media = &waE2E.InteractiveMessage_Header_ImageMessage{ImageMessage: imageMessage}
		header.HasMediaAttachment = proto.Bool(true)
	}

	body := strings.TrimSpace(opts.PreviewBody)
	if body == "" {
		body = strings.TrimSpace(opts.Text)
	}
	interactive := &waE2E.InteractiveMessage{
		Header: header,
		Body:   &waE2E.InteractiveMessage_Body{Text: proto.String(body)},
		InteractiveMessage: &waE2E.InteractiveMessage_NativeFlowMessage_{
			NativeFlowMessage: &waE2E.InteractiveMessage_NativeFlowMessage{
				Buttons: []*waE2E.InteractiveMessage_NativeFlowMessage_NativeFlowButton{{
					Name:             proto.String("cta_url"),
					ButtonParamsJSON: proto.String(params),
				}},
			},
		},
	}
	return &waE2E.Message{
		ViewOnceMessage: &waE2E.FutureProofMessage{
			Message: &waE2E.Message{
				MessageContextInfo: &waE2E.MessageContextInfo{
					DeviceListMetadata:        &waE2E.DeviceListMetadata{},
					DeviceListMetadataVersion: proto.Int32(2),
					MessageSecret:             messageSecret,
				},
				InteractiveMessage: interactive,
			},
		},
	}, nil
}

func buildTemplateURLMessage(ctx context.Context, client *whatsmeow.Client, opts smokeMessageOptions) (*waE2E.Message, error) {
	button, _, err := nativeFlowURLButtonParams(opts)
	if err != nil {
		return nil, err
	}
	hydrated := &waE2E.TemplateMessage_HydratedFourRowTemplate{
		HydratedContentText: proto.String(urlCardText(opts)),
		TemplateID:          proto.String("whatsmeow-url-template"),
		HydratedButtons: []*waE2E.HydratedTemplateButton{{
			HydratedButton: &waE2E.HydratedTemplateButton_UrlButton{
				UrlButton: &waE2E.HydratedTemplateButton_HydratedURLButton{
					DisplayText: proto.String(button),
					URL:         proto.String(strings.TrimSpace(opts.PreviewURL)),
				},
			},
			Index: proto.Uint32(1),
		}},
	}
	if opts.MediaPath != "" {
		imageMessage, err := buildUploadedImageMessage(ctx, client, opts.MediaPath, "")
		if err != nil {
			return nil, err
		}
		hydrated.Title = &waE2E.TemplateMessage_HydratedFourRowTemplate_ImageMessage{
			ImageMessage: imageMessage,
		}
	} else if title := strings.TrimSpace(opts.PreviewTitle); title != "" {
		hydrated.Title = &waE2E.TemplateMessage_HydratedFourRowTemplate_HydratedTitleText{
			HydratedTitleText: title,
		}
	}
	return &waE2E.Message{
		ViewOnceMessage: &waE2E.FutureProofMessage{
			Message: &waE2E.Message{
				TemplateMessage: &waE2E.TemplateMessage{
					HydratedTemplate: hydrated,
				},
			},
		},
	}, nil
}

func urlCardText(opts smokeMessageOptions) string {
	body := strings.TrimSpace(opts.PreviewBody)
	if body == "" {
		body = strings.TrimSpace(opts.Text)
	}
	title := strings.TrimSpace(opts.PreviewTitle)
	if title == "" {
		return body
	}
	if body == "" || body == title {
		return "*" + title + "*"
	}
	return "*" + title + "*\n" + body
}

func buildButtonsURLMessage(ctx context.Context, client *whatsmeow.Client, opts smokeMessageOptions) (*waE2E.Message, error) {
	body := strings.TrimSpace(opts.PreviewBody)
	if body == "" {
		body = opts.Text
	}
	title := strings.TrimSpace(opts.PreviewTitle)
	if title == "" {
		title = "Link"
	}
	button, params, err := nativeFlowURLButtonParams(opts)
	if err != nil {
		return nil, err
	}

	buttonType := waE2E.ButtonsMessage_Button_NATIVE_FLOW
	headerType := waE2E.ButtonsMessage_TEXT
	buttonsMessage := &waE2E.ButtonsMessage{
		Header:      &waE2E.ButtonsMessage_Text{Text: title},
		ContentText: proto.String(body),
		Buttons: []*waE2E.ButtonsMessage_Button{{
			ButtonID: proto.String("cta_url"),
			ButtonText: &waE2E.ButtonsMessage_Button_ButtonText{
				DisplayText: proto.String(button),
			},
			Type: &buttonType,
			NativeFlowInfo: &waE2E.ButtonsMessage_Button_NativeFlowInfo{
				Name:       proto.String("cta_url"),
				ParamsJSON: proto.String(params),
			},
		}},
		HeaderType: &headerType,
	}
	if opts.MediaPath != "" {
		imageMessage, err := buildUploadedImageMessage(ctx, client, opts.MediaPath, "")
		if err != nil {
			return nil, err
		}
		headerType = waE2E.ButtonsMessage_IMAGE
		buttonsMessage.Header = &waE2E.ButtonsMessage_ImageMessage{ImageMessage: imageMessage}
		buttonsMessage.HeaderType = &headerType
	}
	return &waE2E.Message{ButtonsMessage: buttonsMessage}, nil
}

func nativeFlowURLButtonParams(opts smokeMessageOptions) (string, string, error) {
	button := strings.TrimSpace(opts.PreviewButton)
	if button == "" {
		button = "Open"
	}
	previewURL := strings.TrimSpace(opts.PreviewURL)
	if previewURL == "" {
		return "", "", errors.New("interactive URL messages require -preview-url")
	}
	params, err := json.Marshal(map[string]string{
		"display_text": button,
		"url":          previewURL,
	})
	if err != nil {
		return "", "", fmt.Errorf("marshal native flow URL params: %w", err)
	}
	return button, string(params), nil
}

func buildExternalAdMessage(opts smokeMessageOptions) (*waE2E.Message, error) {
	thumbnail, err := readOptionalThumbnail(opts.MediaPath)
	if err != nil {
		return nil, err
	}
	body := strings.TrimSpace(opts.PreviewBody)
	if body == "" {
		body = opts.Text
	}
	mediaType := waE2E.ContextInfo_ExternalAdReplyInfo_IMAGE
	externalAd := &waE2E.ContextInfo_ExternalAdReplyInfo{
		Title:                 proto.String(opts.PreviewTitle),
		Body:                  proto.String(body),
		MediaType:             &mediaType,
		Thumbnail:             thumbnail,
		SourceURL:             proto.String(opts.PreviewURL),
		MediaURL:              proto.String(opts.PreviewURL),
		RenderLargerThumbnail: proto.Bool(true),
		ShowAdAttribution:     proto.Bool(false),
	}
	contextInfo := &waE2E.ContextInfo{
		ExternalAdReply: externalAd,
	}
	if opts.ExternalAction {
		adType := waE2E.ContextInfo_ExternalAdReplyInfo_CTWA
		externalAd.ContainsAutoReply = proto.Bool(true)
		externalAd.AdType = &adType
		externalAd.CtaPayload = proto.String(opts.PreviewButton)
		contextInfo.ActionLink = &waE2E.ActionLink{
			URL:         proto.String(opts.PreviewURL),
			ButtonTitle: proto.String(opts.PreviewButton),
		}
	}
	return &waE2E.Message{
		ExtendedTextMessage: &waE2E.ExtendedTextMessage{
			Text:        proto.String(opts.Text),
			ContextInfo: contextInfo,
		},
	}, nil
}

func buildLinkPreviewMessage(ctx context.Context, client *whatsmeow.Client, opts smokeMessageOptions) (*waE2E.Message, error) {
	thumbnail, err := prepareLinkPreviewThumbnail(ctx, client, opts.MediaPath)
	if err != nil {
		return nil, err
	}
	body := strings.TrimSpace(opts.PreviewBody)
	if body == "" {
		body = opts.Text
	}
	previewType := waE2E.ExtendedTextMessage_NONE
	forwardingScore := uint32(1)
	extended := &waE2E.ExtendedTextMessage{
		Text:        proto.String(opts.Text),
		MatchedText: proto.String(opts.PreviewURL),
		Title:       proto.String(opts.PreviewTitle),
		Description: proto.String(body),
		PreviewType: &previewType,
		ContextInfo: &waE2E.ContextInfo{
			ForwardingScore: proto.Uint32(forwardingScore),
			IsForwarded:     proto.Bool(true),
		},
	}
	if thumbnail != nil {
		extended.JPEGThumbnail = thumbnail.Inline
		extended.ThumbnailDirectPath = proto.String(thumbnail.Upload.DirectPath)
		extended.MediaKey = thumbnail.Upload.MediaKey
		extended.MediaKeyTimestamp = proto.Int64(time.Now().Unix())
		extended.ThumbnailSHA256 = thumbnail.Upload.FileSHA256
		extended.ThumbnailEncSHA256 = thumbnail.Upload.FileEncSHA256
		if thumbnail.Width > 0 {
			extended.ThumbnailWidth = proto.Uint32(thumbnail.Width)
		}
		if thumbnail.Height > 0 {
			extended.ThumbnailHeight = proto.Uint32(thumbnail.Height)
		}
	}
	return &waE2E.Message{ExtendedTextMessage: extended}, nil
}

func readOptionalThumbnail(path string) ([]byte, error) {
	if path == "" {
		return nil, nil
	}
	data, mimetype, err := readMediaFile(path)
	if err != nil {
		return nil, err
	}
	return thumbnailOrNil(data, mimetype), nil
}

type linkPreviewThumbnail struct {
	Inline []byte
	Upload whatsmeow.UploadResponse
	Width  uint32
	Height uint32
}

func prepareLinkPreviewThumbnail(ctx context.Context, client *whatsmeow.Client, path string) (*linkPreviewThumbnail, error) {
	if path == "" {
		return nil, nil
	}
	data, mimetype, err := readMediaFile(path)
	if err != nil {
		return nil, err
	}
	upload, err := client.Upload(ctx, data, whatsmeow.MediaLinkThumbnail)
	if err != nil {
		return nil, fmt.Errorf("upload link preview thumbnail: %w", err)
	}
	width, height := imageDimensions(data)
	return &linkPreviewThumbnail{
		Inline: thumbnailOrNil(data, mimetype),
		Upload: upload,
		Width:  width,
		Height: height,
	}, nil
}

func imageDimensions(data []byte) (uint32, uint32) {
	cfg, _, err := image.DecodeConfig(bytes.NewReader(data))
	if err != nil || cfg.Width <= 0 || cfg.Height <= 0 {
		return 0, 0
	}
	return uint32(cfg.Width), uint32(cfg.Height)
}

func readMediaFile(path string) ([]byte, string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, "", fmt.Errorf("read media path: %w", err)
	}
	mimetype := mime.TypeByExtension(strings.ToLower(filepath.Ext(path)))
	if mimetype == "" {
		mimetype = http.DetectContentType(data[:min(len(data), 512)])
	}
	if mimetype == "" {
		mimetype = "application/octet-stream"
	}
	return data, mimetype, nil
}

func thumbnailOrNil(data []byte, mimetype string) []byte {
	if strings.HasPrefix(mimetype, "image/jpeg") {
		return data
	}
	return nil
}

func addContactBeforeSend(ctx context.Context, client *whatsmeow.Client, device *store.Device, target types.JID, contactName, mode string, myAppStateKeyID []byte, appStateKeyWait time.Duration) error {
	if mode == "appstate" {
		return addContactViaAppState(ctx, client, device, target, contactName, myAppStateKeyID, appStateKeyWait)
	}
	if err := addContactsBeforeSend(ctx, client, device, []types.JID{target}, mode, myAppStateKeyID, appStateKeyWait, 1, 0); err != nil {
		return err
	}
	fmt.Printf("added contact jid=%s name=%q method=%s\n", target, contactName, mode)
	return nil
}

func addContactsBeforeSend(ctx context.Context, client *whatsmeow.Client, device *store.Device, targets []types.JID, mode string, myAppStateKeyID []byte, appStateKeyWait time.Duration, batchSize int, pause time.Duration) error {
	if len(targets) == 0 {
		return nil
	}
	switch mode {
	case "usync":
		for start := 0; start < len(targets); start += batchSize {
			end := min(start+batchSize, len(targets))
			chunk := targets[start:end]
			result, err := addContactsViaUSync(ctx, client, chunk)
			if err != nil {
				return err
			}
			if err := setTrustedContacts(ctx, client, result.In); err != nil {
				fmt.Printf("warning: trusted contact token failed after usync add: %v\n", err)
			}
			fmt.Printf("added contacts via usync requested=%d in=%d out=%d batch=%d-%d/%d\n", len(chunk), len(result.In), len(result.Out), start+1, end, len(targets))
			if pause > 0 && end < len(targets) {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(pause):
				}
			}
		}
		return nil
	case "appstate":
		for start, target := range targets {
			if err := addContactViaAppState(ctx, client, device, target, target.User, myAppStateKeyID, appStateKeyWait); err != nil {
				return err
			}
			if pause > 0 && start+1 < len(targets) {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(pause):
				}
			}
		}
		return nil
	default:
		return fmt.Errorf("unknown add contact method %q", mode)
	}
}

type usyncContactMutationResult struct {
	In  []types.JID
	Out []types.JID
}

func addContactsViaUSync(ctx context.Context, client *whatsmeow.Client, targets []types.JID) (usyncContactMutationResult, error) {
	userList := make([]waBinary.Node, 0, len(targets))
	for _, target := range targets {
		contact := "+" + strings.TrimPrefix(target.ToNonAD().User, "+")
		userList = append(userList, waBinary.Node{
			Tag: "user",
			Content: []waBinary.Node{{
				Tag:     "contact",
				Content: []byte(contact),
			}},
		})
	}
	resp, err := client.DangerousInternals().SendIQ(ctx, whatsmeow.DangerousInfoQuery{
		Namespace: "usync",
		Type:      whatsmeow.DangerousInfoQueryType("get"),
		To:        types.ServerJID,
		Content: []waBinary.Node{{
			Tag: "usync",
			Attrs: waBinary.Attrs{
				"sid":            "sync_sid_delta_" + client.DangerousInternals().GenerateRequestID(),
				"index":          "0",
				"last":           "true",
				"mode":           "delta",
				"context":        "background",
				"allow_mutation": "true",
			},
			Content: []waBinary.Node{
				{
					Tag: "query",
					Content: []waBinary.Node{
						{Tag: "contact"},
						{Tag: "status"},
						{Tag: "business", Content: []waBinary.Node{
							{Tag: "verified_name"},
							{Tag: "profile", Attrs: waBinary.Attrs{"v": "1876"}},
						}},
						{Tag: "devices", Attrs: waBinary.Attrs{"version": "2"}},
						{Tag: "disappearing_mode"},
						{Tag: "lid"},
					},
				},
				{
					Tag:     "list",
					Content: userList,
				},
			},
		}},
	})
	if err != nil {
		return usyncContactMutationResult{}, fmt.Errorf("usync contact mutation: %w", err)
	}
	if err = cacheUSyncLIDMappings(ctx, client, resp); err != nil {
		fmt.Printf("warning: failed to cache usync contact LID mapping: %v\n", err)
	}
	return parseUSyncContactMutationResult(resp), nil
}

func setTrustedContacts(ctx context.Context, client *whatsmeow.Client, targets []types.JID) error {
	if len(targets) == 0 {
		return nil
	}
	for _, target := range targets {
		_, err := client.DangerousInternals().SendIQ(ctx, whatsmeow.DangerousInfoQuery{
			Namespace: "privacy",
			Type:      whatsmeow.DangerousInfoQueryType("set"),
			To:        types.ServerJID,
			Timeout:   15 * time.Second,
			Content: []waBinary.Node{{
				Tag: "tokens",
				Content: []waBinary.Node{{
					Tag: "token",
					Attrs: waBinary.Attrs{
						"jid":  target.ToNonAD(),
						"t":    strconv.FormatInt(time.Now().Unix(), 10),
						"type": "trusted_contact",
					},
				}},
			}},
		})
		if err != nil {
			return fmt.Errorf("set trusted contact token for %s: %w", target.ToNonAD(), err)
		}
	}
	return nil
}

func parseUSyncContactMutationResult(resp *waBinary.Node) usyncContactMutationResult {
	list, ok := resp.GetOptionalChildByTag("usync", "list")
	if !ok {
		return usyncContactMutationResult{}
	}
	var result usyncContactMutationResult
	for _, user := range list.GetChildren() {
		if user.Tag != "user" {
			continue
		}
		jid := user.AttrGetter().OptionalJIDOrEmpty("jid")
		if jid.IsEmpty() {
			continue
		}
		contactNode := user.GetChildByTag("contact")
		switch contactNode.AttrGetter().OptionalString("type") {
		case "in":
			result.In = append(result.In, jid)
		case "out":
			result.Out = append(result.Out, jid)
		}
	}
	return result
}

func usePersistentSQLStore(ctx context.Context, device *store.Device, dialect, address string, maxIdleConns, maxOpenConns int) (*sqlstore.Container, error) {
	dialect = strings.TrimSpace(dialect)
	if dialect == "" {
		dialect = inferSQLDialect(address)
	}
	if dialect == "postgres" {
		sqlstore.PostgresArrayWrapper = pq.Array
	}

	db, err := sql.Open(dialect, address)
	if err != nil {
		return nil, fmt.Errorf("open %s database: %w", dialect, err)
	}
	if maxIdleConns > 0 {
		db.SetMaxIdleConns(maxIdleConns)
	}
	if maxOpenConns > 0 {
		db.SetMaxOpenConns(maxOpenConns)
	}

	container := sqlstore.NewWithDB(db, dialect, nil)
	if err = container.Upgrade(ctx); err != nil {
		_ = container.Close()
		return nil, fmt.Errorf("upgrade %s database: %w", dialect, err)
	}

	device.Initialized = false
	if err = container.PutDevice(ctx, device); err != nil {
		_ = container.Close()
		return nil, fmt.Errorf("store imported device: %w", err)
	}
	if err = updateImportedDeviceCredentials(ctx, db, device); err != nil {
		_ = container.Close()
		return nil, fmt.Errorf("update imported device credentials: %w", err)
	}
	if !device.GetJID().IsEmpty() && !device.GetLID().IsEmpty() {
		if err = device.LIDs.PutLIDMapping(ctx, device.GetLID().ToNonAD(), device.GetJID().ToNonAD()); err != nil {
			_ = container.Close()
			return nil, fmt.Errorf("store own LID mapping: %w", err)
		}
	}
	return container, nil
}

func updateImportedDeviceCredentials(ctx context.Context, db *sql.DB, device *store.Device) error {
	_, err := db.ExecContext(ctx, `
		UPDATE whatsmeow_device
		SET registration_id=$2,
			noise_key=$3,
			identity_key=$4,
			signed_pre_key=$5,
			signed_pre_key_id=$6,
			signed_pre_key_sig=$7,
			adv_key=$8,
			adv_details=$9,
			adv_account_sig=$10,
			adv_account_sig_key=$11,
			adv_device_sig=$12
		WHERE jid=$1
	`,
		device.GetJID(),
		device.RegistrationID,
		device.NoiseKey.Priv[:],
		device.IdentityKey.Priv[:],
		device.SignedPreKey.Priv[:],
		device.SignedPreKey.KeyID,
		device.SignedPreKey.Signature[:],
		device.AdvSecretKey,
		device.Account.Details,
		device.Account.AccountSignature,
		device.Account.AccountSignatureKey,
		device.Account.DeviceSignature,
	)
	return err
}

func inferSQLDialect(address string) string {
	lower := strings.ToLower(strings.TrimSpace(address))
	if strings.HasPrefix(lower, "postgres://") || strings.HasPrefix(lower, "postgresql://") || strings.Contains(lower, "host=") {
		return "postgres"
	}
	if strings.HasPrefix(lower, "file:") || strings.HasSuffix(lower, ".db") || strings.Contains(lower, "_foreign_keys") {
		return "sqlite3"
	}
	return "postgres"
}

func redactSQLAddress(address string) string {
	parsed, err := url.Parse(address)
	if err != nil || parsed.User == nil {
		return address
	}
	username := parsed.User.Username()
	if username == "" {
		parsed.User = url.UserPassword("redacted", "redacted")
	} else {
		parsed.User = url.UserPassword(username, "redacted")
	}
	return parsed.String()
}

func cacheUSyncLIDMappings(ctx context.Context, client *whatsmeow.Client, resp *waBinary.Node) error {
	list, ok := resp.GetOptionalChildByTag("usync", "list")
	if !ok {
		return nil
	}
	var mappings []store.LIDMapping
	for _, user := range list.GetChildren() {
		if user.Tag != "user" {
			continue
		}
		pn := user.AttrGetter().OptionalJIDOrEmpty("jid")
		lidNode := user.GetChildByTag("lid")
		lid := lidNode.AttrGetter().OptionalJIDOrEmpty("val")
		if !pn.IsEmpty() && !lid.IsEmpty() {
			mappings = append(mappings, store.LIDMapping{PN: pn, LID: lid})
		}
	}
	if len(mappings) == 0 {
		return nil
	}
	return client.Store.LIDs.PutManyLIDMappings(ctx, mappings)
}

func addContactViaAppState(ctx context.Context, client *whatsmeow.Client, device *store.Device, target types.JID, contactName string, myAppStateKeyID []byte, appStateKeyWait time.Duration) error {
	if countAppStateKeys(device) == 0 && appStateKeyWait > 0 {
		fmt.Printf("no app-state sync keys loaded; requesting key share and waiting up to %s\n", appStateKeyWait)
		if err := recoverAppStateKeys(ctx, client, device, myAppStateKeyID, appStateKeyWait); err != nil {
			fmt.Printf("warning: app-state key recovery did not complete: %v\n", err)
		} else {
			fmt.Printf("recovered app-state sync keys count=%d\n", countAppStateKeys(device))
		}
	}
	if keyCount := countAppStateKeys(device); keyCount > 0 {
		fmt.Printf("syncing contact app-state before add contact: keys=%d\n", keyCount)
		syncCtx := ctx
		var cancelSync context.CancelFunc
		if appStateKeyWait > 0 {
			syncCtx, cancelSync = context.WithTimeout(ctx, appStateKeyWait)
		}
		if err := client.FetchAppState(syncCtx, appstate.WAPatchCriticalUnblockLow, true, false); err != nil {
			fmt.Printf("warning: contact app-state sync failed before add contact: %v\n", err)
		}
		if cancelSync != nil {
			cancelSync()
		}
	}
	if err := client.SendAppState(ctx, buildAddContactPatch(target, contactName)); err != nil {
		return err
	}
	fmt.Printf("added contact via app-state jid=%s name=%q\n", target, contactName)
	return nil
}

func recoverAppStateKeys(ctx context.Context, client *whatsmeow.Client, device *store.Device, myAppStateKeyID []byte, wait time.Duration) error {
	if countAppStateKeys(device) > 0 {
		return nil
	}

	waitCtx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()

	var requestErr error
	if len(myAppStateKeyID) > 0 {
		requestErr = requestAppStateKeyByID(waitCtx, client, myAppStateKeyID)
		if requestErr == nil {
			initialWait := min(wait, 5*time.Second)
			if waitForAppStateKey(waitCtx, device, initialWait) {
				return nil
			}
		}
	}

	var fetchErrors []error
	for _, name := range appstate.AllPatchNames {
		err := client.FetchAppState(waitCtx, name, true, false)
		if err != nil {
			fetchErrors = append(fetchErrors, fmt.Errorf("%s: %w", name, err))
		}
		if countAppStateKeys(device) > 0 {
			return nil
		}
	}
	if requestErr != nil {
		fetchErrors = append(fetchErrors, fmt.Errorf("request key by id: %w", requestErr))
	}
	if countAppStateKeys(device) > 0 {
		return nil
	}

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-waitCtx.Done():
			if len(fetchErrors) > 0 {
				return fmt.Errorf("timed out waiting for app-state sync key share after recovery attempts: %w; recovery errors: %v", waitCtx.Err(), fetchErrors)
			}
			return fmt.Errorf("timed out waiting for app-state sync key share: %w", waitCtx.Err())
		case <-ticker.C:
			if countAppStateKeys(device) > 0 {
				return nil
			}
		}
	}
}

func waitForAppStateKey(ctx context.Context, device *store.Device, wait time.Duration) bool {
	if countAppStateKeys(device) > 0 {
		return true
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return countAppStateKeys(device) > 0
		case <-timer.C:
			return countAppStateKeys(device) > 0
		case <-ticker.C:
			if countAppStateKeys(device) > 0 {
				return true
			}
		}
	}
}

func waitForInitialAppStateSync(ctx context.Context, syncs <-chan appstate.WAPatchName, wait time.Duration) error {
	if wait <= 0 {
		return nil
	}
	waitCtx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()

	expected := make(map[appstate.WAPatchName]struct{}, len(appstate.AllPatchNames))
	for _, name := range appstate.AllPatchNames {
		expected[name] = struct{}{}
	}
	for len(expected) > 0 {
		select {
		case <-waitCtx.Done():
			pending := make([]string, 0, len(expected))
			for name := range expected {
				pending = append(pending, string(name))
			}
			sort.Strings(pending)
			return fmt.Errorf("%w (pending: %s)", waitCtx.Err(), strings.Join(pending, ", "))
		case name := <-syncs:
			delete(expected, name)
			fmt.Printf("app-state sync complete: name=%s remaining=%d\n", name, len(expected))
		}
	}
	return nil
}

func requestAppStateKeyByID(ctx context.Context, client *whatsmeow.Client, keyID []byte) error {
	_, err := client.SendPeerMessage(ctx, &waE2E.Message{
		ProtocolMessage: &waE2E.ProtocolMessage{
			Type: waE2E.ProtocolMessage_APP_STATE_SYNC_KEY_REQUEST.Enum(),
			AppStateSyncKeyRequest: &waE2E.AppStateSyncKeyRequest{
				KeyIDs: []*waE2E.AppStateSyncKeyId{{
					KeyID: keyID,
				}},
			},
		},
	})
	return err
}

func countAppStateKeys(device *store.Device) int {
	if storeWithCount, ok := device.AppStateKeys.(interface{ AppStateKeyCount() int }); ok {
		return storeWithCount.AppStateKeyCount()
	}
	keys, err := device.AppStateKeys.GetAllAppStateSyncKeys(context.Background())
	if err == nil {
		return len(keys)
	}
	return 0
}

func buildAddContactPatch(target types.JID, fullName string) appstate.PatchInfo {
	fullName = strings.TrimSpace(fullName)
	if fullName == "" {
		fullName = target.User
	}
	return appstate.PatchInfo{
		Type: appstate.WAPatchCriticalUnblockLow,
		Mutations: []appstate.MutationInfo{{
			Index:   []string{appstate.IndexContact, target.String()},
			Version: 2,
			Value: &waSyncAction.SyncActionValue{
				ContactAction: &waSyncAction.ContactAction{
					FullName:                 proto.String(fullName),
					SaveOnPrimaryAddressbook: proto.Bool(true),
				},
			},
		}},
	}
}

func parseTargetJIDList(input string) ([]types.JID, error) {
	if strings.TrimSpace(input) == "" {
		return nil, nil
	}
	parts := strings.FieldsFunc(input, func(r rune) bool {
		return r == ',' || r == ';' || r == '\n' || r == '\r' || r == '\t' || r == ' '
	})
	targets := make([]types.JID, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, part := range parts {
		target, err := parseTargetJID(part)
		if err != nil {
			return nil, fmt.Errorf("parse add contact target %q: %w", part, err)
		}
		key := target.ToNonAD().String()
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		targets = append(targets, target)
	}
	return targets, nil
}

func parseTargetJID(input string) (types.JID, error) {
	if input == "" {
		return types.EmptyJID, errors.New("empty target")
	}
	if strings.Contains(input, "@") {
		if jid, err := types.ParseJID(input); err == nil && jid.User != "" && jid.Server != "" {
			return jid, nil
		}
	}
	phone := make([]byte, 0, len(input))
	for i := 0; i < len(input); i++ {
		if input[i] >= '0' && input[i] <= '9' {
			phone = append(phone, input[i])
		}
	}
	if len(phone) <= 6 {
		return types.EmptyJID, fmt.Errorf("target %q is not a valid JID or international phone number", input)
	}
	return types.NewJID(string(phone), types.DefaultUserServer), nil
}

func cloneBytes(input []byte) []byte {
	if input == nil {
		return nil
	}
	return append([]byte(nil), input...)
}
