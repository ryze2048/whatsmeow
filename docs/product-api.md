# Product API Usage

This project exposes the imported-account flow as library APIs. Other projects should not call
`cmd/import-baileys-creds`; use the packages directly.

## Import a Baileys JSON Account

```go
package main

import (
	"context"
	"time"

	"github.com/lib/pq"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/baileysauth"
	"go.mau.fi/whatsmeow/messagebuilder"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	waLog "go.mau.fi/whatsmeow/util/log"
)

func main() {
	ctx := context.Background()

	sqlstore.PostgresArrayWrapper = pq.Array
	container, err := sqlstore.New(ctx, "postgres", "postgres://admin:admin123@127.0.0.1:5432/app?sslmode=disable", nil)
	if err != nil {
		panic(err)
	}
	defer container.Close()

	imported, err := baileysauth.ImportFileIntoContainer(ctx, container, "/path/to/account.json")
	if err != nil {
		panic(err)
	}

	client := whatsmeow.NewClient(imported.Device, waLog.Stdout("WA", "INFO", true))
	if err = client.Connect(); err != nil {
		panic(err)
	}
	defer client.Disconnect()

	if err = client.RecoverAppStateKeys(ctx, imported.MyAppStateKeyID, time.Minute); err != nil {
		// Existing stores may already have keys. Treat this as operational warning if the
		// next app-state operation does not require immediate recovery.
	}

	target := types.NewJID("919948667476", types.DefaultUserServer)
	if err = client.AddContact(ctx, target, "Test Contact"); err != nil {
		panic(err)
	}

	built, err := messagebuilder.Build(ctx, client, messagebuilder.Options{
		Kind:          messagebuilder.KindNativeFlowURL,
		Text:          "hello world",
		MediaPath:     "/path/to/card.jpg",
		PreviewTitle:  "Link",
		PreviewBody:   "hello world",
		PreviewURL:    "https://example.com/",
		PreviewButton: "Open",
	})
	if err != nil {
		panic(err)
	}
	resp, err := client.SendMessage(ctx, target, built.Message, built.SendRequestExtra())
	if err != nil {
		panic(err)
	}

	time.Sleep(5 * time.Second)
	if err = client.DeleteMessageForMe(ctx, target, types.EmptyJID, resp.ID, true, resp.Timestamp, false); err != nil {
		panic(err)
	}

	lastKey := client.BuildMessageKey(target, types.EmptyJID, resp.ID)
	if err = client.DeleteChatForMe(ctx, target, resp.Timestamp, lastKey, false); err != nil {
		panic(err)
	}
}
```

## Main APIs

`go.mau.fi/whatsmeow/baileysauth`:

- `Parse(data []byte)`
- `ParseFile(path string)`
- `ImportIntoContainer(ctx, container, data)`
- `ImportFileIntoContainer(ctx, container, path)`
- `ImportAuthDir(ctx, device, dir)`

`go.mau.fi/whatsmeow` client methods:

- `AppStateKeyCount(ctx)`
- `RequestAppStateKey(ctx, keyID)`
- `RecoverAppStateKeys(ctx, keyID, wait)`
- `AddContact(ctx, target, fullName)`
- `DeleteMessageForMe(ctx, target, sender, messageID, fromMe, messageTimestamp, deleteMedia)`
- `DeleteChatForMe(ctx, target, lastMessageTimestamp, lastMessageKey, deleteMedia)`

`go.mau.fi/whatsmeow/messagebuilder`:

- `Build(ctx, client, opts)`
- `BuildMessage(ctx, client, opts)`
- `UploadedImage(ctx, client, path, caption)`
- `AdditionalNodesForKind(kind)`
- supported kinds: `text`, `image`, `external-ad`, `link-preview`, `interactive-url`, `native-flow-url`, `template-url`, `buttons-url`

## Lifecycle Wrapper

For application code, prefer `go.mau.fi/whatsmeow/importedclient`. The main
entrypoint accepts JSON bytes, so callers can read files, decrypt payloads, or
fetch credentials from APIs before importing.

```go
account, err := importedclient.Open(ctx, importedclient.Config{
	CredsJSON: jsonBytes,

	DBDialect:      "postgres",
	DBURI:          "postgres://admin:admin123@127.0.0.1:5432/app?sslmode=disable",
	DBMaxIdleConns: 10,
	DBMaxOpenConns: 100,

	Proxy:            "socks5://user:pass@host:port",
	MediaProxy:       "socks5://user:pass@host:port",
	TransportTimeout: 90 * time.Second,
	AppStateKeyWait:  time.Minute,
})
if err != nil {
	return err
}
defer account.Close()

if err = account.EnsureReady(ctx); err != nil {
	return err
}
if _, err = account.AddContact(ctx, "919948667476", "Test Contact"); err != nil {
	return err
}
sent, err := account.Send(ctx, "919948667476", messagebuilder.Options{
	Kind:          messagebuilder.KindNativeFlowURL,
	Text:          "hello world",
	MediaPath:     "/path/to/card.jpg",
	PreviewTitle:  "Link",
	PreviewBody:   "hello world",
	PreviewURL:    "https://example.com/",
	PreviewButton: "Open",
})
if err != nil {
	return err
}
if err = account.DeleteMessageForMeAfter(ctx, sent, 5*time.Second, false); err != nil {
	return err
}
return account.DeleteChatForMe(ctx, sent, false)
```

## Notes

- Use a persistent store for production. PostgreSQL and SQLite are both supported by `sqlstore`.
- `RecoverAppStateKeys` is mainly for imported JSON accounts. QR-linked accounts usually receive
  app-state keys naturally, and the persistent store saves them.
- `DeleteMessageForMe` and `DeleteChatForMe` only affect the currently logged-in account.
  The sender cannot remotely delete the receiver's local chat list.
- Persist every sent message's `chat_jid`, `message_id`, `timestamp`, and `from_me` in your business DB.
