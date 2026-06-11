# 外部账号能力接入开发文档（交付版）

本文面向后续接手开发者或集成方，说明如何在当前 `whatsmeow` 项目中继续封装外部账号能力。本文不是 WhatsApp 官方 API 文档，而是基于当前项目实验结果整理出的二次开发说明。

本文覆盖：

- 外部账号密钥导入登录
- 添加联系人
- 发送消息
- 删除当前账号侧的消息

当前实验入口在 `cmd/import-baileys-creds`。该命令用于验证链路，不是最终业务 API。生产系统应在此基础上抽象账号管理、连接管理、消息发送、消息删除和任务限速。

## 0. 阅读说明

### 0.1 适用读者

- 需要接手本项目继续开发的后端开发者
- 需要把外部账号接入封装成业务 API 的开发者
- 需要理解导入账号、添加联系人、发消息、删除自己侧消息边界的人

### 0.2 已验证结论

- 外部 Baileys 风格 JSON 密钥可以导入到 `whatsmeow` 并登录。
- 通过 USync mutation 可以添加联系人，不依赖 `app-state-sync-key`。
- 文本、图片、链接预览、交互 URL 卡片均可走当前 demo 发送。
- 媒体上传必须走可上传的 media host，当前代码已跳过没有 `<upload/>` 的 host。
- “删除当前账号消息”不是撤回消息，需要 app-state key；纯号商 JSON 通常不具备这个能力。

### 0.3 重要边界

- `BuildRevoke` 是撤回消息，会影响对方，不等于“只删除自己这边”。
- `deleteMessageForMe` 是只删除当前账号侧消息，但依赖 app-state sync key。
- 如果账号没有 app-state key，只能做业务系统本地隐藏，不能同步到 WhatsApp 当前账号所有设备。
- 代理不仅影响登录，也影响媒体上传和下载。
- 不要把真实账号密钥、代理账号密码提交到仓库。

## 1. 能力清单

| 能力 | 当前状态 | 说明 |
| --- | --- | --- |
| 外部 JSON 密钥导入 | 已实现 | 支持 Baileys 风格 `creds.json` / 号商 JSON |
| WebSocket 登录 | 已实现 | 支持 SOCKS5/HTTP 代理 |
| 添加联系人 | 已实现 | 默认走 USync mutation，不依赖 app-state key |
| 批量添加联系人 | 已实现 | `-add-contacts` + batch/pause |
| 文本发送 | 已实现 | `-message-kind text` |
| 图片发送 | 已实现 | `-message-kind image` |
| 链接预览 | 已实现 | `-message-kind link-preview` |
| 交互 URL 卡片 | 已实现 | `-message-kind interactive-url`，已完成服务端 ack；展示效果需按客户端验证 |
| external-ad | 实验性 | 服务端可 ack，但普通会话客户端可能不展示 |
| 删除当前账号消息 | 待封装 | 需要 app-state key；号商 JSON 通常不带 |
| 撤回消息 | 库已有能力 | `Client.BuildRevoke`，但这会影响对方，不是“只删自己” |

## 2. 关键代码位置

| 文件 | 作用 |
| --- | --- |
| `cmd/import-baileys-creds/main.go` | 外部密钥导入、连接、添加联系人、发送 demo |
| `mediaconn.go` | 解析 WhatsApp media host，已记录 host 是否支持 upload |
| `upload.go` | 媒体上传，已支持只优先使用 upload host 并失败切换 host |
| `appstate/encode.go` | app-state patch 构造，后续应补 `BuildDeleteMessageForMe` |
| `appstate.go` | `Client.SendAppState` 和 app-state mutation 分发 |
| `send.go` | 普通消息发送、撤回消息 `BuildRevoke` |

## 3. 外部账号密钥导入登录

### 3.1 输入格式

当前支持 Baileys 风格 JSON，例如：

```text
docs/keypair/21698290649.json
docs/keypair/244975542278.json
```

关键字段包括：

- `me.id`
- `me.lid`
- `noiseKey`
- `signedIdentityKey`
- `signedPreKey`
- `advSecretKey`
- `registrationId`
- `nextPreKeyId`
- `firstUnuploadedPreKeyId`
- `account`

导入后会构造 `store.Device` 和内存 store，让 `whatsmeow.NewClient` 可以直接登录。

### 3.2 登录流程

```text
JSON creds
  -> loadBaileysCreds
  -> store.Device + memoryStore
  -> whatsmeow.NewClient
  -> SetTransport(proxy)
  -> ConnectContext
  -> success
```

注意点：

- 代理同时用于 WebSocket 和媒体上传/下载。
- `lidDbMigrated` 默认保持 `false`，与 Baileys 对这类导入号的行为一致。
- 登录成功不代表账号可长期稳定使用，号状态、代理质量、目标行为都会影响风控。

### 3.3 示例命令

```bash
env GOCACHE=/private/tmp/whatsmeow-gocache go run . \
  -creds '../../docs/keypair/21698290649.json' \
  -connect \
  -timeout 120s \
  -send-timeout 240s \
  -transport-timeout 90s \
  -proxy 'socks5://USER:PASS@HOST:PORT' \
  -debug
```

## 4. 添加联系人

### 4.1 推荐方式：USync mutation

当前默认添加联系人方式是：

```text
USync get
mode="delta"
allow_mutation="true"
query: contact/status/business/devices/disappearing_mode/lid
list: <user><contact>+phone</contact></user>
```

优点：

- 不依赖 `app-state-sync-key`
- 适合号商 JSON
- 可批量添加
- 已验证返回 `contact integrity="pass"` 时可以认为添加动作被服务端接受

### 4.2 单个添加并发送

```bash
env GOCACHE=/private/tmp/whatsmeow-gocache go run . \
  -creds '../../docs/keypair/21698290649.json' \
  -connect \
  -timeout 120s \
  -send-timeout 240s \
  -transport-timeout 90s \
  -proxy 'socks5://USER:PASS@HOST:PORT' \
  -send-to '27651432974' \
  -add-contact \
  -add-contact-method usync \
  -contact-name 'Test Contact' \
  -message 'hello'
```

### 4.3 批量添加

```bash
env GOCACHE=/private/tmp/whatsmeow-gocache go run . \
  -creds '../../docs/keypair/21698290649.json' \
  -connect \
  -timeout 120s \
  -send-timeout 300s \
  -transport-timeout 90s \
  -proxy 'socks5://USER:PASS@HOST:PORT' \
  -add-contacts '27651432974,27650000001,27650000002' \
  -add-contact-batch-size 20 \
  -add-contact-pause 2s \
  -debug
```

返回结果里需要区分：

- `<contact type="in">`：服务端认为该号码有效，可进入 trusted token 流程
- `<contact type="out">`：号码不可用或未通过，不应当继续发 token 或发消息

### 4.4 trusted contact token

添加联系人后会发送 privacy token：

```xml
<iq type="set" xmlns="privacy">
  <tokens>
    <token jid="xxx@s.whatsapp.net" type="trusted_contact"/>
  </tokens>
</iq>
```

这一步用于降低后续直接发消息的异常概率，但不是“加好友成功”的唯一判断。批量时只对 `type="in"` 的联系人发 token。

## 5. 发送消息

### 5.1 支持的消息类型

| `-message-kind` | 说明 |
| --- | --- |
| `text` | 普通文本 |
| `image` | 图片消息，走 media upload |
| `link-preview` | `ExtendedTextMessage` 链接预览 |
| `external-ad` | 广告上下文引用，普通会话不稳定 |
| `interactive-url` | `viewOnceMessage -> interactiveMessage -> cta_url` |
| `buttons-url` | `ButtonsMessage` native-flow URL 兜底 |

### 5.2 文本消息

```bash
env GOCACHE=/private/tmp/whatsmeow-gocache go run . \
  -creds '../../docs/keypair/21698290649.json' \
  -connect \
  -timeout 120s \
  -send-timeout 180s \
  -transport-timeout 90s \
  -proxy 'socks5://USER:PASS@HOST:PORT' \
  -send-to '27651432974' \
  -message-kind text \
  -message 'hello from imported whatsmeow creds'
```

### 5.3 交互 URL 卡片

该结构对齐第三方项目中的实现：

```text
viewOnceMessage
  message
    messageContextInfo
    interactiveMessage
      header.imageMessage
      body.text
      nativeFlowMessage.buttons[0].name = "cta_url"
      nativeFlowMessage.buttons[0].buttonParamsJSON = {"display_text":"...","url":"..."}
```

示例：

```bash
env GOCACHE=/private/tmp/whatsmeow-gocache go run . \
  -creds '../../docs/keypair/21698290649.json' \
  -connect \
  -timeout 120s \
  -send-timeout 240s \
  -transport-timeout 90s \
  -proxy 'socks5://USER:PASS@HOST:PORT' \
  -send-to '27651432974' \
  -message-kind interactive-url \
  -message 'hello world' \
  -media-path '../../docs/image/2026-06-01_180749_007.jpg' \
  -preview-title 'Link' \
  -preview-body 'hello world' \
  -preview-url 'https://example.com/' \
  -preview-button 'click me auto' \
  -debug
```

成功日志应包含：

```text
<ack class="message" ... id="..."/>
sent smoke-test message id=...
```

### 5.4 媒体上传注意点

媒体上传也走代理。当前已修复两点：

- `mediaconn.go` 解析 `<host>` 时记录是否有 `<upload/>`
- `upload.go` 上传时优先使用支持 upload 的 host，并在失败后尝试下一个 host

代理慢时要区分：

- `-timeout`：登录认证超时
- `-send-timeout`：登录成功后的查号、上传、发送超时
- `-transport-timeout`：代理拨号/TLS/header 超时

## 6. 删除当前账号侧的消息

这里必须区分三个概念：

| 行为 | 影响对方 | 协议路径 | 是否需要 app-state key |
| --- | --- | --- | --- |
| 撤回消息 | 会影响对方 | `ProtocolMessage_REVOKE` / `BuildRevoke` | 否 |
| 删除当前账号消息 | 不影响对方 | app-state `deleteMessageForMe` | 是 |
| 仅本地删除 | 不影响 WhatsApp 服务端 | 删除业务库记录 | 否 |

用户当前要的是“发送方自己删除，对方不删除”，对应第二种：`deleteMessageForMe`。

### 6.1 重要限制

`deleteMessageForMe` 是 app-state patch。`Client.SendAppState` 需要本账号存在 app-state sync key：

```text
Store.AppStateKeys.GetLatestAppStateSyncKeyID(ctx)
```

如果号商 JSON 只有登录密钥、identity、prekey、signedPreKey，而没有 app-state key，则会失败：

```text
no app state keys found, creating app state keys is not yet supported
```

因此：

- 扫码登录账号通常可以拿到 app-state key
- Baileys multi-file auth 目录如果保存过 `app-state-sync-key-*`，可以导入
- 纯号商 JSON 通常不能直接做 delete-for-me
- 这种情况下只能做“业务系统本地隐藏”，不能同步到 WhatsApp 当前账号所有设备

### 6.2 当前实现的 helper

当前已经在 `appstate/encode.go` 增加 `BuildDeleteMessageForMe`，实现方式如下：

```go
func BuildDeleteMessageForMe(
	target types.JID,
	sender types.JID,
	messageID types.MessageID,
	fromMe bool,
	messageTimestamp time.Time,
	deleteMedia bool,
) PatchInfo {
	isFromMe := "0"
	if fromMe {
		isFromMe = "1"
	}

	senderJID := sender.String()
	if sender.IsEmpty() || target.User == sender.User {
		senderJID = "0"
	}

	return PatchInfo{
		Type: WAPatchRegularHigh,
		Mutations: []MutationInfo{{
			Index: []string{
				IndexDeleteMessageForMe,
				target.String(),
				messageID,
				isFromMe,
				senderJID,
			},
			Version: 3,
			Value: &waSyncAction.SyncActionValue{
				DeleteMessageForMeAction: &waSyncAction.DeleteMessageForMeAction{
					DeleteMedia:      proto.Bool(deleteMedia),
					MessageTimestamp: proto.Int64(messageTimestamp.Unix()),
				},
			},
		}},
	}
}
```

调用方式：

```go
err := client.SendAppState(ctx, appstate.BuildDeleteMessageForMe(
	chatJID,
	types.EmptyJID,
	messageID,
	true,
	messageTimestamp,
	false,
))
```

### 6.3 需要保存的发送结果

要删除当前账号侧消息，业务层必须保存发送结果：

| 字段 | 来源 | 用途 |
| --- | --- | --- |
| `chat_jid` | 发送目标 | app-state index |
| `message_id` | `SendMessage` 返回值 | app-state index |
| `from_me` | 自己发送为 `true` | app-state index |
| `sender_jid` | 群聊中对方消息才需要 | app-state index |
| `message_timestamp` | `SendMessage` 返回值 | `DeleteMessageForMeAction.messageTimestamp` |
| `delete_media` | 业务参数 | 是否同时删媒体 |

当前 demo 打印了：

```text
sent smoke-test message id=... timestamp=...
```

产品化时必须落库。

## 7. 建议的业务 API 封装

### 7.1 导入账号

```http
POST /accounts/import
```

请求：

```json
{
  "creds_path": "docs/keypair/21698290649.json",
  "proxy": "socks5://USER:PASS@HOST:PORT"
}
```

输出：

```json
{
  "account_jid": "21698290649:2@s.whatsapp.net",
  "lid": "174135623835858:2@lid",
  "platform": "android"
}
```

### 7.2 连接账号

```http
POST /accounts/{account_id}/connect
```

需要支持：

- 代理
- 登录超时
- 重连策略
- 账号状态事件

### 7.3 添加联系人

```http
POST /accounts/{account_id}/contacts:add
```

请求：

```json
{
  "phones": ["27651432974"],
  "method": "usync",
  "batch_size": 20,
  "pause_ms": 2000
}
```

输出需要保留 `in/out`：

```json
{
  "in": ["27651432974@s.whatsapp.net"],
  "out": []
}
```

### 7.4 发送消息

```http
POST /accounts/{account_id}/messages:send
```

请求：

```json
{
  "to": "27651432974",
  "kind": "interactive-url",
  "text": "hello world",
  "media_path": "docs/image/2026-06-01_180749_007.jpg",
  "preview": {
    "title": "Link",
    "body": "hello world",
    "url": "https://example.com/",
    "button": "click me auto"
  }
}
```

输出：

```json
{
  "chat_jid": "27651432974@s.whatsapp.net",
  "message_id": "3EB047AF5DCFFBD7986A5E",
  "timestamp": "2026-06-09T11:57:58+08:00",
  "from_me": true
}
```

### 7.5 删除当前账号消息

```http
POST /accounts/{account_id}/messages:delete-for-me
```

请求：

```json
{
  "chat_jid": "27651432974@s.whatsapp.net",
  "message_id": "3EB047AF5DCFFBD7986A5E",
  "message_timestamp": 1780977478,
  "from_me": true,
  "delete_media": false
}
```

当前 demo 行为：

- 如果账号有 app-state key：发送 `deleteMessageForMe` patch
- 如果没有 app-state key：返回明确错误，不要静默成功
- 如果只做本地隐藏：接口名必须区分，例如 `messages:hide-local`
- 删除刚发送的主消息：使用 `-delete-for-me`
- 删除已存在消息：使用 `-delete-for-me-message-id`、`-delete-for-me-message-timestamp` 和 `-send-to`
- 删除当前账号侧聊天框：使用 `-delete-chat-for-me`
- 接收方聊天框不能由发送方账号远程删除；如果确实要删接收方本地聊天框，必须登录接收方账号并让接收方账号自己执行 `deleteChat`

## 8. 风控和稳定性建议

- 不建议新号直接对陌生号码发消息。
- 发送前先 USync 添加联系人，并等待服务端返回 `type="in"`。
- 批量添加和批量发送要做限速。
- 代理必须能访问：
  - `web.whatsapp.com`
  - `*.whatsapp.net`
  - WhatsApp media CDN host
- 媒体上传要设置较长 `send-timeout`。
- `external-ad` 不作为主链路，它可能服务端 ack 但客户端不展示。
- `interactive-url` 是当前更接近目标卡片效果的主方案。

## 9. 已实现和下一步

已实现：

- `appstate.BuildDeleteMessageForMe`
- demo 参数：`-delete-for-me`、`-delete-for-me-message-id`、`-delete-for-me-message-timestamp`、`-delete-for-me-from-me`、`-delete-media-for-me`
- demo 参数：`-delete-chat-for-me`、`-delete-chat-media`
- PostgreSQL 持久化 store，用于保存 app-state sync key 和 app-state version/hash
- `-recover-app-state-keys` 从已关联设备请求 app-state key share

下一步：

1. 业务层保存每条发送消息的 `chat_jid/message_id/timestamp/from_me`。
2. 批量添加和批量发送增加速率控制、失败重试、账号冷却。
3. 对 `interactive-url` 和 `buttons-url` 做接收端展示矩阵测试。
