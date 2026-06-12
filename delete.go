// Copyright (c) 2026 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package whatsmeow

import (
	"context"
	"time"

	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/types"
)

// DeleteMessageForMe deletes the given message locally from the current user's chat history.
//
// The sender must be set for messages sent by other users in groups. For direct chats and your own
// messages, sender may be empty.
func (cli *Client) DeleteMessageForMe(ctx context.Context, chat, sender types.JID, id types.MessageID, fromMe bool, messageTimestamp time.Time, deleteMedia bool) error {
	return cli.SendAppState(ctx, appstate.BuildDeleteMessageForMe(chat, sender, id, fromMe, messageTimestamp, deleteMedia))
}

// DeleteMessageForEveryone revokes the given message for all participants in the chat.
//
// To delete your own message, pass an empty sender. To delete someone else's message as a group admin,
// pass the original sender JID.
func (cli *Client) DeleteMessageForEveryone(ctx context.Context, chat, sender types.JID, id types.MessageID) (SendResponse, error) {
	return cli.SendMessage(ctx, chat, cli.BuildRevoke(chat, sender, id))
}

// DeleteMessageForAll is an alias for DeleteMessageForEveryone.
func (cli *Client) DeleteMessageForAll(ctx context.Context, chat, sender types.JID, id types.MessageID) (SendResponse, error) {
	return cli.DeleteMessageForEveryone(ctx, chat, sender, id)
}

// DeleteChat deletes the chat from the current user's chat list.
//
// The last message timestamp and key are optional, but passing the latest known message improves
// consistency with other linked devices.
func (cli *Client) DeleteChat(ctx context.Context, chat types.JID, lastMessageTimestamp time.Time, lastMessageKey *waCommon.MessageKey, deleteMedia bool) error {
	return cli.SendAppState(ctx, appstate.BuildDeleteChat(chat, lastMessageTimestamp, lastMessageKey, deleteMedia))
}
