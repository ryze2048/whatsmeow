// Copyright (c) 2026 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package whatsmeow

import (
	"context"
	"encoding/json"

	"google.golang.org/protobuf/proto"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
)

// NativeFlowButton is a button inside an interactive native-flow message.
type NativeFlowButton = waE2E.InteractiveMessage_NativeFlowMessage_NativeFlowButton

type nativeFlowButtonParams struct {
	ID          string `json:"id,omitempty"`
	DisplayText string `json:"display_text,omitempty"`
	URL         string `json:"url,omitempty"`
	PhoneNumber string `json:"phone_number,omitempty"`
	CopyCode    string `json:"copy_code,omitempty"`
	Disabled    bool   `json:"disabled"`
}

// BuildNativeFlowButton builds a native-flow button with raw JSON parameters.
func BuildNativeFlowButton(name, paramsJSON string) *NativeFlowButton {
	return &waE2E.InteractiveMessage_NativeFlowMessage_NativeFlowButton{
		Name:             proto.String(name),
		ButtonParamsJSON: proto.String(paramsJSON),
	}
}

func buildNativeFlowButtonParams(name string, params nativeFlowButtonParams) *NativeFlowButton {
	paramsJSON, _ := json.Marshal(params)
	return BuildNativeFlowButton(name, string(paramsJSON))
}

// BuildQuickReplyButton builds a quick-reply native-flow button.
func BuildQuickReplyButton(id, displayText string) *NativeFlowButton {
	return buildNativeFlowButtonParams("quick_reply", nativeFlowButtonParams{
		ID:          id,
		DisplayText: displayText,
	})
}

// BuildURLButton builds a URL native-flow button.
func BuildURLButton(id, displayText, url string) *NativeFlowButton {
	return buildNativeFlowButtonParams("cta_url", nativeFlowButtonParams{
		ID:          id,
		DisplayText: displayText,
		URL:         url,
	})
}

// BuildCallButton builds a call native-flow button.
func BuildCallButton(id, displayText, phoneNumber string) *NativeFlowButton {
	return buildNativeFlowButtonParams("cta_call", nativeFlowButtonParams{
		ID:          id,
		DisplayText: displayText,
		PhoneNumber: phoneNumber,
	})
}

// BuildCopyButton builds a copy-code native-flow button.
func BuildCopyButton(id, displayText, copyCode string) *NativeFlowButton {
	return buildNativeFlowButtonParams("cta_copy", nativeFlowButtonParams{
		ID:          id,
		DisplayText: displayText,
		CopyCode:    copyCode,
	})
}

// BuildInteractiveButtonsMessage builds an interactive native-flow buttons message.
func BuildInteractiveButtonsMessage(title, body, footer string, buttons ...*NativeFlowButton) *waE2E.Message {
	interactive := &waE2E.InteractiveMessage{
		InteractiveMessage: &waE2E.InteractiveMessage_NativeFlowMessage_{
			NativeFlowMessage: &waE2E.InteractiveMessage_NativeFlowMessage{
				Buttons: buttons,
			},
		},
	}
	if title != "" {
		interactive.Header = &waE2E.InteractiveMessage_Header{
			Title: proto.String(title),
		}
	}
	if body != "" {
		interactive.Body = &waE2E.InteractiveMessage_Body{
			Text: proto.String(body),
		}
	}
	if footer != "" {
		interactive.Footer = &waE2E.InteractiveMessage_Footer{
			Text: proto.String(footer),
		}
	}
	return &waE2E.Message{InteractiveMessage: interactive}
}

// BuildButtonTemplateMessage is an alias for BuildInteractiveButtonsMessage.
func BuildButtonTemplateMessage(title, body, footer string, buttons ...*NativeFlowButton) *waE2E.Message {
	return BuildInteractiveButtonsMessage(title, body, footer, buttons...)
}

// SendInteractiveButtons sends an interactive native-flow buttons message.
func (cli *Client) SendInteractiveButtons(ctx context.Context, to types.JID, title, body, footer string, buttons []*NativeFlowButton, extra ...SendRequestExtra) (SendResponse, error) {
	return cli.SendMessage(ctx, to, BuildInteractiveButtonsMessage(title, body, footer, buttons...), extra...)
}

// SendButtonTemplate sends an interactive native-flow button template message.
func (cli *Client) SendButtonTemplate(ctx context.Context, to types.JID, title, body, footer string, buttons []*NativeFlowButton, extra ...SendRequestExtra) (SendResponse, error) {
	return cli.SendInteractiveButtons(ctx, to, title, body, footer, buttons, extra...)
}
