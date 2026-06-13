// Copyright (c) 2026 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

// Package messagebuilder contains helpers for constructing common WhatsApp
// message payloads on top of whatsmeow's protobuf types.
package messagebuilder

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"image"
	_ "image/jpeg"
	_ "image/png"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"

	"go.mau.fi/whatsmeow"
	waBinary "go.mau.fi/whatsmeow/binary"
	waE2E "go.mau.fi/whatsmeow/proto/waE2E"
)

const (
	KindText           = "text"
	KindImage          = "image"
	KindExternalAd     = "external-ad"
	KindLinkPreview    = "link-preview"
	KindInteractiveURL = "interactive-url"
	KindNativeFlowURL  = "native-flow-url"
	KindTemplateURL    = "template-url"
	KindButtonsURL     = "buttons-url"

	DefaultText       = "whatsmeow message"
	DefaultButtonText = "Open"
	DefaultTitle      = "Link"
)

// Options describes a message payload to build.
type Options struct {
	Kind           string
	Text           string
	MediaPath      string
	PreviewTitle   string
	PreviewBody    string
	PreviewURL     string
	PreviewButton  string
	ExternalAction bool
}

// BuiltMessage is a protobuf message plus optional send metadata.
type BuiltMessage struct {
	Message         *waE2E.Message
	AdditionalNodes *[]waBinary.Node
}

// SendRequestExtra returns the whatsmeow send options needed for this message.
func (bm BuiltMessage) SendRequestExtra() whatsmeow.SendRequestExtra {
	return whatsmeow.SendRequestExtra{AdditionalNodes: bm.AdditionalNodes}
}

// Build creates a message and any required additional send nodes.
func Build(ctx context.Context, client *whatsmeow.Client, opts Options) (BuiltMessage, error) {
	msg, err := BuildMessage(ctx, client, opts)
	if err != nil {
		return BuiltMessage{}, err
	}
	nodes, err := AdditionalNodesForKind(opts.Kind)
	if err != nil {
		return BuiltMessage{}, err
	}
	return BuiltMessage{
		Message:         msg,
		AdditionalNodes: nodes,
	}, nil
}

// BuildMessage creates only the protobuf message. For native-flow messages,
// prefer Build so the required additional send nodes are included.
func BuildMessage(ctx context.Context, client *whatsmeow.Client, opts Options) (*waE2E.Message, error) {
	opts = normalizeOptions(opts)
	switch opts.Kind {
	case KindText:
		return Text(opts.Text), nil
	case KindImage:
		return Image(ctx, client, opts)
	case KindExternalAd:
		return ExternalAd(opts)
	case KindLinkPreview:
		return LinkPreview(ctx, client, opts)
	case KindInteractiveURL:
		return InteractiveURL(ctx, client, opts)
	case KindNativeFlowURL:
		return NativeFlowURL(ctx, client, opts)
	case KindTemplateURL:
		return TemplateURL(ctx, client, opts)
	case KindButtonsURL:
		return ButtonsURL(ctx, client, opts)
	default:
		return nil, fmt.Errorf("unknown message kind %q", opts.Kind)
	}
}

// AdditionalNodesForKind returns send-time nodes required by some message types.
func AdditionalNodesForKind(kind string) (*[]waBinary.Node, error) {
	if kind != KindNativeFlowURL {
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

// Text builds a plain text message.
func Text(text string) *waE2E.Message {
	if strings.TrimSpace(text) == "" {
		text = DefaultText
	}
	return &waE2E.Message{Conversation: proto.String(text)}
}

// Image uploads an image and builds an image message.
func Image(ctx context.Context, client *whatsmeow.Client, opts Options) (*waE2E.Message, error) {
	if opts.MediaPath == "" {
		return nil, errors.New("image messages require media path")
	}
	imageMessage, err := UploadedImage(ctx, client, opts.MediaPath, opts.Text)
	if err != nil {
		return nil, err
	}
	return &waE2E.Message{ImageMessage: imageMessage}, nil
}

// UploadedImage uploads an image and returns the protobuf image payload.
func UploadedImage(ctx context.Context, client *whatsmeow.Client, path, caption string) (*waE2E.ImageMessage, error) {
	if client == nil {
		return nil, whatsmeow.ErrClientIsNil
	}
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

// InteractiveURL builds the working native-flow URL card payload wrapped in a view-once message.
func InteractiveURL(ctx context.Context, client *whatsmeow.Client, opts Options) (*waE2E.Message, error) {
	_, params, err := URLButtonParams(opts.PreviewButton, opts.PreviewURL)
	if err != nil {
		return nil, err
	}
	header := &waE2E.InteractiveMessage_Header{
		HasMediaAttachment: proto.Bool(false),
	}
	if opts.MediaPath != "" {
		imageMessage, err := UploadedImage(ctx, client, opts.MediaPath, "")
		if err != nil {
			return nil, err
		}
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
			Message: &waE2E.Message{InteractiveMessage: interactive},
		},
	}, nil
}

// NativeFlowURL builds a native-flow URL message. Send with the BuiltMessage
// AdditionalNodes returned by Build.
func NativeFlowURL(ctx context.Context, client *whatsmeow.Client, opts Options) (*waE2E.Message, error) {
	_, params, err := URLButtonParams(opts.PreviewButton, opts.PreviewURL)
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
		imageMessage, err := UploadedImage(ctx, client, opts.MediaPath, "")
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

// TemplateURL builds a hydrated URL template message.
func TemplateURL(ctx context.Context, client *whatsmeow.Client, opts Options) (*waE2E.Message, error) {
	button, _, err := URLButtonParams(opts.PreviewButton, opts.PreviewURL)
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
		imageMessage, err := UploadedImage(ctx, client, opts.MediaPath, "")
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

// ButtonsURL builds a buttons message with a native-flow URL button.
func ButtonsURL(ctx context.Context, client *whatsmeow.Client, opts Options) (*waE2E.Message, error) {
	body := strings.TrimSpace(opts.PreviewBody)
	if body == "" {
		body = opts.Text
	}
	title := strings.TrimSpace(opts.PreviewTitle)
	if title == "" {
		title = DefaultTitle
	}
	button, params, err := URLButtonParams(opts.PreviewButton, opts.PreviewURL)
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
		imageMessage, err := UploadedImage(ctx, client, opts.MediaPath, "")
		if err != nil {
			return nil, err
		}
		headerType = waE2E.ButtonsMessage_IMAGE
		buttonsMessage.Header = &waE2E.ButtonsMessage_ImageMessage{ImageMessage: imageMessage}
		buttonsMessage.HeaderType = &headerType
	}
	return &waE2E.Message{ButtonsMessage: buttonsMessage}, nil
}

// ExternalAd builds an extended text message with an external-ad context preview.
func ExternalAd(opts Options) (*waE2E.Message, error) {
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
	contextInfo := &waE2E.ContextInfo{ExternalAdReply: externalAd}
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

// LinkPreview builds an extended text message with a high-quality link thumbnail.
func LinkPreview(ctx context.Context, client *whatsmeow.Client, opts Options) (*waE2E.Message, error) {
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

// URLButtonParams returns the display text and native-flow JSON for a URL button.
func URLButtonParams(buttonText, rawURL string) (string, string, error) {
	button := strings.TrimSpace(buttonText)
	if button == "" {
		button = DefaultButtonText
	}
	previewURL := strings.TrimSpace(rawURL)
	if previewURL == "" {
		return "", "", errors.New("URL messages require preview URL")
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

func normalizeOptions(opts Options) Options {
	if strings.TrimSpace(opts.Kind) == "" {
		opts.Kind = KindText
	}
	if strings.TrimSpace(opts.Text) == "" {
		opts.Text = DefaultText
	}
	if strings.TrimSpace(opts.PreviewTitle) == "" {
		opts.PreviewTitle = DefaultTitle
	}
	if strings.TrimSpace(opts.PreviewBody) == "" {
		opts.PreviewBody = opts.Text
	}
	if strings.TrimSpace(opts.PreviewButton) == "" {
		opts.PreviewButton = DefaultButtonText
	}
	return opts
}

func urlCardText(opts Options) string {
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
	if client == nil {
		return nil, whatsmeow.ErrClientIsNil
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

func secureRandomBytes(size int) ([]byte, error) {
	data := make([]byte, size)
	_, err := rand.Read(data)
	return data, err
}
