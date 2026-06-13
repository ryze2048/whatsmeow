// Copyright (c) 2026 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

// Package baileysauth imports Baileys authentication JSON into whatsmeow stores.
package baileysauth

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"google.golang.org/protobuf/proto"

	"go.mau.fi/whatsmeow/proto/waAdv"
	waE2E "go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/util/keys"
)

// ImportedDevice contains a WhatsApp device imported from Baileys credentials.
type ImportedDevice struct {
	Device          *store.Device
	MyAppStateKeyID []byte
	Warnings        []string

	NextPreKeyID            uint32
	FirstUnuploadedPreKeyID uint32

	ExistingDevice   bool
	StateReset       bool
	StateResetReason string
}

// ImportOptions controls how repeated imports of the same JID are handled.
type ImportOptions struct {
	// ResetExisting clears device-specific state before importing, even if the
	// existing device identity material appears to match.
	ResetExisting bool
	// ResetOnIdentityChange clears device-specific state when the stored device
	// has different identity material for the same JID. This is enabled by the
	// default ImportIntoContainer path.
	ResetOnIdentityChange bool
	// ClearAppStateSyncKeys also deletes stored app-state sync keys during reset.
	// By default sync keys are preserved because they can be expensive to recover.
	ClearAppStateSyncKeys bool
	// ClearContacts also deletes cached contacts and chat settings during reset.
	ClearContacts bool
}

func defaultImportOptions() ImportOptions {
	return ImportOptions{ResetOnIdentityChange: true}
}

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

type baileysIdentity struct {
	ID   string `json:"id"`
	LID  string `json:"lid"`
	Name string `json:"name"`
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

// ParseFile parses a Baileys creds JSON file.
//
// The returned device is not attached to a persistent store. Use ImportFileIntoContainer
// when you want a device that can be passed directly to whatsmeow.NewClient.
func ParseFile(path string) (*ImportedDevice, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return Parse(data)
}

// Parse parses Baileys creds JSON.
//
// The returned device contains the identity material, but is not attached to session stores.
// Use ImportIntoContainer for normal application usage.
func Parse(data []byte) (*ImportedDevice, error) {
	var creds baileysCreds
	if err := json.Unmarshal(data, &creds); err != nil {
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
		Platform: creds.Platform,
		PushName: creds.Me.Name,
	}

	warnings = append(warnings, noiseWarnings...)
	warnings = append(warnings, identityWarnings...)
	warnings = append(warnings, preKeyWarnings...)
	if creds.Phone == "" {
		warnings = append(warnings, "Phone is empty")
	}
	return &ImportedDevice{
		Device:                  device,
		MyAppStateKeyID:         myAppStateKeyID,
		Warnings:                warnings,
		NextPreKeyID:            creds.NextPreKeyID,
		FirstUnuploadedPreKeyID: creds.FirstUnuploadedPreKeyID,
	}, nil
}

// ImportFileIntoContainer parses a Baileys creds JSON file and stores the device in a whatsmeow container.
func ImportFileIntoContainer(ctx context.Context, container store.DeviceContainer, path string) (*ImportedDevice, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return ImportIntoContainer(ctx, container, data)
}

// ImportIntoContainer parses Baileys creds JSON and stores the device in a whatsmeow container.
func ImportIntoContainer(ctx context.Context, container store.DeviceContainer, data []byte) (*ImportedDevice, error) {
	return ImportIntoContainerWithOptions(ctx, container, data, defaultImportOptions())
}

// ImportIntoContainerWithOptions parses Baileys creds JSON and stores the device
// in a whatsmeow container with explicit repeated-import behavior.
func ImportIntoContainerWithOptions(ctx context.Context, container store.DeviceContainer, data []byte, opts ImportOptions) (*ImportedDevice, error) {
	imported, err := Parse(data)
	if err != nil {
		return nil, err
	}
	imported.Device.Deleted = false
	imported.Device.Container = container
	if err = prepareRepeatedImport(ctx, container, imported, opts); err != nil {
		return nil, err
	}
	if err = container.PutDevice(ctx, imported.Device); err != nil {
		return nil, fmt.Errorf("store imported device: %w", err)
	}
	if !imported.Device.GetJID().IsEmpty() && !imported.Device.GetLID().IsEmpty() && imported.Device.LIDs != nil {
		if err = imported.Device.LIDs.PutLIDMapping(ctx, imported.Device.GetLID().ToNonAD(), imported.Device.GetJID().ToNonAD()); err != nil {
			return nil, fmt.Errorf("store own LID mapping: %w", err)
		}
	}
	return imported, nil
}

func prepareRepeatedImport(ctx context.Context, container store.DeviceContainer, imported *ImportedDevice, opts ImportOptions) error {
	if imported == nil || imported.Device == nil || imported.Device.ID == nil {
		return nil
	}
	getter, canGet := container.(store.DeviceGetter)
	resetter, canReset := container.(store.DeviceStateResetter)
	if !canGet {
		return nil
	}
	existing, err := getter.GetDevice(ctx, imported.Device.GetJID())
	if err != nil {
		return fmt.Errorf("get existing imported device: %w", err)
	}
	if existing == nil {
		return nil
	}
	imported.ExistingDevice = true
	shouldReset := opts.ResetExisting
	resetReason := "forced"
	if !shouldReset && opts.ResetOnIdentityChange && deviceIdentityChanged(existing, imported.Device) {
		shouldReset = true
		resetReason = "identity_changed"
	}
	if !shouldReset {
		return nil
	}
	if !canReset {
		imported.Warnings = append(imported.Warnings, "existing device state should be reset, but container does not support state reset")
		return nil
	}
	if err = resetter.ResetDeviceState(ctx, imported.Device.GetJID(), store.DeviceStateResetOptions{
		PreserveAppStateSyncKeys: !opts.ClearAppStateSyncKeys,
		PreserveContacts:         !opts.ClearContacts,
	}); err != nil {
		return fmt.Errorf("reset existing device state: %w", err)
	}
	imported.StateReset = true
	imported.StateResetReason = resetReason
	return nil
}

func deviceIdentityChanged(existing, imported *store.Device) bool {
	if existing == nil || imported == nil {
		return false
	}
	if existing.RegistrationID != imported.RegistrationID {
		return true
	}
	if existing.NoiseKey == nil || imported.NoiseKey == nil || !bytes.Equal(existing.NoiseKey.Priv[:], imported.NoiseKey.Priv[:]) {
		return true
	}
	if existing.IdentityKey == nil || imported.IdentityKey == nil || !bytes.Equal(existing.IdentityKey.Priv[:], imported.IdentityKey.Priv[:]) {
		return true
	}
	return false
}

// AuthDirImportResult contains the result of importing a Baileys multi-file auth directory.
type AuthDirImportResult struct {
	AppStateKeyCount int
	LatestKeyID      []byte
	Warnings         []string
}

// ImportAuthDir imports app-state sync keys from a Baileys multi-file auth directory.
func ImportAuthDir(ctx context.Context, device *store.Device, dir string) (*AuthDirImportResult, error) {
	if device == nil || device.AppStateKeys == nil {
		return nil, errors.New("device must be initialized with stores before importing auth dir")
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var warnings []string
	latestKeyID, err := readBaileysAuthDirAppStateKeyID(dir)
	if err != nil {
		warnings = append(warnings, fmt.Sprintf("failed to read auth dir creds.json myAppStateKeyId: %v", err))
	}

	count := 0
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
	return &AuthDirImportResult{AppStateKeyCount: count, LatestKeyID: latestKeyID, Warnings: warnings}, nil
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
