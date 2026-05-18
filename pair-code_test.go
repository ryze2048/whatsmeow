// Copyright (c) 2026 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package whatsmeow

import (
	"bytes"
	"errors"
	"testing"
)

func TestNormalizeLinkingCode(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    string
		wantErr bool
	}{
		{
			name:  "plain",
			input: "ABCD1234",
			want:  "ABCD1234",
		},
		{
			name:  "hyphen and lowercase",
			input: "abcd-1234",
			want:  "ABCD1234",
		},
		{
			name:  "spaces",
			input: "ABCD 1234",
			want:  "ABCD1234",
		},
		{
			name:    "too short",
			input:   "ABC123",
			wantErr: true,
		},
		{
			name:    "zero is not allowed",
			input:   "ABCD0123",
			wantErr: true,
		},
		{
			name:    "ambiguous letter is not allowed",
			input:   "ABCDI123",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := normalizeLinkingCode(tt.input)
			if tt.wantErr {
				if !errors.Is(err, ErrInvalidPairCode) {
					t.Fatalf("normalizeLinkingCode() error = %v, want ErrInvalidPairCode", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("normalizeLinkingCode() unexpected error = %v", err)
			}
			if got != tt.want {
				t.Fatalf("normalizeLinkingCode() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestGenerateCompanionEphemeralKeyWithCode(t *testing.T) {
	keyPair, ephemeralKey, code, err := generateCompanionEphemeralKeyWithCode("abcd-1234")
	if err != nil {
		t.Fatalf("generateCompanionEphemeralKeyWithCode() error = %v", err)
	}
	if code != "ABCD1234" {
		t.Fatalf("code = %q, want ABCD1234", code)
	}
	if len(ephemeralKey) != 80 {
		t.Fatalf("ephemeral key length = %d, want 80", len(ephemeralKey))
	}
	if bytes.Equal(keyPair.Pub[:], ephemeralKey[48:80]) {
		t.Fatal("encrypted public key matches keyPair.Pub, public key was likely encrypted in place")
	}
}

func TestGenerateCompanionEphemeralKeyRandomCode(t *testing.T) {
	_, ephemeralKey, code, err := generateCompanionEphemeralKeyWithCode("")
	if err != nil {
		t.Fatalf("generateCompanionEphemeralKeyWithCode() error = %v", err)
	}
	if len(code) != linkingCodeLength {
		t.Fatalf("code length = %d, want %d", len(code), linkingCodeLength)
	}
	if len(ephemeralKey) != 80 {
		t.Fatalf("ephemeral key length = %d, want 80", len(ephemeralKey))
	}
	if _, err = normalizeLinkingCode(code); err != nil {
		t.Fatalf("generated code failed validation: %v", err)
	}
}
