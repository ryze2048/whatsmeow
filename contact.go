// Copyright (c) 2026 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package whatsmeow

import (
	"context"

	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/types"
)

// ContactInfo contains the contact fields used when adding a WhatsApp contact.
type ContactInfo = appstate.ContactInfo

// AddContact adds or updates a single contact in the current user's WhatsApp contact list.
func (cli *Client) AddContact(ctx context.Context, target types.JID, contact ContactInfo) error {
	return cli.SendAppState(ctx, appstate.BuildContactAdd(target, contact))
}

// AddContacts adds or updates multiple contacts in the current user's WhatsApp contact list.
func (cli *Client) AddContacts(ctx context.Context, contacts map[types.JID]ContactInfo) error {
	if len(contacts) == 0 {
		return nil
	}
	return cli.SendAppState(ctx, appstate.BuildContactAdds(contacts))
}

// RemoveContact removes a single contact from the current user's WhatsApp contact list.
func (cli *Client) RemoveContact(ctx context.Context, target types.JID) error {
	return cli.SendAppState(ctx, appstate.BuildContactRemove(target))
}

// RemoveContacts removes multiple contacts from the current user's WhatsApp contact list.
func (cli *Client) RemoveContacts(ctx context.Context, targets []types.JID) error {
	if len(targets) == 0 {
		return nil
	}
	return cli.SendAppState(ctx, appstate.BuildContactRemoves(targets))
}
