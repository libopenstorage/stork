// A REST client to interact with LINSTOR's REST API
// Copyright (C) LINBIT HA-Solutions GmbH
// All Rights Reserved.
// Author: Roland Kammerer <roland.kammerer@linbit.com>
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package client

import "context"

// custom code

// EncryptionProvider acts as an abstraction for an EncryptionService. It can be
// swapped out for another EncryptionService implementation, for example for
// testing.
type EncryptionProvider interface {
	// Create creates an encryption with the given passphrase
	Create(ctx context.Context, passphrase Passphrase) error
	// Modify modifies an existing passphrase
	Modify(ctx context.Context, passphrase Passphrase) error
	// Enter is used to enter a password so that content can be decrypted
	Enter(ctx context.Context, password string) error
}

// EncryptionService is the service that deals with encyrption related tasks.
type EncryptionService struct {
	client *Client
}

// Passphrase represents a LINSTOR passphrase
type Passphrase struct {
	NewPassphrase string `json:"new_passphrase,omitempty"`
	OldPassphrase string `json:"old_passphrase,omitempty"`
}

// Create creates an encryption with the given passphrase
func (n *EncryptionService) Create(ctx context.Context, passphrase Passphrase) error {
	_, err := n.client.doPOST(ctx, "/v1/encryption/passphrase", passphrase)
	return err
}

// Modify modifies an existing passphrase
func (n *EncryptionService) Modify(ctx context.Context, passphrase Passphrase) error {
	_, err := n.client.doPUT(ctx, "/v1/encryption/passphrase", passphrase)
	return err
}

// Enter is used to enter a password so that content can be decrypted
func (n *EncryptionService) Enter(ctx context.Context, password string) error {
	_, err := n.client.doPATCH(ctx, "/v1/encryption/passphrase", password)
	return err
}
