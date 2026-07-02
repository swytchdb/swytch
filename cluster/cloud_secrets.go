/*
 * Copyright 2026 Swytch Labs BV
 *
 * This file is part of Swytch.
 *
 * Swytch is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Swytch is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Swytch. If not, see <https://www.gnu.org/licenses/>.
 */

package cluster

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"

	"golang.org/x/crypto/hkdf"
)

// cloudSaltV1 is the HKDF salt shared by every cloud-secret derivation. Per-role
// info strings (below) provide the domain separation; bump the version here to
// rotate the whole tree at once.
const cloudSaltV1 = "swytch-cloud-v1"

// deriveCloud expands ikm into a fresh 32-byte key for the given role using
// HKDF-SHA256. Reading 32 bytes from an SHA-256 HKDF never fails, so an error
// here means a broken runtime, not a recoverable condition.
func deriveCloud(ikm []byte, info string) []byte {
	r := hkdf.New(sha256.New, ikm, []byte(cloudSaltV1), []byte(info))
	out := make([]byte, 32)
	if _, err := io.ReadFull(r, out); err != nil {
		panic(fmt.Sprintf("cloud secret derivation failed: %v", err))
	}
	return out
}

// GenerateConnectionSecret returns a fresh master secret: 32 random bytes encoded
// as base64 RawURL (43 chars, no padding). It is the single secret a customer
// keeps; every other cloud value derives from it and it never leaves the client.
func GenerateConnectionSecret() (string, error) {
	return GeneratePassphrase()
}

// DeriveCloudSecret derives the cloud secret from the connection secret. It is a
// one-way derivation: the customer pastes it into onboarding and the cloud stores
// only a hash of what it yields, so the cloud can authenticate the customer
// without ever being able to recover the connection secret.
func DeriveCloudSecret(connectionSecret string) string {
	return base64.RawURLEncoding.EncodeToString(deriveCloud([]byte(connectionSecret), "cloud-secret"))
}

// DeriveAuthKey derives the wire authentication key from the cloud secret. This
// is the value presented over the network; the cloud verifies it against the
// hash it stored at onboarding.
func DeriveAuthKey(cloudSecret string) string {
	return base64.RawURLEncoding.EncodeToString(DeriveAuthKeyBytes(cloudSecret))
}

// DeriveAuthKeyBytes is DeriveAuthKey before its base64 encoding: the raw
// 32-byte value the dataplane stream handshake carries and the cloud's
// storage.AuthKey derives on its side.
func DeriveAuthKeyBytes(cloudSecret string) []byte {
	return deriveCloud([]byte(cloudSecret), "auth")
}

// CloudFolder derives a cluster's opaque cloud folder name from its auth key,
// mirroring the cloud's storage.Prefix byte-for-byte — the path segment under
// which the CDN serves this cluster's effect blobs.
func CloudFolder(authKey []byte) string {
	h := sha256.New()
	h.Write([]byte("swytch-cloud-folder-v1"))
	h.Write(authKey)
	return hex.EncodeToString(h.Sum(nil))
}

// CloudKeyName maps a key name to the opaque byte string it is known by in the
// cloud. It is a keyed PRF (HMAC-SHA256 under a connection-secret-derived
// subkey), not encryption: the cloud only ever groups tips by key equality, and
// a node querying GetTips always knows the plaintext key it is asking about, so
// nothing ever needs to reverse the mapping — the real key name travels inside
// the HPKE-sealed payload. Deterministic by construction, which is load-bearing:
// every write of one logical key from every node must land in the same cloud
// tip directory or superseding deps and GetTips both break.
func CloudKeyName(keyNameKey, key []byte) []byte {
	mac := hmac.New(sha256.New, keyNameKey)
	mac.Write(key)
	return mac.Sum(nil)
}

// DeriveKeyNameKey derives the PRF subkey for CloudKeyName from the connection
// secret. Like the encryption key, it lives on a master-only HKDF branch the
// cloud can never reach.
func DeriveKeyNameKey(connectionSecret string) []byte {
	return deriveCloud([]byte(connectionSecret), "key-name")
}

// DeriveEncryptionKey derives the data encryption key from the connection secret.
// It lives on a different HKDF branch than the cloud secret, so the cloud cannot
// reach it: the key never crosses the network and losing the connection secret
// makes the data unrecoverable.
func DeriveEncryptionKey(connectionSecret string) []byte {
	return deriveCloud([]byte(connectionSecret), "encryption")
}

// DeriveClusterPassphrase derives the cluster mTLS passphrase from the connection
// secret. Its output is a valid input to DeriveCAFromPassphrase, so a cloud
// cluster's CA flows from the same master secret as everything else.
func DeriveClusterPassphrase(connectionSecret string) string {
	return base64.RawURLEncoding.EncodeToString(deriveCloud([]byte(connectionSecret), "cluster"))
}
