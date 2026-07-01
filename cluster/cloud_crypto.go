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
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"io"
)

// cloudCrypto is the customer encryption layer for effects shipped to Cloud. It
// wraps two independent AES-256-GCM contexts derived from the connection
// secret's encryption_key, so Cloud (which never has that key) stores opaque
// ciphertext it cannot read.
//
// The two contexts differ in how they pick a nonce, and that difference is
// load-bearing:
//
//   - Key names are sealed DETERMINISTICALLY (nonce = HMAC of the name). The
//     same logical key always yields the same ciphertext, so every writer of a
//     key produces the identical `key` bytes — which is exactly what lets Cloud
//     aggregate all of a key's effects into one tips folder. A random nonce here
//     would scatter one logical key across a new folder per write and the tip
//     frontier could never be discovered.
//   - Payloads are sealed with a RANDOM nonce. Each effect is a distinct blob
//     addressed by (nodeID, offset), so there is nothing to aggregate; a random
//     nonce is the standard, strongest choice.
//
// The two contexts use separately-derived AES keys so a deterministic key-name
// nonce can never collide with a random payload nonce under a shared key.
type cloudCrypto struct {
	payloadGCM   cipher.AEAD
	keyNameGCM   cipher.AEAD
	keyNameNonce []byte // HMAC key for the deterministic key-name nonce
}

// newCloudCrypto builds the customer encryption layer from the 32-byte
// encryption_key (cluster.DeriveEncryptionKey). Every subkey is an HKDF
// expansion of that key with a role-specific info string, matching the rest of
// the cloud secret tree.
func newCloudCrypto(encryptionKey []byte) (*cloudCrypto, error) {
	payloadGCM, err := newGCM(deriveCloud(encryptionKey, "cloud-payload-key"))
	if err != nil {
		return nil, fmt.Errorf("payload cipher: %w", err)
	}
	keyNameGCM, err := newGCM(deriveCloud(encryptionKey, "cloud-keyname-key"))
	if err != nil {
		return nil, fmt.Errorf("key-name cipher: %w", err)
	}
	return &cloudCrypto{
		payloadGCM:   payloadGCM,
		keyNameGCM:   keyNameGCM,
		keyNameNonce: deriveCloud(encryptionKey, "cloud-keyname-nonce"),
	}, nil
}

// newGCM builds an AES-256-GCM AEAD from a 32-byte key.
func newGCM(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

// sealPayload encrypts an effect payload with a fresh random nonce, returning
// nonce||ciphertext||tag.
func (c *cloudCrypto) sealPayload(plaintext []byte) ([]byte, error) {
	nonce := make([]byte, c.payloadGCM.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("read nonce: %w", err)
	}
	return c.payloadGCM.Seal(nonce, nonce, plaintext, nil), nil
}

// openPayload reverses sealPayload.
func (c *cloudCrypto) openPayload(sealed []byte) ([]byte, error) {
	return open(c.payloadGCM, sealed)
}

// sealKeyName encrypts a key name deterministically: the nonce is derived from
// the name via HMAC, so the same name always produces byte-identical output.
// That determinism is what makes a logical key resolve to a single Cloud tips
// folder across every node and every write.
func (c *cloudCrypto) sealKeyName(name []byte) []byte {
	mac := hmac.New(sha256.New, c.keyNameNonce)
	mac.Write(name)
	nonce := mac.Sum(nil)[:c.keyNameGCM.NonceSize()]
	return c.keyNameGCM.Seal(append([]byte(nil), nonce...), nonce, name, nil)
}

// openKeyName reverses sealKeyName.
func (c *cloudCrypto) openKeyName(sealed []byte) ([]byte, error) {
	return open(c.keyNameGCM, sealed)
}

// open splits nonce||ciphertext and authenticates-and-decrypts it.
func open(aead cipher.AEAD, sealed []byte) ([]byte, error) {
	ns := aead.NonceSize()
	if len(sealed) < ns {
		return nil, fmt.Errorf("ciphertext too short: %d < nonce %d", len(sealed), ns)
	}
	return aead.Open(nil, sealed[:ns], sealed[ns:], nil)
}
