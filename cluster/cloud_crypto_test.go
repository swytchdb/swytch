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
	"bytes"
	"testing"
)

func newTestCrypto(t *testing.T) *cloudCrypto {
	t.Helper()
	conn, err := GenerateConnectionSecret()
	if err != nil {
		t.Fatalf("generate connection secret: %v", err)
	}
	cc, err := newCloudCrypto(DeriveEncryptionKey(conn))
	if err != nil {
		t.Fatalf("new cloud crypto: %v", err)
	}
	return cc
}

func TestCloudCryptoPayloadRoundTrip(t *testing.T) {
	cc := newTestCrypto(t)
	plaintext := []byte("the quick brown effect jumps over the lazy dag")

	sealed, err := cc.sealPayload(plaintext)
	if err != nil {
		t.Fatalf("seal payload: %v", err)
	}
	if bytes.Equal(sealed, plaintext) {
		t.Fatal("payload not encrypted")
	}
	opened, err := cc.openPayload(sealed)
	if err != nil {
		t.Fatalf("open payload: %v", err)
	}
	if !bytes.Equal(opened, plaintext) {
		t.Fatalf("payload round-trip mismatch: got %q want %q", opened, plaintext)
	}
}

func TestCloudCryptoPayloadNonceIsRandom(t *testing.T) {
	cc := newTestCrypto(t)
	plaintext := []byte("same plaintext, different ciphertext")
	a, err := cc.sealPayload(plaintext)
	if err != nil {
		t.Fatalf("seal a: %v", err)
	}
	b, err := cc.sealPayload(plaintext)
	if err != nil {
		t.Fatalf("seal b: %v", err)
	}
	if bytes.Equal(a, b) {
		t.Fatal("payload seal is deterministic; expected a random nonce")
	}
}

// TestCloudCryptoKeyNameDeterministic is the load-bearing property: every writer
// of a key must produce identical `key` bytes so Cloud aggregates the key's tips
// into one folder.
func TestCloudCryptoKeyNameDeterministic(t *testing.T) {
	conn, err := GenerateConnectionSecret()
	if err != nil {
		t.Fatalf("generate connection secret: %v", err)
	}
	key := DeriveEncryptionKey(conn)

	// Two independently-constructed crypto contexts (two nodes sharing the token).
	n1, err := newCloudCrypto(key)
	if err != nil {
		t.Fatalf("crypto n1: %v", err)
	}
	n2, err := newCloudCrypto(key)
	if err != nil {
		t.Fatalf("crypto n2: %v", err)
	}

	name := []byte("__swytch:members")
	a := n1.sealKeyName(name)
	b := n2.sealKeyName(name)
	if !bytes.Equal(a, b) {
		t.Fatal("key-name seal is not deterministic across nodes")
	}

	// A different key name yields different ciphertext (no nonce collision).
	c := n1.sealKeyName([]byte("__swytch:other"))
	if bytes.Equal(a, c) {
		t.Fatal("distinct key names produced identical ciphertext")
	}

	opened, err := n2.openKeyName(a)
	if err != nil {
		t.Fatalf("open key name: %v", err)
	}
	if !bytes.Equal(opened, name) {
		t.Fatalf("key-name round-trip mismatch: got %q want %q", opened, name)
	}
}

// TestCloudCryptoKeyIsolation confirms a different connection secret cannot open
// another's ciphertext.
func TestCloudCryptoKeyIsolation(t *testing.T) {
	cc := newTestCrypto(t)
	other := newTestCrypto(t)

	sealed, err := cc.sealPayload([]byte("secret"))
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	if _, err := other.openPayload(sealed); err == nil {
		t.Fatal("payload opened under the wrong key")
	}
}
