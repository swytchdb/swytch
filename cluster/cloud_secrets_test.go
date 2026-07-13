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
	"encoding/base64"
	"testing"
)

func TestCloudSecretsDeterministic(t *testing.T) {
	const conn = "test-connection-secret"
	cloud := DeriveCloudSecret(conn)

	if got := DeriveCloudSecret(conn); got != cloud {
		t.Fatalf("cloud secret not deterministic: %q != %q", got, cloud)
	}
	if got, want := DeriveAuthKey(cloud), DeriveAuthKey(cloud); got != want {
		t.Fatalf("auth key not deterministic: %q != %q", got, want)
	}
	if got, want := DeriveEncryptionKey(conn), DeriveEncryptionKey(conn); !bytes.Equal(got, want) {
		t.Fatalf("encryption key not deterministic")
	}
	if got, want := DeriveClusterPassphrase(conn), DeriveClusterPassphrase(conn); got != want {
		t.Fatalf("cluster passphrase not deterministic: %q != %q", got, want)
	}
}

func TestCloudSecretsDistinct(t *testing.T) {
	const conn = "test-connection-secret"
	cloud := DeriveCloudSecret(conn)

	values := map[string]string{
		"cloud":      cloud,
		"auth":       DeriveAuthKey(cloud),
		"encryption": base64.RawURLEncoding.EncodeToString(DeriveEncryptionKey(conn)),
		"cluster":    DeriveClusterPassphrase(conn),
	}
	seen := make(map[string]string, len(values))
	for name, v := range values {
		if other, ok := seen[v]; ok {
			t.Fatalf("derivations %q and %q collide on %q", name, other, v)
		}
		seen[v] = name
	}
}

func TestDeriveClusterPassphraseFeedsCA(t *testing.T) {
	pass := DeriveClusterPassphrase("test-connection-secret")

	key1, cert1, err := DeriveCAFromPassphrase(pass)
	if err != nil {
		t.Fatalf("DeriveCAFromPassphrase: %v", err)
	}
	key2, cert2, err := DeriveCAFromPassphrase(pass)
	if err != nil {
		t.Fatalf("DeriveCAFromPassphrase (second call): %v", err)
	}
	if !key1.Equal(key2) {
		t.Fatal("CA key not deterministic from derived passphrase")
	}
	if !bytes.Equal(cert1.Raw, cert2.Raw) {
		t.Fatal("CA cert not deterministic from derived passphrase")
	}
}

func TestCloudSecretsEncoding(t *testing.T) {
	conn, err := GenerateConnectionSecret()
	if err != nil {
		t.Fatalf("GenerateConnectionSecret: %v", err)
	}
	cloud := DeriveCloudSecret(conn)

	for _, s := range []string{conn, cloud, DeriveAuthKey(cloud), DeriveClusterPassphrase(conn)} {
		if len(s) != 43 {
			t.Fatalf("expected 43-char base64url, got %d: %q", len(s), s)
		}
		b, err := base64.RawURLEncoding.DecodeString(s)
		if err != nil {
			t.Fatalf("not valid base64url: %v", err)
		}
		if len(b) != 32 {
			t.Fatalf("expected 32 decoded bytes, got %d", len(b))
		}
	}
	if got := len(DeriveEncryptionKey(conn)); got != 32 {
		t.Fatalf("expected 32-byte encryption key, got %d", got)
	}
}

func TestDeriveAuthKeyBytesMatchesEncoded(t *testing.T) {
	conn, err := GenerateConnectionSecret()
	if err != nil {
		t.Fatalf("GenerateConnectionSecret: %v", err)
	}
	cloud := DeriveCloudSecret(conn)

	raw := DeriveAuthKeyBytes(cloud)
	if len(raw) != 32 {
		t.Fatalf("expected 32-byte auth key, got %d", len(raw))
	}
	if got, want := base64.RawURLEncoding.EncodeToString(raw), DeriveAuthKey(cloud); got != want {
		t.Fatalf("raw auth key does not encode to DeriveAuthKey: %q != %q", got, want)
	}
}

func TestCloudKeyName(t *testing.T) {
	conn, err := GenerateConnectionSecret()
	if err != nil {
		t.Fatalf("GenerateConnectionSecret: %v", err)
	}
	knk := DeriveKeyNameKey(conn)
	key := []byte("__swytch:members")

	// Deterministic: every node's mapping of one logical key must agree, or the
	// cloud's per-key tip grouping falls apart.
	a := CloudKeyName(knk, key)
	b := CloudKeyName(DeriveKeyNameKey(conn), key)
	if !bytes.Equal(a, b) {
		t.Fatal("CloudKeyName is not deterministic across derivations")
	}

	// Distinct keys must not collide, and the mapping must not leak the key.
	if bytes.Equal(a, CloudKeyName(knk, []byte("__swytch:member"))) {
		t.Fatal("distinct keys mapped to the same cloud name")
	}
	if bytes.Contains(a, key) {
		t.Fatal("cloud key name contains the plaintext key")
	}

	// A different connection secret yields an unrelated mapping.
	other, err := GenerateConnectionSecret()
	if err != nil {
		t.Fatalf("GenerateConnectionSecret: %v", err)
	}
	if bytes.Equal(a, CloudKeyName(DeriveKeyNameKey(other), key)) {
		t.Fatal("different secrets produced the same cloud key name")
	}
}

func TestDifferentConnectionsDifferentCloudSecrets(t *testing.T) {
	a, err := GenerateConnectionSecret()
	if err != nil {
		t.Fatalf("GenerateConnectionSecret: %v", err)
	}
	b, err := GenerateConnectionSecret()
	if err != nil {
		t.Fatalf("GenerateConnectionSecret: %v", err)
	}
	if a == b {
		t.Fatal("two random connection secrets collided")
	}
	if DeriveCloudSecret(a) == DeriveCloudSecret(b) {
		t.Fatal("different connection secrets produced the same cloud secret")
	}
}
