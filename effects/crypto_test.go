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

package effects

import (
	"bytes"
	"testing"
)

func testEncryptor(t *testing.T) *Encryptor {
	t.Helper()
	enc, err := NewEncryptorFromIKM(bytes.Repeat([]byte{0x42}, 32))
	if err != nil {
		t.Fatal(err)
	}
	return enc
}

func TestEncryptor_RoundTrip(t *testing.T) {
	enc := testEncryptor(t)

	plaintext := []byte("hello, world!")
	info := []byte("test-domain")

	sealed, err := enc.SealAndCompress(plaintext, info)
	if err != nil {
		t.Fatal(err)
	}

	if bytes.Contains(sealed, plaintext) {
		t.Fatal("sealed blob contains the plaintext")
	}

	recovered, err := enc.OpenAndDecompress(sealed, info)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(recovered, plaintext) {
		t.Fatalf("round-trip failed: got %q, want %q", recovered, plaintext)
	}
}

func TestNewEncryptorFromIKM_Deterministic(t *testing.T) {
	ikm := bytes.Repeat([]byte{0x42}, 32)

	a, err := NewEncryptorFromIKM(ikm)
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewEncryptorFromIKM(ikm)
	if err != nil {
		t.Fatal(err)
	}

	// Two independent derivations from the same IKM must interoperate: one
	// node seals, another node (deriving on its own) opens.
	plaintext := []byte("cluster-shared cloud payload")
	sealed, err := a.SealAndCompress(plaintext, []byte("effect"))
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := b.OpenAndDecompress(sealed, []byte("effect"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(recovered, plaintext) {
		t.Fatalf("cross-derivation round-trip failed: got %q, want %q", recovered, plaintext)
	}

	// A different IKM must not open the blob.
	other, err := NewEncryptorFromIKM(bytes.Repeat([]byte{0x43}, 32))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := other.OpenAndDecompress(sealed, []byte("effect")); err == nil {
		t.Fatal("blob sealed under one IKM opened under another")
	}
}

func TestEncryptor_DomainSeparation(t *testing.T) {
	enc := testEncryptor(t)

	sealed, err := enc.SealAndCompress([]byte("sensitive data"), []byte("domain-a"))
	if err != nil {
		t.Fatal(err)
	}

	if _, err := enc.OpenAndDecompress(sealed, []byte("domain-b")); err == nil {
		t.Fatal("blob sealed under one domain opened under another")
	}
}

func TestEncryptor_TamperDetection(t *testing.T) {
	enc := testEncryptor(t)

	sealed, err := enc.SealAndCompress([]byte("integrity matters"), []byte("effect"))
	if err != nil {
		t.Fatal(err)
	}

	for _, i := range []int{0, len(sealed) / 2, len(sealed) - 1} {
		tampered := bytes.Clone(sealed)
		tampered[i] ^= 0x01
		if _, err := enc.OpenAndDecompress(tampered, []byte("effect")); err == nil {
			t.Fatalf("tampered blob (byte %d) opened successfully", i)
		}
	}

	if _, err := enc.OpenAndDecompress(sealed[:10], []byte("effect")); err == nil {
		t.Fatal("truncated blob opened successfully")
	}
}

// TestEncryptor_SealOverhead pins the per-blob byte cost. Effects are tiny and
// stored blob-per-effect, so constant sealing overhead multiplies directly
// into storage and egress: nonce (24) + Poly1305 tag (16) on top of the
// compressed payload, and nothing else.
func TestEncryptor_SealOverhead(t *testing.T) {
	enc := testEncryptor(t)

	plaintext := []byte("small effect payload")
	sealed, err := enc.SealAndCompress(plaintext, []byte("effect"))
	if err != nil {
		t.Fatal(err)
	}

	if got, limit := len(sealed), len(Compress(plaintext))+40; got > limit {
		t.Fatalf("sealed %d bytes, want ≤ %d (compressed + nonce + tag)", got, limit)
	}
}

func TestEncryptor_LargePayload(t *testing.T) {
	enc := testEncryptor(t)

	// 1MB payload — tests compression + encryption
	plaintext := make([]byte, 1024*1024)
	for i := range plaintext {
		plaintext[i] = byte(i % 256)
	}

	sealed, err := enc.SealAndCompress(plaintext, []byte("large"))
	if err != nil {
		t.Fatal(err)
	}

	recovered, err := enc.OpenAndDecompress(sealed, []byte("large"))
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(recovered, plaintext) {
		t.Fatal("large payload round-trip failed")
	}
}
