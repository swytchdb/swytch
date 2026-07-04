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
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"io"

	"github.com/klauspost/compress/zstd"
	"golang.org/x/crypto/chacha20poly1305"
	"golang.org/x/crypto/hkdf"
)

// Shared zstd encoder/decoder. klauspost/compress's EncodeAll and DecodeAll are
// safe for concurrent use when the codec isn't also streaming, so a single
// instance of each serves the whole process — both wire framing and sealing.
var (
	wireZEnc *zstd.Encoder
	wireZDec *zstd.Decoder
)

func init() {
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		panic(fmt.Sprintf("zstd encoder init: %v", err))
	}
	dec, err := zstd.NewReader(nil, zstd.WithDecoderMaxMemory(128<<20))
	if err != nil {
		panic(fmt.Sprintf("zstd decoder init: %v", err))
	}
	wireZEnc = enc
	wireZDec = dec
}

// Compress returns the zstd-compressed form of b. Safe for concurrent use.
func Compress(b []byte) []byte {
	return wireZEnc.EncodeAll(b, nil)
}

// Decompress reverses Compress. Decoded size is bounded (see decoder init) to
// guard against decompression bombs from a misbehaving peer.
func Decompress(b []byte) ([]byte, error) {
	out, err := wireZDec.DecodeAll(b, nil)
	if err != nil {
		return nil, fmt.Errorf("zstd decompress: %w", err)
	}
	return out, nil
}

// cipherKeyInfo is the HKDF label separating the AEAD key from the raw IKM
// leaf of the secret-derivation tree. It names the cipher so a future cipher
// change derives a fresh key under a new label instead of reusing this one.
const cipherKeyInfo = "swytch effects seal xchacha20poly1305 v1"

// Encryptor seals payloads with XChaCha20-Poly1305 under a key derived from
// shared input keying material, with zstd compression inside the seal.
//
// Encryption is symmetric on purpose: every sealing party already holds the
// connection secret the key derives from, so asymmetric sealing would add
// per-blob key-encapsulation overhead without separating any capabilities.
// Read-only consumers are carved out by the secret-derivation tree instead —
// hand them the encryption key and the key-name key but not the master secret,
// and they can decrypt blobs yet never derive the auth key a write requires.
// Sealed blobs are stored durably; changing this format strands them.
type Encryptor struct {
	key []byte // 32-byte XChaCha20-Poly1305 key
}

// NewEncryptorFromIKM derives an Encryptor's key deterministically from input
// keying material. Every holder of the same IKM arrives at the same key
// independently — this is how a cluster shares one cloud-payload key derived
// from the connection secret, with no key exchange and nothing for the cloud
// to see.
func NewEncryptorFromIKM(ikm []byte) (*Encryptor, error) {
	key := make([]byte, chacha20poly1305.KeySize)
	if _, err := io.ReadFull(hkdf.New(sha256.New, ikm, nil, []byte(cipherKeyInfo)), key); err != nil {
		return nil, fmt.Errorf("derive seal key: %w", err)
	}
	return &Encryptor{key: key}, nil
}

// SealAndCompress compresses with zstd, then seals with XChaCha20-Poly1305
// under a random nonce, returning nonce ‖ ciphertext. The info parameter is
// bound as additional data for domain separation (e.g. "effect" vs
// "tip-recovery"): a blob sealed under one domain does not open under another.
func (enc *Encryptor) SealAndCompress(plaintext, info []byte) ([]byte, error) {
	aead, err := chacha20poly1305.NewX(enc.key)
	if err != nil {
		return nil, fmt.Errorf("seal init: %w", err)
	}
	compressed := Compress(plaintext)
	out := make([]byte, chacha20poly1305.NonceSizeX, chacha20poly1305.NonceSizeX+len(compressed)+aead.Overhead())
	if _, err := rand.Read(out); err != nil {
		return nil, fmt.Errorf("seal nonce: %w", err)
	}
	return aead.Seal(out, out, compressed, info), nil
}

// OpenAndDecompress reverses SealAndCompress: authenticates and decrypts under
// the same info domain, then decompresses.
func (enc *Encryptor) OpenAndDecompress(sealed, info []byte) ([]byte, error) {
	aead, err := chacha20poly1305.NewX(enc.key)
	if err != nil {
		return nil, fmt.Errorf("open init: %w", err)
	}
	if len(sealed) < chacha20poly1305.NonceSizeX {
		return nil, fmt.Errorf("open: sealed blob shorter than a nonce")
	}
	compressed, err := aead.Open(nil, sealed[:chacha20poly1305.NonceSizeX], sealed[chacha20poly1305.NonceSizeX:], info)
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}
	return Decompress(compressed)
}
