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
	"crypto/hpke"
	"fmt"

	"github.com/klauspost/compress/zstd"
)

// Shared zstd encoder/decoder. klauspost/compress's EncodeAll and DecodeAll are
// safe for concurrent use when the codec isn't also streaming, so a single
// instance of each serves the whole process — both wire framing and HPKE sealing.
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

// Encryptor handles HPKE encryption/decryption with zstd compression.
// Uses MLKEM768X25519 KEM for post-quantum hybrid safety, HKDF-SHA256 for KDF,
// and AES-256-GCM for AEAD.
type Encryptor struct {
	pubKey  hpke.PublicKey
	privKey hpke.PrivateKey // nil for encrypt-only nodes
	kem     hpke.KEM
	kdf     hpke.KDF
	aead    hpke.AEAD
}

// NewEncryptor creates an Encryptor from raw public/private key bytes.
// privKey may be nil for encrypt-only nodes.
func NewEncryptor(pubKeyBytes, privKeyBytes []byte) (*Encryptor, error) {
	kem := hpke.MLKEM768X25519()
	kdf := hpke.HKDFSHA256()
	aead := hpke.AES256GCM()

	pub, err := kem.NewPublicKey(pubKeyBytes)
	if err != nil {
		return nil, fmt.Errorf("parse public key: %w", err)
	}

	var priv hpke.PrivateKey
	if privKeyBytes != nil {
		priv, err = kem.NewPrivateKey(privKeyBytes)
		if err != nil {
			return nil, fmt.Errorf("parse private key: %w", err)
		}
	}

	return &Encryptor{
		pubKey:  pub,
		privKey: priv,
		kem:     kem,
		kdf:     kdf,
		aead:    aead,
	}, nil
}

// SealAndCompress compresses with zstd, then encrypts with HPKE.
// The info parameter enables domain separation (e.g. "effect" vs "tip-recovery").
func (enc *Encryptor) SealAndCompress(plaintext, info []byte) ([]byte, error) {
	compressed := Compress(plaintext)
	sealed, err := hpke.Seal(enc.pubKey, enc.kdf, enc.aead, info, compressed)
	if err != nil {
		return nil, fmt.Errorf("hpke seal: %w", err)
	}
	return sealed, nil
}

// OpenAndDecompress decrypts with HPKE, then decompresses with zstd.
func (enc *Encryptor) OpenAndDecompress(sealed, info []byte) ([]byte, error) {
	if enc.privKey == nil {
		return nil, fmt.Errorf("no private key available for decryption")
	}
	compressed, err := hpke.Open(enc.privKey, enc.kdf, enc.aead, info, sealed)
	if err != nil {
		return nil, fmt.Errorf("hpke open: %w", err)
	}
	return Decompress(compressed)
}

// GenerateKeyPair generates a new HPKE MLKEM768X25519 keypair.
// Returns (publicKeyBytes, privateKeyBytes, error).
func GenerateKeyPair() ([]byte, []byte, error) {
	kem := hpke.MLKEM768X25519()
	priv, err := kem.GenerateKey()
	if err != nil {
		return nil, nil, fmt.Errorf("generate key: %w", err)
	}
	pubBytes := priv.PublicKey().Bytes()
	privBytes, err := priv.Bytes()
	if err != nil {
		return nil, nil, fmt.Errorf("serialize private key: %w", err)
	}
	return pubBytes, privBytes, nil
}
