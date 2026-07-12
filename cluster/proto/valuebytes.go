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

package proto

import (
	"fmt"
	"log/slog"

	"github.com/klauspost/compress/zstd"
)

// valueZDec inflates CompressedValue arms. This package can't reach
// effects.Decompress (effects imports proto), so the decoder lives here;
// DecodeAll on a nil-reader decoder is safe for concurrent use.
var valueZDec *zstd.Decoder

func init() {
	dec, err := zstd.NewReader(nil, zstd.WithDecoderMaxMemory(128<<20))
	if err != nil {
		panic(fmt.Sprintf("value zstd decoder init: %v", err))
	}
	valueZDec = dec
}

// Decompress returns the value's plain bytes regardless of how the writer
// stored them: the raw arm as-is, the compressed arm inflated. This is THE
// byte-value reader — call it wherever GetRaw() would be read as a value, or
// a compressed value silently reads as absent. Non-byte arms (int, float)
// and nil return nil, mirroring GetRaw. The stored effect is never mutated;
// inflation allocates fresh bytes per call.
//
// A corrupt or unknown-codec value returns nil after logging loudly: the
// same bytes fail identically on every observer, so "no value" is at least
// a deterministic answer while the log points at the damage.
func (d *DataEffect) Decompress() []byte {
	if d == nil {
		return nil
	}
	switch v := d.Value.(type) {
	case *DataEffect_Raw:
		return v.Raw
	case *DataEffect_Compressed:
		cv := v.Compressed
		if cv.GetCodec() != Compression_COMPRESSION_ZSTD {
			slog.Error("value decompress: unknown codec", "codec", cv.GetCodec())
			return nil
		}
		out, err := valueZDec.DecodeAll(cv.GetData(), nil)
		if err != nil {
			slog.Error("value decompress: corrupt compressed value", "error", err)
			return nil
		}
		return out
	}
	return nil
}
