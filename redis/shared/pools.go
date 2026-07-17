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

package shared

import (
	"bytes"
	"sync"
	"unsafe"
)

// Buffer sizes for optimal performance
// Larger buffers reduce syscall frequency at the cost of memory per connection
const (
	DefaultBufSize  = 32768   // 32KB for write buffers - reduces write syscalls
	ReadBufSize     = 65536   // 64KB read buffer - better pipelining support
	maxDataBufSize  = 1 << 20 // 1MB max pooled data buffer
	ResponseBufSize = 8192    // 8KB response buffer initial size
)

// commandPool pools Command structs
var commandPool = sync.Pool{
	New: func() any {
		return &Command{
			Args: make([][]byte, 0, 8), // Pre-allocate for common commands
		}
	},
}

// writerPool pools Writer structs
var writerPool = sync.Pool{
	New: func() any {
		buf := &bytes.Buffer{}
		buf.Grow(ResponseBufSize)
		return &Writer{buf: buf}
	},
}

// dataBufSizes maps pool index to buffer size
var dataBufSizes = [...]int{64, 256, 1024, 4096, 16384, 65536, 262144, 1048576}

// dataBufPools - tiered pools for data buffers of various sizes. Each pool
// holds raw pointers to fixed-size backing arrays (as unsafe.Pointer, which
// fits an interface word without boxing) rather than *[]byte: a slice header
// is a fresh heap object per Put, so pooling headers cost an allocation to
// donate a buffer. The class size is re-attached on Get.
var dataBufPools [len(dataBufSizes)]sync.Pool

func init() {
	for i, size := range dataBufSizes {
		dataBufPools[i].New = func() any {
			return unsafe.Pointer(unsafe.SliceData(make([]byte, size)))
		}
	}
}

// sizeClassIndex returns the pool index for the given size using O(1) bit math
// Pool sizes: 64, 256, 1K, 4K, 16K, 64K, 256K, 1M (indices 0-7)
func sizeClassIndex(n int) int {
	if n <= 64 {
		return 0
	}
	if n <= 256 {
		return 1
	}
	if n <= 1024 {
		return 2
	}
	if n <= 4096 {
		return 3
	}
	if n <= 16384 {
		return 4
	}
	if n <= 65536 {
		return 5
	}
	if n <= 262144 {
		return 6
	}
	return 7
}

// GetDataBuf gets a byte buffer of at least size n from the appropriate pool
func GetDataBuf(n int) []byte {
	if n > maxDataBufSize {
		// Too large to pool
		return make([]byte, n)
	}

	poolIdx := sizeClassIndex(n)
	p := dataBufPools[poolIdx].Get().(unsafe.Pointer)
	return unsafe.Slice((*byte)(p), dataBufSizes[poolIdx])[:n]
}

// PutDataBuf returns a byte buffer to the appropriate pool. The caller yields
// ownership of buf's full capacity.
func PutDataBuf(buf []byte) {
	c := cap(buf)
	if c < dataBufSizes[0] || c > maxDataBufSize {
		return
	}

	// Find the right pool based on capacity
	for i := len(dataBufSizes) - 1; i >= 0; i-- {
		if c >= dataBufSizes[i] {
			dataBufPools[i].Put(unsafe.Pointer(unsafe.SliceData(buf)))
			return
		}
	}
}

// GetCommand gets a Command from the pool
func GetCommand() *Command {
	cmd := commandPool.Get().(*Command)
	cmd.Reset()
	return cmd
}

// PutCommand returns a Command to the pool
func PutCommand(cmd *Command) {
	if cmd == nil {
		return
	}
	// Return data buffers if poolable
	for _, arg := range cmd.Args {
		if arg != nil {
			PutDataBuf(arg)
		}
	}
	clear(cmd.Args)
	cmd.keyScratch[0] = ""
	cmd.Args = cmd.Args[:0]
	commandPool.Put(cmd)
}

// GetWriter gets a Writer from the pool
func GetWriter() *Writer {
	w := writerPool.Get().(*Writer)
	w.buf.Reset()
	return w
}

// PutWriter returns a Writer to the pool
func PutWriter(w *Writer) {
	if w == nil || w.buf == nil {
		return
	}
	if w.buf.Cap() > ResponseBufSize*4 {
		// Don't pool overly large buffers
		return
	}
	if w.scratch.Cap() > ResponseBufSize*4 {
		// Same size policy as buf: one huge response must not pin its
		// staging capacity in the pool forever.
		w.scratch = bytes.Buffer{}
	}
	writerPool.Put(w)
}
