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

package redis

import (
	"bufio"
	"bytes"
	"fmt"
	"testing"

	"github.com/swytchdb/swytch/redis/shared"
)

// ==== String Command Benchmarks ====

func BenchmarkHandler_Set(b *testing.B) {
	h := NewHandler(HandlerConfig{
		NumDatabases:  1,
		CapacityPerDB: 100000,
		MemoryLimit:   64 * 1024 * 1024,
	})
	defer h.Close()

	w := shared.GetWriter()
	defer shared.PutWriter(w)
	conn := newEffectsConn(h)

	for i := 0; b.Loop(); i++ {
		w.Buffer().Reset()
		cmd := &shared.Command{
			Type: shared.CmdSet,
			Args: [][]byte{
				fmt.Appendf(nil, "key%d", i),
				[]byte("value"),
			},
		}
		h.ExecuteInto(cmd, w, conn)
	}
}

func BenchmarkHandler_Get(b *testing.B) {
	h := NewHandler(HandlerConfig{
		NumDatabases:  1,
		CapacityPerDB: 100000,
		MemoryLimit:   64 * 1024 * 1024,
	})
	defer h.Close()

	w := shared.GetWriter()
	defer shared.PutWriter(w)
	conn := newEffectsConn(h)

	// Populate
	for i := range 10000 {
		w.Buffer().Reset()
		cmd := &shared.Command{
			Type: shared.CmdSet,
			Args: [][]byte{
				fmt.Appendf(nil, "key%d", i),
				fmt.Appendf(nil, "value%d", i),
			},
		}
		h.ExecuteInto(cmd, w, conn)
	}

	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		w.Buffer().Reset()
		cmd := &shared.Command{
			Type: shared.CmdGet,
			Args: [][]byte{fmt.Appendf(nil, "key%d", i%10000)},
		}
		h.ExecuteInto(cmd, w, conn)
	}
}

// ==== List Command Benchmarks ====

func BenchmarkHandler_LPush(b *testing.B) {
	h := NewHandler(HandlerConfig{
		NumDatabases:  1,
		CapacityPerDB: 100000,
		MemoryLimit:   64 * 1024 * 1024,
	})
	defer h.Close()

	w := shared.GetWriter()
	defer shared.PutWriter(w)
	conn := newEffectsConn(h)

	for i := 0; b.Loop(); i++ {
		w.Buffer().Reset()
		cmd := &shared.Command{
			Type: shared.CmdLPush,
			Args: [][]byte{
				[]byte("mylist"),
				fmt.Appendf(nil, "value%d", i),
			},
		}
		h.ExecuteInto(cmd, w, conn)
	}
}

func BenchmarkHandler_RPush(b *testing.B) {
	h := NewHandler(HandlerConfig{
		NumDatabases:  1,
		CapacityPerDB: 100000,
		MemoryLimit:   64 * 1024 * 1024,
	})
	defer h.Close()

	w := shared.GetWriter()
	defer shared.PutWriter(w)
	conn := newEffectsConn(h)

	for i := 0; b.Loop(); i++ {
		w.Buffer().Reset()
		cmd := &shared.Command{
			Type: shared.CmdRPush,
			Args: [][]byte{
				[]byte("mylist"),
				fmt.Appendf(nil, "value%d", i),
			},
		}
		h.ExecuteInto(cmd, w, conn)
	}
}

func BenchmarkHandler_LPop(b *testing.B) {
	h := NewHandler(HandlerConfig{
		NumDatabases:  1,
		CapacityPerDB: 100000,
		MemoryLimit:   64 * 1024 * 1024,
	})
	defer h.Close()

	w := shared.GetWriter()
	defer shared.PutWriter(w)
	conn := newEffectsConn(h)

	// Populate
	for i := range 100000 {
		w.Buffer().Reset()
		cmd := &shared.Command{
			Type: shared.CmdRPush,
			Args: [][]byte{
				[]byte("mylist"),
				fmt.Appendf(nil, "value%d", i),
			},
		}
		h.ExecuteInto(cmd, w, conn)
	}

	b.ResetTimer()
	for b.Loop() {
		w.Buffer().Reset()
		cmd := &shared.Command{
			Type: shared.CmdLPop,
			Args: [][]byte{[]byte("mylist")},
		}
		h.ExecuteInto(cmd, w, conn)
	}
}

func BenchmarkHandler_RPop(b *testing.B) {
	h := NewHandler(HandlerConfig{
		NumDatabases:  1,
		CapacityPerDB: 100000,
		MemoryLimit:   64 * 1024 * 1024,
	})
	defer h.Close()

	w := shared.GetWriter()
	defer shared.PutWriter(w)
	conn := newEffectsConn(h)

	// Populate
	for i := range 100000 {
		w.Buffer().Reset()
		cmd := &shared.Command{
			Type: shared.CmdRPush,
			Args: [][]byte{
				[]byte("mylist"),
				fmt.Appendf(nil, "value%d", i),
			},
		}
		h.ExecuteInto(cmd, w, conn)
	}

	b.ResetTimer()
	for b.Loop() {
		w.Buffer().Reset()
		cmd := &shared.Command{
			Type: shared.CmdRPop,
			Args: [][]byte{[]byte("mylist")},
		}
		h.ExecuteInto(cmd, w, conn)
	}
}

func BenchmarkHandler_LRange(b *testing.B) {
	h := NewHandler(HandlerConfig{
		NumDatabases:  1,
		CapacityPerDB: 100000,
		MemoryLimit:   64 * 1024 * 1024,
	})
	defer h.Close()

	w := shared.GetWriter()
	defer shared.PutWriter(w)
	conn := newEffectsConn(h)

	// Populate
	for i := range 1000 {
		w.Buffer().Reset()
		cmd := &shared.Command{
			Type: shared.CmdRPush,
			Args: [][]byte{
				[]byte("mylist"),
				fmt.Appendf(nil, "value%d", i),
			},
		}
		h.ExecuteInto(cmd, w, conn)
	}

	b.ResetTimer()
	for b.Loop() {
		w.Buffer().Reset()
		cmd := &shared.Command{
			Type: shared.CmdLRange,
			Args: [][]byte{[]byte("mylist"), []byte("0"), []byte("99")},
		}
		h.ExecuteInto(cmd, w, conn)
	}
}

func BenchmarkHandler_LIndex(b *testing.B) {
	h := NewHandler(HandlerConfig{
		NumDatabases:  1,
		CapacityPerDB: 100000,
		MemoryLimit:   64 * 1024 * 1024,
	})
	defer h.Close()

	w := shared.GetWriter()
	defer shared.PutWriter(w)
	conn := newEffectsConn(h)

	// Populate
	for i := range 1000 {
		w.Buffer().Reset()
		cmd := &shared.Command{
			Type: shared.CmdRPush,
			Args: [][]byte{
				[]byte("mylist"),
				fmt.Appendf(nil, "value%d", i),
			},
		}
		h.ExecuteInto(cmd, w, conn)
	}

	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		w.Buffer().Reset()
		cmd := &shared.Command{
			Type: shared.CmdLIndex,
			Args: [][]byte{[]byte("mylist"), fmt.Appendf(nil, "%d", i%1000)},
		}
		h.ExecuteInto(cmd, w, conn)
	}
}

// ==== Hash Command Benchmarks ====

func BenchmarkHandler_HSet(b *testing.B) {
	h := NewHandler(HandlerConfig{
		NumDatabases:  1,
		CapacityPerDB: 100000,
		MemoryLimit:   64 * 1024 * 1024,
	})
	defer h.Close()

	w := shared.GetWriter()
	defer shared.PutWriter(w)
	conn := newEffectsConn(h)

	for i := 0; b.Loop(); i++ {
		w.Buffer().Reset()
		cmd := &shared.Command{
			Type: shared.CmdHSet,
			Args: [][]byte{
				[]byte("myhash"),
				fmt.Appendf(nil, "field%d", i),
				fmt.Appendf(nil, "value%d", i),
			},
		}
		h.ExecuteInto(cmd, w, conn)
	}
}

func BenchmarkHandler_HGet(b *testing.B) {
	h := NewHandler(HandlerConfig{
		NumDatabases:  1,
		CapacityPerDB: 100000,
		MemoryLimit:   64 * 1024 * 1024,
	})
	defer h.Close()

	w := shared.GetWriter()
	defer shared.PutWriter(w)
	conn := newEffectsConn(h)

	// Populate
	for i := range 1000 {
		w.Buffer().Reset()
		cmd := &shared.Command{
			Type: shared.CmdHSet,
			Args: [][]byte{
				[]byte("myhash"),
				fmt.Appendf(nil, "field%d", i),
				fmt.Appendf(nil, "value%d", i),
			},
		}
		h.ExecuteInto(cmd, w, conn)
	}

	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		w.Buffer().Reset()
		cmd := &shared.Command{
			Type: shared.CmdHGet,
			Args: [][]byte{
				[]byte("myhash"),
				fmt.Appendf(nil, "field%d", i%1000),
			},
		}
		h.ExecuteInto(cmd, w, conn)
	}
}

func BenchmarkHandler_HGetAll(b *testing.B) {
	h := NewHandler(HandlerConfig{
		NumDatabases:  1,
		CapacityPerDB: 100000,
		MemoryLimit:   64 * 1024 * 1024,
	})
	defer h.Close()

	w := shared.GetWriter()
	defer shared.PutWriter(w)
	conn := newEffectsConn(h)

	// Populate
	for i := range 100 {
		w.Buffer().Reset()
		cmd := &shared.Command{
			Type: shared.CmdHSet,
			Args: [][]byte{
				[]byte("myhash"),
				fmt.Appendf(nil, "field%d", i),
				fmt.Appendf(nil, "value%d", i),
			},
		}
		h.ExecuteInto(cmd, w, conn)
	}

	b.ResetTimer()
	for b.Loop() {
		w.Buffer().Reset()
		cmd := &shared.Command{
			Type: shared.CmdHGetAll,
			Args: [][]byte{[]byte("myhash")},
		}
		h.ExecuteInto(cmd, w, conn)
	}
}

func BenchmarkHandler_HIncrBy(b *testing.B) {
	h := NewHandler(HandlerConfig{
		NumDatabases:  1,
		CapacityPerDB: 100000,
		MemoryLimit:   64 * 1024 * 1024,
	})
	defer h.Close()

	w := shared.GetWriter()
	defer shared.PutWriter(w)
	conn := newEffectsConn(h)

	// Setup counter
	w.Buffer().Reset()
	cmd := &shared.Command{
		Type: shared.CmdHSet,
		Args: [][]byte{[]byte("myhash"), []byte("counter"), []byte("0")},
	}
	h.ExecuteInto(cmd, w, conn)

	b.ResetTimer()
	for b.Loop() {
		w.Buffer().Reset()
		cmd := &shared.Command{
			Type: shared.CmdHIncrBy,
			Args: [][]byte{[]byte("myhash"), []byte("counter"), []byte("1")},
		}
		h.ExecuteInto(cmd, w, conn)
	}
}

// ==== String Increment Benchmarks ====

func BenchmarkHandler_Incr(b *testing.B) {
	h := NewHandler(HandlerConfig{
		NumDatabases:  1,
		CapacityPerDB: 100000,
		MemoryLimit:   64 * 1024 * 1024,
	})
	defer h.Close()

	w := shared.GetWriter()
	defer shared.PutWriter(w)
	conn := newEffectsConn(h)

	// Setup counter
	w.Buffer().Reset()
	cmd := &shared.Command{
		Type: shared.CmdSet,
		Args: [][]byte{[]byte("counter"), []byte("0")},
	}
	h.ExecuteInto(cmd, w, conn)

	b.ResetTimer()
	for b.Loop() {
		w.Buffer().Reset()
		cmd := &shared.Command{
			Type: shared.CmdIncr,
			Args: [][]byte{[]byte("counter")},
		}
		h.ExecuteInto(cmd, w, conn)
	}
}

func BenchmarkHandler_IncrBy(b *testing.B) {
	h := NewHandler(HandlerConfig{
		NumDatabases:  1,
		CapacityPerDB: 100000,
		MemoryLimit:   64 * 1024 * 1024,
	})
	defer h.Close()

	w := shared.GetWriter()
	defer shared.PutWriter(w)
	conn := newEffectsConn(h)

	// Setup counter
	w.Buffer().Reset()
	cmd := &shared.Command{
		Type: shared.CmdSet,
		Args: [][]byte{[]byte("counter"), []byte("0")},
	}
	h.ExecuteInto(cmd, w, conn)

	b.ResetTimer()
	for b.Loop() {
		w.Buffer().Reset()
		cmd := &shared.Command{
			Type: shared.CmdIncrBy,
			Args: [][]byte{[]byte("counter"), []byte("10")},
		}
		h.ExecuteInto(cmd, w, conn)
	}
}

// ==== Mixed Workload Benchmarks ====

func BenchmarkHandler_Mixed(b *testing.B) {
	h := NewHandler(HandlerConfig{
		NumDatabases:  1,
		CapacityPerDB: 100000,
		MemoryLimit:   64 * 1024 * 1024,
	})
	defer h.Close()

	w := shared.GetWriter()
	defer shared.PutWriter(w)
	conn := newEffectsConn(h)

	// Populate
	for i := range 10000 {
		w.Buffer().Reset()
		cmd := &shared.Command{
			Type: shared.CmdSet,
			Args: [][]byte{
				fmt.Appendf(nil, "key%d", i),
				fmt.Appendf(nil, "value%d", i),
			},
		}
		h.ExecuteInto(cmd, w, conn)
	}

	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		w.Buffer().Reset()
		if i%5 == 0 {
			// 20% SET
			cmd := &shared.Command{
				Type: shared.CmdSet,
				Args: [][]byte{
					fmt.Appendf(nil, "key%d", i%10000),
					[]byte("newvalue"),
				},
			}
			h.ExecuteInto(cmd, w, conn)
		} else {
			// 80% GET
			cmd := &shared.Command{
				Type: shared.CmdGet,
				Args: [][]byte{fmt.Appendf(nil, "key%d", i%10000)},
			}
			h.ExecuteInto(cmd, w, conn)
		}
	}
}

// ==== Parallel Benchmarks ====

func BenchmarkHandler_SetParallel(b *testing.B) {
	h := NewHandler(HandlerConfig{
		NumDatabases:  1,
		CapacityPerDB: 100000,
		MemoryLimit:   64 * 1024 * 1024,
	})
	defer h.Close()

	b.RunParallel(func(pb *testing.PB) {
		w := shared.GetWriter()
		defer shared.PutWriter(w)
		conn := newEffectsConn(h)
		i := 0
		for pb.Next() {
			w.Buffer().Reset()
			cmd := &shared.Command{
				Type: shared.CmdSet,
				Args: [][]byte{
					fmt.Appendf(nil, "key%d", i),
					[]byte("value"),
				},
			}
			h.ExecuteInto(cmd, w, conn)
			i++
		}
	})
}

func BenchmarkHandler_GetParallel(b *testing.B) {
	h := NewHandler(HandlerConfig{
		NumDatabases:  1,
		CapacityPerDB: 100000,
		MemoryLimit:   64 * 1024 * 1024,
	})
	defer h.Close()

	w := shared.GetWriter()
	conn := newEffectsConn(h)

	// Populate
	for i := range 10000 {
		w.Buffer().Reset()
		cmd := &shared.Command{
			Type: shared.CmdSet,
			Args: [][]byte{
				fmt.Appendf(nil, "key%d", i),
				fmt.Appendf(nil, "value%d", i),
			},
		}
		h.ExecuteInto(cmd, w, conn)
	}
	shared.PutWriter(w)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		localW := shared.GetWriter()
		defer shared.PutWriter(localW)
		localConn := newEffectsConn(h)
		i := 0
		for pb.Next() {
			localW.Buffer().Reset()
			cmd := &shared.Command{
				Type: shared.CmdGet,
				Args: [][]byte{fmt.Appendf(nil, "key%d", i%10000)},
			}
			h.ExecuteInto(cmd, localW, localConn)
			i++
		}
	})
}

func BenchmarkHandler_LPushParallel(b *testing.B) {
	h := NewHandler(HandlerConfig{
		NumDatabases:  1,
		CapacityPerDB: 100000,
		MemoryLimit:   64 * 1024 * 1024,
	})
	defer h.Close()

	b.RunParallel(func(pb *testing.PB) {
		w := shared.GetWriter()
		defer shared.PutWriter(w)
		conn := newEffectsConn(h)
		i := 0
		for pb.Next() {
			w.Buffer().Reset()
			cmd := &shared.Command{
				Type: shared.CmdLPush,
				Args: [][]byte{
					[]byte("mylist"),
					fmt.Appendf(nil, "value%d", i),
				},
			}
			h.ExecuteInto(cmd, w, conn)
			i++
		}
	})
}

func BenchmarkHandler_LPopParallel(b *testing.B) {
	h := NewHandler(HandlerConfig{
		NumDatabases:  1,
		CapacityPerDB: 100000,
		MemoryLimit:   64 * 1024 * 1024,
	})
	defer h.Close()

	w := shared.GetWriter()
	conn := newEffectsConn(h)

	// Pre-populate list with many elements
	for i := range 100000 {
		w.Buffer().Reset()
		cmd := &shared.Command{
			Type: shared.CmdRPush,
			Args: [][]byte{
				[]byte("mylist"),
				fmt.Appendf(nil, "value%d", i),
			},
		}
		h.ExecuteInto(cmd, w, conn)
	}
	shared.PutWriter(w)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		localW := shared.GetWriter()
		defer shared.PutWriter(localW)
		localConn := newEffectsConn(h)
		for pb.Next() {
			localW.Buffer().Reset()
			cmd := &shared.Command{
				Type: shared.CmdLPop,
				Args: [][]byte{[]byte("mylist")},
			}
			h.ExecuteInto(cmd, localW, localConn)
		}
	})
}

func BenchmarkHandler_RPopParallel(b *testing.B) {
	h := NewHandler(HandlerConfig{
		NumDatabases:  1,
		CapacityPerDB: 100000,
		MemoryLimit:   64 * 1024 * 1024,
	})
	defer h.Close()

	w := shared.GetWriter()
	conn := newEffectsConn(h)

	// Pre-populate list with many elements
	for i := range 100000 {
		w.Buffer().Reset()
		cmd := &shared.Command{
			Type: shared.CmdRPush,
			Args: [][]byte{
				[]byte("mylist"),
				fmt.Appendf(nil, "value%d", i),
			},
		}
		h.ExecuteInto(cmd, w, conn)
	}
	shared.PutWriter(w)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		localW := shared.GetWriter()
		defer shared.PutWriter(localW)
		localConn := newEffectsConn(h)
		for pb.Next() {
			localW.Buffer().Reset()
			cmd := &shared.Command{
				Type: shared.CmdRPop,
				Args: [][]byte{[]byte("mylist")},
			}
			h.ExecuteInto(cmd, localW, localConn)
		}
	})
}

func BenchmarkHandler_MixedParallel(b *testing.B) {
	h := NewHandler(HandlerConfig{
		NumDatabases:  1,
		CapacityPerDB: 100000,
		MemoryLimit:   64 * 1024 * 1024,
	})
	defer h.Close()

	w := shared.GetWriter()
	conn := newEffectsConn(h)

	// Populate
	for i := range 10000 {
		w.Buffer().Reset()
		cmd := &shared.Command{
			Type: shared.CmdSet,
			Args: [][]byte{
				fmt.Appendf(nil, "key%d", i),
				fmt.Appendf(nil, "value%d", i),
			},
		}
		h.ExecuteInto(cmd, w, conn)
	}
	shared.PutWriter(w)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		localW := shared.GetWriter()
		defer shared.PutWriter(localW)
		localConn := newEffectsConn(h)
		i := 0
		for pb.Next() {
			localW.Buffer().Reset()
			if i%5 == 0 {
				// 20% SET
				cmd := &shared.Command{
					Type: shared.CmdSet,
					Args: [][]byte{
						fmt.Appendf(nil, "key%d", i%10000),
						[]byte("newvalue"),
					},
				}
				h.ExecuteInto(cmd, localW, localConn)
			} else {
				// 80% GET
				cmd := &shared.Command{
					Type: shared.CmdGet,
					Args: [][]byte{fmt.Appendf(nil, "key%d", i%10000)},
				}
				h.ExecuteInto(cmd, localW, localConn)
			}
			i++
		}
	})
}

// ==== Large Value Benchmarks ====

func BenchmarkHandler_SetLargeValue(b *testing.B) {
	h := NewHandler(HandlerConfig{
		NumDatabases:  1,
		CapacityPerDB: 10000,
		MemoryLimit:   256 * 1024 * 1024,
	})
	defer h.Close()

	w := shared.GetWriter()
	defer shared.PutWriter(w)
	conn := newEffectsConn(h)

	largeValue := make([]byte, 10*1024) // 10KB value
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	for i := 0; b.Loop(); i++ {
		w.Buffer().Reset()
		cmd := &shared.Command{
			Type: shared.CmdSet,
			Args: [][]byte{
				fmt.Appendf(nil, "key%d", i),
				largeValue,
			},
		}
		h.ExecuteInto(cmd, w, conn)
	}
}

// ==== Parser Benchmarks ====

func BenchmarkParser_Set(b *testing.B) {
	input := []byte("*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n")
	reader := bytes.NewReader(input)
	parser := shared.NewParserWithReader(bufio.NewReaderSize(reader, 4096))
	cmd := shared.GetCommand()
	defer shared.PutCommand(cmd)

	b.ReportAllocs()
	for b.Loop() {
		reader.Seek(0, 0)
		parser.Reset(reader)
		cmd.Reset()
		parser.ReadCommandInto(cmd)
	}
}

func BenchmarkParser_Get(b *testing.B) {
	input := []byte("*2\r\n$3\r\nGET\r\n$5\r\nmykey\r\n")
	reader := bytes.NewReader(input)
	parser := shared.NewParserWithReader(bufio.NewReaderSize(reader, 4096))
	cmd := shared.GetCommand()
	defer shared.PutCommand(cmd)

	b.ReportAllocs()
	for b.Loop() {
		reader.Seek(0, 0)
		parser.Reset(reader)
		cmd.Reset()
		parser.ReadCommandInto(cmd)
	}
}

func BenchmarkParser_LPush(b *testing.B) {
	input := []byte("*4\r\n$5\r\nLPUSH\r\n$6\r\nmylist\r\n$6\r\nvalue1\r\n$6\r\nvalue2\r\n")
	reader := bytes.NewReader(input)
	parser := shared.NewParserWithReader(bufio.NewReaderSize(reader, 4096))
	cmd := shared.GetCommand()
	defer shared.PutCommand(cmd)

	b.ReportAllocs()
	for b.Loop() {
		reader.Seek(0, 0)
		parser.Reset(reader)
		cmd.Reset()
		parser.ReadCommandInto(cmd)
	}
}

// ==== Writer Benchmarks ====

func BenchmarkWriter_SimpleString(b *testing.B) {
	w := shared.GetWriter()
	defer shared.PutWriter(w)

	b.ReportAllocs()
	for b.Loop() {
		w.Buffer().Reset()
		w.WriteSimpleString("OK")
	}
}

func BenchmarkWriter_BulkString(b *testing.B) {
	w := shared.GetWriter()
	defer shared.PutWriter(w)
	data := []byte("Hello, World!")

	b.ReportAllocs()
	for b.Loop() {
		w.Buffer().Reset()
		w.WriteBulkString(data)
	}
}

func BenchmarkWriter_Integer(b *testing.B) {
	w := shared.GetWriter()
	defer shared.PutWriter(w)

	b.ReportAllocs()
	for b.Loop() {
		w.Buffer().Reset()
		w.WriteInteger(12345)
	}
}

func BenchmarkWriter_Array(b *testing.B) {
	w := shared.GetWriter()
	defer shared.PutWriter(w)

	b.ReportAllocs()
	for b.Loop() {
		w.Buffer().Reset()
		w.WriteArray(5)
		for i := range 5 {
			w.WriteBulkString(fmt.Appendf(nil, "element%d", i))
		}
	}
}

// BenchmarkHandler_LPush_LargeList benchmarks LPUSH on an already large list
func BenchmarkHandler_LPush_LargeList(b *testing.B) {
	sizes := []int{1000, 10000, 100000, 1000000, 10000000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			h := NewHandler(HandlerConfig{
				NumDatabases:  1,
				CapacityPerDB: 100000,
				MemoryLimit:   1024 * 1024 * 1024, // 1GB
			})
			defer h.Close()

			w := shared.GetWriter()
			defer shared.PutWriter(w)
			conn := newEffectsConn(h)

			// Pre-populate list
			for i := range size {
				w.Buffer().Reset()
				cmd := &shared.Command{
					Type: shared.CmdRPush,
					Args: [][]byte{
						[]byte("mylist"),
						fmt.Appendf(nil, "value%d", i),
					},
				}
				h.ExecuteInto(cmd, w, conn)
			}

			b.ResetTimer()
			for i := 0; b.Loop(); i++ {
				w.Buffer().Reset()
				cmd := &shared.Command{
					Type: shared.CmdLPush,
					Args: [][]byte{
						[]byte("mylist"),
						[]byte("newvalue"),
					},
				}
				h.ExecuteInto(cmd, w, conn)
			}
		})
	}
}

// BenchmarkHandler_MSet benchmarks MSET (in-memory)
func BenchmarkHandler_MSet(b *testing.B) {
	h := NewHandler(HandlerConfig{
		NumDatabases:  1,
		CapacityPerDB: 1000000,
		MemoryLimit:   256 * 1024 * 1024,
	})
	defer h.Close()

	w := shared.GetWriter()
	defer shared.PutWriter(w)
	conn := newEffectsConn(h)

	// MSET with 10 key-value pairs
	args := make([][]byte, 20)
	for i := range 10 {
		args[i*2] = fmt.Appendf(nil, "key%d", i)
		args[i*2+1] = []byte("value")
	}

	for i := 0; b.Loop(); i++ {
		w.Buffer().Reset()
		// Update keys to avoid overwriting same ones
		for j := range 10 {
			args[j*2] = fmt.Appendf(args[j*2][:0], "key%d_%d", j, i)
		}
		cmd := &shared.Command{
			Type: shared.CmdMSet,
			Args: args,
		}
		h.ExecuteInto(cmd, w, conn)
	}
}
