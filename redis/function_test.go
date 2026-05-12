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
	"strings"
	"testing"

	"github.com/swytchdb/swytch/redis/shared"
)

func TestFunctionLoad(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	conn := newEffectsConn(h)
	conn.Protocol = shared.RESP3

	t.Run("basic load", func(t *testing.T) {
		result := execCmd(h, conn, shared.CmdFunction, "LOAD",
			"#!lua name=mylib\nredis.register_function('hello', function() return 'world' end)")
		if !strings.Contains(result, "mylib") {
			t.Errorf("expected result to contain 'mylib', got: %s", result)
		}
	})

	t.Run("load duplicate without replace", func(t *testing.T) {
		result := execCmd(h, conn, shared.CmdFunction, "LOAD",
			"#!lua name=mylib\nredis.register_function('hello2', function() return 'world2' end)")
		if !strings.Contains(result, "already exists") {
			t.Errorf("expected 'already exists' error, got: %s", result)
		}
	})

	t.Run("load with replace", func(t *testing.T) {
		result := execCmd(h, conn, shared.CmdFunction, "LOAD", "REPLACE",
			"#!lua name=mylib\nredis.register_function('hello', function() return 'updated' end)")
		if !strings.Contains(result, "mylib") {
			t.Errorf("expected result to contain 'mylib', got: %s", result)
		}
	})

	t.Run("load empty library", func(t *testing.T) {
		result := execCmd(h, conn, shared.CmdFunction, "LOAD",
			"#!lua name=emptylib\n-- no functions")
		if !strings.Contains(result, "No functions registered") {
			t.Errorf("expected 'No functions registered' error, got: %s", result)
		}
	})

	t.Run("load with invalid shebang", func(t *testing.T) {
		result := execCmd(h, conn, shared.CmdFunction, "LOAD", "local x = 1")
		if !strings.Contains(result, "shebang") {
			t.Errorf("expected shebang error, got: %s", result)
		}
	})
}

func TestFunctionList(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	conn := newEffectsConn(h)
	conn.Protocol = shared.RESP3

	// Load a library first
	execCmd(h, conn, shared.CmdFunction, "LOAD",
		"#!lua name=testlib\nredis.register_function{function_name='myfunc', callback=function() return 1 end, description='test function', flags={'no-writes'}}")

	t.Run("list all", func(t *testing.T) {
		result := execCmd(h, conn, shared.CmdFunction, "LIST")
		if !strings.Contains(result, "testlib") {
			t.Errorf("expected result to contain 'testlib', got: %s", result)
		}
		if !strings.Contains(result, "myfunc") {
			t.Errorf("expected result to contain 'myfunc', got: %s", result)
		}
	})

	t.Run("list with pattern", func(t *testing.T) {
		result := execCmd(h, conn, shared.CmdFunction, "LIST", "LIBRARYNAME", "test*")
		if !strings.Contains(result, "testlib") {
			t.Errorf("expected result to contain 'testlib', got: %s", result)
		}
	})

	t.Run("list with code", func(t *testing.T) {
		result := execCmd(h, conn, shared.CmdFunction, "LIST", "WITHCODE")
		if !strings.Contains(result, "library_code") {
			t.Errorf("expected result to contain 'library_code', got: %s", result)
		}
	})
}

func TestFunctionDelete(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	conn := newEffectsConn(h)
	conn.Protocol = shared.RESP3

	// Load a library first
	execCmd(h, conn, shared.CmdFunction, "LOAD",
		"#!lua name=todelete\nredis.register_function('deleteme', function() return 1 end)")

	t.Run("delete existing", func(t *testing.T) {
		result := execCmd(h, conn, shared.CmdFunction, "DELETE", "todelete")
		if !strings.Contains(result, "OK") {
			t.Errorf("expected OK, got: %s", result)
		}
	})

	t.Run("delete non-existent", func(t *testing.T) {
		result := execCmd(h, conn, shared.CmdFunction, "DELETE", "nonexistent")
		if !strings.Contains(result, "not found") {
			t.Errorf("expected 'not found' error, got: %s", result)
		}
	})
}

func TestFunctionFlush(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	conn := newEffectsConn(h)
	conn.Protocol = shared.RESP3

	// Load some libraries
	for _, name := range []string{"lib1", "lib2", "lib3"} {
		execCmd(h, conn, shared.CmdFunction, "LOAD",
			"#!lua name="+name+"\nredis.register_function('func_"+name+"', function() return 1 end)")
	}

	// Flush
	result := execCmd(h, conn, shared.CmdFunction, "FLUSH")
	if !strings.Contains(result, "OK") {
		t.Errorf("expected OK, got: %s", result)
	}

	// Verify by listing
	listResult := execCmd(h, conn, shared.CmdFunction, "LIST")
	// An empty list should be *0
	if !strings.Contains(listResult, "*0\r\n") {
		t.Errorf("expected empty list after flush, got: %s", listResult)
	}
}

func TestFcall(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	conn := newEffectsConn(h)
	conn.Protocol = shared.RESP3

	// Load a library with functions
	libCode := `#!lua name=fcalllib
redis.register_function('echo', function(keys, args)
    return args[1]
end)

redis.register_function('getset', function(keys, args)
    local old = redis.call('GET', keys[1])
    redis.call('SET', keys[1], args[1])
    return old
end)

redis.register_function('sum', function(keys, args)
    local total = 0
    for i, v in ipairs(args) do
        total = total + tonumber(v)
    end
    return total
end)
`
	execCmd(h, conn, shared.CmdFunction, "LOAD", libCode)

	t.Run("fcall echo", func(t *testing.T) {
		result := execCmd(h, conn, shared.CmdFcall, "echo", "0", "hello world")
		if !strings.Contains(result, "hello world") {
			t.Errorf("expected 'hello world' in result, got: %s", result)
		}
	})

	t.Run("fcall getset", func(t *testing.T) {
		// First, set a value via SET command
		execCmd(h, conn, shared.CmdEval, "redis.call('SET', KEYS[1], ARGV[1])", "1", "testkey", "oldvalue")

		result := execCmd(h, conn, shared.CmdFcall, "getset", "1", "testkey", "newvalue")
		if !strings.Contains(result, "oldvalue") {
			t.Errorf("expected 'oldvalue' in result, got: %s", result)
		}
	})

	t.Run("fcall sum", func(t *testing.T) {
		result := execCmd(h, conn, shared.CmdFcall, "sum", "0", "1", "2", "3", "4")
		if !strings.Contains(result, "10") {
			t.Errorf("expected '10' in result, got: %s", result)
		}
	})

	t.Run("fcall nonexistent function", func(t *testing.T) {
		result := execCmd(h, conn, shared.CmdFcall, "nonexistent", "0")
		if !strings.Contains(result, "not found") {
			t.Errorf("expected 'not found' error, got: %s", result)
		}
	})
}

func TestFcallRO(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	conn := newEffectsConn(h)
	conn.Protocol = shared.RESP3

	// Load a library with read-only function
	libCode := `#!lua name=rolib
redis.register_function{
    function_name='readonly_get',
    callback=function(keys, args)
        return redis.call('GET', keys[1])
    end,
    flags={'no-writes'}
}
`
	execCmd(h, conn, shared.CmdFunction, "LOAD", libCode)

	// Set a value to read
	execCmd(h, conn, shared.CmdEval, "redis.call('SET', KEYS[1], ARGV[1])", "1", "rokey", "rovalue")

	t.Run("fcall_ro read operation", func(t *testing.T) {
		result := execCmd(h, conn, shared.CmdFcallRO, "readonly_get", "1", "rokey")
		if !strings.Contains(result, "rovalue") {
			t.Errorf("expected 'rovalue' in result, got: %s", result)
		}
	})
}

func TestFunctionStats(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	conn := newEffectsConn(h)
	conn.Protocol = shared.RESP3

	// Load a library
	execCmd(h, conn, shared.CmdFunction, "LOAD",
		"#!lua name=statslib\nredis.register_function('f1', function() return 1 end)\nredis.register_function('f2', function() return 2 end)")

	t.Run("stats", func(t *testing.T) {
		result := execCmd(h, conn, shared.CmdFunction, "STATS")
		if !strings.Contains(result, "engines") {
			t.Errorf("expected 'engines' in result, got: %s", result)
		}
		if !strings.Contains(result, "LUA") {
			t.Errorf("expected 'LUA' in result, got: %s", result)
		}
	})
}

func TestRegisterFunctionTableForm(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	conn := newEffectsConn(h)
	conn.Protocol = shared.RESP3

	libCode := `#!lua name=tableformlib
redis.register_function{
    function_name='myfunc',
    callback=function(keys, args) return 'OK' end,
    description='A test function',
    flags={'no-writes', 'allow-stale'}
}
`
	result := execCmd(h, conn, shared.CmdFunction, "LOAD", libCode)
	if !strings.Contains(result, "tableformlib") {
		t.Fatalf("expected successful load, got: %s", result)
	}

	// Verify the function metadata via LIST
	listResult := execCmd(h, conn, shared.CmdFunction, "LIST")
	if !strings.Contains(listResult, "myfunc") {
		t.Errorf("expected function 'myfunc' in list, got: %s", listResult)
	}
	if !strings.Contains(listResult, "A test function") {
		t.Errorf("expected description in list, got: %s", listResult)
	}
}

func TestFunctionDumpRestore(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	conn := newEffectsConn(h)
	conn.Protocol = shared.RESP3

	// Load a library
	execCmd(h, conn, shared.CmdFunction, "LOAD",
		"#!lua name=dumplib\nredis.register_function('dumpfunc', function() return 'dumped' end)")

	// Dump
	dumpData := execCmd(h, conn, shared.CmdFunction, "DUMP")

	// Flush
	execCmd(h, conn, shared.CmdFunction, "FLUSH")

	// Verify flush worked
	listResult := execCmd(h, conn, shared.CmdFunction, "LIST")
	if !strings.Contains(listResult, "*0\r\n") {
		t.Fatalf("flush did not clear libraries, LIST result: %s", listResult)
	}

	// Restore - extract the bulk string from RESP format
	_, after, ok := strings.Cut(dumpData, "\r\n")
	if !ok {
		t.Fatalf("invalid dump format: %s", dumpData)
	}
	restoreData := after
	endIdx := strings.LastIndex(restoreData, "\r\n")
	if endIdx > 0 {
		restoreData = restoreData[:endIdx]
	}

	result := execCmd(h, conn, shared.CmdFunction, "RESTORE", restoreData)
	if !strings.Contains(result, "OK") {
		t.Errorf("expected OK for restore, got: %s", result)
	}

	// Verify restored
	listResult = execCmd(h, conn, shared.CmdFunction, "LIST")
	if !strings.Contains(listResult, "dumplib") {
		t.Errorf("expected 'dumplib' in list after restore, got: %s", listResult)
	}
}

func TestFcallUnknownCommand(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	conn := func() *shared.Connection { c := newEffectsConn(h); c.Protocol = shared.RESP2; return c }()

	// Load a library that calls an unknown command
	libCode := `#!lua name=test
redis.register_function('test', function(KEYS, ARGV)
    redis.call('nosuchcommand')
end)`

	loadResult := execCmd(h, conn, shared.CmdFunction, "LOAD", libCode)
	t.Logf("FUNCTION LOAD result: %q", loadResult)
	if !strings.Contains(loadResult, "test") {
		t.Fatalf("FUNCTION LOAD failed: %s", loadResult)
	}

	result := execCmd(h, conn, shared.CmdFcall, "test", "0")
	t.Logf("FCALL result: %q", result)

	if !strings.HasPrefix(result, "-") {
		t.Errorf("expected RESP error (starting with '-'), got: %q", result)
	}
	if !strings.Contains(result, "Unknown") {
		t.Errorf("expected error to contain 'Unknown', got: %q", result)
	}
}
