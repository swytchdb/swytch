package redis

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/swytchdb/swytch/redis/shared"
)

func TestScriptTimeoutCommandGuard(t *testing.T) {
	for _, killCommand := range []shared.CommandType{shared.CmdScript, shared.CmdFunction} {
		t.Run(killCommand.String(), func(t *testing.T) {
			h := newHandlerWithEffects()
			defer h.Close()

			h.scriptEngine.SetScriptTimeout(time.Millisecond)
			scriptConn := newEffectsConn(h)
			controlConn := newEffectsConn(h)
			done := make(chan string, 1)

			go func() {
				done <- execCmd(h, scriptConn, shared.CmdEval, "while true do end", "0")
			}()

			defer func() {
				// Bypass the guard during cleanup so a regression cannot leave Lua running.
				h.scriptEngine.HandleScript(&shared.Command{Args: [][]byte{[]byte("KILL")}}, shared.NewWriter(&bytes.Buffer{}))

				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Error("script did not stop after KILL")
				}
			}()

			deadline := time.Now().Add(5 * time.Second)
			for !h.scriptEngine.IsScriptTimedOut() {
				if time.Now().After(deadline) {
					t.Fatal("script did not enter BUSY state")
				}

				time.Sleep(time.Millisecond)
			}

			for _, tc := range []struct {
				name string
				cmd  shared.CommandType
				args []string
			}{
				{"script without subcommand", shared.CmdScript, nil},
				{"script load", shared.CmdScript, []string{"LOAD", "return 1"}},
				{"script flush", shared.CmdScript, []string{"FLUSH"}},
				{"function without subcommand", shared.CmdFunction, nil},
				{"function list", shared.CmdFunction, []string{"LIST"}},
				{"function flush", shared.CmdFunction, []string{"FLUSH"}},
				{"ping", shared.CmdPing, nil},
			} {
				t.Run(tc.name, func(t *testing.T) {
					if got := execCmd(h, controlConn, tc.cmd, tc.args...); !strings.HasPrefix(got, "-BUSY ") {
						t.Errorf("expected BUSY, got %q", got)
					}
				})
			}

			if got := execCmd(h, controlConn, shared.CmdMulti); got != "+OK\r\n" {
				t.Fatalf("MULTI: %q", got)
			}

			if got := execCmd(h, controlConn, shared.CmdScript, "FLUSH"); !strings.HasPrefix(got, "-BUSY ") {
				t.Errorf("expected BUSY inside transaction, got %q", got)
			}

			if !controlConn.TxnAborted {
				t.Error("BUSY did not abort the transaction")
			}

			if got := execCmd(h, controlConn, shared.CmdExec); !strings.HasPrefix(got, "-EXECABORT ") {
				t.Errorf("expected EXECABORT, got %q", got)
			}

			if got := execCmd(h, controlConn, killCommand, "kIlL"); got != "+OK\r\n" {
				t.Errorf("KILL must remain available during BUSY: %q", got)
			}
		})
	}
}
