package redis

import (
	"strings"
	"testing"

	"github.com/swytchdb/swytch/redis/shared"
)

func TestConfigSafetyBackingKeys(t *testing.T) {
	h := newHandlerWithEffects()

	t.Cleanup(func() {
		h.Close()

		if err := h.engine.Close(); err != nil {
			t.Error(err)
		}
	})

	c := newEffectsConn(h)
	if got := execCmd(h, c, shared.CmdSet, "__swytch::reserved", "unsafe"); !strings.HasPrefix(got, "-ERR cannot write to internal key") {
		t.Fatalf("internal key protection lost: %s", got)
	}

	if got := execCmd(h, c, shared.CmdConfig, "SET", "cluster_safe_mode", "unsafe", "key_safe_mode", `{"sessions::*":"safe","sessions::public*":"unsafe"}`); got != "+OK\r\n" {
		t.Fatal(got)
	}

	if got := execCmd(h, c, shared.CmdGet, "__swytch::dc"); got != "$6\r\nunsafe\r\n" {
		t.Fatal(got)
	}

	if got := execCmd(h, c, shared.CmdSet, "__swytch::dc", "safe"); got != "+OK\r\n" {
		t.Fatal(got)
	}

	if got := execCmd(h, c, shared.CmdConfig, "GET", "cluster_safe_mode"); !strings.Contains(got, "$4\r\nsafe\r\n") {
		t.Fatalf("direct SET not reflected: %s", got)
	}

	if got := execCmd(h, c, shared.CmdHGet, "__swytch::kc", "sessions::*"); got != "$4\r\nsafe\r\n" {
		t.Fatal(got)
	}

	if got := execCmd(h, c, shared.CmdConfig, "SET", "key_safe_mode", `{"orders:*":"safe"}`); got != "+OK\r\n" {
		t.Fatal(got)
	}

	if got := execCmd(h, c, shared.CmdHGet, "__swytch::kc", "sessions::*"); got != "$-1\r\n" {
		t.Fatalf("replacement retained old field: %s", got)
	}

	if got := execCmd(h, c, shared.CmdHSet, "__swytch::kc", "direct:*", "unsafe"); got != ":1\r\n" {
		t.Fatal(got)
	}

	if got := execCmd(h, c, shared.CmdConfig, "GET", "key_safe_mode"); !strings.Contains(got, `{"direct:*":"unsafe","orders:*":"safe"}`) {
		t.Fatal(got)
	}

	if got := execCmd(h, c, shared.CmdConfig, "SET", "key_safe_mode", `{}`); got != "+OK\r\n" {
		t.Fatal(got)
	}

	if got := execCmd(h, c, shared.CmdConfig, "GET", "key_safe_mode"); !strings.Contains(got, "{}") {
		t.Fatal(got)
	}

	if got := execCmd(h, c, shared.CmdDel, "__swytch::dc"); got != ":1\r\n" {
		t.Fatal(got)
	}

	if got := execCmd(h, c, shared.CmdConfig, "GET", "cluster_safe_mode"); !strings.Contains(got, "$4\r\nsafe\r\n") {
		t.Fatal(got)
	}
}

func TestConfigSafetyValidationBeforeWrites(t *testing.T) {
	h := newHandlerWithEffects()

	t.Cleanup(func() {
		h.Close()

		if err := h.engine.Close(); err != nil {
			t.Error(err)
		}
	})

	c := newEffectsConn(h)

	for _, value := range []string{`null`, `[]`, `"safe"`, `{"x":"typo"}`, `{"x":true}`} {
		got := execCmd(h, c, shared.CmdConfig, "SET", "cluster_safe_mode", "unsafe", "key_safe_mode", value)
		if !strings.HasPrefix(got, "-ERR") {
			t.Fatalf("accepted %s: %s", value, got)
		}

		if got := execCmd(h, c, shared.CmdGet, "__swytch::dc"); got != "$-1\r\n" {
			t.Fatalf("partially applied invalid update: %s", got)
		}
	}

	for _, value := range []string{"SAFE", "true", "garbage", ""} {
		if got := execCmd(h, c, shared.CmdConfig, "SET", "cluster_safe_mode", value); !strings.HasPrefix(got, "-ERR") {
			t.Fatalf("accepted %q: %s", value, got)
		}
	}
}
