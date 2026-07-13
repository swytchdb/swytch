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
	"bytes"
	"log/slog"
	"os"
	"strings"

	"github.com/swytchdb/swytch/redis/shared"
)

// Logger wraps slog.Logger and provides debug mode control
type Logger struct {
	*slog.Logger
	debug bool
}

// NewLogger creates a new logger with the specified debug mode
func NewLogger(debug bool) *Logger {
	var handler slog.Handler
	opts := &slog.HandlerOptions{}

	if debug {
		opts.Level = slog.LevelDebug
	} else {
		opts.Level = slog.LevelInfo
	}

	handler = slog.NewTextHandler(os.Stderr, opts)

	return &Logger{
		Logger: slog.New(handler),
		debug:  debug,
	}
}

// IsDebug returns true if debug mode is enabled
func (l *Logger) IsDebug() bool {
	return l != nil && l.debug
}

// LogCommand logs a command execution at debug level
func (l *Logger) LogCommand(cmd *shared.Command, clientAddr string) {
	if l == nil || !l.debug {
		return
	}

	// Build args representation - show all args
	var argsPreview strings.Builder
	for i, arg := range cmd.Args {
		if i > 0 {
			argsPreview.WriteString(" ")
		}
		argsPreview.WriteString(string(arg))
	}

	cmdName := cmd.Type.String()
	if cmd.Type == shared.CmdUnknown && len(cmd.RawName) > 0 {
		cmdName = string(cmd.RawName)
	}

	l.Debug("command",
		slog.String("cmd", cmdName),
		slog.String("client", clientAddr),
		slog.String("args", argsPreview.String()),
		slog.Int("argc", len(cmd.Args)),
	)
}

// LogUnknownCommand logs an unknown command at info level (always visible)
func (l *Logger) LogUnknownCommand(cmd *shared.Command, clientAddr string) {
	if l == nil {
		return
	}

	cmdName := "unknown"
	if len(cmd.RawName) > 0 {
		cmdName = string(cmd.RawName)
	}

	l.Info("unknown command",
		slog.String("cmd", cmdName),
		slog.String("client", clientAddr),
		slog.Int("argc", len(cmd.Args)),
	)
}

// LogResponse logs response data at debug level
func (l *Logger) LogResponse(clientAddr string, buf *bytes.Buffer) {
	if l == nil || !l.debug {
		return
	}

	data := buf.Bytes()
	preview := string(data)
	// Escape newlines for readability
	preview = strings.ReplaceAll(preview, "\r\n", "\\r\\n")

	l.Debug("response",
		slog.String("client", clientAddr),
		slog.Int("len", len(data)),
		slog.String("data", preview),
	)
}
