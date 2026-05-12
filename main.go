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

package main

import (
	"fmt"
	"os"

	"github.com/swytchdb/swytch/cluster"
	"github.com/swytchdb/swytch/redis"
	"github.com/swytchdb/swytch/sql"
)

// Version is set at build time via ldflags:
//
//	go build -ldflags "-X main.Version=1.2.3"
var Version = "dev"

func init() {
	redis.Version = Version
	sql.Version = Version
}

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "redis":
		if err := redis.Run(os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "redis: %v\n", err)
			os.Exit(1)
		}
	case "sql":
		if err := sql.Run(os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "sql: %v\n", err)
			os.Exit(1)
		}
	case "gen-passphrase":
		pass, err := cluster.GeneratePassphrase()
		if err != nil {
			fmt.Fprintf(os.Stderr, "gen-passphrase: %v\n", err)
			os.Exit(1)
		}
		fmt.Println(pass)
	case "-h", "--help", "help":
		printUsage()
	case "-v", "--version", "version":
		fmt.Printf("swytch %s\n", Version)
	default:
		// For backwards compatibility, assume redis if no subcommand
		if err := redis.Run(os.Args[1:]); err != nil {
			fmt.Fprintf(os.Stderr, "redis: %v\n", err)
			os.Exit(1)
		}
	}
}

func printUsage() {
	fmt.Println(`swytch - High-performance distributed caching database

Usage: swytch <command> [options]

Commands:
  redis            Start a Redis-compatible server
  sql              Start a SQL (pg wire / SQLite dialect) server
  gen-passphrase   Generate a cluster mTLS passphrase
  help             Show this help message
  version          Show version information

Examples:
  swytch redis --port 6379 --maxmemory 256mb
  swytch redis --persistent --db-path /data/redis.db
  swytch sql --listen :5433
  swytch gen-passphrase

Run 'swytch <command> -h' for more information on a command.`)
}
