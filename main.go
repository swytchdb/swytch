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
	"encoding/json"
	"errors"
	"flag"
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
	cluster.Version = Version
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
		if err := runGenPassphrase(os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "gen-passphrase: %v\n", err)
			os.Exit(1)
		}
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

// runGenPassphrase handles the gen-passphrase subcommand. Without --cloud it
// prints a single random cluster mTLS passphrase. With --cloud it prints a cloud
// connection secret (the master) and the cloud secret derived from it.
func runGenPassphrase(args []string) error {
	fs := flag.NewFlagSet("gen-passphrase", flag.ContinueOnError)
	cloud := fs.Bool("cloud", false, "generate a cloud connection secret and its derived cloud secret")
	asJSON := fs.Bool("json", false, "with --cloud, emit the secrets as a JSON object")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil // -h/--help: usage already printed, a successful invocation
		}
		return err
	}

	if !*cloud {
		if *asJSON {
			return fmt.Errorf("--json requires --cloud")
		}
		pass, err := cluster.GeneratePassphrase()
		if err != nil {
			return err
		}
		fmt.Println(pass)
		return nil
	}

	connectionSecret, err := cluster.GenerateConnectionSecret()
	if err != nil {
		return err
	}
	cloudSecret := cluster.DeriveCloudSecret(connectionSecret)

	if *asJSON {
		out, err := json.Marshal(struct {
			CloudSecret      string `json:"cloud_secret"`
			ConnectionSecret string `json:"connection_secret"`
		}{cloudSecret, connectionSecret})
		if err != nil {
			return err
		}
		fmt.Println(string(out))
		return nil
	}

	fmt.Printf("Cloud secret (enter during onboarding): %s\n", cloudSecret)
	fmt.Printf("Connection secret (start with --cloud): %s\n", connectionSecret)
	return nil
}

func printUsage() {
	fmt.Println(`swytch - High-performance distributed caching database

Usage: swytch <command> [options]

Commands:
  redis            Start a Redis-compatible server
  sql              Start a SQL (pg wire / SQLite dialect) server
  gen-passphrase   Generate a cluster mTLS passphrase (use --cloud for cloud secrets)
  help             Show this help message
  version          Show version information

Examples:
  swytch redis --port 6379 --maxmemory 256mb
  swytch redis --persistent --db-path /data/redis.db
  swytch sql --listen :5433
  swytch gen-passphrase
  swytch gen-passphrase --cloud
  swytch gen-passphrase --cloud --json

Run 'swytch <command> -h' for more information on a command.`)
}
