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

package beacon

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
)

// ResolveJoinAddr resolves a DNS name into cluster peer addresses.
// Strategy: SRV first (returns host:port directly), then A/AAAA fallback
// (combined with defaultPort). Returns nil, nil if no records found.
//
// If joinAddr contains commas or is already a host:port literal, the
// addresses are returned directly without DNS resolution.
func ResolveJoinAddr(ctx context.Context, resolver *net.Resolver, joinAddr string, defaultPort int) ([]string, error) {
	if addrs, ok := parseLiteralAddrs(joinAddr, defaultPort); ok {
		return addrs, nil
	}

	if resolver == nil {
		resolver = net.DefaultResolver
	}

	// Try SRV first — returns host:port tuples directly.
	addrs, err := resolveSRV(ctx, resolver, joinAddr)
	if err == nil && len(addrs) > 0 {
		return addrs, nil
	}

	// Fall back to A/AAAA records + defaultPort.
	addrs, err = resolveHost(ctx, resolver, joinAddr, defaultPort)
	if err != nil {
		return nil, fmt.Errorf("dns resolution failed for %q: %w", joinAddr, err)
	}
	return addrs, nil
}

// parseLiteralAddrs detects comma-separated host:port literals in joinAddr
// and returns them directly. A bare IP (no port) gets defaultPort appended.
// Returns ok=true if joinAddr looks like literal addresses (contains a comma
// or is a single host:port / IP literal), false if it should go through DNS.
func parseLiteralAddrs(joinAddr string, defaultPort int) ([]string, bool) {
	parts := strings.Split(joinAddr, ",")
	if len(parts) == 1 {
		// Single value — only treat as literal if it has an explicit port
		// or is an IP address. Bare hostnames go through DNS.
		host, _, err := net.SplitHostPort(joinAddr)
		if err != nil {
			// No port — could be a bare IP or a hostname.
			if net.ParseIP(joinAddr) != nil {
				return []string{net.JoinHostPort(joinAddr, strconv.Itoa(defaultPort))}, true
			}
			return nil, false
		}
		if net.ParseIP(host) != nil {
			return []string{joinAddr}, true
		}
		return nil, false
	}

	// Multiple comma-separated values — always literal.
	addrs := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		_, _, err := net.SplitHostPort(p)
		if err != nil {
			// No port — append default.
			p = net.JoinHostPort(p, strconv.Itoa(defaultPort))
		}
		addrs = append(addrs, p)
	}
	return addrs, true
}

// resolveSRV queries SRV records on the provided name and resolves each
// target hostname to IP addresses. Returns host:port strings.
func resolveSRV(ctx context.Context, resolver *net.Resolver, name string) ([]string, error) {
	_, srvs, err := resolver.LookupSRV(ctx, "", "", name)
	if err != nil {
		return nil, err
	}

	var addrs []string
	for _, srv := range srvs {
		port := strconv.Itoa(int(srv.Port))
		// SRV targets may be hostnames — resolve to IPs.
		ips, err := resolver.LookupHost(ctx, srv.Target)
		if err != nil {
			// Skip unresolvable targets.
			continue
		}
		for _, ip := range ips {
			addrs = append(addrs, net.JoinHostPort(ip, port))
		}
	}
	return addrs, nil
}

// resolveHost queries A/AAAA records and combines with the given port.
func resolveHost(ctx context.Context, resolver *net.Resolver, name string, port int) ([]string, error) {
	ips, err := resolver.LookupHost(ctx, name)
	if err != nil {
		return nil, err
	}

	portStr := strconv.Itoa(port)
	addrs := make([]string, len(ips))
	for i, ip := range ips {
		addrs[i] = net.JoinHostPort(ip, portStr)
	}
	return addrs, nil
}
