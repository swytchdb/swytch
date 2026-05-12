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

package pubsub

import (
	"strings"

	"github.com/swytchdb/swytch/redis/shared"
)

// handleSubscribe handles the SUBSCRIBE command
func handleSubscribe(cmd *shared.Command, w *shared.Writer, conn *shared.Connection) {
	if len(cmd.Args) < 1 {
		w.WriteWrongNumArguments("subscribe")
		return
	}

	broker := shared.GetPubSubBroker()

	// Create PubSubClient if not already in pub/sub mode
	if conn.PubSubClient == nil {
		conn.PubSubClient = shared.NewPubSubClient(conn, conn.Protocol)
	}

	// Convert args to channel names
	channels := make([]string, len(cmd.Args))
	for i, arg := range cmd.Args {
		channels[i] = string(arg)
	}

	// Subscribe to channels
	counts := broker.Subscribe(conn.PubSubClient, channels...)

	// Send confirmation messages for each channel
	for i, channel := range channels {
		msg := &shared.PubSubMessage{
			Type:    "subscribe",
			Channel: channel,
			Count:   counts[i],
		}
		w.WriteRaw(FormatPubSubMessage(msg, conn.Protocol))
	}
}

// handleUnsubscribe handles the UNSUBSCRIBE command
func handleUnsubscribe(cmd *shared.Command, w *shared.Writer, conn *shared.Connection) {
	broker := shared.GetPubSubBroker()

	// If not in pub/sub mode, just return empty unsubscribe
	if conn.PubSubClient == nil {
		if len(cmd.Args) == 0 {
			msg := &shared.PubSubMessage{
				Type:  "unsubscribe",
				Count: 0,
			}
			w.WriteRaw(FormatPubSubMessage(msg, conn.Protocol))
			return
		}
		for _, arg := range cmd.Args {
			msg := &shared.PubSubMessage{
				Type:    "unsubscribe",
				Channel: string(arg),
				Count:   0,
			}
			w.WriteRaw(FormatPubSubMessage(msg, conn.Protocol))
		}
		return
	}

	// Convert args to channel names
	var channels []string
	if len(cmd.Args) > 0 {
		channels = make([]string, len(cmd.Args))
		for i, arg := range cmd.Args {
			channels[i] = string(arg)
		}
	}

	// Unsubscribe from channels
	names, counts := broker.Unsubscribe(conn.PubSubClient, channels...)

	// If no channels were specified and none subscribed
	if len(names) == 0 {
		msg := &shared.PubSubMessage{
			Type:  "unsubscribe",
			Count: broker.SubscriptionCount(conn.PubSubClient),
		}
		w.WriteRaw(FormatPubSubMessage(msg, conn.Protocol))
	} else {
		for i, name := range names {
			msg := &shared.PubSubMessage{
				Type:    "unsubscribe",
				Channel: name,
				Count:   counts[i],
			}
			w.WriteRaw(FormatPubSubMessage(msg, conn.Protocol))
		}
	}

	// Exit pub/sub mode if no more subscriptions
	if broker.SubscriptionCount(conn.PubSubClient) == 0 {
		broker.Cleanup(conn.PubSubClient)
		conn.PubSubClient = nil
	}
}

// handlePSubscribe handles the PSUBSCRIBE command
func handlePSubscribe(cmd *shared.Command, w *shared.Writer, conn *shared.Connection) {
	if len(cmd.Args) < 1 {
		w.WriteWrongNumArguments("psubscribe")
		return
	}

	broker := shared.GetPubSubBroker()

	// Create PubSubClient if not already in pub/sub mode
	if conn.PubSubClient == nil {
		conn.PubSubClient = shared.NewPubSubClient(conn, conn.Protocol)
	}

	// Convert args to pattern names
	patterns := make([]string, len(cmd.Args))
	for i, arg := range cmd.Args {
		patterns[i] = string(arg)
	}

	// Subscribe to patterns
	counts := broker.PSubscribe(conn.PubSubClient, patterns...)

	// Send confirmation messages for each pattern
	for i, pattern := range patterns {
		msg := &shared.PubSubMessage{
			Type:    "psubscribe",
			Pattern: pattern,
			Count:   counts[i],
		}
		w.WriteRaw(FormatPubSubMessage(msg, conn.Protocol))
	}
}

// handlePUnsubscribe handles the PUNSUBSCRIBE command
func handlePUnsubscribe(cmd *shared.Command, w *shared.Writer, conn *shared.Connection) {
	broker := shared.GetPubSubBroker()

	// If not in pub/sub mode, just return empty punsubscribe
	if conn.PubSubClient == nil {
		if len(cmd.Args) == 0 {
			msg := &shared.PubSubMessage{
				Type:  "punsubscribe",
				Count: 0,
			}
			w.WriteRaw(FormatPubSubMessage(msg, conn.Protocol))
			return
		}
		for _, arg := range cmd.Args {
			msg := &shared.PubSubMessage{
				Type:    "punsubscribe",
				Pattern: string(arg),
				Count:   0,
			}
			w.WriteRaw(FormatPubSubMessage(msg, conn.Protocol))
		}
		return
	}

	// Convert args to pattern names
	var patterns []string
	if len(cmd.Args) > 0 {
		patterns = make([]string, len(cmd.Args))
		for i, arg := range cmd.Args {
			patterns[i] = string(arg)
		}
	}

	// Unsubscribe from patterns
	names, counts := broker.PUnsubscribe(conn.PubSubClient, patterns...)

	// If no patterns were specified and none subscribed
	if len(names) == 0 {
		msg := &shared.PubSubMessage{
			Type:  "punsubscribe",
			Count: broker.SubscriptionCount(conn.PubSubClient),
		}
		w.WriteRaw(FormatPubSubMessage(msg, conn.Protocol))
	} else {
		for i, name := range names {
			msg := &shared.PubSubMessage{
				Type:    "punsubscribe",
				Pattern: name,
				Count:   counts[i],
			}
			w.WriteRaw(FormatPubSubMessage(msg, conn.Protocol))
		}
	}

	// Exit pub/sub mode if no more subscriptions
	if broker.SubscriptionCount(conn.PubSubClient) == 0 {
		broker.Cleanup(conn.PubSubClient)
		conn.PubSubClient = nil
	}
}

// handlePublish handles the PUBLISH command
func handlePublish(cmd *shared.Command, w *shared.Writer, _ *shared.Database) (bool, []string, shared.CommandRunner) {
	if len(cmd.Args) != 2 {
		w.WriteWrongNumArguments("publish")
		return false, nil, nil
	}

	return true, nil, func() {
		channel := string(cmd.Args[0])
		message := cmd.Args[1]

		count := shared.GetPubSubBroker().Publish(channel, message)
		w.WriteInteger(int64(count))
	}
}

// handlePubSub handles the PUBSUB command
func handlePubSub(cmd *shared.Command, w *shared.Writer) {
	if len(cmd.Args) < 1 {
		w.WriteWrongNumArguments("pubsub")
		return
	}

	subCommand := strings.ToUpper(string(cmd.Args[0]))

	switch subCommand {
	case "CHANNELS":
		handlePubSubChannels(cmd, w)
	case "NUMSUB":
		handlePubSubNumSub(cmd, w)
	case "NUMPAT":
		handlePubSubNumPat(cmd, w)
	case "HELP":
		handlePubSubHelp(cmd, w)
	default:
		w.WriteError("ERR Unknown PUBSUB subcommand '" + subCommand + "'")
	}
}

// handlePubSubChannels handles PUBSUB CHANNELS [pattern]
func handlePubSubChannels(cmd *shared.Command, w *shared.Writer) {
	var pattern string
	if len(cmd.Args) > 1 {
		pattern = string(cmd.Args[1])
	}

	channels := shared.GetPubSubBroker().Channels(pattern)

	w.WriteArray(len(channels))
	for _, channel := range channels {
		w.WriteBulkStringStr(channel)
	}
}

// handlePubSubNumSub handles PUBSUB NUMSUB [channel ...]
func handlePubSubNumSub(cmd *shared.Command, w *shared.Writer) {
	if len(cmd.Args) < 2 {
		w.WriteArray(0)
		return
	}

	channels := make([]string, len(cmd.Args)-1)
	for i, arg := range cmd.Args[1:] {
		channels[i] = string(arg)
	}

	counts := shared.GetPubSubBroker().NumSub(channels...)

	w.WriteArray(len(channels) * 2)
	for _, channel := range channels {
		w.WriteBulkStringStr(channel)
		w.WriteInteger(int64(counts[channel]))
	}
}

// handlePubSubNumPat handles PUBSUB NUMPAT
func handlePubSubNumPat(_ *shared.Command, w *shared.Writer) {
	count := shared.GetPubSubBroker().NumPat()
	w.WriteInteger(int64(count))
}

// handlePubSubHelp handles PUBSUB HELP
func handlePubSubHelp(_ *shared.Command, w *shared.Writer) {
	help := []string{
		"PUBSUB <subcommand> [<arg> [value] [opt] ...]",
		"CHANNELS [<pattern>]",
		"    Return the currently active channels matching a <pattern> (default: '*').",
		"NUMSUB [<channel> ...]",
		"    Return the number of subscribers for the specified channels, excluding",
		"    pattern subscriptions.",
		"NUMPAT",
		"    Return the number of unique patterns that are subscribed to.",
		"HELP",
		"    Print this help.",
	}

	w.WriteArray(len(help))
	for _, line := range help {
		w.WriteBulkStringStr(line)
	}
}
