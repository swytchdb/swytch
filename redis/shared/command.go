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
	"unsafe"

	"github.com/swytchdb/swytch/effects"
)

// CommandType represents the type of Redis command
type CommandType int

const (
	CmdUnknown CommandType = iota
	CmdNoop

	// Connection commands
	CmdPing
	CmdEcho
	CmdAuth
	CmdSelect
	CmdQuit
	CmdHello

	// String commands
	CmdGet
	CmdSet
	CmdDel
	CmdDelEx
	CmdExists
	CmdExpire
	CmdExpireAt
	CmdPExpire
	CmdPExpireAt
	CmdTTL
	CmdPTTL
	CmdExpireTime
	CmdPExpireTime
	CmdPersist
	CmdRename
	CmdRenameNX
	CmdIncr
	CmdDecr
	CmdIncrBy
	CmdDecrBy
	CmdIncrByFloat
	CmdAppend
	CmdGetSet
	CmdGetDel
	CmdGetEx
	CmdMGet
	CmdMSet
	CmdMSetNX
	CmdMSetEX
	CmdSetNX
	CmdSetEX
	CmdPSetEX
	CmdStrLen
	CmdSetRange
	CmdGetRange
	CmdSubstr // Alias for GETRANGE (deprecated)
	CmdLcs    // Longest Common Subsequence
	CmdDigest // XXH3 hash digest of string value
	CmdType

	// Bitmap commands
	CmdSetBit
	CmdGetBit
	CmdBitCount
	CmdBitPos
	CmdBitOp
	CmdBitField
	CmdBitFieldRO

	// List commands
	CmdLPush
	CmdLPushX
	CmdRPush
	CmdRPushX
	CmdLPop
	CmdRPop
	CmdBLPop
	CmdBRPop
	CmdLMPop
	CmdBLMPop
	CmdLMove
	CmdBLMove
	CmdRPopLPush
	CmdBRPopLPush
	CmdLLen
	CmdLRange
	CmdLIndex
	CmdLSet
	CmdLRem
	CmdLTrim
	CmdLInsert
	CmdLPos

	// Hash commands
	CmdHGet
	CmdHSet
	CmdHSetNX
	CmdHDel
	CmdHExists
	CmdHGetAll
	CmdHKeys
	CmdHVals
	CmdHLen
	CmdHMGet
	CmdHMSet
	CmdHIncrBy
	CmdHIncrByFloat
	CmdHStrLen
	CmdHRandField
	CmdHScan
	CmdHGetDel
	CmdHExpire
	CmdHPExpire
	CmdHExpireAt
	CmdHPExpireAt
	CmdHTTL
	CmdHPTTL
	CmdHExpireTime
	CmdHPExpireTime
	CmdHPersist
	CmdHSetEx
	CmdHGetEx

	// Set commands
	CmdSAdd
	CmdSCard
	CmdSIsMember
	CmdSMIsMember
	CmdSMembers
	CmdSPop
	CmdSRem
	CmdSRandMember
	CmdSInter
	CmdSInterStore
	CmdSInterCard
	CmdSUnion
	CmdSUnionStore
	CmdSDiff
	CmdSDiffStore
	CmdSMove

	// Sorted set commands
	CmdZAdd
	CmdZRem
	CmdZScore
	CmdZCard
	CmdZRank
	CmdZRevRank
	CmdZRange
	CmdZRevRange
	CmdZRangeByScore
	CmdZRevRangeByScore
	CmdZCount
	CmdZIncrBy
	CmdZPopMin
	CmdZPopMax
	CmdZMPop
	CmdZMScore
	CmdBZPopMin
	CmdBZPopMax
	CmdBZMPop
	CmdZRangeByLex
	CmdZRevRangeByLex
	CmdZLexCount
	CmdZRemRangeByScore
	CmdZRemRangeByRank
	CmdZRemRangeByLex
	CmdZUnionStore
	CmdZInterStore
	CmdZUnion
	CmdZInter
	CmdZInterCard
	CmdZDiff
	CmdZDiffStore
	CmdZRangeStore
	CmdZRandMember

	// Generic commands
	CmdCopy
	CmdMove
	CmdRandomKey
	CmdSort
	CmdSortRO

	// Server commands
	CmdInfo
	CmdDBSize
	CmdFlushDB
	CmdFlushAll
	CmdTime
	CmdKeys
	CmdScan
	CmdCommand
	CmdConfig
	CmdClient
	CmdDebug
	CmdMemory
	CmdShutdown

	// Transaction commands
	CmdMulti
	CmdExec
	CmdDiscard
	CmdWatch
	CmdUnwatch

	// Scripting commands
	CmdFunction
	CmdScript
	CmdEval
	CmdEvalSha
	CmdEvalRO
	CmdEvalShaRO
	CmdFcall
	CmdFcallRO

	// Replication commands (stubbed - replication not supported)
	CmdSync
	CmdPSync
	CmdReplConf
	CmdWait
	CmdWaitAof

	// Pub/Sub commands
	CmdSubscribe
	CmdUnsubscribe
	CmdPSubscribe
	CmdPUnsubscribe
	CmdPublish
	CmdPubSub

	// Database commands
	CmdSwapDB

	// Stream commands
	CmdXAdd
	CmdXLen
	CmdXRange
	CmdXRevRange
	CmdXRead
	CmdXGroup
	CmdXReadGroup
	CmdXAck
	CmdXPending
	CmdXTrim
	CmdXDel
	CmdXInfo
	CmdXSetId
	CmdXDelEx
	CmdXAckDel
	CmdXClaim
	CmdXAutoClaim
	CmdXIdmpRecord
	CmdXCfgSet

	// HyperLogLog commands
	CmdPFAdd
	CmdPFCount
	CmdPFMerge
	CmdPFSelfTest
	CmdPFDebug

	// Geo commands
	CmdGeoAdd
	CmdGeoDist
	CmdGeoHash
	CmdGeoPos
	CmdGeoRadius
	CmdGeoRadiusRO
	CmdGeoRadiusByMember
	CmdGeoRadiusByMemberRO
	CmdGeoSearch
	CmdGeoSearchStore

	// ACL commands
	CmdAcl

	// RedisJSON (JSON.*) commands
	CmdJSONSet
	CmdJSONGet
	CmdJSONMGet
	CmdJSONMSet
	CmdJSONMerge
	CmdJSONDel
	CmdJSONForget
	CmdJSONClear
	CmdJSONType

	// CmdMax is a sentinel marking the end of the CommandType enum.
	// Must remain last. Used to size the registry array.
	CmdMax
)

// CommandRunner is a closure that executes a pre-validated command.
type CommandRunner func()

// HandlerFunc is the standard handler signature: validate a command, return keys and a runner.
type HandlerFunc func(cmd *Command, w *Writer, db *Database) (valid bool, keys []string, runner CommandRunner)

// ConnHandlerFunc is for commands that need connection state (blocking, auth, transactions, pubsub, scripting).
type ConnHandlerFunc func(cmd *Command, w *Writer, db *Database, conn *Connection)

// Command represents a parsed Redis command
type Command struct {
	Type        CommandType
	Args        [][]byte // All arguments after the command name
	RawName     []byte   // Original command name (preserved for unknown commands)
	Transaction any
	Context     *effects.Context
	Runtime     *effects.Engine
	keyScratch  [1]string
}

// Reset clears the command for reuse, preserving slice capacity
func (c *Command) Reset() {
	// Return data buffers to pool
	for _, arg := range c.Args {
		if arg != nil {
			PutDataBuf(arg)
		}
	}
	clear(c.Args[:cap(c.Args)])
	if c.RawName != nil {
		PutDataBuf(c.RawName)
	}
	// Zero all fields while preserving Args capacity
	args := c.Args[:0]
	*c = Command{Args: args}
}

// SetSingleKey records arg as this command's one-key scratch value without
// copying the pooled parser bytes. The returned string and SingleKeySlice are
// valid only until the command is reset or returned to the pool. Code that
// retains a key beyond command execution must clone it first.
func (c *Command) SetSingleKey(arg []byte) string {
	key := unsafe.String(unsafe.SliceData(arg), len(arg))
	c.keyScratch[0] = key
	return key
}

// SingleKeyValue returns the value installed by SetSingleKey.
func (c *Command) SingleKeyValue() string {
	return c.keyScratch[0]
}

// SingleKeySlice returns allocation-free one-key scratch storage.
func (c *Command) SingleKeySlice() []string {
	return c.keyScratch[:]
}

// LookupCommandName returns the CommandType for a given uppercase command name.
// Returns CmdUnknown and false if the name is not recognized.
func LookupCommandName(name string) (CommandType, bool) {
	cmd, ok := commandNames[name]
	return cmd, ok
}

// CommandCount returns the number of registered command names.
func CommandCount() int {
	return len(commandNames)
}

// CommandNamesList calls fn for each registered command name (uppercase) and its type.
// If fn returns false, iteration stops.
func CommandNamesList(fn func(name string, cmd CommandType) bool) {
	for name, cmd := range commandNames {
		if !fn(name, cmd) {
			return
		}
	}
}

// commandNames maps command names (uppercase) to CommandType
var commandNames = map[string]CommandType{
	// Connection
	"PING":   CmdPing,
	"ECHO":   CmdEcho,
	"AUTH":   CmdAuth,
	"SELECT": CmdSelect,
	"QUIT":   CmdQuit,
	"HELLO":  CmdHello,

	// Strings
	"GET":         CmdGet,
	"SET":         CmdSet,
	"DEL":         CmdDel,
	"DELEX":       CmdDelEx,
	"EXISTS":      CmdExists,
	"EXPIRE":      CmdExpire,
	"EXPIREAT":    CmdExpireAt,
	"PEXPIRE":     CmdPExpire,
	"PEXPIREAT":   CmdPExpireAt,
	"TTL":         CmdTTL,
	"PTTL":        CmdPTTL,
	"EXPIRETIME":  CmdExpireTime,
	"PEXPIRETIME": CmdPExpireTime,
	"PERSIST":     CmdPersist,
	"RENAME":      CmdRename,
	"RENAMENX":    CmdRenameNX,
	"INCR":        CmdIncr,
	"DECR":        CmdDecr,
	"INCRBY":      CmdIncrBy,
	"DECRBY":      CmdDecrBy,
	"INCRBYFLOAT": CmdIncrByFloat,
	"APPEND":      CmdAppend,
	"GETSET":      CmdGetSet,
	"GETDEL":      CmdGetDel,
	"GETEX":       CmdGetEx,
	"MGET":        CmdMGet,
	"MSET":        CmdMSet,
	"MSETNX":      CmdMSetNX,
	"MSETEX":      CmdMSetEX,
	"SETNX":       CmdSetNX,
	"SETEX":       CmdSetEX,
	"PSETEX":      CmdPSetEX,
	"STRLEN":      CmdStrLen,
	"SETRANGE":    CmdSetRange,
	"GETRANGE":    CmdGetRange,
	"SUBSTR":      CmdSubstr,
	"LCS":         CmdLcs,
	"DIGEST":      CmdDigest,
	"TYPE":        CmdType,

	// Bitmaps
	"SETBIT":      CmdSetBit,
	"GETBIT":      CmdGetBit,
	"BITCOUNT":    CmdBitCount,
	"BITPOS":      CmdBitPos,
	"BITOP":       CmdBitOp,
	"BITFIELD":    CmdBitField,
	"BITFIELD_RO": CmdBitFieldRO,

	// Lists
	"LPUSH":      CmdLPush,
	"LPUSHX":     CmdLPushX,
	"RPUSH":      CmdRPush,
	"RPUSHX":     CmdRPushX,
	"LPOP":       CmdLPop,
	"RPOP":       CmdRPop,
	"BLPOP":      CmdBLPop,
	"BRPOP":      CmdBRPop,
	"LMPOP":      CmdLMPop,
	"BLMPOP":     CmdBLMPop,
	"LMOVE":      CmdLMove,
	"BLMOVE":     CmdBLMove,
	"RPOPLPUSH":  CmdRPopLPush,
	"BRPOPLPUSH": CmdBRPopLPush,
	"LLEN":       CmdLLen,
	"LRANGE":     CmdLRange,
	"LINDEX":     CmdLIndex,
	"LSET":       CmdLSet,
	"LREM":       CmdLRem,
	"LTRIM":      CmdLTrim,
	"LINSERT":    CmdLInsert,
	"LPOS":       CmdLPos,

	// Hashes
	"HGET":         CmdHGet,
	"HSET":         CmdHSet,
	"HSETNX":       CmdHSetNX,
	"HDEL":         CmdHDel,
	"HEXISTS":      CmdHExists,
	"HGETALL":      CmdHGetAll,
	"HKEYS":        CmdHKeys,
	"HVALS":        CmdHVals,
	"HLEN":         CmdHLen,
	"HMGET":        CmdHMGet,
	"HMSET":        CmdHMSet,
	"HINCRBY":      CmdHIncrBy,
	"HINCRBYFLOAT": CmdHIncrByFloat,
	"HSTRLEN":      CmdHStrLen,
	"HRANDFIELD":   CmdHRandField,
	"HSCAN":        CmdHScan,
	"HGETDEL":      CmdHGetDel,
	"HEXPIRE":      CmdHExpire,
	"HPEXPIRE":     CmdHPExpire,
	"HEXPIREAT":    CmdHExpireAt,
	"HPEXPIREAT":   CmdHPExpireAt,
	"HTTL":         CmdHTTL,
	"HPTTL":        CmdHPTTL,
	"HEXPIRETIME":  CmdHExpireTime,
	"HPEXPIRETIME": CmdHPExpireTime,
	"HPERSIST":     CmdHPersist,
	"HSETEX":       CmdHSetEx,
	"HGETEX":       CmdHGetEx,

	// Sets
	"SADD":        CmdSAdd,
	"SCARD":       CmdSCard,
	"SISMEMBER":   CmdSIsMember,
	"SMISMEMBER":  CmdSMIsMember,
	"SMEMBERS":    CmdSMembers,
	"SPOP":        CmdSPop,
	"SREM":        CmdSRem,
	"SRANDMEMBER": CmdSRandMember,
	"SINTER":      CmdSInter,
	"SINTERSTORE": CmdSInterStore,
	"SINTERCARD":  CmdSInterCard,
	"SUNION":      CmdSUnion,
	"SUNIONSTORE": CmdSUnionStore,
	"SDIFF":       CmdSDiff,
	"SDIFFSTORE":  CmdSDiffStore,
	"SMOVE":       CmdSMove,

	// Sorted sets
	"ZADD":             CmdZAdd,
	"ZREM":             CmdZRem,
	"ZSCORE":           CmdZScore,
	"ZCARD":            CmdZCard,
	"ZRANK":            CmdZRank,
	"ZREVRANK":         CmdZRevRank,
	"ZRANGE":           CmdZRange,
	"ZREVRANGE":        CmdZRevRange,
	"ZRANGEBYSCORE":    CmdZRangeByScore,
	"ZREVRANGEBYSCORE": CmdZRevRangeByScore,
	"ZCOUNT":           CmdZCount,
	"ZINCRBY":          CmdZIncrBy,
	"ZPOPMIN":          CmdZPopMin,
	"ZPOPMAX":          CmdZPopMax,
	"ZMPOP":            CmdZMPop,
	"ZMSCORE":          CmdZMScore,
	"BZPOPMIN":         CmdBZPopMin,
	"BZPOPMAX":         CmdBZPopMax,
	"BZMPOP":           CmdBZMPop,
	"ZRANGEBYLEX":      CmdZRangeByLex,
	"ZREVRANGEBYLEX":   CmdZRevRangeByLex,
	"ZLEXCOUNT":        CmdZLexCount,
	"ZREMRANGEBYSCORE": CmdZRemRangeByScore,
	"ZREMRANGEBYRANK":  CmdZRemRangeByRank,
	"ZREMRANGEBYLEX":   CmdZRemRangeByLex,
	"ZUNIONSTORE":      CmdZUnionStore,
	"ZINTERSTORE":      CmdZInterStore,
	"ZUNION":           CmdZUnion,
	"ZINTER":           CmdZInter,
	"ZINTERCARD":       CmdZInterCard,
	"ZDIFF":            CmdZDiff,
	"ZDIFFSTORE":       CmdZDiffStore,
	"ZRANGESTORE":      CmdZRangeStore,
	"ZRANDMEMBER":      CmdZRandMember,

	// Generic
	"COPY":      CmdCopy,
	"MOVE":      CmdMove,
	"RANDOMKEY": CmdRandomKey,
	"SORT":      CmdSort,
	"SORT_RO":   CmdSortRO,

	// Server
	"INFO":     CmdInfo,
	"DBSIZE":   CmdDBSize,
	"FLUSHDB":  CmdFlushDB,
	"FLUSHALL": CmdFlushAll,
	"TIME":     CmdTime,
	"KEYS":     CmdKeys,
	"SCAN":     CmdScan,
	"COMMAND":  CmdCommand,
	"CONFIG":   CmdConfig,
	"CLIENT":   CmdClient,
	"DEBUG":    CmdDebug,
	"MEMORY":   CmdMemory,
	"SHUTDOWN": CmdShutdown,

	// Transactions
	"MULTI":   CmdMulti,
	"EXEC":    CmdExec,
	"DISCARD": CmdDiscard,
	"WATCH":   CmdWatch,
	"UNWATCH": CmdUnwatch,

	// Scripting
	"FUNCTION":   CmdFunction,
	"SCRIPT":     CmdScript,
	"EVAL":       CmdEval,
	"EVALSHA":    CmdEvalSha,
	"EVAL_RO":    CmdEvalRO,
	"EVALSHA_RO": CmdEvalShaRO,
	"FCALL":      CmdFcall,
	"FCALL_RO":   CmdFcallRO,

	// Replication (stubbed)
	"SYNC":     CmdSync,
	"PSYNC":    CmdPSync,
	"REPLCONF": CmdReplConf,
	"WAIT":     CmdWait,
	"WAITAOF":  CmdWaitAof,

	// Pub/Sub
	"SUBSCRIBE":    CmdSubscribe,
	"UNSUBSCRIBE":  CmdUnsubscribe,
	"PSUBSCRIBE":   CmdPSubscribe,
	"PUNSUBSCRIBE": CmdPUnsubscribe,
	"PUBLISH":      CmdPublish,
	"PUBSUB":       CmdPubSub,

	// Database
	"SWAPDB": CmdSwapDB,

	// Streams
	"XADD":        CmdXAdd,
	"XLEN":        CmdXLen,
	"XRANGE":      CmdXRange,
	"XREVRANGE":   CmdXRevRange,
	"XREAD":       CmdXRead,
	"XGROUP":      CmdXGroup,
	"XREADGROUP":  CmdXReadGroup,
	"XACK":        CmdXAck,
	"XPENDING":    CmdXPending,
	"XTRIM":       CmdXTrim,
	"XDEL":        CmdXDel,
	"XINFO":       CmdXInfo,
	"XSETID":      CmdXSetId,
	"XDELEX":      CmdXDelEx,
	"XACKDEL":     CmdXAckDel,
	"XCLAIM":      CmdXClaim,
	"XAUTOCLAIM":  CmdXAutoClaim,
	"XIDMPRECORD": CmdXIdmpRecord,
	"XCFGSET":     CmdXCfgSet,

	// HyperLogLog
	"PFADD":      CmdPFAdd,
	"PFCOUNT":    CmdPFCount,
	"PFMERGE":    CmdPFMerge,
	"PFSELFTEST": CmdPFSelfTest,
	"PFDEBUG":    CmdPFDebug,

	// Geo
	"GEOADD":               CmdGeoAdd,
	"GEODIST":              CmdGeoDist,
	"GEOHASH":              CmdGeoHash,
	"GEOPOS":               CmdGeoPos,
	"GEORADIUS":            CmdGeoRadius,
	"GEORADIUS_RO":         CmdGeoRadiusRO,
	"GEORADIUSBYMEMBER":    CmdGeoRadiusByMember,
	"GEORADIUSBYMEMBER_RO": CmdGeoRadiusByMemberRO,
	"GEOSEARCH":            CmdGeoSearch,
	"GEOSEARCHSTORE":       CmdGeoSearchStore,

	// ACL
	"ACL": CmdAcl,

	// RedisJSON
	"JSON.SET":    CmdJSONSet,
	"JSON.GET":    CmdJSONGet,
	"JSON.MGET":   CmdJSONMGet,
	"JSON.MSET":   CmdJSONMSet,
	"JSON.MERGE":  CmdJSONMerge,
	"JSON.DEL":    CmdJSONDel,
	"JSON.FORGET": CmdJSONForget,
	"JSON.CLEAR":  CmdJSONClear,
	"JSON.TYPE":   CmdJSONType,
}

// commandStrings maps CommandType to command name strings
var commandStrings = map[CommandType]string{
	CmdPing:                "ping",
	CmdEcho:                "echo",
	CmdAuth:                "auth",
	CmdSelect:              "select",
	CmdQuit:                "quit",
	CmdHello:               "hello",
	CmdGet:                 "get",
	CmdSet:                 "set",
	CmdDel:                 "del",
	CmdDelEx:               "delex",
	CmdExists:              "exists",
	CmdExpire:              "expire",
	CmdExpireAt:            "expireat",
	CmdPExpire:             "pexpire",
	CmdPExpireAt:           "pexpireat",
	CmdTTL:                 "ttl",
	CmdPTTL:                "pttl",
	CmdExpireTime:          "expiretime",
	CmdPExpireTime:         "pexpiretime",
	CmdPersist:             "persist",
	CmdRename:              "rename",
	CmdRenameNX:            "renamenx",
	CmdIncr:                "incr",
	CmdDecr:                "decr",
	CmdIncrBy:              "incrby",
	CmdDecrBy:              "decrby",
	CmdIncrByFloat:         "incrbyfloat",
	CmdAppend:              "append",
	CmdGetSet:              "getset",
	CmdGetDel:              "getdel",
	CmdGetEx:               "getex",
	CmdMGet:                "mget",
	CmdMSet:                "mset",
	CmdMSetNX:              "msetnx",
	CmdMSetEX:              "msetex",
	CmdSetNX:               "setnx",
	CmdSetEX:               "setex",
	CmdPSetEX:              "psetex",
	CmdStrLen:              "strlen",
	CmdSetRange:            "setrange",
	CmdGetRange:            "getrange",
	CmdSubstr:              "substr",
	CmdLcs:                 "lcs",
	CmdDigest:              "digest",
	CmdType:                "type",
	CmdSetBit:              "setbit",
	CmdGetBit:              "getbit",
	CmdBitCount:            "bitcount",
	CmdBitPos:              "bitpos",
	CmdBitOp:               "bitop",
	CmdBitField:            "bitfield",
	CmdBitFieldRO:          "bitfield_ro",
	CmdLPush:               "lpush",
	CmdLPushX:              "lpushx",
	CmdRPush:               "rpush",
	CmdRPushX:              "rpushx",
	CmdLPop:                "lpop",
	CmdRPop:                "rpop",
	CmdBLPop:               "blpop",
	CmdBRPop:               "brpop",
	CmdLMPop:               "lmpop",
	CmdBLMPop:              "blmpop",
	CmdLMove:               "lmove",
	CmdBLMove:              "blmove",
	CmdRPopLPush:           "rpoplpush",
	CmdBRPopLPush:          "brpoplpush",
	CmdLLen:                "llen",
	CmdLRange:              "lrange",
	CmdLIndex:              "lindex",
	CmdLSet:                "lset",
	CmdLRem:                "lrem",
	CmdLTrim:               "ltrim",
	CmdLInsert:             "linsert",
	CmdLPos:                "lpos",
	CmdHGet:                "hget",
	CmdHSet:                "hset",
	CmdHSetNX:              "hsetnx",
	CmdHDel:                "hdel",
	CmdHExists:             "hexists",
	CmdHGetAll:             "hgetall",
	CmdHKeys:               "hkeys",
	CmdHVals:               "hvals",
	CmdHLen:                "hlen",
	CmdHMGet:               "hmget",
	CmdHMSet:               "hmset",
	CmdHIncrBy:             "hincrby",
	CmdHIncrByFloat:        "hincrbyfloat",
	CmdHStrLen:             "hstrlen",
	CmdHRandField:          "hrandfield",
	CmdHScan:               "hscan",
	CmdHGetDel:             "hgetdel",
	CmdHExpire:             "hexpire",
	CmdHPExpire:            "hpexpire",
	CmdHExpireAt:           "hexpireat",
	CmdHPExpireAt:          "hpexpireat",
	CmdHTTL:                "httl",
	CmdHPTTL:               "hpttl",
	CmdHExpireTime:         "hexpiretime",
	CmdHPExpireTime:        "hpexpiretime",
	CmdHPersist:            "hpersist",
	CmdHSetEx:              "hsetex",
	CmdHGetEx:              "hgetex",
	CmdSAdd:                "sadd",
	CmdSCard:               "scard",
	CmdSIsMember:           "sismember",
	CmdSMIsMember:          "smismember",
	CmdSMembers:            "smembers",
	CmdSPop:                "spop",
	CmdSRem:                "srem",
	CmdSRandMember:         "srandmember",
	CmdSInter:              "sinter",
	CmdSInterStore:         "sinterstore",
	CmdSInterCard:          "sintercard",
	CmdSUnion:              "sunion",
	CmdSUnionStore:         "sunionstore",
	CmdSDiff:               "sdiff",
	CmdSDiffStore:          "sdiffstore",
	CmdSMove:               "smove",
	CmdZAdd:                "zadd",
	CmdZRem:                "zrem",
	CmdZScore:              "zscore",
	CmdZCard:               "zcard",
	CmdZRank:               "zrank",
	CmdZRevRank:            "zrevrank",
	CmdZRange:              "zrange",
	CmdZRevRange:           "zrevrange",
	CmdZRangeByScore:       "zrangebyscore",
	CmdZRevRangeByScore:    "zrevrangebyscore",
	CmdZCount:              "zcount",
	CmdZIncrBy:             "zincrby",
	CmdZPopMin:             "zpopmin",
	CmdZPopMax:             "zpopmax",
	CmdZMPop:               "zmpop",
	CmdZMScore:             "zmscore",
	CmdBZPopMin:            "bzpopmin",
	CmdBZPopMax:            "bzpopmax",
	CmdBZMPop:              "bzmpop",
	CmdZRangeByLex:         "zrangebylex",
	CmdZRevRangeByLex:      "zrevrangebylex",
	CmdZLexCount:           "zlexcount",
	CmdZRemRangeByScore:    "zremrangebyscore",
	CmdZRemRangeByRank:     "zremrangebyrank",
	CmdZRemRangeByLex:      "zremrangebylex",
	CmdZUnionStore:         "zunionstore",
	CmdZInterStore:         "zinterstore",
	CmdZUnion:              "zunion",
	CmdZInter:              "zinter",
	CmdZInterCard:          "zintercard",
	CmdZDiff:               "zdiff",
	CmdZDiffStore:          "zdiffstore",
	CmdZRangeStore:         "zrangestore",
	CmdZRandMember:         "zrandmember",
	CmdCopy:                "copy",
	CmdMove:                "move",
	CmdRandomKey:           "randomkey",
	CmdSort:                "sort",
	CmdSortRO:              "sort_ro",
	CmdInfo:                "info",
	CmdDBSize:              "dbsize",
	CmdFlushDB:             "flushdb",
	CmdFlushAll:            "flushall",
	CmdTime:                "time",
	CmdKeys:                "keys",
	CmdScan:                "scan",
	CmdCommand:             "command",
	CmdConfig:              "config",
	CmdClient:              "client",
	CmdDebug:               "debug",
	CmdMemory:              "memory",
	CmdShutdown:            "shutdown",
	CmdMulti:               "multi",
	CmdExec:                "exec",
	CmdDiscard:             "discard",
	CmdWatch:               "watch",
	CmdUnwatch:             "unwatch",
	CmdFunction:            "function",
	CmdScript:              "script",
	CmdEval:                "eval",
	CmdEvalSha:             "evalsha",
	CmdEvalRO:              "eval_ro",
	CmdEvalShaRO:           "evalsha_ro",
	CmdFcall:               "fcall",
	CmdFcallRO:             "fcall_ro",
	CmdSync:                "sync",
	CmdPSync:               "psync",
	CmdReplConf:            "replconf",
	CmdWait:                "wait",
	CmdWaitAof:             "waitaof",
	CmdSubscribe:           "subscribe",
	CmdUnsubscribe:         "unsubscribe",
	CmdPSubscribe:          "psubscribe",
	CmdPUnsubscribe:        "punsubscribe",
	CmdPublish:             "publish",
	CmdPubSub:              "pubsub",
	CmdSwapDB:              "swapdb",
	CmdXAdd:                "xadd",
	CmdXLen:                "xlen",
	CmdXRange:              "xrange",
	CmdXRevRange:           "xrevrange",
	CmdXRead:               "xread",
	CmdXGroup:              "xgroup",
	CmdXReadGroup:          "xreadgroup",
	CmdXAck:                "xack",
	CmdXPending:            "xpending",
	CmdXTrim:               "xtrim",
	CmdXDel:                "xdel",
	CmdXInfo:               "xinfo",
	CmdXSetId:              "xsetid",
	CmdXDelEx:              "xdelex",
	CmdXAckDel:             "xackdel",
	CmdXClaim:              "xclaim",
	CmdXAutoClaim:          "xautoclaim",
	CmdXIdmpRecord:         "xidmprecord",
	CmdXCfgSet:             "xcfgset",
	CmdPFAdd:               "pfadd",
	CmdPFCount:             "pfcount",
	CmdPFMerge:             "pfmerge",
	CmdPFSelfTest:          "pfselftest",
	CmdPFDebug:             "pfdebug",
	CmdGeoAdd:              "geoadd",
	CmdGeoDist:             "geodist",
	CmdGeoHash:             "geohash",
	CmdGeoPos:              "geopos",
	CmdGeoRadius:           "georadius",
	CmdGeoRadiusRO:         "georadius_ro",
	CmdGeoRadiusByMember:   "georadiusbymember",
	CmdGeoRadiusByMemberRO: "georadiusbymember_ro",
	CmdGeoSearch:           "geosearch",
	CmdGeoSearchStore:      "geosearchstore",
	CmdAcl:                 "acl",
	CmdJSONSet:             "json.set",
	CmdJSONGet:             "json.get",
	CmdJSONMGet:            "json.mget",
	CmdJSONMSet:            "json.mset",
	CmdJSONMerge:           "json.merge",
	CmdJSONDel:             "json.del",
	CmdJSONForget:          "json.forget",
	CmdJSONClear:           "json.clear",
	CmdJSONType:            "json.type",
}

// String returns the command name
func (c CommandType) String() string {
	if s, ok := commandStrings[c]; ok {
		return s
	}
	return "unknown"
}

// commandStringPtrs interns each command's display name so hot paths can
// store a stable *string (e.g. ClientInfo.LastCmd) without allocating one
// per command.
var commandStringPtrs = func() [CmdMax]*string {
	var t [CmdMax]*string
	for c := range CmdMax {
		s := c.String()
		t[c] = &s
	}
	return t
}()

// StringPtr returns a stable pointer to the command's display name.
func (c CommandType) StringPtr() *string {
	if c < 0 || c >= CmdMax {
		c = CmdUnknown
	}
	return commandStringPtrs[c]
}

// ParseCommandType converts a command name byte slice to CommandType
func ParseCommandType(name []byte) CommandType {
	// Convert to uppercase for lookup
	upper := ToUpper(name)
	if cmd, ok := commandNames[string(upper)]; ok {
		return cmd
	}
	return CmdUnknown
}

// ToUpper converts bytes to uppercase in-place style.
// Returns a new slice if conversion is needed (to avoid modifying original).
func ToUpper(b []byte) []byte {
	needsConversion := false
	for _, c := range b {
		if c >= 'a' && c <= 'z' {
			needsConversion = true
			break
		}
	}
	if !needsConversion {
		return b
	}

	result := make([]byte, len(b))
	for i, c := range b {
		if c >= 'a' && c <= 'z' {
			result[i] = c - 32
		} else {
			result[i] = c
		}
	}
	return result
}
