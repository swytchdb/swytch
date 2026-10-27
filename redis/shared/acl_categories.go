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
	"slices"
	"strings"
)

const (
	categoryNameSet    = "set"
	categoryNameList   = "list"
	categoryNameHash   = "hash"
	categoryNameString = "string"
	categoryNameStream = "stream"
	categoryNamePubSub = "pubsub"
)

// CommandCategory represents a category of Redis commands
type CommandCategory int

const (
	CatRead CommandCategory = 1 << iota
	CatWrite
	CatSet
	CatSortedSet
	CatList
	CatHash
	CatString
	CatBitmap
	CatHyperLogLog
	CatGeo
	CatStream
	CatPubSub
	CatAdmin
	CatFast
	CatSlow
	CatBlocking
	CatDangerous
	CatConnection
	CatTransaction
	CatScripting
	CatKeyspace
	CatAll // Special: represents all commands
)

// categoryNames maps category names to values
var categoryNames = map[string]CommandCategory{
	"read":             CatRead,
	"write":            CatWrite,
	categoryNameSet:    CatSet,
	"sortedset":        CatSortedSet,
	categoryNameList:   CatList,
	categoryNameHash:   CatHash,
	categoryNameString: CatString,
	"bitmap":           CatBitmap,
	"hyperloglog":      CatHyperLogLog,
	"geo":              CatGeo,
	categoryNameStream: CatStream,
	categoryNamePubSub: CatPubSub,
	"admin":            CatAdmin,
	"fast":             CatFast,
	"slow":             CatSlow,
	"blocking":         CatBlocking,
	"dangerous":        CatDangerous,
	"connection":       CatConnection,
	"transaction":      CatTransaction,
	"scripting":        CatScripting,
	"keyspace":         CatKeyspace,
	"all":              CatAll,
}

// categoryToName maps category values to names
var categoryToName = map[CommandCategory]string{
	CatRead:        "read",
	CatWrite:       "write",
	CatSet:         categoryNameSet,
	CatSortedSet:   "sortedset",
	CatList:        categoryNameList,
	CatHash:        categoryNameHash,
	CatString:      categoryNameString,
	CatBitmap:      "bitmap",
	CatHyperLogLog: "hyperloglog",
	CatGeo:         "geo",
	CatStream:      categoryNameStream,
	CatPubSub:      categoryNamePubSub,
	CatAdmin:       "admin",
	CatFast:        "fast",
	CatSlow:        "slow",
	CatBlocking:    "blocking",
	CatDangerous:   "dangerous",
	CatConnection:  "connection",
	CatTransaction: "transaction",
	CatScripting:   "scripting",
	CatKeyspace:    "keyspace",
	CatAll:         "all",
}

// GetCategoryByName returns a category by name
func GetCategoryByName(name string) (CommandCategory, bool) {
	cat, ok := categoryNames[strings.ToLower(name)]

	return cat, ok
}

// GetCategoryName returns the name of a category
func GetCategoryName(cat CommandCategory) string {
	if name, ok := categoryToName[cat]; ok {
		return name
	}

	return unknownName
}

// GetAllCategoryNames returns all category names
func GetAllCategoryNames() []string {
	names := make([]string, 0, len(categoryNames))
	for name := range categoryNames {
		names = append(names, name)
	}

	return names
}

// commandCategories maps commands to their categories
var commandCategories = map[CommandType][]CommandCategory{
	// Connection commands
	CmdPing:   {CatFast, CatConnection},
	CmdEcho:   {CatFast, CatConnection},
	CmdAuth:   {CatFast, CatConnection},
	CmdSelect: {CatFast, CatConnection},
	CmdQuit:   {CatFast, CatConnection},
	CmdHello:  {CatFast, CatConnection},

	// String commands (read)
	CmdGet:      {CatRead, CatString, CatFast},
	CmdMGet:     {CatRead, CatString, CatFast},
	CmdStrLen:   {CatRead, CatString, CatFast},
	CmdGetRange: {CatRead, CatString, CatSlow},
	CmdSubstr:   {CatRead, CatString, CatSlow},

	// String commands (write)
	CmdSet:         {CatWrite, CatString, CatSlow},
	CmdSetNX:       {CatWrite, CatString, CatFast},
	CmdSetEX:       {CatWrite, CatString, CatSlow},
	CmdPSetEX:      {CatWrite, CatString, CatSlow},
	CmdMSet:        {CatWrite, CatString, CatSlow},
	CmdMSetNX:      {CatWrite, CatString, CatSlow},
	CmdMSetEX:      {CatWrite, CatString, CatSlow},
	CmdGetSet:      {CatWrite, CatString, CatFast},
	CmdGetDel:      {CatWrite, CatString, CatFast},
	CmdGetEx:       {CatWrite, CatString, CatFast},
	CmdAppend:      {CatWrite, CatString, CatFast},
	CmdSetRange:    {CatWrite, CatString, CatSlow},
	CmdIncr:        {CatWrite, CatString, CatFast},
	CmdDecr:        {CatWrite, CatString, CatFast},
	CmdIncrBy:      {CatWrite, CatString, CatFast},
	CmdDecrBy:      {CatWrite, CatString, CatFast},
	CmdIncrByFloat: {CatWrite, CatString, CatFast},
	CmdLcs:         {CatRead, CatString, CatSlow},
	CmdDigest:      {CatRead, CatString, CatFast},

	// Key commands
	CmdDel:         {CatWrite, CatKeyspace, CatFast},
	CmdUnlink:      {CatWrite, CatKeyspace, CatFast},
	CmdDelEx:       {CatWrite, CatKeyspace, CatFast},
	CmdExists:      {CatRead, CatKeyspace, CatFast},
	CmdExpire:      {CatWrite, CatKeyspace, CatFast},
	CmdExpireAt:    {CatWrite, CatKeyspace, CatFast},
	CmdPExpire:     {CatWrite, CatKeyspace, CatFast},
	CmdPExpireAt:   {CatWrite, CatKeyspace, CatFast},
	CmdTTL:         {CatRead, CatKeyspace, CatFast},
	CmdPTTL:        {CatRead, CatKeyspace, CatFast},
	CmdExpireTime:  {CatRead, CatKeyspace, CatFast},
	CmdPExpireTime: {CatRead, CatKeyspace, CatFast},
	CmdPersist:     {CatWrite, CatKeyspace, CatFast},
	CmdRename:      {CatWrite, CatKeyspace, CatSlow},
	CmdRenameNX:    {CatWrite, CatKeyspace, CatSlow},
	CmdType:        {CatRead, CatKeyspace, CatFast},
	CmdCopy:        {CatWrite, CatKeyspace, CatSlow},
	CmdMove:        {CatWrite, CatKeyspace, CatSlow},
	CmdRandomKey:   {CatRead, CatKeyspace, CatSlow},
	CmdSort:        {CatWrite, CatKeyspace, CatSlow, CatDangerous},
	CmdSortRO:      {CatRead, CatKeyspace, CatSlow},

	// Bitmap commands
	CmdSetBit:     {CatWrite, CatBitmap, CatSlow},
	CmdGetBit:     {CatRead, CatBitmap, CatFast},
	CmdBitCount:   {CatRead, CatBitmap, CatSlow},
	CmdBitPos:     {CatRead, CatBitmap, CatSlow},
	CmdBitOp:      {CatWrite, CatBitmap, CatSlow},
	CmdBitField:   {CatWrite, CatBitmap, CatSlow},
	CmdBitFieldRO: {CatRead, CatBitmap, CatSlow},

	// List commands (read)
	CmdLLen:   {CatRead, CatList, CatFast},
	CmdLRange: {CatRead, CatList, CatSlow},
	CmdLIndex: {CatRead, CatList, CatSlow},
	CmdLPos:   {CatRead, CatList, CatSlow},

	// List commands (write)
	CmdLPush:      {CatWrite, CatList, CatFast},
	CmdLPushX:     {CatWrite, CatList, CatFast},
	CmdRPush:      {CatWrite, CatList, CatFast},
	CmdRPushX:     {CatWrite, CatList, CatFast},
	CmdLPop:       {CatWrite, CatList, CatFast},
	CmdRPop:       {CatWrite, CatList, CatFast},
	CmdLSet:       {CatWrite, CatList, CatSlow},
	CmdLRem:       {CatWrite, CatList, CatSlow},
	CmdLTrim:      {CatWrite, CatList, CatSlow},
	CmdLInsert:    {CatWrite, CatList, CatSlow},
	CmdLMove:      {CatWrite, CatList, CatSlow},
	CmdRPopLPush:  {CatWrite, CatList, CatSlow},
	CmdLMPop:      {CatWrite, CatList, CatSlow},
	CmdBLPop:      {CatWrite, CatList, CatSlow, CatBlocking},
	CmdBRPop:      {CatWrite, CatList, CatSlow, CatBlocking},
	CmdBLMPop:     {CatWrite, CatList, CatSlow, CatBlocking},
	CmdBLMove:     {CatWrite, CatList, CatSlow, CatBlocking},
	CmdBRPopLPush: {CatWrite, CatList, CatSlow, CatBlocking},

	// Hash commands (read)
	CmdHGet:         {CatRead, CatHash, CatFast},
	CmdHExists:      {CatRead, CatHash, CatFast},
	CmdHGetAll:      {CatRead, CatHash, CatSlow},
	CmdHKeys:        {CatRead, CatHash, CatSlow},
	CmdHVals:        {CatRead, CatHash, CatSlow},
	CmdHLen:         {CatRead, CatHash, CatFast},
	CmdHMGet:        {CatRead, CatHash, CatFast},
	CmdHStrLen:      {CatRead, CatHash, CatFast},
	CmdHRandField:   {CatRead, CatHash, CatSlow},
	CmdHScan:        {CatRead, CatHash, CatSlow},
	CmdHTTL:         {CatRead, CatHash, CatFast},
	CmdHPTTL:        {CatRead, CatHash, CatFast},
	CmdHExpireTime:  {CatRead, CatHash, CatFast},
	CmdHPExpireTime: {CatRead, CatHash, CatFast},

	// Hash commands (write)
	CmdHSet:         {CatWrite, CatHash, CatFast},
	CmdHSetNX:       {CatWrite, CatHash, CatFast},
	CmdHDel:         {CatWrite, CatHash, CatFast},
	CmdHMSet:        {CatWrite, CatHash, CatFast},
	CmdHIncrBy:      {CatWrite, CatHash, CatFast},
	CmdHIncrByFloat: {CatWrite, CatHash, CatFast},
	CmdHGetDel:      {CatWrite, CatHash, CatFast},
	CmdHExpire:      {CatWrite, CatHash, CatFast},
	CmdHPExpire:     {CatWrite, CatHash, CatFast},
	CmdHExpireAt:    {CatWrite, CatHash, CatFast},
	CmdHPExpireAt:   {CatWrite, CatHash, CatFast},
	CmdHPersist:     {CatWrite, CatHash, CatFast},
	CmdHSetEx:       {CatWrite, CatHash, CatFast},
	CmdHGetEx:       {CatWrite, CatHash, CatFast},

	// Set commands (read)
	CmdSCard:       {CatRead, CatSet, CatFast},
	CmdSIsMember:   {CatRead, CatSet, CatFast},
	CmdSMIsMember:  {CatRead, CatSet, CatFast},
	CmdSMembers:    {CatRead, CatSet, CatSlow},
	CmdSRandMember: {CatRead, CatSet, CatSlow},
	CmdSInter:      {CatRead, CatSet, CatSlow},
	CmdSUnion:      {CatRead, CatSet, CatSlow},
	CmdSDiff:       {CatRead, CatSet, CatSlow},
	CmdSInterCard:  {CatRead, CatSet, CatSlow},

	// Set commands (write)
	CmdSAdd:        {CatWrite, CatSet, CatFast},
	CmdSPop:        {CatWrite, CatSet, CatFast},
	CmdSRem:        {CatWrite, CatSet, CatFast},
	CmdSInterStore: {CatWrite, CatSet, CatSlow},
	CmdSUnionStore: {CatWrite, CatSet, CatSlow},
	CmdSDiffStore:  {CatWrite, CatSet, CatSlow},
	CmdSMove:       {CatWrite, CatSet, CatFast},

	// Sorted set commands (read)
	CmdZScore:           {CatRead, CatSortedSet, CatFast},
	CmdZCard:            {CatRead, CatSortedSet, CatFast},
	CmdZRank:            {CatRead, CatSortedSet, CatFast},
	CmdZRevRank:         {CatRead, CatSortedSet, CatFast},
	CmdZRange:           {CatRead, CatSortedSet, CatSlow},
	CmdZRevRange:        {CatRead, CatSortedSet, CatSlow},
	CmdZRangeByScore:    {CatRead, CatSortedSet, CatSlow},
	CmdZRevRangeByScore: {CatRead, CatSortedSet, CatSlow},
	CmdZCount:           {CatRead, CatSortedSet, CatFast},
	CmdZMScore:          {CatRead, CatSortedSet, CatFast},
	CmdZRangeByLex:      {CatRead, CatSortedSet, CatSlow},
	CmdZRevRangeByLex:   {CatRead, CatSortedSet, CatSlow},
	CmdZLexCount:        {CatRead, CatSortedSet, CatFast},
	CmdZRandMember:      {CatRead, CatSortedSet, CatSlow},
	CmdZUnion:           {CatRead, CatSortedSet, CatSlow},
	CmdZInter:           {CatRead, CatSortedSet, CatSlow},
	CmdZDiff:            {CatRead, CatSortedSet, CatSlow},
	CmdZInterCard:       {CatRead, CatSortedSet, CatSlow},

	// Sorted set commands (write)
	CmdZAdd:             {CatWrite, CatSortedSet, CatFast},
	CmdZRem:             {CatWrite, CatSortedSet, CatFast},
	CmdZIncrBy:          {CatWrite, CatSortedSet, CatFast},
	CmdZPopMin:          {CatWrite, CatSortedSet, CatFast},
	CmdZPopMax:          {CatWrite, CatSortedSet, CatFast},
	CmdZMPop:            {CatWrite, CatSortedSet, CatSlow},
	CmdZRemRangeByScore: {CatWrite, CatSortedSet, CatSlow},
	CmdZRemRangeByRank:  {CatWrite, CatSortedSet, CatSlow},
	CmdZRemRangeByLex:   {CatWrite, CatSortedSet, CatSlow},
	CmdZUnionStore:      {CatWrite, CatSortedSet, CatSlow},
	CmdZInterStore:      {CatWrite, CatSortedSet, CatSlow},
	CmdZDiffStore:       {CatWrite, CatSortedSet, CatSlow},
	CmdZRangeStore:      {CatWrite, CatSortedSet, CatSlow},
	CmdBZPopMin:         {CatWrite, CatSortedSet, CatSlow, CatBlocking},
	CmdBZPopMax:         {CatWrite, CatSortedSet, CatSlow, CatBlocking},
	CmdBZMPop:           {CatWrite, CatSortedSet, CatSlow, CatBlocking},

	// Stream commands (read)
	CmdXLen:      {CatRead, CatStream, CatFast},
	CmdXRange:    {CatRead, CatStream, CatSlow},
	CmdXRevRange: {CatRead, CatStream, CatSlow},
	CmdXRead:     {CatRead, CatStream, CatSlow, CatBlocking},
	CmdXPending:  {CatRead, CatStream, CatSlow},
	CmdXInfo:     {CatRead, CatStream, CatSlow},

	// Stream commands (write)
	CmdXAdd:        {CatWrite, CatStream, CatFast},
	CmdXGroup:      {CatWrite, CatStream, CatSlow},
	CmdXReadGroup:  {CatWrite, CatStream, CatSlow, CatBlocking},
	CmdXAck:        {CatWrite, CatStream, CatFast},
	CmdXTrim:       {CatWrite, CatStream, CatSlow},
	CmdXDel:        {CatWrite, CatStream, CatFast},
	CmdXSetId:      {CatWrite, CatStream, CatFast},
	CmdXDelEx:      {CatWrite, CatStream, CatFast},
	CmdXAckDel:     {CatWrite, CatStream, CatFast},
	CmdXClaim:      {CatWrite, CatStream, CatFast},
	CmdXAutoClaim:  {CatWrite, CatStream, CatFast},
	CmdXIdmpRecord: {CatWrite, CatStream, CatFast},
	CmdXCfgSet:     {CatWrite, CatStream, CatFast},

	// HyperLogLog commands
	CmdPFAdd:      {CatWrite, CatHyperLogLog, CatFast},
	CmdPFCount:    {CatRead, CatHyperLogLog, CatSlow},
	CmdPFMerge:    {CatWrite, CatHyperLogLog, CatSlow},
	CmdPFSelfTest: {CatRead, CatHyperLogLog, CatSlow, CatAdmin},
	CmdPFDebug:    {CatRead, CatHyperLogLog, CatSlow, CatAdmin},

	// Geo commands
	CmdGeoAdd:              {CatWrite, CatGeo, CatSlow},
	CmdGeoDist:             {CatRead, CatGeo, CatFast},
	CmdGeoHash:             {CatRead, CatGeo, CatFast},
	CmdGeoPos:              {CatRead, CatGeo, CatFast},
	CmdGeoRadius:           {CatWrite, CatGeo, CatSlow},
	CmdGeoRadiusRO:         {CatRead, CatGeo, CatSlow},
	CmdGeoRadiusByMember:   {CatWrite, CatGeo, CatSlow},
	CmdGeoRadiusByMemberRO: {CatRead, CatGeo, CatSlow},
	CmdGeoSearch:           {CatRead, CatGeo, CatSlow},
	CmdGeoSearchStore:      {CatWrite, CatGeo, CatSlow},

	// Pub/Sub commands
	CmdSubscribe:    {CatPubSub, CatSlow},
	CmdUnsubscribe:  {CatPubSub, CatSlow},
	CmdPSubscribe:   {CatPubSub, CatSlow},
	CmdPUnsubscribe: {CatPubSub, CatSlow},
	CmdPublish:      {CatPubSub, CatFast},
	CmdPubSub:       {CatPubSub, CatSlow},

	// Server commands
	CmdInfo:     {CatRead, CatAdmin, CatSlow},
	CmdDBSize:   {CatRead, CatAdmin, CatFast},
	CmdFlushDB:  {CatWrite, CatAdmin, CatSlow, CatDangerous},
	CmdFlushAll: {CatWrite, CatAdmin, CatSlow, CatDangerous},
	CmdTime:     {CatRead, CatAdmin, CatFast},
	CmdKeys:     {CatRead, CatKeyspace, CatSlow, CatDangerous},
	CmdScan:     {CatRead, CatKeyspace, CatSlow},
	CmdCommand:  {CatRead, CatAdmin, CatSlow},
	CmdConfig:   {CatAdmin, CatSlow, CatDangerous},
	CmdClient:   {CatAdmin, CatSlow},
	CmdDebug:    {CatAdmin, CatSlow, CatDangerous},
	CmdMemory:   {CatRead, CatAdmin, CatSlow},
	CmdShutdown: {CatAdmin, CatSlow, CatDangerous},
	CmdSwapDB:   {CatWrite, CatAdmin, CatSlow, CatDangerous},

	// Transaction commands
	CmdMulti:   {CatTransaction, CatFast},
	CmdExec:    {CatTransaction, CatSlow},
	CmdDiscard: {CatTransaction, CatFast},
	CmdWatch:   {CatTransaction, CatFast},
	CmdUnwatch: {CatTransaction, CatFast},

	// Scripting commands
	CmdFunction:  {CatScripting, CatSlow},
	CmdScript:    {CatScripting, CatSlow},
	CmdEval:      {CatScripting, CatSlow},
	CmdEvalSha:   {CatScripting, CatSlow},
	CmdEvalRO:    {CatScripting, CatSlow, CatRead},
	CmdEvalShaRO: {CatScripting, CatSlow, CatRead},
	CmdFcall:     {CatScripting, CatSlow},
	CmdFcallRO:   {CatScripting, CatSlow, CatRead},

	// Replication commands (stubbed)
	CmdSync:     {CatAdmin, CatSlow},
	CmdPSync:    {CatAdmin, CatSlow},
	CmdReplConf: {CatAdmin, CatSlow},
	CmdWait:     {CatSlow, CatBlocking},
	CmdWaitAof:  {CatSlow, CatBlocking},

	// ACL commands
	CmdAcl: {CatAdmin, CatSlow, CatDangerous},
}

// GetCommandCategories returns the categories for a command
func GetCommandCategories(cmd CommandType) []CommandCategory {
	if cats, ok := commandCategories[cmd]; ok {
		return cats
	}

	return nil
}

// GetCommandsInCategory returns all commands in a category
func GetCommandsInCategory(cat CommandCategory) []CommandType {
	var cmds []CommandType

	for cmd, cats := range commandCategories {
		if slices.Contains(cats, cat) {
			cmds = append(cmds, cmd)
		}
	}

	return cmds
}

// CommandHasCategory checks if a command belongs to a category
func CommandHasCategory(cmd CommandType, cat CommandCategory) bool {
	if cats, ok := commandCategories[cmd]; ok {
		if slices.Contains(cats, cat) {
			return true
		}
	}

	return false
}
