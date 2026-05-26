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

/*
 * This file is part of CloxCache.
 *
 * CloxCache is licensed under the MIT License.
 * See LICENSE file for details.
 */

package keytrie

import (
	"sync"
	"sync/atomic"

	"github.com/puzpuzpuz/xsync/v4"
)

const (
	defaultCritbitArenaChunkSize = 2048
)

// critNode is a flattened node that serves as both internal and leaf.
// Hot-path fields (child, bytePos, otherbits, isLeaf) are in the first 22 bytes.
// Leaf fields start at offset 24. All fits in one cache line.
type critNode struct {
	child     [2]atomic.Pointer[critNode] // 16 bytes, offset 0  (internal)
	bytePos   uint32                      // 4 bytes,  offset 16 (internal)
	otherbits uint8                       // 1 byte,   offset 20 (internal)
	isLeaf    bool                        // 1 byte,   offset 21
	_         [2]byte                     //           offset 22
	key       string                      // 16 bytes, offset 24 (leaf)
	tips      atomic.Pointer[TipSet]      // 8 bytes,  offset 40 (leaf)
	deleted   atomic.Bool                 // 4 bytes,  offset 44 (leaf)
	_         [16]byte                    //           offset 48
}

func noop() {}

func (n *critNode) isDeleted() bool {
	return n.deleted.Load()
}

// Critbit is a crit-bit trie for storing string keys.
type Critbit struct {
	root   atomic.Pointer[critNode]
	size   atomic.Int64
	closed atomic.Bool

	deletedCount atomic.Int64 // deleted-but-linked leaves still in the tree
	reapRunning  atomic.Bool  // prevents concurrent reap goroutines

	initOnce sync.Once
	arena    critbitArena[critNode]

	// reapMu coordinates structural modifications. Inserts take a
	// read-lock (concurrent inserts proceed in parallel). The reaper
	// takes a write-lock so no insert can be mid-CAS on a child
	// pointer while the reaper unlinks a subtree.
	reapMu xsync.RBMutex
}

func NewCritbit() *Critbit {
	c := &Critbit{}
	c.ensureInit()
	return c
}

// arenaChunkList holds an immutable snapshot of the chunk slice.
// Swapped atomically so readers never see a partially-grown slice.
type arenaChunkList[T any] struct {
	chunks [][]T
}

type critbitArena[T any] struct {
	next      atomic.Uint64
	chunkSize uint64
	growMu    sync.Mutex // only held when allocating a new chunk (every chunkSize allocs)
	list      atomic.Pointer[arenaChunkList[T]]
}

func (a *critbitArena[T]) init(chunkSize int) {
	if chunkSize < 1 {
		chunkSize = 1
	}
	a.chunkSize = uint64(chunkSize)
	a.list.Store(&arenaChunkList[T]{})
}

func (a *critbitArena[T]) alloc() *T {
	// Fast path: atomically claim a slot index. No lock needed.
	idx := a.next.Add(1) - 1
	chunkIdx := idx / a.chunkSize
	offset := idx % a.chunkSize

	list := a.list.Load()
	if chunkIdx >= uint64(len(list.chunks)) {
		// Slow path: need a new chunk. Only one goroutine allocates;
		// others wait on the mutex then find the chunk already exists.
		a.grow(chunkIdx)
		list = a.list.Load()
	}

	return &list.chunks[chunkIdx][offset]
}

func (a *critbitArena[T]) grow(needed uint64) {
	a.growMu.Lock()
	defer a.growMu.Unlock()

	list := a.list.Load()
	for uint64(len(list.chunks)) <= needed {
		newChunks := make([][]T, len(list.chunks)+1)
		copy(newChunks, list.chunks)
		newChunks[len(list.chunks)] = make([]T, a.chunkSize)
		list = &arenaChunkList[T]{chunks: newChunks}
		a.list.Store(list)
	}
}

func (a *critbitArena[T]) clear() {
	a.growMu.Lock()
	a.list.Store(&arenaChunkList[T]{})
	a.next.Store(0)
	a.growMu.Unlock()
}

func (c *Critbit) ensureInit() {
	c.initOnce.Do(func() {
		c.arena.init(defaultCritbitArenaChunkSize)
	})
}

func (c *Critbit) allocLeafNode(key string, ts *TipSet) *critNode {
	n := c.arena.alloc()
	n.isLeaf = true
	n.key = key
	n.tips.Store(ts)
	return n
}

func (c *Critbit) allocInternalNode(bytePos uint32, otherbits uint8) *critNode {
	n := c.arena.alloc()
	n.bytePos = bytePos
	n.otherbits = otherbits
	return n
}


func highestBit(b byte) uint8 {
	if b == 0 {
		return 0
	}
	b |= b >> 1
	b |= b >> 2
	b |= b >> 4
	return b ^ (b >> 1)
}

func findCritBit(a, b string) (bytePos uint32, otherbits uint8) {
	minLen := min(len(b), len(a))

	for i := range minLen {
		if a[i] != b[i] {
			return uint32(i), highestBit(a[i]^b[i]) ^ 0xFF
		}
	}

	if len(a) != len(b) {
		bytePos = uint32(minLen)
		var diffByte byte
		if len(a) > minLen {
			diffByte = a[minLen]
		} else {
			diffByte = b[minLen]
		}
		hb := highestBit(diffByte)
		if hb == 0 {
			return bytePos, 0xFF // length-discrimination sentinel
		}
		return bytePos, hb ^ 0xFF
	}
	return 0, 0
}

// getDirection uses branchless arithmetic matching the reference C implementation.
// otherbits has all bits set except the critical bit.
// (1 + uint16(otherbits | c)) >> 8 yields 1 when the critical bit is set in c, 0 otherwise.
func getDirection(key string, bytePos uint32, otherbits uint8) int {
	if int(bytePos) >= len(key) {
		return 0
	}
	c := key[bytePos]
	return int(1+uint16(otherbits|c)) >> 8
}

// Insert attempts to store a TipSet for the given key using CAS.
//
// For new keys (old is nil): CAS retry loop for tree structural insertion.
// For existing keys: single CAS comparing old to the current pointer.
//
// On success: returns (nil, true).
// On CAS failure: returns (currentTips, false) — the conflicting tip set.
func (c *Critbit) Insert(key string, old *TipSet, new *TipSet) (*TipSet, bool) {
	if c.closed.Load() {
		return nil, false
	}
	c.ensureInit()

	for {
		if c.closed.Load() {
			return nil, false
		}

		rootNode := c.root.Load()

		if rootNode == nil {
			newNode := c.allocLeafNode(key, new)
			rt := c.reapMu.RLock()
			ok := c.root.CompareAndSwap(nil, newNode)
			c.reapMu.RUnlock(rt)
			if ok {
				c.size.Add(1)
				return nil, true
			}
			continue
		}

		rt := c.reapMu.RLock()

		bestLeaf := c.findBestMatch(rootNode, key)
		if bestLeaf == nil {
			c.reapMu.RUnlock(rt)
			continue
		}

		if bestLeaf.key == key {
			// Existing key — single CAS attempt.
			// If the leaf is deleted, the caller may pass old=nil (from Contains
			// returning nil). Accept that by swapping old to the actual current
			// pointer so the CAS can succeed.
			current := bestLeaf.tips.Load()
			if old == nil && bestLeaf.isDeleted() {
				old = current
			}
			if bestLeaf.tips.CompareAndSwap(old, new) {
				if bestLeaf.deleted.CompareAndSwap(true, false) {
					c.size.Add(1)
					c.deletedCount.Add(-1)
				}
				c.reapMu.RUnlock(rt)
				return nil, true
			}
			tips := bestLeaf.tips.Load()
			c.reapMu.RUnlock(rt)
			return tips, false
		}

		bytePos, otherbits := findCritBit(key, bestLeaf.key)
		if otherbits == 0 {
			current := bestLeaf.tips.Load()
			if old == nil && bestLeaf.isDeleted() {
				old = current
			}
			if bestLeaf.tips.CompareAndSwap(old, new) {
				if bestLeaf.deleted.CompareAndSwap(true, false) {
					c.size.Add(1)
					c.deletedCount.Add(-1)
				}
				c.reapMu.RUnlock(rt)
				return nil, true
			}
			tips := bestLeaf.tips.Load()
			c.reapMu.RUnlock(rt)
			return tips, false
		}

		newLeafNode := c.allocLeafNode(key, new)
		newInternal := c.allocInternalNode(bytePos, otherbits)

		newDir := getDirection(key, bytePos, otherbits)
		oldDir := 1 - newDir

		ok := c.insertNode(rootNode, newInternal, newLeafNode, newDir, oldDir, bytePos, otherbits)
		c.reapMu.RUnlock(rt)
		if ok {
			c.size.Add(1)
			return nil, true
		}
	}
}

func (c *Critbit) findBestMatch(node *critNode, key string) *critNode {
	if node == nil {
		return nil
	}
	current := node
	for !current.isLeaf {
		dir := getDirection(key, current.bytePos, current.otherbits)
		next := current.child[dir].Load()
		if next == nil {
			return nil
		}
		current = next
	}
	return current
}

func (c *Critbit) insertNode(rootNode *critNode, newInternal *critNode, newLeafNode *critNode, newDir, oldDir int, bytePos uint32, otherbits uint8) bool {
	if rootNode.isLeaf {
		newInternal.child[newDir].Store(newLeafNode)
		newInternal.child[oldDir].Store(rootNode)
		return c.root.CompareAndSwap(rootNode, newInternal)
	}

	if shouldInsertBefore(bytePos, otherbits, rootNode.bytePos, rootNode.otherbits) {
		newInternal.child[newDir].Store(newLeafNode)
		newInternal.child[oldDir].Store(rootNode)
		return c.root.CompareAndSwap(rootNode, newInternal)
	}

	return c.walkAndInsert(rootNode, newInternal, newLeafNode, newDir, oldDir, bytePos, otherbits)
}

func shouldInsertBefore(newBytePos uint32, newOtherbits uint8, curBytePos uint32, curOtherbits uint8) bool {
	if newBytePos < curBytePos {
		return true
	}
	if newBytePos > curBytePos {
		return false
	}
	// Lower otherbits = more significant bit (comparison flips vs bitMask)
	return newOtherbits < curOtherbits
}

func (c *Critbit) walkAndInsert(rootNode *critNode, newInternal *critNode, newLeafNode *critNode, newDir, oldDir int, bytePos uint32, otherbits uint8) bool {
	current := rootNode

	for {
		// The new node's bit must be strictly less significant than the
		// current parent's bit. A concurrent insert may have spliced a
		// new internal node with the same (or more-significant) bit into
		// the path we are walking, which would create a duplicate or
		// out-of-order bit position if we insert here.
		if !shouldInsertBefore(current.bytePos, current.otherbits, bytePos, otherbits) {
			return false
		}

		dir := getDirection(newLeafNode.key, current.bytePos, current.otherbits)
		childNode := current.child[dir].Load()

		if childNode == nil {
			return false
		}

		if childNode.isLeaf {
			// Verify the precomputed crit bit is still valid for this leaf.
			// A concurrent insert may have replaced the original leaf with a
			// different one, making the crit bit (computed against the old
			// best-match leaf) incorrect for this pairing.
			actualBytePos, actualOtherbits := findCritBit(newLeafNode.key, childNode.key)
			if actualBytePos != bytePos || actualOtherbits != otherbits {
				return false // tree changed; retry from Insert's outer loop
			}
			newInternal.child[newDir].Store(newLeafNode)
			newInternal.child[oldDir].Store(childNode)
			return current.child[dir].CompareAndSwap(childNode, newInternal)
		}

		if shouldInsertBefore(bytePos, otherbits, childNode.bytePos, childNode.otherbits) {
			newInternal.child[newDir].Store(newLeafNode)
			newInternal.child[oldDir].Store(childNode)
			return current.child[dir].CompareAndSwap(childNode, newInternal)
		}

		current = childNode
	}
}

// Contains checks if a key exists and returns its TipSet.
// Returns nil if the key doesn't exist or is deleted.
func (c *Critbit) Contains(key string) *TipSet {
	if c.closed.Load() {
		return nil
	}
	rootNode := c.root.Load()
	if rootNode == nil {
		return nil
	}
	leaf := c.findBestMatch(rootNode, key)
	if leaf == nil || leaf.key != key || leaf.isDeleted() {
		return nil
	}
	return leaf.tips.Load()
}

func (c *Critbit) RemoveTips(key string, refs []EffectRef) {
	if c.closed.Load() || len(refs) == 0 {
		return
	}
	rootNode := c.root.Load()
	if rootNode == nil {
		return
	}
	leaf := c.findBestMatch(rootNode, key)
	if leaf == nil || leaf.key != key || leaf.isDeleted() {
		return
	}

	removeSet := make(map[EffectRef]bool, len(refs))
	for _, r := range refs {
		removeSet[r] = true
	}

	for {
		current := leaf.tips.Load()
		if current == nil {
			return
		}
		var kept []EffectRef
		for _, tip := range current.Tips() {
			if !removeSet[tip] {
				kept = append(kept, tip)
			}
		}
		if len(kept) == len(current.Tips()) {
			return // nothing to remove
		}
		newTips := NewTipSet(kept...)
		if leaf.tips.CompareAndSwap(current, newTips) {
			return
		}
		// CAS failed, retry
	}
}

func (c *Critbit) Delete(key string, old *TipSet) bool {
	return c.DeleteAndSnapshot(key, old) != nil
}

// DeleteAndSnapshot removes a key only if its current tips match old
// (CAS). Returns the previous tip set on success, nil on failure.
func (c *Critbit) DeleteAndSnapshot(key string, old *TipSet) *TipSet {
	if c.closed.Load() {
		return nil
	}
	rootNode := c.root.Load()
	if rootNode == nil {
		return nil
	}
	leaf := c.findBestMatch(rootNode, key)
	if leaf == nil || leaf.key != key {
		return nil
	}
	if !leaf.deleted.CompareAndSwap(false, true) {
		return nil
	}
	if !leaf.tips.CompareAndSwap(old, nil) {
		leaf.deleted.Store(false)
		return nil
	}
	c.size.Add(-1)

	deleted := c.deletedCount.Add(1)
	live := c.size.Load()
	if deleted > 0 && deleted > (live+deleted)/10 {
		c.maybeReap()
	}

	return old
}

func (c *Critbit) Size() int64 {
	if c.closed.Load() {
		return 0
	}
	return c.size.Load()
}

func (c *Critbit) Range(fn func(key string) bool) {
	if c.closed.Load() || fn == nil {
		return
	}
	rootNode := c.root.Load()
	if rootNode != nil {
		c.rangeNode(rootNode, fn)
	}
}

func (c *Critbit) rangeNode(node *critNode, fn func(key string) bool) bool {
	if node == nil {
		return true
	}
	if node.isLeaf {
		if !node.isDeleted() {
			return fn(node.key)
		}
		return true
	}
	if left := node.child[0].Load(); left != nil {
		if !c.rangeNode(left, fn) {
			return false
		}
	}
	if right := node.child[1].Load(); right != nil {
		return c.rangeNode(right, fn)
	}
	return true
}

func (c *Critbit) RangeFrom(after string, fn func(key string) bool) {
	if c.closed.Load() || fn == nil {
		return
	}
	if after == "" {
		c.Range(fn)
		return
	}
	rootNode := c.root.Load()
	if rootNode != nil {
		c.rangeFromNode(rootNode, after, fn)
	}
}

func (c *Critbit) rangeFromNode(node *critNode, after string, fn func(key string) bool) bool {
	if node == nil {
		return true
	}
	if node.isLeaf {
		if !node.isDeleted() && node.key > after {
			return fn(node.key)
		}
		return true
	}
	if left := node.child[0].Load(); left != nil {
		if !c.rangeFromNode(left, after, fn) {
			return false
		}
	}
	if right := node.child[1].Load(); right != nil {
		return c.rangeFromNode(right, after, fn)
	}
	return true
}

func (c *Critbit) RangePrefix(prefix string, fn func(key string) bool) {
	if c.closed.Load() || fn == nil {
		return
	}
	rootNode := c.root.Load()
	if rootNode == nil {
		return
	}
	subtree := c.findPrefixSubtree(rootNode, prefix)
	if subtree != nil {
		c.rangePrefixNode(subtree, prefix, fn)
	}
}

func (c *Critbit) findPrefixSubtree(node *critNode, prefix string) *critNode {
	current := node
	for current != nil && !current.isLeaf {
		if int(current.bytePos) >= len(prefix) {
			return current
		}
		dir := getDirection(prefix, current.bytePos, current.otherbits)
		current = current.child[dir].Load()
	}
	return current
}

func (c *Critbit) rangePrefixNode(node *critNode, prefix string, fn func(key string) bool) bool {
	if node == nil {
		return true
	}
	if node.isLeaf {
		if !node.isDeleted() {
			if len(node.key) >= len(prefix) && node.key[:len(prefix)] == prefix {
				return fn(node.key)
			}
		}
		return true
	}
	if left := node.child[0].Load(); left != nil {
		if !c.rangePrefixNode(left, prefix, fn) {
			return false
		}
	}
	if right := node.child[1].Load(); right != nil {
		return c.rangePrefixNode(right, prefix, fn)
	}
	return true
}

func (c *Critbit) Keys() []string {
	if c.closed.Load() {
		return nil
	}
	result := make([]string, 0, c.size.Load())
	c.Range(func(key string) bool {
		result = append(result, key)
		return true
	})
	return result
}

func (c *Critbit) MatchPattern(pattern string) []string {
	if c.closed.Load() {
		return nil
	}
	if pattern == "*" {
		return c.Keys()
	}
	allKeys := c.Keys()
	result := make([]string, 0)
	for _, key := range allKeys {
		if matchGlob(key, pattern) {
			result = append(result, key)
		}
	}
	return result
}

func (c *Critbit) FirstWithPrefix(prefix string, claim bool) (string, bool, ReleaseClaimFunc) {
	if c.closed.Load() {
		return "", false, nil
	}
	rootNode := c.root.Load()
	if rootNode == nil {
		return "", false, nil
	}
	subtree := c.findPrefixSubtree(rootNode, prefix)
	if subtree == nil {
		return "", false, nil
	}
	key, _, found := c.findLeftmost(subtree, prefix)
	if !found {
		return "", false, nil
	}
	if claim {
		return key, true, noop
	}
	return key, true, nil
}

func (c *Critbit) findLeftmost(node *critNode, prefix string) (string, *critNode, bool) {
	if node == nil {
		return "", nil, false
	}
	if node.isLeaf {
		if node.isDeleted() {
			return "", nil, false
		}
		if len(node.key) >= len(prefix) && node.key[:len(prefix)] == prefix {
			return node.key, node, true
		}
		return "", nil, false
	}
	if left := node.child[0].Load(); left != nil {
		if key, leaf, found := c.findLeftmost(left, prefix); found {
			return key, leaf, true
		}
	}
	if right := node.child[1].Load(); right != nil {
		return c.findLeftmost(right, prefix)
	}
	return "", nil, false
}

func (c *Critbit) LastWithPrefix(prefix string, claim bool) (string, bool, ReleaseClaimFunc) {
	if c.closed.Load() {
		return "", false, nil
	}
	rootNode := c.root.Load()
	if rootNode == nil {
		return "", false, nil
	}
	subtree := c.findPrefixSubtree(rootNode, prefix)
	if subtree == nil {
		return "", false, nil
	}
	key, _, found := c.findRightmost(subtree, prefix)
	if !found {
		return "", false, nil
	}
	if claim {
		return key, true, noop
	}
	return key, true, nil
}

func (c *Critbit) findRightmost(node *critNode, prefix string) (string, *critNode, bool) {
	if node == nil {
		return "", nil, false
	}
	if node.isLeaf {
		if node.isDeleted() {
			return "", nil, false
		}
		if len(node.key) >= len(prefix) && node.key[:len(prefix)] == prefix {
			return node.key, node, true
		}
		return "", nil, false
	}
	if right := node.child[1].Load(); right != nil {
		if key, leaf, found := c.findRightmost(right, prefix); found {
			return key, leaf, true
		}
	}
	if left := node.child[0].Load(); left != nil {
		return c.findRightmost(left, prefix)
	}
	return "", nil, false
}

type critbitPathEntry struct {
	node *critNode
	dir  int
}

var pathPool = sync.Pool{
	New: func() any {
		p := make([]critbitPathEntry, 0, 64)
		return &p
	},
}

func (c *Critbit) NextWithPrefix(prefix, after string, claim bool) (string, bool, ReleaseClaimFunc) {
	if c.closed.Load() {
		return "", false, nil
	}
	if len(after) < len(prefix) || after[:len(prefix)] != prefix {
		return c.FirstWithPrefix(prefix, claim)
	}

	pathPtr := pathPool.Get().(*[]critbitPathEntry)
	defer func() {
		*pathPtr = (*pathPtr)[:0]
		pathPool.Put(pathPtr)
	}()

	for {
		rootNode := c.root.Load()
		if rootNode == nil {
			return "", false, nil
		}

		path := (*pathPtr)[:0]
		current := rootNode

		for current != nil && !current.isLeaf {
			dir := getDirection(after, current.bytePos, current.otherbits)
			path = append(path, critbitPathEntry{node: current, dir: dir})
			current = current.child[dir].Load()
		}
		*pathPtr = path

		var resultKey string
		found := false

		// Check if the leaf we landed on is > after (handles case when after doesn't exist)
		if current != nil && current.isLeaf && !current.isDeleted() {
			leafKey := current.key
			if len(leafKey) >= len(prefix) && leafKey[:len(prefix)] == prefix && leafKey > after {
				resultKey, found = leafKey, true
			}
		}

		// If not found yet, walk back up looking for right branches
		if !found {
			for i := len(path) - 1; i >= 0; i-- {
				entry := path[i]
				if entry.dir == 0 {
					rightNode := entry.node.child[1].Load()
					if rightNode == nil {
						continue
					}
					if int(entry.node.bytePos) < len(prefix) {
						if getDirection(prefix, entry.node.bytePos, entry.node.otherbits) == 0 {
							continue
						}
					}
					if key, _, ok := c.findLeftmost(rightNode, prefix); ok && key > after {
						resultKey, found = key, true
						break
					}
				}
			}
		}

		if !found {
			return "", false, nil
		}
		if claim {
			return resultKey, true, noop
		}
		return resultKey, true, nil
	}
}

func (c *Critbit) PrevWithPrefix(prefix, before string, claim bool) (string, bool, ReleaseClaimFunc) {
	if c.closed.Load() {
		return "", false, nil
	}
	if len(before) < len(prefix) || before[:len(prefix)] != prefix {
		return c.LastWithPrefix(prefix, claim)
	}

	pathPtr := pathPool.Get().(*[]critbitPathEntry)
	defer func() {
		*pathPtr = (*pathPtr)[:0]
		pathPool.Put(pathPtr)
	}()

	for {
		rootNode := c.root.Load()
		if rootNode == nil {
			return "", false, nil
		}

		path := (*pathPtr)[:0]
		current := rootNode

		for current != nil && !current.isLeaf {
			dir := getDirection(before, current.bytePos, current.otherbits)
			path = append(path, critbitPathEntry{node: current, dir: dir})
			current = current.child[dir].Load()
		}
		*pathPtr = path

		var resultKey string
		found := false

		for i := len(path) - 1; i >= 0; i-- {
			entry := path[i]
			if entry.dir == 1 {
				leftNode := entry.node.child[0].Load()
				if leftNode == nil {
					continue
				}
				if int(entry.node.bytePos) < len(prefix) {
					if getDirection(prefix, entry.node.bytePos, entry.node.otherbits) == 1 {
						continue
					}
				}
				if key, _, ok := c.findRightmost(leftNode, prefix); ok && key < before {
					resultKey, found = key, true
					break
				}
			}
		}

		if !found {
			return "", false, nil
		}
		if claim {
			return resultKey, true, noop
		}
		return resultKey, true, nil
	}
}

func (c *Critbit) TryClaimKey(key string) (exists bool, release ReleaseClaimFunc) {
	if c.closed.Load() {
		return false, nil
	}
	rootNode := c.root.Load()
	if rootNode == nil {
		return false, nil
	}
	leaf := c.findBestMatch(rootNode, key)
	if leaf == nil || leaf.key != key || leaf.isDeleted() {
		return false, nil
	}
	return true, noop
}

func (c *Critbit) GetHeadHint(prefix string) string      { return "" }
func (c *Critbit) SetHeadHint(prefix string, key string) {}
func (c *Critbit) GetTailHint(prefix string) string      { return "" }
func (c *Critbit) SetTailHint(prefix string, key string) {}

// Snapshot returns a frozen copy of the index. The new Critbit is independent —
// mutations to either copy don't affect the other. TipSets are immutable so
// only pointers are copied. O(n) in key count.
func (c *Critbit) Snapshot() KeyIndex {
	snap := NewCritbit()
	c.Range(func(key string) bool {
		tips := c.Contains(key)
		if tips != nil {
			snap.Insert(key, nil, tips)
		}
		return true
	})
	return snap
}

func (c *Critbit) maybeReap() {
	if !c.reapRunning.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer c.reapRunning.Store(false)
		for c.reap() > 0 {
		}
	}()
}

// reap does a single BFS walk pruning every deleted leaf it finds.
// Takes the write-lock on reapMu so no structural insert can race.
// Returns the number of deleted leaves unlinked.
func (c *Critbit) reap() int {
	if c.closed.Load() {
		return 0
	}

	c.reapMu.Lock()
	defer c.reapMu.Unlock()

	root := c.root.Load()
	if root == nil {
		return 0
	}
	if root.isLeaf {
		if root.isDeleted() {
			c.root.Store(nil)
			c.deletedCount.Add(-1)
			return 1
		}
		return 0
	}

	type bfsEntry struct {
		node          *critNode
		parent        *critNode
		dirFromParent int
	}

	reaped := 0
	queue := make([]bfsEntry, 1, 64)
	queue[0] = bfsEntry{node: root}

	for i := 0; i < len(queue); i++ {
		e := queue[i]
		node := e.node

		pruned := false
		for dir := range 2 {
			child := node.child[dir].Load()
			if child == nil || !child.isLeaf || !child.isDeleted() {
				continue
			}

			sibling := node.child[1-dir].Load()
			if sibling == nil {
				continue
			}

			if e.parent == nil {
				c.root.Store(sibling)
			} else {
				e.parent.child[e.dirFromParent].Store(sibling)
			}
			c.deletedCount.Add(-1)
			reaped++
			pruned = true

			if !sibling.isLeaf {
				queue = append(queue, bfsEntry{
					node:          sibling,
					parent:        e.parent,
					dirFromParent: e.dirFromParent,
				})
			}
			break
		}

		if !pruned {
			for dir := range 2 {
				child := node.child[dir].Load()
				if child != nil && !child.isLeaf {
					queue = append(queue, bfsEntry{
						node:          child,
						parent:        node,
						dirFromParent: dir,
					})
				}
			}
		}
	}

	return reaped
}

func (c *Critbit) Close() error {
	if !c.closed.CompareAndSwap(false, true) {
		return nil
	}
	c.root.Store(nil)
	c.size.Store(0)
	c.arena.clear()
	return nil
}
