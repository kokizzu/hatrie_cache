package hatriecache

import (
	"encoding/base64"
	"encoding/binary"
	"errors"
	"math/bits"
	"time"

	"github.com/cespare/xxhash/v2"
)

const replicationMerkleBucketCount = 1024
const replicationMerkleInitialTableCapacity = 1024
const replicationMerklePendingInlineLimit = 32
const replicationMerkleMaxPendingKeys = 1024

type replicationMerkleLeaf struct {
	xor   uint64
	count uint64
}

type replicationMerkleBucketMask [replicationMerkleBucketCount / 64]uint64

func (mask replicationMerkleBucketMask) contains(bucket int) bool {
	return bucket >= 0 && bucket < replicationMerkleBucketCount && mask[bucket/64]&(uint64(1)<<uint(bucket%64)) != 0
}

func (mask replicationMerkleBucketMask) containsKey(key string) bool {
	return mask.contains(replicationMerkleBucket(xxhash.Sum64String(key)))
}

func (mask replicationMerkleBucketMask) empty() bool {
	for _, word := range mask {
		if word != 0 {
			return false
		}
	}
	return true
}

func encodeReplicationMerkleBucketMask(mask replicationMerkleBucketMask) string {
	var data [len(mask) * 8]byte
	for index, word := range mask {
		binary.LittleEndian.PutUint64(data[index*8:], word)
	}
	return base64.RawStdEncoding.EncodeToString(data[:])
}

func decodeReplicationMerkleBucketMask(value string) (replicationMerkleBucketMask, error) {
	mask := replicationMerkleBucketMask{}
	data := make([]byte, len(mask)*8)
	if len(value) != base64.RawStdEncoding.EncodedLen(len(data)) {
		return mask, errors.New("hatriecache: invalid replication Merkle bucket mask")
	}
	n, err := base64.RawStdEncoding.Decode(data, []byte(value))
	if err != nil || n != len(data) {
		return mask, errors.New("hatriecache: invalid replication Merkle bucket mask")
	}
	for index := range mask {
		mask[index] = binary.LittleEndian.Uint64(data[index*8:])
	}
	return mask, nil
}

type replicationMerkleSnapshot struct {
	root   uint64
	count  uint64
	leaves [replicationMerkleBucketCount]replicationMerkleLeaf
}

func (snapshot replicationMerkleSnapshot) equal(other replicationMerkleSnapshot) bool {
	return snapshot.root == other.root && snapshot.count == other.count
}

func (snapshot replicationMerkleSnapshot) changedBuckets(other replicationMerkleSnapshot) replicationMerkleBucketMask {
	mask := replicationMerkleBucketMask{}
	for bucket := range snapshot.leaves {
		if snapshot.leaves[bucket] != other.leaves[bucket] {
			mask[bucket/64] |= uint64(1) << uint(bucket%64)
		}
	}
	return mask
}

func encodeReplicationMerkleLeaves(snapshot replicationMerkleSnapshot) string {
	data := make([]byte, replicationMerkleBucketCount*16)
	for bucket, leaf := range snapshot.leaves {
		offset := bucket * 16
		binary.LittleEndian.PutUint64(data[offset:], leaf.xor)
		binary.LittleEndian.PutUint64(data[offset+8:], leaf.count)
	}
	return base64.RawStdEncoding.EncodeToString(data)
}

func decodeReplicationMerkleLeaves(value string) (replicationMerkleSnapshot, error) {
	snapshot := replicationMerkleSnapshot{}
	data := make([]byte, replicationMerkleBucketCount*16)
	if len(value) != base64.RawStdEncoding.EncodedLen(len(data)) {
		return snapshot, errors.New("hatriecache: invalid replication Merkle leaves")
	}
	n, err := base64.RawStdEncoding.Decode(data, []byte(value))
	if err != nil || n != len(data) {
		return snapshot, errors.New("hatriecache: invalid replication Merkle leaves")
	}
	for bucket := range snapshot.leaves {
		offset := bucket * 16
		snapshot.leaves[bucket] = replicationMerkleLeaf{
			xor:   binary.LittleEndian.Uint64(data[offset:]),
			count: binary.LittleEndian.Uint64(data[offset+8:]),
		}
		snapshot.count += snapshot.leaves[bucket].count
	}
	snapshot.root = replicationMerkleRoot(snapshot.leaves, snapshot.count)
	return snapshot, nil
}

type replicationMerkleTable struct {
	keys   []uint64
	values []uint64
	used   []uint8
	count  int
}

func newReplicationMerkleTable() replicationMerkleTable {
	return replicationMerkleTable{
		keys:   make([]uint64, replicationMerkleInitialTableCapacity),
		values: make([]uint64, replicationMerkleInitialTableCapacity),
		used:   make([]uint8, replicationMerkleInitialTableCapacity),
	}
}

func (table *replicationMerkleTable) get(key uint64) (uint64, bool) {
	if table == nil || len(table.keys) == 0 {
		return 0, false
	}
	mask := len(table.keys) - 1
	for slot := int(key) & mask; table.used[slot] != 0; slot = (slot + 1) & mask {
		if table.keys[slot] == key {
			return table.values[slot], true
		}
	}
	return 0, false
}

func (table *replicationMerkleTable) set(key uint64, value uint64) (uint64, bool) {
	if table == nil {
		return 0, false
	}
	if len(table.keys) == 0 {
		*table = newReplicationMerkleTable()
	}
	mask := len(table.keys) - 1
	for slot := int(key) & mask; ; slot = (slot + 1) & mask {
		if table.used[slot] == 0 {
			if (table.count+1)*10 > len(table.keys)*7 {
				table.resize(len(table.keys) * 2)
				return table.setWithoutResize(key, value)
			}
			table.used[slot] = 1
			table.keys[slot] = key
			table.values[slot] = value
			table.count++
			return 0, false
		}
		if table.keys[slot] == key {
			previous := table.values[slot]
			table.values[slot] = value
			return previous, true
		}
	}
}

func (table *replicationMerkleTable) setWithoutResize(key uint64, value uint64) (uint64, bool) {
	mask := len(table.keys) - 1
	for slot := int(key) & mask; ; slot = (slot + 1) & mask {
		if table.used[slot] == 0 {
			table.used[slot] = 1
			table.keys[slot] = key
			table.values[slot] = value
			table.count++
			return 0, false
		}
		if table.keys[slot] == key {
			previous := table.values[slot]
			table.values[slot] = value
			return previous, true
		}
	}
}

func (table *replicationMerkleTable) delete(key uint64) (uint64, bool) {
	if table == nil || len(table.keys) == 0 {
		return 0, false
	}
	mask := len(table.keys) - 1
	slot := int(key) & mask
	for table.used[slot] != 0 && table.keys[slot] != key {
		slot = (slot + 1) & mask
	}
	if table.used[slot] == 0 {
		return 0, false
	}
	previous := table.values[slot]
	hole := slot
	for next := (hole + 1) & mask; table.used[next] != 0; next = (next + 1) & mask {
		home := int(table.keys[next]) & mask
		if (next-home)&mask < (next-hole)&mask {
			continue
		}
		table.keys[hole] = table.keys[next]
		table.values[hole] = table.values[next]
		table.used[hole] = 1
		hole = next
	}
	table.keys[hole] = 0
	table.values[hole] = 0
	table.used[hole] = 0
	table.count--
	return previous, true
}

func (table *replicationMerkleTable) resize(capacity int) {
	if capacity < replicationMerkleInitialTableCapacity {
		capacity = replicationMerkleInitialTableCapacity
	}
	capacity = 1 << bits.Len(uint(capacity-1))
	previous := *table
	*table = replicationMerkleTable{
		keys:   make([]uint64, capacity),
		values: make([]uint64, capacity),
		used:   make([]uint8, capacity),
	}
	for slot, state := range previous.used {
		if state != 0 {
			table.setWithoutResize(previous.keys[slot], previous.values[slot])
		}
	}
}

func (table *replicationMerkleTable) retainedBytes() int {
	if table == nil {
		return 0
	}
	return len(table.keys)*8 + len(table.values)*8 + len(table.used)
}

type replicationMerkleIndex struct {
	table         replicationMerkleTable
	leaves        [replicationMerkleBucketCount]replicationMerkleLeaf
	count         uint64
	valid         bool
	scratch       []byte
	pendingInline []replicationMerklePendingKey
	pending       map[uint64]string
}

type replicationMerklePendingKey struct {
	hash uint64
	key  string
}

func newReplicationMerkleIndex() *replicationMerkleIndex {
	return &replicationMerkleIndex{table: newReplicationMerkleTable(), valid: true}
}

func (index *replicationMerkleIndex) set(keyHash uint64, contribution uint64) {
	bucket := replicationMerkleBucket(keyHash)
	previous, existed := index.table.set(keyHash, contribution)
	if existed {
		index.leaves[bucket].xor ^= previous ^ contribution
		return
	}
	index.leaves[bucket].xor ^= contribution
	index.leaves[bucket].count++
	index.count++
}

func (index *replicationMerkleIndex) delete(keyHash uint64) {
	previous, existed := index.table.delete(keyHash)
	if !existed {
		return
	}
	bucket := replicationMerkleBucket(keyHash)
	index.leaves[bucket].xor ^= previous
	index.leaves[bucket].count--
	index.count--
}

func (index *replicationMerkleIndex) snapshot() replicationMerkleSnapshot {
	snapshot := replicationMerkleSnapshot{count: index.count, leaves: index.leaves}
	snapshot.root = replicationMerkleRoot(snapshot.leaves, snapshot.count)
	return snapshot
}

func (index *replicationMerkleIndex) deferUpdate(key string) {
	if index == nil || !index.valid {
		return
	}
	keyHash := xxhash.Sum64String(key)
	if index.pending != nil {
		index.pending[keyHash] = key
		if len(index.pending) > replicationMerkleMaxPendingKeys {
			index.invalidate()
		}
		return
	}
	for idx := range index.pendingInline {
		if index.pendingInline[idx].hash == keyHash {
			index.pendingInline[idx].key = key
			return
		}
	}
	if len(index.pendingInline) < replicationMerklePendingInlineLimit {
		index.pendingInline = append(index.pendingInline, replicationMerklePendingKey{hash: keyHash, key: key})
		return
	}
	index.pending = make(map[uint64]string, len(index.pendingInline)+1)
	for _, pending := range index.pendingInline {
		index.pending[pending.hash] = pending.key
	}
	index.pendingInline = nil
	index.pending[keyHash] = key
}

func (index *replicationMerkleIndex) pendingCount() int {
	if index == nil {
		return 0
	}
	if index.pending != nil {
		return len(index.pending)
	}
	return len(index.pendingInline)
}

func (index *replicationMerkleIndex) clearPending() {
	if index == nil {
		return
	}
	for idx := range index.pendingInline {
		index.pendingInline[idx] = replicationMerklePendingKey{}
	}
	index.pendingInline = index.pendingInline[:0]
	index.pending = nil
}

func (index *replicationMerkleIndex) invalidate() {
	if index == nil {
		return
	}
	index.valid = false
	index.clearPending()
}

func (index *replicationMerkleIndex) retainedBytes() int {
	if index == nil {
		return 0
	}
	retained := index.table.retainedBytes() + replicationMerkleBucketCount*16 + cap(index.scratch)
	retained += cap(index.pendingInline) * 24
	retained += len(index.pending) * 24
	return retained
}

func replicationMerkleBucket(keyHash uint64) int {
	return int(keyHash >> (64 - 10))
}

func replicationMerkleContribution(keyHash uint64, digest replicationDigest) uint64 {
	value := keyHash ^ bits.RotateLeft64(digest.hash, 23) ^ digest.size*0x9e3779b97f4a7c15
	value ^= value >> 30
	value *= 0xbf58476d1ce4e5b9
	value ^= value >> 27
	value *= 0x94d049bb133111eb
	return value ^ (value >> 31)
}

func replicationMerkleRoot(leaves [replicationMerkleBucketCount]replicationMerkleLeaf, count uint64) uint64 {
	root := count ^ 0x6a09e667f3bcc909
	for bucket, leaf := range leaves {
		if leaf.count == 0 {
			continue
		}
		value := leaf.xor ^ bits.RotateLeft64(leaf.count, bucket%63) ^ uint64(bucket)*0x9e3779b97f4a7c15
		root ^= replicationMerkleContribution(uint64(bucket), replicationDigest{hash: value, size: leaf.count})
	}
	return root
}

func (ht *HatTrie) updateReplicationMerkleLocked(keys ...string) {
	if ht == nil || ht.replicationMerkle == nil || !ht.replicationMerkle.valid {
		return
	}
	for _, key := range keys {
		ht.replicationMerkle.deferUpdate(key)
	}
}

func (ht *HatTrie) flushReplicationMerkleLocked(index *replicationMerkleIndex) error {
	if index == nil || !index.valid || index.pendingCount() == 0 {
		return nil
	}
	apply := func(keyHash uint64, key string) error {
		rawPtr := ht.tryLocation(key)
		if rawPtr == nil {
			index.delete(keyHash)
			return nil
		}
		hval := HatValue{}
		hval.fromValue(*rawPtr)
		encoded, ok, err := ht.appendCommandDumpScannedEntryBinaryWithoutStatsLocked(index.scratch[:0], Entry{Key: key, Value: hval})
		index.scratch = encoded
		if err != nil {
			return err
		}
		if !ok {
			return errors.New("hatriecache: Merkle index could not encode live entry")
		}
		index.set(keyHash, replicationMerkleContribution(keyHash, replicationValueDigest(encoded)))
		return nil
	}
	if index.pending != nil {
		for keyHash, key := range index.pending {
			if err := apply(keyHash, key); err != nil {
				index.invalidate()
				return err
			}
		}
	} else {
		for _, pending := range index.pendingInline {
			if err := apply(pending.hash, pending.key); err != nil {
				index.invalidate()
				return err
			}
		}
	}
	index.clearPending()
	return nil
}

func (ht *HatTrie) invalidateReplicationMerkleLocked() {
	if ht != nil && ht.replicationMerkle != nil {
		ht.replicationMerkle.invalidate()
	}
}

func (ht *HatTrie) replicationMerkleSnapshot() (replicationMerkleSnapshot, error) {
	if ht == nil {
		return replicationMerkleSnapshot{}, ErrNilHatTrie
	}
	if partitions := ht.localPartitionSet(); partitions != nil {
		var combined replicationMerkleSnapshot
		snapshots, err := runLocalPartitionTasks(partitions, func(child *HatTrie) (replicationMerkleSnapshot, error) {
			return child.replicationMerkleSnapshot()
		})
		if err != nil {
			return replicationMerkleSnapshot{}, err
		}
		for _, snapshot := range snapshots {
			combined.count += snapshot.count
			for bucket := range combined.leaves {
				combined.leaves[bucket].xor ^= snapshot.leaves[bucket].xor
				combined.leaves[bucket].count += snapshot.leaves[bucket].count
			}
		}
		combined.root = replicationMerkleRoot(combined.leaves, combined.count)
		return combined, nil
	}
	ht.mu.Lock()
	defer ht.mu.Unlock()
	ht.ensureOpen()
	if ht.replicationMerkle != nil && ht.replicationMerkle.valid {
		// A failed flush invalidates the index; rebuilding below retries from live data.
		_ = ht.flushReplicationMerkleLocked(ht.replicationMerkle)
	}
	if ht.replicationMerkle == nil || !ht.replicationMerkle.valid {
		index, err := ht.rebuildReplicationMerkleLocked()
		if err != nil {
			return replicationMerkleSnapshot{}, err
		}
		ht.replicationMerkle = index
	}
	return ht.replicationMerkle.snapshot(), nil
}

func (ht *HatTrie) rebuildReplicationMerkleLocked() (*replicationMerkleIndex, error) {
	index := newReplicationMerkleIndex()
	scan, err := ht.newPackedScanCursorLocked("", false)
	if err != nil {
		return nil, err
	}
	defer scan.closeLocked(ht)
	now := time.Time{}
	if len(ht.expires) > 0 {
		now = ht.currentTime()
	}
	for {
		entry, ok := scan.currentLiveEntryLocked(ht, now)
		if !ok {
			return index, nil
		}
		var dumpErr error
		index.scratch, ok, dumpErr = ht.appendCommandDumpScannedEntryBinaryWithoutStatsLocked(index.scratch[:0], entry)
		if dumpErr != nil {
			return nil, dumpErr
		}
		if !ok {
			return nil, errors.New("hatriecache: Merkle index could not encode live entry")
		}
		keyHash := xxhash.Sum64String(entry.Key)
		index.set(keyHash, replicationMerkleContribution(keyHash, replicationValueDigest(index.scratch)))
		scan.consume()
	}
}

func (ht *HatTrie) replicationMerkleRetainedBytes() int {
	if ht == nil {
		return 0
	}
	if partitions := ht.localPartitionSet(); partitions != nil {
		total := 0
		for _, child := range partitions.tries {
			total += child.replicationMerkleRetainedBytes()
		}
		return total
	}
	ht.mu.RLock()
	defer ht.mu.RUnlock()
	if ht.replicationMerkle == nil {
		return 0
	}
	return ht.replicationMerkle.retainedBytes()
}
