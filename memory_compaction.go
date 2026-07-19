package hatriecache

/*
#cgo CFLAGS: -std=c99 -Wall -Wextra -I${SRCDIR}/luikore__hat-trie/src
#include "luikore__hat-trie/src/hat-trie.h"
*/
import "C"

import (
	"context"
	"errors"
	"fmt"
	"math/bits"
	"sync"
	"time"
	"unsafe"
)

// MemoryCompactionResult reports deterministic backing-storage estimates. It
// excludes allocator metadata, nested value payloads, and C allocator pages.
type MemoryCompactionResult struct {
	Entries            int    `json:"entries"`
	BackingBytesBefore uint64 `json:"backing_bytes_before"`
	BackingBytesAfter  uint64 `json:"backing_bytes_after"`
	MerkleBytesBefore  uint64 `json:"merkle_bytes_before"`
	MerkleBytesAfter   uint64 `json:"merkle_bytes_after"`
}

type memoryCompactionPlan struct {
	raws              *BytesStorage
	disks             *DiskStorage
	maps              *MapStorage
	slices            *SliceStorage
	sets              *SetStorage
	priorityQueues    *PriorityQueueStorage
	bloomFilters      *BloomFilterStorage
	countMinSketches  *CountMinSketchStorage
	hyperLogLogs      *HyperLogLogStorage
	topKs             *TopKStorage
	cuckooFilters     *CuckooFilterStorage
	roaringBitmaps    *RoaringBitmapStorage
	quantileSketches  *QuantileSketchStorage
	fenwickTrees      *FenwickTreeStorage
	sparseBitsets     *SparseBitsetStorage
	reservoirSamples  *ReservoirSampleStorage
	xorFilters        *XorFilterStorage
	radixTrees        *RadixTreeStorage
	dbrefs            *LevelDBReferenceStorage
	remaps            [DATAVALUE_TYPE_RADIX_TREE + 1][]int32
	diskRemap         []int32
	replicationMerkle *replicationMerkleIndex
}

// CompactMemory rebuilds the C trie and densely packs in-memory typed backing
// pools. It preserves values and metadata while briefly blocking operations.
func (ht *HatTrie) CompactMemory() (MemoryCompactionResult, error) {
	if ht == nil {
		return MemoryCompactionResult{}, ErrNilHatTrie
	}
	ht.mu.Lock()
	defer ht.mu.Unlock()
	ht.ensureOpen()
	return ht.compactMemoryLocked()
}

func (ht *HatTrie) compactMemoryLocked() (MemoryCompactionResult, error) {
	result := MemoryCompactionResult{
		Entries:            int(C.hattrie_size(ht.root)),
		BackingBytesBefore: ht.memoryBackingBytesLocked(),
		MerkleBytesBefore:  memoryMerkleBytes(ht.replicationMerkle),
	}
	plan, err := ht.buildMemoryCompactionPlanLocked()
	if err != nil {
		return MemoryCompactionResult{}, err
	}

	nextRoot := C.hattrie_dup(ht.root)
	if nextRoot == nil {
		return MemoryCompactionResult{}, errors.New("hatriecache: could not duplicate trie during memory compaction")
	}
	if err := remapCompactedRoot(nextRoot, &plan); err != nil {
		C.hattrie_free(nextRoot)
		return MemoryCompactionResult{}, err
	}

	oldRoot := ht.root
	ht.root = nextRoot
	ht.raws = plan.raws
	ht.disks = plan.disks
	ht.maps = plan.maps
	ht.slices = plan.slices
	ht.sets = plan.sets
	ht.priorityQueues = plan.priorityQueues
	ht.bloomFilters = plan.bloomFilters
	ht.countMinSketches = plan.countMinSketches
	ht.hyperLogLogs = plan.hyperLogLogs
	ht.topKs = plan.topKs
	ht.cuckooFilters = plan.cuckooFilters
	ht.roaringBitmaps = plan.roaringBitmaps
	ht.quantileSketches = plan.quantileSketches
	ht.fenwickTrees = plan.fenwickTrees
	ht.sparseBitsets = plan.sparseBitsets
	ht.reservoirSamples = plan.reservoirSamples
	ht.xorFilters = plan.xorFilters
	ht.radixTrees = plan.radixTrees
	ht.dbrefs = plan.dbrefs
	ht.replicationMerkle = plan.replicationMerkle
	ht.compactAuxiliaryMemoryLocked()
	ht.hotKey = ""
	ht.hotValue = HatValue{}
	ht.hotValid = false
	ht.mutationEpoch++
	ht.memoryCompactionEpoch = ht.mutationEpoch
	C.hattrie_free(oldRoot)

	result.BackingBytesAfter = ht.memoryBackingBytesLocked()
	result.MerkleBytesAfter = memoryMerkleBytes(ht.replicationMerkle)
	return result, nil
}

func (ht *HatTrie) buildMemoryCompactionPlanLocked() (memoryCompactionPlan, error) {
	plan := memoryCompactionPlan{}
	var err error
	if len(ht.raws.strings) != len(ht.raws.array) || len(ht.raws.stringValid) != len(ht.raws.array) {
		return plan, errors.New("hatriecache: raw backing arrays have inconsistent lengths")
	}
	if len(ht.maps.deleted) != len(ht.maps.array) {
		return plan, errors.New("hatriecache: map backing arrays have inconsistent lengths")
	}

	rawArray, rawRemap, err := compactStorageSlice(ht.raws.array, &ht.raws.reusables)
	if err != nil {
		return plan, fmt.Errorf("hatriecache: compact raw values: %w", err)
	}
	plan.raws = &BytesStorage{
		array:       rawArray,
		strings:     compactParallelSlice(ht.raws.strings, rawRemap),
		stringValid: compactParallelSlice(ht.raws.stringValid, rawRemap),
	}
	plan.remaps[DATAVALUE_TYPE_RAW_BYTES] = rawRemap
	plan.remaps[DATAVALUE_TYPE_RAW_STRING] = rawRemap

	plan.disks = &DiskStorage{
		dir:       ht.disks.dir,
		ownedDir:  ht.disks.ownedDir,
		paths:     cloneStorageSliceExact(ht.disks.paths),
		reusables: cloneReusableIndexes(&ht.disks.reusables),
	}
	plan.diskRemap = stableStorageRemap(len(ht.disks.paths), &ht.disks.reusables)

	mapArray, mapRemap, err := compactStorageSlice(ht.maps.array, &ht.maps.reusables)
	if err != nil {
		return memoryCompactionPlan{}, fmt.Errorf("hatriecache: compact maps: %w", err)
	}
	plan.maps = &MapStorage{array: mapArray, deleted: compactParallelSlice(ht.maps.deleted, mapRemap)}
	plan.remaps[DATAVALUE_TYPE_MAP] = mapRemap

	if plan.slices, plan.remaps[DATAVALUE_TYPE_SLICE], err = compactSingleStorage(ht.slices.array, &ht.slices.reusables, func(values []deque) *SliceStorage { return &SliceStorage{array: values} }); err != nil {
		return memoryCompactionPlan{}, err
	}
	if plan.dbrefs, plan.remaps[DATAVALUE_TYPE_LEVELDB_REF], err = compactSingleStorage(ht.dbrefs.array, &ht.dbrefs.reusables, func(values []LevelDBReference) *LevelDBReferenceStorage {
		return &LevelDBReferenceStorage{array: values}
	}); err != nil {
		return memoryCompactionPlan{}, err
	}
	if plan.sets, plan.remaps[DATAVALUE_TYPE_SET], err = compactSingleStorage(ht.sets.array, &ht.sets.reusables, func(values []setData) *SetStorage { return &SetStorage{array: values} }); err != nil {
		return memoryCompactionPlan{}, err
	}
	if plan.priorityQueues, plan.remaps[DATAVALUE_TYPE_PRIORITY_QUEUE], err = compactSingleStorage(ht.priorityQueues.array, &ht.priorityQueues.reusables, func(values []priorityQueueData) *PriorityQueueStorage { return &PriorityQueueStorage{array: values} }); err != nil {
		return memoryCompactionPlan{}, err
	}
	if plan.bloomFilters, plan.remaps[DATAVALUE_TYPE_BLOOM_FILTER], err = compactSingleStorage(ht.bloomFilters.array, &ht.bloomFilters.reusables, func(values []bloomFilterData) *BloomFilterStorage { return &BloomFilterStorage{array: values} }); err != nil {
		return memoryCompactionPlan{}, err
	}
	if plan.countMinSketches, plan.remaps[DATAVALUE_TYPE_COUNT_MIN_SKETCH], err = compactSingleStorage(ht.countMinSketches.array, &ht.countMinSketches.reusables, func(values []countMinSketchData) *CountMinSketchStorage { return &CountMinSketchStorage{array: values} }); err != nil {
		return memoryCompactionPlan{}, err
	}
	if plan.hyperLogLogs, plan.remaps[DATAVALUE_TYPE_HYPERLOGLOG], err = compactSingleStorage(ht.hyperLogLogs.array, &ht.hyperLogLogs.reusables, func(values []hyperLogLogData) *HyperLogLogStorage { return &HyperLogLogStorage{array: values} }); err != nil {
		return memoryCompactionPlan{}, err
	}
	if plan.topKs, plan.remaps[DATAVALUE_TYPE_TOP_K], err = compactSingleStorage(ht.topKs.array, &ht.topKs.reusables, func(values []topKData) *TopKStorage { return &TopKStorage{array: values} }); err != nil {
		return memoryCompactionPlan{}, err
	}
	if plan.cuckooFilters, plan.remaps[DATAVALUE_TYPE_CUCKOO_FILTER], err = compactSingleStorage(ht.cuckooFilters.array, &ht.cuckooFilters.reusables, func(values []cuckooFilterData) *CuckooFilterStorage { return &CuckooFilterStorage{array: values} }); err != nil {
		return memoryCompactionPlan{}, err
	}
	if plan.roaringBitmaps, plan.remaps[DATAVALUE_TYPE_ROARING_BITMAP], err = compactSingleStorage(ht.roaringBitmaps.array, &ht.roaringBitmaps.reusables, func(values []roaringBitmapData) *RoaringBitmapStorage { return &RoaringBitmapStorage{array: values} }); err != nil {
		return memoryCompactionPlan{}, err
	}
	if plan.quantileSketches, plan.remaps[DATAVALUE_TYPE_QUANTILE_SKETCH], err = compactSingleStorage(ht.quantileSketches.array, &ht.quantileSketches.reusables, func(values []quantileSketchData) *QuantileSketchStorage { return &QuantileSketchStorage{array: values} }); err != nil {
		return memoryCompactionPlan{}, err
	}
	if plan.fenwickTrees, plan.remaps[DATAVALUE_TYPE_FENWICK_TREE], err = compactSingleStorage(ht.fenwickTrees.array, &ht.fenwickTrees.reusables, func(values []fenwickTreeData) *FenwickTreeStorage { return &FenwickTreeStorage{array: values} }); err != nil {
		return memoryCompactionPlan{}, err
	}
	if plan.sparseBitsets, plan.remaps[DATAVALUE_TYPE_SPARSE_BITSET], err = compactSingleStorage(ht.sparseBitsets.array, &ht.sparseBitsets.reusables, func(values []sparseBitsetData) *SparseBitsetStorage { return &SparseBitsetStorage{array: values} }); err != nil {
		return memoryCompactionPlan{}, err
	}
	if plan.reservoirSamples, plan.remaps[DATAVALUE_TYPE_RESERVOIR_SAMPLE], err = compactSingleStorage(ht.reservoirSamples.array, &ht.reservoirSamples.reusables, func(values []reservoirSampleData) *ReservoirSampleStorage {
		return &ReservoirSampleStorage{array: values}
	}); err != nil {
		return memoryCompactionPlan{}, err
	}
	if plan.xorFilters, plan.remaps[DATAVALUE_TYPE_XOR_FILTER], err = compactSingleStorage(ht.xorFilters.array, &ht.xorFilters.reusables, func(values []xorFilterData) *XorFilterStorage { return &XorFilterStorage{array: values} }); err != nil {
		return memoryCompactionPlan{}, err
	}
	if plan.radixTrees, plan.remaps[DATAVALUE_TYPE_RADIX_TREE], err = compactSingleStorage(ht.radixTrees.array, &ht.radixTrees.reusables, func(values []radixTreeData) *RadixTreeStorage { return &RadixTreeStorage{array: values} }); err != nil {
		return memoryCompactionPlan{}, err
	}
	plan.replicationMerkle = compactReplicationMerkleIndex(ht.replicationMerkle)
	return plan, nil
}

func compactSingleStorage[T any, S any](source []T, reusables *reusableIndexes, build func([]T) *S) (*S, []int32, error) {
	values, remap, err := compactStorageSlice(source, reusables)
	if err != nil {
		return nil, nil, err
	}
	return build(values), remap, nil
}

func compactStorageSlice[T any](source []T, reusables *reusableIndexes) ([]T, []int32, error) {
	if reusables == nil {
		return nil, nil, errors.New("missing reusable-index state")
	}
	remap := make([]int32, len(source))
	live := 0
	for idx := range source {
		if reusables.Has(int32(idx)) {
			remap[idx] = -1
			continue
		}
		if live > int(^uint32(0)>>1) {
			return nil, nil, errors.New("compacted storage index exceeds int32")
		}
		remap[idx] = int32(live)
		live++
	}
	if len(source)-live != reusables.Len() {
		return nil, nil, errors.New("reusable-index count does not match storage holes")
	}
	return compactParallelSlice(source, remap), remap, nil
}

func stableStorageRemap(length int, reusables *reusableIndexes) []int32 {
	remap := make([]int32, length)
	for idx := range remap {
		if reusables != nil && reusables.Has(int32(idx)) {
			remap[idx] = -1
		} else {
			remap[idx] = int32(idx)
		}
	}
	return remap
}

func cloneReusableIndexes(source *reusableIndexes) reusableIndexes {
	if source == nil || source.count == 0 {
		return reusableIndexes{}
	}
	return reusableIndexes{
		stack: cloneStorageSliceExact(source.stack),
		bits:  cloneStorageSliceExact(source.bits),
		count: source.count,
	}
}

func cloneStorageSliceExact[T any](source []T) []T {
	if len(source) == 0 {
		return nil
	}
	next := make([]T, len(source))
	copy(next, source)
	return next
}

func compactParallelSlice[T any](source []T, remap []int32) []T {
	live := 0
	for _, next := range remap {
		if next >= 0 {
			live++
		}
	}
	if live == 0 {
		return nil
	}
	next := make([]T, live)
	for oldIndex, nextIndex := range remap {
		if nextIndex >= 0 && oldIndex < len(source) {
			next[nextIndex] = source[oldIndex]
		}
	}
	return next
}

func remapCompactedRoot(root *C.hattrie_t, plan *memoryCompactionPlan) error {
	iterator := C.hattrie_iter_begin(root, C.bool(false))
	defer C.hattrie_iter_free(iterator)
	for !bool(C.hattrie_iter_finished(iterator)) {
		var keyLength C.size_t
		key := C.hattrie_iter_key(iterator, &keyLength)
		location := C.hattrie_tryget(root, key, keyLength)
		if location == nil {
			return errors.New("hatriecache: compacted trie iterator key has no value")
		}
		value := HatValue{}
		value.fromValue(*location)
		if err := plan.remapValue(&value); err != nil {
			return err
		}
		*location = value.toValue()
		C.hattrie_iter_next(iterator)
	}
	return nil
}

func (plan *memoryCompactionPlan) remapValue(value *HatValue) error {
	if value == nil || value.Empty() || value.IsCounter() {
		return nil
	}
	var remap []int32
	if value.IsBytesAtRaws() && value.OnDisk() {
		remap = plan.diskRemap
	} else if int(value.Type()) < len(plan.remaps) {
		remap = plan.remaps[value.Type()]
	}
	oldIndex := int(value.Index)
	if oldIndex < 0 || oldIndex >= len(remap) || remap[oldIndex] < 0 {
		return fmt.Errorf("hatriecache: live %s references missing backing index %d", value.String(), oldIndex)
	}
	value.Index = remap[oldIndex]
	return nil
}

func compactReplicationMerkleIndex(source *replicationMerkleIndex) *replicationMerkleIndex {
	if source == nil || !source.valid {
		return nil
	}
	capacity := replicationMerkleInitialTableCapacity
	if source.table.count > 0 {
		minimum := (source.table.count*10 + 6) / 7
		if minimum > capacity {
			capacity = 1 << bits.Len(uint(minimum-1))
		}
	}
	next := &replicationMerkleIndex{
		table: replicationMerkleTable{
			keys:   make([]uint64, capacity),
			values: make([]uint64, capacity),
			used:   make([]uint8, capacity),
		},
		leaves: source.leaves,
		count:  source.count,
		valid:  true,
	}
	for slot, used := range source.table.used {
		if used != 0 {
			next.table.setWithoutResize(source.table.keys[slot], source.table.values[slot])
		}
	}
	return next
}

func (ht *HatTrie) compactAuxiliaryMemoryLocked() {
	if len(ht.expires) == 0 {
		ht.expires = map[string]uint32{}
		ht.expirations = nil
	} else {
		nextExpires := make(map[string]uint32, len(ht.expires))
		for key, at := range ht.expires {
			nextExpires[key] = at
		}
		ht.expires = nextExpires
		ht.rebuildExpirationHeapLocked()
	}
	ht.compactKeyStatsLocked()
	ht.levelDBSpillKeys = cloneStringSet(ht.levelDBSpillKeys)
	ht.levelDBHotValues = cloneStringInt64Map(ht.levelDBHotValues)
}

func (ht *HatTrie) compactKeyStatsLocked() {
	if len(ht.keyStats) == 0 {
		ht.keyStats = map[string]*trackedKeyStats{}
		ht.keyStatsSlots = nil
		ht.keyStatsFree = nil
		ht.keyStatsHand = 0
		return
	}
	nextStats := make(map[string]*trackedKeyStats, len(ht.keyStats))
	for key, stats := range ht.keyStats {
		nextStats[key] = stats
	}
	ht.keyStats = nextStats
	if ht.keyStatsMode != KeyStatsModeBounded {
		ht.keyStatsSlots = nil
		ht.keyStatsFree = nil
		ht.keyStatsHand = 0
		for _, stats := range ht.keyStats {
			stats.slot = keyStatsNoSlot
		}
		return
	}
	nextSlots := make([]string, 0, len(ht.keyStats))
	seen := make(map[string]struct{}, len(ht.keyStats))
	for _, key := range ht.keyStatsSlots {
		if key == "" {
			continue
		}
		if _, ok := ht.keyStats[key]; !ok {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		nextSlots = append(nextSlots, key)
	}
	for key := range ht.keyStats {
		if _, ok := seen[key]; !ok {
			nextSlots = append(nextSlots, key)
		}
	}
	for slot, key := range nextSlots {
		ht.keyStats[key].slot = uint32(slot)
	}
	ht.keyStatsSlots = nextSlots
	ht.keyStatsFree = nil
	if len(nextSlots) == 0 {
		ht.keyStatsHand = 0
	} else {
		ht.keyStatsHand %= len(nextSlots)
	}
}

func cloneStringSet(source map[string]struct{}) map[string]struct{} {
	if source == nil {
		return nil
	}
	next := make(map[string]struct{}, len(source))
	for key := range source {
		next[key] = struct{}{}
	}
	return next
}

func cloneStringInt64Map(source map[string]int64) map[string]int64 {
	if source == nil {
		return nil
	}
	next := make(map[string]int64, len(source))
	for key, value := range source {
		next[key] = value
	}
	return next
}

func memoryMerkleBytes(index *replicationMerkleIndex) uint64 {
	if index == nil {
		return 0
	}
	return uint64(index.table.retainedBytes() + replicationMerkleBucketCount*16 + cap(index.scratch))
}

func (ht *HatTrie) memoryBackingBytesLocked() uint64 {
	if ht == nil {
		return 0
	}
	var total uint64
	total += storageSliceBytes(ht.raws.array) + storageSliceBytes(ht.raws.strings) + storageSliceBytes(ht.raws.stringValid) + reusableBackingBytes(&ht.raws.reusables)
	total += storageSliceBytes(ht.disks.paths) + reusableBackingBytes(&ht.disks.reusables)
	total += storageSliceBytes(ht.maps.array) + storageSliceBytes(ht.maps.deleted) + reusableBackingBytes(&ht.maps.reusables)
	total += storageSliceBytes(ht.slices.array) + reusableBackingBytes(&ht.slices.reusables)
	total += storageSliceBytes(ht.sets.array) + reusableBackingBytes(&ht.sets.reusables)
	total += storageSliceBytes(ht.priorityQueues.array) + reusableBackingBytes(&ht.priorityQueues.reusables)
	total += storageSliceBytes(ht.bloomFilters.array) + reusableBackingBytes(&ht.bloomFilters.reusables)
	total += storageSliceBytes(ht.countMinSketches.array) + reusableBackingBytes(&ht.countMinSketches.reusables)
	total += storageSliceBytes(ht.hyperLogLogs.array) + reusableBackingBytes(&ht.hyperLogLogs.reusables)
	total += storageSliceBytes(ht.topKs.array) + reusableBackingBytes(&ht.topKs.reusables)
	total += storageSliceBytes(ht.cuckooFilters.array) + reusableBackingBytes(&ht.cuckooFilters.reusables)
	total += storageSliceBytes(ht.roaringBitmaps.array) + reusableBackingBytes(&ht.roaringBitmaps.reusables)
	total += storageSliceBytes(ht.quantileSketches.array) + reusableBackingBytes(&ht.quantileSketches.reusables)
	total += storageSliceBytes(ht.fenwickTrees.array) + reusableBackingBytes(&ht.fenwickTrees.reusables)
	total += storageSliceBytes(ht.sparseBitsets.array) + reusableBackingBytes(&ht.sparseBitsets.reusables)
	total += storageSliceBytes(ht.reservoirSamples.array) + reusableBackingBytes(&ht.reservoirSamples.reusables)
	total += storageSliceBytes(ht.xorFilters.array) + reusableBackingBytes(&ht.xorFilters.reusables)
	total += storageSliceBytes(ht.radixTrees.array) + reusableBackingBytes(&ht.radixTrees.reusables)
	total += storageSliceBytes(ht.dbrefs.array) + reusableBackingBytes(&ht.dbrefs.reusables)
	total += memoryMerkleBytes(ht.replicationMerkle)
	return total
}

func storageSliceBytes[T any](values []T) uint64 {
	var value T
	return uint64(cap(values)) * uint64(unsafe.Sizeof(value))
}

func reusableBackingBytes(indexes *reusableIndexes) uint64 {
	if indexes == nil {
		return 0
	}
	return uint64(cap(indexes.stack))*uint64(unsafe.Sizeof(int32(0))) +
		uint64(cap(indexes.bits))*uint64(unsafe.Sizeof(uint64(0)))
}

// StartMemoryCompactor starts an opt-in background compactor. It skips ticks
// when the trie has not changed since the preceding compaction.
func (ht *HatTrie) StartMemoryCompactor(interval time.Duration) func() {
	return ht.StartMemoryCompactorContext(context.Background(), interval)
}

// StartMemoryCompactorContext runs CompactMemory periodically until stopped,
// the context is canceled, or the trie is destroyed.
func (ht *HatTrie) StartMemoryCompactorContext(ctx context.Context, interval time.Duration) func() {
	if interval <= 0 {
		panic("hatriecache: memory compactor interval must be positive")
	}
	if ht == nil {
		return func() {}
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ticker := time.NewTicker(interval)
	done := make(chan struct{})
	stopped := make(chan struct{})
	var stopOnce sync.Once
	go func() {
		defer close(stopped)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if !ht.compactMemoryIfChangedAndOpen() {
					return
				}
			case <-ctx.Done():
				return
			case <-done:
				return
			}
		}
	}()
	return func() {
		stopOnce.Do(func() {
			close(done)
			<-stopped
		})
	}
}

func (ht *HatTrie) compactMemoryIfChangedAndOpen() bool {
	ht.mu.Lock()
	defer ht.mu.Unlock()
	if ht.root == nil {
		return false
	}
	if ht.memoryCompactionEpoch == ht.mutationEpoch {
		return true
	}
	_, _ = ht.compactMemoryLocked()
	return true
}
