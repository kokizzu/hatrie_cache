package hatriecache

import (
	"errors"
	"fmt"
	"math/bits"
	"runtime"
	"time"
)

const memoryCompactionCutoverMaxKeys = 64
const memoryCompactionMaxCatchUpRounds = 8

var errMemoryCompactionClosed = errors.New("hatriecache: memory compaction requires an open trie")

type memoryCompactionEntry struct {
	key       string
	entry     snapshotEntry
	reference LevelDBReference
	present   bool
	isRef     bool
}

type memoryCompactionGeneration struct {
	trie    *HatTrie
	strings packedStringBuilder
}

func (ht *HatTrie) compactMemoryOnline() (MemoryCompactionResult, error) {
	ht.snapshotCaptureMu.Lock()
	defer ht.snapshotCaptureMu.Unlock()

	stage, err := newSnapshotRestoreTrie(ht)
	if err != nil {
		return MemoryCompactionResult{}, err
	}
	adopted := false
	defer func() {
		if !adopted {
			stage.Destroy()
		}
	}()
	generation := &memoryCompactionGeneration{trie: stage}

	tracker := &snapshotMutationTracker{}
	ht.mu.Lock()
	if ht.root == nil {
		ht.mu.Unlock()
		return MemoryCompactionResult{}, errMemoryCompactionClosed
	}
	if ht.snapshotMutations != nil {
		ht.mu.Unlock()
		return MemoryCompactionResult{}, errors.New("hatriecache: snapshot mutation tracking is already active")
	}
	if err := ht.validateMemoryCompactionBackingLocked(); err != nil {
		ht.mu.Unlock()
		return MemoryCompactionResult{}, err
	}
	result := MemoryCompactionResult{
		Entries:            ht.sizeLocked(),
		BackingBytesBefore: ht.memoryBackingBytesLocked(),
		MerkleBytesBefore:  memoryMerkleBytes(ht.replicationMerkle),
	}
	ht.snapshotMutations = tracker
	ht.mu.Unlock()

	trackerActive := true
	defer func() {
		if !trackerActive {
			return
		}
		ht.mu.Lock()
		if ht.snapshotMutations == tracker {
			ht.snapshotMutations = nil
		}
		ht.mu.Unlock()
	}()

	if err := ht.buildMemoryCompactionGeneration(generation); err != nil {
		return MemoryCompactionResult{}, err
	}

	for {
		ht.mu.Lock()
		if ht.root == nil {
			ht.mu.Unlock()
			return MemoryCompactionResult{}, errMemoryCompactionClosed
		}
		if ht.snapshotMutations != tracker {
			ht.mu.Unlock()
			return MemoryCompactionResult{}, errors.New("hatriecache: memory compaction mutation tracker changed")
		}
		keys := tracker.drain()
		if len(keys) <= memoryCompactionCutoverMaxKeys || result.CatchUpRounds >= memoryCompactionMaxCatchUpRounds {
			cutoverStarted := time.Now()
			for len(keys) > 0 {
				entries, captureErr := ht.captureMemoryCompactionKeysLocked(keys)
				if captureErr != nil {
					ht.mu.Unlock()
					return MemoryCompactionResult{}, captureErr
				}
				if applyErr := applyMemoryCompactionEntries(generation, entries); applyErr != nil {
					ht.mu.Unlock()
					return MemoryCompactionResult{}, applyErr
				}
				result.ReplayedKeys += len(keys)
				keys = tracker.drain()
			}
			if !snapshotRestoreConfigurationMatches(ht, stage) || ht.localPartitionSet() != nil {
				ht.mu.Unlock()
				return MemoryCompactionResult{}, errors.New("hatriecache: trie configuration changed during memory compaction")
			}
			nextMerkle := compactReplicationMerkleIndex(ht.replicationMerkle)
			ht.snapshotMutations = nil
			trackerActive = false
			adoptSnapshotDiskOwnership(ht.disks, stage.disks)
			swapMemoryCompactionGenerationLocked(ht, stage, nextMerkle)
			ht.hotKey = ""
			ht.hotValue = HatValue{}
			ht.hotValid = false
			ht.mutationEpoch++
			ht.memoryCompactionEpoch = ht.mutationEpoch
			result.Entries = ht.sizeLocked()
			result.BackingBytesAfter = ht.memoryBackingBytesLocked()
			result.MerkleBytesAfter = memoryMerkleBytes(ht.replicationMerkle)
			result.CutoverNanos = time.Since(cutoverStarted).Nanoseconds()
			ht.mu.Unlock()

			adopted = true
			stage.Destroy()
			return result, nil
		}
		ht.mu.Unlock()

		if err := ht.replayMemoryCompactionKeys(generation, keys); err != nil {
			return MemoryCompactionResult{}, err
		}
		result.ReplayedKeys += len(keys)
		result.CatchUpRounds++
	}
}

func (ht *HatTrie) buildMemoryCompactionGeneration(generation *memoryCompactionGeneration) error {
	cursor := &replicationSyncCursor{}
	defer cursor.close(ht)
	afterKey := ""
	hasAfterKey := false
	pageNumber := 0
	for {
		entries := make([]memoryCompactionEntry, 0, snapshotCaptureScanPageEntries)
		page, err := replicationSyncEntriesPageWithCursor(ht, "", afterKey, hasAfterKey, snapshotCaptureScanPageEntries, cursor, func(entry Entry) error {
			captured, captureErr := ht.captureMemoryCompactionLiveEntryLocked(entry)
			if captureErr != nil {
				return captureErr
			}
			entries = append(entries, captured)
			return nil
		})
		if err != nil {
			return err
		}
		if err := applyMemoryCompactionEntries(generation, entries); err != nil {
			return err
		}
		pageNumber++
		if hook := ht.snapshotCapturePageHook; hook != nil {
			hook(pageNumber)
		}
		if !page.hasMore {
			return nil
		}
		afterKey = page.nextAfterKey
		hasAfterKey = true
		runtime.Gosched()
	}
}

func (ht *HatTrie) replayMemoryCompactionKeys(generation *memoryCompactionGeneration, keys []string) error {
	for first := 0; first < len(keys); first += snapshotCaptureScanPageEntries {
		last := first + snapshotCaptureScanPageEntries
		if last > len(keys) {
			last = len(keys)
		}
		ht.mu.Lock()
		if ht.root == nil {
			ht.mu.Unlock()
			return errMemoryCompactionClosed
		}
		entries, err := ht.captureMemoryCompactionKeysLocked(keys[first:last])
		ht.mu.Unlock()
		if err != nil {
			return err
		}
		if err := applyMemoryCompactionEntries(generation, entries); err != nil {
			return err
		}
		runtime.Gosched()
	}
	return nil
}

func (ht *HatTrie) captureMemoryCompactionKeysLocked(keys []string) ([]memoryCompactionEntry, error) {
	entries := make([]memoryCompactionEntry, 0, len(keys))
	for _, key := range keys {
		captured, err := ht.captureMemoryCompactionKeyLocked(key)
		if err != nil {
			return nil, err
		}
		entries = append(entries, captured)
	}
	return entries, nil
}

func (ht *HatTrie) captureMemoryCompactionKeyLocked(key string) (memoryCompactionEntry, error) {
	rawPtr := ht.tryLocation(key)
	if rawPtr == nil {
		return memoryCompactionEntry{key: key}, nil
	}
	hval := HatValue{}
	hval.fromValue(*rawPtr)
	if ht.expireIfNeededLocked(key, hval) {
		return memoryCompactionEntry{key: key}, nil
	}
	return ht.captureMemoryCompactionLiveEntryLocked(Entry{Key: key, Value: hval})
}

func (ht *HatTrie) captureMemoryCompactionLiveEntryLocked(entry Entry) (memoryCompactionEntry, error) {
	if err := ht.validateMemoryCompactionValueLocked(entry.Value); err != nil {
		return memoryCompactionEntry{}, err
	}
	if entry.Value.IsLevelDBReference() {
		reference, ok := ht.dbrefs.Get(entry.Value.Index)
		if !ok || reference.Store == nil {
			return memoryCompactionEntry{}, errors.New("hatriecache: missing leveldb reference during memory compaction")
		}
		reference.Key = entry.Key
		reference.ExpiresAt = cloneTimePtr(snapshotExpiresAt(ht.expirationTimeLocked(entry.Key)))
		reference.Stats = cloneKeyStatsPtr(reference.Stats)
		return memoryCompactionEntry{key: entry.Key, reference: reference, present: true, isRef: true}, nil
	}
	captured, err := ht.captureSnapshotEntryForStoreLocked(entry, nil, nil)
	if err != nil {
		return memoryCompactionEntry{}, err
	}
	captured.Stats = nil
	return memoryCompactionEntry{key: entry.Key, entry: captured, present: true}, nil
}

func applyMemoryCompactionEntries(generation *memoryCompactionGeneration, entries []memoryCompactionEntry) error {
	for _, entry := range entries {
		if err := applyMemoryCompactionEntry(generation, entry); err != nil {
			return err
		}
	}
	return nil
}

func applyMemoryCompactionEntry(generation *memoryCompactionGeneration, captured memoryCompactionEntry) error {
	stage := generation.trie
	if !captured.present {
		stage.deleteLocked(captured.key)
		return nil
	}
	if captured.isRef {
		stage.deleteLocked(captured.key)
		reference := captured.reference
		index := stage.dbrefs.Add(reference)
		rawPtr := stage.upsertLocation(captured.key)
		hval := HatValue{Index: index, Flags: DATAVALUE_TYPE_LEVELDB_REF}
		*rawPtr = hval.toValue()
		if reference.ExpiresAt != nil {
			hval = stage.setExpirationLocked(captured.key, *reference.ExpiresAt, rawPtr, hval)
			*rawPtr = hval.toValue()
		}
		if reference.Token > stage.nextDBReferenceToken {
			stage.nextDBReferenceToken = reference.Token
		}
		return nil
	}
	if captured.entry.Type == "string" {
		captured.entry.String = generation.strings.append(captured.entry.String)
	}
	operation, err := snapshotOperationForEntry(captured.entry)
	if err != nil {
		return err
	}
	_, err = stage.applySnapshotOperationAtLocked(operation, stage.currentTime())
	return err
}

func swapMemoryCompactionGenerationLocked(live *HatTrie, staged *HatTrie, merkle *replicationMerkleIndex) {
	live.root, staged.root = staged.root, live.root
	live.strings, staged.strings = staged.strings, live.strings
	live.raws, staged.raws = staged.raws, live.raws
	live.disks, staged.disks = staged.disks, live.disks
	live.maps, staged.maps = staged.maps, live.maps
	live.slices, staged.slices = staged.slices, live.slices
	live.sets, staged.sets = staged.sets, live.sets
	live.priorityQueues, staged.priorityQueues = staged.priorityQueues, live.priorityQueues
	live.bloomFilters, staged.bloomFilters = staged.bloomFilters, live.bloomFilters
	live.countMinSketches, staged.countMinSketches = staged.countMinSketches, live.countMinSketches
	live.hyperLogLogs, staged.hyperLogLogs = staged.hyperLogLogs, live.hyperLogLogs
	live.topKs, staged.topKs = staged.topKs, live.topKs
	live.cuckooFilters, staged.cuckooFilters = staged.cuckooFilters, live.cuckooFilters
	live.roaringBitmaps, staged.roaringBitmaps = staged.roaringBitmaps, live.roaringBitmaps
	live.quantileSketches, staged.quantileSketches = staged.quantileSketches, live.quantileSketches
	live.fenwickTrees, staged.fenwickTrees = staged.fenwickTrees, live.fenwickTrees
	live.sparseBitsets, staged.sparseBitsets = staged.sparseBitsets, live.sparseBitsets
	live.reservoirSamples, staged.reservoirSamples = staged.reservoirSamples, live.reservoirSamples
	live.xorFilters, staged.xorFilters = staged.xorFilters, live.xorFilters
	live.radixTrees, staged.radixTrees = staged.radixTrees, live.radixTrees
	live.dbrefs, staged.dbrefs = staged.dbrefs, live.dbrefs
	live.expires, staged.expires = staged.expires, live.expires
	live.expirations, staged.expirations = staged.expirations, live.expirations
	live.replicationMerkle, staged.replicationMerkle = merkle, live.replicationMerkle
}

func (ht *HatTrie) validateMemoryCompactionBackingLocked() error {
	if len(ht.maps.deleted) != len(ht.maps.array) {
		return errors.New("hatriecache: map backing arrays have inconsistent lengths")
	}
	checks := []struct {
		name     string
		length   int
		reusable *reusableIndexes
	}{
		{"string", len(ht.strings.array), &ht.strings.reusables},
		{"raw", len(ht.raws.array), &ht.raws.reusables},
		{"disk", len(ht.disks.paths), &ht.disks.reusables},
		{"map", len(ht.maps.array), &ht.maps.reusables},
		{"slice", len(ht.slices.array), &ht.slices.reusables},
		{"set", len(ht.sets.array), &ht.sets.reusables},
		{"priority queue", len(ht.priorityQueues.array), &ht.priorityQueues.reusables},
		{"bloom filter", len(ht.bloomFilters.array), &ht.bloomFilters.reusables},
		{"count-min sketch", len(ht.countMinSketches.array), &ht.countMinSketches.reusables},
		{"hyperloglog", len(ht.hyperLogLogs.array), &ht.hyperLogLogs.reusables},
		{"top-k", len(ht.topKs.array), &ht.topKs.reusables},
		{"cuckoo filter", len(ht.cuckooFilters.array), &ht.cuckooFilters.reusables},
		{"roaring bitmap", len(ht.roaringBitmaps.array), &ht.roaringBitmaps.reusables},
		{"quantile sketch", len(ht.quantileSketches.array), &ht.quantileSketches.reusables},
		{"fenwick tree", len(ht.fenwickTrees.array), &ht.fenwickTrees.reusables},
		{"sparse bitset", len(ht.sparseBitsets.array), &ht.sparseBitsets.reusables},
		{"reservoir sample", len(ht.reservoirSamples.array), &ht.reservoirSamples.reusables},
		{"xor filter", len(ht.xorFilters.array), &ht.xorFilters.reusables},
		{"radix tree", len(ht.radixTrees.array), &ht.radixTrees.reusables},
		{"leveldb reference", len(ht.dbrefs.array), &ht.dbrefs.reusables},
	}
	for _, check := range checks {
		if err := validateMemoryCompactionReusableIndexes(check.length, check.reusable); err != nil {
			return fmt.Errorf("hatriecache: compact %s values: %w", check.name, err)
		}
	}
	return nil
}

func validateMemoryCompactionReusableIndexes(length int, indexes *reusableIndexes) error {
	if indexes == nil || indexes.count < 0 || indexes.count > length {
		return errors.New("reusable-index count does not match storage holes")
	}
	count := 0
	for wordIndex, word := range indexes.bits {
		start := wordIndex * 64
		if start >= length {
			if word != 0 {
				return errors.New("reusable index exceeds storage length")
			}
			continue
		}
		if remaining := length - start; remaining < 64 {
			mask := uint64(1)<<uint(remaining) - 1
			if word & ^mask != 0 {
				return errors.New("reusable index exceeds storage length")
			}
		}
		count += bits.OnesCount64(word)
	}
	if count != indexes.count {
		return errors.New("reusable-index count does not match storage holes")
	}
	return nil
}

func (ht *HatTrie) validateMemoryCompactionValueLocked(value HatValue) error {
	if value.Empty() || value.IsCounter() {
		return nil
	}
	index := int(value.Index)
	length := 0
	var reusable *reusableIndexes
	if value.IsBytesAtRaws() && value.OnDisk() {
		length, reusable = len(ht.disks.paths), &ht.disks.reusables
	} else {
		switch value.Type() {
		case DATAVALUE_TYPE_RAW_STRING:
			length, reusable = len(ht.strings.array), &ht.strings.reusables
		case DATAVALUE_TYPE_RAW_BYTES:
			length, reusable = len(ht.raws.array), &ht.raws.reusables
		case DATAVALUE_TYPE_MAP:
			length, reusable = len(ht.maps.array), &ht.maps.reusables
		case DATAVALUE_TYPE_SLICE:
			length, reusable = len(ht.slices.array), &ht.slices.reusables
		case DATAVALUE_TYPE_SET:
			length, reusable = len(ht.sets.array), &ht.sets.reusables
		case DATAVALUE_TYPE_PRIORITY_QUEUE:
			length, reusable = len(ht.priorityQueues.array), &ht.priorityQueues.reusables
		case DATAVALUE_TYPE_BLOOM_FILTER:
			length, reusable = len(ht.bloomFilters.array), &ht.bloomFilters.reusables
		case DATAVALUE_TYPE_COUNT_MIN_SKETCH:
			length, reusable = len(ht.countMinSketches.array), &ht.countMinSketches.reusables
		case DATAVALUE_TYPE_HYPERLOGLOG:
			length, reusable = len(ht.hyperLogLogs.array), &ht.hyperLogLogs.reusables
		case DATAVALUE_TYPE_TOP_K:
			length, reusable = len(ht.topKs.array), &ht.topKs.reusables
		case DATAVALUE_TYPE_CUCKOO_FILTER:
			length, reusable = len(ht.cuckooFilters.array), &ht.cuckooFilters.reusables
		case DATAVALUE_TYPE_ROARING_BITMAP:
			length, reusable = len(ht.roaringBitmaps.array), &ht.roaringBitmaps.reusables
		case DATAVALUE_TYPE_QUANTILE_SKETCH:
			length, reusable = len(ht.quantileSketches.array), &ht.quantileSketches.reusables
		case DATAVALUE_TYPE_FENWICK_TREE:
			length, reusable = len(ht.fenwickTrees.array), &ht.fenwickTrees.reusables
		case DATAVALUE_TYPE_SPARSE_BITSET:
			length, reusable = len(ht.sparseBitsets.array), &ht.sparseBitsets.reusables
		case DATAVALUE_TYPE_RESERVOIR_SAMPLE:
			length, reusable = len(ht.reservoirSamples.array), &ht.reservoirSamples.reusables
		case DATAVALUE_TYPE_XOR_FILTER:
			length, reusable = len(ht.xorFilters.array), &ht.xorFilters.reusables
		case DATAVALUE_TYPE_RADIX_TREE:
			length, reusable = len(ht.radixTrees.array), &ht.radixTrees.reusables
		case DATAVALUE_TYPE_LEVELDB_REF:
			length, reusable = len(ht.dbrefs.array), &ht.dbrefs.reusables
		default:
			return errors.New("hatriecache: unsupported memory compaction value type")
		}
	}
	if index < 0 || index >= length || reusable.Has(value.Index) {
		return fmt.Errorf("hatriecache: live %s references missing backing index %d", value.String(), index)
	}
	return nil
}
