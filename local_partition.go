package hatriecache

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/syndtr/goleveldb/leveldb"
)

const (
	// DefaultLocalPartitions preserves the single-trie behavior unless enabled.
	DefaultLocalPartitions = 0
	// MaxLocalPartitions bounds open C tries, backing pools, and disk directories.
	MaxLocalPartitions = 256
)

type localPartitionSet struct {
	tries []*HatTrie
	mask  uint64
}

// LocalPartitioningStats describes the optional in-process key partitions.
type LocalPartitioningStats struct {
	Enabled    bool  `json:"enabled"`
	Partitions int   `json:"partitions"`
	Sizes      []int `json:"sizes,omitempty"`
}

// ValidateLocalPartitions accepts zero (disabled) or a power of two from 2 to
// 256. Power-of-two routing avoids division on every key operation.
func ValidateLocalPartitions(count int) error {
	if count < 0 || count == 1 || count > MaxLocalPartitions || (count != 0 && count&(count-1) != 0) {
		return fmt.Errorf("hatriecache: local partitions must be zero or a power of two from 2 through %d", MaxLocalPartitions)
	}
	return nil
}

// ConfigureLocalPartitions enables independent in-process HAT tries. It is a
// one-time operation on an empty trie; zero preserves the default single root.
func (ht *HatTrie) ConfigureLocalPartitions(count int) error {
	if ht == nil {
		return ErrNilHatTrie
	}
	if err := ValidateLocalPartitions(count); err != nil {
		return err
	}
	if current := ht.localPartitions.Load(); current != nil {
		if len(current.tries) == count {
			return nil
		}
		return errors.New("hatriecache: local partitions are already configured")
	}
	if count == 0 {
		return nil
	}

	ht.mu.Lock()
	defer ht.mu.Unlock()
	ht.ensureOpen()
	if current := ht.localPartitions.Load(); current != nil {
		if len(current.tries) == count {
			return nil
		}
		return errors.New("hatriecache: local partitions are already configured")
	}
	if ht.sizeLocked() != 0 {
		return errors.New("hatriecache: local partitions require an empty trie")
	}
	if ht.disks == nil || ht.disks.dir == "" {
		return errors.New("hatriecache: local partitions require disk storage")
	}

	set := &localPartitionSet{tries: make([]*HatTrie, 0, count), mask: uint64(count - 1)}
	partitionDir := filepath.Join(ht.disks.dir, "local-partitions")
	for index := 0; index < count; index++ {
		child, err := CreateHatTrieWithDiskDir(filepath.Join(partitionDir, fmt.Sprintf("%03d", index)), false)
		if err != nil {
			for _, created := range set.tries {
				created.Destroy()
			}
			return err
		}
		child.now = func() time.Time { return ht.currentTime() }
		if len(ht.counterWriteStripes) != 0 {
			if err := child.ConfigureCounterWriteStripes(len(ht.counterWriteStripes)); err != nil {
				child.Destroy()
				for _, created := range set.tries {
					created.Destroy()
				}
				return err
			}
		}
		set.tries = append(set.tries, child)
	}
	if err := configureLocalPartitionKeyStats(set, ht.keyStatsMode, ht.keyStatsCapacity); err != nil {
		for _, child := range set.tries {
			child.Destroy()
		}
		return err
	}
	ht.localPartitions.Store(set)
	return nil
}

func configureLocalPartitionKeyStats(set *localPartitionSet, mode KeyStatsMode, capacity int) error {
	for index, child := range set.tries {
		childMode := mode
		childCapacity := capacity
		if mode == KeyStatsModeBounded {
			childCapacity = capacity / len(set.tries)
			if index < capacity%len(set.tries) {
				childCapacity++
			}
			if childCapacity == 0 {
				childMode = KeyStatsModeOff
			}
		}
		if err := child.ConfigureKeyStats(childMode, childCapacity); err != nil {
			return err
		}
	}
	return nil
}

// LocalPartitionForKey reports the deterministic local partition for key.
func (ht *HatTrie) LocalPartitionForKey(key string) (int, bool, error) {
	if ht == nil {
		return 0, false, ErrNilHatTrie
	}
	if err := validateKey(key); err != nil {
		return 0, false, err
	}
	set := ht.localPartitions.Load()
	if set == nil {
		return 0, false, nil
	}
	return int(xxhash.Sum64String(key) & set.mask), true, nil
}

func (ht *HatTrie) localPartitionForKey(key string) *HatTrie {
	if ht == nil {
		return nil
	}
	set := ht.localPartitions.Load()
	if set == nil {
		return nil
	}
	return set.tries[xxhash.Sum64String(key)&set.mask]
}

func (ht *HatTrie) localPartitionSet() *localPartitionSet {
	if ht == nil {
		return nil
	}
	return ht.localPartitions.Load()
}

// LocalPartitioningStats returns per-partition key counts for operations and
// capacity planning. Sizes are in deterministic partition order.
func (ht *HatTrie) LocalPartitioningStats() LocalPartitioningStats {
	set := ht.localPartitionSet()
	if set == nil {
		return LocalPartitioningStats{}
	}
	stats := LocalPartitioningStats{Enabled: true, Partitions: len(set.tries), Sizes: make([]int, len(set.tries))}
	for index, child := range set.tries {
		stats.Sizes[index] = child.Size()
	}
	return stats
}

func aggregateLocalPartitionStats(set *localPartitionSet) CacheStats {
	var result CacheStats
	for _, child := range set.tries {
		stats := child.Stats()
		result.Reads += stats.Reads
		result.Hits += stats.Hits
		result.Misses += stats.Misses
		result.Writes += stats.Writes
		result.Deletes += stats.Deletes
		result.Expirations += stats.Expirations
		result.LastHit = laterTime(result.LastHit, stats.LastHit)
		result.LastMiss = laterTime(result.LastMiss, stats.LastMiss)
		result.LastWrite = laterTime(result.LastWrite, stats.LastWrite)
	}
	result.updateRates()
	return result
}

func laterTime(left time.Time, right time.Time) time.Time {
	if right.After(left) {
		return right
	}
	return left
}

func localPartitionKeys(set *localPartitionSet, prefix string, sortedKeys bool) ([]string, error) {
	result := make([]string, 0)
	for _, child := range set.tries {
		keys, err := child.KeysWithPrefixChecked(prefix, false)
		if err != nil {
			return nil, err
		}
		result = append(result, keys...)
	}
	if sortedKeys {
		sort.Strings(result)
	}
	return result, nil
}

func localPartitionEntries(set *localPartitionSet, prefix string, sortedEntries bool) ([]Entry, error) {
	result := make([]Entry, 0)
	for _, child := range set.tries {
		entries, err := child.EntriesWithPrefixChecked(prefix, false)
		if err != nil {
			return nil, err
		}
		result = append(result, entries...)
	}
	if sortedEntries {
		sort.Slice(result, func(left int, right int) bool { return result[left].Key < result[right].Key })
	}
	return result, nil
}

func localPartitionMonitoringEntriesPage(set *localPartitionSet, prefix string, afterKey string, hasAfterKey bool, limit int) MonitoringEntriesResponse {
	all := make([]MonitoringEntry, 0)
	for _, child := range set.tries {
		all = append(all, child.monitoringEntries(prefix)...)
	}
	sort.Slice(all, func(left int, right int) bool { return all[left].Key < all[right].Key })
	response := MonitoringEntriesResponse{}
	if limit > 0 {
		response.Limit = uint64(limit)
	}
	if hasAfterKey {
		response.AfterKey = afterKey
		response.afterKeySet = true
	}
	start := 0
	if hasAfterKey {
		start = sort.Search(len(all), func(index int) bool { return all[index].Key > afterKey })
	}
	end := len(all)
	if limit > 0 && end-start > limit {
		end = start + limit
		response.HasMore = true
	}
	response.Entries = append([]MonitoringEntry(nil), all[start:end]...)
	if response.Entries == nil {
		response.Entries = []MonitoringEntry{}
	}
	if response.HasMore && len(response.Entries) > 0 {
		response.NextAfterKey = response.Entries[len(response.Entries)-1].Key
		response.nextAfterKeySet = true
	}
	return response
}

func (ht *HatTrie) executePartitionedPublicBatchCommand(request CacheCommandRequest) CacheCommandResponse {
	payloads, err := publicCommandBatchRequests(request)
	if err != nil {
		return commandError(err.Error())
	}
	set := ht.localPartitionSet()
	type partitionBatch struct {
		indexes   []int
		requests  []CacheCommandRequest
		responses []CacheCommandResponse
	}
	groups := make([]partitionBatch, len(set.tries))
	for index, payload := range payloads {
		partition := int(xxhash.Sum64String(strings.TrimSpace(payload.Key)) & set.mask)
		groups[partition].indexes = append(groups[partition].indexes, index)
		groups[partition].requests = append(groups[partition].requests, payload)
	}
	var workers sync.WaitGroup
	for partition := range groups {
		group := &groups[partition]
		if len(group.requests) == 0 {
			continue
		}
		workers.Add(1)
		go func(child *HatTrie, group *partitionBatch) {
			defer workers.Done()
			response := child.ExecuteCommand(CacheCommandRequest{Command: "BATCH", Batch: group.requests})
			if len(response.Responses) == len(group.requests) {
				group.responses = response.Responses
				return
			}
			group.responses = make([]CacheCommandResponse, len(group.requests))
			for index := range group.responses {
				group.responses[index] = commandError(response.Message)
			}
		}(set.tries[partition], group)
	}
	workers.Wait()

	responses := make([]CacheCommandResponse, len(payloads))
	allOK := true
	for _, group := range groups {
		for offset, response := range group.responses {
			responses[group.indexes[offset]] = response
			if !response.OK {
				allOK = false
			}
		}
	}
	return publicCommandBatchResponse(responses, allOK)
}

func executePartitionedPublicCommandBatch(ctx context.Context, trie *HatTrie, request CacheCommandRequest, payloads []CacheCommandRequest, options commandExecutionOptions) (CacheCommandResponse, bool) {
	set := trie.localPartitionSet()
	if options.Journal == nil && options.DirtyTracker == nil && options.Replicator == nil && !options.EnforceLeaderWrites {
		return trie.executePartitionedPublicBatchCommand(request), false
	}
	partition := -1
	for _, payload := range payloads {
		candidate := int(xxhash.Sum64String(strings.TrimSpace(payload.Key)) & set.mask)
		if partition == -1 {
			partition = candidate
		} else if partition != candidate {
			partition = -2
			break
		}
	}
	if partition >= 0 {
		return executePublicCommandBatch(ctx, set.tries[partition], request, options)
	}

	responses := make([]CacheCommandResponse, 0, len(payloads))
	allOK := true
	for index, payload := range payloads {
		if err := ctx.Err(); err != nil {
			responses = append(responses, commandError(err.Error()))
			allOK = false
			break
		}
		if err := validatePublicCommandBatchPayload(payload, index); err != nil {
			responses = append(responses, commandError(err.Error()))
			allOK = false
			continue
		}
		response, stop := executeCacheCommand(ctx, trie, payload, options)
		responses = append(responses, response)
		if !response.OK {
			allOK = false
		}
		if stop {
			break
		}
	}
	return publicCommandBatchResponse(responses, allOK), false
}

func (ht *HatTrie) captureLocalPartitionEntries(currentStore *LevelDBStore, currentDB *leveldb.DB, barrier snapshotCaptureBarrier) ([]snapshotEntry, uint64, error) {
	entries := make([]snapshotEntry, 0)
	sequence, err := ht.visitCapturedLocalPartitionEntries(currentStore, currentDB, barrier, func(entry snapshotEntry) error {
		entries = append(entries, entry)
		return nil
	})
	return entries, sequence, err
}

type localPartitionScanItem struct {
	partition int
	entry     Entry
}

type localPartitionScanHeap []localPartitionScanItem

func (items localPartitionScanHeap) Len() int { return len(items) }
func (items localPartitionScanHeap) Less(left int, right int) bool {
	return items[left].entry.Key < items[right].entry.Key
}
func (items localPartitionScanHeap) Swap(left int, right int) {
	items[left], items[right] = items[right], items[left]
}
func (items *localPartitionScanHeap) Push(value interface{}) {
	*items = append(*items, value.(localPartitionScanItem))
}
func (items *localPartitionScanHeap) Pop() interface{} {
	old := *items
	last := len(old) - 1
	value := old[last]
	*items = old[:last]
	return value
}

func (ht *HatTrie) visitCapturedLocalPartitionEntries(currentStore *LevelDBStore, currentDB *leveldb.DB, barrier snapshotCaptureBarrier, visit func(snapshotEntry) error) (uint64, error) {
	set := ht.localPartitionSet()
	if set == nil {
		return 0, errors.New("hatriecache: local partitions are not configured")
	}
	ht.snapshotCaptureMu.Lock()
	defer ht.snapshotCaptureMu.Unlock()

	var sequence uint64
	var release func()
	if barrier != nil {
		var err error
		sequence, release, err = barrier()
		if err != nil {
			return 0, err
		}
	}
	if release != nil {
		defer release()
	}
	for _, child := range set.tries {
		child.mu.Lock()
	}
	defer func() {
		for index := len(set.tries) - 1; index >= 0; index-- {
			set.tries[index].mu.Unlock()
		}
	}()

	now := ht.currentTime()
	scans := make([]*hatTrieScanCursor, len(set.tries))
	defer func() {
		for index, scan := range scans {
			if scan != nil {
				scan.closeLocked(set.tries[index])
			}
		}
	}()
	items := make(localPartitionScanHeap, 0, len(set.tries))
	for index, child := range set.tries {
		child.ensureOpen()
		scan, err := child.newScanCursorLocked("", true)
		if err != nil {
			return 0, err
		}
		scans[index] = scan
		if entry, ok := scan.currentLiveEntryLocked(child, now); ok {
			heap.Push(&items, localPartitionScanItem{partition: index, entry: entry})
		}
	}
	for len(items) > 0 {
		item := heap.Pop(&items).(localPartitionScanItem)
		child := set.tries[item.partition]
		captured, err := child.captureSnapshotEntryForStoreLocked(item.entry, currentStore, currentDB)
		if err != nil {
			return 0, err
		}
		if err := visit(captured); err != nil {
			return 0, err
		}
		scan := scans[item.partition]
		scan.consume()
		if entry, ok := scan.currentLiveEntryLocked(child, now); ok {
			heap.Push(&items, localPartitionScanItem{partition: item.partition, entry: entry})
		}
	}
	return sequence, nil
}

func (ht *HatTrie) loadPartitionedSnapshot(path string) (SnapshotMetadata, error) {
	file, err := os.Open(path)
	if err != nil {
		return SnapshotMetadata{}, err
	}
	defer file.Close()

	now := ht.currentTime()
	var activeKeyList []string
	metadata, err := scanSnapshotFileReader(file, func(entry snapshotEntry) error {
		operation, active, err := validateSnapshotLoadEntry(entry, now, false)
		if err != nil {
			return err
		}
		if active {
			activeKeyList = append(activeKeyList, operation.entry.Key)
		}
		return nil
	})
	if err != nil {
		return SnapshotMetadata{}, err
	}
	if metadata.Version != snapshotVersion {
		return SnapshotMetadata{}, errors.New("hatriecache: unsupported snapshot version")
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return SnapshotMetadata{}, err
	}
	activeKeys, err := newSnapshotActiveKeys(activeKeyList)
	if err != nil {
		return SnapshotMetadata{}, err
	}
	return ht.applyPartitionedSnapshotFile(file, metadata, activeKeys, now)
}

func (ht *HatTrie) applyPartitionedSnapshotFile(file *os.File, metadata snapshotFileMetadata, activeKeys snapshotActiveKeys, now time.Time) (SnapshotMetadata, error) {
	set := ht.localPartitionSet()
	for _, child := range set.tries {
		child.mu.Lock()
		child.invalidateReplicationMerkleLocked()
	}
	defer func() {
		for index := len(set.tries) - 1; index >= 0; index-- {
			set.tries[index].mu.Unlock()
		}
	}()

	created := make([]map[string]struct{}, len(set.tries))
	rollbacks := make([][]snapshotOperation, len(set.tries))
	for index := range created {
		created[index] = make(map[string]struct{})
	}
	applied := 0
	applyMetadata, applyErr := scanSnapshotFileReader(file, func(entry snapshotEntry) error {
		operation, active, err := validateSnapshotLoadEntry(entry, now, true)
		if err != nil || !active {
			return err
		}
		if err := activeKeys.markSeen(operation.entry.Key); err != nil {
			return err
		}
		partition := int(xxhash.Sum64String(operation.entry.Key) & set.mask)
		child := set.tries[partition]
		rollback, existed, err := child.restoreRollbackOperationLocked(operation.entry.Key)
		if err != nil {
			return err
		}
		if _, err := child.applySnapshotOperationAtLocked(operation, now); err != nil {
			if existed {
				rollbacks[partition] = append(rollbacks[partition], rollback)
			}
			return err
		}
		if existed {
			rollbacks[partition] = append(rollbacks[partition], rollback)
		} else {
			created[partition][operation.entry.Key] = struct{}{}
		}
		applied++
		return nil
	})
	if applyErr != nil || applyMetadata.Version != snapshotVersion || applyMetadata.JournalSequence != metadata.JournalSequence || applied != len(activeKeys.keys) {
		if applyErr == nil {
			applyErr = errSnapshotChangedDuringLoad
		}
		for index, child := range set.tries {
			_ = child.rollbackRestoreLocked(created[index], rollbacks[index], now)
		}
		return SnapshotMetadata{}, applyErr
	}
	for _, child := range set.tries {
		child.deleteKeysNotInSortedLocked(activeKeys.keys, now)
	}
	return SnapshotMetadata{JournalSequence: metadata.JournalSequence}, nil
}

func loadLocalPartitionPersistentEntryData(trie *HatTrie, store persistentReferenceStore, policy LevelDBLoadPolicy, scan func(func(snapshotEntry, []byte) error) error) (LevelDBLoadResult, error) {
	set := trie.localPartitionSet()
	now := trie.currentTime()
	for _, child := range set.tries {
		child.mu.Lock()
		child.invalidateReplicationMerkleLocked()
	}
	defer func() {
		for index := len(set.tries) - 1; index >= 0; index-- {
			set.tries[index].mu.Unlock()
		}
	}()

	created := make([]map[string]struct{}, len(set.tries))
	rollbacks := make([][]snapshotOperation, len(set.tries))
	for index := range created {
		created[index] = make(map[string]struct{})
	}
	activeKeys := make([]string, 0)
	result := LevelDBLoadResult{}
	applied := false
	loadErr := scan(func(entry snapshotEntry, data []byte) error {
		loadEntry, active, err := prepareLevelDBLoadEntry(entry, now, policy, true)
		if err != nil || !active {
			return err
		}
		activeKeys = append(activeKeys, loadEntry.entry.Key)
		result.KeysLoaded++
		partition := int(xxhash.Sum64String(loadEntry.entry.Key) & set.mask)
		child := set.tries[partition]
		rollback, existed, err := child.restoreRollbackOperationLocked(loadEntry.entry.Key)
		if err != nil {
			return err
		}
		if loadEntry.reference {
			_, err = child.applyLevelDBReferenceLocked(store, loadEntry.entry, data)
		} else {
			_, err = child.applySnapshotOperationAtLocked(loadEntry.operation, now)
			if err == nil {
				result.ValuesLoaded++
			}
		}
		if err != nil {
			if existed {
				rollbacks[partition] = append(rollbacks[partition], rollback)
			}
			return err
		}
		applied = true
		if existed {
			rollbacks[partition] = append(rollbacks[partition], rollback)
		} else {
			created[partition][loadEntry.entry.Key] = struct{}{}
		}
		return nil
	})
	if loadErr != nil {
		if applied {
			for index, child := range set.tries {
				_ = child.rollbackRestoreLocked(created[index], rollbacks[index], now)
			}
		}
		return LevelDBLoadResult{}, loadErr
	}
	sort.Strings(activeKeys)
	for _, child := range set.tries {
		child.deleteKeysNotInSortedLocked(activeKeys, now)
	}
	return result, nil
}
