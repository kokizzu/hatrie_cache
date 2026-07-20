package hatriecache

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
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

func runLocalPartitionTasks[T any](set *localPartitionSet, task func(*HatTrie) (T, error)) ([]T, error) {
	results := make([]T, len(set.tries))
	errs := make([]error, len(set.tries))
	workers := runtime.GOMAXPROCS(0)
	if workers > len(set.tries) {
		workers = len(set.tries)
	}
	if workers <= 1 {
		for index, child := range set.tries {
			results[index], errs[index] = task(child)
		}
	} else {
		var next atomic.Uint32
		var group sync.WaitGroup
		work := func() {
			defer group.Done()
			for {
				index := int(next.Add(1)) - 1
				if index >= len(set.tries) {
					return
				}
				results[index], errs[index] = task(set.tries[index])
			}
		}
		group.Add(workers)
		for worker := 1; worker < workers; worker++ {
			go work()
		}
		work()
		group.Wait()
	}
	for _, err := range errs {
		if err != nil {
			return results, err
		}
	}
	return results, nil
}

func flattenLocalPartitionSlices[T any](partitions [][]T) []T {
	total := 0
	for _, values := range partitions {
		total += len(values)
	}
	result := make([]T, 0, total)
	for _, values := range partitions {
		result = append(result, values...)
	}
	return result
}

type localPartitionMergeCursor struct {
	partition int
	offset    int
	key       string
}

func mergeSortedLocalPartitionSlices[T any](partitions [][]T, key func(T) string) []T {
	total := 0
	cursors := make([]localPartitionMergeCursor, 0, len(partitions))
	for partition, values := range partitions {
		total += len(values)
		if len(values) != 0 {
			cursors = append(cursors, localPartitionMergeCursor{partition: partition, key: key(values[0])})
		}
	}
	less := func(left int, right int) bool { return cursors[left].key < cursors[right].key }
	swap := func(left int, right int) { cursors[left], cursors[right] = cursors[right], cursors[left] }
	down := func(root int) {
		for {
			left := root*2 + 1
			if left >= len(cursors) {
				return
			}
			smallest := left
			right := left + 1
			if right < len(cursors) && less(right, left) {
				smallest = right
			}
			if !less(smallest, root) {
				return
			}
			swap(root, smallest)
			root = smallest
		}
	}
	for index := len(cursors)/2 - 1; index >= 0; index-- {
		down(index)
	}

	result := make([]T, 0, total)
	for len(cursors) != 0 {
		cursor := cursors[0]
		values := partitions[cursor.partition]
		result = append(result, values[cursor.offset])
		cursor.offset++
		if cursor.offset < len(values) {
			cursor.key = key(values[cursor.offset])
			cursors[0] = cursor
		} else {
			last := len(cursors) - 1
			cursors[0] = cursors[last]
			cursors = cursors[:last]
		}
		if len(cursors) != 0 {
			down(0)
		}
	}
	return result
}

func localPartitionKeys(set *localPartitionSet, prefix string, sortedKeys bool) ([]string, error) {
	partitions, err := runLocalPartitionTasks(set, func(child *HatTrie) ([]string, error) {
		return child.KeysWithPrefixChecked(prefix, sortedKeys)
	})
	if err != nil {
		return nil, err
	}
	if sortedKeys {
		return mergeSortedLocalPartitionSlices(partitions, func(value string) string { return value }), nil
	}
	return flattenLocalPartitionSlices(partitions), nil
}

func localPartitionEntries(set *localPartitionSet, prefix string, sortedEntries bool) ([]Entry, error) {
	partitions, err := runLocalPartitionTasks(set, func(child *HatTrie) ([]Entry, error) {
		return child.EntriesWithPrefixChecked(prefix, sortedEntries)
	})
	if err != nil {
		return nil, err
	}
	if sortedEntries {
		return mergeSortedLocalPartitionSlices(partitions, func(entry Entry) string { return entry.Key }), nil
	}
	return flattenLocalPartitionSlices(partitions), nil
}

func localPartitionMonitoringEntriesPage(set *localPartitionSet, prefix string, afterKey string, hasAfterKey bool, limit int) MonitoringEntriesResponse {
	partitions, err := runLocalPartitionTasks(set, func(child *HatTrie) ([]MonitoringEntry, error) {
		return child.monitoringEntries(prefix), nil
	})
	if err != nil {
		return MonitoringEntriesResponse{Entries: []MonitoringEntry{}}
	}
	all := mergeSortedLocalPartitionSlices(partitions, func(entry MonitoringEntry) string { return entry.Key })
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
	capture := snapshotCapture{}
	replacements, sequence, err := ht.visitCapturedLocalPartitionEntries(currentStore, currentDB, barrier, func(entry snapshotEntry) error {
		capture.append(entry)
		return nil
	})
	if err != nil {
		return nil, 0, err
	}
	capture = reconcileSnapshotCapture(capture, replacements)
	entries := make([]snapshotEntry, 0, capture.count)
	for _, page := range capture.pages {
		entries = append(entries, page...)
	}
	return entries, sequence, nil
}

type localPartitionCapturedItem struct {
	partition int
	entry     snapshotEntry
}

type localPartitionCaptureHeap []localPartitionCapturedItem

func (items localPartitionCaptureHeap) Len() int { return len(items) }
func (items localPartitionCaptureHeap) Less(left int, right int) bool {
	return items[left].entry.Key < items[right].entry.Key
}
func (items localPartitionCaptureHeap) Swap(left int, right int) {
	items[left], items[right] = items[right], items[left]
}
func (items *localPartitionCaptureHeap) Push(value interface{}) {
	*items = append(*items, value.(localPartitionCapturedItem))
}
func (items *localPartitionCaptureHeap) Pop() interface{} {
	old := *items
	last := len(old) - 1
	value := old[last]
	*items = old[:last]
	return value
}

type localPartitionCaptureResult struct {
	entry snapshotEntry
	err   error
}

func (ht *HatTrie) visitCapturedLocalPartitionEntries(currentStore *LevelDBStore, currentDB *leveldb.DB, barrier snapshotCaptureBarrier, visit func(snapshotEntry) error) (map[string]snapshotCaptureReplacement, uint64, error) {
	set := ht.localPartitionSet()
	if set == nil {
		return nil, 0, errors.New("hatriecache: local partitions are not configured")
	}
	ht.snapshotCaptureMu.Lock()
	defer ht.snapshotCaptureMu.Unlock()

	tracker := &snapshotMutationTracker{dirty: make(map[string]struct{})}
	lockLocalPartitionSet(set)
	for _, child := range set.tries {
		child.ensureOpen()
		if child.snapshotMutations != nil {
			unlockLocalPartitionSet(set)
			return nil, 0, errors.New("hatriecache: local partition snapshot capture already active")
		}
	}
	for _, child := range set.tries {
		child.snapshotMutations = tracker
	}
	unlockLocalPartitionSet(set)
	active := true
	defer func() {
		if !active {
			return
		}
		clearLocalPartitionSnapshotTracker(set, tracker)
	}()

	done := make(chan struct{})
	concurrency := runtime.GOMAXPROCS(0)
	if concurrency > len(set.tries) {
		concurrency = len(set.tries)
	}
	if concurrency < 1 {
		concurrency = 1
	}
	tokens := make(chan struct{}, concurrency)
	results := make([]chan localPartitionCaptureResult, len(set.tries))
	var workers sync.WaitGroup
	for index, child := range set.tries {
		results[index] = make(chan localPartitionCaptureResult, 1)
		workers.Add(1)
		go func(child *HatTrie, output chan<- localPartitionCaptureResult) {
			defer workers.Done()
			defer close(output)
			cursor := &replicationSyncCursor{}
			defer cursor.close(child)
			afterKey := ""
			hasAfterKey := false
			entries := make([]snapshotEntry, 0, snapshotCaptureScanPageEntries)
			for {
				select {
				case tokens <- struct{}{}:
				case <-done:
					return
				}
				entries = entries[:0]
				page, err := replicationSyncEntriesPageWithCursor(child, "", afterKey, hasAfterKey, snapshotCaptureScanPageEntries, cursor, func(entry Entry) error {
					captured, captureErr := child.captureSnapshotEntryForStoreLocked(entry, currentStore, currentDB)
					if captureErr != nil {
						return captureErr
					}
					entries = append(entries, captured)
					return nil
				})
				<-tokens
				if err != nil {
					select {
					case output <- localPartitionCaptureResult{err: err}:
					case <-done:
					}
					return
				}
				for _, captured := range entries {
					select {
					case output <- localPartitionCaptureResult{entry: captured}:
					case <-done:
						return
					}
				}
				if !page.hasMore {
					return
				}
				afterKey = page.nextAfterKey
				hasAfterKey = true
				runtime.Gosched()
			}
		}(child, results[index])
	}
	defer func() {
		close(done)
		workers.Wait()
	}()

	items := make(localPartitionCaptureHeap, 0, len(set.tries))
	for index, output := range results {
		result, ok := <-output
		if !ok {
			continue
		}
		if result.err != nil {
			return nil, 0, result.err
		}
		heap.Push(&items, localPartitionCapturedItem{partition: index, entry: result.entry})
	}
	pageEntries := 0
	pageNumber := 0
	for len(items) > 0 {
		item := heap.Pop(&items).(localPartitionCapturedItem)
		if err := visit(item.entry); err != nil {
			return nil, 0, err
		}
		pageEntries++
		if pageEntries == snapshotCaptureScanPageEntries {
			pageEntries = 0
			pageNumber++
			if hook := ht.snapshotCapturePageHook; hook != nil {
				hook(pageNumber)
			}
			runtime.Gosched()
		}
		result, ok := <-results[item.partition]
		if !ok {
			continue
		}
		if result.err != nil {
			return nil, 0, result.err
		}
		heap.Push(&items, localPartitionCapturedItem{partition: item.partition, entry: result.entry})
	}
	if pageEntries != 0 || pageNumber == 0 {
		pageNumber++
		if hook := ht.snapshotCapturePageHook; hook != nil {
			hook(pageNumber)
		}
	}
	replacements, sequence, err := ht.captureLocalPartitionMutationReplacements(set, tracker, currentStore, currentDB, barrier)
	if err != nil {
		return nil, 0, err
	}
	active = false
	return replacements, sequence, nil
}

func clearLocalPartitionSnapshotTracker(set *localPartitionSet, tracker *snapshotMutationTracker) {
	if set == nil || tracker == nil {
		return
	}
	lockLocalPartitionSet(set)
	for _, child := range set.tries {
		if child.snapshotMutations == tracker {
			child.snapshotMutations = nil
		}
	}
	unlockLocalPartitionSet(set)
}

func (ht *HatTrie) captureLocalPartitionMutationReplacements(set *localPartitionSet, tracker *snapshotMutationTracker, currentStore *LevelDBStore, currentDB *leveldb.DB, barrier snapshotCaptureBarrier) (map[string]snapshotCaptureReplacement, uint64, error) {
	replacements := make(map[string]snapshotCaptureReplacement)
	for {
		var sequence uint64
		var releaseBarrier func()
		if barrier != nil {
			var err error
			sequence, releaseBarrier, err = barrier()
			if err != nil {
				return nil, 0, err
			}
		}
		lockLocalPartitionSet(set)
		valid := true
		for _, child := range set.tries {
			if child.snapshotMutations != tracker {
				valid = false
				break
			}
		}
		dirty := tracker.take()
		if !valid {
			unlockLocalPartitionSet(set)
			if releaseBarrier != nil {
				releaseBarrier()
			}
			return nil, 0, errors.New("hatriecache: local partition snapshot mutation tracker changed during capture")
		}
		if len(dirty) == 0 {
			for _, child := range set.tries {
				child.snapshotMutations = nil
			}
			unlockLocalPartitionSet(set)
			if releaseBarrier != nil {
				releaseBarrier()
			}
			return replacements, sequence, nil
		}
		unlockLocalPartitionSet(set)
		if releaseBarrier != nil {
			releaseBarrier()
		}
		keys := make([]string, 0, len(dirty))
		for key := range dirty {
			keys = append(keys, key)
		}

		for first := 0; first < len(keys); first += snapshotCaptureScanPageEntries {
			last := first + snapshotCaptureScanPageEntries
			if last > len(keys) {
				last = len(keys)
			}
			groups := make([][]string, len(set.tries))
			for _, key := range keys[first:last] {
				partition := int(xxhash.Sum64String(key) & set.mask)
				groups[partition] = append(groups[partition], key)
			}
			for partition, partitionKeys := range groups {
				if len(partitionKeys) == 0 {
					continue
				}
				child := set.tries[partition]
				child.mu.Lock()
				for _, key := range partitionKeys {
					replacement, err := child.captureSnapshotReplacementLocked(key, currentStore, currentDB)
					if err != nil {
						child.mu.Unlock()
						return nil, 0, err
					}
					replacements[key] = replacement
				}
				child.mu.Unlock()
			}
			runtime.Gosched()
		}
	}
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

	states := newLocalPartitionRestoreStates(len(set.tries))
	pool := newLocalPartitionRestorePool(set, func(partition int, operation snapshotOperation) error {
		return applyLocalPartitionSnapshotRestore(set.tries[partition], &states[partition], operation, now)
	})
	applyMetadata, applyErr := scanSnapshotFileReader(file, func(entry snapshotEntry) error {
		operation, active, err := validateSnapshotLoadEntry(entry, now, true)
		if err != nil || !active {
			return err
		}
		if err := activeKeys.markSeen(operation.entry.Key); err != nil {
			return err
		}
		partition := int(xxhash.Sum64String(operation.entry.Key) & set.mask)
		return pool.dispatch(partition, detachLocalPartitionRestoreOperation(operation))
	})
	applyErr = pool.finish(applyErr)
	applied := localPartitionRestoreApplied(states)
	if applyErr != nil || applyMetadata.Version != snapshotVersion || applyMetadata.JournalSequence != metadata.JournalSequence || applied != len(activeKeys.keys) {
		if applyErr == nil {
			applyErr = errSnapshotChangedDuringLoad
		}
		if rollbackErr := rollbackLocalPartitionRestore(set, states, now); rollbackErr != nil {
			applyErr = errors.Join(applyErr, fmt.Errorf("hatriecache: partition restore rollback failed: %w", rollbackErr))
		}
		return SnapshotMetadata{}, applyErr
	}
	_ = visitLocalPartitionsInParallel(set, func(_ int, child *HatTrie) error {
		child.deleteKeysNotInSortedLocked(activeKeys.keys, now)
		return nil
	})
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

	states := newLocalPartitionRestoreStates(len(set.tries))
	pool := newLocalPartitionRestorePool(set, func(partition int, work localPartitionPersistentRestoreWork) error {
		child := set.tries[partition]
		state := &states[partition]
		rollback, existed, err := child.restoreRollbackOperationLocked(work.load.entry.Key)
		if err != nil {
			return err
		}
		if work.load.reference {
			_, err = child.applyLevelDBReferenceMetadataLocked(store, work.load.entry, work.recordBytes, work.recordChecksum)
		} else {
			_, err = child.applySnapshotOperationAtLocked(work.load.operation, now)
			if err == nil {
				state.valuesLoaded++
			}
		}
		if err != nil {
			if existed {
				state.rollbacks = append(state.rollbacks, rollback)
			}
			return err
		}
		state.recordApplied(work.load.entry.Key, rollback, existed)
		return nil
	})
	activeKeys := make([]string, 0)
	result := LevelDBLoadResult{}
	loadErr := scan(func(entry snapshotEntry, data []byte) error {
		loadEntry, active, err := prepareLevelDBLoadEntry(entry, now, policy, true)
		if err != nil || !active {
			return err
		}
		activeKeys = append(activeKeys, loadEntry.entry.Key)
		result.KeysLoaded++
		partition := int(xxhash.Sum64String(loadEntry.entry.Key) & set.mask)
		work := localPartitionPersistentRestoreWork{load: loadEntry}
		if loadEntry.reference {
			work.recordBytes = len(data)
			work.recordChecksum = levelDBRecordChecksum(data)
		} else {
			work.load.operation = detachLocalPartitionRestoreOperation(work.load.operation)
		}
		work.load.entry.rawBytes = nil
		return pool.dispatch(partition, work)
	})
	loadErr = pool.finish(loadErr)
	if loadErr != nil {
		if rollbackErr := rollbackLocalPartitionRestore(set, states, now); rollbackErr != nil {
			loadErr = errors.Join(loadErr, fmt.Errorf("hatriecache: partition restore rollback failed: %w", rollbackErr))
		}
		return LevelDBLoadResult{}, loadErr
	}
	for index := range states {
		result.ValuesLoaded += states[index].valuesLoaded
	}
	sort.Strings(activeKeys)
	_ = visitLocalPartitionsInParallel(set, func(_ int, child *HatTrie) error {
		child.deleteKeysNotInSortedLocked(activeKeys, now)
		return nil
	})
	return result, nil
}

const localPartitionRestoreQueueDepth = 8

type localPartitionRestoreState struct {
	created      map[string]struct{}
	rollbacks    []snapshotOperation
	applied      int
	valuesLoaded int
}

func newLocalPartitionRestoreStates(count int) []localPartitionRestoreState {
	states := make([]localPartitionRestoreState, count)
	for index := range states {
		states[index].created = make(map[string]struct{})
	}
	return states
}

func (state *localPartitionRestoreState) recordApplied(key string, rollback snapshotOperation, existed bool) {
	if existed {
		state.rollbacks = append(state.rollbacks, rollback)
	} else {
		state.created[key] = struct{}{}
	}
	state.applied++
}

func applyLocalPartitionSnapshotRestore(child *HatTrie, state *localPartitionRestoreState, operation snapshotOperation, now time.Time) error {
	rollback, existed, err := child.restoreRollbackOperationLocked(operation.entry.Key)
	if err != nil {
		return err
	}
	if _, err := child.applySnapshotOperationAtLocked(operation, now); err != nil {
		if existed {
			state.rollbacks = append(state.rollbacks, rollback)
		}
		return err
	}
	state.recordApplied(operation.entry.Key, rollback, existed)
	return nil
}

func detachLocalPartitionRestoreOperation(operation snapshotOperation) snapshotOperation {
	if operation.entry.rawBytes == nil {
		return operation
	}
	operation.bytes = cloneBytes(operation.bytes)
	operation.entry.rawBytes = nil
	return operation
}

func localPartitionRestoreApplied(states []localPartitionRestoreState) int {
	total := 0
	for index := range states {
		total += states[index].applied
	}
	return total
}

func rollbackLocalPartitionRestore(set *localPartitionSet, states []localPartitionRestoreState, now time.Time) error {
	return visitLocalPartitionsInParallel(set, func(index int, child *HatTrie) error {
		return child.rollbackRestoreLocked(states[index].created, states[index].rollbacks, now)
	})
}

type localPartitionPersistentRestoreWork struct {
	load           levelDBLoadEntry
	recordBytes    int
	recordChecksum uint64
}

type localPartitionRestoreWork[T any] struct {
	partition int
	value     T
}

type localPartitionRestorePool[T any] struct {
	queues   []chan localPartitionRestoreWork[T]
	done     chan struct{}
	workers  sync.WaitGroup
	failOnce sync.Once
	firstErr error
	apply    func(int, T) error
}

func newLocalPartitionRestorePool[T any](set *localPartitionSet, apply func(int, T) error) *localPartitionRestorePool[T] {
	workerCount := localPartitionRestoreWorkerCount(len(set.tries))
	pool := &localPartitionRestorePool[T]{
		done:  make(chan struct{}),
		apply: apply,
	}
	if workerCount == 1 {
		return pool
	}
	pool.queues = make([]chan localPartitionRestoreWork[T], workerCount)
	for worker := 0; worker < workerCount; worker++ {
		queue := make(chan localPartitionRestoreWork[T], localPartitionRestoreQueueDepth)
		pool.queues[worker] = queue
		pool.workers.Add(1)
		go func() {
			defer pool.workers.Done()
			for {
				select {
				case <-pool.done:
					return
				case work, ok := <-queue:
					if !ok {
						return
					}
					if err := apply(work.partition, work.value); err != nil {
						pool.fail(err)
						return
					}
				}
			}
		}()
	}
	return pool
}

func (pool *localPartitionRestorePool[T]) dispatch(partition int, value T) error {
	if len(pool.queues) == 0 {
		if err := pool.apply(partition, value); err != nil {
			pool.fail(err)
			return err
		}
		return nil
	}
	select {
	case <-pool.done:
		return pool.firstErr
	default:
	}
	work := localPartitionRestoreWork[T]{partition: partition, value: value}
	select {
	case <-pool.done:
		return pool.firstErr
	case pool.queues[partition%len(pool.queues)] <- work:
		return nil
	}
}

func (pool *localPartitionRestorePool[T]) finish(scanErr error) error {
	if scanErr != nil {
		pool.fail(scanErr)
	}
	if len(pool.queues) == 0 {
		return pool.firstErr
	}
	for _, queue := range pool.queues {
		close(queue)
	}
	pool.workers.Wait()
	return pool.firstErr
}

func (pool *localPartitionRestorePool[T]) fail(err error) {
	if err == nil {
		return
	}
	pool.failOnce.Do(func() {
		pool.firstErr = err
		close(pool.done)
	})
}

func localPartitionRestoreWorkerCount(partitions int) int {
	workers := runtime.GOMAXPROCS(0)
	if workers > partitions {
		workers = partitions
	}
	if workers < 1 {
		workers = 1
	}
	return workers
}

func visitLocalPartitionsInParallel(set *localPartitionSet, visit func(int, *HatTrie) error) error {
	workerCount := localPartitionRestoreWorkerCount(len(set.tries))
	errs := make([]error, len(set.tries))
	if workerCount == 1 {
		for index := range set.tries {
			errs[index] = visit(index, set.tries[index])
		}
		return errors.Join(errs...)
	}
	var workers sync.WaitGroup
	workers.Add(workerCount)
	for worker := 0; worker < workerCount; worker++ {
		go func(first int) {
			defer workers.Done()
			for index := first; index < len(set.tries); index += workerCount {
				errs[index] = visit(index, set.tries[index])
			}
		}(worker)
	}
	workers.Wait()
	return errors.Join(errs...)
}
