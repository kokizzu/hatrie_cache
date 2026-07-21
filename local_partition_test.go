package hatriecache

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	hatriecachev1 "hatrie_cache/internal/gen/hatriecache/v1"
)

func TestLocalPartitionsDefaultOffAndValidateConfiguration(t *testing.T) {
	trie := newTestTrie(t)
	if stats := trie.LocalPartitioningStats(); stats.Enabled || stats.Partitions != 0 || len(stats.Sizes) != 0 {
		t.Fatalf("default local partition stats = %#v, want disabled", stats)
	}
	for _, count := range []int{-1, 1, 3, MaxLocalPartitions + 1} {
		if err := trie.ConfigureLocalPartitions(count); err == nil {
			t.Fatalf("ConfigureLocalPartitions(%d) error = nil, want validation error", count)
		}
	}
	if err := trie.ConfigureLocalPartitions(0); err != nil {
		t.Fatalf("ConfigureLocalPartitions(0) error = %v", err)
	}
	trie.UpsertString("existing", "value")
	if err := trie.ConfigureLocalPartitions(8); err == nil {
		t.Fatal("ConfigureLocalPartitions() on nonempty trie error = nil")
	}
}

func TestLocalPartitionsRouteBasicOperationsAndMergeScans(t *testing.T) {
	trie := newTestTrie(t)
	if err := trie.ConfigureLocalPartitions(8); err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1_700_000_000, 0)
	trie.now = func() time.Time { return now }
	wantKeys := make([]string, 0, 130)
	seenPartitions := map[int]struct{}{}
	for index := 0; index < 128; index++ {
		key := "partition:key:" + strconv.Itoa(index)
		partition, enabled, err := trie.LocalPartitionForKey(key)
		if err != nil || !enabled {
			t.Fatalf("LocalPartitionForKey(%q) = %d/%v/%v, want enabled", key, partition, enabled, err)
		}
		seenPartitions[partition] = struct{}{}
		trie.UpsertString(key, "value-"+strconv.Itoa(index))
		wantKeys = append(wantKeys, key)
	}
	if len(seenPartitions) < 2 {
		t.Fatalf("128 keys used %d local partition, want distribution", len(seenPartitions))
	}
	trie.UpsertCounter("partition:counter", 40)
	trie.IncrementCounter("partition:counter", 2)
	wantKeys = append(wantKeys, "partition:counter")
	trie.UpsertString("partition:ttl", "temporary")
	if !trie.Expire("partition:ttl", time.Minute) {
		t.Fatal("Expire(partition:ttl) = false")
	}
	wantKeys = append(wantKeys, "partition:ttl")

	if got := trie.GetString("partition:key:73"); got != "value-73" {
		t.Fatalf("GetString(partition:key:73) = %q", got)
	}
	if got := trie.GetCounter("partition:counter"); got != 42 {
		t.Fatalf("GetCounter(partition:counter) = %d, want 42", got)
	}
	if ttl := trie.TTL("partition:ttl"); ttl != time.Minute {
		t.Fatalf("TTL(partition:ttl) = %s, want 1m", ttl)
	}
	if !trie.Delete("partition:key:9") || trie.Exists("partition:key:9") {
		t.Fatal("partitioned delete did not remove key")
	}
	wantKeys = removeTestString(wantKeys, "partition:key:9")
	sort.Strings(wantKeys)
	if got := trie.Keys(true); !reflect.DeepEqual(got, wantKeys) {
		t.Fatalf("partitioned Keys(true) mismatch: got %d keys, want %d", len(got), len(wantKeys))
	}
	if got := trie.Size(); got != len(wantKeys) {
		t.Fatalf("partitioned Size() = %d, want %d", got, len(wantKeys))
	}
	stats := trie.LocalPartitioningStats()
	if !stats.Enabled || stats.Partitions != 8 || sumTestInts(stats.Sizes) != len(wantKeys) {
		t.Fatalf("local partition stats = %#v, want eight partitions and %d keys", stats, len(wantKeys))
	}
}

func TestRunLocalPartitionTasksRunsConcurrentlyAndPreservesOrder(t *testing.T) {
	previousProcs := runtime.GOMAXPROCS(8)
	defer runtime.GOMAXPROCS(previousProcs)

	trie := newTestTrie(t)
	if err := trie.ConfigureLocalPartitions(8); err != nil {
		t.Fatal(err)
	}
	set := trie.localPartitionSet()
	indexes := make(map[*HatTrie]int, len(set.tries))
	for index, child := range set.tries {
		indexes[child] = index
	}
	started := make(chan struct{}, len(set.tries))
	release := make(chan struct{})
	type taskResult struct {
		values []int
		err    error
	}
	done := make(chan taskResult, 1)
	go func() {
		values, err := runLocalPartitionTasks(set, func(child *HatTrie) (int, error) {
			started <- struct{}{}
			<-release
			return indexes[child], nil
		})
		done <- taskResult{values: values, err: err}
	}()
	for range set.tries {
		select {
		case <-started:
		case <-time.After(time.Second):
			close(release)
			t.Fatal("partition tasks did not run concurrently")
		}
	}
	close(release)
	result := <-done
	if result.err != nil {
		t.Fatal(result.err)
	}
	for index, value := range result.values {
		if value != index {
			t.Fatalf("result %d = %d, want stable partition order", index, value)
		}
	}

	wantErr := fmt.Errorf("partition task failed")
	var calls atomic.Int32
	_, err := runLocalPartitionTasks(set, func(child *HatTrie) (int, error) {
		calls.Add(1)
		if indexes[child] == 3 {
			return 0, wantErr
		}
		return indexes[child], nil
	})
	if err != wantErr || calls.Load() != int32(len(set.tries)) {
		t.Fatalf("task error/calls = %v/%d", err, calls.Load())
	}
}

func TestLocalPartitionsExecuteCommandsAndCrossPartitionBatchInOrder(t *testing.T) {
	trie := newTestTrie(t)
	if err := trie.ConfigureLocalPartitions(16); err != nil {
		t.Fatal(err)
	}
	requests := make([]CacheCommandRequest, 0, 192)
	for index := 0; index < 64; index++ {
		key := "batch:key:" + strconv.Itoa(index)
		requests = append(requests, CacheCommandRequest{Command: "SETSTR", Key: key, Value: strconv.Itoa(index)})
	}
	for index := 0; index < 64; index++ {
		key := "batch:key:" + strconv.Itoa(index)
		requests = append(requests, CacheCommandRequest{Command: "GET", Key: key})
	}
	response := trie.ExecuteCommand(CacheCommandRequest{Command: "BATCH", Batch: requests})
	if !response.OK || len(response.Responses) != len(requests) {
		t.Fatalf("partitioned BATCH envelope = %#v", response)
	}
	for index := 0; index < 64; index++ {
		if got := response.Responses[64+index].Value; got != strconv.Itoa(index) {
			t.Fatalf("partitioned BATCH GET %d = %q", index, got)
		}
	}
	put := trie.ExecuteCommand(CacheCommandRequest{Command: "PUTMAP", Key: "profile", Pairs: Map{"city": "Singapore"}})
	peek := trie.ExecuteCommand(CacheCommandRequest{Command: "PEEKMAP", Key: "profile", Subkey: "city"})
	if !put.OK || !peek.OK || peek.Value != "Singapore" {
		t.Fatalf("partitioned structured commands = %#v/%#v", put, peek)
	}
}

func TestLocalPartitionsSnapshotRoundTripRemainsPortable(t *testing.T) {
	source := newTestTrie(t)
	if err := source.ConfigureLocalPartitions(8); err != nil {
		t.Fatal(err)
	}
	source.UpsertString("name", "ivi")
	source.UpsertCounter("count", 42)
	if response := source.ExecuteCommand(CacheCommandRequest{Command: "PUTMAP", Key: "profile", Pairs: Map{"city": "Singapore"}}); !response.OK {
		t.Fatalf("PUTMAP response = %#v", response)
	}
	path := filepath.Join(t.TempDir(), "partitioned.snapshot")
	if err := source.SaveSnapshotWithFormat(path, SnapshotFormatBinary); err != nil {
		t.Fatalf("SaveSnapshotWithFormat() error = %v", err)
	}

	partitioned := newTestTrie(t)
	if err := partitioned.ConfigureLocalPartitions(8); err != nil {
		t.Fatal(err)
	}
	if err := partitioned.LoadSnapshot(path); err != nil {
		t.Fatalf("partitioned LoadSnapshot() error = %v", err)
	}
	assertLocalPartitionSnapshotValues(t, partitioned)

	plain := newTestTrie(t)
	if err := plain.LoadSnapshot(path); err != nil {
		t.Fatalf("plain LoadSnapshot() error = %v", err)
	}
	assertLocalPartitionSnapshotValues(t, plain)
}

func TestLocalPartitionSnapshotAtomicallySwapsStagedChildren(t *testing.T) {
	source := newTestTrie(t)
	source.UpsertString("fresh", "value")
	path := filepath.Join(t.TempDir(), "snapshot.hc")
	if err := source.SaveSnapshotWithFormat(path, SnapshotFormatBinary); err != nil {
		t.Fatal(err)
	}

	target := newTestTrie(t)
	if err := target.ConfigureLocalPartitions(8); err != nil {
		t.Fatal(err)
	}
	target.UpsertString("stale", "old")
	oldSet := target.localPartitions.Load()
	oldRoots := make([]unsafe.Pointer, len(oldSet.tries))
	for idx, child := range oldSet.tries {
		oldRoots[idx] = unsafe.Pointer(child.root)
	}
	if err := target.LoadSnapshot(path); err != nil {
		t.Fatal(err)
	}
	newSet := target.localPartitions.Load()
	if newSet != oldSet {
		t.Fatal("LoadSnapshot() replaced partition routing objects instead of their owned generations")
	}
	if len(newSet.tries) != len(oldSet.tries) {
		t.Fatalf("partition count after restore = %d, want %d", len(newSet.tries), len(oldSet.tries))
	}
	for idx := range newSet.tries {
		if unsafe.Pointer(newSet.tries[idx].root) == oldRoots[idx] {
			t.Fatalf("partition %d retained its live C trie instead of swapping a staged generation", idx)
		}
	}
	if target.GetString("fresh") != "value" || target.Exists("stale") {
		t.Fatal("staged partition generation did not replace the exact key set")
	}
}

func TestLocalPartitionSnapshotCaptureReconcilesMutationsBetweenPages(t *testing.T) {
	trie := newTestTrie(t)
	if err := trie.ConfigureLocalPartitions(8); err != nil {
		t.Fatal(err)
	}
	for index := 0; index < snapshotCaptureScanPageEntries*4; index++ {
		trie.UpsertString(fmt.Sprintf("key:%05d", index), "before")
	}
	trie.UpsertString("", "blank-before")

	firstPage := make(chan struct{})
	release := make(chan struct{})
	trie.snapshotCapturePageHook = func(page int) {
		if page != 1 {
			return
		}
		close(firstPage)
		<-release
	}
	captured := make(chan snapshotCapture, 1)
	captureErrors := make(chan error, 1)
	go func() {
		capture, err := trie.captureSnapshot()
		captured <- capture
		captureErrors <- err
	}()

	select {
	case <-firstPage:
	case <-time.After(2 * time.Second):
		close(release)
		t.Fatal("partition snapshot did not expose a bounded scan page")
	}

	mutated := make(chan struct{})
	go func() {
		trie.UpsertString("", "blank-after")
		trie.UpsertString("key:00000", "after")
		trie.Delete("key:00001")
		trie.UpsertMap("key:00002", Map{"state": "structured-after"})
		trie.UpsertString("key:00000:new", "inserted-behind-cursor")
		trie.UpsertString(fmt.Sprintf("key:%05d", snapshotCaptureScanPageEntries*4-1), "tail-after")
		close(mutated)
	}()
	select {
	case <-mutated:
	case <-time.After(2 * time.Second):
		close(release)
		t.Fatal("writer could not complete between partition snapshot pages")
	}
	close(release)
	if err := <-captureErrors; err != nil {
		t.Fatal(err)
	}
	capture := <-captured

	entries := make(map[string]snapshotEntry, capture.count)
	for _, page := range capture.pages {
		for _, entry := range page {
			entries[entry.Key] = entry
		}
	}
	if got := entries["key:00000"].String; got != "after" {
		t.Fatalf("updated value = %q, want after", got)
	}
	if got := entries[""].String; got != "blank-after" {
		t.Fatalf("updated blank-key value = %q, want blank-after", got)
	}
	if _, exists := entries["key:00001"]; exists {
		t.Fatal("deleted key remained in partition snapshot")
	}
	if got := entries["key:00002"].Map["state"]; got != "structured-after" {
		t.Fatalf("structured value = %#v, want structured-after", got)
	}
	if got := entries["key:00000:new"].String; got != "inserted-behind-cursor" {
		t.Fatalf("inserted value = %q, want inserted-behind-cursor", got)
	}
	tailKey := fmt.Sprintf("key:%05d", snapshotCaptureScanPageEntries*4-1)
	if got := entries[tailKey].String; got != "tail-after" {
		t.Fatalf("tail value = %q, want tail-after", got)
	}
	if capture.count != snapshotCaptureScanPageEntries*4+1 {
		t.Fatalf("capture count = %d, want %d", capture.count, snapshotCaptureScanPageEntries*4+1)
	}
}

func TestLocalPartitionSnapshotStreamReconcilesMutationsBetweenPages(t *testing.T) {
	trie := newTestTrie(t)
	if err := trie.ConfigureLocalPartitions(8); err != nil {
		t.Fatal(err)
	}
	for index := 0; index < snapshotCaptureScanPageEntries*2; index++ {
		trie.UpsertString(fmt.Sprintf("stream:%05d", index), "before")
	}

	firstPage := make(chan struct{})
	release := make(chan struct{})
	trie.snapshotCapturePageHook = func(page int) {
		if page == 1 {
			close(firstPage)
			<-release
		}
	}
	var snapshot bytes.Buffer
	written := make(chan error, 1)
	go func() {
		written <- trie.writeSnapshot(&snapshot, 0, SnapshotFormatBinary)
	}()
	select {
	case <-firstPage:
	case <-time.After(2 * time.Second):
		close(release)
		t.Fatal("partition stream snapshot did not expose a bounded scan page")
	}

	trie.UpsertString("stream:00000", "after")
	trie.Delete("stream:00001")
	trie.UpsertString("stream:00000:new", "inserted")
	tailKey := fmt.Sprintf("stream:%05d", snapshotCaptureScanPageEntries*2-1)
	trie.UpsertString(tailKey, "tail-after")
	close(release)
	if err := <-written; err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(t.TempDir(), "partition-stream.snapshot")
	if err := os.WriteFile(path, snapshot.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	restored := newTestTrie(t)
	if err := restored.LoadSnapshot(path); err != nil {
		t.Fatal(err)
	}
	if got := restored.GetString("stream:00000"); got != "after" {
		t.Fatalf("updated stream value = %q, want after", got)
	}
	if restored.Exists("stream:00001") {
		t.Fatal("deleted stream key remained in snapshot")
	}
	if got := restored.GetString("stream:00000:new"); got != "inserted" {
		t.Fatalf("inserted stream value = %q, want inserted", got)
	}
	if got := restored.GetString(tailKey); got != "tail-after" {
		t.Fatalf("tail stream value = %q, want tail-after", got)
	}
	if got := restored.Size(); got != snapshotCaptureScanPageEntries*2 {
		t.Fatalf("restored stream size = %d, want %d", got, snapshotCaptureScanPageEntries*2)
	}
}

func TestLocalPartitionWholeKeyspaceOperationsRemainDeterministic(t *testing.T) {
	trie := newTestTrie(t)
	if err := trie.ConfigureLocalPartitions(16); err != nil {
		t.Fatal(err)
	}

	wantKeys := make([]string, 0, 4096)
	wantValues := make(map[string]string, 4096)
	for index := 0; index < 4096; index++ {
		key := fmt.Sprintf("whole:%06d", index)
		value := "value-" + strconv.Itoa(index)
		trie.UpsertString(key, value)
		wantKeys = append(wantKeys, key)
		wantValues[key] = value
	}

	if got := trie.Keys(true); !reflect.DeepEqual(got, wantKeys) {
		t.Fatalf("Keys(true) mismatch: got %d keys, want %d", len(got), len(wantKeys))
	}
	entries := trie.Entries(true)
	if len(entries) != len(wantKeys) {
		t.Fatalf("Entries(true) returned %d entries, want %d", len(entries), len(wantKeys))
	}
	for index, entry := range entries {
		if entry.Key != wantKeys[index] || trie.GetString(entry.Key) != wantValues[entry.Key] {
			t.Fatalf("entry %d = %#v", index, entry)
		}
	}
	prefixed, err := trie.KeysWithPrefixChecked("whole:001", true)
	if err != nil || len(prefixed) != 1000 || prefixed[0] != "whole:001000" || prefixed[len(prefixed)-1] != "whole:001999" {
		t.Fatalf("prefixed keys = %d/%v", len(prefixed), err)
	}

	var monitored []string
	afterKey := ""
	hasAfterKey := false
	for {
		page := trie.monitoringEntriesPage("whole:", afterKey, hasAfterKey, 127)
		for _, entry := range page.Entries {
			monitored = append(monitored, entry.Key)
		}
		if !page.HasMore {
			break
		}
		afterKey = page.NextAfterKey
		hasAfterKey = true
	}
	if !reflect.DeepEqual(monitored, wantKeys) {
		t.Fatalf("monitoring pagination returned %d keys, want %d", len(monitored), len(wantKeys))
	}

	firstPath := filepath.Join(t.TempDir(), "first.snapshot")
	secondPath := filepath.Join(t.TempDir(), "second.snapshot")
	if err := trie.SaveSnapshotWithFormat(firstPath, SnapshotFormatBinary); err != nil {
		t.Fatal(err)
	}
	if err := trie.SaveSnapshotWithFormat(secondPath, SnapshotFormatBinary); err != nil {
		t.Fatal(err)
	}
	first, err := os.ReadFile(firstPath)
	if err != nil {
		t.Fatal(err)
	}
	second, err := os.ReadFile(secondPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("unchanged partitioned snapshots are not byte-stable")
	}
	restored := newTestTrie(t)
	if err := restored.LoadSnapshot(firstPath); err != nil {
		t.Fatal(err)
	}
	if got := restored.Keys(true); !reflect.DeepEqual(got, wantKeys) {
		t.Fatalf("portable snapshot restored %d keys, want %d", len(got), len(wantKeys))
	}
}

func TestLocalPartitionsPebbleRoundTrip(t *testing.T) {
	source := newTestTrie(t)
	if err := source.ConfigureLocalPartitions(8); err != nil {
		t.Fatal(err)
	}
	source.UpsertString("name", "ivi")
	source.UpsertCounter("count", 42)
	path := filepath.Join(t.TempDir(), "cache.pebble")
	store, err := OpenPebbleStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(source); err != nil {
		store.Close()
		t.Fatalf("Pebble Save() error = %v", err)
	}

	restored := newTestTrie(t)
	if err := restored.ConfigureLocalPartitions(8); err != nil {
		store.Close()
		t.Fatal(err)
	}
	if _, err := store.LoadWithPolicy(restored, LevelDBLoadPolicy{HotValuesOnly: true}); err != nil {
		store.Close()
		t.Fatalf("Pebble LoadWithPolicy() error = %v", err)
	}
	assertLocalPartitionScalarValues(t, restored)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestLocalPartitionsPebbleRestoreIsExactForMixedValuesAndColdReferences(t *testing.T) {
	source := newTestTrie(t)
	for index := 0; index < 4096; index++ {
		key := fmt.Sprintf("restore:%05d", index)
		switch index % 4 {
		case 0:
			source.UpsertString(key, strings.Repeat("s", 128))
		case 1:
			source.UpsertBytes(key, bytes.Repeat([]byte{byte(index)}, 192))
		case 2:
			source.UpsertCounter(key, int32(index))
		case 3:
			source.UpsertMap(key, Map{"index": index, "state": "current"})
		}
	}
	store, err := OpenPebbleStore(filepath.Join(t.TempDir(), "source.pebble"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Save(source); err != nil {
		t.Fatal(err)
	}

	for _, policy := range []LevelDBLoadPolicy{{}, {HotValuesOnly: true}} {
		target := newTestTrie(t)
		if err := target.ConfigureLocalPartitions(16); err != nil {
			t.Fatal(err)
		}
		target.UpsertString("stale", "remove")
		target.UpsertString("restore:00002", "replace-type")
		result, err := store.LoadWithPolicy(target, policy)
		if err != nil {
			t.Fatal(err)
		}
		if result.KeysLoaded != 4096 || target.Size() != 4096 || target.Exists("stale") {
			t.Fatalf("restore result/size/stale = %#v/%d/%v", result, target.Size(), target.Exists("stale"))
		}
		if target.GetString("restore:00000") != strings.Repeat("s", 128) || target.GetCounter("restore:00002") != 2 {
			t.Fatal("restored scalar values do not match")
		}
		if got := target.GetBytes("restore:00001"); !bytes.Equal(got, bytes.Repeat([]byte{1}, 192)) {
			t.Fatalf("restored bytes = %v", got)
		}
		if got := target.GetMap("restore:00003"); got["state"] != "current" {
			t.Fatalf("restored map = %#v", got)
		}
	}
}

func TestLocalPartitionPersistentRestoreRollsBackAllPartitionsOnError(t *testing.T) {
	target := newTestTrie(t)
	if err := target.ConfigureLocalPartitions(8); err != nil {
		t.Fatal(err)
	}
	target.UpsertString("existing:a", "original-a")
	target.UpsertString("existing:b", "original-b")

	_, err := loadLocalPartitionPersistentEntryData(target, nil, LevelDBLoadPolicy{}, func(visit func(snapshotEntry, []byte) error) error {
		for _, entry := range []snapshotEntry{
			{Key: "existing:a", Type: "string", String: "changed-a"},
			{Key: "created:a", Type: "string", String: "created"},
			{Key: "existing:b", Type: "string", String: "changed-b"},
			{Key: "invalid", Type: "unsupported"},
		} {
			if err := visit(entry, nil); err != nil {
				return err
			}
		}
		return nil
	})
	if err == nil {
		t.Fatal("partition restore error = nil, want invalid entry error")
	}
	if target.GetString("existing:a") != "original-a" || target.GetString("existing:b") != "original-b" || target.Exists("created:a") {
		t.Fatalf("partition rollback entries = %#v", target.Entries(true))
	}
}

func TestLocalPartitionSnapshotRestoreRollsBackAllPartitionsOnMismatch(t *testing.T) {
	source := newTestTrie(t)
	source.UpsertString("a", "changed")
	source.UpsertString("b", "created")
	path := filepath.Join(t.TempDir(), "source.snapshot")
	if err := source.SaveSnapshotWithFormat(path, SnapshotFormatBinary); err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	metadata, err := scanSnapshotFileReader(file, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	activeKeys, err := newSnapshotActiveKeys([]string{"a"})
	if err != nil {
		t.Fatal(err)
	}
	target := newTestTrie(t)
	if err := target.ConfigureLocalPartitions(8); err != nil {
		t.Fatal(err)
	}
	target.UpsertString("a", "original")
	if _, err := target.applyPartitionedSnapshotFile(file, metadata, activeKeys, target.currentTime()); !errors.Is(err, errSnapshotChangedDuringLoad) {
		t.Fatalf("partition snapshot restore error = %v, want changed-during-load", err)
	}
	if target.GetString("a") != "original" || target.Exists("b") {
		t.Fatalf("partition snapshot rollback entries = %#v", target.Entries(true))
	}
}

func TestLocalPartitionsMonitoringAndScalarGRPC(t *testing.T) {
	trie := newTestTrie(t)
	if err := trie.ConfigureLocalPartitions(8); err != nil {
		t.Fatal(err)
	}
	trie.UpsertString("session:1", "one")
	trie.UpsertString("session:2", "two")
	entries := trie.monitoringEntriesPage("session:", "", false, 10)
	if len(entries.Entries) != 2 {
		t.Fatalf("partitioned monitoring entries = %#v, want two", entries)
	}
	if got := []string{entries.Entries[0].Key, entries.Entries[1].Key}; !reflect.DeepEqual(got, []string{"session:1", "session:2"}) {
		t.Fatalf("partitioned monitoring keys = %v", got)
	}
	metrics := NewMonitoringHandler(trie, MonitoringOptions{}).prometheusMetrics()
	if !strings.Contains(metrics, "hatrie_cache_local_partitions{node=\"") || !strings.Contains(metrics, "hatrie_cache_local_partition_keys{") {
		t.Fatalf("partition metrics are missing:\n%s", metrics)
	}

	client, stop := newTestGRPCClient(t, trie, CacheGRPCOptions{})
	defer stop()
	stream, err := client.ScalarBatchStream(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&hatriecachev1.ScalarBatchRequest{
		Operations: []hatriecachev1.ScalarCommand{
			hatriecachev1.ScalarCommand_SCALAR_COMMAND_SET_COUNTER,
			hatriecachev1.ScalarCommand_SCALAR_COMMAND_INCREMENT,
			hatriecachev1.ScalarCommand_SCALAR_COMMAND_GET,
		},
		Keys:          []string{"grpc:count", "grpc:count", "grpc:count"},
		IntegerValues: []int64{40, 2},
	}); err != nil {
		t.Fatal(err)
	}
	response, err := stream.Recv()
	if err != nil || !response.GetOk() || string(response.GetValues()) != "42" || !reflect.DeepEqual(response.GetIntegerValues(), []int64{42}) {
		t.Fatalf("partitioned scalar gRPC response = %#v/%v", response, err)
	}
}

func TestLocalPartitionsRouteAllDirectValueFamilies(t *testing.T) {
	trie := newTestTrie(t)
	if err := trie.ConfigureLocalPartitions(16); err != nil {
		t.Fatal(err)
	}

	trie.UpsertBytes("direct:bytes", []byte("payload"))
	if got := string(trie.GetBytes("direct:bytes")); got != "payload" {
		t.Fatalf("bytes = %q", got)
	}
	if got := trie.Get("direct:bytes"); !got.IsBytesAtRaws() {
		t.Fatalf("generic Get() = %#v, want bytes", got)
	}
	trie.UpsertMap("direct:map", Map{"field": "value"})
	if got := trie.PeekMap("direct:map", "field"); got != "value" {
		t.Fatalf("map field = %#v", got)
	}
	trie.PushSlice("direct:slice", "first", "last")
	if got := trie.TailSlice("direct:slice"); got != "last" {
		t.Fatalf("slice tail = %#v", got)
	}
	trie.AddSet("direct:set", "member")
	if !trie.HasSet("direct:set", "member") {
		t.Fatal("set member is missing")
	}
	trie.PushPriorityQueue("direct:pq", 1, "job")
	if item, ok := trie.PeekPriorityQueue("direct:pq"); !ok || item.Value != "job" {
		t.Fatalf("priority queue peek = %#v/%v", item, ok)
	}
	trie.AddBloomFilter("direct:bloom", "member")
	if !trie.HasBloomFilter("direct:bloom", "member") {
		t.Fatal("bloom member is missing")
	}
	trie.AddCuckooFilter("direct:cuckoo", "member")
	if !trie.HasCuckooFilter("direct:cuckoo", "member") {
		t.Fatal("cuckoo member is missing")
	}
	if _, err := trie.AddXorFilter("direct:xor", "member"); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := trie.BuildXorFilter("direct:xor"); err != nil || !ok {
		t.Fatalf("xor build = %v/%v", ok, err)
	}
	if hit, queryable := trie.HasXorFilter("direct:xor", "member"); !hit || !queryable {
		t.Fatalf("xor member = %v/%v", hit, queryable)
	}
	trie.AddRoaringBitmap("direct:roaring", 42)
	if !trie.HasRoaringBitmap("direct:roaring", 42) {
		t.Fatal("roaring member is missing")
	}
	trie.AddSparseBitset("direct:sparse", 1<<40)
	if !trie.HasSparseBitset("direct:sparse", 1<<40) {
		t.Fatal("sparse member is missing")
	}
	trie.PutRadixTree("direct:radix", "session:42", "active")
	if got, ok := trie.GetRadixTree("direct:radix", "session:42"); !ok || got != "active" {
		t.Fatalf("radix value = %#v/%v", got, ok)
	}
	trie.IncrementCountMinSketch("direct:cms", "path", 3)
	if got, ok := trie.EstimateCountMinSketch("direct:cms", "path"); !ok || got < 3 {
		t.Fatalf("count-min estimate = %d/%v", got, ok)
	}
	trie.AddHyperLogLog("direct:hll", "visitor")
	if got, ok := trie.CountHyperLogLog("direct:hll"); !ok || got == 0 {
		t.Fatalf("hyperloglog count = %d/%v", got, ok)
	}
	trie.AddTopK("direct:topk", "path", 3)
	if got := trie.EstimateTopK("direct:topk", "path"); !got.Tracked || got.Count < 3 {
		t.Fatalf("top-k estimate = %#v", got)
	}
	trie.AddReservoirSample("direct:reservoir", "request")
	if got := trie.GetReservoirSample("direct:reservoir"); len(got) != 1 {
		t.Fatalf("reservoir items = %#v", got)
	}
	trie.AddQuantileSketch("direct:quantile", 42)
	if got, ok := trie.EstimateQuantileSketch("direct:quantile", 0.5); !ok || got.Value != 42 {
		t.Fatalf("quantile estimate = %#v/%v", got, ok)
	}
	if _, ok := trie.AddFenwickTree("direct:fenwick", 5, 42); !ok {
		t.Fatal("fenwick add failed")
	}
	if got, ok := trie.RangeSumFenwickTree("direct:fenwick", 0, 5); !ok || got != 42 {
		t.Fatalf("fenwick range sum = %d/%v", got, ok)
	}

	if got := trie.Size(); got != 17 {
		t.Fatalf("partitioned direct value family size = %d, want 17", got)
	}
}

func TestLocalPartitionsMaintenancePaths(t *testing.T) {
	trie := newTestTrie(t)
	if err := trie.ConfigureLocalPartitions(8); err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 64; index++ {
		trie.UpsertString("maintenance:"+strconv.Itoa(index), "value")
	}

	compacted, err := trie.CompactMemory()
	if err != nil || compacted.Entries != 64 {
		t.Fatalf("CompactMemory() = %#v/%v", compacted, err)
	}
	merkle, err := trie.replicationMerkleSnapshot()
	if err != nil || merkle.count != 64 {
		t.Fatalf("replicationMerkleSnapshot() = %d/%v", merkle.count, err)
	}
	var replicated []string
	page, err := replicationSyncEntriesPage(trie, "maintenance:", "", false, 10, func(entry Entry) error {
		replicated = append(replicated, entry.Key)
		return nil
	})
	if err != nil || len(replicated) != 10 || !page.hasMore {
		t.Fatalf("replication page = %#v/%v, keys=%d", page, err, len(replicated))
	}
}

func TestLocalPartitionsColdSpillAcrossStorageBackends(t *testing.T) {
	for _, backend := range []StorageBackend{StorageBackendLevelDB, StorageBackendPebble} {
		t.Run(string(backend), func(t *testing.T) {
			trie := newTestTrie(t)
			if err := trie.ConfigureLocalPartitions(8); err != nil {
				t.Fatal(err)
			}
			value := strings.Repeat("x", 256)
			for index := 0; index < 64; index++ {
				trie.UpsertString("spill:"+strconv.Itoa(index), value)
			}
			store, err := OpenPersistentStoreWithFormat(filepath.Join(t.TempDir(), "cache"), backend, StorageFormatBinary)
			if err != nil {
				t.Fatal(err)
			}
			underCap, err := store.SpillCold(trie, LevelDBSpillOptions{MaxHotBytes: 1 << 20, MinValueBytes: 1})
			if err != nil || underCap.KeysSpilled != 0 || underCap.HotBytesAfter != underCap.HotBytesBefore {
				store.Close()
				t.Fatalf("SpillCold(under cap) = %#v/%v", underCap, err)
			}
			result, err := store.SpillCold(trie, LevelDBSpillOptions{MaxHotBytes: 0, MinValueBytes: 1})
			if err != nil {
				store.Close()
				t.Fatalf("SpillCold() error = %v", err)
			}
			if result.KeysSpilled != 64 || result.HotBytesAfter != 0 || result.WriteBatches == 0 || result.WriteBatches > 8 {
				store.Close()
				t.Fatalf("SpillCold() = %#v", result)
			}
			if got := trie.GetString("spill:42"); got != value {
				store.Close()
				t.Fatalf("hydrated value length = %d", len(got))
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestLocalPartitionsSaveAndLoadAggregateStats(t *testing.T) {
	source := newTestTrie(t)
	if err := source.ConfigureLocalPartitions(8); err != nil {
		t.Fatal(err)
	}
	source.UpsertString("stats:a", "a")
	source.UpsertString("stats:b", "b")
	_ = source.GetString("stats:a")
	_ = source.GetString("stats:missing")
	path := filepath.Join(t.TempDir(), "stats.json")
	if err := source.SaveStats(path); err != nil {
		t.Fatal(err)
	}

	target := newTestTrie(t)
	if err := target.ConfigureLocalPartitions(8); err != nil {
		t.Fatal(err)
	}
	if err := target.LoadStats(path); err != nil {
		t.Fatal(err)
	}
	got := target.Stats()
	if got.Writes != 2 || got.Reads != 2 || got.Hits != 1 || got.Misses != 1 {
		t.Fatalf("restored aggregate stats = %#v", got)
	}
}

func TestLocalPartitionsBoundGlobalKeyStatsCapacity(t *testing.T) {
	trie := newTestTrie(t)
	if err := trie.ConfigureLocalPartitions(16); err != nil {
		t.Fatal(err)
	}
	if err := trie.ConfigureCounterWriteStripes(64); err != nil {
		t.Fatal(err)
	}
	if stats := trie.CounterWriteStripingStats(); !stats.Enabled || stats.Stripes != 64 {
		t.Fatalf("partitioned counter stripes = %#v", stats)
	}
	if err := trie.ConfigureKeyStats(KeyStatsModeBounded, 10); err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 1_000; index++ {
		key := "key-stats:" + strconv.Itoa(index)
		trie.UpsertString(key, "value")
		_ = trie.GetString(key)
	}
	policy := trie.KeyStatsPolicy()
	if policy.Mode != KeyStatsModeBounded || policy.Capacity != 10 || policy.Tracked > 10 {
		t.Fatalf("partitioned key stats policy = %#v", policy)
	}
}

func assertLocalPartitionSnapshotValues(t *testing.T, trie *HatTrie) {
	t.Helper()
	if trie.GetString("name") != "ivi" || trie.GetCounter("count") != 42 {
		t.Fatalf("restored scalar values = %q/%d", trie.GetString("name"), trie.GetCounter("count"))
	}
	peek := trie.ExecuteCommand(CacheCommandRequest{Command: "PEEKMAP", Key: "profile", Subkey: "city"})
	if !peek.OK || peek.Value != "Singapore" {
		t.Fatalf("restored map response = %#v", peek)
	}
}

func assertLocalPartitionScalarValues(t *testing.T, trie *HatTrie) {
	t.Helper()
	if trie.GetString("name") != "ivi" || trie.GetCounter("count") != 42 {
		t.Fatalf("restored scalar values = %q/%d", trie.GetString("name"), trie.GetCounter("count"))
	}
}

func removeTestString(values []string, remove string) []string {
	for index, value := range values {
		if value == remove {
			return append(values[:index], values[index+1:]...)
		}
	}
	return values
}

func sumTestInts(values []int) int {
	total := 0
	for _, value := range values {
		total += value
	}
	return total
}
