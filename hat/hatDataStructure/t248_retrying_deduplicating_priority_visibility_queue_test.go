package hatDataStructure

import (
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
	"time"
)

func TestT248RetryingDeduplicatingPriorityVisibilityQueueDeadLettersAfterMaxAttempts(t *testing.T) {
	queue := NewRetryingDeduplicatingPriorityVisibilityQueueWithOptions[string](RetryingDeduplicatingPriorityVisibilityQueueOptions{
		PriorityVisibilityQueueOptions: PriorityVisibilityQueueOptions{
			VisibilityTimeout: time.Second,
		},
		MaxAttempts: 2,
	})
	if !queue.Enqueue("job-1", 10, "payload") {
		t.Fatal("enqueue failed")
	}
	now := time.Unix(100, 0).UTC()
	first, ok := queue.Lease(now)
	if !ok || first.Attempts != 1 {
		t.Fatalf("first Lease() = %#v/%v, want attempt 1", first, ok)
	}
	if !queue.Nack(first.ID, time.Time{}) {
		t.Fatal("first Nack() failed")
	}
	second, ok := queue.Lease(now)
	if !ok || second.Attempts != 2 {
		t.Fatalf("second Lease() = %#v/%v, want attempt 2", second, ok)
	}
	if !queue.Nack(second.ID, time.Time{}) {
		t.Fatal("max-attempt Nack() failed")
	}
	if queue.Len() != 0 || queue.DeadLetterLen() != 1 {
		t.Fatalf("queue lengths = work %d, dead letters %d; want 0 and 1", queue.Len(), queue.DeadLetterLen())
	}
	dead, ok := queue.PopDeadLetter()
	if !ok || dead.Key != "job-1" || dead.Value != "payload" || dead.Attempts != 2 || dead.Reason != RetryDeadLetterMaxAttempts {
		t.Fatalf("PopDeadLetter() = %#v/%v, want max-attempt record", dead, ok)
	}
	if !queue.Enqueue("job-1", 1, "replacement") {
		t.Fatal("key was not released after dead-letter routing")
	}
}

func TestT248RetryingDeduplicatingPriorityVisibilityQueueExpiredLeaseCountsTowardLimit(t *testing.T) {
	queue := NewRetryingDeduplicatingPriorityVisibilityQueueWithOptions[string](RetryingDeduplicatingPriorityVisibilityQueueOptions{
		PriorityVisibilityQueueOptions: PriorityVisibilityQueueOptions{
			VisibilityTimeout: time.Second,
		},
		MaxAttempts: 2,
	})
	if !queue.Enqueue("job-2", 1, "crash-retry") {
		t.Fatal("enqueue failed")
	}
	now := time.Unix(200, 0).UTC()
	first, ok := queue.Lease(now)
	if !ok {
		t.Fatal("first Lease() returned no item")
	}
	if got := queue.RequeueExpired(first.LeaseUntil); got != 1 {
		t.Fatalf("first RequeueExpired() = %d, want 1", got)
	}
	second, ok := queue.Lease(first.LeaseUntil)
	if !ok || second.Attempts != 2 {
		t.Fatalf("second Lease() = %#v/%v, want attempt 2", second, ok)
	}
	if got := queue.RequeueExpired(second.LeaseUntil); got != 1 {
		t.Fatalf("second RequeueExpired() = %d, want one dead-lettered item", got)
	}
	dead, ok := queue.PopDeadLetter()
	if !ok || dead.Reason != RetryDeadLetterVisibilityExpired || dead.Attempts != 2 {
		t.Fatalf("expired dead letter = %#v/%v, want expiry reason and attempt 2", dead, ok)
	}
}

func TestT248RetryingDeduplicatingPriorityVisibilityQueueAllowsUnlimitedRetriesWhenDisabled(t *testing.T) {
	queue := NewRetryingDeduplicatingPriorityVisibilityQueue[string](0, time.Minute, 0)
	if !queue.Enqueue("unlimited", 1, "value") {
		t.Fatal("enqueue failed")
	}
	now := time.Unix(300, 0).UTC()
	for attempt := uint32(1); attempt <= 3; attempt++ {
		item, ok := queue.Lease(now)
		if !ok || item.Attempts != attempt {
			t.Fatalf("Lease() = %#v/%v, want attempt %d", item, ok, attempt)
		}
		if !queue.Nack(item.ID, time.Time{}) {
			t.Fatalf("Nack() failed on attempt %d", attempt)
		}
	}
	if queue.DeadLetterLen() != 0 {
		t.Fatalf("DeadLetterLen() = %d, want 0", queue.DeadLetterLen())
	}
}

func TestT248RetryingDeduplicatingPriorityVisibilityQueueSnapshotPreservesDeadLettersAndLeases(t *testing.T) {
	queue := NewRetryingDeduplicatingPriorityVisibilityQueueWithOptions[string](RetryingDeduplicatingPriorityVisibilityQueueOptions{
		PriorityVisibilityQueueOptions: PriorityVisibilityQueueOptions{
			Capacity:          8,
			VisibilityTimeout: time.Minute,
		},
		MaxAttempts: 1,
	})
	if !queue.Enqueue("dead", 10, "dead-value") {
		t.Fatal("dead-letter fixture enqueue failed")
	}
	deadItem, ok := queue.Lease(time.Unix(400, 0).UTC())
	if !ok || !queue.Nack(deadItem.ID, time.Time{}) {
		t.Fatal("dead-letter fixture failed")
	}
	if !queue.Enqueue("active", 1, "active-value") {
		t.Fatal("active fixture enqueue failed")
	}
	active, ok := queue.LeaseWithToken(time.Unix(400, 0).UTC())
	if !ok {
		t.Fatal("active LeaseWithToken() returned no item")
	}
	snapshot := queue.Snapshot()
	if snapshot.MaxAttempts != 1 || len(snapshot.DeadLetters) != 1 || len(snapshot.Queue.Leases) != 1 {
		t.Fatalf("snapshot = %#v, want max attempts, dead letter, and lease", snapshot)
	}
	restored, err := RestoreRetryingDeduplicatingPriorityVisibilityQueue(snapshot, 0)
	if err != nil {
		t.Fatalf("RestoreRetryingDeduplicatingPriorityVisibilityQueue() error = %v", err)
	}
	if restored.Enqueue("active", 1, "duplicate") {
		t.Fatal("restored active duplicate succeeded")
	}
	if !restored.AckToken(active.Token) {
		t.Fatal("restored AckToken() failed")
	}
	if !restored.Enqueue("active", 1, "replacement") {
		t.Fatal("enqueue after restored AckToken() failed")
	}
	dead, ok := restored.PopDeadLetter()
	if !ok || dead.Key != "dead" || dead.Value != "dead-value" {
		t.Fatalf("restored dead letter = %#v/%v, want original", dead, ok)
	}
}

func TestT248RetryingDeduplicatingPriorityVisibilityQueueBinaryAndFileRestore(t *testing.T) {
	queue := NewRetryingDeduplicatingPriorityVisibilityQueueWithOptions[string](RetryingDeduplicatingPriorityVisibilityQueueOptions{
		PriorityVisibilityQueueOptions: PriorityVisibilityQueueOptions{
			VisibilityTimeout: time.Minute,
			Epoch:             17,
		},
		MaxAttempts: 1,
	})
	if !queue.Enqueue("dead-key", 1, "dead-value") {
		t.Fatal("dead-letter enqueue failed")
	}
	deadLease, ok := queue.LeaseWithToken(time.Unix(500, 0).UTC())
	if !ok || !queue.NackToken(deadLease.Token, time.Time{}) {
		t.Fatal("dead-letter routing failed")
	}
	if !queue.Enqueue("disk-key", 1, "disk-value") {
		t.Fatal("enqueue failed")
	}
	lease, ok := queue.LeaseWithToken(time.Unix(500, 0).UTC())
	if !ok {
		t.Fatal("LeaseWithToken() returned no item")
	}
	codec := RetryingDeduplicatingPriorityVisibilityQueueCodec[string]{
		Encode: func(value string) ([]byte, error) { return []byte(value), nil },
		Decode: func(value []byte) (string, error) { return string(value), nil },
	}
	payload, err := queue.MarshalSnapshot(codec)
	if err != nil {
		t.Fatalf("MarshalSnapshot() error = %v", err)
	}
	corrupt := append([]byte(nil), payload...)
	corrupt[len(corrupt)-1] ^= 1
	if _, err := UnmarshalRetryingDeduplicatingPriorityVisibilityQueue(corrupt, codec); err == nil {
		t.Fatal("UnmarshalRetryingDeduplicatingPriorityVisibilityQueue accepted corrupt checksum")
	}
	unmarshaled, err := UnmarshalRetryingDeduplicatingPriorityVisibilityQueue(payload, codec)
	if err != nil {
		t.Fatalf("UnmarshalRetryingDeduplicatingPriorityVisibilityQueue() error = %v", err)
	}
	dead, ok := unmarshaled.PopDeadLetter()
	if !ok || dead.Key != "dead-key" || dead.Value != "dead-value" {
		t.Fatalf("unmarshaled dead letter = %#v/%v, want original", dead, ok)
	}
	if !unmarshaled.AckToken(lease.Token) {
		t.Fatal("unmarshaled AckToken() failed")
	}
	if !queue.Enqueue("disk-key-2", 1, "second") {
		t.Fatal("second enqueue failed")
	}
	path := filepath.Join(t.TempDir(), "retrying-queue.snapshot")
	if err := SaveRetryingDeduplicatingPriorityVisibilityQueue(path, queue, codec); err != nil {
		t.Fatalf("SaveRetryingDeduplicatingPriorityVisibilityQueue() error = %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat(snapshot) error = %v", err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("snapshot mode = %o, want 600", got)
	}
	loaded, err := LoadRetryingDeduplicatingPriorityVisibilityQueue[string](path, codec)
	if err != nil {
		t.Fatalf("LoadRetryingDeduplicatingPriorityVisibilityQueue() error = %v", err)
	}
	if loaded.Enqueue("disk-key", 1, "duplicate") {
		t.Fatal("loaded duplicate leased key succeeded")
	}
	if loaded.AckToken(lease.Token) {
		t.Fatal("stale loaded AckToken() succeeded")
	}
	dead, ok = loaded.PopDeadLetter()
	if !ok || dead.Key != "dead-key" || dead.Value != "dead-value" {
		t.Fatalf("loaded dead letter = %#v/%v, want original", dead, ok)
	}
}

func TestT248RetryingDeduplicatingPriorityVisibilityQueueReportsActiveHeap(t *testing.T) {
	const itemCount = 10000
	baseBytes, base := measureT248ActiveQueueHeap(func() interface{} {
		queue := NewDeduplicatingPriorityVisibilityQueue[int](itemCount, time.Hour)
		for index := 0; index < itemCount; index++ {
			if !queue.Enqueue("active-"+strconv.Itoa(index), int64(index), index) {
				t.Fatalf("base enqueue %d failed", index)
			}
		}
		for index := 0; index < itemCount; index++ {
			if _, ok := queue.Lease(time.Unix(100, 0).UTC()); !ok {
				t.Fatalf("base lease %d failed", index)
			}
		}
		return queue
	})
	retryingBytes, retrying := measureT248ActiveQueueHeap(func() interface{} {
		queue := NewRetryingDeduplicatingPriorityVisibilityQueue[int](itemCount, time.Hour, 3)
		for index := 0; index < itemCount; index++ {
			if !queue.Enqueue("active-"+strconv.Itoa(index), int64(index), index) {
				t.Fatalf("retrying enqueue %d failed", index)
			}
		}
		for index := 0; index < itemCount; index++ {
			if _, ok := queue.Lease(time.Unix(100, 0).UTC()); !ok {
				t.Fatalf("retrying lease %d failed", index)
			}
		}
		return queue
	})
	runtime.KeepAlive(base)
	runtime.KeepAlive(retrying)
	t.Logf("active heap for %d leased items: base=%d bytes (%.1f/item), retrying=%d bytes (%.1f/item), ratio=%.2fx", itemCount, baseBytes, float64(baseBytes)/itemCount, retryingBytes, float64(retryingBytes)/itemCount, float64(retryingBytes)/float64(baseBytes))
}

func measureT248ActiveQueueHeap(build func() interface{}) (uint64, interface{}) {
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	queue := build()
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	runtime.KeepAlive(queue)
	if after.HeapAlloc < before.HeapAlloc {
		return 0, queue
	}
	return after.HeapAlloc - before.HeapAlloc, queue
}
