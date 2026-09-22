package hatDataStructure

import (
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
	"time"
)

func TestT247DeduplicatingPriorityVisibilityQueueRejectsDuplicateUntilAck(t *testing.T) {
	queue := NewDeduplicatingPriorityVisibilityQueueWithOptions[string](PriorityVisibilityQueueOptions{
		VisibilityTimeout: time.Minute,
	})
	if !queue.Enqueue("job-1", 10, "first") {
		t.Fatal("initial enqueue failed")
	}
	if queue.Enqueue("job-1", 1, "duplicate") {
		t.Fatal("duplicate pending enqueue succeeded")
	}
	item, ok := queue.Lease(time.Unix(100, 0).UTC())
	if !ok || item.Key != "job-1" || item.Value != "first" {
		t.Fatalf("Lease() = %#v/%v, want first job", item, ok)
	}
	if queue.Enqueue("job-1", 1, "duplicate while leased") {
		t.Fatal("duplicate leased enqueue succeeded")
	}
	if !queue.Ack(item.ID) {
		t.Fatal("Ack() failed")
	}
	if !queue.Enqueue("job-1", 1, "replacement") {
		t.Fatal("enqueue after ack failed")
	}
}

func TestT247DeduplicatingPriorityVisibilityQueueRetainsIdentityAcrossRetryAndExpiry(t *testing.T) {
	now := time.Unix(200, 0).UTC()
	queue := NewDeduplicatingPriorityVisibilityQueueWithOptions[string](PriorityVisibilityQueueOptions{
		VisibilityTimeout: time.Second,
	})
	if !queue.Enqueue("job-2", 1, "retryable") {
		t.Fatal("enqueue failed")
	}
	item, ok := queue.Lease(now)
	if !ok || !queue.Nack(item.ID, now.Add(5*time.Second)) {
		t.Fatalf("Nack() failed for %#v/%v", item, ok)
	}
	if queue.Enqueue("job-2", 1, "duplicate during nack delay") {
		t.Fatal("duplicate during nack delay succeeded")
	}
	retry, ok := queue.Lease(now.Add(5 * time.Second))
	if !ok || retry.Key != "job-2" || retry.Attempts != 2 {
		t.Fatalf("retry Lease() = %#v/%v, want second attempt", retry, ok)
	}
	if !queue.Nack(retry.ID, time.Time{}) {
		t.Fatal("immediate Nack() failed")
	}
	expiring, ok := queue.Lease(now.Add(6 * time.Second))
	if !ok {
		t.Fatal("second lease returned no item")
	}
	if got := queue.RequeueExpired(expiring.LeaseUntil); got != 1 {
		t.Fatalf("RequeueExpired() = %d, want 1", got)
	}
	if queue.Enqueue("job-2", 1, "duplicate after expiry") {
		t.Fatal("duplicate after expiry succeeded")
	}
	recovered, ok := queue.Lease(expiring.LeaseUntil)
	if !ok || recovered.Key != "job-2" {
		t.Fatalf("recovered Lease() = %#v/%v, want job-2", recovered, ok)
	}
}

func TestT247DeduplicatingPriorityVisibilityQueueSnapshotPreservesKeys(t *testing.T) {
	queue := NewDeduplicatingPriorityVisibilityQueueWithOptions[string](PriorityVisibilityQueueOptions{
		Capacity:          8,
		VisibilityTimeout: time.Minute,
		Epoch:             7,
		StarvationAfter:   2,
	})
	if !queue.Enqueue("pending", 10, "pending-value") || !queue.EnqueueAt("delayed", 1, time.Unix(300, 0).UTC(), "delayed-value") {
		t.Fatal("enqueue snapshot fixtures failed")
	}
	leased, ok := queue.Lease(time.Unix(100, 0).UTC())
	if !ok || leased.Key != "pending" {
		t.Fatalf("Lease() = %#v/%v, want pending item", leased, ok)
	}
	snapshot := queue.Snapshot()
	if snapshot.StarvationAfter != 2 || len(snapshot.Pending) != 1 || len(snapshot.Leases) != 1 {
		t.Fatalf("snapshot = %#v, want fairness, pending, and lease state", snapshot)
	}
	restored, err := RestoreDeduplicatingPriorityVisibilityQueue(snapshot, 8)
	if err != nil {
		t.Fatalf("RestoreDeduplicatingPriorityVisibilityQueue() error = %v", err)
	}
	if restored.Enqueue("pending", 1, "duplicate") {
		t.Fatal("restored duplicate pending/leased key succeeded")
	}
	if !restored.Ack(leased.ID) {
		t.Fatal("restored Ack() failed")
	}
	if !restored.Enqueue("pending", 1, "replacement") {
		t.Fatal("restored enqueue after Ack() failed")
	}
}

func TestT247DeduplicatingPriorityVisibilityQueueBinaryRoundTrip(t *testing.T) {
	queue := NewDeduplicatingPriorityVisibilityQueueWithOptions[string](PriorityVisibilityQueueOptions{
		VisibilityTimeout: time.Minute,
		StarvationAfter:   4,
	})
	if !queue.Enqueue("binary-key", 3, "binary-value") {
		t.Fatal("enqueue failed")
	}
	codec := DeduplicatingPriorityVisibilityQueueCodec[string]{
		Encode: func(value string) ([]byte, error) { return []byte(value), nil },
		Decode: func(value []byte) (string, error) { return string(value), nil },
	}
	payload, err := queue.MarshalSnapshot(codec)
	if err != nil {
		t.Fatalf("MarshalSnapshot() error = %v", err)
	}
	restored, err := UnmarshalDeduplicatingPriorityVisibilityQueue(payload, codec)
	if err != nil {
		t.Fatalf("UnmarshalDeduplicatingPriorityVisibilityQueue() error = %v", err)
	}
	item, ok := restored.Lease(time.Unix(400, 0).UTC())
	if !ok || item.Key != "binary-key" || item.Value != "binary-value" {
		t.Fatalf("binary Lease() = %#v/%v, want original key/value", item, ok)
	}
}

func TestT247DeduplicatingPriorityVisibilityQueueSaveLoadAndTokens(t *testing.T) {
	now := time.Unix(500, 0).UTC()
	queue := NewDeduplicatingPriorityVisibilityQueueWithOptions[string](PriorityVisibilityQueueOptions{
		VisibilityTimeout: time.Minute,
		Epoch:             19,
	})
	if !queue.Enqueue("token-key", 1, "token-value") {
		t.Fatal("enqueue failed")
	}
	lease, ok := queue.LeaseWithToken(now)
	if !ok || !queue.NackToken(lease.Token, time.Time{}) {
		t.Fatalf("LeaseWithToken/NackToken failed: %#v/%v", lease, ok)
	}
	retry, ok := queue.LeaseWithToken(now)
	if !ok {
		t.Fatal("retry LeaseWithToken returned no item")
	}
	codec := DeduplicatingPriorityVisibilityQueueCodec[string]{
		Encode: func(value string) ([]byte, error) { return []byte(value), nil },
		Decode: func(value []byte) (string, error) { return string(value), nil },
	}
	path := filepath.Join(t.TempDir(), "queue.snapshot")
	if err := SaveDeduplicatingPriorityVisibilityQueue(path, queue, codec); err != nil {
		t.Fatalf("SaveDeduplicatingPriorityVisibilityQueue() error = %v", err)
	}
	loaded, err := LoadDeduplicatingPriorityVisibilityQueue[string](path, codec)
	if err != nil {
		t.Fatalf("LoadDeduplicatingPriorityVisibilityQueue() error = %v", err)
	}
	if loaded.Enqueue("token-key", 1, "duplicate") {
		t.Fatal("duplicate loaded leased key succeeded")
	}
	if loaded.AckToken(retry.Token) {
		t.Fatal("stale persisted AckToken() succeeded after load")
	}
	if got := loaded.RequeueExpired(retry.LeaseUntil); got != 1 {
		t.Fatalf("loaded RequeueExpired() = %d, want 1", got)
	}
	reloaded, ok := loaded.LeaseWithToken(retry.LeaseUntil)
	if !ok {
		t.Fatal("reloaded LeaseWithToken() returned no item")
	}
	if !loaded.AckToken(reloaded.Token) {
		t.Fatal("loaded AckToken() with refreshed token failed")
	}
	if !loaded.Enqueue("token-key", 1, "replacement") {
		t.Fatal("enqueue after loaded AckToken() failed")
	}
}

func TestT247DeduplicatingPriorityVisibilityQueueReportsResidentHeap(t *testing.T) {
	const itemCount = 10000
	baselineBytes, baseline := measureT247QueueHeap(func() interface{} {
		queue := NewPriorityVisibilityQueue[int](itemCount, time.Minute)
		for index := 0; index < itemCount; index++ {
			if !queue.Enqueue(int64(index), index) {
				t.Fatalf("baseline enqueue %d failed", index)
			}
		}
		return queue
	})
	deduplicatingBytes, deduplicating := measureT247QueueHeap(func() interface{} {
		queue := NewDeduplicatingPriorityVisibilityQueue[int](itemCount, time.Minute)
		for index := 0; index < itemCount; index++ {
			if !queue.Enqueue(strconv.Itoa(index), int64(index), index) {
				t.Fatalf("deduplicating enqueue %d failed", index)
			}
		}
		return queue
	})
	runtime.KeepAlive(baseline)
	runtime.KeepAlive(deduplicating)
	t.Logf("resident heap for %d pending items: baseline=%d bytes (%.1f/item), deduplicating=%d bytes (%.1f/item), ratio=%.2fx", itemCount, baselineBytes, float64(baselineBytes)/itemCount, deduplicatingBytes, float64(deduplicatingBytes)/itemCount, float64(deduplicatingBytes)/float64(baselineBytes))
}

func TestT247DeduplicatingPriorityVisibilityQueueRejectsDuplicateSnapshotKeys(t *testing.T) {
	_, err := RestoreDeduplicatingPriorityVisibilityQueue(DeduplicatingPriorityVisibilityQueueSnapshot[string]{
		Capacity: 2,
		NextID:   2,
		Pending: []DeduplicatingPriorityVisibilityQueueSnapshotItem[string]{
			{ID: 1, Key: "same", Sequence: 1, Value: "pending"},
		},
		Leases: []DeduplicatingPriorityVisibilityQueueLeaseSnapshot[string]{
			{ID: 2, Key: "same", Sequence: 2, Value: "leased", LeaseUntil: time.Unix(100, 0).UTC()},
		},
	}, 1)
	if err == nil {
		t.Fatal("RestoreDeduplicatingPriorityVisibilityQueue accepted duplicate keys")
	}
}

func TestT247DeduplicatingPriorityVisibilityQueueRejectsInvalidKeys(t *testing.T) {
	queue := NewDeduplicatingPriorityVisibilityQueue[string](0, time.Minute)
	if queue.Enqueue("", 1, "empty") {
		t.Fatal("empty key was accepted")
	}
	tooLong := make([]byte, deduplicatingPriorityVisibilityQueueMaxKeyBytes+1)
	for index := range tooLong {
		tooLong[index] = 'x'
	}
	if queue.Enqueue(string(tooLong), 1, "too long") {
		t.Fatal("oversized key was accepted")
	}
}

func measureT247QueueHeap(build func() interface{}) (uint64, interface{}) {
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
