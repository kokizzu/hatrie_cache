package hatDataStructure

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func priorityVisibilityQueueStringCodec() PriorityVisibilityQueueCodec[string] {
	return PriorityVisibilityQueueCodec[string]{
		Encode: func(value string) ([]byte, error) { return []byte(value), nil },
		Decode: func(payload []byte) (string, error) { return string(payload), nil },
	}
}

func ExamplePriorityVisibilityQueue() {
	now := time.Unix(100, 0).UTC()
	queue := NewPriorityVisibilityQueue[string](1024, 30*time.Second)
	queue.Enqueue(20, "normal")
	queue.Enqueue(1, "urgent")
	item, ok := queue.Lease(now)
	fmt.Printf("%d %d %s %d %t\n", item.ID, item.Priority, item.Value, item.Attempts, ok)
	if ok {
		queue.Ack(item.ID)
	}
	// Output:
	// 2 1 urgent 1 true
}

func TestPriorityVisibilityQueuePriorityLeaseAndDelay(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	queue := NewPriorityVisibilityQueue[string](4, time.Second)
	if !queue.Enqueue(10, "normal") || !queue.Enqueue(1, "urgent") {
		t.Fatal("Enqueue returned false")
	}
	if !queue.EnqueueAt(-100, now.Add(time.Minute), "future") {
		t.Fatal("EnqueueAt returned false")
	}

	first, ok := queue.LeaseWithToken(now)
	if !ok || first.Value != "urgent" || first.Priority != 1 || first.Attempts != 1 {
		t.Fatalf("first LeaseWithToken() = %#v/%v, want urgent priority 1 attempt 1", first, ok)
	}
	second, ok := queue.Lease(now)
	if !ok || second.Value != "normal" || second.Priority != 10 {
		t.Fatalf("second Lease() = %#v/%v, want normal priority 10", second, ok)
	}
	if _, ok := queue.Lease(now); ok {
		t.Fatal("Lease returned delayed work before its ready time")
	}
	if !queue.AckToken(first.Token) || !queue.Ack(second.ID) {
		t.Fatal("Ack rejected active leases")
	}

	future, ok := queue.Lease(now.Add(time.Minute))
	if !ok || future.Value != "future" || future.Priority != -100 {
		t.Fatalf("future Lease() = %#v/%v, want delayed priority -100", future, ok)
	}
}

func TestPriorityVisibilityQueueRetryAndExpiry(t *testing.T) {
	now := time.Unix(200, 0).UTC()
	queue := NewPriorityVisibilityQueue[string](0, time.Second)
	if !queue.Enqueue(5, "retry") || !queue.Enqueue(6, "expiry") {
		t.Fatal("Enqueue returned false")
	}

	first, ok := queue.LeaseWithToken(now)
	if !ok {
		t.Fatal("first LeaseWithToken returned no item")
	}
	if !queue.NackToken(first.Token, now.Add(10*time.Second)) {
		t.Fatal("NackToken rejected active lease")
	}
	retry, ok := queue.LeaseWithToken(now.Add(10 * time.Second))
	if !ok || retry.Token.ID != first.Token.ID || retry.Attempts != 2 || retry.Value != "retry" {
		t.Fatalf("retry LeaseWithToken() = %#v/%v, want same ID and attempt 2", retry, ok)
	}
	if !queue.AckToken(retry.Token) {
		t.Fatal("AckToken rejected retried lease")
	}

	expiring, ok := queue.Lease(now)
	if !ok || expiring.Value != "expiry" {
		t.Fatalf("expiry Lease() = %#v/%v, want expiry", expiring, ok)
	}
	if got := queue.RequeueExpired(now.Add(time.Second)); got != 1 {
		t.Fatalf("RequeueExpired() = %d, want 1", got)
	}
	recovered, ok := queue.Lease(now.Add(time.Second))
	if !ok || recovered.ID != expiring.ID || recovered.Attempts != 2 {
		t.Fatalf("recovered Lease() = %#v/%v, want same ID and attempt 2", recovered, ok)
	}
}

func TestPriorityVisibilityQueueSnapshotRestoreFencesEpoch(t *testing.T) {
	now := time.Unix(300, 0).UTC()
	queue := NewPriorityVisibilityQueueWithEpoch[string](0, time.Second, 41)
	if !queue.Enqueue(20, "pending") || !queue.Enqueue(10, "leased") {
		t.Fatal("Enqueue returned false")
	}
	active, ok := queue.LeaseWithToken(now)
	if !ok {
		t.Fatal("LeaseWithToken returned no item")
	}
	if !queue.EnqueueAt(1, now.Add(time.Minute), "delayed") {
		t.Fatal("delayed EnqueueAt returned false")
	}

	restored, err := RestorePriorityVisibilityQueue(queue.Snapshot(), 42)
	if err != nil {
		t.Fatalf("RestorePriorityVisibilityQueue() error = %v", err)
	}
	if restored.Epoch() != 42 || restored.Len() != 3 || restored.LeaseLen() != 1 {
		t.Fatalf("restored queue epoch/lengths = %d/%d/%d, want 42/3/1", restored.Epoch(), restored.Len(), restored.LeaseLen())
	}
	if restored.AckToken(active.Token) {
		t.Fatal("restored queue accepted a lease token from the old epoch")
	}
	pending, ok := restored.Lease(now)
	if !ok || pending.Value != "pending" || pending.Priority != 20 {
		t.Fatalf("restored pending Lease() = %#v/%v, want pending priority 20", pending, ok)
	}
	if got := restored.RequeueExpired(now.Add(time.Second)); got != 2 {
		t.Fatalf("restored RequeueExpired() = %d, want 2", got)
	}
	recovered, ok := restored.Lease(now.Add(time.Second))
	if !ok || recovered.Value != "leased" || recovered.ID != active.Token.ID {
		t.Fatalf("restored recovered Lease() = %#v/%v, want leased item", recovered, ok)
	}
	pending, ok = restored.Lease(now.Add(time.Second))
	if !ok || pending.Value != "pending" {
		t.Fatalf("restored retried pending Lease() = %#v/%v, want pending", pending, ok)
	}
	if _, ok := restored.Lease(now.Add(time.Second)); ok {
		t.Fatal("restored queue exposed delayed item before its ready time")
	}
}

func TestPriorityVisibilityQueueBinarySaveLoadAndCorruption(t *testing.T) {
	now := time.Unix(400, 0).UTC()
	queue := NewPriorityVisibilityQueueWithEpoch[string](0, time.Second, 7)
	if !queue.Enqueue(2, "pending") || !queue.Enqueue(1, "leased") {
		t.Fatal("Enqueue returned false")
	}
	active, ok := queue.LeaseWithToken(now)
	if !ok {
		t.Fatal("LeaseWithToken returned no item")
	}
	path := filepath.Join(t.TempDir(), "queue.hpq")
	codec := priorityVisibilityQueueStringCodec()
	if err := SavePriorityVisibilityQueue(path, queue, codec); err != nil {
		t.Fatalf("SavePriorityVisibilityQueue() error = %v", err)
	}
	loaded, err := LoadPriorityVisibilityQueue(path, codec)
	if err != nil {
		t.Fatalf("LoadPriorityVisibilityQueue() error = %v", err)
	}
	if loaded.Epoch() != 8 {
		t.Fatalf("loaded Epoch() = %d, want restart epoch 8", loaded.Epoch())
	}
	if loaded.AckToken(active.Token) {
		t.Fatal("loaded queue accepted old epoch token")
	}
	first, ok := loaded.Lease(now)
	if !ok || first.Value != "pending" || first.Priority != 2 {
		t.Fatalf("first loaded Lease() = %#v/%v, want pending priority 2", first, ok)
	}
	if got := loaded.RequeueExpired(now.Add(2 * time.Second)); got != 2 {
		t.Fatalf("loaded RequeueExpired() = %d, want 2", got)
	}
	second, ok := loaded.Lease(now.Add(2 * time.Second))
	if !ok || second.Value != "leased" || second.Priority != 1 {
		t.Fatalf("second loaded Lease() = %#v/%v, want leased priority 1", second, ok)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	raw[len(raw)-1] ^= 1
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	if _, err := LoadPriorityVisibilityQueue(path, codec); err == nil {
		t.Fatal("LoadPriorityVisibilityQueue accepted a corrupted checksum")
	}
}

func TestPriorityVisibilityQueueRejectsInvalidSnapshot(t *testing.T) {
	queue := NewPriorityVisibilityQueue[int](0, time.Second)
	if !queue.Enqueue(1, 1) {
		t.Fatal("Enqueue returned false")
	}
	snapshot := queue.Snapshot()
	snapshot.Pending[0].ID = 0
	if _, err := RestorePriorityVisibilityQueue(snapshot, 2); err == nil {
		t.Fatal("RestorePriorityVisibilityQueue accepted zero pending ID")
	}
}

func TestPriorityVisibilityQueueBinaryRejectsTruncatedAndOversizedRecords(t *testing.T) {
	queue := NewPriorityVisibilityQueue[string](0, time.Second)
	if !queue.Enqueue(1, "value") {
		t.Fatal("Enqueue returned false")
	}
	codec := priorityVisibilityQueueStringCodec()
	payload, err := queue.MarshalSnapshot(codec)
	if err != nil {
		t.Fatalf("MarshalSnapshot() error = %v", err)
	}
	for _, length := range []int{0, priorityVisibilityQueueHeaderSize + priorityVisibilityQueueChecksumSize - 1, len(payload) - 1} {
		if _, err := UnmarshalPriorityVisibilityQueue(payload[:length], codec); err == nil {
			t.Fatalf("UnmarshalPriorityVisibilityQueue(length %d) error = nil", length)
		}
	}

	malformed := append([]byte(nil), payload...)
	binary.LittleEndian.PutUint32(malformed[44:48], 2)
	refreshPriorityVisibilityQueueChecksum(malformed)
	if _, err := UnmarshalPriorityVisibilityQueue(malformed, codec); err == nil {
		t.Fatal("UnmarshalPriorityVisibilityQueue accepted a truncated record count")
	}

	malformed = append([]byte(nil), payload...)
	binary.LittleEndian.PutUint32(malformed[priorityVisibilityQueueHeaderSize+37:], ^uint32(0))
	refreshPriorityVisibilityQueueChecksum(malformed)
	if _, err := UnmarshalPriorityVisibilityQueue(malformed, codec); err == nil {
		t.Fatal("UnmarshalPriorityVisibilityQueue accepted an oversized value length")
	}
}

func refreshPriorityVisibilityQueueChecksum(payload []byte) {
	binary.LittleEndian.PutUint32(payload[len(payload)-priorityVisibilityQueueChecksumSize:], crc32.Checksum(payload[:len(payload)-priorityVisibilityQueueChecksumSize], crc32.IEEETable))
}

func BenchmarkPriorityVisibilityQueueLeaseAck(b *testing.B) {
	queue := NewPriorityVisibilityQueue[string](1, time.Minute)
	now := time.Unix(500, 0).UTC()
	value := "value"
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if !queue.Enqueue(int64(index&7), value) {
			b.Fatal("Enqueue returned false")
		}
		lease, ok := queue.LeaseWithToken(now)
		if !ok || !queue.AckToken(lease.Token) {
			b.Fatal("lease/ack failed")
		}
	}
}

func BenchmarkVisibilityQueueLeaseAckBaseline(b *testing.B) {
	queue := NewVisibilityQueue[string](1, time.Minute)
	now := time.Unix(500, 0).UTC()
	value := "value"
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if !queue.Enqueue(value) {
			b.Fatal("Enqueue returned false")
		}
		item, ok := queue.Lease(now)
		if !ok || !queue.Ack(item.ID) {
			b.Fatal("lease/ack failed")
		}
	}
}

func BenchmarkPriorityVisibilityQueueMarshal(b *testing.B) {
	queue := NewPriorityVisibilityQueue[string](0, time.Minute)
	for index := 0; index < 1024; index++ {
		queue.Enqueue(int64(index&31), "queue-value")
	}
	codec := priorityVisibilityQueueStringCodec()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		payload, err := queue.MarshalSnapshot(codec)
		if err != nil {
			b.Fatal(err)
		}
		b.SetBytes(int64(len(payload)))
		b.ReportMetric(float64(len(payload)), "snapshot-bytes")
	}
}

func BenchmarkPriorityVisibilityQueueJSONMarshal(b *testing.B) {
	queue := NewPriorityVisibilityQueue[string](0, time.Minute)
	for index := 0; index < 1024; index++ {
		queue.Enqueue(int64(index&31), "queue-value")
	}
	snapshot := queue.Snapshot()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		payload, err := json.Marshal(snapshot)
		if err != nil {
			b.Fatal(err)
		}
		b.SetBytes(int64(len(payload)))
		b.ReportMetric(float64(len(payload)), "snapshot-bytes")
	}
}

func BenchmarkPriorityVisibilityQueueBinaryValue(b *testing.B) {
	codec := PriorityVisibilityQueueCodec[uint64]{
		Encode: func(value uint64) ([]byte, error) {
			payload := make([]byte, 8)
			binary.LittleEndian.PutUint64(payload, value)
			return payload, nil
		},
		Decode: func(payload []byte) (uint64, error) {
			return binary.LittleEndian.Uint64(payload), nil
		},
	}
	queue := NewPriorityVisibilityQueue[uint64](0, time.Minute)
	for index := uint64(0); index < 1024; index++ {
		queue.Enqueue(int64(index&31), index)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		payload, err := queue.MarshalSnapshot(codec)
		if err != nil {
			b.Fatal(err)
		}
		b.SetBytes(int64(len(payload)))
	}
}
