package hatDataStructure

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"testing"
	"time"
)

func TestT246PriorityVisibilityQueueBoundsStarvation(t *testing.T) {
	now := time.Unix(600, 0).UTC()
	queue := NewPriorityVisibilityQueue[string](0, time.Minute)
	if queue.StarvationBound() != 0 {
		t.Fatalf("default StarvationBound() = %d, want disabled", queue.StarvationBound())
	}
	queue.SetStarvationBound(2)
	if queue.StarvationBound() != 2 {
		t.Fatalf("StarvationBound() = %d, want 2", queue.StarvationBound())
	}
	if !queue.Enqueue(100, "low") {
		t.Fatal("low-priority Enqueue returned false")
	}

	for index := 0; index < 3; index++ {
		if !queue.Enqueue(1, fmt.Sprintf("high-%d", index)) {
			t.Fatal("high-priority Enqueue returned false")
		}
		item, ok := queue.Lease(now)
		if !ok {
			t.Fatalf("Lease(%d) returned no item", index)
		}
		want := fmt.Sprintf("high-%d", index)
		if index == 2 {
			want = "low"
		}
		if item.Value != want {
			t.Fatalf("Lease(%d) = %#v, want %q", index, item, want)
		}
		if !queue.Ack(item.ID) {
			t.Fatalf("Ack(%d) rejected lease", index)
		}
	}
}

func TestT246PriorityVisibilityQueuePersistsFairnessState(t *testing.T) {
	now := time.Unix(601, 0).UTC()
	queue := NewPriorityVisibilityQueueWithEpoch[string](0, time.Minute, 11)
	queue.SetStarvationBound(1)
	if !queue.Enqueue(100, "low") || !queue.Enqueue(1, "high-1") {
		t.Fatal("initial Enqueue returned false")
	}
	first, ok := queue.Lease(now)
	if !ok || first.Value != "high-1" || !queue.Ack(first.ID) {
		t.Fatalf("initial Lease/Ack = %#v/%v, want high-1", first, ok)
	}
	if !queue.Enqueue(1, "high-2") {
		t.Fatal("second high-priority Enqueue returned false")
	}

	snapshot := queue.Snapshot()
	if snapshot.StarvationBound != 1 || snapshot.StarvationBurst != 1 {
		t.Fatalf("snapshot fairness state = %d/%d, want 1/1", snapshot.StarvationBound, snapshot.StarvationBurst)
	}
	restored, err := RestorePriorityVisibilityQueue(snapshot, 12)
	if err != nil {
		t.Fatalf("RestorePriorityVisibilityQueue() error = %v", err)
	}
	if restored.StarvationBound() != 1 {
		t.Fatalf("restored StarvationBound() = %d, want 1", restored.StarvationBound())
	}
	item, ok := restored.Lease(now)
	if !ok || item.Value != "low" {
		t.Fatalf("restored Lease() = %#v/%v, want low after persisted burst", item, ok)
	}
}

func TestT246PriorityVisibilityQueueBinaryFairnessAndV1Compatibility(t *testing.T) {
	queue := NewPriorityVisibilityQueue[string](0, time.Minute)
	queue.SetStarvationBound(2)
	if !queue.Enqueue(100, "low") || !queue.Enqueue(1, "high") {
		t.Fatal("Enqueue returned false")
	}
	codec := priorityVisibilityQueueStringCodec()
	payload, err := queue.MarshalSnapshot(codec)
	if err != nil {
		t.Fatalf("MarshalSnapshot() error = %v", err)
	}
	loaded, err := UnmarshalPriorityVisibilityQueue(payload, codec)
	if err != nil || loaded.StarvationBound() != 2 {
		t.Fatalf("v2 Unmarshal = %v/%d, want bound 2", err, loaded.StarvationBound())
	}

	legacy := t246PriorityVisibilityQueueV1Payload(payload)
	legacyLoaded, err := UnmarshalPriorityVisibilityQueue(legacy, codec)
	if err != nil {
		t.Fatalf("v1 Unmarshal() error = %v", err)
	}
	if legacyLoaded.StarvationBound() != 0 {
		t.Fatalf("v1 StarvationBound() = %d, want strict-priority default", legacyLoaded.StarvationBound())
	}
}

func t246PriorityVisibilityQueueV1Payload(v2 []byte) []byte {
	legacy := make([]byte, priorityVisibilityQueueLegacyHeaderSize)
	copy(legacy, v2[:priorityVisibilityQueueLegacyHeaderSize-8])
	legacy[4] = priorityVisibilityQueueLegacyVersion
	binary.LittleEndian.PutUint32(legacy[44:48], binary.LittleEndian.Uint32(v2[52:56]))
	binary.LittleEndian.PutUint32(legacy[48:52], binary.LittleEndian.Uint32(v2[56:60]))
	legacy = append(legacy, v2[priorityVisibilityQueueHeaderSize:len(v2)-priorityVisibilityQueueChecksumSize]...)
	checksum := crc32.Checksum(legacy, crc32.IEEETable)
	var checksumBytes [priorityVisibilityQueueChecksumSize]byte
	binary.LittleEndian.PutUint32(checksumBytes[:], checksum)
	return append(legacy, checksumBytes[:]...)
}
