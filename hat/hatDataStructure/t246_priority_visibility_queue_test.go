package hatDataStructure

import (
	"encoding/binary"
	"hash/crc32"
	"testing"
	"time"
)

func TestT246PriorityVisibilityQueueBoundsStarvation(t *testing.T) {
	queue := NewPriorityVisibilityQueueWithOptions[string](PriorityVisibilityQueueOptions{
		StarvationAfter:   2,
		VisibilityTimeout: time.Minute,
	})
	if !queue.Enqueue(100, "old") {
		t.Fatal("enqueue old item failed")
	}
	for index := 0; index < 8; index++ {
		if !queue.Enqueue(1, "urgent") {
			t.Fatalf("enqueue urgent item %d failed", index)
		}
	}

	now := time.Unix(100, 0).UTC()
	for index := 0; index < 2; index++ {
		item, ok := queue.Lease(now)
		if !ok || item.Value != "urgent" {
			t.Fatalf("lease %d = %#v/%v, want urgent item before starvation bound", index, item, ok)
		}
		if !queue.Ack(item.ID) {
			t.Fatalf("ack urgent item %d failed", index)
		}
	}

	item, ok := queue.Lease(now)
	if !ok || item.Value != "old" {
		t.Fatalf("bounded lease = %#v/%v, want old item after two bypasses", item, ok)
	}
}

func TestT246PriorityVisibilityQueueSnapshotPreservesStarvationPolicy(t *testing.T) {
	queue := NewPriorityVisibilityQueueWithOptions[string](PriorityVisibilityQueueOptions{
		Capacity:          16,
		VisibilityTimeout: time.Minute,
		Epoch:             7,
		StarvationAfter:   3,
	})
	if !queue.Enqueue(10, "value") {
		t.Fatal("enqueue failed")
	}
	snapshot := queue.Snapshot()
	if snapshot.StarvationAfter != 3 {
		t.Fatalf("snapshot starvation bound = %d, want 3", snapshot.StarvationAfter)
	}
	restored, err := RestorePriorityVisibilityQueue(snapshot, 8)
	if err != nil {
		t.Fatalf("RestorePriorityVisibilityQueue() error = %v", err)
	}
	if got := restored.Snapshot().StarvationAfter; got != 3 {
		t.Fatalf("restored starvation bound = %d, want 3", got)
	}
}

func TestT246PriorityVisibilityQueueBinarySnapshotPreservesStarvationPolicy(t *testing.T) {
	queue := NewPriorityVisibilityQueueWithOptions[string](PriorityVisibilityQueueOptions{
		VisibilityTimeout: time.Minute,
		StarvationAfter:   4,
	})
	if !queue.Enqueue(10, "value") {
		t.Fatal("enqueue failed")
	}
	codec := PriorityVisibilityQueueCodec[string]{
		Encode: func(value string) ([]byte, error) { return []byte(value), nil },
		Decode: func(value []byte) (string, error) { return string(value), nil },
	}
	payload, err := queue.MarshalSnapshot(codec)
	if err != nil {
		t.Fatalf("MarshalSnapshot() error = %v", err)
	}
	restored, err := UnmarshalPriorityVisibilityQueue(payload, codec)
	if err != nil {
		t.Fatalf("UnmarshalPriorityVisibilityQueue() error = %v", err)
	}
	if got := restored.Snapshot().StarvationAfter; got != 4 {
		t.Fatalf("binary restored starvation bound = %d, want 4", got)
	}
}

func TestT246PriorityVisibilityQueueDecodesLegacyV1Snapshot(t *testing.T) {
	payload := make([]byte, priorityVisibilityQueueLegacyHeaderSize+priorityVisibilityQueueChecksumSize)
	copy(payload[:4], priorityVisibilityQueueMagic[:])
	payload[4] = priorityVisibilityQueueLegacyFormatVersion
	binary.LittleEndian.PutUint64(payload[12:20], uint64(int64(time.Minute)))
	binary.LittleEndian.PutUint64(payload[20:28], 1)
	checksum := crc32.Checksum(payload[:priorityVisibilityQueueLegacyHeaderSize], crc32.IEEETable)
	binary.LittleEndian.PutUint32(payload[priorityVisibilityQueueLegacyHeaderSize:], checksum)
	codec := PriorityVisibilityQueueCodec[string]{
		Decode: func(value []byte) (string, error) { return string(value), nil },
	}
	queue, err := UnmarshalPriorityVisibilityQueue(payload, codec)
	if err != nil {
		t.Fatalf("UnmarshalPriorityVisibilityQueue() legacy error = %v", err)
	}
	if got := queue.Snapshot().StarvationAfter; got != 0 {
		t.Fatalf("legacy starvation bound = %d, want disabled", got)
	}
}
