package hatReplication

import (
	"errors"
	"testing"
	"time"
)

func TestSinkRetryQueueDeduplicatesAndCompactsPendingUpserts(t *testing.T) {
	now := time.Unix(100, 0)
	options := SinkRetryOptions{
		Source:     "orders",
		MaxPending: 4,
		MaxBytes:   1 << 20,
		BaseDelay:  10 * time.Millisecond,
		MaxDelay:   40 * time.Millisecond,
	}
	queue, err := NewSinkRetryQueue(options)
	if err != nil {
		t.Fatal(err)
	}

	first := retryTestRecord(1, "order-1", "pending")
	result, err := queue.Enqueue(first, now)
	if err != nil {
		t.Fatal(err)
	}
	if result.Action != SinkRetryEnqueued {
		t.Fatalf("first enqueue action = %v", result.Action)
	}

	result, err = queue.Enqueue(first, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if result.Action != SinkRetryDuplicate {
		t.Fatalf("duplicate enqueue action = %v", result.Action)
	}

	newer := retryTestRecord(2, "order-1", "latest")
	result, err = queue.Enqueue(newer, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if result.Action != SinkRetryReplaced {
		t.Fatalf("replacement enqueue action = %v", result.Action)
	}
	if stats := queue.Stats(); stats.Pending != 1 || stats.InFlight != 0 {
		t.Fatalf("after replacement stats = %+v", stats)
	}
	if stats := queue.Stats(); stats.Bytes != sinkRetryRecordSize(newer) {
		t.Fatalf("replacement bytes = %d, want %d", stats.Bytes, sinkRetryRecordSize(newer))
	}

	delivery, ok, err := queue.Next(now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if !ok || delivery.Attempt != 1 || delivery.Record.Sequence != newer.Sequence || string(delivery.Record.Value) != "latest" {
		t.Fatalf("delivery = %+v, ok = %v", delivery, ok)
	}
	if err := queue.Ack(delivery.Record.OutputID, delivery.Record.Sequence); err != nil {
		t.Fatal(err)
	}
	if stats := queue.Stats(); stats.Pending != 0 || stats.Bytes != 0 || stats.InFlight != 0 {
		t.Fatalf("after ack stats = %+v", stats)
	}
}

func TestSinkRetryQueueBackoffIsDeterministicAndCapped(t *testing.T) {
	now := time.Unix(200, 0)
	queue, err := NewSinkRetryQueue(SinkRetryOptions{
		Source:    "orders",
		BaseDelay: 10 * time.Millisecond,
		MaxDelay:  25 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	record := retryTestRecord(1, "order-1", "value")
	if _, err := queue.Enqueue(record, now); err != nil {
		t.Fatal(err)
	}

	delivery, ok, err := queue.Next(now)
	if err != nil || !ok {
		t.Fatalf("first next: delivery=%+v ok=%v err=%v", delivery, ok, err)
	}
	if delivery.Attempt != 1 {
		t.Fatalf("first attempt = %d", delivery.Attempt)
	}
	if err := queue.Retry(record.OutputID, record.Sequence, now); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := queue.Next(now.Add(9 * time.Millisecond)); err != nil || ok {
		t.Fatalf("early retry: ok=%v err=%v", ok, err)
	}

	delivery, ok, err = queue.Next(now.Add(10 * time.Millisecond))
	if err != nil || !ok || delivery.Attempt != 2 {
		t.Fatalf("second next: delivery=%+v ok=%v err=%v", delivery, ok, err)
	}
	secondAttemptAt := now.Add(10 * time.Millisecond)
	if err := queue.Retry(record.OutputID, record.Sequence, secondAttemptAt); err != nil {
		t.Fatal(err)
	}

	thirdAttemptAt := secondAttemptAt.Add(20 * time.Millisecond)
	delivery, ok, err = queue.Next(thirdAttemptAt)
	if err != nil || !ok || delivery.Attempt != 3 {
		t.Fatalf("third next: delivery=%+v ok=%v err=%v", delivery, ok, err)
	}
	if err := queue.Retry(record.OutputID, record.Sequence, thirdAttemptAt); err != nil {
		t.Fatal(err)
	}

	if _, ok, err := queue.Next(thirdAttemptAt.Add(24 * time.Millisecond)); err != nil || ok {
		t.Fatalf("uncapped retry: ok=%v err=%v", ok, err)
	}
	if delivery, ok, err = queue.Next(thirdAttemptAt.Add(25 * time.Millisecond)); err != nil || !ok || delivery.Attempt != 4 {
		t.Fatalf("capped retry: delivery=%+v ok=%v err=%v", delivery, ok, err)
	}
}

func TestSinkRetryQueueRejectsUnsafeConflictsAndBounds(t *testing.T) {
	now := time.Unix(300, 0)
	queue, err := NewSinkRetryQueue(SinkRetryOptions{
		Source:     "orders",
		MaxPending: 1,
		MaxBytes:   1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	first := retryTestRecord(1, "order-1", "first")
	if _, err := queue.Enqueue(first, now); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.Enqueue(retryTestRecord(2, "order-2", "second"), now); !errors.Is(err, ErrSinkRetryQueueFull) {
		t.Fatalf("full queue error = %v", err)
	}

	delivery, ok, err := queue.Next(now)
	if err != nil || !ok {
		t.Fatalf("next: delivery=%+v ok=%v err=%v", delivery, ok, err)
	}
	if _, err := queue.Enqueue(retryTestRecord(2, "order-1", "newer"), now); !errors.Is(err, ErrSinkRetryInFlight) {
		t.Fatalf("in-flight replacement error = %v", err)
	}
	if err := queue.Ack("order-1", 99); !errors.Is(err, ErrSinkRetryConflict) {
		t.Fatalf("ack conflict error = %v", err)
	}
	if err := queue.Retry("order-1", 99, now); !errors.Is(err, ErrSinkRetryConflict) {
		t.Fatalf("retry conflict error = %v", err)
	}
	if err := queue.Ack(delivery.Record.OutputID, delivery.Record.Sequence); err != nil {
		t.Fatal(err)
	}
}

func TestSinkRetryQueueBinarySnapshotRoundTripRestoresPendingAndInFlight(t *testing.T) {
	now := time.Unix(400, 0)
	options := SinkRetryOptions{
		Source:    "orders",
		BaseDelay: 10 * time.Millisecond,
		MaxDelay:  1 * time.Second,
	}
	queue, err := NewSinkRetryQueue(options)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := queue.Enqueue(retryTestRecord(1, "order-1", "one"), now); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.Enqueue(retryTestRecord(2, "order-2", "two"), now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := queue.Next(now); err != nil || !ok {
		t.Fatalf("next: ok=%v err=%v", ok, err)
	}

	encoded, err := queue.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	restored, err := NewSinkRetryQueueFromBinary(options, encoded)
	if err != nil {
		t.Fatal(err)
	}
	if stats := restored.Stats(); stats.Pending != 2 || stats.InFlight != 0 {
		t.Fatalf("restored stats = %+v", stats)
	}

	first, ok, err := restored.Next(now)
	if err != nil || !ok || first.Record.OutputID != "order-1" || first.Attempt != 2 {
		t.Fatalf("restored in-flight next: delivery=%+v ok=%v err=%v", first, ok, err)
	}
	if err := restored.Ack(first.Record.OutputID, first.Record.Sequence); err != nil {
		t.Fatal(err)
	}
	second, ok, err := restored.Next(now.Add(time.Second))
	if err != nil || !ok || second.Record.OutputID != "order-2" || second.Attempt != 1 {
		t.Fatalf("restored pending next: delivery=%+v ok=%v err=%v", second, ok, err)
	}
}

func TestSinkRetryQueueCopiesCallerOwnedRecordAndRejectsMalformedSnapshots(t *testing.T) {
	now := time.Unix(450, 0)
	queue, err := NewSinkRetryQueue(SinkRetryOptions{Source: "orders", MaxPending: 2, MaxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	record := retryTestRecord(1, "order-1", "before")
	if _, err := queue.Enqueue(record, now); err != nil {
		t.Fatal(err)
	}
	record.Value[0] = 'X'
	delivery, ok, err := queue.Next(now)
	if err != nil || !ok || string(delivery.Record.Value) != "before" {
		t.Fatalf("copied delivery = %+v ok=%v err=%v", delivery, ok, err)
	}

	malformed := [][]byte{
		[]byte("srt0"),
		[]byte("srt1\x01\x00"),
	}
	for index, data := range malformed {
		if _, err := UnmarshalSinkRetryQueueSnapshot(data); !errors.Is(err, ErrSinkRetrySnapshotInvalid) {
			t.Fatalf("malformed snapshot %d error = %v", index, err)
		}
	}

	snapshot := SinkRetryQueueSnapshot{
		Source: "orders",
		Items: []SinkRetrySnapshotItem{
			{Record: retryTestRecord(1, "order-1", "one")},
			{Record: retryTestRecord(2, "order-1", "two")},
		},
	}
	if _, err := snapshot.MarshalBinary(); !errors.Is(err, ErrSinkRetrySnapshotInvalid) {
		t.Fatalf("duplicate snapshot error = %v", err)
	}
}

func retryTestRecord(sequence uint64, outputID, value string) ExactlyOnceUpsertSinkRecord {
	return ExactlyOnceUpsertSinkRecord{
		Sequence: sequence,
		OutputID: outputID,
		Key:      []byte(outputID),
		Value:    []byte(value),
	}
}
