package hatReplication

import (
	"errors"
	"reflect"
	"sync"
	"testing"
)

func TestChangefeedExactlyOnceRestartAndDuplicateDecisions(t *testing.T) {
	consumer, err := NewChangefeedExactlyOnceConsumer("orders")
	if err != nil {
		t.Fatal(err)
	}
	if got, err := consumer.RestartOffset(); err != nil || got != 0 {
		t.Fatalf("new consumer should restart at offset zero: offset=%d err=%v", got, err)
	}
	decision, err := consumer.BeginBatch("batch-1", 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if decision.Action != ChangefeedExactlyOnceApply {
		t.Fatalf("first batch should apply, got %+v", decision)
	}
	if got, err := consumer.RestartOffset(); err != nil || got != 0 {
		t.Fatalf("uncommitted batch must replay from committed offset: offset=%d err=%v", got, err)
	}
	if _, err := consumer.CommitBatch("batch-1", 10); err != nil {
		t.Fatal(err)
	}
	if got, err := consumer.RestartOffset(); err != nil || got != 11 {
		t.Fatalf("restart offset should follow committed batch: offset=%d err=%v", got, err)
	}
	decision, err = consumer.BeginBatch("batch-1", 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if decision.Action != ChangefeedExactlyOnceSkip {
		t.Fatalf("committed retry should skip, got %+v", decision)
	}
	decision, err = consumer.BeginBatch("batch-2", 11, 20)
	if err != nil || decision.Action != ChangefeedExactlyOnceApply {
		t.Fatalf("next batch should apply: decision=%+v err=%v", decision, err)
	}
	if _, err := consumer.CommitBatch("batch-2", 20); err != nil {
		t.Fatal(err)
	}
}

func TestChangefeedExactlyOncePendingResumeGapAndAbort(t *testing.T) {
	consumer, err := NewChangefeedExactlyOnceConsumer("orders")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := consumer.BeginBatch("batch-1", 0, 10); err != nil {
		t.Fatal(err)
	}
	resumed, err := consumer.BeginBatch("batch-1", 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if resumed.Action != ChangefeedExactlyOnceResume {
		t.Fatalf("same pending batch should resume, got %+v", resumed)
	}
	if _, err := consumer.BeginBatch("other", 0, 10); !errors.Is(err, ErrChangefeedExactlyOncePending) {
		t.Fatalf("different pending batch should be rejected, got %v", err)
	}
	if _, err := consumer.AbortBatch("batch-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := consumer.BeginBatch("batch-2", 1, 2); !errors.Is(err, ErrChangefeedExactlyOnceGap) {
		t.Fatalf("offset gap should be rejected, got %v", err)
	}
	if _, err := consumer.BeginBatch("batch-2", 0, 10); err != nil {
		t.Fatal(err)
	}
	if _, err := consumer.CommitBatch("batch-2", 10); err != nil {
		t.Fatal(err)
	}
	if _, err := consumer.BeginBatch("batch-3", 5, 11); !errors.Is(err, ErrChangefeedExactlyOnceOverlap) {
		t.Fatalf("overlapping batch should be rejected, got %v", err)
	}
}

func TestChangefeedExactlyOnceCommitIsAtomicAndIdempotent(t *testing.T) {
	consumer, err := NewChangefeedExactlyOnceConsumer("orders")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := consumer.BeginBatch("batch-1", 0, 10); err != nil {
		t.Fatal(err)
	}
	if _, err := consumer.CommitBatch("batch-1", 100); err != nil {
		t.Fatal(err)
	}
	want := consumer.Snapshot()
	got, err := consumer.CommitBatch("batch-1", 1)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("duplicate commit changed state: want=%+v got=%+v", want, got)
	}
	if _, err := consumer.BeginBatch("batch-2", 11, 20); err != nil {
		t.Fatal(err)
	}
	if _, err := consumer.CommitBatch("batch-2", 99); !errors.Is(err, ErrChangefeedExactlyOnceFrontierRegressed) {
		t.Fatalf("frontier regression should not commit offset, got %v", err)
	}
	if got := consumer.Snapshot(); got.CommittedOffset != 10 || got.Pending == nil {
		t.Fatalf("failed commit should leave pending batch and old offset: %+v", got)
	}
}

func TestChangefeedExactlyOnceSnapshotRoundTripAndMalformedData(t *testing.T) {
	consumer, err := NewChangefeedExactlyOnceConsumer("orders")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := consumer.BeginBatch("batch-1", 0, 10); err != nil {
		t.Fatal(err)
	}
	want := consumer.Snapshot()
	encoded, err := consumer.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := UnmarshalChangefeedExactlyOnceSnapshot(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, want) {
		t.Fatalf("pending state changed across binary round trip: want=%+v got=%+v", want, decoded)
	}
	restored, err := NewChangefeedExactlyOnceConsumerFromSnapshot(decoded)
	if err != nil {
		t.Fatal(err)
	}
	resumed, err := restored.BeginBatch("batch-1", 0, 10)
	if err != nil || resumed.Action != ChangefeedExactlyOnceResume {
		t.Fatalf("restored pending batch should resume: decision=%+v err=%v", resumed, err)
	}
	for _, malformed := range [][]byte{
		encoded[:len(encoded)-1],
		append(append([]byte(nil), encoded...), 0),
		[]byte("invalid"),
	} {
		if _, err := UnmarshalChangefeedExactlyOnceSnapshot(malformed); !errors.Is(err, ErrChangefeedExactlyOnceSnapshotInvalid) {
			t.Errorf("expected malformed snapshot error, got %v", err)
		}
	}
	invalid := want
	invalidPending := *want.Pending
	invalid.Pending = &invalidPending
	invalid.Pending.StartOffset = 1
	if err := consumer.Restore(invalid); !errors.Is(err, ErrChangefeedExactlyOnceSnapshotInvalid) {
		t.Fatalf("invalid restore should be rejected, got %v", err)
	}
	if got := consumer.Snapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("invalid restore changed state: want=%+v got=%+v", want, got)
	}
}

func TestChangefeedExactlyOnceConcurrentBeginHasOnePendingBatch(t *testing.T) {
	consumer, err := NewChangefeedExactlyOnceConsumer("orders")
	if err != nil {
		t.Fatal(err)
	}
	const contenders = 32
	var wg sync.WaitGroup
	wg.Add(contenders)
	winners := make(chan ChangefeedExactlyOnceDecision, contenders)
	for index := 0; index < contenders; index++ {
		go func(index int) {
			defer wg.Done()
			decision, err := consumer.BeginBatch("batch-"+string(rune('a'+index)), 0, 10)
			if err == nil {
				winners <- decision
			} else if !errors.Is(err, ErrChangefeedExactlyOncePending) {
				t.Errorf("unexpected concurrent begin error: %v", err)
			}
		}(index)
	}
	wg.Wait()
	close(winners)
	count := 0
	for range winners {
		count++
	}
	if count != 1 {
		t.Fatalf("expected one pending batch, got %d", count)
	}
}
