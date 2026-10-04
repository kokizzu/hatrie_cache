package hatSort

import (
	"bytes"
	"context"
	"errors"
	"os"
	"testing"
)

func TestCHG02ExternalSortStableMergeAndCleanup(t *testing.T) {
	spillDir := t.TempDir()
	records := []Record{
		{Key: []byte("c"), Value: []byte("third")},
		{Key: []byte("b"), Value: []byte("first-b")},
		{Key: []byte("b"), Value: []byte("second-b")},
		{Key: []byte("a"), Value: []byte("first-a")},
	}
	got, stats, err := ExternalSort(context.Background(), records, Options{
		SpillDirectory: spillDir,
		MaxMemoryBytes: 32,
		MaxSpillBytes:  1 << 20,
	})
	if err != nil {
		t.Fatalf("ExternalSort() error = %v", err)
	}
	want := []Record{
		{Key: []byte("a"), Value: []byte("first-a")},
		{Key: []byte("b"), Value: []byte("first-b")},
		{Key: []byte("b"), Value: []byte("second-b")},
		{Key: []byte("c"), Value: []byte("third")},
	}
	if !recordsEqual(got, want) {
		t.Fatalf("sorted records = %#v, want %#v", got, want)
	}
	if stats.Runs < 2 || stats.SpilledBytes == 0 {
		t.Fatalf("stats = %#v, want multiple spilled runs", stats)
	}
	entries, err := os.ReadDir(spillDir)
	if err != nil {
		t.Fatalf("ReadDir() error = %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("spill directory after sort = %#v, want empty", entries)
	}
}

func TestCHG02ExternalSortRejectsSpillBudgetAndCleansUp(t *testing.T) {
	spillDir := t.TempDir()
	_, _, err := ExternalSort(context.Background(), []Record{
		{Key: []byte("a"), Value: bytes.Repeat([]byte{'x'}, 64)},
		{Key: []byte("b"), Value: bytes.Repeat([]byte{'y'}, 64)},
	}, Options{SpillDirectory: spillDir, MaxMemoryBytes: 128, MaxSpillBytes: 1})
	if !errors.Is(err, ErrSpillBudgetExceeded) {
		t.Fatalf("ExternalSort() error = %v, want ErrSpillBudgetExceeded", err)
	}
	entries, readErr := os.ReadDir(spillDir)
	if readErr != nil {
		t.Fatalf("ReadDir() error = %v", readErr)
	}
	if len(entries) != 0 {
		t.Fatalf("spill directory after failure = %#v, want empty", entries)
	}
}

func TestCHG02ExternalSortEmitterFailureCleansUp(t *testing.T) {
	spillDir := t.TempDir()
	wantErr := errors.New("consumer stopped")
	_, err := ExternalSortInto(context.Background(), []Record{
		{Key: []byte("d"), Value: []byte("4")},
		{Key: []byte("c"), Value: []byte("3")},
		{Key: []byte("b"), Value: []byte("2")},
		{Key: []byte("a"), Value: []byte("1")},
	}, Options{SpillDirectory: spillDir, MaxMemoryBytes: 16, MaxSpillBytes: 1 << 20}, func(Record) error {
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("ExternalSortInto() error = %v, want consumer error", err)
	}
	entries, readErr := os.ReadDir(spillDir)
	if readErr != nil {
		t.Fatalf("ReadDir() error = %v", readErr)
	}
	if len(entries) != 0 {
		t.Fatalf("spill directory after emitter failure = %#v, want empty", entries)
	}
}

func TestCHG02ExternalSortHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err := ExternalSort(ctx, []Record{{Key: []byte("a")}}, Options{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("ExternalSort() error = %v, want context.Canceled", err)
	}
}

func TestCHG02ExternalSortUsesBoundedMergePasses(t *testing.T) {
	spillDir := t.TempDir()
	records := make([]Record, 12)
	for index := range records {
		records[index] = Record{
			Key:   []byte{byte('z' - index)},
			Value: []byte{byte(index)},
		}
	}
	got, stats, err := ExternalSort(context.Background(), records, Options{
		SpillDirectory: spillDir,
		MaxMemoryBytes: 16,
		MaxSpillBytes:  1 << 20,
		MaxMergeRuns:   2,
	})
	if err != nil {
		t.Fatalf("ExternalSort() error = %v", err)
	}
	if stats.MergePasses == 0 {
		t.Fatalf("stats = %#v, want a bounded merge pass", stats)
	}
	for index := 1; index < len(got); index++ {
		if bytes.Compare(got[index-1].Key, got[index].Key) > 0 {
			t.Fatalf("records are not ordered at %d: %#v then %#v", index, got[index-1], got[index])
		}
	}
	entries, err := os.ReadDir(spillDir)
	if err != nil {
		t.Fatalf("ReadDir() error = %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("spill directory after merge = %#v, want empty", entries)
	}
}

func TestCHG02ExternalSortIntoStreamsOutput(t *testing.T) {
	var got []Record
	stats, err := ExternalSortInto(context.Background(), []Record{
		{Key: []byte("c"), Value: []byte("3")},
		{Key: []byte("a"), Value: []byte("1")},
		{Key: []byte("b"), Value: []byte("2")},
	}, Options{MaxMemoryBytes: 32}, func(record Record) error {
		got = append(got, cloneRecord(record))
		return nil
	})
	if err != nil {
		t.Fatalf("ExternalSortInto() error = %v", err)
	}
	if stats.OutputRecords != len(got) || !recordsEqual(got, []Record{
		{Key: []byte("a"), Value: []byte("1")},
		{Key: []byte("b"), Value: []byte("2")},
		{Key: []byte("c"), Value: []byte("3")},
	}) {
		t.Fatalf("streamed records/stats = %#v/%#v", got, stats)
	}
}

func TestCHG02ExternalSortOwnsReturnedBytes(t *testing.T) {
	input := []Record{
		{Key: []byte("b"), Value: []byte("input-b")},
		{Key: []byte("a"), Value: []byte("input-a")},
	}
	got, _, err := ExternalSort(context.Background(), input, Options{})
	if err != nil {
		t.Fatalf("ExternalSort() error = %v", err)
	}
	input[0].Key[0] = 'z'
	input[0].Value[0] = 'z'
	if !recordsEqual(got, []Record{
		{Key: []byte("a"), Value: []byte("input-a")},
		{Key: []byte("b"), Value: []byte("input-b")},
	}) {
		t.Fatalf("returned records changed after input mutation = %#v", got)
	}
	got[0].Value[0] = 'x'
	if string(input[1].Value) != "input-a" {
		t.Fatalf("returned records alias input: input = %#v", input)
	}
}

func TestCHG02ExternalSortValidatesBounds(t *testing.T) {
	if _, _, err := ExternalSort(context.Background(), nil, Options{MaxMemoryBytes: -1}); !errors.Is(err, ErrInvalidOptions) {
		t.Fatalf("negative memory error = %v, want ErrInvalidOptions", err)
	}
	if _, _, err := ExternalSort(context.Background(), []Record{{Key: []byte("too-large")}}, Options{MaxMemoryBytes: 16}); !errors.Is(err, ErrRecordTooLarge) {
		t.Fatalf("oversized record error = %v, want ErrRecordTooLarge", err)
	}
}

func recordsEqual(left, right []Record) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if !bytes.Equal(left[index].Key, right[index].Key) || !bytes.Equal(left[index].Value, right[index].Value) {
			return false
		}
	}
	return true
}
