package hatDataStructure

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestT250DurableSequencePersistsCurrentBeforeReturning(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sequence.bin")
	sequence, err := NewDurableSequence(DurableSequenceOptions{Path: path, Initial: 41})
	if err != nil {
		t.Fatalf("NewDurableSequence() error = %v", err)
	}
	for want := uint64(42); want <= 43; want++ {
		got, err := sequence.Allocate()
		if err != nil {
			t.Fatalf("Allocate() error = %v", err)
		}
		if got != want {
			t.Fatalf("Allocate() = %d, want %d", got, want)
		}
	}
	if got := sequence.Current(); got != 43 {
		t.Fatalf("Current() = %d, want 43", got)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat() error = %v", err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("sequence file mode = %o, want 600", got)
	}

	reopened, err := NewDurableSequence(DurableSequenceOptions{Path: path, Initial: 999})
	if err != nil {
		t.Fatalf("reopen sequence error = %v", err)
	}
	got, err := reopened.Allocate()
	if err != nil {
		t.Fatalf("reopened Allocate() error = %v", err)
	}
	if got != 44 {
		t.Fatalf("reopened Allocate() = %d, want 44", got)
	}
}

func TestT250DurableSequenceBinaryRoundTripAndCorruption(t *testing.T) {
	sequence, err := NewDurableSequence(DurableSequenceOptions{Initial: 8})
	if err != nil {
		t.Fatalf("NewDurableSequence() error = %v", err)
	}
	if _, err := sequence.Allocate(); err != nil {
		t.Fatalf("Allocate() error = %v", err)
	}
	data, err := sequence.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary() error = %v", err)
	}
	restored, err := UnmarshalDurableSequence(data)
	if err != nil {
		t.Fatalf("UnmarshalDurableSequence() error = %v", err)
	}
	if got := restored.Current(); got != 9 {
		t.Fatalf("restored Current() = %d, want 9", got)
	}
	if got, err := restored.Allocate(); err != nil || got != 10 {
		t.Fatalf("restored Allocate() = %d, %v; want 10, nil", got, err)
	}
	data[len(data)-1] ^= 0x80
	if _, err := UnmarshalDurableSequence(data); !errors.Is(err, ErrDurableSequenceChecksum) {
		t.Fatalf("corrupt UnmarshalDurableSequence() error = %v, want checksum error", err)
	}
}

func TestT250DurableSequenceConcurrentAllocationIsUniqueAndContiguous(t *testing.T) {
	sequence, err := NewDurableSequence(DurableSequenceOptions{})
	if err != nil {
		t.Fatalf("NewDurableSequence() error = %v", err)
	}
	const workers = 8
	const allocationsPerWorker = 100
	values := make(chan uint64, workers*allocationsPerWorker)
	var group sync.WaitGroup
	group.Add(workers)
	for range workers {
		go func() {
			defer group.Done()
			for range allocationsPerWorker {
				value, err := sequence.Allocate()
				if err != nil {
					t.Errorf("Allocate() error = %v", err)
					return
				}
				values <- value
			}
		}()
	}
	group.Wait()
	close(values)
	seen := make(map[uint64]struct{}, workers*allocationsPerWorker)
	for value := range values {
		if _, exists := seen[value]; exists {
			t.Fatalf("duplicate allocated value %d", value)
		}
		seen[value] = struct{}{}
	}
	if len(seen) != workers*allocationsPerWorker || sequence.Current() != workers*allocationsPerWorker {
		t.Fatalf("allocated values = %d, current = %d", len(seen), sequence.Current())
	}
	for value := uint64(1); value <= workers*allocationsPerWorker; value++ {
		if _, exists := seen[value]; !exists {
			t.Fatalf("missing allocated value %d", value)
		}
	}
}

func TestT250DurableSequenceRejectsExhaustionAndMissingPath(t *testing.T) {
	sequence, err := NewDurableSequence(DurableSequenceOptions{Initial: ^uint64(0)})
	if err != nil {
		t.Fatalf("NewDurableSequence() error = %v", err)
	}
	if _, err := sequence.Allocate(); !errors.Is(err, ErrDurableSequenceExhausted) {
		t.Fatalf("exhausted Allocate() error = %v", err)
	}
	if err := sequence.Save(); !errors.Is(err, ErrDurableSequencePath) {
		t.Fatalf("in-memory Save() error = %v, want path error", err)
	}
}
