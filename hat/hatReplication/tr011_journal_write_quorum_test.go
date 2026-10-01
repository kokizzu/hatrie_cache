package hatReplication

import (
	"context"
	"crypto/sha256"
	"errors"
	"sync"
	"testing"
)

func TestTU10JournalWriteQuorumIsDisabledByDefault(t *testing.T) {
	coordinator, err := NewJournalWriteQuorumCoordinator(JournalWriteQuorumOptions{})
	if err != nil {
		t.Fatalf("new disabled coordinator: %v", err)
	}
	called := false
	result, err := coordinator.Wait(context.Background(), JournalWriteQuorumRequest{Sequence: 7}, func(context.Context, string, JournalWriteQuorumRequest) (JournalWriteQuorumAck, error) {
		called = true
		return JournalWriteQuorumAck{}, nil
	})
	if err != nil {
		t.Fatalf("disabled Wait() error = %v", err)
	}
	if result.Enabled || result.Decision.Satisfied || called {
		t.Fatalf("disabled Wait() = %#v, called=%v; want no-op", result, called)
	}
}

func TestTU10JournalWriteQuorumRequiresExactSequenceAndDigest(t *testing.T) {
	digest := sha256.Sum256([]byte("journal-entry-9"))
	coordinator, err := NewJournalWriteQuorumCoordinator(JournalWriteQuorumOptions{
		Enabled:  true,
		Nodes:    []string{"east", "west", "backup", "archive"},
		Required: 2,
	})
	if err != nil {
		t.Fatalf("new enabled coordinator: %v", err)
	}

	var mu sync.Mutex
	called := make(map[string]bool)
	result, err := coordinator.Wait(context.Background(), JournalWriteQuorumRequest{Sequence: 9, Digest: digest}, func(_ context.Context, node string, request JournalWriteQuorumRequest) (JournalWriteQuorumAck, error) {
		mu.Lock()
		called[node] = true
		mu.Unlock()
		if node == "west" {
			return JournalWriteQuorumAck{Sequence: request.Sequence - 1, Digest: request.Digest, Applied: true}, nil
		}
		if node == "archive" {
			return JournalWriteQuorumAck{Sequence: request.Sequence, Digest: sha256.Sum256([]byte("different-entry")), Applied: true}, nil
		}
		return JournalWriteQuorumAck{Sequence: request.Sequence, Digest: request.Digest, Applied: true}, nil
	})
	if err != nil {
		t.Fatalf("exact quorum Wait() error = %v", err)
	}
	if !result.Decision.Satisfied || result.Decision.Acknowledged != 2 || result.Sequence != 9 {
		t.Fatalf("exact quorum result = %#v, want two exact acknowledgements", result)
	}
	if len(result.Attempts) != 4 || !result.Attempts[0].Exact || result.Attempts[1].Exact || !result.Attempts[2].Exact || result.Attempts[3].Exact {
		t.Fatalf("attempt exactness = %#v, want only east/backup exact", result.Attempts)
	}
	if len(called) != 4 {
		t.Fatalf("called nodes = %#v, want all targets attempted", called)
	}
}

func TestTU10JournalWriteQuorumRejectsInvalidAndUnsatisfiedWrites(t *testing.T) {
	if _, err := NewJournalWriteQuorumCoordinator(JournalWriteQuorumOptions{Enabled: true, Nodes: []string{"east"}, Required: 2}); !errors.Is(err, ErrJournalWriteQuorumInvalid) {
		t.Fatalf("invalid required count error = %v, want invalid", err)
	}
	majority, err := NewJournalWriteQuorumCoordinator(JournalWriteQuorumOptions{Enabled: true, Nodes: []string{"east", "west", "backup"}})
	if err != nil || majority.Required() != 2 {
		t.Fatalf("default required = %d, error=%v, want majority 2", majority.Required(), err)
	}
	coordinator, err := NewJournalWriteQuorumCoordinator(JournalWriteQuorumOptions{Enabled: true, Nodes: []string{"east", "west"}, Required: 2})
	if err != nil {
		t.Fatalf("new coordinator: %v", err)
	}
	if _, err := coordinator.Wait(context.Background(), JournalWriteQuorumRequest{}, func(context.Context, string, JournalWriteQuorumRequest) (JournalWriteQuorumAck, error) {
		return JournalWriteQuorumAck{}, nil
	}); !errors.Is(err, ErrJournalWriteQuorumInvalid) {
		t.Fatalf("zero sequence error = %v, want invalid", err)
	}
	result, err := coordinator.Wait(context.Background(), JournalWriteQuorumRequest{Sequence: 12}, func(_ context.Context, node string, request JournalWriteQuorumRequest) (JournalWriteQuorumAck, error) {
		if node == "east" {
			return JournalWriteQuorumAck{Sequence: request.Sequence, Applied: true}, nil
		}
		return JournalWriteQuorumAck{}, errors.New("peer unavailable")
	})
	if !errors.Is(err, ErrJournalWriteQuorumUnsatisfied) || result.Decision.Acknowledged != 1 {
		t.Fatalf("unsatisfied result = %#v, error=%v, want one acknowledgement", result, err)
	}
}

func TestTU10JournalWriteQuorumStopsBeforeCallbacksWhenCanceled(t *testing.T) {
	coordinator, err := NewJournalWriteQuorumCoordinator(JournalWriteQuorumOptions{Enabled: true, Nodes: []string{"east", "west"}, Required: 1})
	if err != nil {
		t.Fatalf("new coordinator: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	called := false
	result, err := coordinator.Wait(ctx, JournalWriteQuorumRequest{Sequence: 1}, func(context.Context, string, JournalWriteQuorumRequest) (JournalWriteQuorumAck, error) {
		called = true
		return JournalWriteQuorumAck{}, nil
	})
	if !errors.Is(err, ErrJournalWriteQuorumContextCanceled) || called || result.Decision.Acknowledged != 0 {
		t.Fatalf("canceled result = %#v, error=%v, called=%v", result, err, called)
	}
}

func BenchmarkTU10JournalWriteQuorum(b *testing.B) {
	digest := sha256.Sum256([]byte("journal-entry"))
	b.Run("disabled_noop", func(b *testing.B) {
		coordinator, err := NewJournalWriteQuorumCoordinator(JournalWriteQuorumOptions{})
		if err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := coordinator.Wait(context.Background(), JournalWriteQuorumRequest{Sequence: uint64(i + 1)}, nil); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("enabled_exact", func(b *testing.B) {
		coordinator, err := NewJournalWriteQuorumCoordinator(JournalWriteQuorumOptions{Enabled: true, Nodes: []string{"east", "west", "backup"}, Required: 2})
		if err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			result, err := coordinator.Wait(context.Background(), JournalWriteQuorumRequest{Sequence: uint64(i + 1), Digest: digest}, func(_ context.Context, _ string, request JournalWriteQuorumRequest) (JournalWriteQuorumAck, error) {
				return JournalWriteQuorumAck{Sequence: request.Sequence, Digest: request.Digest, Applied: true}, nil
			})
			if err != nil || !result.Decision.Satisfied {
				b.Fatalf("exact benchmark result = %#v, error=%v", result, err)
			}
		}
	})
}
