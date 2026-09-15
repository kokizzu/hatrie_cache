package hatCache

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestCH009AsyncInsertBufferFlushesBoundedAtomicBatches(t *testing.T) {
	trie := CreateHatTrie()
	defer trie.Destroy()

	journal, err := OpenCommandJournalWithOptions(filepath.Join(t.TempDir(), "commands.journal"), CommandJournalOptions{
		GroupCommitMaxBatch: 64,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()

	buffer, err := NewAsyncInsertBuffer(journal, trie, AsyncInsertBufferOptions{
		BatchSize:     2,
		Capacity:      4,
		FlushInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}

	first, err := buffer.Submit(context.Background(), CacheCommandRequest{Command: "SETSTR", Key: "batch:first", Value: "one"})
	if err != nil {
		t.Fatal(err)
	}
	if first.Status() != AsyncInsertSubmissionPending {
		t.Fatalf("first submission status before batch completion = %v, want pending", first.Status())
	}
	second, err := buffer.Submit(context.Background(), CacheCommandRequest{Command: "SETSTR", Key: "batch:second", Value: "two"})
	if err != nil {
		t.Fatal(err)
	}

	firstResponse, err := first.Wait(context.Background())
	if err != nil || !firstResponse.OK {
		t.Fatalf("first response = %#v/%v", firstResponse, err)
	}
	secondResponse, err := second.Wait(context.Background())
	if err != nil || !secondResponse.OK {
		t.Fatalf("second response = %#v/%v", secondResponse, err)
	}
	if got := journal.Sequence(); got != 2 {
		t.Fatalf("journal sequence = %d, want two committed records", got)
	}
	if got := trie.ExecuteCommand(CacheCommandRequest{Command: "GETSTR", Key: "batch:first"}); !got.OK || got.Value != "one" {
		t.Fatalf("first value = %#v, want one", got)
	}
	if got := trie.ExecuteCommand(CacheCommandRequest{Command: "GETSTR", Key: "batch:second"}); !got.OK || got.Value != "two" {
		t.Fatalf("second value = %#v, want two", got)
	}

	if err := buffer.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestCH009AsyncInsertBufferFlushesPartialBatchAndOwnsRequests(t *testing.T) {
	trie := CreateHatTrie()
	defer trie.Destroy()

	journal, err := OpenCommandJournalWithOptions(filepath.Join(t.TempDir(), "commands.journal"), CommandJournalOptions{
		GroupCommitMaxBatch: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()

	buffer, err := NewAsyncInsertBuffer(journal, trie, AsyncInsertBufferOptions{
		BatchSize:     4,
		Capacity:      4,
		FlushInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}

	ttl := int64(60)
	request := CacheCommandRequest{Command: "SETSTR", Key: "batch:owned", Value: "before", TTLSeconds: &ttl}
	receipt, err := buffer.Submit(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	request.Value = "after"
	ttl = 0
	if err := buffer.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	response, err := receipt.Wait(context.Background())
	if err != nil || !response.OK {
		t.Fatalf("partial batch response = %#v/%v", response, err)
	}
	if got := trie.ExecuteCommand(CacheCommandRequest{Command: "GETSTR", Key: "batch:owned"}); !got.OK || got.Value != "before" {
		t.Fatalf("owned value = %#v, want before", got)
	}
	if err := buffer.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestCH009AsyncInsertBufferJournalReplaysBatchedWrites(t *testing.T) {
	trie := CreateHatTrie()
	defer trie.Destroy()

	journalPath := filepath.Join(t.TempDir(), "commands.journal")
	journal, err := OpenCommandJournalWithOptions(journalPath, CommandJournalOptions{
		GroupCommitMaxBatch: 8,
	})
	if err != nil {
		t.Fatal(err)
	}

	buffer, err := NewAsyncInsertBuffer(journal, trie, AsyncInsertBufferOptions{
		BatchSize:     2,
		Capacity:      2,
		FlushInterval: time.Hour,
	})
	if err != nil {
		_ = journal.Close()
		t.Fatal(err)
	}
	if _, err := buffer.Submit(context.Background(), CacheCommandRequest{Command: "SETSTR", Key: "batch:replay:first", Value: "one"}); err != nil {
		t.Fatal(err)
	}
	if _, err := buffer.Submit(context.Background(), CacheCommandRequest{Command: "SETSTR", Key: "batch:replay:second", Value: "two"}); err != nil {
		t.Fatal(err)
	}
	if err := buffer.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenCommandJournal(journalPath)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	recovered := CreateHatTrie()
	defer recovered.Destroy()
	if sequence, err := reopened.Replay(recovered, 0); err != nil || sequence != 2 {
		t.Fatalf("Replay() sequence/error = %d/%v, want 2/nil", sequence, err)
	}
	for key, want := range map[string]string{
		"batch:replay:first":  "one",
		"batch:replay:second": "two",
	} {
		if response := recovered.ExecuteCommand(CacheCommandRequest{Command: "GETSTR", Key: key}); !response.OK || response.Value != want {
			t.Fatalf("replayed %q = %#v, want %q", key, response, want)
		}
	}
}

func TestCH009AsyncInsertSubmissionReturnsIndividualBatchResponse(t *testing.T) {
	batch := &asyncInsertBatch{
		done: make(chan struct{}),
		requests: []CacheCommandRequest{
			{Command: "SETSTR", Key: "first"},
			{Command: "SETSTR", Key: "second"},
		},
	}
	batch.complete(CacheCommandResponse{
		OK:      true,
		Message: "batch completed",
		Responses: []CacheCommandResponse{
			{OK: true, Message: "stored string"},
			{OK: false, Message: "command rejected"},
		},
	}, nil)

	for index := range batch.requests {
		response, err := (&AsyncInsertSubmission{batch: batch, index: index}).Wait(context.Background())
		if err != nil {
			t.Fatalf("receipt %d wait error = %v", index, err)
		}
		if index == 0 && (!response.OK || response.Message != "stored string") {
			t.Fatalf("receipt %d response = %#v, want individual success", index, response)
		}
		if index == 1 && (response.OK || response.Message != "command rejected") {
			t.Fatalf("receipt %d response = %#v, want individual failure", index, response)
		}
	}
}

func TestCH009AsyncInsertBufferRejectsReadsAndBoundsAdmission(t *testing.T) {
	trie := CreateHatTrie()
	defer trie.Destroy()

	journal, err := OpenCommandJournalWithOptions(filepath.Join(t.TempDir(), "commands.journal"), CommandJournalOptions{
		GroupCommitMaxBatch: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()

	buffer, err := NewAsyncInsertBuffer(journal, trie, AsyncInsertBufferOptions{
		BatchSize:     2,
		Capacity:      1,
		FlushInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer buffer.Close(context.Background())

	if _, err := buffer.Submit(context.Background(), CacheCommandRequest{Command: "GETSTR", Key: "read"}); !errors.Is(err, ErrAsyncInsertBufferWriteOnly) {
		t.Fatalf("read submission error = %v, want write-only error", err)
	}
	if _, err := buffer.Submit(context.Background(), CacheCommandRequest{Command: "BATCH", Batch: []CacheCommandRequest{{Command: "SETSTR", Key: "nested", Value: "value"}}}); !errors.Is(err, ErrAsyncInsertBufferWriteOnly) {
		t.Fatalf("batch submission error = %v, want write-only error", err)
	}
	if _, err := buffer.Submit(context.Background(), CacheCommandRequest{Command: "SETSTR", Key: "idempotent", Value: "value", IdempotencyKey: "request-1"}); !errors.Is(err, ErrAsyncInsertBufferIdempotencyDisabled) {
		t.Fatalf("idempotent submission error = %v, want idempotency-disabled error", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := buffer.Submit(canceled, CacheCommandRequest{Command: "SETSTR", Key: "canceled", Value: "value"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled submission error = %v, want context canceled", err)
	}
	if _, err := buffer.Submit(context.Background(), CacheCommandRequest{Command: "SETSTR", Key: "batch:full", Value: "value"}); err != nil {
		t.Fatal(err)
	}
	if _, err := buffer.Submit(context.Background(), CacheCommandRequest{Command: "SETSTR", Key: "batch:overflow", Value: "value"}); !errors.Is(err, ErrAsyncInsertBufferFull) {
		t.Fatalf("overflow submission error = %v, want full error", err)
	}
	if err := buffer.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestCH009AsyncInsertBufferRequiresJournalGroupCommit(t *testing.T) {
	trie := CreateHatTrie()
	defer trie.Destroy()

	journal, err := OpenCommandJournalWithOptions(filepath.Join(t.TempDir(), "commands.journal"), CommandJournalOptions{
		GroupCommitMaxBatch: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()

	if _, err := NewAsyncInsertBuffer(journal, trie, AsyncInsertBufferOptions{}); !errors.Is(err, ErrCommandJournalAsyncUnsupported) {
		t.Fatalf("constructor error = %v, want async unsupported", err)
	}
}
