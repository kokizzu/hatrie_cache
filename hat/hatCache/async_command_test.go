package hatCache

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestCommandJournalSubmitAsyncCommandReturnsBeforeDurableApply(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commands.journal")
	trie := CreateHatTrie()
	defer trie.Destroy()

	journal, err := OpenCommandJournalWithOptions(path, CommandJournalOptions{
		GroupCommitMaxBatch: 4,
	})
	if err != nil {
		t.Fatal(err)
	}

	syncStarted := make(chan struct{})
	releaseSync := make(chan struct{})
	journal.syncHook = func() error {
		close(syncStarted)
		<-releaseSync
		return journal.file.Sync()
	}

	submission, err := journal.SubmitAsyncCommand(trie, CacheCommandRequest{
		Command: "SET",
		Key:     "async:key",
		Value:   "value",
	})
	if err != nil {
		journal.syncHook = nil
		close(releaseSync)
		_ = journal.Close()
		t.Fatal(err)
	}
	if submission.Status() != AsyncCommandSubmissionPending {
		t.Fatalf("submission status before sync = %v, want pending", submission.Status())
	}
	select {
	case <-syncStarted:
	case <-time.After(time.Second):
		t.Fatal("async submission did not reach journal sync")
	}
	select {
	case <-submission.Done():
		t.Fatal("async submission completed before sync was released")
	default:
	}

	close(releaseSync)
	response, err := submission.Wait(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !response.OK || response.Message != "stored string" {
		t.Fatalf("async submission response = %#v, want stored string", response)
	}
	repeated, err := submission.Wait(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if repeated.OK != response.OK || repeated.Message != response.Message || repeated.Value != response.Value || len(repeated.Responses) != len(response.Responses) {
		t.Fatalf("repeated wait response = %#v, want %#v", repeated, response)
	}
	if submission.Status() != AsyncCommandSubmissionCompleted {
		t.Fatalf("submission status after wait = %v, want completed", submission.Status())
	}
	if got := trie.ExecuteCommand(CacheCommandRequest{Command: "GET", Key: "async:key"}); got.Value != "value" {
		t.Fatalf("in-memory value = %#v, want value", got)
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}

	recovered := CreateHatTrie()
	defer recovered.Destroy()
	reopened, err := OpenCommandJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.Replay(recovered, 0); err != nil {
		_ = reopened.Close()
		t.Fatal(err)
	}
	if got := recovered.ExecuteCommand(CacheCommandRequest{Command: "GET", Key: "async:key"}); got.Value != "value" {
		t.Fatalf("replayed value = %#v, want value", got)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCommandJournalSubmitAsyncCommandRejectsUnsupportedModes(t *testing.T) {
	trie := CreateHatTrie()
	defer trie.Destroy()

	journal, err := OpenCommandJournalWithOptions(filepath.Join(t.TempDir(), "commands.journal"), CommandJournalOptions{
		GroupCommitMaxBatch: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()

	if _, err := journal.SubmitAsyncCommand(trie, CacheCommandRequest{Command: "SET", Key: "key", Value: "value"}); err == nil {
		t.Fatal("SubmitAsyncCommand() error = nil with group commit disabled")
	} else if !errors.Is(err, ErrCommandJournalAsyncUnsupported) {
		t.Fatalf("SubmitAsyncCommand() error = %v, want unsupported error", err)
	}
	if _, err := journal.SubmitAsyncCommand(trie, CacheCommandRequest{Command: "GET", Key: "key"}); err == nil {
		t.Fatal("SubmitAsyncCommand() error = nil for read command")
	} else if !errors.Is(err, ErrCommandJournalAsyncWriteOnly) {
		t.Fatalf("SubmitAsyncCommand() error = %v, want write-only error", err)
	}
}

func TestCommandJournalSubmitAsyncCommandAppliesBoundedBacklogAfterRelease(t *testing.T) {
	trie := CreateHatTrie()
	defer trie.Destroy()

	journal, err := OpenCommandJournalWithOptions(filepath.Join(t.TempDir(), "commands.journal"), CommandJournalOptions{GroupCommitMaxBatch: 2})
	if err != nil {
		t.Fatal(err)
	}

	syncStarted := make(chan struct{})
	releaseSync := make(chan struct{})
	var syncOnce sync.Once
	journal.syncHook = func() error {
		syncOnce.Do(func() { close(syncStarted) })
		<-releaseSync
		return journal.file.Sync()
	}

	first, err := journal.SubmitAsyncCommand(trie, CacheCommandRequest{Command: "SET", Key: "async:1", Value: "one"})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-syncStarted:
	case <-time.After(time.Second):
		t.Fatal("first async submission did not reach journal sync")
	}
	second, err := journal.SubmitAsyncCommand(trie, CacheCommandRequest{Command: "SET", Key: "async:2", Value: "two"})
	if err != nil {
		t.Fatal(err)
	}
	third, err := journal.SubmitAsyncCommand(trie, CacheCommandRequest{Command: "SET", Key: "async:3", Value: "three"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := journal.SubmitAsyncCommand(trie, CacheCommandRequest{Command: "SET", Key: "async:4", Value: "four"}); !errors.Is(err, ErrCommandJournalAsyncQueueFull) {
		t.Fatalf("fourth async submission error = %v, want queue full", err)
	}
	if got := trie.ExecuteCommand(CacheCommandRequest{Command: "GET", Key: "async:1"}); got.Value != "" {
		t.Fatalf("value before release = %#v, want empty", got)
	}

	close(releaseSync)
	for index, submission := range []*CommandJournalSubmission{first, second, third} {
		response, waitErr := submission.Wait(context.Background())
		if waitErr != nil || !response.OK {
			t.Fatalf("submission %d = %#v/%v, want success", index, response, waitErr)
		}
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCommandJournalSubmitAsyncCommandContextCancellationDoesNotCancelWrite(t *testing.T) {
	trie := CreateHatTrie()
	defer trie.Destroy()
	journal, err := OpenCommandJournalWithOptions(filepath.Join(t.TempDir(), "commands.journal"), CommandJournalOptions{GroupCommitMaxBatch: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()

	submission, err := journal.SubmitAsyncCommand(trie, CacheCommandRequest{Command: "SET", Key: "async:cancel", Value: "value"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := submission.Wait(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled wait error = %v, want context canceled", err)
	}
	response, err := submission.Wait(context.Background())
	if err != nil || !response.OK {
		t.Fatalf("uncancelled wait = %#v/%v, want success", response, err)
	}
}

func TestCommandJournalSubmitAsyncCommandPreservesIdempotency(t *testing.T) {
	trie := CreateHatTrie()
	defer trie.Destroy()
	journal, err := OpenCommandJournalWithOptions(filepath.Join(t.TempDir(), "commands.journal"), CommandJournalOptions{
		GroupCommitMaxBatch: 4,
		IdempotencyCapacity: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()

	request := CacheCommandRequest{Command: "SET", Key: "async:idempotent", Value: "value", IdempotencyKey: "request-1"}
	first, err := journal.SubmitAsyncCommand(trie, request)
	if err != nil {
		t.Fatal(err)
	}
	second, err := journal.SubmitAsyncCommand(trie, request)
	if err != nil {
		t.Fatal(err)
	}
	for index, submission := range []*CommandJournalSubmission{first, second} {
		response, waitErr := submission.Wait(context.Background())
		if waitErr != nil || !response.OK {
			t.Fatalf("submission %d = %#v/%v, want success", index, response, waitErr)
		}
	}
	if got := journal.Sequence(); got != 1 {
		t.Fatalf("journal sequence = %d, want one durable command", got)
	}
}

func TestCommandJournalSubmitAsyncCommandOwnsOptionalPointers(t *testing.T) {
	trie := CreateHatTrie()
	defer trie.Destroy()
	journal, err := OpenCommandJournalWithOptions(filepath.Join(t.TempDir(), "commands.journal"), CommandJournalOptions{GroupCommitMaxBatch: 2})
	if err != nil {
		t.Fatal(err)
	}

	syncStarted := make(chan struct{})
	releaseSync := make(chan struct{})
	var syncOnce sync.Once
	journal.syncHook = func() error {
		syncOnce.Do(func() { close(syncStarted) })
		<-releaseSync
		return journal.file.Sync()
	}
	ttl := int64(60)
	submission, err := journal.SubmitAsyncCommand(trie, CacheCommandRequest{
		Command:    "SET",
		Key:        "async:pointer",
		Value:      "value",
		TTLSeconds: &ttl,
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-syncStarted:
	case <-time.After(time.Second):
		t.Fatal("async submission did not reach journal sync")
	}
	ttl = 0
	close(releaseSync)
	response, err := submission.Wait(context.Background())
	if err != nil || !response.OK {
		t.Fatalf("async submission = %#v/%v, want success", response, err)
	}
	if got := trie.ExecuteCommand(CacheCommandRequest{Command: "GET", Key: "async:pointer"}); got.Value != "value" {
		t.Fatalf("value after caller pointer mutation = %#v, want value", got)
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkCommandJournalAsyncSubmission(b *testing.B) {
	benchmark := func(b *testing.B, mode string) {
		trie := CreateHatTrie()
		journal, err := OpenCommandJournalWithOptions(filepath.Join(b.TempDir(), "commands.journal"), CommandJournalOptions{
			GroupCommitMaxBatch: 64,
		})
		if err != nil {
			b.Fatal(err)
		}
		defer trie.Destroy()
		defer journal.Close()

		request := CacheCommandRequest{Command: "SET", Key: "async:benchmark", Value: "value"}
		b.ReportAllocs()
		b.ResetTimer()
		switch mode {
		case "sync_execute":
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					if response := journal.ExecuteCommand(trie, request); !response.OK {
						b.Errorf("ExecuteCommand() = %#v, want success", response)
					}
				}
			})
		case "async_submit_wait":
			var benchmarkErrors = make(chan error, 1)
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					for {
						submission, submitErr := journal.SubmitAsyncCommand(trie, request)
						if errors.Is(submitErr, ErrCommandJournalAsyncQueueFull) {
							runtime.Gosched()
							continue
						}
						if submitErr != nil {
							select {
							case benchmarkErrors <- submitErr:
							default:
							}
							return
						}
						response, waitErr := submission.Wait(context.Background())
						if waitErr != nil || !response.OK {
							select {
							case benchmarkErrors <- fmt.Errorf("wait = %#v/%v", response, waitErr):
							default:
							}
						}
						break
					}
				}
			})
			select {
			case benchmarkErr := <-benchmarkErrors:
				b.Fatal(benchmarkErr)
			default:
			}
		case "async_admission":
			completions := make(chan *CommandJournalSubmission, 64)
			completionErrors := make(chan error, 1)
			var collector sync.WaitGroup
			collector.Add(1)
			go func() {
				defer collector.Done()
				for submission := range completions {
					response, waitErr := submission.Wait(context.Background())
					if waitErr != nil || !response.OK {
						select {
						case completionErrors <- fmt.Errorf("wait = %#v/%v", response, waitErr):
						default:
						}
					}
				}
			}()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					for {
						submission, submitErr := journal.SubmitAsyncCommand(trie, request)
						if errors.Is(submitErr, ErrCommandJournalAsyncQueueFull) {
							runtime.Gosched()
							continue
						}
						if submitErr != nil {
							select {
							case completionErrors <- submitErr:
							default:
							}
							return
						}
						completions <- submission
						break
					}
				}
			})
			b.StopTimer()
			close(completions)
			collector.Wait()
			select {
			case benchmarkErr := <-completionErrors:
				b.Fatal(benchmarkErr)
			default:
			}
		}
	}

	b.Run("sync_execute", func(b *testing.B) { benchmark(b, "sync_execute") })
	b.Run("async_submit_wait", func(b *testing.B) { benchmark(b, "async_submit_wait") })
	b.Run("async_admission", func(b *testing.B) { benchmark(b, "async_admission") })
}
