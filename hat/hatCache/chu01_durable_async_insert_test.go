package hatCache

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestCHU01DurableAsyncInsertDeduplicatesAcrossJournalReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commands.journal")
	request := CacheCommandRequest{
		Command:        "SET",
		Key:            "chu01:key",
		Value:          "first",
		IdempotencyKey: "insert-1",
	}

	firstTrie := CreateHatTrie()
	firstJournal, err := OpenCommandJournalWithOptions(path, CommandJournalOptions{
		GroupCommitMaxBatch: 4,
		IdempotencyCapacity: 8,
	})
	if err != nil {
		firstTrie.Destroy()
		t.Fatal(err)
	}
	firstBuffer, err := NewAsyncInsertBuffer(firstJournal, firstTrie, AsyncInsertBufferOptions{
		BatchSize:     2,
		Capacity:      4,
		FlushInterval: time.Hour,
	})
	if err != nil {
		_ = firstJournal.Close()
		firstTrie.Destroy()
		t.Fatal(err)
	}

	submission, err := firstBuffer.Submit(context.Background(), request)
	if err != nil {
		_ = firstBuffer.Close(context.Background())
		_ = firstJournal.Close()
		firstTrie.Destroy()
		t.Fatal(err)
	}
	if err := firstBuffer.Flush(context.Background()); err != nil {
		_ = firstBuffer.Close(context.Background())
		_ = firstJournal.Close()
		firstTrie.Destroy()
		t.Fatal(err)
	}
	response, err := submission.Wait(context.Background())
	if err != nil || !response.OK {
		_ = firstBuffer.Close(context.Background())
		_ = firstJournal.Close()
		firstTrie.Destroy()
		t.Fatalf("first async insert response = %#v, error = %v", response, err)
	}
	if got := firstTrie.ExecuteCommand(CacheCommandRequest{Command: "GET", Key: request.Key}).Value; got != request.Value {
		t.Fatalf("first async insert value = %#v, want %q", got, request.Value)
	}
	if err := firstBuffer.Close(context.Background()); err != nil {
		_ = firstJournal.Close()
		firstTrie.Destroy()
		t.Fatal(err)
	}
	if err := firstJournal.Close(); err != nil {
		firstTrie.Destroy()
		t.Fatal(err)
	}
	firstTrie.Destroy()

	secondTrie := CreateHatTrie()
	secondJournal, err := OpenCommandJournalWithOptions(path, CommandJournalOptions{
		GroupCommitMaxBatch: 4,
		IdempotencyCapacity: 8,
	})
	if err != nil {
		secondTrie.Destroy()
		t.Fatal(err)
	}
	if _, err := secondJournal.Replay(secondTrie, 0); err != nil {
		_ = secondJournal.Close()
		secondTrie.Destroy()
		t.Fatal(err)
	}
	if got := secondTrie.ExecuteCommand(CacheCommandRequest{Command: "GET", Key: request.Key}).Value; got != request.Value {
		_ = secondJournal.Close()
		secondTrie.Destroy()
		t.Fatalf("replayed async insert value = %#v, want %q", got, request.Value)
	}

	secondBuffer, err := NewAsyncInsertBuffer(secondJournal, secondTrie, AsyncInsertBufferOptions{
		BatchSize:     2,
		Capacity:      4,
		FlushInterval: time.Hour,
	})
	if err != nil {
		_ = secondJournal.Close()
		secondTrie.Destroy()
		t.Fatal(err)
	}
	beforeDuplicate, err := os.Stat(path)
	if err != nil {
		_ = secondBuffer.Close(context.Background())
		_ = secondJournal.Close()
		secondTrie.Destroy()
		t.Fatal(err)
	}
	duplicate, err := secondBuffer.Submit(context.Background(), request)
	if err != nil {
		_ = secondBuffer.Close(context.Background())
		_ = secondJournal.Close()
		secondTrie.Destroy()
		t.Fatal(err)
	}
	if err := secondBuffer.Flush(context.Background()); err != nil {
		_ = secondBuffer.Close(context.Background())
		_ = secondJournal.Close()
		secondTrie.Destroy()
		t.Fatal(err)
	}
	duplicateResponse, err := duplicate.Wait(context.Background())
	if err != nil || !duplicateResponse.OK {
		_ = secondBuffer.Close(context.Background())
		_ = secondJournal.Close()
		secondTrie.Destroy()
		t.Fatalf("duplicate async insert response = %#v, error = %v", duplicateResponse, err)
	}
	afterDuplicate, err := os.Stat(path)
	if err != nil {
		_ = secondBuffer.Close(context.Background())
		_ = secondJournal.Close()
		secondTrie.Destroy()
		t.Fatal(err)
	}
	if afterDuplicate.Size() != beforeDuplicate.Size() {
		t.Fatalf("duplicate async insert grew journal from %d to %d bytes", beforeDuplicate.Size(), afterDuplicate.Size())
	}
	if got := secondTrie.ExecuteCommand(CacheCommandRequest{Command: "GET", Key: request.Key}).Value; got != request.Value {
		t.Fatalf("duplicate async insert value = %#v, want %q", got, request.Value)
	}

	conflict, err := secondBuffer.Submit(context.Background(), CacheCommandRequest{
		Command:        request.Command,
		Key:            request.Key,
		Value:          "second",
		IdempotencyKey: request.IdempotencyKey,
	})
	if err != nil {
		_ = secondBuffer.Close(context.Background())
		_ = secondJournal.Close()
		secondTrie.Destroy()
		t.Fatal(err)
	}
	if err := secondBuffer.Flush(context.Background()); err != nil {
		_ = secondBuffer.Close(context.Background())
		_ = secondJournal.Close()
		secondTrie.Destroy()
		t.Fatal(err)
	}
	conflictResponse, err := conflict.Wait(context.Background())
	if err != nil {
		_ = secondBuffer.Close(context.Background())
		_ = secondJournal.Close()
		secondTrie.Destroy()
		t.Fatal(err)
	}
	if conflictResponse.OK {
		t.Fatalf("idempotency conflict response = %#v, want failure", conflictResponse)
	}
	if got := secondTrie.ExecuteCommand(CacheCommandRequest{Command: "GET", Key: request.Key}).Value; got != request.Value {
		t.Fatalf("idempotency conflict changed value = %#v, want %q", got, request.Value)
	}
	if err := secondBuffer.Close(context.Background()); err != nil {
		_ = secondJournal.Close()
		secondTrie.Destroy()
		t.Fatal(err)
	}
	if err := secondJournal.Close(); err != nil {
		secondTrie.Destroy()
		t.Fatal(err)
	}
	secondTrie.Destroy()
}

func TestCHU01KeyedAsyncInsertRequiresJournalIdempotency(t *testing.T) {
	trie := CreateHatTrie()
	defer trie.Destroy()
	journal, err := OpenCommandJournalWithOptions(filepath.Join(t.TempDir(), "commands.journal"), CommandJournalOptions{
		GroupCommitMaxBatch: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()

	buffer, err := NewAsyncInsertBuffer(journal, trie, AsyncInsertBufferOptions{FlushInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer buffer.Close(context.Background())

	_, err = buffer.Submit(context.Background(), CacheCommandRequest{
		Command:        "SET",
		Key:            "chu01:disabled",
		Value:          "value",
		IdempotencyKey: "insert-disabled",
	})
	if !errors.Is(err, ErrAsyncInsertBufferIdempotencyDisabled) {
		t.Fatalf("keyed async insert error = %v, want idempotency-disabled error", err)
	}
}
