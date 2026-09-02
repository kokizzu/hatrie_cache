package hatCache_test

import (
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"hatrie_cache/hat/hatCache"
)

func TestCommandJournalIdempotencyIsDisabledByDefault(t *testing.T) {
	journal, err := hatCache.OpenCommandJournal(filepath.Join(t.TempDir(), "commands.journal"))
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	trie := hatCache.CreateHatTrie()
	defer trie.Destroy()

	request := hatCache.CacheCommandRequest{Command: "INC", Key: "counter", Value: "1", IdempotencyKey: "retry-1"}
	if response := journal.ExecuteCommand(trie, request); !response.OK {
		t.Fatalf("first response = %#v", response)
	}
	if response := journal.ExecuteCommand(trie, request); !response.OK {
		t.Fatalf("second response = %#v", response)
	}
	if got := journal.Sequence(); got != 2 {
		t.Fatalf("Sequence() = %d, want 2 with default-off deduplication", got)
	}
	tail, err := journal.Tail(0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(tail.Entries) != 2 || tail.Entries[0].Request.IdempotencyKey != "" || tail.Entries[1].Request.IdempotencyKey != "" {
		t.Fatalf("default-off journal tail = %#v, want idempotency keys omitted", tail.Entries)
	}
}

func TestCommandJournalIdempotencyDeduplicatesAndRejectsConflicts(t *testing.T) {
	journal := openIdempotentCommandJournal(t, hatCache.CommandJournalOptions{})
	defer journal.Close()
	trie := hatCache.CreateHatTrie()
	defer trie.Destroy()

	request := hatCache.CacheCommandRequest{Command: "INC", Key: "counter", Value: "1", IdempotencyKey: "retry-1"}
	first := journal.ExecuteCommand(trie, request)
	if !first.OK {
		t.Fatalf("first response = %#v", first)
	}
	duplicate := journal.ExecuteCommand(trie, request)
	if !duplicate.OK || !reflect.DeepEqual(duplicate, first) {
		t.Fatalf("duplicate response = %#v, want original %#v", duplicate, first)
	}
	conflict := journal.ExecuteCommand(trie, hatCache.CacheCommandRequest{
		Command: "INC", Key: "counter", Value: "2", IdempotencyKey: "retry-1",
	})
	if conflict.OK || !strings.Contains(conflict.Message, "idempotency key") {
		t.Fatalf("conflict response = %#v, want idempotency conflict", conflict)
	}
	if strings.Contains(conflict.Message, "retry-1") {
		t.Fatalf("conflict response = %#v, must not echo idempotency key", conflict)
	}
	if got := journal.Sequence(); got != 1 {
		t.Fatalf("Sequence() = %d, want 1 after duplicate and conflict", got)
	}
	value := trie.ExecuteCommand(hatCache.CacheCommandRequest{Command: "GET", Key: "counter"})
	if !value.OK || value.Value != "1" {
		t.Fatalf("counter = %#v, want one applied increment", value)
	}
}

func TestCommandJournalIdempotencySurvivesReopen(t *testing.T) {
	for _, format := range []hatCache.CommandJournalFormat{
		hatCache.CommandJournalFormatBinary,
		hatCache.CommandJournalFormatJSON,
	} {
		t.Run(string(format), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "commands.journal")
			options := idempotentCommandJournalOptions()
			options.Format = format
			journal, err := hatCache.OpenCommandJournalWithOptions(path, options)
			if err != nil {
				t.Fatal(err)
			}
			trie := hatCache.CreateHatTrie()
			request := hatCache.CacheCommandRequest{Command: "INC", Key: "counter", Value: "1", IdempotencyKey: "retry-1"}
			if response := journal.ExecuteCommand(trie, request); !response.OK {
				t.Fatalf("first response = %#v", response)
			}
			if err := journal.Close(); err != nil {
				t.Fatal(err)
			}
			trie.Destroy()

			reopened, err := hatCache.OpenCommandJournalWithOptions(path, options)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			replayed := hatCache.CreateHatTrie()
			defer replayed.Destroy()
			if sequence, err := reopened.Replay(replayed, 0); err != nil || sequence != 1 {
				t.Fatalf("Replay() = %d/%v, want sequence 1", sequence, err)
			}
			duplicate := reopened.ExecuteCommand(replayed, request)
			if !duplicate.OK {
				t.Fatalf("duplicate after reopen = %#v", duplicate)
			}
			if got := reopened.Sequence(); got != 1 {
				t.Fatalf("Sequence() = %d, want 1 after restart duplicate", got)
			}
			value := replayed.ExecuteCommand(hatCache.CacheCommandRequest{Command: "GET", Key: "counter"})
			if !value.OK || value.Value != "1" {
				t.Fatalf("replayed counter = %#v, want one applied increment", value)
			}
		})
	}
}

func TestCommandJournalIdempotencyCapacityEvictsOldest(t *testing.T) {
	options := idempotentCommandJournalOptions()
	options.IdempotencyCapacity = 1
	journal, err := hatCache.OpenCommandJournalWithOptions(filepath.Join(t.TempDir(), "commands.journal"), options)
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	trie := hatCache.CreateHatTrie()
	defer trie.Destroy()

	for _, request := range []hatCache.CacheCommandRequest{
		{Command: "INC", Key: "first", Value: "1", IdempotencyKey: "first-request"},
		{Command: "INC", Key: "second", Value: "1", IdempotencyKey: "second-request"},
		{Command: "INC", Key: "first", Value: "1", IdempotencyKey: "first-request"},
	} {
		if response := journal.ExecuteCommand(trie, request); !response.OK {
			t.Fatalf("ExecuteCommand(%#v) = %#v", request, response)
		}
	}
	if got := trie.ExecuteCommand(hatCache.CacheCommandRequest{Command: "GET", Key: "first"}).Value; got != "2" {
		t.Fatalf("first counter = %q, want two increments after eviction", got)
	}
	if got := trie.ExecuteCommand(hatCache.CacheCommandRequest{Command: "GET", Key: "second"}).Value; got != "1" {
		t.Fatalf("second counter = %q, want one increment", got)
	}
	if got := journal.Sequence(); got != 3 {
		t.Fatalf("Sequence() = %d, want three journal records after eviction", got)
	}
}

func TestCommandJournalIdempotencyRejectsOversizedKey(t *testing.T) {
	journal := openIdempotentCommandJournal(t, hatCache.CommandJournalOptions{})
	defer journal.Close()
	trie := hatCache.CreateHatTrie()
	defer trie.Destroy()

	response := journal.ExecuteCommand(trie, hatCache.CacheCommandRequest{
		Command:        "INC",
		Key:            "counter",
		Value:          "1",
		IdempotencyKey: strings.Repeat("x", hatCache.MaxCommandJournalIdempotencyKeyBytes+1),
	})
	if response.OK || !strings.Contains(response.Message, "idempotency key") {
		t.Fatalf("oversized-key response = %#v, want validation error", response)
	}
	if got := journal.Sequence(); got != 0 {
		t.Fatalf("Sequence() = %d, want no journal append", got)
	}
	if got := trie.ExecuteCommand(hatCache.CacheCommandRequest{Command: "GET", Key: "counter"}).Value; got != "" {
		t.Fatalf("counter = %q, want rejected command not applied", got)
	}
}

func TestCommandJournalIdempotencyDeduplicatesGroupCommitRetries(t *testing.T) {
	options := idempotentCommandJournalOptions()
	options.GroupCommitMaxBatch = 8
	options.GroupCommitWindow = time.Millisecond
	journal, err := hatCache.OpenCommandJournalWithOptions(filepath.Join(t.TempDir(), "commands.journal"), options)
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	trie := hatCache.CreateHatTrie()
	defer trie.Destroy()

	request := hatCache.CacheCommandRequest{Command: "INC", Key: "counter", Value: "1", IdempotencyKey: "retry-group"}
	const workers = 8
	start := make(chan struct{})
	responses := make(chan hatCache.CacheCommandResponse, workers)
	var group sync.WaitGroup
	for index := 0; index < workers; index++ {
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			responses <- journal.ExecuteCommand(trie, request)
		}()
	}
	close(start)
	group.Wait()
	close(responses)
	for response := range responses {
		if !response.OK {
			t.Fatalf("group response = %#v", response)
		}
	}
	if got := journal.Sequence(); got != 1 {
		t.Fatalf("Sequence() = %d, want one committed group request", got)
	}
	value := trie.ExecuteCommand(hatCache.CacheCommandRequest{Command: "GET", Key: "counter"})
	if !value.OK || value.Value != "1" {
		t.Fatalf("group counter = %#v, want one applied increment", value)
	}
}

func TestCommandJournalIdempotencyGroupCommitRejectsSameBatchConflict(t *testing.T) {
	options := idempotentCommandJournalOptions()
	options.GroupCommitMaxBatch = 8
	options.GroupCommitWindow = time.Millisecond
	journal, err := hatCache.OpenCommandJournalWithOptions(filepath.Join(t.TempDir(), "commands.journal"), options)
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	trie := hatCache.CreateHatTrie()
	defer trie.Destroy()

	requests := []hatCache.CacheCommandRequest{
		{Command: "INC", Key: "conflict-counter", Value: "1", IdempotencyKey: "same-batch-key"},
		{Command: "INC", Key: "conflict-counter", Value: "2", IdempotencyKey: "same-batch-key"},
	}
	start := make(chan struct{})
	responses := make(chan hatCache.CacheCommandResponse, len(requests))
	var group sync.WaitGroup
	for _, request := range requests {
		group.Add(1)
		go func(request hatCache.CacheCommandRequest) {
			defer group.Done()
			<-start
			responses <- journal.ExecuteCommand(trie, request)
		}(request)
	}
	close(start)
	group.Wait()
	close(responses)

	okCount := 0
	conflictCount := 0
	for response := range responses {
		if response.OK {
			okCount++
		} else if strings.Contains(response.Message, "idempotency key") {
			conflictCount++
		} else {
			t.Fatalf("unexpected group response = %#v", response)
		}
	}
	if okCount != 1 || conflictCount != 1 {
		t.Fatalf("group responses = %d successes/%d conflicts, want one each", okCount, conflictCount)
	}
	if got := journal.Sequence(); got != 1 {
		t.Fatalf("Sequence() = %d, want one journal record", got)
	}
	value := trie.ExecuteCommand(hatCache.CacheCommandRequest{Command: "GET", Key: "conflict-counter"})
	if !value.OK || (value.Value != "1" && value.Value != "2") {
		t.Fatalf("conflict counter = %#v, want one applied increment", value)
	}
}

func openIdempotentCommandJournal(t *testing.T, overrides hatCache.CommandJournalOptions) *hatCache.CommandJournal {
	t.Helper()
	options := idempotentCommandJournalOptions()
	if overrides.GroupCommitMaxBatch != 0 {
		options.GroupCommitMaxBatch = overrides.GroupCommitMaxBatch
	}
	if overrides.GroupCommitWindow != 0 {
		options.GroupCommitWindow = overrides.GroupCommitWindow
	}
	journal, err := hatCache.OpenCommandJournalWithOptions(filepath.Join(t.TempDir(), "commands.journal"), options)
	if err != nil {
		t.Fatal(err)
	}
	return journal
}

func idempotentCommandJournalOptions() hatCache.CommandJournalOptions {
	return hatCache.CommandJournalOptions{
		Format:              hatCache.DefaultCommandJournalFormat,
		GroupCommitMaxBatch: 1,
		IdempotencyCapacity: 16,
	}
}
