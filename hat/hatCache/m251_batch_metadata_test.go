package hatCache

import (
	"path/filepath"
	"testing"
	"time"
)

func TestM251GroupCommitRollbackKeepsSuccessfulPrefixAndSuffix(t *testing.T) {
	for _, idempotent := range []bool{false, true} {
		t.Run(map[bool]string{false: "ordinary", true: "idempotent"}[idempotent], func(t *testing.T) {
			options := CommandJournalOptions{GroupCommitMaxBatch: 8}
			if idempotent {
				options.IdempotencyCapacity = 8
			}
			journal, err := OpenCommandJournalWithOptions(filepath.Join(t.TempDir(), "commands.journal"), options)
			if err != nil {
				t.Fatal(err)
			}
			defer journal.Close()

			trie := newTestTrie(t)
			requests := []CacheCommandRequest{
				{Command: "SETSTR", Key: "m251:first", Value: "one"},
				{Command: "SETINT", Key: "m251:rejected", Value: "invalid"},
				{Command: "SETSTR", Key: "m251:last", Value: "three"},
			}
			jobs := make([]*commandJournalJob, len(requests))
			for index, request := range requests {
				job := &commandJournalJob{
					trie:           trie,
					request:        request,
					journalRequest: normalizeJournalRequest(request, time.Unix(2_510, 0)),
					result:         make(chan CacheCommandResponse, 1),
				}
				if idempotent {
					request.IdempotencyKey = "m251:" + request.Key
					job.request = request
					job.journalRequest = normalizeJournalRequest(request, time.Unix(2_510, 0))
					job.idempotency, err = journal.idempotencyCheck(request)
					if err != nil {
						t.Fatal(err)
					}
				}
				jobs[index] = job
			}

			journal.processGroupCommit(jobs)
			for index, job := range jobs {
				response := <-job.result
				if index == 1 {
					if response.OK {
						t.Fatalf("rejected response = %#v, want error", response)
					}
					continue
				}
				if !response.OK {
					t.Fatalf("successful response %d = %#v", index, response)
				}
			}
			if got := trie.GetString("m251:first"); got != "one" {
				t.Fatalf("first value = %q, want one", got)
			}
			if got := trie.GetString("m251:rejected"); got != "" {
				t.Fatalf("rejected value = %q, want missing", got)
			}
			if got := trie.GetString("m251:last"); got != "three" {
				t.Fatalf("last value = %q, want three", got)
			}
			tail, err := journal.Tail(0, 10)
			if err != nil {
				t.Fatal(err)
			}
			if len(tail.Entries) != 2 || tail.Entries[0].Request.Key != "m251:first" || tail.Entries[1].Request.Key != "m251:last" {
				t.Fatalf("journal tail = %#v, want successful prefix and suffix", tail.Entries)
			}
		})
	}
}

func BenchmarkM251GroupCommitMetadata(b *testing.B) {
	journal, err := OpenCommandJournalWithOptions(filepath.Join(b.TempDir(), "commands.journal"), CommandJournalOptions{
		GroupCommitMaxBatch: 64,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer journal.Close()
	journal.syncHook = func() error { return nil }
	journal.writeHook = func(data []byte) (int, error) { return len(data), nil }
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	jobs := make([]*commandJournalJob, 64)
	for index := range jobs {
		request := CacheCommandRequest{Command: "SETSTR", Key: "m251:bench:" + string(rune(index)), Value: "value"}
		jobs[index] = &commandJournalJob{
			trie:           trie,
			request:        request,
			journalRequest: normalizeJournalRequest(request, time.Unix(2_511, 0)),
			result:         make(chan CacheCommandResponse, 1),
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		journal.processGroupCommit(jobs)
		for _, job := range jobs {
			<-job.result
		}
	}
}
