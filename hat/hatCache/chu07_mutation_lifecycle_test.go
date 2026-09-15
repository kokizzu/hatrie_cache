package hatCache

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestCommandJournalSubmissionExposesDurableMutationLifecycle(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commands.journal")
	options := CommandJournalOptions{
		GroupCommitWindow:   10 * time.Millisecond,
		GroupCommitMaxBatch: 4,
	}
	journal, err := OpenCommandJournalWithOptions(path, options)
	if err != nil {
		t.Fatal(err)
	}
	trie := CreateHatTrie()
	defer trie.Destroy()
	defer journal.Close()

	submission, err := journal.SubmitAsyncCommand(trie, CacheCommandRequest{
		Command: "SETSTR",
		Key:     "mutation-lifecycle",
		Value:   "ready",
	})
	if err != nil {
		t.Fatal(err)
	}
	response, err := submission.Wait(context.Background())
	if err != nil || !response.OK {
		t.Fatalf("submission.Wait() = %#v/%v, want committed response", response, err)
	}
	sequence := submission.Sequence()
	if sequence == 0 {
		t.Fatalf("submission.Sequence() = 0, want durable journal sequence")
	}
	if got := submission.Status(); got != AsyncCommandSubmissionCommitted {
		t.Fatalf("submission.Status() = %v, want committed", got)
	}

	status, err := journal.MutationStatus(sequence)
	if err != nil {
		t.Fatal(err)
	}
	if status.Sequence != sequence || status.State != CommandJournalMutationCommitted || status.Progress != 1 {
		t.Fatalf("MutationStatus(%d) = %#v, want committed sequence with progress 1", sequence, status)
	}
	if status.Command != "SETSTR" || status.Key != "mutation-lifecycle" {
		t.Fatalf("MutationStatus(%d) metadata = %#v, want command/key", sequence, status)
	}

	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenCommandJournalWithOptions(path, options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	restored, err := reopened.MutationStatus(sequence)
	if err != nil {
		t.Fatal(err)
	}
	if restored.State != CommandJournalMutationCommitted || restored.Sequence != sequence {
		t.Fatalf("reopened MutationStatus(%d) = %#v, want committed durable status", sequence, restored)
	}
}

func TestSQLSystemMutationsIncludesLifecycleMetadata(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commands.journal")
	journal, err := OpenCommandJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	trie := CreateHatTrie()
	defer trie.Destroy()
	defer journal.Close()

	response := journal.ExecuteCommand(trie, CacheCommandRequest{Command: "SETSTR", Key: "system-mutation", Value: "hidden"})
	if !response.OK {
		t.Fatalf("ExecuteCommand() = %#v, want ok", response)
	}
	rows, err := NewSQLSystemTablesResolver(trie, SQLSystemTablesResolverOptions{Journal: journal}).ResolveSQLSource("CACHE", SQLSystemMutationsTable)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("system.mutations rows = %#v, want one row", rows)
	}
	row := rows[0]
	if row["mutation_id"] != uint64(1) || row["state"] != "committed" || row["progress"] != int64(1) {
		t.Fatalf("system.mutations row = %#v, want lifecycle metadata", row)
	}
	if row["command"] != "SETSTR" || row["key"] != "system-mutation" {
		t.Fatalf("system.mutations metadata = %#v, want command/key", row)
	}
	if _, exposed := row["value"]; exposed {
		t.Fatalf("system.mutations must not expose values: %#v", row)
	}
}

func TestCommandJournalSubmissionRejectsWithoutPhantomMutation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commands.journal")
	journal, err := OpenCommandJournalWithOptions(path, CommandJournalOptions{
		GroupCommitWindow:   time.Millisecond,
		GroupCommitMaxBatch: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	trie := CreateHatTrie()
	defer trie.Destroy()
	if response := trie.ExecuteCommand(CacheCommandRequest{Command: "SETINT", Key: "rejected-mutation", Value: "2147483647"}); !response.OK {
		t.Fatalf("seed rejection fixture = %#v, want ok", response)
	}

	submission, err := journal.SubmitAsyncCommand(trie, CacheCommandRequest{
		Command: "INC",
		Key:     "rejected-mutation",
		Value:   "1",
	})
	if err != nil {
		t.Fatal(err)
	}
	response, err := submission.Wait(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if response.OK || submission.Status() != AsyncCommandSubmissionRejected {
		t.Fatalf("rejected submission = %#v/%v, want rejected", response, submission.Status())
	}
	if submission.Sequence() != 0 || journal.Sequence() != 0 {
		t.Fatalf("rejected submission sequence/journal sequence = %d/%d, want zero", submission.Sequence(), journal.Sequence())
	}
	if submission.Error() != nil {
		t.Fatalf("rejected submission Error() = %v, want nil command-level error", submission.Error())
	}
}

func TestCommandJournalSubmissionReportsDurabilityFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commands.journal")
	journal, err := OpenCommandJournalWithOptions(path, CommandJournalOptions{
		GroupCommitWindow:   time.Millisecond,
		GroupCommitMaxBatch: 2,
		IdempotencyCapacity: 16,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	trie := CreateHatTrie()
	defer trie.Destroy()
	wantErr := errors.New("injected mutation fsync failure")
	journal.syncHook = func() error { return wantErr }

	submission, err := journal.SubmitAsyncCommand(trie, CacheCommandRequest{
		Command:        "SETSTR",
		Key:            "failed-mutation",
		Value:          "not-durable",
		IdempotencyKey: "failed-mutation-operation",
	})
	if err != nil {
		t.Fatal(err)
	}
	response, err := submission.Wait(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if response.OK || submission.Status() != AsyncCommandSubmissionFailed {
		t.Fatalf("failed submission = %#v/%v, want failed", response, submission.Status())
	}
	if !errors.Is(submission.Error(), wantErr) {
		t.Fatalf("submission.Error() = %v, want %v", submission.Error(), wantErr)
	}
	if submission.Sequence() != 0 || journal.Sequence() != 0 || trie.Exists("failed-mutation") {
		t.Fatalf("failed mutation changed sequence or trie: submission=%d journal=%d exists=%v", submission.Sequence(), journal.Sequence(), trie.Exists("failed-mutation"))
	}
}
