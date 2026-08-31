package hatCache_test

import (
	"context"
	"path/filepath"
	"testing"

	"hatrie_cache/hat/hatCache"
	"hatrie_cache/hat/hatSql"
)

type projectionJournalTail struct {
	tail                 hatCache.CommandJournalTail
	after                uint64
	limit                int
	called               bool
	watermarkNames       []string
	watermarkSequences   []uint64
	removedWatermarkName string
}

func (journal *projectionJournalTail) Tail(afterSequence uint64, limit int) (hatCache.CommandJournalTail, error) {
	journal.after = afterSequence
	journal.limit = limit
	journal.called = true
	return journal.tail, nil
}

func (journal *projectionJournalTail) SetProjectionWatermark(name string, sequence uint64) error {
	journal.watermarkNames = append(journal.watermarkNames, name)
	journal.watermarkSequences = append(journal.watermarkSequences, sequence)
	return nil
}

func (journal *projectionJournalTail) RemoveProjectionWatermark(name string) bool {
	journal.removedWatermarkName = name
	return true
}

func TestSQLJournalProjectionRunnerConsumesJournalTail(t *testing.T) {
	rows := map[string][]hatSql.Row{"people": {{"name": "Ada"}}}
	resolver := hatSql.SourceResolverFunc(func(_ string, key string) ([]hatSql.Row, error) {
		return hatSql.CloneRows(rows[key]), nil
	})
	views := hatSql.NewMaterializedViews()
	if _, err := views.Create(context.Background(), hatSql.MaterializedViewDefinition{Name: "people_view", Query: "FROM CACHE('people') SELECT name", Dependencies: []string{"people"}}, resolver, hatSql.QueryOptions{}); err != nil {
		t.Fatal(err)
	}
	runner, err := hatCache.NewSQLJournalProjectionRunner(views, resolver, hatSql.QueryOptions{}, hatSql.IncrementalProjectionRunnerOptions{Name: "people", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	rows["people"] = []hatSql.Row{{"name": "Lin"}}
	journal := &projectionJournalTail{tail: hatCache.CommandJournalTail{LastSequence: 1, Entries: []hatCache.CommandJournalRecord{{Sequence: 1, Request: hatCache.CacheCommandRequest{Command: "SET", Key: "people"}}}}}
	run, err := runner.RunOnce(context.Background(), journal, 16)
	if err != nil {
		t.Fatal(err)
	}
	if !journal.called || journal.after != 0 || journal.limit != 16 || run.ThroughSequence != 1 || len(run.Refreshed) != 1 {
		t.Fatalf("journal = %#v, run = %#v", journal, run)
	}
	if len(journal.watermarkNames) != 0 {
		t.Fatalf("legacy runner unexpectedly set retention watermarks: %#v", journal.watermarkNames)
	}
	view, ok := views.Get("people_view")
	if !ok || view.Status.Revision != 2 || view.Result.Rows[0]["name"] != "Lin" {
		t.Fatalf("view = %#v", view)
	}
}

func TestSQLJournalProjectionRunnerConsumesCommandJournal(t *testing.T) {
	trie := hatCache.CreateHatTrie()
	defer trie.Destroy()
	journal, err := hatCache.OpenCommandJournal(filepath.Join(t.TempDir(), "cache.journal"))
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	if response := journal.ExecuteCommand(trie, hatCache.CacheCommandRequest{Command: "SETSTR", Key: "people", Value: `[{"name":"Ada"}]`}); !response.OK {
		t.Fatalf("initial journal write = %#v", response)
	}
	views := hatSql.NewMaterializedViews()
	if _, err := views.Create(context.Background(), hatSql.MaterializedViewDefinition{Name: "people_view", Query: "FROM CACHE('people') SELECT name", Dependencies: []string{"people"}}, trie, hatSql.QueryOptions{}); err != nil {
		t.Fatal(err)
	}
	if response := journal.ExecuteCommand(trie, hatCache.CacheCommandRequest{Command: "SETSTR", Key: "people", Value: `[{"name":"Lin"}]`}); !response.OK {
		t.Fatalf("updated journal write = %#v", response)
	}
	runner, err := hatCache.NewSQLJournalProjectionRunner(views, trie, hatSql.QueryOptions{}, hatSql.IncrementalProjectionRunnerOptions{Name: "people", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	run, err := runner.RunOnce(context.Background(), journal, 16)
	if err != nil {
		t.Fatal(err)
	}
	if run.ThroughSequence != 2 || run.Changes != 2 || len(run.Refreshed) != 1 {
		t.Fatalf("run = %#v", run)
	}
	view, ok := views.Get("people_view")
	if !ok || view.Status.Revision != 2 || view.Result.Rows[0]["name"] != "Lin" {
		t.Fatalf("view = %#v", view)
	}
}

func TestSQLJournalProjectionRunnerProtectsJournalRetentionWhenEnabled(t *testing.T) {
	rows := map[string][]hatSql.Row{"people": {{"name": "Ada"}}}
	resolver := hatSql.SourceResolverFunc(func(_ string, key string) ([]hatSql.Row, error) {
		return hatSql.CloneRows(rows[key]), nil
	})
	views := hatSql.NewMaterializedViews()
	if _, err := views.Create(context.Background(), hatSql.MaterializedViewDefinition{Name: "people_view", Query: "FROM CACHE('people') SELECT name", Dependencies: []string{"people"}}, resolver, hatSql.QueryOptions{}); err != nil {
		t.Fatal(err)
	}
	runner, err := hatCache.NewSQLJournalProjectionRunnerWithOptions(views, resolver, hatSql.QueryOptions{}, hatCache.SQLJournalProjectionRunnerOptions{
		Incremental:             hatSql.IncrementalProjectionRunnerOptions{Name: "people", Enabled: true},
		ProtectJournalRetention: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	rows["people"] = []hatSql.Row{{"name": "Lin"}}
	journal := &projectionJournalTail{tail: hatCache.CommandJournalTail{LastSequence: 1, Entries: []hatCache.CommandJournalRecord{{Sequence: 1, Request: hatCache.CacheCommandRequest{Command: "SET", Key: "people"}}}}}
	run, err := runner.RunOnce(context.Background(), journal, 16)
	if err != nil {
		t.Fatal(err)
	}
	if run.ThroughSequence != 1 || len(journal.watermarkNames) != 2 || journal.watermarkNames[0] != "people" || journal.watermarkSequences[0] != 0 || journal.watermarkSequences[1] != 1 {
		t.Fatalf("run = %#v, journal = %#v", run, journal)
	}
	if !runner.RemoveJournalRetention(journal) || journal.removedWatermarkName != "people" {
		t.Fatalf("journal retention removal = %#v", journal)
	}
}

func TestSQLJournalProjectionRunnerRetainsUnappliedJournalRecordsAcrossSnapshot(t *testing.T) {
	trie := hatCache.CreateHatTrie()
	defer trie.Destroy()
	journal, err := hatCache.OpenCommandJournal(filepath.Join(t.TempDir(), "cache.journal"))
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	if response := journal.ExecuteCommand(trie, hatCache.CacheCommandRequest{Command: "SETSTR", Key: "people", Value: `[{"name":"Ada"}]`}); !response.OK {
		t.Fatalf("initial journal write = %#v", response)
	}
	views := hatSql.NewMaterializedViews()
	if _, err := views.Create(context.Background(), hatSql.MaterializedViewDefinition{Name: "people_view", Query: "FROM CACHE('people') SELECT name", Dependencies: []string{"people"}}, trie, hatSql.QueryOptions{}); err != nil {
		t.Fatal(err)
	}
	runner, err := hatCache.NewSQLJournalProjectionRunnerWithOptions(views, trie, hatSql.QueryOptions{}, hatCache.SQLJournalProjectionRunnerOptions{
		Incremental:             hatSql.IncrementalProjectionRunnerOptions{Name: "people", Enabled: true},
		ProtectJournalRetention: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runner.RunOnce(context.Background(), journal, 16); err != nil {
		t.Fatal(err)
	}
	if response := journal.ExecuteCommand(trie, hatCache.CacheCommandRequest{Command: "SETSTR", Key: "people", Value: `[{"name":"Lin"}]`}); !response.OK {
		t.Fatalf("updated journal write = %#v", response)
	}
	if err := journal.SaveSnapshot(trie, filepath.Join(t.TempDir(), "snapshot.hc")); err != nil {
		t.Fatal(err)
	}
	tail, err := journal.Tail(1, 16)
	if err != nil {
		t.Fatal(err)
	}
	if tail.CompactedThrough != 1 || len(tail.Entries) != 1 || tail.Entries[0].Sequence != 2 {
		t.Fatalf("Tail(1) after protected snapshot = %#v", tail)
	}
}
