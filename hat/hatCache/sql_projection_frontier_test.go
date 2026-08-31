package hatCache_test

import (
	"context"
	"reflect"
	"testing"

	"hatrie_cache/hat/hatCache"
	"hatrie_cache/hat/hatSql"
)

func TestSQLProjectionRetentionFrontierCommitsOnlyCompleteSourceSet(t *testing.T) {
	frontier, err := hatCache.NewSQLProjectionRetentionFrontier("analytics", []string{"people", "orders"})
	if err != nil {
		t.Fatal(err)
	}
	journal := &projectionJournalTail{}
	if err := frontier.Begin(journal); err != nil {
		t.Fatal(err)
	}
	if got := journal.watermarkSequences; !reflect.DeepEqual(got, []uint64{0}) {
		t.Fatalf("begin watermarks = %#v, want []uint64{0}", got)
	}
	if err := frontier.Commit(journal, map[string]uint64{"people": 12}); err == nil {
		t.Fatal("Commit() unexpectedly accepted a partial source set")
	}
	if frontier.Checkpoint() != 0 || len(journal.watermarkSequences) != 1 {
		t.Fatalf("partial commit advanced frontier: checkpoint=%d watermarks=%#v", frontier.Checkpoint(), journal.watermarkSequences)
	}
	if err := frontier.Commit(journal, map[string]uint64{"people": 12, "orders": 9}); err != nil {
		t.Fatal(err)
	}
	if frontier.Checkpoint() != 9 {
		t.Fatalf("Checkpoint() = %d, want 9", frontier.Checkpoint())
	}
	if got := journal.watermarkSequences; !reflect.DeepEqual(got, []uint64{0, 9}) {
		t.Fatalf("commit watermarks = %#v, want []uint64{0, 9}", got)
	}
	if got, want := frontier.SourceCheckpoints(), []hatCache.SQLProjectionSourceCheckpoint{{Source: "orders", Sequence: 9}, {Source: "people", Sequence: 12}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("SourceCheckpoints() = %#v, want %#v", got, want)
	}
}

func TestSQLProjectionRetentionFrontierRunOnceCommitsAfterAllRunners(t *testing.T) {
	rows := map[string][]hatSql.Row{
		"people": {{"name": "Ada"}},
		"orders": {{"item": "Keyboard"}},
	}
	resolver := hatSql.SourceResolverFunc(func(_ string, key string) ([]hatSql.Row, error) {
		return hatSql.CloneRows(rows[key]), nil
	})
	views := hatSql.NewMaterializedViews()
	for _, definition := range []hatSql.MaterializedViewDefinition{
		{Name: "people_view", Query: "FROM CACHE('people') SELECT name", Dependencies: []string{"people"}},
		{Name: "orders_view", Query: "FROM CACHE('orders') SELECT item", Dependencies: []string{"orders"}},
	} {
		if _, err := views.Create(context.Background(), definition, resolver, hatSql.QueryOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	people, err := hatCache.NewSQLJournalProjectionRunner(views, resolver, hatSql.QueryOptions{}, hatSql.IncrementalProjectionRunnerOptions{Name: "people", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	orders, err := hatCache.NewSQLJournalProjectionRunner(views, resolver, hatSql.QueryOptions{}, hatSql.IncrementalProjectionRunnerOptions{Name: "orders", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	frontier, err := hatCache.NewSQLProjectionRetentionFrontier("analytics", []string{"orders", "people"})
	if err != nil {
		t.Fatal(err)
	}
	rows["people"] = []hatSql.Row{{"name": "Lin"}}
	rows["orders"] = []hatSql.Row{{"item": "Mouse"}}
	journal := &projectionJournalTail{tail: hatCache.CommandJournalTail{LastSequence: 2, Entries: []hatCache.CommandJournalRecord{
		{Sequence: 1, Request: hatCache.CacheCommandRequest{Command: "SET", Key: "people"}},
		{Sequence: 2, Request: hatCache.CacheCommandRequest{Command: "SET", Key: "orders"}},
	}}}
	run, err := frontier.RunOnce(context.Background(), journal, 16, map[string]*hatCache.SQLJournalProjectionRunner{"people": people, "orders": orders})
	if err != nil {
		t.Fatal(err)
	}
	if run.ThroughSequence != 2 || len(run.Runs) != 2 || frontier.Checkpoint() != 2 {
		t.Fatalf("run = %#v, checkpoint = %d", run, frontier.Checkpoint())
	}
	if got := journal.watermarkSequences; !reflect.DeepEqual(got, []uint64{0, 2}) {
		t.Fatalf("watermarks = %#v, want []uint64{0, 2}", got)
	}
}

func TestSQLProjectionRetentionFrontierRunOnceRejectsProtectedRunner(t *testing.T) {
	frontier, err := hatCache.NewSQLProjectionRetentionFrontier("analytics", []string{"people"})
	if err != nil {
		t.Fatal(err)
	}
	rows := map[string][]hatSql.Row{"people": {{"name": "Ada"}}}
	resolver := hatSql.SourceResolverFunc(func(_ string, key string) ([]hatSql.Row, error) { return hatSql.CloneRows(rows[key]), nil })
	views := hatSql.NewMaterializedViews()
	if _, err := views.Create(context.Background(), hatSql.MaterializedViewDefinition{Name: "people_view", Query: "FROM CACHE('people') SELECT name", Dependencies: []string{"people"}}, resolver, hatSql.QueryOptions{}); err != nil {
		t.Fatal(err)
	}
	runner, err := hatCache.NewSQLJournalProjectionRunnerWithOptions(views, resolver, hatSql.QueryOptions{}, hatCache.SQLJournalProjectionRunnerOptions{Incremental: hatSql.IncrementalProjectionRunnerOptions{Name: "people", Enabled: true}, ProtectJournalRetention: true})
	if err != nil {
		t.Fatal(err)
	}
	journal := &projectionJournalTail{}
	if _, err := frontier.RunOnce(context.Background(), journal, 16, map[string]*hatCache.SQLJournalProjectionRunner{"people": runner}); err == nil {
		t.Fatal("RunOnce() unexpectedly accepted an independently protected runner")
	}
	if len(journal.watermarkSequences) != 0 {
		t.Fatalf("rejected runner changed journal watermarks: %#v", journal.watermarkSequences)
	}
}

func TestSQLProjectionRetentionFrontierRejectsRegressionAndCanRemoveWatermark(t *testing.T) {
	frontier, err := hatCache.NewSQLProjectionRetentionFrontier("analytics", []string{"people", "orders"})
	if err != nil {
		t.Fatal(err)
	}
	journal := &projectionJournalTail{}
	if err := frontier.Commit(journal, map[string]uint64{"people": 12, "orders": 9}); err != nil {
		t.Fatal(err)
	}
	if err := frontier.Commit(journal, map[string]uint64{"people": 11, "orders": 10}); err == nil {
		t.Fatal("Commit() unexpectedly accepted source regression")
	}
	if frontier.Checkpoint() != 9 {
		t.Fatalf("failed commit changed checkpoint to %d", frontier.Checkpoint())
	}
	if !frontier.Remove(journal) || journal.removedWatermarkName != "analytics" {
		t.Fatalf("Remove() = %t, journal = %#v", frontier.Remove(journal), journal)
	}
}
