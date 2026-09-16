package hatCache_test

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"hatrie_cache/hat/hatCache"
	"hatrie_cache/hat/hatSql"
)

type c204ProjectionJournal struct {
	tail hatCache.CommandJournalTail
}

func (journal *c204ProjectionJournal) Tail(uint64, int) (hatCache.CommandJournalTail, error) {
	return journal.tail, nil
}

func TestC204SQLJournalProjectionRunnerPropagatesIdempotencyKeys(t *testing.T) {
	rows := map[string][]hatSql.Row{"people": {{"name": "Ada"}}}
	resolver := hatSql.SourceResolverFunc(func(_ string, key string) ([]hatSql.Row, error) {
		return hatSql.CloneRows(rows[key]), nil
	})
	views := hatSql.NewMaterializedViews()
	if _, err := views.Create(context.Background(), hatSql.MaterializedViewDefinition{
		Name:         "people_view",
		Query:        "FROM CACHE('people') SELECT name",
		Dependencies: []string{"people"},
	}, resolver, hatSql.QueryOptions{}); err != nil {
		t.Fatal(err)
	}
	runner, err := hatCache.NewSQLJournalProjectionRunner(views, resolver, hatSql.QueryOptions{}, hatSql.IncrementalProjectionRunnerOptions{
		Name:    "people",
		Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	rows["people"] = []hatSql.Row{{"name": "Lin"}}
	journal := &c204ProjectionJournal{tail: hatCache.CommandJournalTail{
		LastSequence: 3,
		Entries: []hatCache.CommandJournalRecord{
			{Sequence: 1, Request: hatCache.CacheCommandRequest{Command: "SET", Key: "people", IdempotencyKey: " async-1 "}},
			{Sequence: 2, Request: hatCache.CacheCommandRequest{Command: "SET", Key: "people"}},
			{Sequence: 3, Request: hatCache.CacheCommandRequest{Command: "SET", Key: "people", IdempotencyKey: "async-2"}},
		},
	}}
	run, err := runner.RunOnce(context.Background(), journal, 16)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(run.IdempotencyKeys, []string{"async-1", "async-2"}) {
		t.Fatalf("projection idempotency keys = %#v, want [async-1 async-2]", run.IdempotencyKeys)
	}
	if len(run.Refreshed) != 1 || run.ThroughSequence != 3 || !reflect.DeepEqual(run.Refreshed[0].IdempotencyKeys, []string{"async-1", "async-2"}) {
		t.Fatalf("projection run = %#v, want one refreshed view through sequence 3", run)
	}
	view, ok := views.Get("people_view")
	if !ok || !reflect.DeepEqual(view.Status.IdempotencyKeys, []string{"async-1", "async-2"}) {
		t.Fatalf("materialized view idempotency keys = %#v, want [async-1 async-2]", view.Status.IdempotencyKeys)
	}
	view.Status.IdempotencyKeys[0] = "caller-mutation"
	view, ok = views.Get("people_view")
	if !ok || !reflect.DeepEqual(view.Status.IdempotencyKeys, []string{"async-1", "async-2"}) {
		t.Fatalf("materialized view idempotency keys were not cloned: %#v", view.Status.IdempotencyKeys)
	}
	if _, err := views.RefreshChanged(context.Background(), []string{"people"}, resolver, hatSql.QueryOptions{}); err != nil {
		t.Fatal(err)
	}
	view, ok = views.Get("people_view")
	if !ok || len(view.Status.IdempotencyKeys) != 0 {
		t.Fatalf("legacy refresh retained idempotency keys: %#v", view.Status.IdempotencyKeys)
	}
}

func TestC204ProjectionRunnerRejectsMisalignedIdempotencyKeys(t *testing.T) {
	resolver := hatSql.SourceResolverFunc(func(_ string, _ string) ([]hatSql.Row, error) {
		return []hatSql.Row{{"name": "Ada"}}, nil
	})
	views := hatSql.NewMaterializedViews()
	if _, err := views.Create(context.Background(), hatSql.MaterializedViewDefinition{
		Name:         "people_view",
		Query:        "FROM CACHE('people') SELECT name",
		Dependencies: []string{"people"},
	}, resolver, hatSql.QueryOptions{}); err != nil {
		t.Fatal(err)
	}
	runner, err := hatSql.NewIncrementalProjectionRunner(views, resolver, hatSql.QueryOptions{}, hatSql.IncrementalProjectionRunnerOptions{
		Name:    "people",
		Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = runner.ApplyWithIdempotencyKeys(context.Background(), []hatSql.ProjectionChange{{Sequence: 1, Dependency: "people"}}, []string{"one", "two"})
	if err == nil || !strings.Contains(err.Error(), "must match change count") {
		t.Fatalf("misaligned idempotency keys error = %v, want change-count validation", err)
	}
}
