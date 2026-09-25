package hatSql_test

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"hatrie_cache/hat/hatSql"
)

func TestCH033DistributedQueryFansOutAndMergesInShardOrder(t *testing.T) {
	shards := []hatSql.SQLDistributedQueryShard{
		{ID: "west", Resolver: hatSql.SQLSourceResolverFunc(func(string, string) ([]hatSql.Row, error) {
			return []hatSql.Row{{"id": int64(2), "region": "west"}}, nil
		})},
		{ID: "east", Resolver: hatSql.SQLSourceResolverFunc(func(string, string) ([]hatSql.Row, error) {
			return []hatSql.Row{{"id": int64(1), "region": "east"}}, nil
		})},
	}

	result, err := hatSql.ExecuteSQLDistributedQuery(
		context.Background(),
		"FROM CACHE('users') SELECT id, region",
		shards,
		nil,
		hatSql.SQLQueryOptions{},
		hatSql.SQLDistributedQueryOptions{MaxConcurrency: 2},
	)
	if err != nil {
		t.Fatalf("ExecuteSQLDistributedQuery() error = %v", err)
	}
	if !reflect.DeepEqual(result.Columns, []string{"id", "region"}) {
		t.Fatalf("columns = %#v", result.Columns)
	}
	want := []hatSql.Row{
		{"id": int64(2), "region": "west"},
		{"id": int64(1), "region": "east"},
	}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("rows = %#v, want %#v", result.Rows, want)
	}
}

func TestCH033DistributedQueryBoundsConcurrentShardExecution(t *testing.T) {
	var active atomic.Int32
	var maximum atomic.Int32
	started := make(chan struct{}, 4)
	release := make(chan struct{})
	resolver := hatSql.SQLContextSourceResolverFunc(func(ctx context.Context, _ string, _ string) ([]hatSql.Row, error) {
		current := active.Add(1)
		defer active.Add(-1)
		for {
			observed := maximum.Load()
			if current <= observed || maximum.CompareAndSwap(observed, current) {
				break
			}
		}
		started <- struct{}{}
		select {
		case <-release:
			return []hatSql.Row{{"id": int64(current)}}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})
	shards := make([]hatSql.SQLDistributedQueryShard, 4)
	for index := range shards {
		shards[index] = hatSql.SQLDistributedQueryShard{ID: string(rune('a' + index)), Resolver: resolver}
	}
	done := make(chan error, 1)
	go func() {
		_, err := hatSql.ExecuteSQLDistributedQuery(
			context.Background(),
			"FROM CACHE('users') SELECT id",
			shards,
			nil,
			hatSql.SQLQueryOptions{},
			hatSql.SQLDistributedQueryOptions{MaxConcurrency: 2},
		)
		done <- err
	}()
	for index := 0; index < 2; index++ {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for bounded workers")
		}
	}
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("ExecuteSQLDistributedQuery() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for distributed query")
	}
	if got := maximum.Load(); got > 2 {
		t.Fatalf("maximum concurrent shards = %d, want <= 2", got)
	}
}

func TestCH033DistributedQueryUsesCustomMergeAndGlobalRowLimit(t *testing.T) {
	resolver := hatSql.SQLSourceResolverFunc(func(string, string) ([]hatSql.Row, error) {
		return []hatSql.Row{{"id": int64(1)}}, nil
	})
	shards := []hatSql.SQLDistributedQueryShard{
		{ID: "a", Resolver: resolver},
		{ID: "b", Resolver: resolver},
	}
	result, err := hatSql.ExecuteSQLDistributedQuery(
		context.Background(),
		"FROM CACHE('users') SELECT id",
		shards,
		nil,
		hatSql.SQLQueryOptions{},
		hatSql.SQLDistributedQueryOptions{
			MaxRows: 2,
			Merge: func(results []hatSql.SQLDistributedQueryShardResult) (hatSql.SQLQueryResult, error) {
				return hatSql.SQLQueryResult{Columns: []string{"shards"}, Rows: []hatSql.Row{{"shards": int64(len(results))}}}, nil
			},
		},
	)
	if err != nil {
		t.Fatalf("custom merge error = %v", err)
	}
	if !reflect.DeepEqual(result.Rows, []hatSql.Row{{"shards": int64(2)}}) {
		t.Fatalf("custom merge rows = %#v", result.Rows)
	}
	_, err = hatSql.ExecuteSQLDistributedQuery(
		context.Background(),
		"FROM CACHE('users') SELECT id",
		shards,
		nil,
		hatSql.SQLQueryOptions{},
		hatSql.SQLDistributedQueryOptions{MaxRows: 1},
	)
	if !errors.Is(err, hatSql.ErrSQLDistributedQueryMaxRows) {
		t.Fatalf("row limit error = %v, want ErrSQLDistributedQueryMaxRows", err)
	}
}

func TestCH033DistributedQueryValidatesShardInput(t *testing.T) {
	resolver := hatSql.SQLSourceResolverFunc(func(string, string) ([]hatSql.Row, error) { return nil, nil })
	for name, shards := range map[string][]hatSql.SQLDistributedQueryShard{
		"empty":   nil,
		"missing": {{ID: "", Resolver: resolver}},
		"nil":     {{ID: "a"}},
		"duplicate": {
			{ID: "a", Resolver: resolver},
			{ID: " a ", Resolver: resolver},
		},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := hatSql.ExecuteSQLDistributedQuery(context.Background(), "FROM CACHE('users') SELECT id", shards, nil, hatSql.SQLQueryOptions{}, hatSql.SQLDistributedQueryOptions{})
			if err == nil {
				t.Fatal("ExecuteSQLDistributedQuery() succeeded for invalid shards")
			}
		})
	}
}

func TestCH033DistributedQueryPropagatesCanceledContext(t *testing.T) {
	resolver := hatSql.SQLSourceResolverFunc(func(string, string) ([]hatSql.Row, error) { return nil, nil })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := hatSql.ExecuteSQLDistributedQuery(
		ctx,
		"FROM CACHE('users') SELECT id",
		[]hatSql.SQLDistributedQueryShard{{ID: "a", Resolver: resolver}},
		nil,
		hatSql.SQLQueryOptions{},
		hatSql.SQLDistributedQueryOptions{},
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled context error = %v, want context.Canceled", err)
	}
}
