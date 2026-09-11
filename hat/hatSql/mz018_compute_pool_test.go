package hatSql_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"hatrie_cache/hat/hatSql"
)

func TestSQLQueryManagerComputePoolBoundsConcurrentQueries(t *testing.T) {
	manager := hatSql.NewSQLQueryManagerWithOptions(hatSql.SQLQueryManagerOptions{
		ComputeWorkers:       2,
		ComputeQueueCapacity: 1,
	})
	defer func() {
		if err := manager.Close(); err != nil {
			t.Fatalf("close manager: %v", err)
		}
	}()

	var started atomic.Int32
	entered := make(chan struct{}, 3)
	release := make(chan struct{})
	resolver := hatSql.SourceResolverFunc(func(string, string) ([]hatSql.Row, error) {
		started.Add(1)
		entered <- struct{}{}
		<-release
		return []hatSql.Row{{"id": int64(1)}}, nil
	})

	type execution struct {
		result hatSql.QueryResult
		err    error
	}
	results := make(chan execution, 3)
	for _, queryID := range []string{"compute-1", "compute-2", "compute-3"} {
		queryID := queryID
		go func() {
			result, err := manager.Execute(context.Background(), "SELECT id FROM CACHE('items')", resolver, nil, hatSql.QueryOptions{QueryID: queryID})
			results <- execution{result: result, err: err}
		}()
	}

	for range 2 {
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("compute pool did not start both worker queries")
		}
	}
	if got := started.Load(); got != 2 {
		t.Fatalf("started queries = %d, want 2", got)
	}
	select {
	case <-entered:
		t.Fatal("queued query bypassed compute worker bound")
	default:
	}

	close(release)
	for range 3 {
		select {
		case result := <-results:
			if result.err != nil {
				t.Fatalf("managed query failed: %v", result.err)
			}
			if len(result.result.Rows) != 1 || result.result.Rows[0]["id"] != int64(1) {
				t.Fatalf("unexpected result: %#v", result.result.Rows)
			}
		case <-time.After(time.Second):
			t.Fatal("compute pool query did not finish")
		}
	}
	if got := started.Load(); got != 3 {
		t.Fatalf("started queries = %d, want 3 after release", got)
	}
}

func TestSQLQueryManagerComputePoolCloseRejectsNewQueries(t *testing.T) {
	manager := hatSql.NewSQLQueryManagerWithOptions(hatSql.SQLQueryManagerOptions{
		ComputeWorkers: 1,
	})
	if err := manager.Close(); err != nil {
		t.Fatalf("close manager: %v", err)
	}
	_, err := manager.Execute(context.Background(), "FROM VALUES (1) AS item(value) SELECT value", nil, nil, hatSql.QueryOptions{QueryID: "after-close"})
	if !errors.Is(err, hatSql.ErrSQLQueryManagerClosed) {
		t.Fatalf("execute after close error = %v, want %v", err, hatSql.ErrSQLQueryManagerClosed)
	}
}

func TestSQLQueryManagerComputePoolCloseDrainsRunningQuery(t *testing.T) {
	manager := hatSql.NewSQLQueryManagerWithOptions(hatSql.SQLQueryManagerOptions{
		ComputeWorkers: 1,
	})
	started := make(chan struct{})
	release := make(chan struct{})
	resolver := hatSql.SourceResolverFunc(func(string, string) ([]hatSql.Row, error) {
		close(started)
		<-release
		return []hatSql.Row{{"id": int64(1)}}, nil
	})
	resultCh := make(chan struct {
		result hatSql.QueryResult
		err    error
	}, 1)
	go func() {
		result, err := manager.Execute(context.Background(), "SELECT id FROM CACHE('items')", resolver, nil, hatSql.QueryOptions{QueryID: "drain"})
		resultCh <- struct {
			result hatSql.QueryResult
			err    error
		}{result: result, err: err}
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("compute query did not start")
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- manager.Close() }()
	select {
	case err := <-closeDone:
		t.Fatalf("close returned before running query drained: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	select {
	case execution := <-resultCh:
		if execution.err != nil {
			t.Fatalf("drained query failed: %v", execution.err)
		}
		if len(execution.result.Rows) != 1 {
			t.Fatalf("rows = %d, want 1", len(execution.result.Rows))
		}
	case <-time.After(time.Second):
		t.Fatal("drained query did not finish")
	}
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("close manager: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("close did not finish after query drained")
	}
}

func TestSQLQueryManagerRejectsInvalidComputePoolOptions(t *testing.T) {
	for name, options := range map[string]hatSql.SQLQueryManagerOptions{
		"negative workers": {ComputeWorkers: -1},
		"negative queue":   {ComputeWorkers: 1, ComputeQueueCapacity: -1},
	} {
		t.Run(name, func(t *testing.T) {
			manager := hatSql.NewSQLQueryManagerWithOptions(options)
			defer manager.Close()
			_, err := manager.Execute(context.Background(), "FROM VALUES (1) AS item(value) SELECT value", nil, nil, hatSql.QueryOptions{QueryID: name})
			if err == nil {
				t.Fatal("invalid compute pool options were accepted")
			}
		})
	}
}
