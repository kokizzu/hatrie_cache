package hatSql_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"hatrie_cache/hat/hatSql"
)

func TestSQLQueryManagerCancelsWithOperatorReason(t *testing.T) {
	manager := hatSql.NewSQLQueryManager(4)
	started := make(chan struct{})
	release := make(chan struct{})
	var startOnce sync.Once
	resolver := hatSql.SourceResolverFunc(func(string, string) ([]hatSql.Row, error) {
		startOnce.Do(func() { close(started) })
		<-release
		return []hatSql.Row{{"id": int64(1)}}, nil
	})

	type execution struct {
		result hatSql.QueryResult
		err    error
	}
	done := make(chan execution, 1)
	go func() {
		result, err := manager.Execute(context.Background(), "SELECT id FROM CACHE('items')", resolver, nil, hatSql.QueryOptions{QueryID: "job-1"})
		done <- execution{result: result, err: err}
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("query did not start")
	}

	status, ok := manager.Status("job-1")
	if !ok || status.State != hatSql.SQLQueryStateRunning {
		t.Fatalf("running status = %#v/%v", status, ok)
	}
	canceled, err := manager.Cancel("job-1", "operator cleanup")
	if err != nil {
		t.Fatalf("Cancel() error = %v", err)
	}
	if canceled.State != hatSql.SQLQueryStateCancelRequested || canceled.CancelReason != "operator cleanup" {
		t.Fatalf("cancel status = %#v", canceled)
	}
	close(release)

	select {
	case result := <-done:
		if !errors.Is(result.err, context.Canceled) {
			t.Fatalf("Execute() error = %v, want context canceled", result.err)
		}
		var canceledErr *hatSql.SQLQueryCanceledError
		if !errors.As(result.err, &canceledErr) || canceledErr.Reason != "operator cleanup" || canceledErr.QueryID != "job-1" {
			t.Fatalf("Execute() cancellation error = %#v, want operator reason", result.err)
		}
		if result.result.QueryID != "job-1" {
			t.Fatalf("result query ID = %q, want job-1", result.result.QueryID)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled query did not finish")
	}

	status, ok = manager.Status("job-1")
	if !ok || status.State != hatSql.SQLQueryStateCanceled || status.CancelReason != "operator cleanup" {
		t.Fatalf("final status = %#v/%v", status, ok)
	}
	history := manager.History()
	if len(history) != 1 || history[0].QueryID != "job-1" || history[0].State != hatSql.SQLQueryStateCanceled {
		t.Fatalf("history = %#v", history)
	}
}

func TestSQLQueryManagerGeneratesIDsAndBoundsHistory(t *testing.T) {
	manager := hatSql.NewSQLQueryManager(1)
	resolver := hatSql.SourceResolverFunc(func(string, string) ([]hatSql.Row, error) {
		return []hatSql.Row{{"id": int64(1)}}, nil
	})
	for range 2 {
		result, err := manager.Execute(context.Background(), "SELECT id FROM CACHE('items')", resolver, nil, hatSql.QueryOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if !strings.HasPrefix(result.QueryID, "query-") {
			t.Fatalf("generated query ID = %q", result.QueryID)
		}
	}
	if history := manager.History(); len(history) != 1 || history[0].State != hatSql.SQLQueryStateSucceeded {
		t.Fatalf("bounded history = %#v", history)
	}
}

func TestSQLQueryManagerRejectsDuplicateIDsAndInvalidCancellation(t *testing.T) {
	manager := hatSql.NewSQLQueryManager(2)
	started := make(chan struct{})
	release := make(chan struct{})
	var startOnce sync.Once
	resolver := hatSql.SourceResolverFunc(func(string, string) ([]hatSql.Row, error) {
		startOnce.Do(func() { close(started) })
		<-release
		return []hatSql.Row{{"id": int64(1)}}, nil
	})
	done := make(chan error, 1)
	go func() {
		_, err := manager.Execute(context.Background(), "SELECT id FROM CACHE('items')", resolver, nil, hatSql.QueryOptions{QueryID: "duplicate"})
		done <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("query did not start")
	}
	if _, err := manager.Execute(context.Background(), "SELECT id FROM CACHE('items')", resolver, nil, hatSql.QueryOptions{QueryID: "duplicate"}); hatSql.ErrorCodeOf(err) != hatSql.ErrorConflict {
		t.Fatalf("duplicate Execute() error = %v, code = %s", err, hatSql.ErrorCodeOf(err))
	}
	if _, err := manager.Cancel("duplicate", " "); err == nil {
		t.Fatal("Cancel() with blank reason succeeded")
	}
	if _, err := manager.Cancel("duplicate", strings.Repeat("x", 513)); err == nil {
		t.Fatal("Cancel() with oversized reason succeeded")
	}
	if _, err := manager.Cancel("duplicate", "test"); err != nil {
		t.Fatalf("Cancel() error = %v", err)
	}
	if _, err := manager.Cancel("duplicate", "second reason"); err != nil {
		t.Fatalf("idempotent Cancel() error = %v", err)
	}
	close(release)
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("managed duplicate query error = %v, want context canceled", err)
	}
	if _, err := manager.Cancel("duplicate", "after completion"); err == nil {
		t.Fatal("Cancel() after completion succeeded")
	}
}
