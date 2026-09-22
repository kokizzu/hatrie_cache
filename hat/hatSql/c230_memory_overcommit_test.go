package hatSql

import (
	"context"
	"errors"
	"testing"
	"time"
)

func c230QueryAdmission(t *testing.T) *SQLClusterAdmission {
	t.Helper()
	admission, err := NewSQLClusterAdmission(SQLClusterAdmissionOptions{
		Default: SQLClusterAdmissionPolicy{
			Serving: SQLClusterAdmissionPool{
				CPUUnits:    1,
				MemoryBytes: 64,
				MaxRunning:  1,
				MaxQueued:   2,
			},
			Maintenance: SQLClusterAdmissionPool{
				CPUUnits:    1,
				MemoryBytes: 64,
				MaxRunning:  1,
				MaxQueued:   2,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return admission
}

func c230QueryAdmissionRequest() SQLClusterAdmissionRequest {
	return SQLClusterAdmissionRequest{
		Cluster:     "c230",
		Class:       SQLClusterWorkServing,
		CPUUnits:    1,
		MemoryBytes: 64,
	}
}

func TestC230SQLQueryWaitsForMemoryAdmissionBeforeExecution(t *testing.T) {
	admission := c230QueryAdmission(t)
	held, err := admission.Acquire(context.Background(), c230QueryAdmissionRequest())
	if err != nil {
		t.Fatal(err)
	}
	defer held.Release()

	resultCh := make(chan struct {
		result SQLQueryResult
		err    error
	}, 1)
	go func() {
		result, queryErr := ExecuteSQLQueryContext(context.Background(),
			"FROM VALUES (1) AS src(value) SELECT src.value", nil,
			SQLQueryOptions{
				ClusterAdmission:        admission,
				ClusterAdmissionRequest: c230QueryAdmissionRequest(),
			})
		resultCh <- struct {
			result SQLQueryResult
			err    error
		}{result: result, err: queryErr}
	}()

	c230WaitForQueuedQuery(t, admission)
	select {
	case got := <-resultCh:
		t.Fatalf("query completed before memory was released: result=%#v err=%v", got.result, got.err)
	case <-time.After(20 * time.Millisecond):
	}

	held.Release()
	select {
	case got := <-resultCh:
		if got.err != nil {
			t.Fatalf("queued query error = %v", got.err)
		}
		if len(got.result.Rows) != 1 || got.result.Rows[0]["value"] != int64(1) {
			t.Fatalf("queued query result = %#v", got.result.Rows)
		}
	case <-time.After(time.Second):
		t.Fatal("queued query did not run after memory was released")
	}
}

func TestC230SQLQueryAdmissionHonorsCancellation(t *testing.T) {
	admission := c230QueryAdmission(t)
	held, err := admission.Acquire(context.Background(), c230QueryAdmissionRequest())
	if err != nil {
		t.Fatal(err)
	}
	defer held.Release()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	resultCh := make(chan error, 1)
	go func() {
		_, queryErr := ExecuteSQLQueryContext(ctx,
			"FROM VALUES (1) AS src(value) SELECT src.value", nil,
			SQLQueryOptions{
				ClusterAdmission:        admission,
				ClusterAdmissionRequest: c230QueryAdmissionRequest(),
			})
		resultCh <- queryErr
	}()

	c230WaitForQueuedQuery(t, admission)
	cancel()
	select {
	case queryErr := <-resultCh:
		if !errors.Is(queryErr, context.Canceled) {
			t.Fatalf("canceled queued query error = %v, want context.Canceled", queryErr)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled queued query did not return")
	}
	stats, ok := admission.Stats("c230")
	if !ok || stats.Serving.Queued != 0 {
		t.Fatalf("queue after cancellation = %#v/%v", stats, ok)
	}
}

func TestC230SQLRowsWaitsForMemoryAdmission(t *testing.T) {
	admission := c230QueryAdmission(t)
	held, err := admission.Acquire(context.Background(), c230QueryAdmissionRequest())
	if err != nil {
		t.Fatal(err)
	}
	defer held.Release()

	done := make(chan struct {
		rows int
		err  error
	}, 1)
	go func() {
		rows := 0
		queryErr := ExecuteSQLQueryRows(context.Background(),
			"FROM VALUES (1) AS src(value) SELECT src.value", nil, nil,
			SQLQueryOptions{
				ClusterAdmission:        admission,
				ClusterAdmissionRequest: c230QueryAdmissionRequest(),
			}, func([]string, SQLRow) error {
				rows++
				return nil
			})
		done <- struct {
			rows int
			err  error
		}{rows: rows, err: queryErr}
	}()

	c230WaitForQueuedQuery(t, admission)
	held.Release()
	select {
	case got := <-done:
		if got.err != nil || got.rows != 1 {
			t.Fatalf("streamed admitted query rows=%d err=%v, want one row and nil error", got.rows, got.err)
		}
	case <-time.After(time.Second):
		t.Fatal("queued streamed query did not run after memory was released")
	}
}

func TestC230SQLPageWaitsForMemoryAdmission(t *testing.T) {
	admission := c230QueryAdmission(t)
	held, err := admission.Acquire(context.Background(), c230QueryAdmissionRequest())
	if err != nil {
		t.Fatal(err)
	}
	defer held.Release()

	resultCh := make(chan struct {
		result SQLQueryResult
		err    error
	}, 1)
	go func() {
		result, queryErr := ExecuteSQLQueryPage(context.Background(),
			"FROM VALUES (1) AS src(value) SELECT src.value", nil, nil,
			SQLQueryOptions{
				ClusterAdmission:        admission,
				ClusterAdmissionRequest: c230QueryAdmissionRequest(),
			}, 10, "")
		resultCh <- struct {
			result SQLQueryResult
			err    error
		}{result: result, err: queryErr}
	}()

	c230WaitForQueuedQuery(t, admission)
	held.Release()
	select {
	case got := <-resultCh:
		if got.err != nil {
			t.Fatalf("paged admitted query error = %v", got.err)
		}
		if len(got.result.Rows) != 1 || got.result.Rows[0]["value"] != int64(1) {
			t.Fatalf("paged admitted query result = %#v", got.result.Rows)
		}
	case <-time.After(time.Second):
		t.Fatal("queued paged query did not run after memory was released")
	}
}

func c230WaitForQueuedQuery(t *testing.T, admission *SQLClusterAdmission) {
	t.Helper()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		stats, ok := admission.Stats("c230")
		if ok && stats.Serving.Queued == 1 {
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("query did not enter admission queue: %#v/%v", stats, ok)
		case <-ticker.C:
		}
	}
}
