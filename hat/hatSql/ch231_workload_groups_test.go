package hatSql

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestC231SQLExecutionControlUsesClusterAdmission(t *testing.T) {
	admission, err := NewSQLClusterAdmission(SQLClusterAdmissionOptions{
		Default: SQLClusterAdmissionPolicy{
			Serving: SQLClusterAdmissionPool{
				CPUUnits:    1,
				MemoryBytes: 64,
				MaxRunning:  1,
				MaxQueued:   2,
			},
		},
	})
	if err != nil {
		t.Fatalf("NewSQLClusterAdmission() error = %v", err)
	}
	request := SQLClusterAdmissionRequest{
		Cluster:       "c231",
		Class:         SQLClusterWorkServing,
		WorkloadClass: SQLClusterWorkloadAdHoc,
		CPUUnits:      1,
		MemoryBytes:   32,
	}
	options := SQLQueryOptions{
		ClusterAdmission:        admission,
		ClusterAdmissionRequest: request,
	}

	first, firstCancel, err := newSQLExecutionControl(context.Background(), options)
	if err != nil {
		t.Fatalf("first newSQLExecutionControl() error = %v", err)
	}
	defer firstCancel()
	defer first.releaseAdmission()

	stats, ok := admission.Stats("c231")
	if !ok || stats.Serving.Running != 1 || stats.Serving.MemoryUsed != 32 {
		t.Fatalf("first reservation stats = %#v, ok=%v", stats, ok)
	}

	secondDone := make(chan error, 1)
	go func() {
		second, secondCancel, secondErr := newSQLExecutionControl(context.Background(), options)
		if secondErr == nil {
			second.releaseAdmission()
		}
		secondCancel()
		secondDone <- secondErr
	}()
	waitForC231Admission(t, admission, "c231", 1)

	first.releaseAdmission()
	select {
	case err := <-secondDone:
		if err != nil {
			t.Fatalf("queued SQL control error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("queued SQL control did not start after the first reservation released")
	}

	stats, ok = admission.Stats("c231")
	if !ok || stats.Serving.Running != 0 || stats.Serving.Queued != 0 || stats.Serving.MemoryUsed != 0 {
		t.Fatalf("released reservation stats = %#v, ok=%v", stats, ok)
	}
}

func TestC231SQLExecutionControlAdmissionCancellation(t *testing.T) {
	admission, err := NewSQLClusterAdmission(SQLClusterAdmissionOptions{
		Default: SQLClusterAdmissionPolicy{
			Serving: SQLClusterAdmissionPool{CPUUnits: 1, MaxRunning: 1, MaxQueued: 1},
		},
	})
	if err != nil {
		t.Fatalf("NewSQLClusterAdmission() error = %v", err)
	}
	request := SQLClusterAdmissionRequest{
		Cluster:     "c231-cancel",
		Class:       SQLClusterWorkServing,
		CPUUnits:    1,
		MemoryBytes: 1,
	}
	options := SQLQueryOptions{ClusterAdmission: admission, ClusterAdmissionRequest: request}
	first, firstCancel, err := newSQLExecutionControl(context.Background(), options)
	if err != nil {
		t.Fatalf("first newSQLExecutionControl() error = %v", err)
	}
	defer firstCancel()
	defer first.releaseAdmission()

	ctx, cancel := context.WithCancel(context.Background())
	secondDone := make(chan error, 1)
	go func() {
		second, secondCancel, secondErr := newSQLExecutionControl(ctx, options)
		if secondErr == nil {
			second.releaseAdmission()
		}
		secondCancel()
		secondDone <- secondErr
	}()
	waitForC231Admission(t, admission, "c231-cancel", 1)
	cancel()
	select {
	case err := <-secondDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("queued cancellation error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("queued SQL control did not observe cancellation")
	}
	stats, ok := admission.Stats("c231-cancel")
	if !ok || stats.Serving.Running != 1 || stats.Serving.Queued != 0 {
		t.Fatalf("canceled reservation stats = %#v, ok=%v", stats, ok)
	}
}

func TestC231SQLQueryReleasesAdmissionOnEveryEntryPoint(t *testing.T) {
	tests := []struct {
		name string
		run  func(SQLQueryOptions) error
	}{
		{
			name: "materialized",
			run: func(options SQLQueryOptions) error {
				_, err := ExecuteSQLQueryContext(context.Background(), "FROM VALUES (1) AS src(value) SELECT src.value", nil, options)
				return err
			},
		},
		{
			name: "rows",
			run: func(options SQLQueryOptions) error {
				return ExecuteSQLQueryRows(context.Background(), "FROM VALUES (1) AS src(value) SELECT src.value", nil, nil, options, func([]string, SQLRow) error {
					return nil
				})
			},
		},
		{
			name: "page",
			run: func(options SQLQueryOptions) error {
				_, err := ExecuteSQLQueryPage(context.Background(), "FROM VALUES (1) AS src(value) SELECT src.value", nil, nil, options, 1, "")
				return err
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			admission, err := NewSQLClusterAdmission(SQLClusterAdmissionOptions{
				Default: SQLClusterAdmissionPolicy{
					Serving: SQLClusterAdmissionPool{CPUUnits: 1, MemoryBytes: 64, MaxRunning: 1, MaxQueued: 1},
				},
			})
			if err != nil {
				t.Fatalf("NewSQLClusterAdmission() error = %v", err)
			}
			options := SQLQueryOptions{
				ClusterAdmission: admission,
				ClusterAdmissionRequest: SQLClusterAdmissionRequest{
					Cluster:     "c231-entry-" + test.name,
					Class:       SQLClusterWorkServing,
					CPUUnits:    1,
					MemoryBytes: 1,
				},
			}
			if err := test.run(options); err != nil {
				t.Fatalf("query error = %v", err)
			}
			stats, ok := admission.Stats("c231-entry-" + test.name)
			if !ok || stats.Serving.Running != 0 || stats.Serving.Queued != 0 || stats.Serving.MemoryUsed != 0 {
				t.Fatalf("post-query reservation stats = %#v, ok=%v", stats, ok)
			}
		})
	}
}

func waitForC231Admission(t *testing.T, admission *SQLClusterAdmission, cluster string, queued int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		stats, ok := admission.Stats(cluster)
		if ok && stats.Serving.Queued >= queued {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("admission did not reach queued=%d: %#v, ok=%v", queued, stats, ok)
		}
		time.Sleep(time.Millisecond)
	}
}
