package hatSql

import (
	"context"
	"errors"
	"testing"
	"time"
)

func mu023AdmissionPolicy() SQLClusterAdmissionPolicy {
	return SQLClusterAdmissionPolicy{
		Serving: SQLClusterAdmissionPool{
			CPUUnits:    1,
			MemoryBytes: 100,
			MaxRunning:  1,
			MaxQueued:   1,
		},
		Maintenance: SQLClusterAdmissionPool{
			CPUUnits:    1,
			MemoryBytes: 100,
			MaxRunning:  1,
			MaxQueued:   1,
		},
	}
}

func mu023AdmissionRequest(class SQLClusterWorkClass, cpu, memory int64) SQLClusterAdmissionRequest {
	return SQLClusterAdmissionRequest{
		Cluster:     "analytics",
		Class:       class,
		CPUUnits:    cpu,
		MemoryBytes: memory,
	}
}

func TestSQLClusterAdmissionSeparatesServingAndMaintenancePools(t *testing.T) {
	admission, err := NewSQLClusterAdmission(SQLClusterAdmissionOptions{
		Default: mu023AdmissionPolicy(),
	})
	if err != nil {
		t.Fatal(err)
	}
	serving, err := admission.Acquire(context.Background(), mu023AdmissionRequest(SQLClusterWorkServing, 1, 50))
	if err != nil {
		t.Fatal(err)
	}
	defer serving.Release()
	if stats, ok := admission.Stats("analytics"); !ok || stats.Serving.CPUUsed != 1 || stats.Serving.MemoryUsed != 50 || stats.Serving.Running != 1 {
		t.Fatalf("serving usage stats = %#v/%v", stats, ok)
	}
	maintenance, err := admission.Acquire(context.Background(), mu023AdmissionRequest(SQLClusterWorkMaintenance, 1, 50))
	if err != nil {
		t.Fatalf("maintenance admission while serving is full: %v", err)
	}
	maintenance.Release()

	queuedResult := make(chan error, 1)
	go func() {
		queued, acquireErr := admission.Acquire(context.Background(), mu023AdmissionRequest(SQLClusterWorkServing, 1, 50))
		if queued != nil {
			queued.Release()
		}
		queuedResult <- acquireErr
	}()
	mu023WaitForQueuedServing(t, admission)
	if _, err := admission.Acquire(context.Background(), mu023AdmissionRequest(SQLClusterWorkServing, 1, 50)); !errors.Is(err, ErrSQLClusterAdmissionQueueFull) {
		t.Fatalf("full serving queue error = %v", err)
	}
	serving.Release()
	select {
	case err := <-queuedResult:
		if err != nil {
			t.Fatalf("queued serving admission error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("queued serving request was not granted after release")
	}
	stats, ok := admission.Stats("analytics")
	if !ok || stats.Serving.Running != 0 || stats.Serving.Queued != 0 || stats.Maintenance.Running != 0 {
		t.Fatalf("final cluster stats = %#v/%v", stats, ok)
	}
}

func TestSQLClusterAdmissionCancellationAndRequestBounds(t *testing.T) {
	admission, err := NewSQLClusterAdmission(SQLClusterAdmissionOptions{Default: mu023AdmissionPolicy()})
	if err != nil {
		t.Fatal(err)
	}
	active, err := admission.Acquire(context.Background(), mu023AdmissionRequest(SQLClusterWorkServing, 1, 50))
	if err != nil {
		t.Fatal(err)
	}
	defer active.Release()
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		lease, acquireErr := admission.Acquire(ctx, mu023AdmissionRequest(SQLClusterWorkServing, 1, 50))
		if lease != nil {
			lease.Release()
		}
		result <- acquireErr
	}()
	mu023WaitForQueuedServing(t, admission)
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled admission error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled admission did not return")
	}
	if stats, ok := admission.Stats("analytics"); !ok || stats.Serving.Queued != 0 {
		t.Fatalf("queue after cancellation = %#v/%v", stats, ok)
	}
	if _, err := admission.Acquire(context.Background(), mu023AdmissionRequest(SQLClusterWorkServing, 2, 1)); !errors.Is(err, ErrSQLClusterAdmissionRequestTooLarge) {
		t.Fatalf("oversized CPU request error = %v", err)
	}
	if _, err := admission.Acquire(context.Background(), mu023AdmissionRequest(SQLClusterWorkServing, 1, 101)); !errors.Is(err, ErrSQLClusterAdmissionRequestTooLarge) {
		t.Fatalf("oversized memory request error = %v", err)
	}
}

func TestSQLClusterAdmissionCloseAndExecuteRelease(t *testing.T) {
	admission, err := NewSQLClusterAdmission(SQLClusterAdmissionOptions{Default: mu023AdmissionPolicy()})
	if err != nil {
		t.Fatal(err)
	}
	active, err := admission.Acquire(context.Background(), mu023AdmissionRequest(SQLClusterWorkServing, 1, 50))
	if err != nil {
		t.Fatal(err)
	}
	queuedResult := make(chan error, 1)
	go func() {
		lease, acquireErr := admission.Acquire(context.Background(), mu023AdmissionRequest(SQLClusterWorkServing, 1, 50))
		if lease != nil {
			lease.Release()
		}
		queuedResult <- acquireErr
	}()
	mu023WaitForQueuedServing(t, admission)
	if err := admission.Close(); err != nil {
		t.Fatal(err)
	}
	active.Release()
	select {
	case err := <-queuedResult:
		if !errors.Is(err, ErrSQLClusterAdmissionClosed) {
			t.Fatalf("closed queued admission error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("close did not wake queued admission")
	}
	if _, err := admission.Acquire(context.Background(), mu023AdmissionRequest(SQLClusterWorkServing, 1, 1)); !errors.Is(err, ErrSQLClusterAdmissionClosed) {
		t.Fatalf("acquire after close error = %v", err)
	}

	open, err := NewSQLClusterAdmission(SQLClusterAdmissionOptions{Default: mu023AdmissionPolicy()})
	if err != nil {
		t.Fatal(err)
	}
	wantErr := errors.New("query failed")
	if err := open.Execute(context.Background(), mu023AdmissionRequest(SQLClusterWorkServing, 1, 1), func(context.Context) error {
		return wantErr
	}); !errors.Is(err, wantErr) {
		t.Fatalf("Execute error = %v", err)
	}
	stats, ok := open.Stats("analytics")
	if !ok || stats.Serving.Running != 0 || stats.Serving.Queued != 0 {
		t.Fatalf("Execute did not release = %#v/%v", stats, ok)
	}
}

func TestSQLClusterAdmissionRejectsInvalidConfigurationAndInput(t *testing.T) {
	if _, err := NewSQLClusterAdmission(SQLClusterAdmissionOptions{Default: SQLClusterAdmissionPolicy{
		Serving: SQLClusterAdmissionPool{CPUUnits: -1},
	}}); !errors.Is(err, ErrSQLClusterAdmissionInvalid) {
		t.Fatalf("negative CPU configuration error = %v", err)
	}
	if _, err := NewSQLClusterAdmission(SQLClusterAdmissionOptions{Default: mu023AdmissionPolicy(), Clusters: map[string]SQLClusterAdmissionPolicy{"bad\n": mu023AdmissionPolicy()}}); !errors.Is(err, ErrSQLClusterAdmissionInvalid) {
		t.Fatalf("unsafe cluster configuration error = %v", err)
	}
	if _, err := NewSQLClusterAdmission(SQLClusterAdmissionOptions{Default: mu023AdmissionPolicy(), MaxClusters: 1, Clusters: map[string]SQLClusterAdmissionPolicy{
		"one": mu023AdmissionPolicy(), "two": mu023AdmissionPolicy(),
	}}); !errors.Is(err, ErrSQLClusterAdmissionInvalid) {
		t.Fatalf("oversized cluster configuration error = %v", err)
	}
	admission, err := NewSQLClusterAdmission(SQLClusterAdmissionOptions{Default: mu023AdmissionPolicy()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admission.Acquire(context.Background(), SQLClusterAdmissionRequest{Cluster: "analytics", Class: "unknown", CPUUnits: 1}); !errors.Is(err, ErrSQLClusterAdmissionInvalid) {
		t.Fatalf("unknown class error = %v", err)
	}
	if err := admission.Execute(context.Background(), mu023AdmissionRequest(SQLClusterWorkServing, 1, 1), nil); !errors.Is(err, ErrSQLClusterAdmissionInvalid) {
		t.Fatalf("nil Execute callback error = %v", err)
	}
}

func mu023WaitForQueuedServing(t *testing.T, admission *SQLClusterAdmission) {
	t.Helper()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		stats, ok := admission.Stats("analytics")
		if ok && stats.Serving.Queued == 1 {
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("serving request did not enter queue: %#v/%v", stats, ok)
		case <-ticker.C:
		}
	}
}
