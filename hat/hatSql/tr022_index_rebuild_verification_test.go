package hatSql

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
)

func TestTR022IndexRebuildVerificationMarksSuccessfulVerification(t *testing.T) {
	queue := newTR022TestQueue(t)
	defer queue.Close()
	var verified atomic.Bool
	if _, err := queue.Enqueue(SQLIndexRebuildRequest{
		ID:   "verified-success",
		Name: "orders",
		Run: func(context.Context, SQLIndexRebuildProgressFunc) error {
			return nil
		},
		Verify: func(context.Context) error {
			verified.Store(true)
			return nil
		},
	}); err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}
	if err := queue.Flush(context.Background()); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	status, ok := queue.Status("verified-success")
	if !ok || status.State != SQLIndexRebuildSucceeded {
		t.Fatalf("status = %#v, found = %v, want succeeded", status, ok)
	}
	if !status.VerificationRequested || !status.Verified || !verified.Load() {
		t.Fatalf("verification status = %#v, callback = %v", status, verified.Load())
	}
}

func TestTR022IndexRebuildVerificationFailurePreventsSuccess(t *testing.T) {
	queue := newTR022TestQueue(t)
	defer queue.Close()
	wantErr := errors.New("index checksum mismatch")
	if _, err := queue.Enqueue(SQLIndexRebuildRequest{
		ID:   "verified-failure",
		Name: "orders",
		Run: func(context.Context, SQLIndexRebuildProgressFunc) error {
			return nil
		},
		Verify: func(context.Context) error {
			return wantErr
		},
	}); err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}
	if err := queue.Flush(context.Background()); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	status, ok := queue.Status("verified-failure")
	if !ok || status.State != SQLIndexRebuildFailed || !strings.Contains(status.Error, wantErr.Error()) {
		t.Fatalf("status = %#v, found = %v, want verification failure", status, ok)
	}
	if status.Verified {
		t.Fatal("failed verification status.Verified = true")
	}
}

func TestTR022IndexRebuildVerificationSkipsWhenRebuildFails(t *testing.T) {
	queue := newTR022TestQueue(t)
	defer queue.Close()
	var verified atomic.Bool
	if _, err := queue.Enqueue(SQLIndexRebuildRequest{
		ID:   "rebuild-failure",
		Name: "orders",
		Run: func(context.Context, SQLIndexRebuildProgressFunc) error {
			return errors.New("rebuild failed")
		},
		Verify: func(context.Context) error {
			verified.Store(true)
			return nil
		},
	}); err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}
	if err := queue.Flush(context.Background()); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	status, ok := queue.Status("rebuild-failure")
	if !ok || status.State != SQLIndexRebuildFailed {
		t.Fatalf("status = %#v, found = %v, want failed", status, ok)
	}
	if verified.Load() {
		t.Fatal("verification callback ran after rebuild failure")
	}
}

func newTR022TestQueue(t *testing.T) *SQLIndexRebuildQueue {
	t.Helper()
	queue, err := NewSQLIndexRebuildQueue(SQLIndexRebuildQueueOptions{Workers: 1})
	if err != nil {
		t.Fatalf("NewSQLIndexRebuildQueue() error = %v", err)
	}
	if err := queue.Start(context.Background()); err != nil {
		queue.Close()
		t.Fatalf("Start() error = %v", err)
	}
	return queue
}
