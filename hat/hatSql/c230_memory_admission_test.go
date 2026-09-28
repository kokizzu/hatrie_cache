package hatSql

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestC230MemoryAdmissionWaitsAndReleases(t *testing.T) {
	admission, err := NewSQLMemoryAdmission(SQLMemoryAdmissionOptions{MaxBytes: 100, MaxPending: 2})
	if err != nil {
		t.Fatalf("NewSQLMemoryAdmission() error = %v", err)
	}
	holder, err := admission.Acquire(context.Background(), 100)
	if err != nil {
		t.Fatalf("initial Acquire() error = %v", err)
	}
	defer holder()

	done := make(chan error, 1)
	go func() {
		lease, acquireErr := admission.Acquire(context.Background(), 60)
		if acquireErr == nil {
			lease()
		}
		done <- acquireErr
	}()

	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	for admission.Stats().Pending != 1 {
		select {
		case err := <-done:
			t.Fatalf("waiter completed before release: %v", err)
		case <-deadline.C:
			t.Fatalf("waiter did not enter queue; stats = %#v", admission.Stats())
		default:
			time.Sleep(time.Millisecond)
		}
	}
	holder()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("waiter Acquire() error after release = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waiter was not released")
	}
	if stats := admission.Stats(); stats.ActiveBytes != 0 || stats.Pending != 0 {
		t.Fatalf("final stats = %#v, want no active bytes or pending requests", stats)
	}
}

func TestC230MemoryAdmissionCancellationDoesNotLeak(t *testing.T) {
	admission, err := NewSQLMemoryAdmission(SQLMemoryAdmissionOptions{MaxBytes: 100, MaxPending: 1})
	if err != nil {
		t.Fatalf("NewSQLMemoryAdmission() error = %v", err)
	}
	holder, err := admission.Acquire(context.Background(), 100)
	if err != nil {
		t.Fatalf("initial Acquire() error = %v", err)
	}
	defer holder()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, acquireErr := admission.Acquire(ctx, 50)
		done <- acquireErr
	}()
	deadline := time.Now().Add(2 * time.Second)
	for admission.Stats().Pending != 1 {
		if time.Now().After(deadline) {
			t.Fatalf("waiter did not enter queue; stats = %#v", admission.Stats())
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled Acquire() error = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("canceled waiter did not return")
	}
	if stats := admission.Stats(); stats.Pending != 0 || stats.ActiveBytes != 100 {
		t.Fatalf("stats after canceled waiter = %#v, want one holder and no waiter", stats)
	}
}

func TestC230MemoryAdmissionRejectsOversizedAndFullQueue(t *testing.T) {
	admission, err := NewSQLMemoryAdmission(SQLMemoryAdmissionOptions{MaxBytes: 100, MaxPending: 1})
	if err != nil {
		t.Fatalf("NewSQLMemoryAdmission() error = %v", err)
	}
	if _, err := admission.Acquire(context.Background(), 101); !errors.Is(err, ErrSQLMemoryAdmissionRequestTooLarge) {
		t.Fatalf("oversized Acquire() error = %v, want %v", err, ErrSQLMemoryAdmissionRequestTooLarge)
	}
	holder, err := admission.Acquire(context.Background(), 100)
	if err != nil {
		t.Fatalf("initial Acquire() error = %v", err)
	}
	defer holder()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	queued := make(chan error, 1)
	go func() {
		_, acquireErr := admission.Acquire(ctx, 50)
		queued <- acquireErr
	}()
	deadline := time.Now().Add(2 * time.Second)
	for admission.Stats().Pending != 1 {
		if time.Now().After(deadline) {
			t.Fatalf("waiter did not enter queue; stats = %#v", admission.Stats())
		}
		time.Sleep(time.Millisecond)
	}
	if _, err := admission.Acquire(context.Background(), 25); !errors.Is(err, ErrSQLMemoryAdmissionQueueFull) {
		t.Fatalf("full queue Acquire() error = %v, want %v", err, ErrSQLMemoryAdmissionQueueFull)
	}
	cancel()
	if err := <-queued; !errors.Is(err, context.Canceled) {
		t.Fatalf("queued cancellation error = %v, want context.Canceled", err)
	}
}

type c230MemoryAdmissionResolver struct {
	calls atomic.Int64
}

func (resolver *c230MemoryAdmissionResolver) ResolveSQLSource(string, string) ([]SQLRow, error) {
	resolver.calls.Add(1)
	return []SQLRow{{"id": int64(1)}}, nil
}

func TestC230QueryWaitsBeforeSourceExecution(t *testing.T) {
	admission, err := NewSQLMemoryAdmission(SQLMemoryAdmissionOptions{MaxBytes: 100, MaxPending: 1})
	if err != nil {
		t.Fatalf("NewSQLMemoryAdmission() error = %v", err)
	}
	holder, err := admission.Acquire(context.Background(), 100)
	if err != nil {
		t.Fatalf("initial Acquire() error = %v", err)
	}
	defer holder()
	resolver := new(c230MemoryAdmissionResolver)
	done := make(chan error, 1)
	go func() {
		_, queryErr := ExecuteSQLQueryContext(context.Background(), "FROM CACHE('events') SELECT id", resolver, SQLQueryOptions{
			MemoryAdmission:        admission,
			MemoryReservationBytes: 50,
		})
		done <- queryErr
	}()
	deadline := time.Now().Add(2 * time.Second)
	for admission.Stats().Pending != 1 {
		if time.Now().After(deadline) {
			t.Fatalf("query did not enter memory queue; stats = %#v", admission.Stats())
		}
		time.Sleep(time.Millisecond)
	}
	if got := resolver.calls.Load(); got != 0 {
		t.Fatalf("resolver calls while query waited = %d, want 0", got)
	}
	holder()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("query error after memory release = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("query did not complete after memory release")
	}
	if got := resolver.calls.Load(); got != 1 {
		t.Fatalf("resolver calls after query completion = %d, want 1", got)
	}
}

func BenchmarkC230MemoryAdmissionAcquireRelease(b *testing.B) {
	admission, err := NewSQLMemoryAdmission(SQLMemoryAdmissionOptions{MaxBytes: 1 << 30, MaxPending: 1})
	if err != nil {
		b.Fatalf("NewSQLMemoryAdmission() error = %v", err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		lease, acquireErr := admission.Acquire(context.Background(), 1024)
		if acquireErr != nil {
			b.Fatalf("Acquire() error = %v", acquireErr)
		}
		lease()
	}
}
