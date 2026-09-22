package hatSql

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

var m237MaterializedViewSink MaterializedView

func TestM237GetOrHydrateSharesFirstHydration(t *testing.T) {
	views := NewMaterializedViews()
	definition := MaterializedViewDefinition{
		Name:         "people",
		Query:        "FROM CACHE('people') SELECT id, name",
		Dependencies: []string{"people"},
	}
	if _, err := views.CreateCold(definition); err != nil {
		t.Fatalf("CreateCold() error = %v", err)
	}

	started := make(chan struct{})
	release := make(chan struct{})
	var resolverCalls atomic.Int32
	resolver := SourceResolverFunc(func(name, key string) ([]Row, error) {
		if name != "CACHE" || key != "people" {
			t.Fatalf("resolver source = %q/%q, want CACHE/people", name, key)
		}
		resolverCalls.Add(1)
		close(started)
		<-release
		return []Row{{"id": int64(1), "name": "Ada"}}, nil
	})

	type outcome struct {
		view MaterializedView
		err  error
	}
	firstDone := make(chan outcome, 1)
	go func() {
		view, err := views.GetOrHydrate(context.Background(), definition.Name, resolver, QueryOptions{})
		firstDone <- outcome{view: view, err: err}
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first reader did not start hydration")
	}

	secondDone := make(chan outcome, 1)
	go func() {
		view, err := views.GetOrHydrate(context.Background(), definition.Name, resolver, QueryOptions{})
		secondDone <- outcome{view: view, err: err}
	}()
	select {
	case <-secondDone:
		t.Fatal("second reader returned before shared hydration completed")
	case <-time.After(20 * time.Millisecond):
	}

	close(release)
	for name, done := range map[string]chan outcome{"first": firstDone, "second": secondDone} {
		select {
		case result := <-done:
			if result.err != nil {
				t.Fatalf("%s reader error = %v", name, result.err)
			}
			if result.view.Status.HydrationState != MaterializedViewHydrationStateReady {
				t.Fatalf("%s reader state = %q, want ready", name, result.view.Status.HydrationState)
			}
			if len(result.view.Result.Rows) != 1 || result.view.Result.Rows[0]["id"] != int64(1) {
				t.Fatalf("%s reader rows = %#v, want one row with id 1", name, result.view.Result.Rows)
			}
		case <-time.After(time.Second):
			t.Fatalf("%s reader did not finish", name)
		}
	}
	if got := resolverCalls.Load(); got != 1 {
		t.Fatalf("resolver calls = %d, want one shared hydration", got)
	}
}

func TestM237GetOrHydrateWaitingReaderCanCancel(t *testing.T) {
	views := NewMaterializedViews()
	definition := MaterializedViewDefinition{
		Name:         "people",
		Query:        "FROM CACHE('people') SELECT id",
		Dependencies: []string{"people"},
	}
	if _, err := views.CreateCold(definition); err != nil {
		t.Fatalf("CreateCold() error = %v", err)
	}

	started := make(chan struct{})
	release := make(chan struct{})
	resolver := SourceResolverFunc(func(name, key string) ([]Row, error) {
		close(started)
		<-release
		return []Row{{"id": int64(1)}}, nil
	})
	ownerDone := make(chan error, 1)
	go func() {
		_, err := views.GetOrHydrate(context.Background(), definition.Name, resolver, QueryOptions{})
		ownerDone <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("owner did not start hydration")
	}

	waiterContext, cancel := context.WithCancel(context.Background())
	waiterDone := make(chan error, 1)
	go func() {
		_, err := views.GetOrHydrate(waiterContext, definition.Name, resolver, QueryOptions{})
		waiterDone <- err
	}()
	cancel()
	select {
	case err := <-waiterDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("waiting reader error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("waiting reader did not honor cancellation")
	}

	close(release)
	select {
	case err := <-ownerDone:
		if err != nil {
			t.Fatalf("owner error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("owner did not finish")
	}
}

func TestM237GetOrHydrateHonorsCanceledColdReaderAndReadyFastPath(t *testing.T) {
	definition := MaterializedViewDefinition{
		Name:         "people",
		Query:        "FROM CACHE('people') SELECT id",
		Dependencies: []string{"people"},
	}

	t.Run("canceled cold reader", func(t *testing.T) {
		views := NewMaterializedViews()
		if _, err := views.CreateCold(definition); err != nil {
			t.Fatalf("CreateCold() error = %v", err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		var resolverCalls atomic.Int32
		_, err := views.GetOrHydrate(ctx, definition.Name, SourceResolverFunc(func(string, string) ([]Row, error) {
			resolverCalls.Add(1)
			return []Row{{"id": int64(1)}}, nil
		}), QueryOptions{})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled cold reader error = %v, want context.Canceled", err)
		}
		if got := resolverCalls.Load(); got != 0 {
			t.Fatalf("resolver calls after canceled reader = %d, want zero", got)
		}
		view, ok := views.Get(definition.Name)
		if !ok || view.Status.HydrationState != MaterializedViewHydrationStateCold {
			t.Fatalf("cold view after canceled reader = %#v/%v, want cold", view.Status, ok)
		}
	})

	t.Run("ready fast path", func(t *testing.T) {
		views := NewMaterializedViews()
		if _, err := views.Create(context.Background(), definition, SourceResolverFunc(func(string, string) ([]Row, error) {
			return []Row{{"id": int64(1)}}, nil
		}), QueryOptions{}); err != nil {
			t.Fatalf("Create() error = %v", err)
		}
		var resolverCalls atomic.Int32
		view, err := views.GetOrHydrate(context.Background(), definition.Name, SourceResolverFunc(func(string, string) ([]Row, error) {
			resolverCalls.Add(1)
			return nil, errors.New("ready path should not resolve")
		}), QueryOptions{})
		if err != nil {
			t.Fatalf("ready reader error = %v", err)
		}
		if view.Status.HydrationState != MaterializedViewHydrationStateReady || len(view.Result.Rows) != 1 {
			t.Fatalf("ready reader result = %#v, want one ready row", view)
		}
		if got := resolverCalls.Load(); got != 0 {
			t.Fatalf("ready path resolver calls = %d, want zero", got)
		}
	})
}

func TestM237GetOrHydrateFailureReturnsColdForRetry(t *testing.T) {
	views := NewMaterializedViews()
	definition := MaterializedViewDefinition{
		Name:         "people",
		Query:        "FROM CACHE('people') SELECT id",
		Dependencies: []string{"people"},
	}
	if _, err := views.CreateCold(definition); err != nil {
		t.Fatalf("CreateCold() error = %v", err)
	}
	firstErr := errors.New("source unavailable")
	_, err := views.GetOrHydrate(context.Background(), definition.Name, SourceResolverFunc(func(string, string) ([]Row, error) {
		return nil, firstErr
	}), QueryOptions{})
	if !errors.Is(err, firstErr) {
		t.Fatalf("failed lazy hydration error = %v, want source error", err)
	}
	if view, ok := views.Get(definition.Name); !ok || view.Status.HydrationState != MaterializedViewHydrationStateCold {
		t.Fatalf("view after failed lazy hydration = %#v/%v, want cold", view.Status, ok)
	}

	view, err := views.GetOrHydrate(context.Background(), definition.Name, SourceResolverFunc(func(string, string) ([]Row, error) {
		return []Row{{"id": int64(2)}}, nil
	}), QueryOptions{})
	if err != nil {
		t.Fatalf("retry lazy hydration error = %v", err)
	}
	if len(view.Result.Rows) != 1 || view.Result.Rows[0]["id"] != int64(2) {
		t.Fatalf("retry lazy hydration rows = %#v, want id 2", view.Result.Rows)
	}
}

func TestM237GetOrHydrateDropWakesWaiter(t *testing.T) {
	views := NewMaterializedViews()
	definition := MaterializedViewDefinition{
		Name:         "people",
		Query:        "FROM CACHE('people') SELECT id",
		Dependencies: []string{"people"},
	}
	if _, err := views.CreateCold(definition); err != nil {
		t.Fatalf("CreateCold() error = %v", err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	resolver := SourceResolverFunc(func(string, string) ([]Row, error) {
		close(started)
		<-release
		return []Row{{"id": int64(1)}}, nil
	})
	ownerDone := make(chan error, 1)
	go func() {
		_, err := views.GetOrHydrate(context.Background(), definition.Name, resolver, QueryOptions{})
		ownerDone <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("owner did not start hydration")
	}

	waiterDone := make(chan error, 1)
	go func() {
		_, err := views.GetOrHydrate(context.Background(), definition.Name, resolver, QueryOptions{})
		waiterDone <- err
	}()
	if err := views.Drop(definition.Name); err != nil {
		t.Fatalf("Drop() error = %v", err)
	}
	select {
	case err := <-waiterDone:
		if err == nil {
			t.Fatal("waiting reader returned nil after Drop()")
		}
	case <-time.After(time.Second):
		t.Fatal("waiting reader was not woken by Drop()")
	}
	close(release)
	select {
	case err := <-ownerDone:
		if err == nil {
			t.Fatal("owner returned nil after Drop()")
		}
	case <-time.After(time.Second):
		t.Fatal("owner did not finish after Drop()")
	}
}

func BenchmarkM237MaterializedViewGet(b *testing.B) {
	views := m237BenchmarkViews(b)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		view, ok := views.Get("people")
		if !ok {
			b.Fatal("Get() did not find people")
		}
		m237MaterializedViewSink = view
	}
}

func BenchmarkM237GetOrHydrateReady(b *testing.B) {
	views := m237BenchmarkViews(b)
	resolver := SourceResolverFunc(func(string, string) ([]Row, error) {
		return nil, errors.New("ready benchmark must not resolve")
	})
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		view, err := views.GetOrHydrate(context.Background(), "people", resolver, QueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		m237MaterializedViewSink = view
	}
}

func m237BenchmarkViews(b *testing.B) *MaterializedViews {
	b.Helper()
	views := NewMaterializedViews()
	definition := MaterializedViewDefinition{
		Name:         "people",
		Query:        "FROM CACHE('people') SELECT id, name",
		Dependencies: []string{"people"},
	}
	if _, err := views.Create(context.Background(), definition, SourceResolverFunc(func(string, string) ([]Row, error) {
		return []Row{{"id": int64(1), "name": "Ada"}}, nil
	}), QueryOptions{}); err != nil {
		b.Fatal(err)
	}
	return views
}
