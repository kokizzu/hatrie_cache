package hatSql

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

type m224HydrationProgressResolver struct {
	rows         []Row
	firstVisited chan struct{}
	release      chan struct{}
	once         sync.Once
}

func (resolver *m224HydrationProgressResolver) ResolveSQLSource(name, key string) ([]Row, error) {
	if name != "CACHE" || key != "people" {
		return nil, nil
	}
	return CloneRows(resolver.rows), nil
}

func (resolver *m224HydrationProgressResolver) SQLSourceCardinality(name, key string) (int, bool, bool, error) {
	if name != "CACHE" || key != "people" {
		return 0, false, false, nil
	}
	return len(resolver.rows), true, true, nil
}

func (resolver *m224HydrationProgressResolver) StreamSQLSource(ctx context.Context, name, key string, visit func(Row) error) error {
	if name != "CACHE" || key != "people" {
		return nil
	}
	for index, row := range resolver.rows {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := visit(row); err != nil {
			return err
		}
		if index == 0 && resolver.firstVisited != nil {
			resolver.once.Do(func() { close(resolver.firstVisited) })
			<-resolver.release
		}
	}
	return nil
}

func (resolver *m224HydrationProgressResolver) ResolveSQLSourceWithProgress(ctx context.Context, name, key string, report func(Row) error) ([]Row, error) {
	if name != "CACHE" || key != "people" {
		return nil, nil
	}
	for index, row := range resolver.rows {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := report(row); err != nil {
			return nil, err
		}
		if index == 0 && resolver.firstVisited != nil {
			resolver.once.Do(func() { close(resolver.firstVisited) })
			<-resolver.release
		}
	}
	return CloneRows(resolver.rows), nil
}

func TestM224MaterializedViewHydrationProgressReportsLiveEstimate(t *testing.T) {
	views := NewMaterializedViews()
	definition := m223HydrationDefinition()
	if _, err := views.CreateCold(definition); err != nil {
		t.Fatal(err)
	}
	resolver := &m224HydrationProgressResolver{
		rows:         []Row{{"id": "u1", "name": "Ada"}, {"id": "u2", "name": "Grace"}, {"id": "u3", "name": "Lin"}},
		firstVisited: make(chan struct{}),
		release:      make(chan struct{}),
	}
	type hydrationResult struct {
		status MaterializedViewStatus
		err    error
	}
	done := make(chan hydrationResult, 1)
	go func() {
		status, err := views.Hydrate(context.Background(), definition.Name, resolver, QueryOptions{})
		done <- hydrationResult{status: status, err: err}
	}()
	select {
	case <-resolver.firstVisited:
	case <-time.After(time.Second):
		t.Fatal("hydration did not visit its first source row")
	}

	view, ok := views.Get(definition.Name)
	if !ok {
		t.Fatal("hydrating view is missing")
	}
	progress := view.Status.HydrationProgress
	if view.Status.HydrationState != MaterializedViewHydrationStateHydrating {
		t.Fatalf("hydration state = %q, want hydrating", view.Status.HydrationState)
	}
	if !progress.EstimateAvailable || !progress.EstimateExact || !progress.ProgressKnown {
		t.Fatalf("progress estimate flags = %#v", progress)
	}
	if progress.CompletedWork != 1 || progress.EstimatedWork != 3 || progress.EstimatedRemainingWork != 2 {
		t.Fatalf("live hydration progress = %#v, want completed=1 estimated=3 remaining=2", progress)
	}
	if progress.Progress < 0.333 || progress.Progress > 0.334 {
		t.Fatalf("live hydration progress fraction = %v, want one third", progress.Progress)
	}

	close(resolver.release)
	result := <-done
	if result.err != nil {
		t.Fatal(result.err)
	}
	if result.status.HydrationState != MaterializedViewHydrationStateReady {
		t.Fatalf("final hydration state = %q, want ready", result.status.HydrationState)
	}
	if progress := result.status.HydrationProgress; progress.CompletedWork != 3 || progress.EstimatedWork != 3 || progress.EstimatedRemainingWork != 0 || !progress.ProgressKnown || progress.Progress != 1 {
		t.Fatalf("final hydration progress = %#v", progress)
	}
}

func TestM224MaterializedViewHydrationProgressUnknownEstimateRemainsSafe(t *testing.T) {
	views := NewMaterializedViews()
	definition := m223HydrationDefinition()
	if _, err := views.CreateCold(definition); err != nil {
		t.Fatal(err)
	}
	resolver := &m224UnknownHydrationProgressResolver{rows: []Row{{"id": "u1"}, {"id": "u2"}}}
	if _, err := views.Hydrate(context.Background(), definition.Name, resolver, QueryOptions{}); err != nil {
		t.Fatal(err)
	}
	status, ok := views.Get(definition.Name)
	if !ok {
		t.Fatal("hydrated view is missing")
	}
	progress := status.Status.HydrationProgress
	if progress.CompletedWork != 2 || progress.EstimatedWork != 0 || progress.EstimatedRemainingWork != 0 || progress.EstimateAvailable || progress.ProgressKnown || progress.Progress != 1 {
		t.Fatalf("unknown hydration progress = %#v", progress)
	}
}

type m224UnknownHydrationProgressResolver struct {
	rows []Row
}

func (resolver *m224UnknownHydrationProgressResolver) ResolveSQLSource(name, key string) ([]Row, error) {
	if name != "CACHE" || key != "people" {
		return nil, nil
	}
	return CloneRows(resolver.rows), nil
}

func (resolver *m224UnknownHydrationProgressResolver) StreamSQLSource(ctx context.Context, name, key string, visit func(Row) error) error {
	if name != "CACHE" || key != "people" {
		return nil
	}
	for _, row := range resolver.rows {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := visit(row); err != nil {
			return err
		}
	}
	return nil
}

func (resolver *m224UnknownHydrationProgressResolver) ResolveSQLSourceWithProgress(ctx context.Context, name, key string, report func(Row) error) ([]Row, error) {
	if name != "CACHE" || key != "people" {
		return nil, nil
	}
	for _, row := range resolver.rows {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := report(row); err != nil {
			return nil, err
		}
	}
	return CloneRows(resolver.rows), nil
}

func TestM224MaterializedViewHydrationProgressRejectsInvalidCardinality(t *testing.T) {
	views := NewMaterializedViews()
	definition := m223HydrationDefinition()
	if _, err := views.CreateCold(definition); err != nil {
		t.Fatal(err)
	}
	resolver := &m224InvalidCardinalityResolver{}
	if _, err := views.Hydrate(context.Background(), definition.Name, resolver, QueryOptions{}); err != nil {
		t.Fatal(err)
	}
	status, ok := views.Get(definition.Name)
	if !ok || status.Status.HydrationState != MaterializedViewHydrationStateReady {
		t.Fatalf("hydrated status = %#v, exists=%v", status, ok)
	}
	if status.Status.HydrationProgress.EstimateAvailable {
		t.Fatalf("invalid cardinality should not be advertised: %#v", status.Status.HydrationProgress)
	}
}

type m224InvalidCardinalityResolver struct{}

func (*m224InvalidCardinalityResolver) ResolveSQLSource(name, key string) ([]Row, error) {
	if name != "CACHE" || key != "people" {
		return nil, fmt.Errorf("unexpected source %s(%q)", name, key)
	}
	return []Row{{"id": "u1"}}, nil
}

func (*m224InvalidCardinalityResolver) SQLSourceCardinality(name, key string) (int, bool, bool, error) {
	if name != "CACHE" || key != "people" {
		return 0, false, false, nil
	}
	return -1, false, true, nil
}
