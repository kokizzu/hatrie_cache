package hatSql

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
)

type m223HydrationResolver struct {
	mu      sync.RWMutex
	rows    []Row
	err     error
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (resolver *m223HydrationResolver) ResolveSQLSource(kind, key string) ([]Row, error) {
	if kind != "CACHE" || key != "people" {
		return nil, nil
	}
	if resolver.started != nil {
		resolver.once.Do(func() { close(resolver.started) })
		<-resolver.release
	}
	resolver.mu.RLock()
	defer resolver.mu.RUnlock()
	if resolver.err != nil {
		return nil, resolver.err
	}
	return CloneRows(resolver.rows), nil
}

func (resolver *m223HydrationResolver) replace(rows []Row, err error) {
	resolver.mu.Lock()
	resolver.rows = CloneRows(rows)
	resolver.err = err
	resolver.mu.Unlock()
}

func m223HydrationDefinition() MaterializedViewDefinition {
	return MaterializedViewDefinition{
		Name:         "people_view",
		Query:        "FROM CACHE('people') SELECT id, name",
		Dependencies: []string{"people"},
	}
}

func TestM223MaterializedViewHydrationStateMachine(t *testing.T) {
	views := NewMaterializedViews()
	definition := m223HydrationDefinition()
	status, err := views.CreateCold(definition)
	if err != nil {
		t.Fatal(err)
	}
	if status.HydrationState != MaterializedViewHydrationStateCold || status.Revision != 0 {
		t.Fatalf("cold status = %#v", status)
	}

	resolver := &m223HydrationResolver{
		rows:    []Row{{"id": "u1", "name": "Ada"}},
		started: make(chan struct{}),
		release: make(chan struct{}),
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
	<-resolver.started

	hydrating, ok := views.Get(definition.Name)
	if !ok || hydrating.Status.HydrationState != MaterializedViewHydrationStateHydrating {
		t.Fatalf("hydrating view = %#v, exists=%v", hydrating, ok)
	}
	if len(hydrating.Result.Rows) != 0 {
		t.Fatalf("hydrating view published rows = %#v", hydrating.Result.Rows)
	}
	if err := views.MarkMaterializedViewCold(definition.Name); !errors.Is(err, ErrMaterializedViewHydrationInProgress) {
		t.Fatalf("MarkMaterializedViewCold() while hydrating = %v, want ErrMaterializedViewHydrationInProgress", err)
	}
	if _, err := views.RefreshChanged(context.Background(), []string{"people"}, resolver, QueryOptions{}); !errors.Is(err, ErrMaterializedViewHydrationNotReady) {
		t.Fatalf("RefreshChanged() error = %v, want ErrMaterializedViewHydrationNotReady", err)
	}

	close(resolver.release)
	result := <-done
	if result.err != nil {
		t.Fatal(result.err)
	}
	if result.status.HydrationState != MaterializedViewHydrationStateReady || result.status.Revision != 1 {
		t.Fatalf("hydrated status = %#v", result.status)
	}
	ready, ok := views.Get(definition.Name)
	if !ok || ready.Status.HydrationState != MaterializedViewHydrationStateReady || len(ready.Result.Rows) != 1 || ready.Result.Rows[0]["name"] != "Ada" {
		t.Fatalf("ready view = %#v, exists=%v", ready, ok)
	}
	if _, err := views.Hydrate(context.Background(), definition.Name, resolver, QueryOptions{}); !errors.Is(err, ErrMaterializedViewHydrationNotCold) {
		t.Fatalf("Hydrate() on ready view = %v, want ErrMaterializedViewHydrationNotCold", err)
	}
}

func TestM223MaterializedViewHydrationFailureReturnsCold(t *testing.T) {
	views := NewMaterializedViews()
	definition := m223HydrationDefinition()
	if _, err := views.CreateCold(definition); err != nil {
		t.Fatal(err)
	}
	resolver := &m223HydrationResolver{err: fmt.Errorf("source unavailable")}
	if _, err := views.Hydrate(context.Background(), definition.Name, resolver, QueryOptions{}); err == nil {
		t.Fatal("Hydrate() error = nil, want source error")
	}
	view, ok := views.Get(definition.Name)
	if !ok || view.Status.HydrationState != MaterializedViewHydrationStateCold || view.Status.Revision != 0 || len(view.Result.Rows) != 0 {
		t.Fatalf("failed hydration view = %#v, exists=%v", view, ok)
	}
	if usage := views.Usage(); usage.Rows != 0 || usage.Bytes != 0 {
		t.Fatalf("failed hydration usage = %#v, want empty", usage)
	}
}

func TestM223MaterializedViewColdWithdrawsPointLookupUntilHydrated(t *testing.T) {
	views := NewMaterializedViews()
	definition := m223HydrationDefinition()
	resolver := &m223HydrationResolver{rows: []Row{{"id": "u1", "name": "Ada"}}}
	if _, err := views.Create(context.Background(), definition, resolver, QueryOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := views.CreatePointLookupIndex(MaterializedViewPointLookupDefinition{
		Name:     "people_by_id",
		ViewName: definition.Name,
		Key: func(row Row) (string, error) {
			id, ok := row["id"].(string)
			if !ok {
				return "", fmt.Errorf("id is not a string")
			}
			return id, nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := views.MarkMaterializedViewCold(definition.Name); err != nil {
		t.Fatal(err)
	}
	cold, ok := views.Get(definition.Name)
	if !ok || cold.Status.HydrationState != MaterializedViewHydrationStateCold || len(cold.Result.Rows) != 0 {
		t.Fatalf("cold view = %#v, exists=%v", cold, ok)
	}
	if _, _, err := views.LookupPoint("people_by_id", "u1"); !errors.Is(err, ErrMaterializedViewHydrationNotReady) {
		t.Fatalf("cold LookupPoint() error = %v, want ErrMaterializedViewHydrationNotReady", err)
	}
	if _, err := views.AcquirePointLookupReader("people_by_id"); !errors.Is(err, ErrMaterializedViewHydrationNotReady) {
		t.Fatalf("cold AcquirePointLookupReader() error = %v, want ErrMaterializedViewHydrationNotReady", err)
	}
	if usage := views.Usage(); usage.Rows != 0 {
		t.Fatalf("cold usage = %#v, want zero retained rows", usage)
	}

	resolver.replace([]Row{{"id": "u1", "name": "Grace"}}, nil)
	if _, err := views.Hydrate(context.Background(), definition.Name, resolver, QueryOptions{}); err != nil {
		t.Fatal(err)
	}
	result, found, err := views.LookupPoint("people_by_id", "u1")
	if err != nil || !found || len(result.Rows) != 1 || result.Rows[0]["name"] != "Grace" {
		t.Fatalf("rehydrated LookupPoint() = %#v, found=%v, err=%v", result, found, err)
	}
	if usage := views.Usage(); usage.Rows != 1 {
		t.Fatalf("rehydrated usage = %#v, want one retained row", usage)
	}
}

func TestM223MaterializedViewCreateRemainsReady(t *testing.T) {
	views := NewMaterializedViews()
	status, err := views.Create(context.Background(), m223HydrationDefinition(), &m223HydrationResolver{
		rows: []Row{{"id": "u1", "name": "Ada"}},
	}, QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if status.HydrationState != MaterializedViewHydrationStateReady {
		t.Fatalf("Create() status = %#v, want ready", status)
	}
}

func TestM223MaterializedViewColdTransitionWinsOverInFlightRefresh(t *testing.T) {
	views := NewMaterializedViews()
	definition := m223HydrationDefinition()
	initialResolver := &m223HydrationResolver{rows: []Row{{"id": "u1", "name": "Ada"}}}
	if _, err := views.Create(context.Background(), definition, initialResolver, QueryOptions{}); err != nil {
		t.Fatal(err)
	}
	resolver := &m223HydrationResolver{
		rows:    []Row{{"id": "u1", "name": "Grace"}},
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	type refreshResult struct {
		statuses []MaterializedViewStatus
		err      error
	}
	done := make(chan refreshResult, 1)
	go func() {
		statuses, err := views.RefreshChanged(context.Background(), []string{"people"}, resolver, QueryOptions{})
		done <- refreshResult{statuses: statuses, err: err}
	}()
	<-resolver.started
	if err := views.MarkMaterializedViewCold(definition.Name); err != nil {
		t.Fatal(err)
	}
	close(resolver.release)
	refresh := <-done
	if refresh.err != nil || len(refresh.statuses) != 0 {
		t.Fatalf("in-flight refresh = %#v, want no publication", refresh)
	}
	view, ok := views.Get(definition.Name)
	if !ok || view.Status.HydrationState != MaterializedViewHydrationStateCold || len(view.Result.Rows) != 0 {
		t.Fatalf("view after cold transition = %#v, exists=%v", view, ok)
	}
}
