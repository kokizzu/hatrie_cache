package hatSql

import (
	"context"
	"errors"
	"testing"
)

type m209HistoricalResolver struct{}

func (m209HistoricalResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return []Row{{"id": int64(1)}}, nil
}

func (m209HistoricalResolver) ResolveSQLSourceAt(string, string, uint64) ([]Row, error) {
	return []Row{{"id": int64(1)}}, nil
}

func TestM209LogicalFrontierAdvancesMonotonically(t *testing.T) {
	frontier := NewSQLLogicalFrontier(2)
	if got := frontier.Current(); got != 2 {
		t.Fatalf("initial Current() = %d, want 2", got)
	}
	advanced, err := frontier.Advance(5)
	if err != nil || !advanced {
		t.Fatalf("Advance(5) = %v, %v, want true and nil", advanced, err)
	}
	advanced, err = frontier.Advance(5)
	if err != nil || advanced {
		t.Fatalf("repeated Advance(5) = %v, %v, want false and nil", advanced, err)
	}
	if _, err := frontier.Advance(4); !errors.Is(err, ErrSQLLogicalFrontierRegression) {
		t.Fatalf("Advance(4) error = %v, want regression", err)
	}
	if got := frontier.Current(); got != 5 {
		t.Fatalf("Current() after regression = %d, want 5", got)
	}
}

func TestM209ReadAPIRejectsRegressedAsOfFrontier(t *testing.T) {
	frontier := NewSQLLogicalFrontier(0)
	asOf := uint64(5)
	options := SQLQueryOptions{AsOfFrontier: &asOf, LogicalFrontier: frontier}
	resolver := &mz008TestResolver{historical: []Row{{"id": int64(1)}}}
	if _, err := ExecuteSQLQueryContext(context.Background(), "FROM CACHE('items') SELECT id", resolver, options); err != nil {
		t.Fatalf("first read error = %v", err)
	}
	if got := frontier.Current(); got != 5 {
		t.Fatalf("frontier after first read = %d, want 5", got)
	}
	asOf = 4
	if _, err := ExecuteSQLQueryContext(context.Background(), "FROM CACHE('items') SELECT id", resolver, options); !errors.Is(err, ErrSQLLogicalFrontierRegression) {
		t.Fatalf("regressed read error = %v, want regression", err)
	}
	if got := frontier.Current(); got != 5 {
		t.Fatalf("frontier after rejected read = %d, want 5", got)
	}
}

func TestM209ReadFailureDoesNotAdvanceLogicalFrontier(t *testing.T) {
	frontier := NewSQLLogicalFrontier(0)
	asOf := uint64(5)
	resolver := &mz008TestResolver{beginErr: errors.New("snapshot unavailable")}
	options := SQLQueryOptions{AsOfFrontier: &asOf, LogicalFrontier: frontier}
	if _, err := ExecuteSQLQueryContext(context.Background(), "FROM CACHE('items') SELECT id", resolver, options); err == nil {
		t.Fatal("failed read error = nil")
	}
	if got := frontier.Current(); got != 0 {
		t.Fatalf("frontier after failed read = %d, want 0", got)
	}
	resolver.beginErr = nil
	if _, err := ExecuteSQLQueryContext(context.Background(), "FROM CACHE('items') SELECT id", resolver, options); err != nil {
		t.Fatalf("retry read error = %v", err)
	}
	if got := frontier.Current(); got != 5 {
		t.Fatalf("frontier after successful retry = %d, want 5", got)
	}
}

func TestM209StreamAPIRejectsRegressedFrontierBeforeStateChange(t *testing.T) {
	frontier := NewSQLLogicalFrontier(0)
	registry := NewQuerySubscriptions(1)
	subscription, err := registry.Subscribe(context.Background(), QuerySubscriptionDefinition{
		Query:           "SELECT * FROM VALUES (1)",
		Dependencies:    []string{"items"},
		LogicalFrontier: frontier,
	}, SourceResolverFunc(nil), QueryOptions{})
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	defer subscription.Close()
	if err := registry.NotifyChangedAt(context.Background(), 5, nil, SourceResolverFunc(nil), QueryOptions{}); err != nil {
		t.Fatalf("NotifyChangedAt(5) error = %v", err)
	}
	snapshot, ok := subscription.Snapshot()
	if !ok || snapshot.Frontier != 5 {
		t.Fatalf("snapshot after first frontier = %#v, %v", snapshot, ok)
	}
	if err := registry.NotifyChangedAt(context.Background(), 4, nil, SourceResolverFunc(nil), QueryOptions{}); !errors.Is(err, ErrSQLLogicalFrontierRegression) {
		t.Fatalf("NotifyChangedAt(4) error = %v, want regression", err)
	}
	snapshot, ok = subscription.Snapshot()
	if !ok || snapshot.Frontier != 5 {
		t.Fatalf("snapshot after rejected frontier = %#v, %v", snapshot, ok)
	}
}

func TestM209SubscriptionInitialAsOfUsesLogicalFrontier(t *testing.T) {
	frontier := NewSQLLogicalFrontier(5)
	registry := NewQuerySubscriptions(1)
	_, err := registry.Subscribe(context.Background(), QuerySubscriptionDefinition{
		Query:           "FROM CACHE('items') SELECT id",
		Dependencies:    []string{"items"},
		AsOf:            4,
		LogicalFrontier: frontier,
	}, m209HistoricalResolver{}, QueryOptions{})
	if !errors.Is(err, ErrSQLLogicalFrontierRegression) {
		t.Fatalf("Subscribe() error = %v, want regression", err)
	}
	if got := frontier.Current(); got != 5 {
		t.Fatalf("frontier after rejected subscription = %d, want 5", got)
	}
}

func TestM209HeartbeatUsesLogicalFrontier(t *testing.T) {
	frontier := NewSQLLogicalFrontier(0)
	registry := NewQuerySubscriptions(2)
	subscription, err := registry.Subscribe(context.Background(), QuerySubscriptionDefinition{
		Query:           "SELECT * FROM VALUES (1)",
		Dependencies:    []string{"items"},
		EmitProgress:    true,
		LogicalFrontier: frontier,
	}, SourceResolverFunc(nil), QueryOptions{})
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	defer subscription.Close()
	if err := registry.Heartbeat(7); err != nil {
		t.Fatalf("Heartbeat(7) error = %v", err)
	}
	if err := registry.Heartbeat(6); !errors.Is(err, ErrSQLLogicalFrontierRegression) {
		t.Fatalf("Heartbeat(6) error = %v, want regression", err)
	}
	if got := frontier.Current(); got != 7 {
		t.Fatalf("frontier after heartbeat regression = %d, want 7", got)
	}
}
