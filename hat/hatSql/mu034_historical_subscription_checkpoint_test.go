package hatSql

import (
	"context"
	"reflect"
	"testing"
	"time"
)

type mu034HistoricalSubscriptionResolver struct {
	history map[uint64][]Row
}

func (resolver *mu034HistoricalSubscriptionResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return CloneRows(resolver.history[30]), nil
}

func (resolver *mu034HistoricalSubscriptionResolver) ResolveSQLSourceAt(_ string, _ string, frontier uint64) ([]Row, error) {
	return CloneRows(resolver.history[frontier]), nil
}

func mu034SubscriptionDefinition() QuerySubscriptionDefinition {
	return QuerySubscriptionDefinition{
		Query:        "FROM CACHE('people') SELECT id, name",
		Dependencies: []string{"people"},
		AsOf:         10,
	}
}

func mu034ReceiveSnapshot(t *testing.T, subscription *QuerySubscription) QuerySubscriptionSnapshot {
	t.Helper()
	select {
	case snapshot := <-subscription.Updates():
		return snapshot
	case <-time.After(time.Second):
		t.Fatal("subscription update timeout")
		return QuerySubscriptionSnapshot{}
	}
}

func TestMU034CheckpointExcludesUnacknowledgedHistoricalUpdate(t *testing.T) {
	resolver := &mu034HistoricalSubscriptionResolver{history: map[uint64][]Row{
		10: {{"id": int64(1), "name": "Ada"}},
		20: {{"id": int64(1), "name": "Lin"}},
		30: {{"id": int64(1), "name": "Bea"}},
	}}
	registry := NewQuerySubscriptions(2)
	subscription, err := registry.Subscribe(context.Background(), mu034SubscriptionDefinition(), resolver, QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	initial, ok := subscription.Snapshot()
	if !ok {
		t.Fatal("initial snapshot is unavailable")
	}
	if err := subscription.Acknowledge(initial); err != nil {
		t.Fatalf("Acknowledge(initial) error = %v", err)
	}
	if err := registry.NotifyChangedAt(context.Background(), 20, []string{"people"}, resolver, QueryOptions{}); err != nil {
		t.Fatal(err)
	}
	update := mu034ReceiveSnapshot(t, subscription)
	if update.Frontier != 20 || update.Result.Rows[0]["name"] != "Lin" {
		t.Fatalf("update = %#v", update)
	}

	checkpoint, err := subscription.CloseWithCheckpoint()
	if err != nil {
		t.Fatalf("CloseWithCheckpoint() error = %v", err)
	}
	if checkpoint.Snapshot.Frontier != 10 || checkpoint.Snapshot.Result.Rows[0]["name"] != "Ada" {
		t.Fatalf("checkpoint = %#v, want acknowledged frontier 10", checkpoint)
	}

	resumed, err := NewQuerySubscriptions(2).Resume(checkpoint)
	if err != nil {
		t.Fatalf("Resume() error = %v", err)
	}
	defer resumed.Close()
	current, ok := resumed.Snapshot()
	if !ok || current.Frontier != 10 || current.Result.Rows[0]["name"] != "Ada" {
		t.Fatalf("resumed snapshot = %#v/%t", current, ok)
	}
}

func TestMU034CheckpointResumeSkipsAlreadyAcknowledgedFrontier(t *testing.T) {
	resolver := &mu034HistoricalSubscriptionResolver{history: map[uint64][]Row{
		10: {{"id": int64(1), "name": "Ada"}},
		20: {{"id": int64(1), "name": "Lin"}},
		30: {{"id": int64(1), "name": "Bea"}},
	}}
	registry := NewQuerySubscriptions(2)
	subscription, err := registry.Subscribe(context.Background(), mu034SubscriptionDefinition(), resolver, QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Close()
	initial, _ := subscription.Snapshot()
	if err := subscription.Acknowledge(initial); err != nil {
		t.Fatal(err)
	}
	if err := registry.NotifyChangedAt(context.Background(), 20, []string{"people"}, resolver, QueryOptions{}); err != nil {
		t.Fatal(err)
	}
	update := mu034ReceiveSnapshot(t, subscription)
	if err := subscription.Acknowledge(update); err != nil {
		t.Fatal(err)
	}
	checkpoint, err := subscription.CloseWithCheckpoint()
	if err != nil {
		t.Fatal(err)
	}
	if checkpoint.Snapshot.Frontier != 20 || checkpoint.Snapshot.Result.Rows[0]["name"] != "Lin" {
		t.Fatalf("checkpoint = %#v", checkpoint)
	}

	resumed, err := NewQuerySubscriptions(2).Resume(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	defer resumed.Close()
	// Replaying the acknowledged frontier is a no-op; the next frontier is
	// the first one that can publish a new result.
	resumeRegistry := resumed.registry
	if err := resumeRegistry.NotifyChangedAt(context.Background(), 20, []string{"people"}, resolver, QueryOptions{}); err != nil {
		t.Fatal(err)
	}
	select {
	case update := <-resumed.Updates():
		t.Fatalf("duplicate acknowledged update = %#v", update)
	default:
	}
	if err := resumeRegistry.NotifyChangedAt(context.Background(), 30, []string{"people"}, resolver, QueryOptions{}); err != nil {
		t.Fatal(err)
	}
	update = mu034ReceiveSnapshot(t, resumed)
	if update.Frontier != 30 || update.Result.Rows[0]["name"] != "Bea" || update.Revision != checkpoint.Snapshot.Revision+1 {
		t.Fatalf("resumed update = %#v", update)
	}
}

func TestMU034CheckpointRequiresAcknowledgementAndClonesState(t *testing.T) {
	resolver := &mu034HistoricalSubscriptionResolver{history: map[uint64][]Row{
		10: {{"id": int64(1), "name": "Ada"}},
	}}
	registry := NewQuerySubscriptions(1)
	subscription, err := registry.Subscribe(context.Background(), mu034SubscriptionDefinition(), resolver, QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Close()
	if _, err := subscription.CloseWithCheckpoint(); err == nil {
		t.Fatal("CloseWithCheckpoint() accepted an unacknowledged subscription")
	}
	if _, ok := subscription.Snapshot(); !ok {
		t.Fatal("failed checkpoint attempt closed the subscription")
	}
	initial, _ := subscription.Snapshot()
	if err := subscription.Acknowledge(initial); err != nil {
		t.Fatal(err)
	}
	checkpoint, err := subscription.CloseWithCheckpoint()
	if err != nil {
		t.Fatal(err)
	}
	checkpoint.Snapshot.Result.Rows[0]["name"] = "mutated"
	if checkpoint.Snapshot.Result.Rows[0]["name"] != "mutated" {
		t.Fatal("test failed to mutate detached checkpoint")
	}
	if !reflect.DeepEqual(checkpoint.Definition.Dependencies, []string{"people"}) {
		t.Fatalf("checkpoint definition = %#v", checkpoint.Definition)
	}
}

func TestMU034DifferentialCheckpointResumeDoesNotReplayInitialBatch(t *testing.T) {
	resolver := &mu034HistoricalSubscriptionResolver{history: map[uint64][]Row{
		10: {{"id": int64(1), "name": "Ada"}},
		20: {{"id": int64(1), "name": "Lin"}},
		30: {{"id": int64(1), "name": "Bea"}},
	}}
	registry := NewQuerySubscriptions(2)
	differential, err := registry.SubscribeDifferential(context.Background(), mu034SubscriptionDefinition(), resolver, QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	initial := <-differential.Updates()
	if err := differential.Acknowledge(initial); err != nil {
		t.Fatalf("Acknowledge(initial) error = %v", err)
	}
	if err := registry.NotifyChangedAt(context.Background(), 20, []string{"people"}, resolver, QueryOptions{}); err != nil {
		t.Fatal(err)
	}
	update := <-differential.Updates()
	if err := differential.Acknowledge(update); err != nil {
		t.Fatalf("Acknowledge(update) error = %v", err)
	}
	checkpoint, err := differential.CloseWithCheckpoint()
	if err != nil {
		t.Fatal(err)
	}
	if !checkpoint.Differential {
		t.Fatal("differential checkpoint lost its mode")
	}

	resumed, err := NewQuerySubscriptions(2).ResumeDifferential(checkpoint)
	if err != nil {
		t.Fatalf("ResumeDifferential() error = %v", err)
	}
	defer resumed.Close()
	select {
	case batch := <-resumed.Updates():
		t.Fatalf("resumed differential emitted initial batch = %#v", batch)
	default:
	}
	resumeRegistry := resumed.subscription.registry
	if err := resumeRegistry.NotifyChangedAt(context.Background(), 20, []string{"people"}, resolver, QueryOptions{}); err != nil {
		t.Fatal(err)
	}
	select {
	case batch := <-resumed.Updates():
		t.Fatalf("replayed differential emitted batch = %#v", batch)
	default:
	}
	if err := resumeRegistry.NotifyChangedAt(context.Background(), 30, []string{"people"}, resolver, QueryOptions{}); err != nil {
		t.Fatal(err)
	}
	batch := <-resumed.Updates()
	if batch.Frontier != 30 || len(batch.Deltas) != 2 {
		t.Fatalf("resumed differential batch = %#v", batch)
	}
}
