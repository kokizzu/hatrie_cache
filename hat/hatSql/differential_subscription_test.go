package hatSql

import (
	"context"
	"reflect"
	"testing"
	"time"
)

func TestQueryDifferentialSubscriptionPreservesMultiplicityAndTypes(t *testing.T) {
	rows := []Row{
		{"id": int64(1), "name": "Ada", "payload": []byte{1}},
		{"id": int64(1), "name": "Ada", "payload": []byte{1}},
		{"id": int64(2), "name": "Lin", "payload": nil},
	}
	resolver := SourceResolverFunc(func(string, string) ([]Row, error) {
		return CloneRows(rows), nil
	})
	registry := NewQuerySubscriptions(1)
	subscription, err := registry.SubscribeDifferential(context.Background(), QuerySubscriptionDefinition{
		Query:        "FROM CACHE('people') SELECT id, name, payload",
		Dependencies: []string{"people"},
	}, resolver, QueryOptions{})
	if err != nil {
		t.Fatalf("SubscribeDifferential() error = %v", err)
	}
	defer subscription.Close()
	initial := receiveDifferentialBatch(t, subscription)
	if initial.ID == 0 || initial.Revision != 1 || initial.Frontier != 0 || initial.Progress || initial.Complete {
		t.Fatalf("initial envelope = %#v", initial)
	}
	if len(initial.Deltas) != 2 || initial.Deltas[0].Diff != 2 || initial.Deltas[1].Diff != 1 {
		t.Fatalf("initial deltas = %#v, want two rows with multiplicity 2 and 1", initial.Deltas)
	}
	if got := initial.Deltas[0].Row["id"]; got != int64(1) {
		t.Fatalf("initial id type/value = %#v, want int64(1)", got)
	}
	if got := initial.Deltas[0].Row["payload"]; !reflect.DeepEqual(got, []byte{1}) {
		t.Fatalf("initial payload = %#v, want []byte{1}", got)
	}

	rows = []Row{
		{"id": int64(1), "name": "Ada", "payload": []byte{1}},
		{"id": int64(2), "name": "Lin", "payload": nil},
		{"id": uint64(3), "name": "Mo", "payload": []byte{2}},
	}
	if err := registry.NotifyChanged(context.Background(), []string{"people"}, resolver, QueryOptions{}); err != nil {
		t.Fatalf("NotifyChanged() error = %v", err)
	}
	update := receiveDifferentialBatch(t, subscription)
	if update.Revision != 2 || update.Progress || update.Complete || len(update.Deltas) != 2 {
		t.Fatalf("update envelope = %#v", update)
	}
	if got := update.Deltas[0]; got.Diff != -1 || got.Row["id"] != int64(1) {
		t.Fatalf("retraction = %#v, want one int64(1) retraction", got)
	}
	if got := update.Deltas[1]; got.Diff != 1 || got.Row["id"] != uint64(3) {
		t.Fatalf("addition = %#v, want one uint64(3) addition", got)
	}
}

func TestQueryDifferentialSubscriptionEmitsProgressAndCompletion(t *testing.T) {
	rows := []Row{{"name": "Ada"}}
	resolver := SourceResolverFunc(func(string, string) ([]Row, error) {
		return CloneRows(rows), nil
	})
	registry := NewQuerySubscriptions(1)
	subscription, err := registry.SubscribeDifferential(context.Background(), QuerySubscriptionDefinition{
		Query:        "FROM CACHE('people') SELECT name",
		Dependencies: []string{"people"},
		EmitProgress: true,
		StartLive:    true,
		UpTo:         3,
	}, resolver, QueryOptions{})
	if err != nil {
		t.Fatalf("SubscribeDifferential() error = %v", err)
	}
	defer subscription.Close()
	select {
	case initial := <-subscription.Updates():
		t.Fatalf("start-live initial envelope = %#v, want none", initial)
	default:
	}

	if err := registry.NotifyChangedAt(context.Background(), 1, []string{"people"}, resolver, QueryOptions{}); err != nil {
		t.Fatalf("NotifyChangedAt(1) error = %v", err)
	}
	data := receiveDifferentialBatch(t, subscription)
	if data.Frontier != 1 || data.Revision != 1 || len(data.Deltas) != 1 || data.Deltas[0].Diff != 1 || data.Progress {
		t.Fatalf("data envelope = %#v", data)
	}
	progress := receiveDifferentialBatch(t, subscription)
	if progress.Frontier != 1 || progress.Revision != 1 || !progress.Progress || progress.Complete || len(progress.Deltas) != 0 {
		t.Fatalf("progress envelope = %#v", progress)
	}

	if err := registry.NotifyChangedAt(context.Background(), 3, nil, resolver, QueryOptions{}); err != nil {
		t.Fatalf("NotifyChangedAt(3) error = %v", err)
	}
	completion := receiveDifferentialBatch(t, subscription)
	if completion.Frontier != 3 || completion.Revision != 1 || !completion.Progress || !completion.Complete || len(completion.Deltas) != 0 {
		t.Fatalf("completion envelope = %#v", completion)
	}
	select {
	case _, open := <-subscription.Updates():
		if open {
			t.Fatal("completed differential channel remains open")
		}
	case <-time.After(time.Second):
		t.Fatal("completed differential channel did not close")
	}
}

func TestQueryDifferentialSubscriptionCloseIsIdempotent(t *testing.T) {
	resolver := SourceResolverFunc(func(string, string) ([]Row, error) {
		return []Row{{"name": "Ada"}}, nil
	})
	registry := NewQuerySubscriptions(1)
	subscription, err := registry.SubscribeDifferential(context.Background(), QuerySubscriptionDefinition{
		Query:        "FROM CACHE('people') SELECT name",
		Dependencies: []string{"people"},
	}, resolver, QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got := receiveDifferentialBatch(t, subscription); len(got.Deltas) != 1 {
		t.Fatalf("initial deltas = %#v", got.Deltas)
	}
	subscription.Close()
	subscription.Close()
	select {
	case _, open := <-subscription.Updates():
		if open {
			t.Fatal("closed differential channel remains open")
		}
	case <-time.After(time.Second):
		t.Fatal("closed differential channel did not close")
	}
}

func TestQueryDifferentialSubscriptionMarksResetAfterCoalescing(t *testing.T) {
	rows := []Row{{"id": int64(1)}}
	resolver := SourceResolverFunc(func(string, string) ([]Row, error) {
		return CloneRows(rows), nil
	})
	registry := NewQuerySubscriptions(1)
	subscription, err := registry.SubscribeDifferential(context.Background(), QuerySubscriptionDefinition{
		Query:        "FROM CACHE('items') SELECT id",
		Dependencies: []string{"items"},
	}, resolver, QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Close()
	_ = receiveDifferentialBatch(t, subscription)
	for id := int64(2); id <= 4; id++ {
		rows = []Row{{"id": id}}
		if err := registry.NotifyChanged(context.Background(), []string{"items"}, resolver, QueryOptions{}); err != nil {
			t.Fatalf("NotifyChanged(%d) error = %v", id, err)
		}
	}
	reset := receiveDifferentialBatch(t, subscription)
	if !reset.Reset || len(reset.Deltas) != 1 || reset.Deltas[0].Diff != 1 || reset.Deltas[0].Row["id"] != int64(4) {
		t.Fatalf("coalesced batch = %#v, want reset to id 4", reset)
	}
}

func receiveDifferentialBatch(t *testing.T, subscription *QueryDifferentialSubscription) QuerySubscriptionDeltaBatch {
	t.Helper()
	select {
	case update := <-subscription.Updates():
		return update
	case <-time.After(time.Second):
		t.Fatal("differential update timeout")
		return QuerySubscriptionDeltaBatch{}
	}
}
