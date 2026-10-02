package hatSql_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"hatrie_cache/hat/hatSql"
)

type mu034HistoricalResolver struct {
	history map[uint64][]hatSql.Row
}

func (resolver *mu034HistoricalResolver) ResolveSQLSource(name, key string) ([]hatSql.Row, error) {
	return resolver.ResolveSQLSourceAt(name, key, 3)
}

func (resolver *mu034HistoricalResolver) ResolveSQLSourceAt(name, key string, frontier uint64) ([]hatSql.Row, error) {
	if name != "CACHE" || key != "people" {
		return nil, nil
	}
	rows, ok := resolver.history[frontier]
	if !ok {
		return nil, fmt.Errorf("frontier %d is unavailable", frontier)
	}
	return hatSql.CloneRows(rows), nil
}

func TestMU034HistoricalSubscriptionResumesAndPersistsTerminalState(t *testing.T) {
	definition := hatSql.QuerySubscriptionDefinition{
		Query:        "FROM CACHE('people') SELECT id, name",
		Dependencies: []string{"people"},
		AsOf:         1,
		UpTo:         3,
		EmitProgress: true,
	}
	resolver := &mu034HistoricalResolver{history: map[uint64][]hatSql.Row{
		1: {{"id": int64(1), "name": "Ada"}},
		2: {{"id": int64(1), "name": "Ada"}, {"id": int64(2), "name": "Lin"}},
		3: {{"id": int64(1), "name": "Ada"}, {"id": int64(2), "name": "Lin"}, {"id": int64(3), "name": "Mo"}},
	}}

	registry := hatSql.NewQuerySubscriptions(4)
	subscription, err := registry.SubscribeHistorical(context.Background(), definition, resolver, hatSql.QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Close()

	initial, ok := subscription.Snapshot()
	if !ok || initial.Revision != 1 || initial.Frontier != 1 || len(initial.Result.Rows) != 1 {
		t.Fatalf("initial snapshot = %#v, %v", initial, ok)
	}
	checkpoint := subscription.Checkpoint()
	if checkpoint.ID == 0 || checkpoint.Revision != 1 || checkpoint.Frontier != 1 || checkpoint.Canceled || checkpoint.Complete {
		t.Fatalf("initial checkpoint = %#v", checkpoint)
	}
	checkpoint.Result.QueryID = "query-1"
	checkpoint.Result.HasMore = true
	checkpoint.Result.NextCursor = "next"

	encoded, err := checkpoint.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	var decoded hatSql.QuerySubscriptionCheckpoint
	if err := decoded.UnmarshalBinary(encoded); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, checkpoint) {
		t.Fatalf("checkpoint round trip = %#v, want %#v", decoded, checkpoint)
	}

	if err := registry.NotifyChangedAt(context.Background(), 2, []string{"people"}, resolver, hatSql.QueryOptions{}); err != nil {
		t.Fatal(err)
	}
	data := receiveMU034Snapshot(t, subscription)
	if data.Revision != 2 || data.Frontier != 2 || data.Progress || len(data.Result.Rows) != 2 {
		t.Fatalf("data update = %#v", data)
	}
	if err := subscription.Ack(data); err != nil {
		t.Fatal(err)
	}
	checkpoint = subscription.Checkpoint()
	if checkpoint.Revision != 2 || checkpoint.Frontier != 2 || len(checkpoint.Result.Rows) != 2 {
		t.Fatalf("acknowledged checkpoint = %#v", checkpoint)
	}

	resumedRegistry := hatSql.NewQuerySubscriptions(4)
	resumed, err := resumedRegistry.ResumeHistorical(definition, checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	defer resumed.Close()
	resumedInitial, ok := resumed.Snapshot()
	if !ok || !reflect.DeepEqual(resumedInitial.Result.Columns, checkpoint.Result.Columns) || !reflect.DeepEqual(resumedInitial.Result.Rows, checkpoint.Result.Rows) || resumedInitial.Revision != 2 {
		t.Fatalf("resumed snapshot = %#v, %v", resumedInitial, ok)
	}
	if err := resumedRegistry.NotifyChangedAt(context.Background(), 3, []string{"people"}, resolver, hatSql.QueryOptions{}); err != nil {
		t.Fatal(err)
	}
	resumedData := receiveMU034Snapshot(t, resumed)
	if resumedData.Revision != 3 || resumedData.Frontier != 3 || resumedData.Progress || len(resumedData.Result.Rows) != 3 {
		t.Fatalf("resumed data = %#v", resumedData)
	}
	if err := resumed.Ack(resumedData); err != nil {
		t.Fatal(err)
	}
	completion := receiveMU034Snapshot(t, resumed)
	if !completion.Progress || !completion.Complete || completion.Revision != 3 || completion.Frontier != 3 {
		t.Fatalf("completion = %#v", completion)
	}
	if err := resumed.Ack(completion); err != nil {
		t.Fatal(err)
	}
	completedCheckpoint := resumed.Checkpoint()
	if !completedCheckpoint.Complete || completedCheckpoint.Frontier != 3 || len(completedCheckpoint.Result.Rows) != 3 {
		t.Fatalf("completed checkpoint = %#v", completedCheckpoint)
	}
	if _, err := resumedRegistry.ResumeHistorical(definition, completedCheckpoint); !errors.Is(err, hatSql.ErrQuerySubscriptionCheckpointTerminal) {
		t.Fatalf("resume completed checkpoint error = %v", err)
	}

	canceled, err := subscription.Cancel()
	if err != nil {
		t.Fatal(err)
	}
	if !canceled.Canceled || canceled.Revision != 2 {
		t.Fatalf("canceled checkpoint = %#v", canceled)
	}
	if _, err := resumedRegistry.ResumeHistorical(definition, canceled); !errors.Is(err, hatSql.ErrQuerySubscriptionCheckpointTerminal) {
		t.Fatalf("resume canceled checkpoint error = %v", err)
	}
	if _, err := (hatSql.QuerySubscriptionCheckpoint{}).MarshalBinary(); !errors.Is(err, hatSql.ErrQuerySubscriptionCheckpointInvalid) {
		t.Fatalf("empty checkpoint error = %v", err)
	}
	corrupt := append([]byte(nil), encoded...)
	corrupt[len(corrupt)-1] ^= 1
	if err := decoded.UnmarshalBinary(corrupt); !errors.Is(err, hatSql.ErrQuerySubscriptionCheckpointCorrupt) {
		t.Fatalf("corrupt checkpoint error = %v", err)
	}
}

func TestMU034HistoricalSubscriptionRejectsStaleAcknowledgements(t *testing.T) {
	definition := hatSql.QuerySubscriptionDefinition{
		Query:        "FROM CACHE('people') SELECT id",
		Dependencies: []string{"people"},
		AsOf:         1,
	}
	resolver := &mu034HistoricalResolver{history: map[uint64][]hatSql.Row{
		1: {{"id": int64(1)}},
		2: {{"id": int64(2)}},
		3: {{"id": int64(3)}},
	}}
	registry := hatSql.NewQuerySubscriptions(4)
	subscription, err := registry.SubscribeHistorical(context.Background(), definition, resolver, hatSql.QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Close()
	if err := registry.NotifyChangedAt(context.Background(), 2, []string{"people"}, resolver, hatSql.QueryOptions{}); err != nil {
		t.Fatal(err)
	}
	stale := receiveMU034Snapshot(t, subscription)
	if err := registry.NotifyChangedAt(context.Background(), 3, []string{"people"}, resolver, hatSql.QueryOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := subscription.Ack(stale); !errors.Is(err, hatSql.ErrQuerySubscriptionCheckpointSequence) {
		t.Fatalf("stale acknowledgement error = %v", err)
	}
	current, ok := subscription.Snapshot()
	if !ok {
		t.Fatal("current snapshot unavailable")
	}
	if err := subscription.Ack(current); err != nil {
		t.Fatal(err)
	}
}

func receiveMU034Snapshot(t *testing.T, subscription *hatSql.HistoricalQuerySubscription) hatSql.QuerySubscriptionSnapshot {
	t.Helper()
	select {
	case snapshot := <-subscription.Updates():
		return snapshot
	case <-time.After(time.Second):
		t.Fatal("historical subscription update timeout")
		return hatSql.QuerySubscriptionSnapshot{}
	}
}
