package hatSql

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestTT029BeforeTriggerTransformsPrimaryEvent(t *testing.T) {
	registry := NewSQLTriggerRegistry()
	var afterSeen SQLTriggerEvent
	if err := registry.Register(SQLTrigger{
		Name:      "normalize",
		Source:    "items",
		Operation: "UPDATE",
		Order:     10,
		Before: func(_ context.Context, event SQLTriggerEvent) (SQLTriggerEvent, error) {
			event.After["normalized"] = true
			return event, nil
		},
	}); err != nil {
		t.Fatalf("Register(before) error = %v", err)
	}
	if err := registry.Register(SQLTrigger{
		Name:      "observe",
		Source:    "items",
		Operation: "UPDATE",
		Order:     20,
		Prepare: func(_ context.Context, event SQLTriggerEvent) (SQLTriggerAction, error) {
			afterSeen = event
			return SQLTriggerAction{}, nil
		},
	}); err != nil {
		t.Fatalf("Register(after) error = %v", err)
	}

	transaction, err := registry.BeginSQLTriggerTransaction(context.Background())
	if err != nil {
		t.Fatalf("BeginSQLTriggerTransaction() error = %v", err)
	}
	event := SQLTriggerEvent{
		Source:    "items",
		Operation: "UPDATE",
		Key:       "1",
		Before:    Row{"id": int64(1), "name": "old"},
		After:     Row{"id": int64(1), "name": "new"},
	}
	if err := transaction.Add(event); err != nil {
		t.Fatalf("Add() error = %v", err)
	}
	var applied []SQLTriggerEvent
	if err := transaction.Commit(func(_ context.Context, events []SQLTriggerEvent) (SQLTriggerAction, error) {
		applied = events
		return SQLTriggerAction{}, nil
	}); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}

	if len(applied) != 1 || !reflect.DeepEqual(applied[0].After["normalized"], true) {
		t.Fatalf("primary events = %#v, want normalized before primary apply", applied)
	}
	if !reflect.DeepEqual(afterSeen.After["normalized"], true) {
		t.Fatalf("after trigger event = %#v, want transformed event", afterSeen)
	}
	if _, ok := event.After["normalized"]; ok {
		t.Fatal("before trigger mutated caller-owned event")
	}
}

func TestTT029BeforeTriggerRejectsBeforePrimaryApply(t *testing.T) {
	registry := NewSQLTriggerRegistry()
	wantErr := errors.New("rejected by policy")
	if err := registry.Register(SQLTrigger{
		Name: "reject",
		Before: func(_ context.Context, event SQLTriggerEvent) (SQLTriggerEvent, error) {
			return event, wantErr
		},
	}); err != nil {
		t.Fatalf("Register(before) error = %v", err)
	}
	transaction, err := registry.BeginSQLTriggerTransaction(context.Background())
	if err != nil {
		t.Fatalf("BeginSQLTriggerTransaction() error = %v", err)
	}
	if err := transaction.Add(SQLTriggerEvent{Source: "items", Operation: "INSERT", Key: "1", After: Row{"id": int64(1)}}); err != nil {
		t.Fatalf("Add() error = %v", err)
	}
	applyCalls := 0
	err = transaction.Commit(func(context.Context, []SQLTriggerEvent) (SQLTriggerAction, error) {
		applyCalls++
		return SQLTriggerAction{}, nil
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("Commit() error = %v, want %v", err, wantErr)
	}
	if applyCalls != 0 {
		t.Fatalf("primary apply calls = %d, want 0", applyCalls)
	}
}

func TestTT029BeforeTriggerValidation(t *testing.T) {
	registry := NewSQLTriggerRegistry()
	if err := registry.Register(SQLTrigger{Name: "missing-callback"}); err == nil {
		t.Fatal("Register() accepted trigger without a callback")
	}
}

func TestTT029BeforeTriggerRejectsMetadataMutation(t *testing.T) {
	registry := NewSQLTriggerRegistry()
	if err := registry.Register(SQLTrigger{
		Name: "mutate-key",
		Before: func(_ context.Context, event SQLTriggerEvent) (SQLTriggerEvent, error) {
			event.Key = "different"
			return event, nil
		},
	}); err != nil {
		t.Fatalf("Register(before) error = %v", err)
	}
	transaction, err := registry.BeginSQLTriggerTransaction(context.Background())
	if err != nil {
		t.Fatalf("BeginSQLTriggerTransaction() error = %v", err)
	}
	if err := transaction.Add(SQLTriggerEvent{Source: "items", Operation: "UPDATE", Key: "1", After: Row{"id": int64(1)}}); err != nil {
		t.Fatalf("Add() error = %v", err)
	}
	applyCalls := 0
	err = transaction.Commit(func(context.Context, []SQLTriggerEvent) (SQLTriggerAction, error) {
		applyCalls++
		return SQLTriggerAction{}, nil
	})
	if !errors.Is(err, ErrSQLTriggerInvalid) {
		t.Fatalf("Commit() error = %v, want %v", err, ErrSQLTriggerInvalid)
	}
	if applyCalls != 0 {
		t.Fatalf("primary apply calls = %d, want 0", applyCalls)
	}
}
