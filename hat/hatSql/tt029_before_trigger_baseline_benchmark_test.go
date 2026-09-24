package hatSql

import (
	"context"
	"testing"
)

func BenchmarkTT029AfterTriggerBaseline(b *testing.B) {
	registry := NewSQLTriggerRegistry()
	if err := registry.Register(SQLTrigger{
		Name:      "observe",
		Source:    "items",
		Operation: "UPDATE",
		Prepare: func(context.Context, SQLTriggerEvent) (SQLTriggerAction, error) {
			return SQLTriggerAction{}, nil
		},
	}); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		transaction, err := registry.BeginSQLTriggerTransaction(context.Background())
		if err != nil {
			b.Fatal(err)
		}
		if err := transaction.Add(SQLTriggerEvent{
			Source:    "items",
			Operation: "UPDATE",
			Key:       "1",
			Before:    Row{"id": int64(1), "name": "old"},
			After:     Row{"id": int64(1), "name": "new"},
		}); err != nil {
			b.Fatal(err)
		}
		if err := transaction.Commit(func(context.Context, []SQLTriggerEvent) (SQLTriggerAction, error) {
			return SQLTriggerAction{}, nil
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTT029BeforeTrigger(b *testing.B) {
	registry := NewSQLTriggerRegistry()
	if err := registry.Register(SQLTrigger{
		Name:      "normalize",
		Source:    "items",
		Operation: "UPDATE",
		Before: func(_ context.Context, event SQLTriggerEvent) (SQLTriggerEvent, error) {
			event.After["normalized"] = true
			return event, nil
		},
	}); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		transaction, err := registry.BeginSQLTriggerTransaction(context.Background())
		if err != nil {
			b.Fatal(err)
		}
		if err := transaction.Add(SQLTriggerEvent{
			Source:    "items",
			Operation: "UPDATE",
			Key:       "1",
			Before:    Row{"id": int64(1), "name": "old"},
			After:     Row{"id": int64(1), "name": "new"},
		}); err != nil {
			b.Fatal(err)
		}
		if err := transaction.Commit(func(context.Context, []SQLTriggerEvent) (SQLTriggerAction, error) {
			return SQLTriggerAction{}, nil
		}); err != nil {
			b.Fatal(err)
		}
	}
}
