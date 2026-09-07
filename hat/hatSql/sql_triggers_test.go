package hatSql_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestSQLTriggerTransactionOrdersPrepareAndCommit(t *testing.T) {
	registry := hatSql.NewSQLTriggerRegistry()
	log := make([]string, 0, 8)
	register := func(name string, order int) {
		t.Helper()
		if err := registry.Register(hatSql.SQLTrigger{
			Name: name, Source: "people", Operation: "insert", Order: order,
			Prepare: func(context.Context, hatSql.SQLTriggerEvent) (hatSql.SQLTriggerAction, error) {
				log = append(log, "prepare:"+name)
				return hatSql.SQLTriggerAction{Commit: func(context.Context) error {
					log = append(log, "commit:"+name)
					return nil
				}}, nil
			},
		}); err != nil {
			t.Fatal(err)
		}
	}
	register("second", 20)
	register("first", 10)

	transaction, err := registry.BeginSQLTriggerTransaction(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	event := hatSql.SQLTriggerEvent{
		Source: "people", Operation: "insert", Key: "ada",
		Before: hatSql.Row{"name": "old"}, After: hatSql.Row{"name": "Ada"},
	}
	if err := transaction.Add(event); err != nil {
		t.Fatal(err)
	}
	event.After["name"] = "mutated outside transaction"
	if got := transaction.Events()[0].After["name"]; got != "Ada" {
		t.Fatalf("transaction event after value = %v, want Ada", got)
	}
	if err := transaction.Commit(func(context.Context, []hatSql.SQLTriggerEvent) (hatSql.SQLTriggerAction, error) {
		log = append(log, "commit:apply")
		return hatSql.SQLTriggerAction{}, nil
	}); err != nil {
		t.Fatal(err)
	}

	want := []string{"prepare:first", "prepare:second", "commit:apply", "commit:first", "commit:second"}
	if !reflect.DeepEqual(log, want) {
		t.Fatalf("trigger log = %v, want %v", log, want)
	}
	if err := transaction.Commit(nil); !errors.Is(err, hatSql.ErrSQLTriggerTransactionClosed) {
		t.Fatalf("second Commit() error = %v, want closed", err)
	}
}

func TestSQLTriggerTransactionRollsBackPreparedActionsBeforeApply(t *testing.T) {
	wantErr := errors.New("trigger rejected event")
	registry := hatSql.NewSQLTriggerRegistry()
	log := make([]string, 0, 4)
	if err := registry.Register(hatSql.SQLTrigger{
		Name: "first", Source: "people", Operation: "INSERT",
		Prepare: func(context.Context, hatSql.SQLTriggerEvent) (hatSql.SQLTriggerAction, error) {
			return hatSql.SQLTriggerAction{Rollback: func(context.Context) error {
				log = append(log, "rollback:first")
				return nil
			}}, nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(hatSql.SQLTrigger{
		Name: "reject", Source: "people", Operation: "INSERT", Order: 2,
		Prepare: func(context.Context, hatSql.SQLTriggerEvent) (hatSql.SQLTriggerAction, error) {
			return hatSql.SQLTriggerAction{}, wantErr
		},
	}); err != nil {
		t.Fatal(err)
	}
	transaction, err := registry.BeginSQLTriggerTransaction(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := transaction.Add(hatSql.SQLTriggerEvent{Source: "people", Operation: "INSERT"}); err != nil {
		t.Fatal(err)
	}
	applyCalled := false
	err = transaction.Commit(func(context.Context, []hatSql.SQLTriggerEvent) (hatSql.SQLTriggerAction, error) {
		applyCalled = true
		return hatSql.SQLTriggerAction{}, nil
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("Commit() error = %v, want %v", err, wantErr)
	}
	if applyCalled {
		t.Fatal("primary apply ran after trigger rejection")
	}
	if !reflect.DeepEqual(log, []string{"rollback:first"}) {
		t.Fatalf("rollback log = %v, want first trigger rollback", log)
	}
}

func TestSQLTriggerTransactionRollsBackAfterCommitFailureAndFiltersEvents(t *testing.T) {
	wantErr := errors.New("trigger commit failed")
	registry := hatSql.NewSQLTriggerRegistry()
	log := make([]string, 0, 5)
	if err := registry.Register(hatSql.SQLTrigger{
		Name: "insert-only", Source: "people", Operation: "INSERT",
		Prepare: func(context.Context, hatSql.SQLTriggerEvent) (hatSql.SQLTriggerAction, error) {
			return hatSql.SQLTriggerAction{
				Commit:   func(context.Context) error { log = append(log, "commit:trigger"); return wantErr },
				Rollback: func(context.Context) error { log = append(log, "rollback:trigger"); return nil },
			}, nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	transaction, err := registry.BeginSQLTriggerTransaction(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := transaction.Add(hatSql.SQLTriggerEvent{Source: "people", Operation: "UPDATE"}); err != nil {
		t.Fatal(err)
	}
	if err := transaction.Commit(func(context.Context, []hatSql.SQLTriggerEvent) (hatSql.SQLTriggerAction, error) {
		log = append(log, "commit:apply")
		return hatSql.SQLTriggerAction{
			Rollback: func(context.Context) error { log = append(log, "rollback:apply"); return nil },
		}, nil
	}); err != nil {
		t.Fatalf("filtered Commit() error = %v", err)
	}
	if !reflect.DeepEqual(log, []string{"commit:apply"}) {
		t.Fatalf("filtered log = %v, want only apply", log)
	}

	transaction, err = registry.BeginSQLTriggerTransaction(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := transaction.Add(hatSql.SQLTriggerEvent{Source: "people", Operation: "INSERT"}); err != nil {
		t.Fatal(err)
	}
	log = log[:0]
	err = transaction.Commit(func(context.Context, []hatSql.SQLTriggerEvent) (hatSql.SQLTriggerAction, error) {
		log = append(log, "commit:apply")
		return hatSql.SQLTriggerAction{
			Rollback: func(context.Context) error { log = append(log, "rollback:apply"); return nil },
		}, nil
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("commit failure error = %v, want %v", err, wantErr)
	}
	if !reflect.DeepEqual(log, []string{"commit:apply", "commit:trigger", "rollback:trigger", "rollback:apply"}) {
		t.Fatalf("commit failure log = %v", log)
	}
}

func TestSQLTriggerRegistryValidatesLifecycleAndRegistration(t *testing.T) {
	var nilRegistry *hatSql.SQLTriggerRegistry
	if _, err := nilRegistry.BeginSQLTriggerTransaction(context.Background()); !errors.Is(err, hatSql.ErrSQLTriggerRegistryNil) {
		t.Fatalf("nil registry error = %v", err)
	}
	registry := hatSql.NewSQLTriggerRegistry()
	invalid := []hatSql.SQLTrigger{
		{Source: "people", Operation: "INSERT"},
		{Name: "missing-callback", Source: "people", Operation: "INSERT"},
	}
	for _, trigger := range invalid {
		if err := registry.Register(trigger); !errors.Is(err, hatSql.ErrSQLTriggerInvalid) {
			t.Fatalf("Register(%#v) error = %v", trigger, err)
		}
	}
	trigger := hatSql.SQLTrigger{Name: "one", Source: "people", Operation: "INSERT", Prepare: func(context.Context, hatSql.SQLTriggerEvent) (hatSql.SQLTriggerAction, error) {
		return hatSql.SQLTriggerAction{}, nil
	}}
	if err := registry.Register(trigger); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(trigger); !errors.Is(err, hatSql.ErrSQLTriggerDuplicate) {
		t.Fatalf("duplicate Register() error = %v", err)
	}
	if !registry.Unregister("one") || registry.Unregister("one") {
		t.Fatal("Unregister() did not report the expected state")
	}
	transaction, err := registry.BeginSQLTriggerTransaction(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := transaction.Add(hatSql.SQLTriggerEvent{Operation: "INSERT"}); !errors.Is(err, hatSql.ErrSQLTriggerInvalid) {
		t.Fatalf("invalid event error = %v", err)
	}
	transaction.Rollback()
	if err := transaction.Add(hatSql.SQLTriggerEvent{Source: "people", Operation: "INSERT"}); !errors.Is(err, hatSql.ErrSQLTriggerTransactionClosed) {
		t.Fatalf("Add() after rollback error = %v", err)
	}
}
