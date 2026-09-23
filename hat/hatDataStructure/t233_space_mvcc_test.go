package hatDataStructure_test

import (
	"context"
	"errors"
	"testing"

	"hatrie_cache/hat/hatDataStructure"
)

func TestT233MVCCSnapshotIsRepeatableAndInherited(t *testing.T) {
	for _, engine := range []hatDataStructure.SpaceEngine{
		hatDataStructure.SpaceEngineMemtx,
		hatDataStructure.SpaceEngineVinyl,
	} {
		t.Run(string(engine), func(t *testing.T) {
			space, err := hatDataStructure.NewSpace(t232SpaceOptions(engine))
			if err != nil {
				t.Fatal(err)
			}
			if err := space.Put("stable", []byte("old")); err != nil {
				t.Fatal(err)
			}
			if err := space.Put("vanish", []byte("before")); err != nil {
				t.Fatal(err)
			}

			tx, err := space.BeginMVCCTransaction()
			if err != nil {
				t.Fatal(err)
			}
			if got, ok := tx.Get("stable"); !ok || string(got) != "old" {
				t.Fatalf("initial snapshot = %q, %t, want old, true", got, ok)
			}
			if err := space.Put("stable", []byte("new")); err != nil {
				t.Fatal(err)
			}
			if err := space.Delete("vanish"); err != nil {
				t.Fatal(err)
			}
			if err := space.Put("added", []byte("after")); err != nil {
				t.Fatal(err)
			}
			if got, ok := tx.Get("stable"); !ok || string(got) != "old" {
				t.Fatalf("repeatable snapshot = %q, %t, want old, true", got, ok)
			}
			if got, ok := tx.Get("vanish"); !ok || string(got) != "before" {
				t.Fatalf("deleted snapshot = %q, %t, want before, true", got, ok)
			}
			if _, ok := tx.Get("added"); ok {
				t.Fatal("post-snapshot key is visible in MVCC read view")
			}

			child, err := tx.BeginNested()
			if err != nil {
				t.Fatal(err)
			}
			if got, ok := child.Get("stable"); !ok || string(got) != "old" {
				t.Fatalf("nested snapshot = %q, %t, want old, true", got, ok)
			}
			if err := child.Put("local", []byte("staged")); err != nil {
				t.Fatal(err)
			}
			if err := child.Commit(); err != nil {
				t.Fatal(err)
			}
			if got, ok := tx.Get("local"); !ok || string(got) != "staged" {
				t.Fatalf("nested staged value = %q, %t, want staged, true", got, ok)
			}
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestT233MVCCYieldCooperatesWithContext(t *testing.T) {
	space, err := hatDataStructure.NewSpace(t232SpaceOptions(hatDataStructure.SpaceEngineMemtx))
	if err != nil {
		t.Fatal(err)
	}
	tx, err := space.BeginMVCCTransaction()
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Yield(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := tx.Yield(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled yield = %v, want %v", err, context.Canceled)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := tx.Yield(context.Background()); !errors.Is(err, hatDataStructure.ErrSpaceTransactionClosed) {
		t.Fatalf("yield after rollback = %v, want %v", err, hatDataStructure.ErrSpaceTransactionClosed)
	}
}

func TestT233MVCCGetStillCopiesValues(t *testing.T) {
	space, err := hatDataStructure.NewSpace(t232SpaceOptions(hatDataStructure.SpaceEngineMemtx))
	if err != nil {
		t.Fatal(err)
	}
	if err := space.Put("key", []byte("value")); err != nil {
		t.Fatal(err)
	}
	tx, err := space.BeginMVCCTransaction()
	if err != nil {
		t.Fatal(err)
	}
	got, ok := tx.Get("key")
	if !ok || string(got) != "value" {
		t.Fatalf("snapshot value = %q, %t, want value, true", got, ok)
	}
	got[0] = 'x'
	got, ok = tx.Get("key")
	if !ok || string(got) != "value" {
		t.Fatalf("snapshot after read mutation = %q, %t, want value, true", got, ok)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
}
