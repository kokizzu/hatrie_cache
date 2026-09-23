package hatDataStructure_test

import (
	"errors"
	"reflect"
	"testing"

	"hatrie_cache/hat/hatDataStructure"
)

func TestT232NestedScopesCommitAndRollback(t *testing.T) {
	for _, engine := range []hatDataStructure.SpaceEngine{
		hatDataStructure.SpaceEngineMemtx,
		hatDataStructure.SpaceEngineVinyl,
	} {
		t.Run(string(engine), func(t *testing.T) {
			space, err := hatDataStructure.NewSpace(t232SpaceOptions(engine))
			if err != nil {
				t.Fatal(err)
			}
			if err := space.Put("existing", []byte("old")); err != nil {
				t.Fatal(err)
			}

			tx, err := space.BeginTransaction()
			if err != nil {
				t.Fatal(err)
			}
			if err := tx.Put("outer", []byte("one")); err != nil {
				t.Fatal(err)
			}
			child, err := tx.BeginNested()
			if err != nil {
				t.Fatal(err)
			}
			if err := child.Put("discarded", []byte("no")); err != nil {
				t.Fatal(err)
			}
			if err := child.Rollback(); err != nil {
				t.Fatal(err)
			}
			child, err = tx.BeginNested()
			if err != nil {
				t.Fatal(err)
			}
			if err := child.Put("committed", []byte("yes")); err != nil {
				t.Fatal(err)
			}
			if err := child.Commit(); err != nil {
				t.Fatal(err)
			}
			if got, ok := tx.Get("committed"); !ok || string(got) != "yes" {
				t.Fatalf("nested committed value = %q, %t, want yes, true", got, ok)
			}
			if _, ok := tx.Get("discarded"); ok {
				t.Fatal("rolled-back nested value is visible in parent")
			}
			if _, ok := space.Get("outer"); ok {
				t.Fatal("staged outer value is visible before root commit")
			}
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}

			for key, want := range map[string]string{
				"existing":  "old",
				"outer":     "one",
				"committed": "yes",
			} {
				got, ok := space.Get(key)
				if !ok || string(got) != want {
					t.Fatalf("%s = %q, %t, want %q, true", key, got, ok, want)
				}
			}
			if _, ok := space.Get("discarded"); ok {
				t.Fatal("rolled-back nested value reached storage")
			}
		})
	}
}

func TestT232NestedRollbackKeepsParentOpen(t *testing.T) {
	space, err := hatDataStructure.NewSpace(t232SpaceOptions(hatDataStructure.SpaceEngineMemtx))
	if err != nil {
		t.Fatal(err)
	}
	tx, err := space.BeginTransaction()
	if err != nil {
		t.Fatal(err)
	}
	child, err := tx.BeginNested()
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); !errors.Is(err, hatDataStructure.ErrSpaceTransactionChildOpen) {
		t.Fatalf("commit with open child = %v, want %v", err, hatDataStructure.ErrSpaceTransactionChildOpen)
	}
	if err := tx.Rollback(); !errors.Is(err, hatDataStructure.ErrSpaceTransactionChildOpen) {
		t.Fatalf("rollback with open child = %v, want %v", err, hatDataStructure.ErrSpaceTransactionChildOpen)
	}
	if err := child.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); !errors.Is(err, hatDataStructure.ErrSpaceTransactionClosed) {
		t.Fatalf("second commit = %v, want %v", err, hatDataStructure.ErrSpaceTransactionClosed)
	}
}

func TestT232CommitValidationIsAtomic(t *testing.T) {
	conflict := errors.New("conflict")
	for _, engine := range []hatDataStructure.SpaceEngine{
		hatDataStructure.SpaceEngineMemtx,
		hatDataStructure.SpaceEngineVinyl,
	} {
		t.Run(string(engine), func(t *testing.T) {
			var onReplaces []hatDataStructure.SpaceReplace
			var audits []hatDataStructure.SpaceReplaceAudit
			options := t232SpaceOptions(engine)
			options.BeforeReplace = func(event hatDataStructure.SpaceReplace) error {
				if event.Key == "reject" {
					return conflict
				}
				return nil
			}
			options.OnReplace = func(event hatDataStructure.SpaceReplace) {
				onReplaces = append(onReplaces, event)
			}
			options.AfterReplace = func(event hatDataStructure.SpaceReplaceAudit) {
				audits = append(audits, event)
			}
			space, err := hatDataStructure.NewSpace(options)
			if err != nil {
				t.Fatal(err)
			}
			tx, err := space.BeginTransaction()
			if err != nil {
				t.Fatal(err)
			}
			if err := tx.Put("allow", []byte("new")); err != nil {
				t.Fatal(err)
			}
			if err := tx.Put("reject", []byte("bad")); err != nil {
				t.Fatal(err)
			}
			if err := tx.Commit(); !errors.Is(err, conflict) {
				t.Fatalf("commit error = %v, want %v", err, conflict)
			}
			if _, ok := space.Get("allow"); ok {
				t.Fatal("allow mutation became visible after rejected commit")
			}
			if _, ok := space.Get("reject"); ok {
				t.Fatal("reject mutation became visible after rejected commit")
			}
			if len(onReplaces) != 0 || len(audits) != 0 {
				t.Fatalf("callbacks after rejected commit = %d/%d, want 0/0", len(onReplaces), len(audits))
			}
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestT232CommitAppliesFinalMutationsAndCopiesEvents(t *testing.T) {
	for _, engine := range []hatDataStructure.SpaceEngine{
		hatDataStructure.SpaceEngineMemtx,
		hatDataStructure.SpaceEngineVinyl,
	} {
		t.Run(string(engine), func(t *testing.T) {
			var onReplaces []hatDataStructure.SpaceReplace
			var audits []hatDataStructure.SpaceReplaceAudit
			options := t232SpaceOptions(engine)
			options.OnReplace = func(event hatDataStructure.SpaceReplace) {
				onReplaces = append(onReplaces, hatDataStructure.SpaceReplace{
					Key:      event.Key,
					OldValue: append([]byte(nil), event.OldValue...),
					NewValue: append([]byte(nil), event.NewValue...),
					Exists:   event.Exists,
					Delete:   event.Delete,
				})
				if len(event.NewValue) > 0 {
					event.NewValue[0] = 'x'
				}
			}
			options.AfterReplace = func(event hatDataStructure.SpaceReplaceAudit) {
				audits = append(audits, hatDataStructure.SpaceReplaceAudit{
					TransactionID: event.TransactionID,
					SpaceReplace: hatDataStructure.SpaceReplace{
						Key:      event.Key,
						OldValue: append([]byte(nil), event.OldValue...),
						NewValue: append([]byte(nil), event.NewValue...),
						Exists:   event.Exists,
						Delete:   event.Delete,
					},
				})
			}
			space, err := hatDataStructure.NewSpace(options)
			if err != nil {
				t.Fatal(err)
			}
			if err := space.Put("replace", []byte("old")); err != nil {
				t.Fatal(err)
			}
			onReplaces = nil
			audits = nil
			tx, err := space.BeginTransaction()
			if err != nil {
				t.Fatal(err)
			}
			if err := tx.Put("new", []byte("value")); err != nil {
				t.Fatal(err)
			}
			if err := tx.Put("replace", []byte("updated")); err != nil {
				t.Fatal(err)
			}
			if err := tx.Delete("replace"); err != nil {
				t.Fatal(err)
			}
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
			if got, ok := space.Get("replace"); ok {
				t.Fatalf("deleted replacement = %q, %t, want missing", got, ok)
			}
			if got, ok := space.Get("new"); !ok || string(got) != "value" {
				t.Fatalf("new value = %q, %t, want value, true", got, ok)
			}
			wantEvents := []hatDataStructure.SpaceReplace{
				{Key: "new", NewValue: []byte("value")},
				{Key: "replace", OldValue: []byte("old"), Exists: true, Delete: true},
			}
			if !reflect.DeepEqual(onReplaces, wantEvents) {
				t.Fatalf("on-replace events = %#v, want %#v", onReplaces, wantEvents)
			}
			wantAudits := []hatDataStructure.SpaceReplaceAudit{
				{TransactionID: 2, SpaceReplace: wantEvents[0]},
				{TransactionID: 3, SpaceReplace: wantEvents[1]},
			}
			if !reflect.DeepEqual(audits, wantAudits) {
				t.Fatalf("audit events = %#v, want %#v", audits, wantAudits)
			}
		})
	}
}

func TestT232CommitCapacityFailureLeavesSpaceUnchanged(t *testing.T) {
	space, err := hatDataStructure.NewSpace(hatDataStructure.SpaceOptions{
		Memtx: hatDataStructure.MemtxSpaceOptions{MaxRecords: 1, MaxValueBytes: 8},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := space.Put("existing", []byte("old")); err != nil {
		t.Fatal(err)
	}
	tx, err := space.BeginTransaction()
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Put("new", []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); !errors.Is(err, hatDataStructure.ErrSpaceFull) {
		t.Fatalf("commit error = %v, want %v", err, hatDataStructure.ErrSpaceFull)
	}
	if got, ok := space.Get("existing"); !ok || string(got) != "old" {
		t.Fatalf("existing value = %q, %t, want old, true", got, ok)
	}
	if _, ok := space.Get("new"); ok {
		t.Fatal("new value became visible after capacity failure")
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
}

func TestT232TransactionCopiesStagedValues(t *testing.T) {
	space, err := hatDataStructure.NewSpace(t232SpaceOptions(hatDataStructure.SpaceEngineMemtx))
	if err != nil {
		t.Fatal(err)
	}
	tx, err := space.BeginTransaction()
	if err != nil {
		t.Fatal(err)
	}
	value := []byte("value")
	if err := tx.Put("key", value); err != nil {
		t.Fatal(err)
	}
	value[0] = 'x'
	got, ok := tx.Get("key")
	if !ok || string(got) != "value" {
		t.Fatalf("staged value = %q, %t, want value, true", got, ok)
	}
	got[0] = 'x'
	if got, ok := tx.Get("key"); !ok || string(got) != "value" {
		t.Fatalf("staged value after read mutation = %q, %t, want value, true", got, ok)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
}

func t232SpaceOptions(engine hatDataStructure.SpaceEngine) hatDataStructure.SpaceOptions {
	options := hatDataStructure.SpaceOptions{Engine: engine}
	if engine == hatDataStructure.SpaceEngineMemtx {
		options.Memtx = hatDataStructure.MemtxSpaceOptions{MaxRecords: 16, MaxValueBytes: 64}
		return options
	}
	options.Vinyl = hatDataStructure.LSMTableOptions{
		MemtableMaxRecords: 1 << 20,
		RunOptions:         hatDataStructure.SealedUpsertRunOptions{MaxRecords: 1 << 20},
	}
	return options
}
