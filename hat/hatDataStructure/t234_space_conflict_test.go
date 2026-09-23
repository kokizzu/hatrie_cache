package hatDataStructure_test

import (
	"errors"
	"testing"

	"hatrie_cache/hat/hatDataStructure"
)

func TestT234ConflictDetectingTransactionsRejectCompetingWrites(t *testing.T) {
	for _, engine := range []hatDataStructure.SpaceEngine{
		hatDataStructure.SpaceEngineMemtx,
		hatDataStructure.SpaceEngineVinyl,
	} {
		t.Run(string(engine), func(t *testing.T) {
			space, err := hatDataStructure.NewSpace(t232SpaceOptions(engine))
			if err != nil {
				t.Fatal(err)
			}
			if err := space.Put("same", []byte("initial")); err != nil {
				t.Fatal(err)
			}

			first, err := space.BeginConflictDetectingTransaction()
			if err != nil {
				t.Fatal(err)
			}
			second, err := space.BeginConflictDetectingTransaction()
			if err != nil {
				t.Fatal(err)
			}
			if err := first.Put("same", []byte("first")); err != nil {
				t.Fatal(err)
			}
			if err := second.Put("same", []byte("second")); err != nil {
				t.Fatal(err)
			}
			if err := first.Commit(); err != nil {
				t.Fatal(err)
			}
			if err := second.Commit(); !errors.Is(err, hatDataStructure.ErrSpaceTransactionConflict) {
				t.Fatalf("competing commit = %v, want %v", err, hatDataStructure.ErrSpaceTransactionConflict)
			}
			if got, ok := space.Get("same"); !ok || string(got) != "first" {
				t.Fatalf("value after rejected conflict = %q, %t, want first, true", got, ok)
			}
			if err := second.Rollback(); err != nil {
				t.Fatal(err)
			}

			disjoint, err := space.BeginConflictDetectingTransaction()
			if err != nil {
				t.Fatal(err)
			}
			other, err := space.BeginConflictDetectingTransaction()
			if err != nil {
				t.Fatal(err)
			}
			if err := disjoint.Put("left", []byte("one")); err != nil {
				t.Fatal(err)
			}
			if err := other.Put("right", []byte("two")); err != nil {
				t.Fatal(err)
			}
			if err := disjoint.Commit(); err != nil {
				t.Fatal(err)
			}
			if err := other.Commit(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestT234RegularTransactionsKeepLastWriterWins(t *testing.T) {
	space, err := hatDataStructure.NewSpace(t232SpaceOptions(hatDataStructure.SpaceEngineMemtx))
	if err != nil {
		t.Fatal(err)
	}
	first, err := space.BeginTransaction()
	if err != nil {
		t.Fatal(err)
	}
	second, err := space.BeginTransaction()
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Put("same", []byte("first")); err != nil {
		t.Fatal(err)
	}
	if err := second.Put("same", []byte("second")); err != nil {
		t.Fatal(err)
	}
	if err := first.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := second.Commit(); err != nil {
		t.Fatal(err)
	}
	if got, ok := space.Get("same"); !ok || string(got) != "second" {
		t.Fatalf("regular transaction value = %q, %t, want second, true", got, ok)
	}
}

func TestT234NestedConflictTransactionInheritsDetection(t *testing.T) {
	space, err := hatDataStructure.NewSpace(t232SpaceOptions(hatDataStructure.SpaceEngineMemtx))
	if err != nil {
		t.Fatal(err)
	}
	parent, err := space.BeginConflictDetectingTransaction()
	if err != nil {
		t.Fatal(err)
	}
	child, err := parent.BeginNested()
	if err != nil {
		t.Fatal(err)
	}
	if err := child.Put("same", []byte("child")); err != nil {
		t.Fatal(err)
	}
	if err := child.Commit(); err != nil {
		t.Fatal(err)
	}
	competitor, err := space.BeginConflictDetectingTransaction()
	if err != nil {
		t.Fatal(err)
	}
	if err := competitor.Put("same", []byte("competitor")); err != nil {
		t.Fatal(err)
	}
	if err := competitor.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := parent.Commit(); !errors.Is(err, hatDataStructure.ErrSpaceTransactionConflict) {
		t.Fatalf("nested conflict commit = %v, want %v", err, hatDataStructure.ErrSpaceTransactionConflict)
	}
	if err := parent.Rollback(); err != nil {
		t.Fatal(err)
	}
}
