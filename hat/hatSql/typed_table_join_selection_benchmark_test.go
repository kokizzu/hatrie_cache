package hatSql_test

import (
	"testing"

	"hatrie_cache/hat/hatSql"
)

func BenchmarkMU45JoinArrangementSelection(b *testing.B) {
	left, right := newJoinSelectionTables(b)
	arrangements, err := hatSql.NewTypedTableJoinArrangements(left, right)
	if err != nil {
		b.Fatal(err)
	}
	definition := hatSql.TypedTableJoinDefinition{LeftField: "id", RightField: "id"}
	seed, err := arrangements.Acquire(definition)
	if err != nil {
		b.Fatal(err)
	}
	defer seed.Release()
	candidates := []hatSql.TypedTableJoinArrangementCandidate{
		{Definition: hatSql.TypedTableJoinDefinition{LeftField: "team", RightField: "team"}, EstimatedStateRows: 100},
		{Definition: definition, EstimatedStateRows: 1},
	}

	b.Run("exact_acquire", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			lease, err := arrangements.Acquire(definition)
			if err != nil {
				b.Fatal(err)
			}
			lease.Release()
		}
	})
	b.Run("acquire_best", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			lease, selection, err := arrangements.AcquireBest(candidates)
			if err != nil {
				b.Fatal(err)
			}
			if selection.Definition != definition || !selection.Reused {
				b.Fatalf("selection = %#v", selection)
			}
			lease.Release()
		}
	})
}
