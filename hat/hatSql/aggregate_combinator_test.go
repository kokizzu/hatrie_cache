package hatSql_test

import (
	"errors"
	"reflect"
	"sort"
	"sync"
	"testing"

	"hatrie_cache/hat/hatSql"
)

type testSumAggregateState struct {
	total int64
}

func (state *testSumAggregateState) Add(value interface{}) error {
	number, ok := value.(int64)
	if !ok {
		return errors.New("test sum expects int64")
	}
	state.total += number
	return nil
}

func (state *testSumAggregateState) Merge(other hatSql.SQLAggregateState) error {
	source, ok := other.(*testSumAggregateState)
	if !ok {
		return errors.New("test sum state type mismatch")
	}
	state.total += source.total
	return nil
}

func (state *testSumAggregateState) Finalize() (interface{}, error) {
	return state.total, nil
}

func TestSQLAggregateCombinatorRegistrySupportsStateMergeAndFinalize(t *testing.T) {
	combinator, err := hatSql.NewSQLAggregateCombinator("sum", func() hatSql.SQLAggregateState {
		return &testSumAggregateState{}
	})
	if err != nil {
		t.Fatalf("NewSQLAggregateCombinator() error = %v", err)
	}
	registry := hatSql.NewSQLAggregateCombinatorRegistry()
	if err := registry.Register(combinator); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	left, err := registry.NewState(" SUM ")
	if err != nil {
		t.Fatalf("NewState(left) error = %v", err)
	}
	right, err := registry.NewState("sum")
	if err != nil {
		t.Fatalf("NewState(right) error = %v", err)
	}
	if err := left.Add(int64(2)); err != nil {
		t.Fatal(err)
	}
	if err := left.Add(int64(3)); err != nil {
		t.Fatal(err)
	}
	if err := right.Add(int64(5)); err != nil {
		t.Fatal(err)
	}
	if err := left.Merge(right); err != nil {
		t.Fatalf("Merge() error = %v", err)
	}
	result, err := left.Finalize()
	if err != nil {
		t.Fatalf("Finalize() error = %v", err)
	}
	if result != int64(10) {
		t.Fatalf("Finalize() = %v, want 10", result)
	}
	if got := registry.Names(); !reflect.DeepEqual(got, []string{"SUM"}) {
		t.Fatalf("Names() = %#v, want [SUM]", got)
	}
}

func TestSQLAggregateCombinatorRegistryRejectsInvalidAndDuplicateDefinitions(t *testing.T) {
	registry := hatSql.NewSQLAggregateCombinatorRegistry()
	if err := registry.Register(hatSql.SQLAggregateCombinator{}); !errors.Is(err, hatSql.ErrSQLAggregateCombinatorInvalid) {
		t.Fatalf("invalid Register() error = %v, want ErrSQLAggregateCombinatorInvalid", err)
	}
	combinator, err := hatSql.NewSQLAggregateCombinator("count", func() hatSql.SQLAggregateState {
		return &testSumAggregateState{}
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(combinator); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(combinator); !errors.Is(err, hatSql.ErrSQLAggregateCombinatorExists) {
		t.Fatalf("duplicate Register() error = %v, want ErrSQLAggregateCombinatorExists", err)
	}
	if _, err := registry.NewState("missing"); !errors.Is(err, hatSql.ErrSQLAggregateCombinatorMissing) {
		t.Fatalf("missing NewState() error = %v, want ErrSQLAggregateCombinatorMissing", err)
	}
	if _, err := hatSql.NewSQLAggregateCombinator(" ", func() hatSql.SQLAggregateState {
		return &testSumAggregateState{}
	}); !errors.Is(err, hatSql.ErrSQLAggregateCombinatorInvalid) {
		t.Fatalf("empty name error = %v, want ErrSQLAggregateCombinatorInvalid", err)
	}
}

func TestSQLAggregateCombinatorRegistryNamesAreSorted(t *testing.T) {
	registry := hatSql.NewSQLAggregateCombinatorRegistry()
	for _, name := range []string{"zeta", "alpha", "middle"} {
		combinator, err := hatSql.NewSQLAggregateCombinator(name, func() hatSql.SQLAggregateState {
			return &testSumAggregateState{}
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := registry.Register(combinator); err != nil {
			t.Fatal(err)
		}
	}
	names := registry.Names()
	if !sort.StringsAreSorted(names) {
		t.Fatalf("Names() = %#v, want sorted", names)
	}
}

func TestSQLAggregateCombinatorRegistrySupportsConcurrentLookups(t *testing.T) {
	registry := hatSql.NewSQLAggregateCombinatorRegistry()
	combinator, err := hatSql.NewSQLAggregateCombinator("sum", func() hatSql.SQLAggregateState {
		return &testSumAggregateState{}
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(combinator); err != nil {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		group.Add(1)
		go func() {
			defer group.Done()
			for i := 0; i < 100; i++ {
				state, err := registry.NewState("sum")
				if err != nil {
					t.Errorf("NewState() error = %v", err)
					return
				}
				if err := state.Add(int64(i)); err != nil {
					t.Errorf("Add() error = %v", err)
					return
				}
			}
		}()
	}
	group.Wait()
}

func BenchmarkSQLAggregateCombinatorRegistryNewState(b *testing.B) {
	registry := hatSql.NewSQLAggregateCombinatorRegistry()
	combinator, err := hatSql.NewSQLAggregateCombinator("sum", func() hatSql.SQLAggregateState {
		return &testSumAggregateState{}
	})
	if err != nil {
		b.Fatal(err)
	}
	if err := registry.Register(combinator); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := registry.NewState("sum"); err != nil {
			b.Fatal(err)
		}
	}
}
