package hatSql

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"testing"
)

func TestM037KStatefulGroupCountMaintainsStateAcrossBatches(t *testing.T) {
	groupCount, err := NewIncrementalGroupCount(func(row SQLRow) string {
		return row["group"].(string)
	})
	if err != nil {
		t.Fatalf("NewIncrementalGroupCount() error = %v", err)
	}

	changes, err := groupCount.Apply([]DifferentialRow{
		{Key: "a-1", Time: 1, Diff: 2, Row: Row{"group": "a"}},
		{Key: "b-1", Time: 1, Diff: 1, Row: Row{"group": "b"}},
	})
	if err != nil {
		t.Fatalf("initial Apply() error = %v", err)
	}
	want := []DifferentialRow{
		{Key: "a", Time: 1, Diff: 1, Row: Row{"count": int64(2)}},
		{Key: "b", Time: 1, Diff: 1, Row: Row{"count": int64(1)}},
	}
	if !reflect.DeepEqual(changes, want) {
		t.Fatalf("initial changes = %#v, want %#v", changes, want)
	}

	changes, err = groupCount.Apply([]DifferentialRow{{Key: "a-2", Time: 2, Diff: 3, Row: Row{"group": "a"}}})
	if err != nil {
		t.Fatalf("increment Apply() error = %v", err)
	}
	want = []DifferentialRow{
		{Key: "a", Time: 2, Diff: -1, Row: Row{"count": int64(2)}},
		{Key: "a", Time: 2, Diff: 1, Row: Row{"count": int64(5)}},
	}
	if !reflect.DeepEqual(changes, want) {
		t.Fatalf("increment changes = %#v, want %#v", changes, want)
	}

	changes, err = groupCount.Apply([]DifferentialRow{{Key: "a-3", Time: 3, Diff: -4, Row: Row{"group": "a"}}})
	if err != nil {
		t.Fatalf("partial retraction Apply() error = %v", err)
	}
	want = []DifferentialRow{
		{Key: "a", Time: 3, Diff: -1, Row: Row{"count": int64(5)}},
		{Key: "a", Time: 3, Diff: 1, Row: Row{"count": int64(1)}},
	}
	if !reflect.DeepEqual(changes, want) {
		t.Fatalf("partial retraction changes = %#v, want %#v", changes, want)
	}

	changes, err = groupCount.Apply([]DifferentialRow{{Key: "b-2", Time: 4, Diff: -1, Row: Row{"group": "b"}}})
	if err != nil {
		t.Fatalf("group removal Apply() error = %v", err)
	}
	want = []DifferentialRow{{Key: "b", Time: 4, Diff: -1, Row: Row{"count": int64(1)}}}
	if !reflect.DeepEqual(changes, want) {
		t.Fatalf("group removal changes = %#v, want %#v", changes, want)
	}

	wantSnapshot := []DifferentialRow{{Key: "a", Time: 3, Diff: 1, Row: Row{"count": int64(1)}}}
	if got := groupCount.Snapshot(); !reflect.DeepEqual(got, wantSnapshot) {
		t.Fatalf("Snapshot() = %#v, want %#v", got, wantSnapshot)
	}

	changes, err = groupCount.Apply([]DifferentialRow{{Key: "a-4", Time: 5, Diff: -1, Row: Row{"group": "a"}}})
	if err != nil {
		t.Fatalf("final removal Apply() error = %v", err)
	}
	want = []DifferentialRow{{Key: "a", Time: 5, Diff: -1, Row: Row{"count": int64(1)}}}
	if !reflect.DeepEqual(changes, want) {
		t.Fatalf("final removal changes = %#v, want %#v", changes, want)
	}
	if got := groupCount.Snapshot(); got != nil {
		t.Fatalf("final Snapshot() = %#v, want nil", got)
	}
}

func TestM037KStatefulGroupCountIsAtomicAndRejectsInvalidCounts(t *testing.T) {
	groupCount, err := NewIncrementalGroupCount(func(row SQLRow) string {
		return row["group"].(string)
	})
	if err != nil {
		t.Fatalf("NewIncrementalGroupCount() error = %v", err)
	}
	if _, err := groupCount.Apply([]DifferentialRow{{Key: "seed", Time: 1, Diff: 2, Row: Row{"group": "a"}}}); err != nil {
		t.Fatalf("seed Apply() error = %v", err)
	}
	before := groupCount.Snapshot()

	if changes, err := groupCount.Apply([]DifferentialRow{
		{Key: "valid", Time: 2, Diff: 1, Row: Row{"group": "a"}},
		{Key: "invalid", Time: 2, Diff: -1, Row: Row{"group": "missing"}},
	}); !errors.Is(err, ErrIncrementalGroupCountNegative) || changes != nil {
		t.Fatalf("negative batch = %#v, %v; want nil and ErrIncrementalGroupCountNegative", changes, err)
	}
	if got := groupCount.Snapshot(); !reflect.DeepEqual(got, before) {
		t.Fatalf("state after rejected negative batch = %#v, want %#v", got, before)
	}

	if _, err := groupCount.Apply([]DifferentialRow{{Key: "max", Time: 3, Diff: math.MaxInt64, Row: Row{"group": "max"}}}); err != nil {
		t.Fatalf("max-count Apply() error = %v", err)
	}
	before = groupCount.Snapshot()
	if changes, err := groupCount.Apply([]DifferentialRow{{Key: "overflow", Time: 4, Diff: 1, Row: Row{"group": "max"}}}); !errors.Is(err, ErrIncrementalGroupCountOverflow) || changes != nil {
		t.Fatalf("overflow batch = %#v, %v; want nil and ErrIncrementalGroupCountOverflow", changes, err)
	}
	if got := groupCount.Snapshot(); !reflect.DeepEqual(got, before) {
		t.Fatalf("state after rejected overflow batch = %#v, want %#v", got, before)
	}
}

func TestM037KStatefulGroupCountRequiresKey(t *testing.T) {
	if _, err := NewIncrementalGroupCount(nil); !errors.Is(err, ErrDifferentialGroupByKeyRequired) {
		t.Fatalf("constructor error = %v, want ErrDifferentialGroupByKeyRequired", err)
	}
}

func TestM037KStatefulGroupCountNilReceiver(t *testing.T) {
	var groupCount *IncrementalGroupCount
	if _, err := groupCount.Apply(nil); !errors.Is(err, ErrIncrementalGroupCountNil) {
		t.Fatalf("nil Apply() error = %v, want ErrIncrementalGroupCountNil", err)
	}
	if got := groupCount.Snapshot(); got != nil {
		t.Fatalf("nil Snapshot() = %#v, want nil", got)
	}
}

func TestM037KStatefulGroupCountZeroValueRequiresKey(t *testing.T) {
	var groupCount IncrementalGroupCount
	if _, err := groupCount.Apply([]DifferentialRow{{Diff: 1, Row: Row{"group": "a"}}}); !errors.Is(err, ErrDifferentialGroupByKeyRequired) {
		t.Fatalf("zero-value Apply() error = %v, want ErrDifferentialGroupByKeyRequired", err)
	}
}

func TestM037KStatefulGroupCountMatchesReferenceRandomized(t *testing.T) {
	groupCount, err := NewIncrementalGroupCount(func(row SQLRow) string {
		return row["group"].(string)
	})
	if err != nil {
		t.Fatalf("NewIncrementalGroupCount() error = %v", err)
	}
	reference := make(map[string]int64)
	random := rand.New(rand.NewSource(37037))
	for iteration := 0; iteration < 2000; iteration++ {
		key := "group-" + string(rune('a'+random.Intn(8)))
		current := reference[key]
		diff := int64(1 + random.Intn(3))
		if current > 0 && random.Intn(3) == 0 {
			if diff > current {
				diff = current
			}
			diff = -diff
		}
		next := current + diff
		if next == 0 {
			delete(reference, key)
		} else {
			reference[key] = next
		}

		if _, err := groupCount.Apply([]DifferentialRow{{Key: "row", Time: uint64(iteration), Diff: diff, Row: Row{"group": key}}}); err != nil {
			t.Fatalf("iteration %d Apply() error = %v", iteration, err)
		}
		snapshot := groupCount.Snapshot()
		if len(snapshot) != len(reference) {
			t.Fatalf("iteration %d Snapshot() length = %d, want %d: %#v", iteration, len(snapshot), len(reference), snapshot)
		}
		for _, row := range snapshot {
			if row.Diff != 1 || row.Row["count"] != reference[row.Key] {
				t.Fatalf("iteration %d Snapshot() row = %#v, reference = %#v", iteration, row, reference)
			}
		}
	}
}

func ExampleIncrementalGroupCount() {
	groupCount, err := NewIncrementalGroupCount(func(row SQLRow) string {
		return row["team"].(string)
	})
	if err != nil {
		panic(err)
	}
	changes, err := groupCount.Apply([]DifferentialRow{
		{Key: "one", Time: 1, Diff: 1, Row: Row{"team": "red"}},
		{Key: "two", Time: 2, Diff: 1, Row: Row{"team": "red"}},
	})
	if err != nil {
		panic(err)
	}
	for _, change := range changes {
		if change.Diff > 0 {
			fmt.Println(change.Key, change.Row["count"].(int64))
		}
	}
	// Output:
	// red 1
	// red 2
}
