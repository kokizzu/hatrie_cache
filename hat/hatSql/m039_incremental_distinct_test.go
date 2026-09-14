package hatSql

import (
	"errors"
	"math"
	"math/rand"
	"reflect"
	"sort"
	"testing"
)

func TestMZ039IncrementalDistinctMaintainsStateAcrossBatches(t *testing.T) {
	distinct := NewIncrementalDistinct()
	changes, err := distinct.Apply([]DifferentialRow{
		{Key: "b", Time: 1, Diff: 2, Row: Row{"value": "b"}},
		{Key: "a", Time: 1, Diff: 1, Row: Row{"value": "a"}},
	})
	if err != nil {
		t.Fatalf("initial Apply() error = %v", err)
	}
	if got, want := m039DistinctChangeMap(changes), map[string]int64{"a": 1, "b": 1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("initial changes = %#v, want %#v", got, want)
	}
	m039AssertDistinctSnapshot(t, distinct, []string{"a", "b"})

	changes, err = distinct.Apply([]DifferentialRow{{Key: "b", Time: 2, Diff: 3}})
	if err != nil {
		t.Fatalf("multiplicity increment error = %v", err)
	}
	if changes != nil {
		t.Fatalf("multiplicity increment changes = %#v, want nil", changes)
	}

	changes, err = distinct.Apply([]DifferentialRow{{Key: "a", Time: 3, Diff: -1}})
	if err != nil {
		t.Fatalf("first delete error = %v", err)
	}
	if len(changes) != 1 || changes[0].Key != "a" || changes[0].Diff != -1 || changes[0].Time != 3 {
		t.Fatalf("first delete changes = %#v, want a -1 at time 3", changes)
	}
	m039AssertDistinctSnapshot(t, distinct, []string{"b"})

	changes, err = distinct.Apply([]DifferentialRow{{Key: "b", Time: 4, Diff: -4}})
	if err != nil {
		t.Fatalf("partial delete error = %v", err)
	}
	if changes != nil {
		t.Fatalf("partial delete changes = %#v, want nil", changes)
	}
	changes, err = distinct.Apply([]DifferentialRow{{Key: "b", Time: 5, Diff: -1}})
	if err != nil {
		t.Fatalf("final delete error = %v", err)
	}
	if len(changes) != 1 || changes[0].Key != "b" || changes[0].Diff != -1 {
		t.Fatalf("final delete changes = %#v, want b -1", changes)
	}
	if got := distinct.Snapshot(); got != nil {
		t.Fatalf("final Snapshot() = %#v, want nil", got)
	}
}

func TestMZ039IncrementalDistinctIsAtomicAndOwnsRows(t *testing.T) {
	distinct := NewIncrementalDistinct()
	mutable := Row{"value": []byte("before")}
	if _, err := distinct.Apply([]DifferentialRow{{Key: "a", Diff: 2, Row: mutable}}); err != nil {
		t.Fatalf("seed Apply() error = %v", err)
	}
	mutable["value"].([]byte)[0] = 'x'

	before := distinct.Snapshot()
	if _, err := distinct.Apply([]DifferentialRow{{Key: "missing", Diff: -1}}); !errors.Is(err, ErrIncrementalDistinctNegativeMultiplicity) {
		t.Fatalf("missing delete error = %v, want ErrIncrementalDistinctNegativeMultiplicity", err)
	}
	if _, err := distinct.Apply([]DifferentialRow{{Key: "a", Diff: 1, Row: Row{"value": []byte("other")}}}); !errors.Is(err, ErrIncrementalDistinctRowConflict) {
		t.Fatalf("conflicting row error = %v, want ErrIncrementalDistinctRowConflict", err)
	}
	if got := distinct.Snapshot(); !reflect.DeepEqual(got, before) {
		t.Fatalf("state after rejected batches = %#v, want %#v", got, before)
	}
	if got := before[0].Row["value"].([]byte); string(got) != "before" {
		t.Fatalf("cloned value = %q, want before", got)
	}

	if _, err := distinct.Apply([]DifferentialRow{{Key: "large", Diff: math.MaxInt64, Row: Row{"value": "large"}}}); err != nil {
		t.Fatalf("max-count insert error = %v", err)
	}
	if _, err := distinct.Apply([]DifferentialRow{{Key: "large", Diff: 1}}); !errors.Is(err, ErrIncrementalDistinctOverflow) {
		t.Fatalf("count overflow error = %v, want ErrIncrementalDistinctOverflow", err)
	}
}

func TestMZ039IncrementalDistinctMatchesReferenceRandomized(t *testing.T) {
	distinct := NewIncrementalDistinct()
	type referenceEntry struct {
		row   Row
		count int64
	}
	reference := make(map[string]referenceEntry)
	random := rand.New(rand.NewSource(39039))
	for iteration := 0; iteration < 1000; iteration++ {
		key := m039DistinctKey(random.Intn(64))
		entry, exists := reference[key]
		var update DifferentialRow
		if !exists {
			entry = referenceEntry{row: Row{"value": int64(random.Intn(1000))}, count: 1}
			reference[key] = entry
			update = DifferentialRow{Key: key, Diff: 1, Row: entry.row}
		} else if random.Intn(3) == 0 {
			entry.count--
			if entry.count == 0 {
				delete(reference, key)
			} else {
				reference[key] = entry
			}
			update = DifferentialRow{Key: key, Diff: -1}
		} else {
			entry.count++
			reference[key] = entry
			update = DifferentialRow{Key: key, Diff: 1}
		}
		if _, err := distinct.Apply([]DifferentialRow{update}); err != nil {
			t.Fatalf("iteration %d Apply() error = %v", iteration, err)
		}
		keys := make([]string, 0, len(reference))
		for key := range reference {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		m039AssertDistinctSnapshot(t, distinct, keys)
	}
}

func TestIncrementalDistinctNilReceiver(t *testing.T) {
	var distinct *IncrementalDistinct
	if _, err := distinct.Apply(nil); !errors.Is(err, ErrIncrementalDistinctNil) {
		t.Fatalf("nil Apply() error = %v, want ErrIncrementalDistinctNil", err)
	}
	if got := distinct.Snapshot(); got != nil {
		t.Fatalf("nil Snapshot() = %#v, want nil", got)
	}
}

func m039DistinctChangeMap(rows []DifferentialRow) map[string]int64 {
	changes := make(map[string]int64, len(rows))
	for _, row := range rows {
		changes[row.Key] += row.Diff
	}
	return changes
}

func m039AssertDistinctSnapshot(t *testing.T, distinct *IncrementalDistinct, want []string) {
	t.Helper()
	got := distinct.Snapshot()
	if len(got) != len(want) {
		t.Fatalf("Snapshot() length = %d, want %d: %#v", len(got), len(want), got)
	}
	for index, key := range want {
		if got[index].Key != key || got[index].Diff != 1 {
			t.Fatalf("Snapshot()[%d] = %#v, want key=%q diff=1", index, got[index], key)
		}
	}
}

func m039DistinctKey(index int) string {
	return "key-" + string(rune('a'+index/26)) + string(rune('a'+index%26))
}
