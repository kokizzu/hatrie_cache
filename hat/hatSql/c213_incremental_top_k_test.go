package hatSql

import (
	"errors"
	"math"
	"math/rand"
	"reflect"
	"sort"
	"testing"
)

func TestC213IncrementalTopKMaintainsSignedWeightedRows(t *testing.T) {
	topK, err := NewIncrementalTopK(IncrementalTopKDefinition{
		K:          3,
		OrderKey:   c213TopKOrderKey,
		Descending: true,
	})
	if err != nil {
		t.Fatalf("NewIncrementalTopK() error = %v", err)
	}

	initial := []DifferentialRow{
		{Key: "a", Time: 1, Diff: 1, Row: Row{"score": int64(10)}},
		{Key: "b", Time: 1, Diff: 1, Row: Row{"score": int64(9)}},
		{Key: "c", Time: 1, Diff: 1, Row: Row{"score": int64(8)}},
		{Key: "d", Time: 1, Diff: 1, Row: Row{"score": int64(7)}},
	}
	changes, err := topK.Apply(initial)
	if err != nil {
		t.Fatalf("initial Apply() error = %v", err)
	}
	if got, want := c213TopKChangeMap(changes), map[string]int64{"a": 1, "b": 1, "c": 1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("initial changes = %#v, want %#v", got, want)
	}
	c213AssertTopKSnapshot(t, topK, []c213TopKExpected{{"a", 1}, {"b", 1}, {"c", 1}})

	changes, err = topK.Apply([]DifferentialRow{{Key: "b", Time: 2, Diff: -1}})
	if err != nil {
		t.Fatalf("delete Apply() error = %v", err)
	}
	if got, want := c213TopKChangeMap(changes), map[string]int64{"b": -1, "d": 1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("delete changes = %#v, want %#v", got, want)
	}
	c213AssertTopKSnapshot(t, topK, []c213TopKExpected{{"a", 1}, {"c", 1}, {"d", 1}})

	changes, err = topK.Apply([]DifferentialRow{{Key: "e", Time: 3, Diff: 2, Row: Row{"score": int64(11)}}})
	if err != nil {
		t.Fatalf("weighted insert Apply() error = %v", err)
	}
	if got, want := c213TopKChangeMap(changes), map[string]int64{"c": -1, "d": -1, "e": 2}; !reflect.DeepEqual(got, want) {
		t.Fatalf("weighted insert changes = %#v, want %#v", got, want)
	}
	c213AssertTopKSnapshot(t, topK, []c213TopKExpected{{"e", 2}, {"a", 1}})
}

func TestC213IncrementalTopKUsesStableTieBreakAndClonesRows(t *testing.T) {
	topK, err := NewIncrementalTopK(IncrementalTopKDefinition{
		K:          2,
		OrderKey:   c213TopKOrderKey,
		Descending: true,
	})
	if err != nil {
		t.Fatalf("NewIncrementalTopK() error = %v", err)
	}
	mutable := Row{"score": int64(4), "payload": []byte("before")}
	if _, err := topK.Apply([]DifferentialRow{
		DifferentialRow{Key: "b", Diff: 1, Row: Row{"score": int64(5)}},
		DifferentialRow{Key: "a", Diff: 1, Row: Row{"score": int64(5)}},
		DifferentialRow{Key: "mutable", Diff: 1, Row: mutable},
	}); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	mutable["score"] = int64(99)
	mutable["payload"].([]byte)[0] = 'x'

	snapshot := topK.Snapshot()
	if got, want := []string{snapshot[0].Key, snapshot[1].Key}, []string{"a", "b"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("tie order = %#v, want %#v", got, want)
	}
	if got := snapshot[0].Row["score"]; got != int64(5) {
		t.Fatalf("snapshot score = %#v, want 5", got)
	}
	if got := topK.AllRows(); len(got) != 3 || got[2].Row["score"] != int64(4) {
		t.Fatalf("AllRows() = %#v, want mutable row retained with score 4", got)
	}
	if got := topK.AllRows()[2].Row["payload"].([]byte); string(got) != "before" {
		t.Fatalf("cloned payload = %q, want before", got)
	}
}

func TestC213IncrementalTopKApplyIsAtomic(t *testing.T) {
	topK, err := NewIncrementalTopK(IncrementalTopKDefinition{
		K:        1,
		OrderKey: c213TopKOrderKey,
	})
	if err != nil {
		t.Fatalf("NewIncrementalTopK() error = %v", err)
	}
	if _, err := topK.Apply([]DifferentialRow{{Key: "a", Diff: 1, Row: Row{"score": int64(10)}}}); err != nil {
		t.Fatalf("seed Apply() error = %v", err)
	}
	before := topK.AllRows()

	if _, err := topK.Apply([]DifferentialRow{{Key: "missing", Diff: -1}}); !errors.Is(err, ErrIncrementalTopKNegativeMultiplicity) {
		t.Fatalf("missing delete error = %v, want ErrIncrementalTopKNegativeMultiplicity", err)
	}
	if _, err := topK.Apply([]DifferentialRow{{Key: "a", Diff: 1, Row: Row{"score": int64(11)}}}); !errors.Is(err, ErrIncrementalTopKRowConflict) {
		t.Fatalf("conflicting insert error = %v, want ErrIncrementalTopKRowConflict", err)
	}
	if got := topK.AllRows(); !reflect.DeepEqual(got, before) {
		t.Fatalf("state after rejected batches = %#v, want %#v", got, before)
	}

	maxCount, err := topK.Apply([]DifferentialRow{{Key: "large", Diff: math.MaxInt64, Row: Row{"score": int64(1)}}})
	if err != nil {
		t.Fatalf("max-count insert error = %v", err)
	}
	if got, want := c213TopKChangeMap(maxCount), map[string]int64{"a": -1, "large": 1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("max-count changes = %#v, want %#v", got, want)
	}
	allRows := topK.AllRows()
	allRowsByKey := make(map[string]DifferentialRow, len(allRows))
	for _, row := range allRows {
		allRowsByKey[row.Key] = row
	}
	if len(allRows) != 2 || allRowsByKey["large"].Diff != math.MaxInt64 {
		t.Fatalf("AllRows() after max-count insert = %#v, want large max count", allRows)
	}
	if _, err := topK.Apply([]DifferentialRow{{Key: "large", Diff: 1}}); !errors.Is(err, ErrIncrementalTopKOverflow) {
		t.Fatalf("count overflow error = %v, want ErrIncrementalTopKOverflow", err)
	}
}

func TestC213IncrementalTopKValidatesDefinitionAndNilReceiver(t *testing.T) {
	if _, err := NewIncrementalTopK(IncrementalTopKDefinition{K: -1, OrderKey: c213TopKOrderKey}); !errors.Is(err, ErrIncrementalTopKInvalidLimit) {
		t.Fatalf("negative K error = %v, want ErrIncrementalTopKInvalidLimit", err)
	}
	if _, err := NewIncrementalTopK(IncrementalTopKDefinition{K: 1}); !errors.Is(err, ErrIncrementalTopKOrderKeyRequired) {
		t.Fatalf("nil callback error = %v, want ErrIncrementalTopKOrderKeyRequired", err)
	}
	var topK *IncrementalTopK
	if _, err := topK.Apply(nil); !errors.Is(err, ErrIncrementalTopKNil) {
		t.Fatalf("nil Apply() error = %v, want ErrIncrementalTopKNil", err)
	}
	if got := topK.Snapshot(); got != nil {
		t.Fatalf("nil Snapshot() = %#v, want nil", got)
	}
}

func TestC213IncrementalTopKMatchesReferenceRandomized(t *testing.T) {
	const (
		k    = 7
		seed = 213
	)
	topK, err := NewIncrementalTopK(IncrementalTopKDefinition{
		K:          k,
		OrderKey:   c213TopKOrderKey,
		Descending: true,
	})
	if err != nil {
		t.Fatalf("NewIncrementalTopK() error = %v", err)
	}
	reference := make(map[string]c213TopKReferenceEntry)
	for index := 0; index < 32; index++ {
		key := c213RandomTopKKey(index)
		row := Row{"score": int64(index % 11)}
		reference[key] = c213TopKReferenceEntry{row: row, count: 1}
	}
	initial := make([]DifferentialRow, 0, len(reference))
	for key, entry := range reference {
		initial = append(initial, DifferentialRow{Key: key, Diff: entry.count, Row: entry.row})
	}
	if _, err := topK.Apply(initial); err != nil {
		t.Fatalf("initial Apply() error = %v", err)
	}
	random := rand.New(rand.NewSource(seed))
	for iteration := 0; iteration < 1000; iteration++ {
		key := c213RandomTopKKey(random.Intn(64))
		entry, exists := reference[key]
		var update []DifferentialRow
		switch {
		case !exists:
			entry = c213TopKReferenceEntry{row: Row{"score": int64(random.Intn(100))}, count: 1}
			reference[key] = entry
			update = []DifferentialRow{{Key: key, Diff: 1, Row: entry.row}}
		case random.Intn(4) == 0:
			entry.count--
			if entry.count == 0 {
				delete(reference, key)
			} else {
				reference[key] = entry
			}
			update = []DifferentialRow{{Key: key, Diff: -1}}
		default:
			entry.count++
			reference[key] = entry
			update = []DifferentialRow{{Key: key, Diff: 1}}
		}
		if _, err := topK.Apply(update); err != nil {
			t.Fatalf("iteration %d Apply() error = %v", iteration, err)
		}
		c213AssertTopKMatchesReference(t, topK, reference)
	}
}

func c213TopKOrderKey(row Row) (interface{}, error) {
	return row["score"], nil
}

func c213RandomTopKKey(index int) string {
	return "key-" + string(rune('a'+index/26)) + string(rune('a'+index%26))
}

type c213TopKExpected struct {
	key  string
	diff int64
}

type c213TopKReferenceEntry struct {
	row   Row
	count int64
}

func c213AssertTopKSnapshot(t *testing.T, topK *IncrementalTopK, want []c213TopKExpected) {
	t.Helper()
	got := topK.Snapshot()
	if len(got) != len(want) {
		t.Fatalf("Snapshot() length = %d, want %d: %#v", len(got), len(want), got)
	}
	for index, expected := range want {
		if got[index].Key != expected.key || got[index].Diff != expected.diff {
			t.Fatalf("Snapshot()[%d] = %#v, want key=%q diff=%d", index, got[index], expected.key, expected.diff)
		}
	}
}

func c213TopKChangeMap(rows []DifferentialRow) map[string]int64 {
	changes := make(map[string]int64, len(rows))
	for _, row := range rows {
		changes[row.Key] += row.Diff
	}
	return changes
}

func c213AssertTopKMatchesReference(t *testing.T, topK *IncrementalTopK, reference map[string]c213TopKReferenceEntry) {
	t.Helper()
	keys := make([]string, 0, len(reference))
	for key := range reference {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(left, right int) bool {
		leftScore := reference[keys[left]].row["score"].(int64)
		rightScore := reference[keys[right]].row["score"].(int64)
		if leftScore != rightScore {
			return leftScore > rightScore
		}
		return keys[left] < keys[right]
	})
	want := make([]c213TopKExpected, 0, len(keys))
	remaining := int64(topK.k)
	for _, key := range keys {
		if remaining == 0 {
			break
		}
		count := reference[key].count
		if count > remaining {
			count = remaining
		}
		want = append(want, c213TopKExpected{key: key, diff: count})
		remaining -= count
	}
	c213AssertTopKSnapshot(t, topK, want)
}
