package hatSql

import (
	"errors"
	"math"
	"math/rand"
	"reflect"
	"sort"
	"testing"
)

func TestMZ040IncrementalPercentileMaintainsWeightedOrderStatistics(t *testing.T) {
	percentile, err := NewIncrementalPercentile(IncrementalPercentileDefinition{
		OrderKey: m040PercentileOrderKey,
	})
	if err != nil {
		t.Fatalf("NewIncrementalPercentile() error = %v", err)
	}
	if err := percentile.Apply([]DifferentialRow{
		{Key: "c", Time: 1, Diff: 3, Row: Row{"score": int64(30)}},
		{Key: "a", Time: 1, Diff: 2, Row: Row{"score": int64(10)}},
		{Key: "b", Time: 1, Diff: 1, Row: Row{"score": int64(20)}},
	}); err != nil {
		t.Fatalf("initial Apply() error = %v", err)
	}
	if got, want := percentile.TotalWeight(), uint64(6); got != want {
		t.Fatalf("TotalWeight() = %d, want %d", got, want)
	}
	m040AssertPercentileKey(t, percentile, 0, "a")
	m040AssertPercentileKey(t, percentile, 0.5, "b")
	m040AssertPercentileKey(t, percentile, 1, "c")
	m040AssertPercentileSnapshot(t, percentile, []m040PercentileExpected{{"a", 2}, {"b", 1}, {"c", 3}})

	if err := percentile.Apply([]DifferentialRow{
		{Key: "a", Diff: -2},
		{Key: "a", Time: 2, Diff: 1, Row: Row{"score": int64(5)}},
	}); err != nil {
		t.Fatalf("replace Apply() error = %v", err)
	}
	m040AssertPercentileKey(t, percentile, 0, "a")
	row, ok, err := percentile.Percentile(0)
	if err != nil || !ok || row.Row["score"] != int64(5) || row.Time != 2 {
		t.Fatalf("replacement Percentile(0) = %#v, %v, %v; want a score 5 at time 2", row, ok, err)
	}
}

func TestMZ040IncrementalPercentileUsesSQLTieOrderAndClonesRows(t *testing.T) {
	percentile, err := NewIncrementalPercentile(IncrementalPercentileDefinition{
		OrderKey: m040PercentileOrderKey,
	})
	if err != nil {
		t.Fatalf("NewIncrementalPercentile() error = %v", err)
	}
	mutable := Row{"score": int64(6), "payload": []byte("before")}
	if err := percentile.Apply([]DifferentialRow{
		{Key: "b", Diff: 1, Row: Row{"score": int64(5)}},
		{Key: "a", Diff: 1, Row: Row{"score": int64(5)}},
		{Key: "mutable", Diff: 1, Row: mutable},
	}); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	mutable["score"] = int64(99)
	mutable["payload"].([]byte)[0] = 'x'

	m040AssertPercentileKey(t, percentile, 0, "a")
	allRows := percentile.AllRows()
	if len(allRows) != 3 || allRows[2].Key != "mutable" || allRows[2].Row["score"] != int64(6) {
		t.Fatalf("AllRows() = %#v, want mutable score 6 last", allRows)
	}
	if got := allRows[2].Row["payload"].([]byte); string(got) != "before" {
		t.Fatalf("cloned payload = %q, want before", got)
	}
}

func TestMZ040IncrementalPercentileReplacementPreservesPartialMultiplicity(t *testing.T) {
	percentile, err := NewIncrementalPercentile(IncrementalPercentileDefinition{
		OrderKey: m040PercentileOrderKey,
	})
	if err != nil {
		t.Fatalf("NewIncrementalPercentile() error = %v", err)
	}
	row := Row{"score": int64(10)}
	if err := percentile.Apply([]DifferentialRow{{Key: "a", Diff: 3, Row: row}}); err != nil {
		t.Fatalf("seed Apply() error = %v", err)
	}
	if err := percentile.Apply([]DifferentialRow{
		{Key: "a", Diff: -1},
		{Key: "a", Diff: 1, Row: Row{"score": int64(10)}},
	}); err != nil {
		t.Fatalf("partial replacement Apply() error = %v", err)
	}
	if err := percentile.Apply([]DifferentialRow{
		{Key: "a", Diff: -2},
		{Key: "a", Diff: 1},
	}); err != nil {
		t.Fatalf("partial nil-row replacement Apply() error = %v", err)
	}
	m040AssertPercentileSnapshot(t, percentile, []m040PercentileExpected{{"a", 2}})
	before := percentile.AllRows()
	if err := percentile.Apply([]DifferentialRow{
		{Key: "a", Diff: -1},
		{Key: "a", Diff: 1, Row: Row{"score": int64(11)}},
	}); !errors.Is(err, ErrIncrementalPercentileRowConflict) {
		t.Fatalf("partial conflicting replacement error = %v, want ErrIncrementalPercentileRowConflict", err)
	}
	if got := percentile.AllRows(); !reflect.DeepEqual(got, before) {
		t.Fatalf("state after rejected replacement = %#v, want %#v", got, before)
	}
}

func TestMZ040IncrementalPercentileSingleUpdatePreservesOwnershipAndAtomicity(t *testing.T) {
	percentile, err := NewIncrementalPercentile(IncrementalPercentileDefinition{
		OrderKey: m040PercentileOrderKey,
	})
	if err != nil {
		t.Fatalf("NewIncrementalPercentile() error = %v", err)
	}
	mutable := Row{"score": int64(10), "payload": []byte("before")}
	if err := percentile.Apply([]DifferentialRow{{Key: "a", Time: 1, Diff: 1, Row: mutable}}); err != nil {
		t.Fatalf("single insert Apply() error = %v", err)
	}
	mutable["score"] = int64(99)
	mutable["payload"].([]byte)[0] = 'x'
	row, ok, err := percentile.Percentile(0)
	if err != nil || !ok || row.Key != "a" || row.Time != 1 || row.Row["score"] != int64(10) || string(row.Row["payload"].([]byte)) != "before" {
		t.Fatalf("insert Percentile(0) = %#v, %v, %v; want owned score 10 payload before", row, ok, err)
	}

	if err := percentile.Apply([]DifferentialRow{{Key: "a", Diff: 1}}); err != nil {
		t.Fatalf("single increment Apply() error = %v", err)
	}
	if got, want := percentile.TotalWeight(), uint64(2); got != want {
		t.Fatalf("TotalWeight() after increment = %d, want %d", got, want)
	}
	if err := percentile.Apply([]DifferentialRow{{Key: "a", Diff: -1}}); err != nil {
		t.Fatalf("single partial delete Apply() error = %v", err)
	}
	if got, want := percentile.TotalWeight(), uint64(1); got != want {
		t.Fatalf("TotalWeight() after partial delete = %d, want %d", got, want)
	}

	before := percentile.AllRows()
	if err := percentile.Apply([]DifferentialRow{{Key: "a", Diff: 1, Row: Row{"score": int64(11)}}}); !errors.Is(err, ErrIncrementalPercentileRowConflict) {
		t.Fatalf("single conflicting update error = %v, want ErrIncrementalPercentileRowConflict", err)
	}
	if got := percentile.AllRows(); !reflect.DeepEqual(got, before) {
		t.Fatalf("state after rejected single update = %#v, want %#v", got, before)
	}
	if err := percentile.Apply([]DifferentialRow{{Key: "missing", Diff: -1}}); !errors.Is(err, ErrIncrementalPercentileNegativeMultiplicity) {
		t.Fatalf("single missing delete error = %v, want ErrIncrementalPercentileNegativeMultiplicity", err)
	}
	if got := percentile.AllRows(); !reflect.DeepEqual(got, before) {
		t.Fatalf("state after rejected missing delete = %#v, want %#v", got, before)
	}
}

func TestMZ040IncrementalPercentileApplyIsAtomic(t *testing.T) {
	percentile, err := NewIncrementalPercentile(IncrementalPercentileDefinition{
		OrderKey: m040PercentileOrderKey,
	})
	if err != nil {
		t.Fatalf("NewIncrementalPercentile() error = %v", err)
	}
	if err := percentile.Apply([]DifferentialRow{{Key: "a", Diff: 1, Row: Row{"score": int64(10)}}}); err != nil {
		t.Fatalf("seed Apply() error = %v", err)
	}
	before := percentile.AllRows()
	if err := percentile.Apply([]DifferentialRow{{Key: "missing", Diff: -1}}); !errors.Is(err, ErrIncrementalPercentileNegativeMultiplicity) {
		t.Fatalf("missing delete error = %v, want ErrIncrementalPercentileNegativeMultiplicity", err)
	}
	if err := percentile.Apply([]DifferentialRow{{Key: "a", Diff: 1, Row: Row{"score": int64(11)}}}); !errors.Is(err, ErrIncrementalPercentileRowConflict) {
		t.Fatalf("conflicting row error = %v, want ErrIncrementalPercentileRowConflict", err)
	}
	if got := percentile.AllRows(); !reflect.DeepEqual(got, before) {
		t.Fatalf("state after rejected batches = %#v, want %#v", got, before)
	}
	if err := percentile.Apply([]DifferentialRow{{Key: "large", Diff: math.MaxInt64, Row: Row{"score": int64(1)}}}); err != nil {
		t.Fatalf("max-count insert error = %v", err)
	}
	if err := percentile.Apply([]DifferentialRow{{Key: "large", Diff: 1}}); !errors.Is(err, ErrIncrementalPercentileOverflow) {
		t.Fatalf("count overflow error = %v, want ErrIncrementalPercentileOverflow", err)
	}
	if _, ok, err := percentile.Percentile(-0.1); !errors.Is(err, ErrIncrementalPercentileInvalid) || ok {
		t.Fatalf("negative percentile = ok %v err %v, want invalid", ok, err)
	}
	if _, ok, err := percentile.Percentile(math.NaN()); !errors.Is(err, ErrIncrementalPercentileInvalid) || ok {
		t.Fatalf("NaN percentile = ok %v err %v, want invalid", ok, err)
	}
}

func TestMZ040IncrementalPercentileMatchesReferenceRandomized(t *testing.T) {
	percentile, err := NewIncrementalPercentile(IncrementalPercentileDefinition{
		OrderKey: m040PercentileOrderKey,
	})
	if err != nil {
		t.Fatalf("NewIncrementalPercentile() error = %v", err)
	}
	reference := make(map[string]m040PercentileReferenceEntry)
	random := rand.New(rand.NewSource(40040))
	for iteration := 0; iteration < 1000; iteration++ {
		key := m040PercentileKey(random.Intn(64))
		entry, exists := reference[key]
		var update DifferentialRow
		if !exists {
			entry = m040PercentileReferenceEntry{row: Row{"score": int64(random.Intn(1000))}, count: 1}
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
		if err := percentile.Apply([]DifferentialRow{update}); err != nil {
			t.Fatalf("iteration %d Apply() error = %v", iteration, err)
		}
		m040AssertPercentileReference(t, percentile, reference)
	}
}

func TestIncrementalPercentileNilReceiver(t *testing.T) {
	var percentile *IncrementalPercentile
	if err := percentile.Apply(nil); !errors.Is(err, ErrIncrementalPercentileNil) {
		t.Fatalf("nil Apply() error = %v, want ErrIncrementalPercentileNil", err)
	}
	if _, ok, err := percentile.Percentile(0.5); !errors.Is(err, ErrIncrementalPercentileNil) || ok {
		t.Fatalf("nil Percentile() = ok %v err %v, want nil error", ok, err)
	}
	if got := percentile.Snapshot(); got != nil {
		t.Fatalf("nil Snapshot() = %#v, want nil", got)
	}
}

func m040PercentileOrderKey(row Row) (interface{}, error) {
	return row["score"], nil
}

type m040PercentileExpected struct {
	key  string
	diff int64
}

type m040PercentileReferenceEntry struct {
	row   Row
	count int64
}

func m040PercentileKey(index int) string {
	return "key-" + string(rune('a'+index/26)) + string(rune('a'+index%26))
}

func m040AssertPercentileKey(t *testing.T, percentile *IncrementalPercentile, value float64, want string) {
	t.Helper()
	row, ok, err := percentile.Percentile(value)
	if err != nil || !ok || row.Key != want {
		t.Fatalf("Percentile(%v) = %#v, %v, %v; want key %q", value, row, ok, err, want)
	}
}

func m040AssertPercentileSnapshot(t *testing.T, percentile *IncrementalPercentile, want []m040PercentileExpected) {
	t.Helper()
	got := percentile.Snapshot()
	if len(got) != len(want) {
		t.Fatalf("Snapshot() length = %d, want %d: %#v", len(got), len(want), got)
	}
	for index, expected := range want {
		if got[index].Key != expected.key || got[index].Diff != expected.diff {
			t.Fatalf("Snapshot()[%d] = %#v, want key=%q diff=%d", index, got[index], expected.key, expected.diff)
		}
	}
}

func m040AssertPercentileReference(t *testing.T, percentile *IncrementalPercentile, reference map[string]m040PercentileReferenceEntry) {
	t.Helper()
	keys := make([]string, 0, len(reference))
	for key := range reference {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(left, right int) bool {
		leftScore := reference[keys[left]].row["score"].(int64)
		rightScore := reference[keys[right]].row["score"].(int64)
		if leftScore != rightScore {
			return leftScore < rightScore
		}
		return keys[left] < keys[right]
	})
	total := int64(0)
	for _, key := range keys {
		total += reference[key].count
	}
	got := percentile.Snapshot()
	if len(got) != len(keys) {
		t.Fatalf("Snapshot() length = %d, want %d", len(got), len(keys))
	}
	for index, key := range keys {
		if got[index].Key != key || got[index].Diff != reference[key].count {
			t.Fatalf("Snapshot()[%d] = %#v, want key=%q diff=%d", index, got[index], key, reference[key].count)
		}
	}
	if total == 0 {
		return
	}
	for _, value := range []float64{0, 0.5, 1} {
		rank := int64(math.Ceil(value * float64(total)))
		if rank == 0 {
			rank = 1
		}
		remaining := rank
		want := keys[len(keys)-1]
		for _, key := range keys {
			remaining -= reference[key].count
			if remaining <= 0 {
				want = key
				break
			}
		}
		m040AssertPercentileKey(t, percentile, value, want)
	}
}
