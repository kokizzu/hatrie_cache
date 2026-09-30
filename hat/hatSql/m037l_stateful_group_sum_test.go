package hatSql

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"sort"
	"testing"
)

func m037LTestGroupKey(row SQLRow) string {
	return row["group"].(string)
}

func m037LTestValue(row SQLRow) (int64, error) {
	return row["value"].(int64), nil
}

func TestM037LStatefulGroupSumMaintainsStateAcrossBatches(t *testing.T) {
	groupSum, err := NewIncrementalGroupSumInt64(m037LTestGroupKey, m037LTestValue)
	if err != nil {
		t.Fatalf("NewIncrementalGroupSumInt64() error = %v", err)
	}

	changes, err := groupSum.Apply([]DifferentialRow{
		{Key: "a-1", Time: 1, Diff: 2, Row: Row{"group": "a", "value": int64(4)}},
		{Key: "b-1", Time: 1, Diff: 1, Row: Row{"group": "b", "value": int64(3)}},
	})
	if err != nil {
		t.Fatalf("initial Apply() error = %v", err)
	}
	want := []DifferentialRow{
		{Key: "a", Time: 1, Diff: 1, Row: Row{"sum": int64(8)}},
		{Key: "b", Time: 1, Diff: 1, Row: Row{"sum": int64(3)}},
	}
	if !reflect.DeepEqual(changes, want) {
		t.Fatalf("initial changes = %#v, want %#v", changes, want)
	}

	changes, err = groupSum.Apply([]DifferentialRow{{Key: "a-2", Time: 2, Diff: 3, Row: Row{"group": "a", "value": int64(4)}}})
	if err != nil {
		t.Fatalf("increment Apply() error = %v", err)
	}
	want = []DifferentialRow{
		{Key: "a", Time: 2, Diff: -1, Row: Row{"sum": int64(8)}},
		{Key: "a", Time: 2, Diff: 1, Row: Row{"sum": int64(20)}},
	}
	if !reflect.DeepEqual(changes, want) {
		t.Fatalf("increment changes = %#v, want %#v", changes, want)
	}

	changes, err = groupSum.Apply([]DifferentialRow{{Key: "a-3", Time: 3, Diff: -4, Row: Row{"group": "a", "value": int64(4)}}})
	if err != nil {
		t.Fatalf("partial retraction Apply() error = %v", err)
	}
	want = []DifferentialRow{
		{Key: "a", Time: 3, Diff: -1, Row: Row{"sum": int64(20)}},
		{Key: "a", Time: 3, Diff: 1, Row: Row{"sum": int64(4)}},
	}
	if !reflect.DeepEqual(changes, want) {
		t.Fatalf("partial retraction changes = %#v, want %#v", changes, want)
	}

	changes, err = groupSum.Apply([]DifferentialRow{{Key: "b-2", Time: 4, Diff: -1, Row: Row{"group": "b", "value": int64(3)}}})
	if err != nil {
		t.Fatalf("final retraction Apply() error = %v", err)
	}
	want = []DifferentialRow{{Key: "b", Time: 4, Diff: -1, Row: Row{"sum": int64(3)}}}
	if !reflect.DeepEqual(changes, want) {
		t.Fatalf("final retraction changes = %#v, want %#v", changes, want)
	}

	want = []DifferentialRow{{Key: "a", Time: 3, Diff: 1, Row: Row{"sum": int64(4)}}}
	if got := groupSum.Snapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("Snapshot() = %#v, want %#v", got, want)
	}
}

func TestM037LStatefulGroupSumPreservesZeroSumPresence(t *testing.T) {
	groupSum, err := NewIncrementalGroupSumInt64(m037LTestGroupKey, m037LTestValue)
	if err != nil {
		t.Fatalf("constructor error = %v", err)
	}

	changes, err := groupSum.Apply([]DifferentialRow{{Time: 1, Diff: 1, Row: Row{"group": "zero", "value": int64(0)}}})
	if err != nil {
		t.Fatalf("zero-sum insertion error = %v", err)
	}
	want := []DifferentialRow{{Key: "zero", Time: 1, Diff: 1, Row: Row{"sum": int64(0)}}}
	if !reflect.DeepEqual(changes, want) {
		t.Fatalf("zero-sum changes = %#v, want %#v", changes, want)
	}
	if got := groupSum.Snapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("zero-sum Snapshot() = %#v, want %#v", got, want)
	}

	changes, err = groupSum.Apply([]DifferentialRow{{Time: 2, Diff: -1, Row: Row{"group": "zero", "value": int64(0)}}})
	if err != nil {
		t.Fatalf("zero-sum retraction error = %v", err)
	}
	want = []DifferentialRow{{Key: "zero", Time: 2, Diff: -1, Row: Row{"sum": int64(0)}}}
	if !reflect.DeepEqual(changes, want) {
		t.Fatalf("zero-sum retraction = %#v, want %#v", changes, want)
	}
	if got := groupSum.Snapshot(); got != nil {
		t.Fatalf("Snapshot() after removal = %#v, want nil", got)
	}
}

func TestM037LStatefulGroupSumResetsWhenGroupReentersWithinBatch(t *testing.T) {
	groupSum, err := NewIncrementalGroupSumInt64(m037LTestGroupKey, m037LTestValue)
	if err != nil {
		t.Fatalf("constructor error = %v", err)
	}
	if _, err := groupSum.Apply([]DifferentialRow{{Time: 1, Diff: 1, Row: Row{"group": "a", "value": int64(10)}}}); err != nil {
		t.Fatalf("initial Apply() error = %v", err)
	}

	changes, err := groupSum.Apply([]DifferentialRow{
		{Time: 2, Diff: -1, Row: Row{"group": "a", "value": int64(10)}},
		{Time: 2, Diff: 1, Row: Row{"group": "a", "value": int64(7)}},
	})
	if err != nil {
		t.Fatalf("leave-and-reenter Apply() error = %v", err)
	}
	want := []DifferentialRow{
		{Key: "a", Time: 2, Diff: -1, Row: Row{"sum": int64(10)}},
		{Key: "a", Time: 2, Diff: 1, Row: Row{"sum": int64(7)}},
	}
	if !reflect.DeepEqual(changes, want) {
		t.Fatalf("leave-and-reenter changes = %#v, want %#v", changes, want)
	}
	if got := groupSum.Snapshot(); !reflect.DeepEqual(got, []DifferentialRow{{Key: "a", Time: 2, Diff: 1, Row: Row{"sum": int64(7)}}}) {
		t.Fatalf("leave-and-reenter Snapshot() = %#v", got)
	}
}

func TestM037LStatefulGroupSumRejectsInvalidBatchesAtomically(t *testing.T) {
	groupSum, err := NewIncrementalGroupSumInt64(m037LTestGroupKey, m037LTestValue)
	if err != nil {
		t.Fatalf("constructor error = %v", err)
	}
	if _, err := groupSum.Apply([]DifferentialRow{{Time: 1, Diff: 1, Row: Row{"group": "a", "value": int64(10)}}}); err != nil {
		t.Fatalf("initial Apply() error = %v", err)
	}
	before := groupSum.Snapshot()

	_, err = groupSum.Apply([]DifferentialRow{
		{Time: 2, Diff: 1, Row: Row{"group": "b", "value": int64(20)}},
		{Time: 2, Diff: -2, Row: Row{"group": "a", "value": int64(10)}},
	})
	if !errors.Is(err, ErrIncrementalGroupSumInt64Negative) {
		t.Fatalf("negative-count error = %v, want ErrIncrementalGroupSumInt64Negative", err)
	}
	if got := groupSum.Snapshot(); !reflect.DeepEqual(got, before) {
		t.Fatalf("state after negative-count failure = %#v, want %#v", got, before)
	}

	overflow, err := NewIncrementalGroupSumInt64(m037LTestGroupKey, m037LTestValue)
	if err != nil {
		t.Fatalf("overflow constructor error = %v", err)
	}
	if _, err := overflow.Apply([]DifferentialRow{{Time: 1, Diff: 1, Row: Row{"group": "max", "value": int64(math.MaxInt64)}}}); err != nil {
		t.Fatalf("overflow setup error = %v", err)
	}
	before = overflow.Snapshot()
	_, err = overflow.Apply([]DifferentialRow{{Time: 2, Diff: 1, Row: Row{"group": "max", "value": int64(1)}}})
	if !errors.Is(err, ErrIncrementalGroupSumInt64SumOverflow) {
		t.Fatalf("sum-overflow error = %v, want ErrIncrementalGroupSumInt64SumOverflow", err)
	}
	if got := overflow.Snapshot(); !reflect.DeepEqual(got, before) {
		t.Fatalf("state after sum-overflow failure = %#v, want %#v", got, before)
	}

	countOverflow, err := NewIncrementalGroupSumInt64(m037LTestGroupKey, m037LTestValue)
	if err != nil {
		t.Fatalf("count-overflow constructor error = %v", err)
	}
	if _, err := countOverflow.Apply([]DifferentialRow{{Time: 1, Diff: math.MaxInt64, Row: Row{"group": "count", "value": int64(0)}}}); err != nil {
		t.Fatalf("count-overflow setup error = %v", err)
	}
	before = countOverflow.Snapshot()
	_, err = countOverflow.Apply([]DifferentialRow{{Time: 2, Diff: 1, Row: Row{"group": "count", "value": int64(0)}}})
	if !errors.Is(err, ErrIncrementalGroupSumInt64CountOverflow) {
		t.Fatalf("count-overflow error = %v, want ErrIncrementalGroupSumInt64CountOverflow", err)
	}
	if got := countOverflow.Snapshot(); !reflect.DeepEqual(got, before) {
		t.Fatalf("state after count-overflow failure = %#v, want %#v", got, before)
	}
}

func TestM037LStatefulGroupSumPropagatesValueErrorsAtomically(t *testing.T) {
	wantErr := errors.New("bad value")
	groupSum, err := NewIncrementalGroupSumInt64(m037LTestGroupKey, func(row SQLRow) (int64, error) {
		if row["bad"] == true {
			return 0, wantErr
		}
		return row["value"].(int64), nil
	})
	if err != nil {
		t.Fatalf("constructor error = %v", err)
	}
	if _, err := groupSum.Apply([]DifferentialRow{{Time: 1, Diff: 1, Row: Row{"group": "a", "value": int64(5)}}}); err != nil {
		t.Fatalf("initial Apply() error = %v", err)
	}
	before := groupSum.Snapshot()
	_, err = groupSum.Apply([]DifferentialRow{{Time: 2, Diff: 1, Row: Row{"group": "b", "value": int64(9), "bad": true}}})
	if !errors.Is(err, wantErr) {
		t.Fatalf("value error = %v, want wrapped sentinel", err)
	}
	if got := groupSum.Snapshot(); !reflect.DeepEqual(got, before) {
		t.Fatalf("state after value failure = %#v, want %#v", got, before)
	}
}

func TestM037LStatefulGroupSumRequiresCallbacksAndHandlesZeroValue(t *testing.T) {
	if _, err := NewIncrementalGroupSumInt64(nil, m037LTestValue); !errors.Is(err, ErrDifferentialGroupByKeyRequired) {
		t.Fatalf("nil key error = %v, want ErrDifferentialGroupByKeyRequired", err)
	}
	if _, err := NewIncrementalGroupSumInt64(m037LTestGroupKey, nil); !errors.Is(err, ErrDifferentialGroupByValueRequired) {
		t.Fatalf("nil value error = %v, want ErrDifferentialGroupByValueRequired", err)
	}

	var zero IncrementalGroupSumInt64
	if _, err := zero.Apply([]DifferentialRow{{Diff: 1, Row: Row{"group": "a", "value": int64(1)}}}); !errors.Is(err, ErrDifferentialGroupByKeyRequired) {
		t.Fatalf("zero-value error = %v, want ErrDifferentialGroupByKeyRequired", err)
	}
	var nilOperator *IncrementalGroupSumInt64
	if _, err := nilOperator.Apply(nil); !errors.Is(err, ErrIncrementalGroupSumInt64Nil) {
		t.Fatalf("nil receiver error = %v, want ErrIncrementalGroupSumInt64Nil", err)
	}
}

func TestM037LStatefulGroupSumSnapshotIsSortedAndDetached(t *testing.T) {
	groupSum, err := NewIncrementalGroupSumInt64(m037LTestGroupKey, m037LTestValue)
	if err != nil {
		t.Fatalf("constructor error = %v", err)
	}
	if _, err := groupSum.Apply([]DifferentialRow{
		{Time: 1, Diff: 1, Row: Row{"group": "c", "value": int64(3)}},
		{Time: 2, Diff: 1, Row: Row{"group": "a", "value": int64(1)}},
		{Time: 3, Diff: 1, Row: Row{"group": "b", "value": int64(2)}},
	}); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}

	snapshot := groupSum.Snapshot()
	if got := []string{snapshot[0].Key, snapshot[1].Key, snapshot[2].Key}; !reflect.DeepEqual(got, []string{"a", "b", "c"}) {
		t.Fatalf("snapshot keys = %#v, want [a b c]", got)
	}
	snapshot[0].Row["sum"] = int64(99)
	if got := groupSum.Snapshot()[0].Row["sum"]; got != int64(1) {
		t.Fatalf("state was aliased through Snapshot(): got %v, want 1", got)
	}
}

func TestM037LStatefulGroupSumMatchesRandomizedReference(t *testing.T) {
	groupSum, err := NewIncrementalGroupSumInt64(m037LTestGroupKey, m037LTestValue)
	if err != nil {
		t.Fatalf("constructor error = %v", err)
	}
	type referenceEntry struct {
		count int64
		sum   int64
		time  uint64
	}
	reference := make(map[string]referenceEntry)
	random := rand.New(rand.NewSource(37))
	groups := []string{"a", "b", "c", "d"}
	for batch := 1; batch <= 200; batch++ {
		updates := make([]DifferentialRow, 0, 4)
		for item := 0; item < 1+random.Intn(4); item++ {
			group := groups[random.Intn(len(groups))]
			entry := reference[group]
			diff := int64(1)
			if entry.count > 0 && random.Intn(3) == 0 {
				diff = -1
			}
			value := int64(random.Intn(11) - 5)
			updates = append(updates, DifferentialRow{
				Time: uint64(batch),
				Diff: diff,
				Row:  Row{"group": group, "value": value},
			})
			entry.count += diff
			entry.sum += value * diff
			entry.time = uint64(batch)
			if entry.count == 0 {
				delete(reference, group)
			} else {
				reference[group] = entry
			}
		}
		if _, err := groupSum.Apply(updates); err != nil {
			t.Fatalf("batch %d Apply() error = %v", batch, err)
		}

		keys := make([]string, 0, len(reference))
		for key := range reference {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		want := make([]DifferentialRow, 0, len(keys))
		for _, key := range keys {
			entry := reference[key]
			want = append(want, DifferentialRow{Key: key, Time: entry.time, Diff: 1, Row: Row{"sum": entry.sum}})
		}
		if got := groupSum.Snapshot(); !reflect.DeepEqual(got, want) {
			t.Fatalf("batch %d Snapshot() = %#v, want %#v", batch, got, want)
		}
	}
}

func ExampleNewIncrementalGroupSumInt64() {
	groupSum, _ := NewIncrementalGroupSumInt64(
		func(row SQLRow) string { return row["group"].(string) },
		func(row SQLRow) (int64, error) { return row["value"].(int64), nil },
	)
	_, _ = groupSum.Apply([]DifferentialRow{{Time: 1, Diff: 1, Row: Row{"group": "a", "value": int64(7)}}})
	snapshot := groupSum.Snapshot()
	fmt.Printf("%s=%d\n", snapshot[0].Key, snapshot[0].Row["sum"])
	// Output:
	// a=7
}
