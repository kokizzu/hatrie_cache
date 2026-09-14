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

type m029ReferenceEntry struct {
	row   Row
	count int64
}

func TestIncrementalIntervalJoinWeightedOverlap(t *testing.T) {
	join, err := NewIncrementalIntervalJoin(m029TestIntervalJoinDefinition())
	if err != nil {
		t.Fatalf("create interval join: %v", err)
	}

	deltas, err := join.Apply([]IncrementalIntervalJoinUpdate{
		{Side: IncrementalIntervalJoinLeft, Row: DifferentialRow{
			Key: "l1", Diff: 2, Row: Row{"id": "l1", "group": "g", "start": int64(0), "end": int64(10)},
		}},
		{Side: IncrementalIntervalJoinRight, Row: DifferentialRow{
			Key: "r1", Diff: 3, Row: Row{"id": "r1", "group": "g", "start": int64(5), "end": int64(12)},
		}},
	})
	if err != nil {
		t.Fatalf("apply weighted overlap: %v", err)
	}
	want := []DifferentialRow{{Key: "l1\x00r1", Diff: 6, Row: Row{"left": "l1", "right": "r1"}}}
	if !reflect.DeepEqual(deltas, want) {
		t.Fatalf("weighted deltas = %#v, want %#v", deltas, want)
	}

	deltas, err = join.Apply([]IncrementalIntervalJoinUpdate{{
		Side: IncrementalIntervalJoinRight,
		Row:  DifferentialRow{Key: "r2", Diff: 1, Row: Row{"id": "r2", "group": "g", "start": int64(10), "end": int64(20)}},
	}})
	if err != nil {
		t.Fatalf("apply touching interval: %v", err)
	}
	if len(deltas) != 0 {
		t.Fatalf("touching interval produced deltas: %#v", deltas)
	}

	deltas, err = join.Apply([]IncrementalIntervalJoinUpdate{{
		Side: IncrementalIntervalJoinRight,
		Row:  DifferentialRow{Key: "r3", Diff: 1, Row: Row{"id": "r3", "group": "g", "start": int64(9), "end": int64(11)}},
	}})
	if err != nil {
		t.Fatalf("apply second overlap: %v", err)
	}
	want = []DifferentialRow{{Key: "l1\x00r3", Diff: 2, Row: Row{"left": "l1", "right": "r3"}}}
	if !reflect.DeepEqual(deltas, want) {
		t.Fatalf("second deltas = %#v, want %#v", deltas, want)
	}

	deltas, err = join.Apply([]IncrementalIntervalJoinUpdate{{
		Side: IncrementalIntervalJoinRight,
		Row:  DifferentialRow{Key: "r1", Diff: -1},
	}})
	if err != nil {
		t.Fatalf("retract overlap: %v", err)
	}
	want = []DifferentialRow{{Key: "l1\x00r1", Diff: -2, Row: Row{"left": "l1", "right": "r1"}}}
	if !reflect.DeepEqual(deltas, want) {
		t.Fatalf("retraction deltas = %#v, want %#v", deltas, want)
	}

	snapshot, err := join.Snapshot()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	want = []DifferentialRow{
		{Key: "l1\x00r1", Diff: 4, Row: Row{"left": "l1", "right": "r1"}},
		{Key: "l1\x00r3", Diff: 2, Row: Row{"left": "l1", "right": "r3"}},
	}
	if !reflect.DeepEqual(snapshot, want) {
		t.Fatalf("snapshot = %#v, want %#v", snapshot, want)
	}
}

func TestIncrementalIntervalJoinReplacementAndAtomicValidation(t *testing.T) {
	join, err := NewIncrementalIntervalJoin(m029TestIntervalJoinDefinition())
	if err != nil {
		t.Fatalf("create interval join: %v", err)
	}
	_, err = join.Apply([]IncrementalIntervalJoinUpdate{
		{Side: IncrementalIntervalJoinLeft, Row: DifferentialRow{
			Key: "l1", Diff: 2, Row: Row{"id": "l1", "group": "g", "start": int64(0), "end": int64(10)},
		}},
		{Side: IncrementalIntervalJoinRight, Row: DifferentialRow{
			Key: "r1", Diff: 1, Row: Row{"id": "r1", "group": "g", "start": int64(1), "end": int64(9)},
		}},
	})
	if err != nil {
		t.Fatalf("seed interval join: %v", err)
	}
	_, err = join.Apply([]IncrementalIntervalJoinUpdate{
		{Side: IncrementalIntervalJoinLeft, Row: DifferentialRow{
			Key: "l1", Diff: -2,
		}},
		{Side: IncrementalIntervalJoinLeft, Row: DifferentialRow{
			Key: "l1", Diff: 1, Row: Row{"id": "l1", "group": "g", "start": int64(20), "end": int64(30)},
		}},
	})
	if err != nil {
		t.Fatalf("replace interval row: %v", err)
	}
	snapshot, err := join.Snapshot()
	if err != nil {
		t.Fatalf("snapshot after replacement: %v", err)
	}
	if len(snapshot) != 0 {
		t.Fatalf("replacement retained stale overlap: %#v", snapshot)
	}

	_, err = join.Apply([]IncrementalIntervalJoinUpdate{{
		Side: IncrementalIntervalJoinRight,
		Row:  DifferentialRow{Key: "r2", Diff: 1, Row: Row{"id": "r2", "group": "g", "start": int64(25), "end": int64(26)}},
	}})
	if err != nil {
		t.Fatalf("apply replacement overlap: %v", err)
	}

	_, err = join.Apply([]IncrementalIntervalJoinUpdate{{
		Side: IncrementalIntervalJoinLeft,
		Row:  DifferentialRow{Key: "l1", Diff: 1, Row: Row{"id": "l1", "group": "g", "start": int64(30), "end": int64(31)}},
	}})
	if !errors.Is(err, ErrIncrementalIntervalJoinRowConflict) {
		t.Fatalf("conflicting active row error = %v, want row conflict", err)
	}
	afterConflict, err := join.Snapshot()
	if err != nil {
		t.Fatalf("snapshot after conflict: %v", err)
	}
	want := []DifferentialRow{{Key: "l1\x00r2", Diff: 1, Row: Row{"left": "l1", "right": "r2"}}}
	if !reflect.DeepEqual(afterConflict, want) {
		t.Fatalf("state after conflict = %#v, want %#v", afterConflict, want)
	}

	_, err = join.Apply([]IncrementalIntervalJoinUpdate{{
		Side: IncrementalIntervalJoinRight,
		Row:  DifferentialRow{Key: "r2", Diff: -2},
	}})
	if !errors.Is(err, ErrIncrementalIntervalJoinMultiplicity) {
		t.Fatalf("excess retraction error = %v, want multiplicity", err)
	}
	unchanged, err := join.Snapshot()
	if err != nil {
		t.Fatalf("snapshot after excess retraction: %v", err)
	}
	if !reflect.DeepEqual(unchanged, want) {
		t.Fatalf("state after excess retraction = %#v, want %#v", unchanged, want)
	}

	_, err = join.Apply([]IncrementalIntervalJoinUpdate{{
		Side: IncrementalIntervalJoinRight,
		Row:  DifferentialRow{Key: "r2", Diff: -1, Row: Row{"id": "r2", "group": "g", "start": int64(24), "end": int64(26)}},
	}})
	if !errors.Is(err, ErrIncrementalIntervalJoinRowConflict) {
		t.Fatalf("conflicting retraction error = %v, want row conflict", err)
	}

	if _, err = join.Apply([]IncrementalIntervalJoinUpdate{{
		Side: IncrementalIntervalJoinRight,
		Row:  DifferentialRow{Key: "overflow", Diff: math.MaxInt64, Row: Row{"id": "overflow", "group": "overflow", "start": int64(0), "end": int64(1)}},
	}}); err != nil {
		t.Fatalf("seed overflow interval row: %v", err)
	}
	if _, err = join.Apply([]IncrementalIntervalJoinUpdate{{
		Side: IncrementalIntervalJoinLeft,
		Row:  DifferentialRow{Key: "overflow-left", Diff: 1, Row: Row{"id": "overflow-left", "group": "overflow", "start": int64(0), "end": int64(1)}},
	}}); err != nil {
		t.Fatalf("join max multiplicity interval row: %v", err)
	}
	beforeOverflow, err := join.Snapshot()
	if err != nil {
		t.Fatalf("snapshot before product overflow: %v", err)
	}
	_, err = join.Apply([]IncrementalIntervalJoinUpdate{{
		Side: IncrementalIntervalJoinLeft,
		Row:  DifferentialRow{Key: "overflow-left", Diff: 1},
	}})
	if !errors.Is(err, ErrIncrementalIntervalJoinOverflow) {
		t.Fatalf("product overflow error = %v, want overflow", err)
	}
	afterOverflow, err := join.Snapshot()
	if err != nil {
		t.Fatalf("snapshot after product overflow: %v", err)
	}
	if !reflect.DeepEqual(afterOverflow, beforeOverflow) {
		t.Fatalf("state after product overflow changed: before=%#v after=%#v", beforeOverflow, afterOverflow)
	}

	_, err = join.Apply([]IncrementalIntervalJoinUpdate{{
		Side: IncrementalIntervalJoinSide(99),
		Row:  DifferentialRow{Key: "invalid", Diff: 1, Row: Row{"id": "invalid", "group": "g", "start": int64(0), "end": int64(1)}},
	}})
	if !errors.Is(err, ErrIncrementalIntervalJoinSideInvalid) {
		t.Fatalf("invalid side error = %v, want invalid side", err)
	}

	beforeInvalid, err := join.Snapshot()
	if err != nil {
		t.Fatalf("snapshot before invalid interval: %v", err)
	}
	if _, err = join.Apply([]IncrementalIntervalJoinUpdate{{
		Side: IncrementalIntervalJoinLeft,
		Row:  DifferentialRow{Key: "bad-interval", Diff: 1, Row: Row{"id": "bad-interval", "group": "g", "start": int64(5), "end": int64(5)}},
	}}); !errors.Is(err, ErrIncrementalIntervalJoinIntervalInvalid) {
		t.Fatalf("invalid interval error = %v, want invalid interval", err)
	}
	final, err := join.Snapshot()
	if err != nil {
		t.Fatalf("final snapshot: %v", err)
	}
	if !reflect.DeepEqual(final, beforeInvalid) {
		t.Fatalf("invalid batch unexpectedly changed state: got=%#v want=%#v", final, beforeInvalid)
	}
}

func TestIncrementalIntervalJoinClonesRows(t *testing.T) {
	join, err := NewIncrementalIntervalJoin(m029TestIntervalJoinDefinition())
	if err != nil {
		t.Fatalf("create interval join: %v", err)
	}
	input := Row{"id": "l1", "group": "g", "start": int64(0), "end": int64(10), "nested": map[string]any{"value": "before"}}
	if _, err = join.Apply([]IncrementalIntervalJoinUpdate{{
		Side: IncrementalIntervalJoinLeft,
		Row:  DifferentialRow{Key: "l1", Diff: 1, Row: input},
	}}); err != nil {
		t.Fatalf("apply input: %v", err)
	}
	input["group"] = "changed"
	input["nested"].(map[string]any)["value"] = "changed"

	if _, err = join.Apply([]IncrementalIntervalJoinUpdate{{
		Side: IncrementalIntervalJoinRight,
		Row:  DifferentialRow{Key: "r1", Diff: 1, Row: Row{"id": "r1", "group": "g", "start": int64(1), "end": int64(2)}},
	}}); err != nil {
		t.Fatalf("apply right input: %v", err)
	}
	snapshot, err := join.Snapshot()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if len(snapshot) != 1 || snapshot[0].Diff != 1 {
		t.Fatalf("snapshot = %#v, want one joined row", snapshot)
	}
	if snapshot[0].Row["left"] != "l1" || snapshot[0].Row["right"] != "r1" {
		t.Fatalf("merged row = %#v", snapshot[0].Row)
	}

	snapshot[0].Row["left"] = "mutated"
	second, err := join.Snapshot()
	if err != nil {
		t.Fatalf("second snapshot: %v", err)
	}
	if second[0].Row["left"] != "l1" {
		t.Fatalf("retained output was not cloned: %#v", second)
	}
}

func TestIncrementalIntervalJoinRandomizedReference(t *testing.T) {
	join, err := NewIncrementalIntervalJoin(m029TestIntervalJoinDefinition())
	if err != nil {
		t.Fatalf("create interval join: %v", err)
	}
	left := make(map[string]m029ReferenceEntry)
	right := make(map[string]m029ReferenceEntry)
	random := rand.New(rand.NewSource(29029))

	for operation := 0; operation < 2000; operation++ {
		side := IncrementalIntervalJoinLeft
		entries := left
		if random.Intn(2) == 1 {
			side = IncrementalIntervalJoinRight
			entries = right
		}
		key := fmt.Sprintf("%c-%02d", 'a'+rune(random.Intn(5)), random.Intn(20))
		entry, exists := entries[key]
		var updates []IncrementalIntervalJoinUpdate
		var next m029ReferenceEntry
		if exists && entry.count > 0 && random.Intn(5) == 0 {
			newRow := m029RandomIntervalRow(random, key, operation+10000)
			updates = []IncrementalIntervalJoinUpdate{
				{Side: side, Row: DifferentialRow{Key: key, Diff: -entry.count}},
				{Side: side, Row: DifferentialRow{Key: key, Diff: 1, Row: newRow}},
			}
			next = m029ReferenceEntry{row: newRow, count: 1}
		} else if exists && entry.count > 0 {
			diff := int64(1)
			if random.Intn(3) == 0 {
				diff = -1
			}
			row := Row(nil)
			if diff > 0 {
				row = entry.row
			}
			updates = []IncrementalIntervalJoinUpdate{{Side: side, Row: DifferentialRow{Key: key, Diff: diff, Row: row}}}
			next = entry
			next.count += diff
		} else {
			row := m029RandomIntervalRow(random, key, operation)
			updates = []IncrementalIntervalJoinUpdate{{Side: side, Row: DifferentialRow{Key: key, Diff: 1, Row: row}}}
			next = m029ReferenceEntry{row: row, count: 1}
		}

		if _, err = join.Apply(updates); err != nil {
			t.Fatalf("operation %d apply %v %#v: %v", operation, side, updates, err)
		}
		if next.count == 0 {
			delete(entries, key)
		} else {
			entries[key] = next
		}
		want := m029ReferenceIntervalSnapshot(left, right)
		got, err := join.Snapshot()
		if err != nil {
			t.Fatalf("operation %d snapshot: %v", operation, err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("operation %d snapshot = %#v, want %#v", operation, got, want)
		}
	}
}

func m029RandomIntervalRow(random *rand.Rand, key string, salt int) Row {
	start := int64(random.Intn(40) - 20)
	length := int64(random.Intn(8) + 1)
	return Row{
		"id":    key + fmt.Sprintf("-%d", salt),
		"group": fmt.Sprintf("g-%d", random.Intn(4)),
		"start": start,
		"end":   start + length,
	}
}

func m029ReferenceIntervalSnapshot(left, right map[string]m029ReferenceEntry) []DifferentialRow {
	leftKeys := make([]string, 0, len(left))
	for key := range left {
		leftKeys = append(leftKeys, key)
	}
	rightKeys := make([]string, 0, len(right))
	for key := range right {
		rightKeys = append(rightKeys, key)
	}
	sort.Strings(leftKeys)
	sort.Strings(rightKeys)
	result := make([]DifferentialRow, 0)
	for _, leftKey := range leftKeys {
		leftRow := left[leftKey].row
		for _, rightKey := range rightKeys {
			rightRow := right[rightKey].row
			if leftRow["group"] != rightRow["group"] || !intervalsOverlap(leftRow["start"].(int64), leftRow["end"].(int64), rightRow["start"].(int64), rightRow["end"].(int64)) {
				continue
			}
			result = append(result, DifferentialRow{
				Key:  leftKey + "\x00" + rightKey,
				Diff: left[leftKey].count * right[rightKey].count,
				Row:  Row{"left": leftRow["id"], "right": rightRow["id"]},
			})
		}
	}
	return result
}

func m029TestIntervalJoinDefinition() IncrementalIntervalJoinDefinition {
	key := func(row Row) (string, error) {
		value, ok := row["group"].(string)
		if !ok {
			return "", fmt.Errorf("group is not a string")
		}
		return value, nil
	}
	interval := func(row Row) (int64, int64, error) {
		start, ok := row["start"].(int64)
		if !ok {
			return 0, 0, fmt.Errorf("start is not int64")
		}
		end, ok := row["end"].(int64)
		if !ok {
			return 0, 0, fmt.Errorf("end is not int64")
		}
		return start, end, nil
	}
	return IncrementalIntervalJoinDefinition{
		LeftKey:       key,
		RightKey:      key,
		LeftInterval:  interval,
		RightInterval: interval,
		Merge: func(left, right Row) (Row, error) {
			return Row{"left": left["id"], "right": right["id"]}, nil
		},
	}
}
