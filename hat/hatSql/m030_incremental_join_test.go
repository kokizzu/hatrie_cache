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

func TestMZ030IncrementalJoinMaintainsWeightedMatches(t *testing.T) {
	join, err := NewIncrementalJoin(IncrementalJoinDefinition{
		LeftKey:  m030JoinKey,
		RightKey: m030JoinKey,
		Merge:    m030JoinMerge,
	})
	if err != nil {
		t.Fatalf("NewIncrementalJoin() error = %v", err)
	}
	output, err := join.Apply([]IncrementalJoinUpdate{
		{Side: IncrementalJoinLeft, Row: DifferentialRow{Key: "l1", Time: 1, Diff: 2, Row: Row{"id": "l1", "group": "g", "value": int64(10)}}},
		{Side: IncrementalJoinRight, Row: DifferentialRow{Key: "r1", Time: 2, Diff: 3, Row: Row{"id": "r1", "group": "g", "value": int64(20)}}},
		{Side: IncrementalJoinRight, Row: DifferentialRow{Key: "r2", Time: 2, Diff: 1, Row: Row{"id": "r2", "group": "other", "value": int64(30)}}},
	})
	if err != nil {
		t.Fatalf("seed Apply() error = %v", err)
	}
	m030AssertJoinRows(t, output, []DifferentialRow{
		{Key: "l1\x00r1", Time: 2, Diff: 6, Row: Row{"left_id": "l1", "right_id": "r1", "group": "g"}},
	})
	m030AssertJoinSnapshot(t, join, []DifferentialRow{
		{Key: "l1\x00r1", Time: 2, Diff: 6, Row: Row{"left_id": "l1", "right_id": "r1", "group": "g"}},
	})

	output, err = join.Apply([]IncrementalJoinUpdate{{Side: IncrementalJoinRight, Row: DifferentialRow{Key: "r1", Diff: -1}}})
	if err != nil {
		t.Fatalf("right retraction Apply() error = %v", err)
	}
	m030AssertJoinRows(t, output, []DifferentialRow{
		{Key: "l1\x00r1", Time: 2, Diff: -2, Row: Row{"left_id": "l1", "right_id": "r1", "group": "g"}},
	})
	m030AssertJoinSnapshot(t, join, []DifferentialRow{
		{Key: "l1\x00r1", Time: 2, Diff: 4, Row: Row{"left_id": "l1", "right_id": "r1", "group": "g"}},
	})

	output, err = join.Apply([]IncrementalJoinUpdate{{
		Side: IncrementalJoinLeft,
		Row:  DifferentialRow{Key: "l2", Time: 4, Diff: 1, Row: Row{"id": "l2", "group": "g", "value": int64(11)}},
	}})
	if err != nil {
		t.Fatalf("second left insert Apply() error = %v", err)
	}
	m030AssertJoinRows(t, output, []DifferentialRow{
		{Key: "l2\x00r1", Time: 4, Diff: 2, Row: Row{"left_id": "l2", "right_id": "r1", "group": "g"}},
	})
	m030AssertJoinSnapshot(t, join, []DifferentialRow{
		{Key: "l1\x00r1", Time: 2, Diff: 4, Row: Row{"left_id": "l1", "right_id": "r1", "group": "g"}},
		{Key: "l2\x00r1", Time: 4, Diff: 2, Row: Row{"left_id": "l2", "right_id": "r1", "group": "g"}},
	})
}

func TestMZ030IncrementalJoinIsAtomicAndRejectsInvalidUpdates(t *testing.T) {
	join, err := NewIncrementalJoin(IncrementalJoinDefinition{
		LeftKey:  m030JoinKey,
		RightKey: m030JoinKey,
		Merge:    m030JoinMerge,
	})
	if err != nil {
		t.Fatalf("NewIncrementalJoin() error = %v", err)
	}
	if _, err := join.Apply([]IncrementalJoinUpdate{
		{Side: IncrementalJoinLeft, Row: DifferentialRow{Key: "l", Diff: 1, Row: Row{"id": "l", "group": "g"}}},
		{Side: IncrementalJoinRight, Row: DifferentialRow{Key: "r", Diff: 1, Row: Row{"id": "r", "group": "g"}}},
	}); err != nil {
		t.Fatalf("seed Apply() error = %v", err)
	}
	before := m030JoinSnapshot(t, join)
	cases := []struct {
		name  string
		want  error
		batch []IncrementalJoinUpdate
	}{
		{
			name:  "missing deletion",
			want:  ErrIncrementalJoinNegativeMultiplicity,
			batch: []IncrementalJoinUpdate{{Side: IncrementalJoinLeft, Row: DifferentialRow{Key: "missing", Diff: -1}}},
		},
		{
			name:  "conflicting row",
			want:  ErrIncrementalJoinRowConflict,
			batch: []IncrementalJoinUpdate{{Side: IncrementalJoinLeft, Row: DifferentialRow{Key: "l", Diff: 1, Row: Row{"id": "l", "group": "other"}}}},
		},
		{
			name:  "invalid side",
			want:  ErrIncrementalJoinSideInvalid,
			batch: []IncrementalJoinUpdate{{Side: 99, Row: DifferentialRow{Key: "x", Diff: 1, Row: Row{"id": "x", "group": "g"}}}},
		},
		{
			name:  "empty key",
			want:  ErrIncrementalJoinKeyRequired,
			batch: []IncrementalJoinUpdate{{Side: IncrementalJoinLeft, Row: DifferentialRow{Diff: 1, Row: Row{"id": "x", "group": "g"}}}},
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			output, err := join.Apply(testCase.batch)
			if !errors.Is(err, testCase.want) {
				t.Fatalf("Apply() error = %v, want %v", err, testCase.want)
			}
			if output != nil {
				t.Fatalf("rejected Apply() output = %#v, want nil", output)
			}
			m030AssertJoinSnapshot(t, join, before)
		})
	}

	large, err := NewIncrementalJoin(IncrementalJoinDefinition{
		LeftKey:  m030JoinKey,
		RightKey: m030JoinKey,
		Merge:    m030JoinMerge,
	})
	if err != nil {
		t.Fatalf("NewIncrementalJoin(large) error = %v", err)
	}
	if _, err := large.Apply([]IncrementalJoinUpdate{
		{Side: IncrementalJoinLeft, Row: DifferentialRow{Key: "large", Diff: math.MaxInt64/2 + 1, Row: Row{"id": "large", "group": "g"}}},
		{Side: IncrementalJoinRight, Row: DifferentialRow{Key: "right", Diff: 1, Row: Row{"id": "right", "group": "g"}}},
	}); err != nil {
		t.Fatalf("large seed Apply() error = %v", err)
	}
	largeBefore := m030JoinSnapshot(t, large)
	output, err := large.Apply([]IncrementalJoinUpdate{{Side: IncrementalJoinRight, Row: DifferentialRow{Key: "right", Diff: 1}}})
	if !errors.Is(err, ErrIncrementalJoinOverflow) || output != nil {
		t.Fatalf("join output overflow = %#v, %v; want nil and ErrIncrementalJoinOverflow", output, err)
	}
	m030AssertJoinSnapshot(t, large, largeBefore)
}

func TestMZ030IncrementalJoinReplacementEmitsOldAndNewRows(t *testing.T) {
	join, err := NewIncrementalJoin(IncrementalJoinDefinition{
		LeftKey:  m030JoinKey,
		RightKey: m030JoinKey,
		Merge:    m030JoinMergeWithValue,
	})
	if err != nil {
		t.Fatalf("NewIncrementalJoin() error = %v", err)
	}
	if _, err := join.Apply([]IncrementalJoinUpdate{
		{Side: IncrementalJoinLeft, Row: DifferentialRow{Key: "left", Time: 1, Diff: 2, Row: Row{"id": "left", "group": "g", "value": int64(10)}}},
		{Side: IncrementalJoinRight, Row: DifferentialRow{Key: "right", Time: 2, Diff: 2, Row: Row{"id": "right", "group": "g", "value": int64(20)}}},
	}); err != nil {
		t.Fatalf("seed Apply() error = %v", err)
	}
	before := m030JoinSnapshot(t, join)
	output, err := join.Apply([]IncrementalJoinUpdate{
		{Side: IncrementalJoinLeft, Row: DifferentialRow{Key: "left", Diff: -2}},
		{Side: IncrementalJoinLeft, Row: DifferentialRow{Key: "left", Time: 3, Diff: 1, Row: Row{"id": "left", "group": "g", "value": int64(11)}}},
	})
	if err != nil {
		t.Fatalf("replacement Apply() error = %v", err)
	}
	m030AssertJoinRows(t, output, []DifferentialRow{
		{Key: "left\x00right", Time: 2, Diff: -4, Row: Row{"left_id": "left", "right_id": "right", "left_value": int64(10), "right_value": int64(20), "group": "g"}},
		{Key: "left\x00right", Time: 3, Diff: 2, Row: Row{"left_id": "left", "right_id": "right", "left_value": int64(11), "right_value": int64(20), "group": "g"}},
	})
	if len(before) != 1 || before[0].Diff != 4 {
		t.Fatalf("pre-replacement snapshot = %#v, want one pair with diff 4", before)
	}
	m030AssertJoinSnapshot(t, join, []DifferentialRow{
		{Key: "left\x00right", Time: 3, Diff: 2, Row: Row{"left_id": "left", "right_id": "right", "left_value": int64(11), "right_value": int64(20), "group": "g"}},
	})
	if _, err := join.Apply([]IncrementalJoinUpdate{{
		Side: IncrementalJoinLeft,
		Row:  DifferentialRow{Key: "left", Diff: 1, Row: Row{"id": "left", "group": "g", "value": int64(11)}},
	}}); err != nil {
		t.Fatalf("multiplicity restore Apply() error = %v", err)
	}

	stateBefore := m030JoinSnapshot(t, join)
	output, err = join.Apply([]IncrementalJoinUpdate{
		{Side: IncrementalJoinLeft, Row: DifferentialRow{Key: "left", Diff: -1}},
		{Side: IncrementalJoinLeft, Row: DifferentialRow{Key: "left", Diff: 1, Row: Row{"id": "left", "group": "other", "value": int64(12)}}},
	})
	if !errors.Is(err, ErrIncrementalJoinRowConflict) || output != nil {
		t.Fatalf("conflicting replacement = %#v, %v; want nil and ErrIncrementalJoinRowConflict", output, err)
	}
	m030AssertJoinSnapshot(t, join, stateBefore)
}

func TestMZ030IncrementalJoinClonesRowsAndSupportsEmptySnapshots(t *testing.T) {
	join, err := NewIncrementalJoin(IncrementalJoinDefinition{
		LeftKey:  m030JoinKey,
		RightKey: m030JoinKey,
		Merge:    m030JoinMerge,
	})
	if err != nil {
		t.Fatalf("NewIncrementalJoin() error = %v", err)
	}
	if snapshot, err := join.Snapshot(); err != nil || snapshot != nil {
		t.Fatalf("empty Snapshot() = %#v, %v; want nil, nil", snapshot, err)
	}
	left := Row{"id": "l", "group": "g", "payload": []byte("before")}
	right := Row{"id": "r", "group": "g"}
	output, err := join.Apply([]IncrementalJoinUpdate{
		{Side: IncrementalJoinLeft, Row: DifferentialRow{Key: "l", Diff: 1, Row: left}},
		{Side: IncrementalJoinRight, Row: DifferentialRow{Key: "r", Diff: 1, Row: right}},
	})
	if err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	left["group"] = "changed"
	left["payload"].([]byte)[0] = 'x'
	right["group"] = "changed"
	output[0].Row["left_id"] = "changed"
	output[0].Row["right_id"] = "changed"
	m030AssertJoinSnapshot(t, join, []DifferentialRow{
		{Key: "l\x00r", Diff: 1, Row: Row{"left_id": "l", "right_id": "r", "group": "g"}},
	})

	var nilJoin *IncrementalJoin
	if output, err := nilJoin.Apply(nil); !errors.Is(err, ErrIncrementalJoinNil) || output != nil {
		t.Fatalf("nil Apply() = %#v, %v; want nil and ErrIncrementalJoinNil", output, err)
	}
	if snapshot, err := nilJoin.Snapshot(); !errors.Is(err, ErrIncrementalJoinNil) || snapshot != nil {
		t.Fatalf("nil Snapshot() = %#v, %v; want nil and ErrIncrementalJoinNil", snapshot, err)
	}
}

func TestMZ030IncrementalJoinMatchesReferenceRandomized(t *testing.T) {
	join, err := NewIncrementalJoin(IncrementalJoinDefinition{
		LeftKey:  m030JoinKey,
		RightKey: m030JoinKey,
		Merge:    m030JoinMerge,
	})
	if err != nil {
		t.Fatalf("NewIncrementalJoin() error = %v", err)
	}
	reference := map[IncrementalJoinSide]map[string]m030JoinReferenceEntry{
		IncrementalJoinLeft:  make(map[string]m030JoinReferenceEntry),
		IncrementalJoinRight: make(map[string]m030JoinReferenceEntry),
	}
	random := rand.New(rand.NewSource(30030))
	for iteration := 0; iteration < 1000; iteration++ {
		side := IncrementalJoinLeft
		if random.Intn(2) == 1 {
			side = IncrementalJoinRight
		}
		key := fmt.Sprintf("%c%02d", map[IncrementalJoinSide]byte{IncrementalJoinLeft: 'l', IncrementalJoinRight: 'r'}[side], random.Intn(24))
		entries := reference[side]
		entry, exists := entries[key]
		diff := int64(1)
		row := Row{"id": key, "group": fmt.Sprintf("g%02d", random.Intn(8)), "value": int64(iteration)}
		if exists && entry.count > 0 && random.Intn(3) != 0 {
			diff = -1
			row = nil
		} else if exists && entry.count > 0 {
			row = nil
		}
		update := IncrementalJoinUpdate{Side: side, Row: DifferentialRow{Key: key, Time: uint64(iteration + 1), Diff: diff, Row: row}}
		wantOutput := m030ReferenceDelta(update, reference)
		output, err := join.Apply([]IncrementalJoinUpdate{update})
		if err != nil {
			t.Fatalf("iteration %d Apply() error = %v", iteration, err)
		}
		m030AssertJoinRows(t, output, wantOutput)
		m030ApplyReference(update, reference)
		m030AssertJoinSnapshot(t, join, m030ReferenceSnapshot(reference))
	}
}

func m030JoinKey(row Row) (string, error) {
	group, ok := row["group"].(string)
	if !ok || group == "" {
		return "", fmt.Errorf("join group is required")
	}
	return group, nil
}

func m030JoinMerge(left, right Row) (Row, error) {
	return Row{
		"left_id":  left["id"],
		"right_id": right["id"],
		"group":    left["group"],
	}, nil
}

func m030JoinMergeWithValue(left, right Row) (Row, error) {
	return Row{
		"left_id":     left["id"],
		"right_id":    right["id"],
		"left_value":  left["value"],
		"right_value": right["value"],
		"group":       left["group"],
	}, nil
}

func m030AssertJoinRows(t *testing.T, got, want []DifferentialRow) {
	t.Helper()
	if len(got) == 0 && len(want) == 0 {
		return
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("join rows = %#v, want %#v", got, want)
	}
}

func m030AssertJoinSnapshot(t *testing.T, join *IncrementalJoin, want []DifferentialRow) {
	t.Helper()
	got, err := join.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	m030AssertJoinRows(t, got, want)
}

func m030JoinSnapshot(t *testing.T, join *IncrementalJoin) []DifferentialRow {
	t.Helper()
	snapshot, err := join.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	return snapshot
}

type m030JoinReferenceEntry struct {
	time  uint64
	row   Row
	group string
	count int64
}

func m030ReferenceDelta(update IncrementalJoinUpdate, reference map[IncrementalJoinSide]map[string]m030JoinReferenceEntry) []DifferentialRow {
	entry, exists := reference[update.Side][update.Row.Key]
	if !exists || entry.count == 0 {
		if update.Row.Diff < 0 {
			return nil
		}
		entry = m030JoinReferenceEntry{time: update.Row.Time, row: update.Row.Row, group: update.Row.Row["group"].(string)}
	}
	group := entry.group
	opposite := IncrementalJoinRight
	if update.Side == IncrementalJoinRight {
		opposite = IncrementalJoinLeft
	}
	keys := make([]string, 0)
	for key, candidate := range reference[opposite] {
		if candidate.group == group && candidate.count > 0 {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	result := make([]DifferentialRow, 0, len(keys))
	for _, key := range keys {
		candidate := reference[opposite][key]
		leftKey, rightKey := update.Row.Key, key
		leftEntry, rightEntry := entry, candidate
		if update.Side == IncrementalJoinRight {
			leftKey, rightKey = key, update.Row.Key
			leftEntry, rightEntry = candidate, entry
		}
		result = append(result, DifferentialRow{
			Key:  leftKey + "\x00" + rightKey,
			Time: m030JoinTime(leftEntry.time, rightEntry.time),
			Diff: update.Row.Diff * candidate.count,
			Row:  Row{"left_id": leftKey, "right_id": rightKey, "group": group},
		})
	}
	return result
}

func m030ApplyReference(update IncrementalJoinUpdate, reference map[IncrementalJoinSide]map[string]m030JoinReferenceEntry) {
	entries := reference[update.Side]
	entry := entries[update.Row.Key]
	if update.Row.Diff > 0 && entry.count == 0 {
		entry = m030JoinReferenceEntry{time: update.Row.Time, row: update.Row.Row, group: update.Row.Row["group"].(string)}
	}
	entry.count += update.Row.Diff
	if entry.count == 0 {
		delete(entries, update.Row.Key)
	} else {
		entries[update.Row.Key] = entry
	}
}

func m030ReferenceSnapshot(reference map[IncrementalJoinSide]map[string]m030JoinReferenceEntry) []DifferentialRow {
	keys := make([]string, 0)
	for leftKey, left := range reference[IncrementalJoinLeft] {
		if left.count == 0 {
			continue
		}
		for rightKey, right := range reference[IncrementalJoinRight] {
			if right.count == 0 || left.group != right.group {
				continue
			}
			keys = append(keys, leftKey+"\x00"+rightKey)
		}
	}
	sort.Strings(keys)
	result := make([]DifferentialRow, 0, len(keys))
	for _, key := range keys {
		separator := sort.Search(len(key), func(index int) bool { return key[index] == 0 })
		leftKey, rightKey := key[:separator], key[separator+1:]
		left := reference[IncrementalJoinLeft][leftKey]
		right := reference[IncrementalJoinRight][rightKey]
		result = append(result, DifferentialRow{
			Key:  key,
			Time: m030JoinTime(left.time, right.time),
			Diff: left.count * right.count,
			Row:  Row{"left_id": leftKey, "right_id": rightKey, "group": left.group},
		})
	}
	return result
}

func m030JoinTime(left, right uint64) uint64 {
	if left > right {
		return left
	}
	return right
}
