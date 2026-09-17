package hatSql

import (
	"errors"
	"reflect"
	"sort"
	"testing"
)

func TestC213IncrementalTopKReportsRankMovement(t *testing.T) {
	topK, err := NewIncrementalTopK(IncrementalTopKDefinition{
		K:          3,
		OrderKey:   c213TopKOrderKey,
		Descending: true,
	})
	if err != nil {
		t.Fatalf("NewIncrementalTopK() error = %v", err)
	}
	if _, err := topK.Apply([]DifferentialRow{
		{Key: "a", Diff: 1, Row: Row{"score": int64(10)}},
		{Key: "b", Diff: 1, Row: Row{"score": int64(9)}},
		{Key: "c", Diff: 1, Row: Row{"score": int64(8)}},
	}); err != nil {
		t.Fatalf("seed Apply() error = %v", err)
	}

	changes, err := topK.ApplyWithRankChanges([]DifferentialRow{
		{Key: "c", Diff: -1},
		{Key: "c", Diff: 1, Row: Row{"score": int64(11)}},
	})
	if err != nil {
		t.Fatalf("replacement ApplyWithRankChanges() error = %v", err)
	}
	assertC213RankChanges(t, changes, map[string]IncrementalTopKRankChange{
		"a": {Key: "a", Diff: 0, BeforeRank: 1, AfterRank: 2, BeforeCount: 1, AfterCount: 1},
		"b": {Key: "b", Diff: 0, BeforeRank: 2, AfterRank: 3, BeforeCount: 1, AfterCount: 1},
		"c": {Key: "c", Diff: 0, BeforeRank: 3, AfterRank: 1, BeforeCount: 1, AfterCount: 1, Row: Row{"score": int64(11)}},
	})
	if got, want := c213RankChangeKeys(changes), []string{"a", "b", "c"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("replacement rank change order = %#v, want %#v", got, want)
	}

	changes, err = topK.ApplyWithRankChanges([]DifferentialRow{
		{Key: "d", Diff: 1, Row: Row{"score": int64(12)}},
	})
	if err != nil {
		t.Fatalf("insert ApplyWithRankChanges() error = %v", err)
	}
	assertC213RankChanges(t, changes, map[string]IncrementalTopKRankChange{
		"a": {Key: "a", Diff: 0, BeforeRank: 2, AfterRank: 3, BeforeCount: 1, AfterCount: 1},
		"b": {Key: "b", Diff: -1, BeforeRank: 3, AfterRank: 0, BeforeCount: 1, AfterCount: 0},
		"c": {Key: "c", Diff: 0, BeforeRank: 1, AfterRank: 2, BeforeCount: 1, AfterCount: 1},
		"d": {Key: "d", Diff: 1, BeforeRank: 0, AfterRank: 1, BeforeCount: 0, AfterCount: 1, Row: Row{"score": int64(12)}},
	})
	if got, want := c213RankChangeKeys(changes), []string{"c", "a", "b", "d"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("insert rank change order = %#v, want %#v", got, want)
	}
	if got, want := topK.Snapshot(), []DifferentialRow{
		{Key: "d", Diff: 1, Row: Row{"score": int64(12)}},
		{Key: "c", Diff: 1, Row: Row{"score": int64(11)}},
		{Key: "a", Diff: 1, Row: Row{"score": int64(10)}},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Snapshot() = %#v, want %#v", got, want)
	}
}

func TestC213IncrementalTopKRankChangesUseSelectedWeight(t *testing.T) {
	topK, err := NewIncrementalTopK(IncrementalTopKDefinition{
		K:          4,
		OrderKey:   c213TopKOrderKey,
		Descending: true,
	})
	if err != nil {
		t.Fatalf("NewIncrementalTopK() error = %v", err)
	}
	if _, err := topK.Apply([]DifferentialRow{
		{Key: "a", Diff: 2, Row: Row{"score": int64(10)}},
		{Key: "b", Diff: 1, Row: Row{"score": int64(9)}},
		{Key: "c", Diff: 1, Row: Row{"score": int64(8)}},
	}); err != nil {
		t.Fatalf("seed Apply() error = %v", err)
	}

	changes, err := topK.ApplyWithRankChanges([]DifferentialRow{{Key: "b", Diff: 1}})
	if err != nil {
		t.Fatalf("weighted ApplyWithRankChanges() error = %v", err)
	}
	assertC213RankChanges(t, changes, map[string]IncrementalTopKRankChange{
		"b": {Key: "b", Diff: 1, BeforeRank: 3, AfterRank: 3, BeforeCount: 1, AfterCount: 2},
		"c": {Key: "c", Diff: -1, BeforeRank: 4, AfterRank: 0, BeforeCount: 1, AfterCount: 0},
	})
}

func TestC213IncrementalTopKRankChangesAreAtomic(t *testing.T) {
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

	changes, err := topK.ApplyWithRankChanges([]DifferentialRow{{Key: "missing", Diff: -1}})
	if !errors.Is(err, ErrIncrementalTopKNegativeMultiplicity) {
		t.Fatalf("invalid batch error = %v, want ErrIncrementalTopKNegativeMultiplicity", err)
	}
	if changes != nil {
		t.Fatalf("invalid batch changes = %#v, want nil", changes)
	}
	if got := topK.AllRows(); !reflect.DeepEqual(got, before) {
		t.Fatalf("state after rejected batch = %#v, want %#v", got, before)
	}
	if changes, err := topK.ApplyWithRankChanges(nil); err != nil || changes != nil {
		t.Fatalf("empty ApplyWithRankChanges() = %#v, %v; want nil, nil", changes, err)
	}
	var nilTopK *IncrementalTopK
	if changes, err := nilTopK.ApplyWithRankChanges(nil); !errors.Is(err, ErrIncrementalTopKNil) || changes != nil {
		t.Fatalf("nil ApplyWithRankChanges() = %#v, %v; want nil, ErrIncrementalTopKNil", changes, err)
	}
}

func TestC213IncrementalTopKRankChangesMatchRebuild(t *testing.T) {
	const (
		rows = 128
		k    = 7
	)
	topK, err := NewIncrementalTopK(IncrementalTopKDefinition{
		K:          k,
		OrderKey:   c213TopKOrderKey,
		Descending: true,
	})
	if err != nil {
		t.Fatalf("NewIncrementalTopK() error = %v", err)
	}
	scores := make(map[string]int64, rows)
	seed := make([]DifferentialRow, 0, rows)
	for index := 0; index < rows; index++ {
		key := "row-" + string(rune(index))
		score := int64(index)
		scores[key] = score
		seed = append(seed, DifferentialRow{Key: key, Diff: 1, Row: Row{"score": score}})
	}
	if _, err := topK.Apply(seed); err != nil {
		t.Fatalf("seed Apply() error = %v", err)
	}
	previous := c213RankReference(scores, k)
	for iteration := 0; iteration < 1000; iteration++ {
		index := (iteration*7919 + 17) % rows
		key := "row-" + string(rune(index))
		score := int64((iteration*104729 + 23) % (rows * 4))
		scores[key] = score
		changes, err := topK.ApplyWithRankChanges([]DifferentialRow{
			{Key: key, Diff: -1},
			{Key: key, Diff: 1, Row: Row{"score": score}},
		})
		if err != nil {
			t.Fatalf("iteration %d ApplyWithRankChanges() error = %v", iteration, err)
		}
		current := c213RankReference(scores, k)
		c213AssertRankChangesMatchReference(t, iteration, changes, previous, current)
		c213AssertTopKSnapshot(t, topK, c213RankExpectedSnapshot(current))
		previous = current
	}
}

func TestC213IncrementalTopKRankChangesMatchLargeRebuildWorkload(t *testing.T) {
	const (
		rows       = 10000
		k          = 20
		iterations = 100
	)
	topK, err := NewIncrementalTopK(IncrementalTopKDefinition{
		K:          k,
		OrderKey:   c213TopKOrderKey,
		Descending: true,
	})
	if err != nil {
		t.Fatalf("NewIncrementalTopK() error = %v", err)
	}
	scores := make(map[string]int64, rows)
	seed := make([]DifferentialRow, 0, rows)
	for index := 0; index < rows; index++ {
		key := "row-" + string(rune(index))
		score := int64(index)
		scores[key] = score
		seed = append(seed, DifferentialRow{Key: key, Diff: 1, Row: Row{"score": score}})
	}
	if _, err := topK.Apply(seed); err != nil {
		t.Fatalf("seed Apply() error = %v", err)
	}
	previous := c213RankReference(scores, k)
	for iteration := 0; iteration < iterations; iteration++ {
		index := (iteration*7919 + 17) % rows
		key := "row-" + string(rune(index))
		score := int64((iteration*104729 + 23) % (rows * 4))
		scores[key] = score
		changes, err := topK.ApplyWithRankChanges([]DifferentialRow{
			{Key: key, Diff: -1},
			{Key: key, Diff: 1, Row: Row{"score": score}},
		})
		if err != nil {
			t.Fatalf("iteration %d ApplyWithRankChanges() error = %v", iteration, err)
		}
		current := c213RankReference(scores, k)
		c213AssertRankChangesMatchReference(t, iteration, changes, previous, current)
		previous = current
	}
}

func assertC213RankChanges(t *testing.T, changes []IncrementalTopKRankChange, want map[string]IncrementalTopKRankChange) {
	t.Helper()
	if len(changes) != len(want) {
		t.Fatalf("rank changes length = %d, want %d: %#v", len(changes), len(want), changes)
	}
	got := make(map[string]IncrementalTopKRankChange, len(changes))
	for _, change := range changes {
		got[change.Key] = change
	}
	for key, expected := range want {
		actual, ok := got[key]
		if !ok {
			t.Fatalf("rank changes missing key %q: %#v", key, changes)
		}
		if actual.Diff != expected.Diff || actual.BeforeRank != expected.BeforeRank || actual.AfterRank != expected.AfterRank || actual.BeforeCount != expected.BeforeCount || actual.AfterCount != expected.AfterCount {
			t.Fatalf("rank change %q = %#v, want %#v", key, actual, expected)
		}
		if expected.Row != nil && !reflect.DeepEqual(actual.Row, expected.Row) {
			t.Fatalf("rank change %q row = %#v, want %#v", key, actual.Row, expected.Row)
		}
	}
}

func c213RankChangeKeys(changes []IncrementalTopKRankChange) []string {
	keys := make([]string, 0, len(changes))
	for _, change := range changes {
		keys = append(keys, change.Key)
	}
	return keys
}

type c213RankReferenceRow struct {
	score int64
	rank  int
}

func c213RankReference(scores map[string]int64, k int) map[string]c213RankReferenceRow {
	keys := make([]string, 0, len(scores))
	for key := range scores {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(left, right int) bool {
		if scores[keys[left]] != scores[keys[right]] {
			return scores[keys[left]] > scores[keys[right]]
		}
		return keys[left] < keys[right]
	})
	result := make(map[string]c213RankReferenceRow, k)
	for index, key := range keys {
		if index == k {
			break
		}
		result[key] = c213RankReferenceRow{score: scores[key], rank: index + 1}
	}
	return result
}

func c213RankExpectedSnapshot(reference map[string]c213RankReferenceRow) []c213TopKExpected {
	keys := make([]string, 0, len(reference))
	for key := range reference {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(left, right int) bool {
		return reference[keys[left]].rank < reference[keys[right]].rank
	})
	want := make([]c213TopKExpected, 0, len(keys))
	for _, key := range keys {
		want = append(want, c213TopKExpected{key: key, diff: 1})
	}
	return want
}

func c213AssertRankChangesMatchReference(t *testing.T, iteration int, changes []IncrementalTopKRankChange, before, after map[string]c213RankReferenceRow) {
	t.Helper()
	want := make(map[string]IncrementalTopKRankChange)
	for key, previous := range before {
		current, exists := after[key]
		if !exists || current.rank != previous.rank {
			change := IncrementalTopKRankChange{
				Key:         key,
				Diff:        -1,
				BeforeRank:  previous.rank,
				BeforeCount: 1,
			}
			if exists {
				change.Diff = 0
				change.AfterRank = current.rank
				change.AfterCount = 1
				change.Row = Row{"score": current.score}
			}
			want[key] = change
		}
	}
	for key, current := range after {
		previous, exists := before[key]
		if !exists {
			want[key] = IncrementalTopKRankChange{
				Key:        key,
				Diff:       1,
				AfterRank:  current.rank,
				AfterCount: 1,
				Row:        Row{"score": current.score},
			}
			continue
		}
		if previous.rank != current.rank {
			want[key] = IncrementalTopKRankChange{
				Key:         key,
				Diff:        0,
				BeforeRank:  previous.rank,
				AfterRank:   current.rank,
				BeforeCount: 1,
				AfterCount:  1,
				Row:         Row{"score": current.score},
			}
		}
	}
	if len(changes) != len(want) {
		t.Fatalf("iteration %d rank changes length = %d, want %d: %#v", iteration, len(changes), len(want), changes)
	}
	got := make(map[string]IncrementalTopKRankChange, len(changes))
	for _, change := range changes {
		got[change.Key] = change
	}
	for key, expected := range want {
		actual, exists := got[key]
		if !exists {
			t.Fatalf("iteration %d rank changes missing %q: %#v", iteration, key, changes)
		}
		if actual.Diff != expected.Diff || actual.BeforeRank != expected.BeforeRank || actual.AfterRank != expected.AfterRank || actual.BeforeCount != expected.BeforeCount || actual.AfterCount != expected.AfterCount {
			t.Fatalf("iteration %d rank change %q = %#v, want %#v", iteration, key, actual, expected)
		}
		if expected.Row != nil && !reflect.DeepEqual(actual.Row, expected.Row) {
			t.Fatalf("iteration %d rank change %q row = %#v, want %#v", iteration, key, actual.Row, expected.Row)
		}
	}
}
