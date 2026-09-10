package hatSql

import (
	"errors"
	"reflect"
	"testing"
)

func m065MutableRankWindowDefinition(kind IncrementalRankWindowKind) IncrementalRankWindowDefinition {
	return IncrementalRankWindowDefinition{
		Kind:         kind,
		OutputColumn: "rank_value",
		PartitionKey: func(row Row) (string, error) { return row["group"].(string), nil },
		OrderKey:     func(row Row) (interface{}, error) { return row["score"], nil },
		RowKey:       func(row Row) (string, error) { return row["id"].(string), nil },
	}
}

func TestMutableIncrementalRankWindowAppliesRetractionsAndKeepsStatePrivate(t *testing.T) {
	window, err := NewMutableIncrementalRankWindow(m065MutableRankWindowDefinition(IncrementalWindowRowNumber))
	if err != nil {
		t.Fatal(err)
	}
	seed := []Row{
		{"id": "a", "group": "g", "score": int64(10)},
		{"id": "b", "group": "g", "score": int64(20)},
		{"id": "c", "group": "g", "score": int64(30)},
	}
	initial, err := window.Append(seed)
	if err != nil {
		t.Fatal(err)
	}
	initial[0].Row["rank_value"] = int64(999)
	seed[0]["score"] = int64(999)

	updates, err := window.Apply([]IncrementalRankWindowMutation{{
		Kind: IncrementalRankWindowUpdate,
		Key:  "b",
		Row:  Row{"id": "b", "group": "g", "score": int64(5)},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(updates) != 4 {
		t.Fatalf("updates = %#v, want four retraction/insertion rows", updates)
	}
	got := map[string][]int64{}
	for _, update := range updates {
		got[update.Key] = append(got[update.Key], update.Diff, update.Row["rank_value"].(int64))
		update.Row["rank_value"] = int64(777)
	}
	if !reflect.DeepEqual(got["a"], []int64{-1, 1, 1, 2}) || !reflect.DeepEqual(got["b"], []int64{-1, 2, 1, 1}) {
		t.Fatalf("update diffs = %#v, want a and b rank changes", got)
	}
	if got["a"][1] != 1 {
		t.Fatalf("retracted a row = %#v, want original rank 1", got["a"])
	}

	deleted, err := window.Apply([]IncrementalRankWindowMutation{{
		Kind: IncrementalRankWindowDelete,
		Key:  "a",
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(deleted) != 3 {
		t.Fatalf("delete updates = %#v, want a retraction for a and c rank change", deleted)
	}
	deletedRanks := map[string][]int64{}
	for _, update := range deleted {
		deletedRanks[update.Key] = append(deletedRanks[update.Key], update.Diff, update.Row["rank_value"].(int64))
	}
	if !reflect.DeepEqual(deletedRanks["a"], []int64{-1, 2}) || !reflect.DeepEqual(deletedRanks["c"], []int64{-1, 3, 1, 2}) {
		t.Fatalf("delete diffs = %#v, want a rank 2 and c rank 3 -> 2", deletedRanks)
	}
}

func TestMutableIncrementalRankWindowAcceptsUnorderedInsertsAndAllRankKinds(t *testing.T) {
	for _, test := range []struct {
		name string
		kind IncrementalRankWindowKind
	}{
		{name: "row number", kind: IncrementalWindowRowNumber},
		{name: "rank", kind: IncrementalWindowRank},
		{name: "dense rank", kind: IncrementalWindowDenseRank},
	} {
		t.Run(test.name, func(t *testing.T) {
			window, err := NewMutableIncrementalRankWindow(m065MutableRankWindowDefinition(test.kind))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := window.Append([]Row{
				{"id": "a", "group": "g", "score": int64(10)},
				{"id": "b", "group": "g", "score": int64(20)},
			}); err != nil {
				t.Fatal(err)
			}
			updates, err := window.Apply([]IncrementalRankWindowMutation{{
				Kind: IncrementalRankWindowInsert,
				Key:  "before",
				Row:  Row{"id": "before", "group": "g", "score": int64(5)},
			}})
			if err != nil {
				t.Fatal(err)
			}
			if len(updates) != 5 {
				t.Fatalf("updates = %#v, want two retractions and three insertions", updates)
			}
			positive := map[string]int64{}
			negative := map[string]int64{}
			for _, update := range updates {
				value := update.Row["rank_value"].(int64)
				if update.Diff < 0 {
					negative[update.Key] = value
				} else {
					positive[update.Key] = value
				}
			}
			want := map[string]int64{"before": 1, "a": 2, "b": 3}
			if !reflect.DeepEqual(positive, want) {
				t.Fatalf("positive ranks = %#v, want %#v", positive, want)
			}
			if !reflect.DeepEqual(negative, map[string]int64{"a": 1, "b": 2}) {
				t.Fatalf("negative ranks = %#v, want old ranks", negative)
			}
		})
	}
}

func TestMutableIncrementalRankWindowMutationValidationIsAtomic(t *testing.T) {
	window, err := NewMutableIncrementalRankWindow(m065MutableRankWindowDefinition(IncrementalWindowRank))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := window.Append([]Row{{"id": "a", "group": "g", "score": int64(10)}}); err != nil {
		t.Fatal(err)
	}
	for name, mutation := range map[string][]IncrementalRankWindowMutation{
		"duplicate batch key": {
			{Kind: IncrementalRankWindowInsert, Key: "b", Row: Row{"id": "b", "group": "g", "score": int64(20)}},
			{Kind: IncrementalRankWindowInsert, Key: "b", Row: Row{"id": "b2", "group": "g", "score": int64(30)}},
		},
		"missing update": {{Kind: IncrementalRankWindowUpdate, Key: "missing", Row: Row{"id": "missing", "group": "g", "score": int64(20)}}},
		"missing delete": {{Kind: IncrementalRankWindowDelete, Key: "missing"}},
		"key mismatch":   {{Kind: IncrementalRankWindowInsert, Key: "b", Row: Row{"id": "other", "group": "g", "score": int64(20)}}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := window.Apply(mutation); err == nil {
				t.Fatal("Apply unexpectedly succeeded")
			}
		})
	}
	updates, err := window.Apply([]IncrementalRankWindowMutation{{
		Kind: IncrementalRankWindowInsert,
		Key:  "b",
		Row:  Row{"id": "b", "group": "g", "score": int64(20)},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(updates) != 1 || updates[0].Row["rank_value"] != int64(2) {
		t.Fatalf("post-validation state = %#v, want rank 2", updates)
	}
}

func TestMutableIncrementalRankWindowMovesRowsAcrossPartitions(t *testing.T) {
	window, err := NewMutableIncrementalRankWindow(m065MutableRankWindowDefinition(IncrementalWindowRowNumber))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := window.Append([]Row{
		{"id": "a1", "group": "a", "score": int64(10)},
		{"id": "a2", "group": "a", "score": int64(20)},
		{"id": "b1", "group": "b", "score": int64(10)},
	}); err != nil {
		t.Fatal(err)
	}
	updates, err := window.Apply([]IncrementalRankWindowMutation{{
		Kind: IncrementalRankWindowUpdate,
		Key:  "a2",
		Row:  Row{"id": "a2", "group": "b", "score": int64(5)},
	}})
	if err != nil {
		t.Fatal(err)
	}
	got := map[string][]int64{}
	for _, update := range updates {
		got[update.Key] = append(got[update.Key], update.Diff, update.Row["rank_value"].(int64))
	}
	want := map[string][]int64{
		"a2": {-1, 2, 1, 1},
		"b1": {-1, 1, 1, 2},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("partition move updates = %#v, want %#v", got, want)
	}
}

func TestMutableIncrementalRankWindowRebuildFailureDoesNotPublish(t *testing.T) {
	failure := errors.New("order callback failed")
	window, err := NewMutableIncrementalRankWindow(IncrementalRankWindowDefinition{
		Kind:         IncrementalWindowRowNumber,
		OutputColumn: "rank_value",
		OrderKey: func(row Row) (interface{}, error) {
			if row["score"] == int64(999) {
				return nil, failure
			}
			return row["score"], nil
		},
		RowKey: func(row Row) (string, error) { return row["id"].(string), nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := window.Append([]Row{
		{"id": "a", "score": int64(10)},
		{"id": "b", "score": int64(20)},
	}); err != nil {
		t.Fatal(err)
	}
	_, err = window.Apply([]IncrementalRankWindowMutation{{
		Kind: IncrementalRankWindowUpdate,
		Key:  "b",
		Row:  Row{"id": "b", "score": int64(999)},
	}})
	if !errors.Is(err, failure) {
		t.Fatalf("rebuild error = %v, want %v", err, failure)
	}
	updates, err := window.Apply([]IncrementalRankWindowMutation{{
		Kind: IncrementalRankWindowInsert,
		Key:  "c",
		Row:  Row{"id": "c", "score": int64(30)},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(updates) != 1 || updates[0].Key != "c" || updates[0].Row["rank_value"] != int64(3) {
		t.Fatalf("post-failure state = %#v, want c rank 3", updates)
	}
}

func TestIncrementalRankWindowApplyRequiresOptIn(t *testing.T) {
	window, err := NewIncrementalRankWindow(m065MutableRankWindowDefinition(IncrementalWindowRank))
	if err != nil {
		t.Fatal(err)
	}
	_, err = window.Apply([]IncrementalRankWindowMutation{{
		Kind: IncrementalRankWindowInsert,
		Key:  "a",
		Row:  Row{"id": "a", "group": "g", "score": int64(1)},
	}})
	if !errors.Is(err, ErrIncrementalWindowMutationsDisabled) {
		t.Fatalf("Apply error = %v, want %v", err, ErrIncrementalWindowMutationsDisabled)
	}
}
