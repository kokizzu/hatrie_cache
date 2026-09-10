package hatSql

import (
	"errors"
	"reflect"
	"testing"
)

var errM065RankWindowTest = errors.New("M065 rank window test failure")

func TestIncrementalRankWindowModes(t *testing.T) {
	rows := []Row{
		{"id": "a1", "group": "a", "score": int64(10)},
		{"id": "a2", "group": "a", "score": int64(10)},
		{"id": "a3", "group": "a", "score": int64(20)},
		{"id": "b1", "group": "b", "score": int64(5)},
	}
	tests := []struct {
		name  string
		kind  IncrementalRankWindowKind
		field string
		want  []int64
	}{
		{name: "row number", kind: IncrementalWindowRowNumber, field: "row_number", want: []int64{1, 2, 3, 1}},
		{name: "rank", kind: IncrementalWindowRank, field: "rank", want: []int64{1, 1, 3, 1}},
		{name: "dense rank", kind: IncrementalWindowDenseRank, field: "dense_rank", want: []int64{1, 1, 2, 1}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			window, err := NewIncrementalRankWindow(IncrementalRankWindowDefinition{
				Kind:         test.kind,
				OutputColumn: test.field,
				PartitionKey: func(row Row) (string, error) { return row["group"].(string), nil },
				OrderKey:     func(row Row) (interface{}, error) { return row["score"], nil },
				RowKey:       func(row Row) (string, error) { return row["id"].(string), nil },
			})
			if err != nil {
				t.Fatal(err)
			}
			updates, err := window.Append(rows)
			if err != nil {
				t.Fatal(err)
			}
			if len(updates) != len(rows) {
				t.Fatalf("updates = %d, want %d", len(updates), len(rows))
			}
			for index, update := range updates {
				if update.Key != rows[index]["id"] || update.Diff != 1 || update.Row[test.field] != test.want[index] {
					t.Fatalf("update[%d] = %#v, want key/rank %v/%d", index, update, rows[index]["id"], test.want[index])
				}
				if _, exists := rows[index][test.field]; exists {
					t.Fatalf("input row %d acquired output field", index)
				}
			}
			updates[0].Row["group"] = "changed"
			if rows[0]["group"] != "a" {
				t.Fatal("output row aliases input row map")
			}
		})
	}
}

func TestIncrementalRankWindowAppendOnlyOrderingAndAtomicity(t *testing.T) {
	window, err := NewIncrementalRankWindow(IncrementalRankWindowDefinition{
		Kind:         IncrementalWindowRank,
		OutputColumn: "rank",
		OrderKey:     func(row Row) (interface{}, error) { return row["score"], nil },
		RowKey:       func(row Row) (string, error) { return row["id"].(string), nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := window.Append([]Row{{"id": "one", "score": int64(10)}}); err != nil {
		t.Fatal(err)
	}
	_, err = window.Append([]Row{{"id": "three", "score": int64(20)}, {"id": "two", "score": int64(15)}})
	if !errors.Is(err, ErrIncrementalWindowOutOfOrder) {
		t.Fatalf("out-of-order error = %v, want %v", err, ErrIncrementalWindowOutOfOrder)
	}
	updates, err := window.Append([]Row{{"id": "two", "score": int64(20)}})
	if err != nil {
		t.Fatal(err)
	}
	if len(updates) != 1 || updates[0].Row["rank"] != int64(2) {
		t.Fatalf("post-rollback update = %#v, want rank 2", updates)
	}
	_, err = window.Append([]Row{{"id": "two", "score": int64(20)}})
	if !errors.Is(err, ErrIncrementalWindowDuplicateKey) {
		t.Fatalf("duplicate error = %v, want %v", err, ErrIncrementalWindowDuplicateKey)
	}
}

func TestIncrementalRankWindowDescendingAndCallbackRollback(t *testing.T) {
	window, err := NewIncrementalRankWindow(IncrementalRankWindowDefinition{
		Kind:         IncrementalWindowDenseRank,
		OutputColumn: "dense_rank",
		Descending:   true,
		OrderKey: func(row Row) (interface{}, error) {
			if row["id"] == "bad" {
				return nil, errM065RankWindowTest
			}
			return row["score"], nil
		},
		RowKey: func(row Row) (string, error) { return row["id"].(string), nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = window.Append([]Row{{"id": "high", "score": int64(20)}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = window.Append([]Row{{"id": "bad", "score": int64(10)}})
	if !errors.Is(err, errM065RankWindowTest) {
		t.Fatalf("callback error = %v, want %v", err, errM065RankWindowTest)
	}
	updates, err := window.Append([]Row{{"id": "low", "score": int64(10)}})
	if err != nil {
		t.Fatal(err)
	}
	if len(updates) != 1 || updates[0].Row["dense_rank"] != int64(2) {
		t.Fatalf("descending update = %#v, want dense rank 2", updates)
	}
}

func TestIncrementalRankWindowValidatesDefinitionAndOutputOwnership(t *testing.T) {
	valid := IncrementalRankWindowDefinition{
		Kind:         IncrementalWindowRank,
		OutputColumn: "rank",
		OrderKey:     func(Row) (interface{}, error) { return int64(1), nil },
		RowKey:       func(Row) (string, error) { return "key", nil },
	}
	for name, definition := range map[string]IncrementalRankWindowDefinition{
		"invalid kind":      {OutputColumn: "rank", OrderKey: valid.OrderKey, RowKey: valid.RowKey},
		"missing output":    {Kind: valid.Kind, OrderKey: valid.OrderKey, RowKey: valid.RowKey},
		"missing order key": {Kind: valid.Kind, OutputColumn: valid.OutputColumn, RowKey: valid.RowKey},
		"missing row key":   {Kind: valid.Kind, OutputColumn: valid.OutputColumn, OrderKey: valid.OrderKey},
	} {
		if _, err := NewIncrementalRankWindow(definition); err == nil {
			t.Fatalf("%s: constructor unexpectedly succeeded", name)
		}
	}
	window, err := NewIncrementalRankWindow(valid)
	if err != nil {
		t.Fatal(err)
	}
	input := Row{"id": "input", "score": int64(1), "rank": "existing"}
	if _, err := window.Append([]Row{input}); !errors.Is(err, ErrIncrementalWindowOutputConflict) {
		t.Fatalf("output conflict error = %v, want %v", err, ErrIncrementalWindowOutputConflict)
	}
	if !reflect.DeepEqual(input, Row{"id": "input", "score": int64(1), "rank": "existing"}) {
		t.Fatalf("input row mutated = %#v", input)
	}
}

func TestIncrementalRankWindowNilReceiver(t *testing.T) {
	var window *IncrementalRankWindow
	if _, err := window.Append(nil); !errors.Is(err, ErrIncrementalRankWindowNil) {
		t.Fatalf("nil Append error = %v, want %v", err, ErrIncrementalRankWindowNil)
	}
}

func TestIncrementalRankWindowExampleUsesSQLRows(t *testing.T) {
	window, err := NewIncrementalRankWindow(IncrementalRankWindowDefinition{
		Kind:         IncrementalWindowRowNumber,
		OutputColumn: "row_number",
		OrderKey:     func(row Row) (interface{}, error) { return row["sequence"], nil },
		RowKey:       func(row Row) (string, error) { return row["id"].(string), nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	updates, err := window.Append([]Row{{"id": "a", "sequence": int64(1)}})
	if err != nil || len(updates) != 1 {
		t.Fatalf("Append() = %#v, %v", updates, err)
	}
	if got := updates[0].Row["row_number"]; got != int64(1) {
		t.Fatalf("row number = %#v, want 1", got)
	}
}
