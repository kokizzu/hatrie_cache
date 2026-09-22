//go:build t217

package hatDataStructure

import (
	"errors"
	"reflect"
	"testing"
)

func TestT217ColumnarSpaceStoresTypedColumnsAndNulls(t *testing.T) {
	space, err := NewColumnarSpace(ColumnarSpaceOptions{
		Name:     "events",
		Capacity: 4,
		Columns: []ColumnarColumnSpec{
			{Name: "id", Kind: ColumnarInt64},
			{Name: "score", Kind: ColumnarFloat64},
			{Name: "region", Kind: ColumnarString},
			{Name: "active", Kind: ColumnarBool},
			{Name: "payload", Kind: ColumnarBytes},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer space.Close()

	rows := [][]ColumnarValue{
		{
			{Kind: ColumnarInt64, Valid: true, Int64: 101},
			{Kind: ColumnarFloat64, Valid: true, Float64: 1.5},
			{Kind: ColumnarString, Valid: true, String: "sg"},
			{Kind: ColumnarBool, Valid: true, Bool: true},
			{Kind: ColumnarBytes, Valid: true, Bytes: []byte{1, 2}},
		},
		{
			{Kind: ColumnarInt64, Valid: true, Int64: 102},
			{Kind: ColumnarFloat64, Valid: false, Float64: 99},
			{Kind: ColumnarString, Valid: true, String: "apac"},
			{Kind: ColumnarBool, Valid: true, Bool: false},
			{Kind: ColumnarBytes, Valid: false, Bytes: []byte{9, 9}},
		},
	}
	for _, row := range rows {
		if err := space.Append(row); err != nil {
			t.Fatal(err)
		}
	}
	if got := space.Rows(); got != 2 {
		t.Fatalf("Rows() = %d, want 2", got)
	}
	if got := space.ColumnCount(); got != 5 {
		t.Fatalf("ColumnCount() = %d, want 5", got)
	}

	region, found, err := space.Column("region")
	if err != nil || !found {
		t.Fatalf("Column(region) = %#v, %v, want found", region, err)
	}
	if got := region.StringData; string(got) != "sgapac" {
		t.Fatalf("region.StringData = %q, want compact concatenation", got)
	}
	if want := []uint32{0, 2, 6}; !reflect.DeepEqual(region.StringOffsets, want) {
		t.Fatalf("region.StringOffsets = %v, want %v", region.StringOffsets, want)
	}
	if got, valid := region.StringAt(1); !valid || got != "apac" {
		t.Fatalf("region.StringAt(1) = %q, %t, want apac,true", got, valid)
	}

	score, found, err := space.Column("score")
	if err != nil || !found {
		t.Fatalf("Column(score) = %#v, %v, want found", score, err)
	}
	if score.ValidAt(1) {
		t.Fatal("score row 1 is valid, want null")
	}
	if got := score.Float64Values[0]; got != 1.5 {
		t.Fatalf("score[0] = %v, want 1.5", got)
	}

	active, found, err := space.Column("active")
	if err != nil || !found {
		t.Fatalf("Column(active) = %#v, %v, want found", active, err)
	}
	if got, valid := active.BoolAt(1); !valid || got {
		t.Fatalf("active.BoolAt(1) = %t, %t, want false,true", got, valid)
	}

	payload, found, err := space.Column("payload")
	if err != nil || !found {
		t.Fatalf("Column(payload) = %#v, %v, want found", payload, err)
	}
	if want := []byte{1, 2}; !reflect.DeepEqual(payload.BytesData, want) {
		t.Fatalf("payload.BytesData = %v, want %v", payload.BytesData, want)
	}
	if want := []uint32{0, 2, 2}; !reflect.DeepEqual(payload.BytesOffsets, want) {
		t.Fatalf("payload.BytesOffsets = %v, want %v", payload.BytesOffsets, want)
	}
	if _, valid := payload.BytesAt(1); valid {
		t.Fatal("payload row 1 is valid, want null")
	}

	value, valid, err := space.ValueAt(0, "payload")
	if err != nil || !valid || !reflect.DeepEqual(value.Bytes, []byte{1, 2}) {
		t.Fatalf("ValueAt(payload) = %#v, %t, %v", value, valid, err)
	}
}

func TestT217ColumnarSpaceRejectsMalformedRowsAtomically(t *testing.T) {
	space, err := NewColumnarSpace(ColumnarSpaceOptions{
		Name:     "events",
		Capacity: 1,
		Columns:  []ColumnarColumnSpec{{Name: "id", Kind: ColumnarInt64}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer space.Close()

	if err := space.Append([]ColumnarValue{{Kind: ColumnarString, Valid: true, String: "wrong"}}); !errors.Is(err, ErrColumnarSpaceValueInvalid) {
		t.Fatalf("wrong type error = %v, want %v", err, ErrColumnarSpaceValueInvalid)
	}
	if got := space.Rows(); got != 0 {
		t.Fatalf("Rows() after wrong type = %d, want 0", got)
	}
	if err := space.Append(nil); !errors.Is(err, ErrColumnarSpaceValueInvalid) {
		t.Fatalf("wrong width error = %v, want %v", err, ErrColumnarSpaceValueInvalid)
	}
	if err := space.Append([]ColumnarValue{{Kind: ColumnarInt64, Valid: true, Int64: 7}}); err != nil {
		t.Fatal(err)
	}
	if err := space.Append([]ColumnarValue{{Kind: ColumnarInt64, Valid: true, Int64: 8}}); !errors.Is(err, ErrColumnarSpaceCapacityExceeded) {
		t.Fatalf("capacity error = %v, want %v", err, ErrColumnarSpaceCapacityExceeded)
	}
	if got := space.Rows(); got != 1 {
		t.Fatalf("Rows() after capacity error = %d, want 1", got)
	}
}

func TestT217ColumnarSpaceValidatesSchemaAndNilReceivers(t *testing.T) {
	if _, err := NewColumnarSpace(ColumnarSpaceOptions{Name: ""}); !errors.Is(err, ErrColumnarSpaceSchemaInvalid) {
		t.Fatalf("empty schema error = %v, want %v", err, ErrColumnarSpaceSchemaInvalid)
	}
	if _, err := NewColumnarSpace(ColumnarSpaceOptions{
		Name:    "events",
		Columns: []ColumnarColumnSpec{{Name: "id", Kind: ColumnarColumnKind(99)}},
	}); !errors.Is(err, ErrColumnarSpaceSchemaInvalid) {
		t.Fatalf("invalid kind error = %v, want %v", err, ErrColumnarSpaceSchemaInvalid)
	}
	var space *ColumnarSpace
	if err := space.Append(nil); !errors.Is(err, ErrColumnarSpaceNil) {
		t.Fatalf("nil Append error = %v, want %v", err, ErrColumnarSpaceNil)
	}
	if got := space.Rows(); got != 0 {
		t.Fatalf("nil Rows() = %d, want 0", got)
	}
}
