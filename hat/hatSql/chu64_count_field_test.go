package hatSql

import (
	"context"
	"encoding/binary"
	"errors"
	"reflect"
	"testing"
)

type chu64CountFieldResolver struct {
	batch ColumnarBatch
}

func (resolver chu64CountFieldResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return nil, errors.New("row source must not be resolved for columnar count")
}

func (resolver chu64CountFieldResolver) ResolveSQLColumnarSource(string, string, []string) (ColumnarBatch, bool, error) {
	return resolver.batch, true, nil
}

func chu64NumericBatch(values []int64, valid []bool) ColumnarBatch {
	data := make([]byte, len(values)*8)
	validity := make([]byte, (len(values)+7)/8)
	allValid := true
	for index, value := range values {
		binary.LittleEndian.PutUint64(data[index*8:], uint64(value))
		if valid[index] {
			validity[index>>3] |= 1 << uint(index&7)
		} else {
			allValid = false
		}
	}
	if allValid {
		validity = nil
	}
	return ColumnarBatch{
		NumericColumns: map[string]ColumnarNumericColumn{
			"value": {Kind: ColumnarNumericInt64, Data: data, Validity: validity, Rows: len(values)},
		},
		Rows: len(values),
	}
}

func TestSQLColumnarCountFieldUsesValidityMetadata(t *testing.T) {
	numeric := chu64NumericBatch(
		[]int64{1, 2, 3, 4, 5},
		[]bool{true, false, true, false, true},
	)
	if count, ok := sqlColumnarNonNullCount(numeric, "value"); !ok || count != 3 {
		t.Fatalf("numeric non-NULL count = %d, %t; want 3, true", count, ok)
	}

	boolean := ColumnarBatch{
		BoolColumns: map[string]ColumnarBoolColumn{
			"value": {Bits: []byte{0x15}, Validity: []byte{0x17}, Rows: 5},
		},
		Rows: 5,
	}
	if count, ok := sqlColumnarNonNullCount(boolean, "value"); !ok || count != 4 {
		t.Fatalf("boolean non-NULL count = %d, %t; want 4, true", count, ok)
	}

	packed := ColumnarBatch{
		PackedColumns: map[string]ColumnarPackedColumn{
			"value": {Values: []interface{}{int64(1), int64(3), int64(5)}, Validity: []byte{0x15}, Ranks: []uint32{0, 3}, Rows: 5},
		},
		Rows: 5,
	}
	if count, ok := sqlColumnarNonNullCount(packed, "value"); !ok || count != 3 {
		t.Fatalf("packed non-NULL count = %d, %t; want 3, true", count, ok)
	}

	result, err := ExecuteSQLQueryParameters(context.Background(), "SELECT COUNT(value) AS total FROM CACHE('items')", chu64CountFieldResolver{batch: numeric}, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if want := []SQLRow{{"total": int64(3)}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("numeric count result = %#v, want %#v", result.Rows, want)
	}

	for name, testCase := range map[string]struct {
		batch ColumnarBatch
		want  int64
	}{
		"boolean": {batch: boolean, want: 4},
		"packed":  {batch: packed, want: 3},
	} {
		t.Run(name, func(t *testing.T) {
			result, err := ExecuteSQLQueryParameters(context.Background(), "SELECT COUNT(value) AS total FROM CACHE('items')", chu64CountFieldResolver{batch: testCase.batch}, nil, SQLQueryOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if want := []SQLRow{{"total": testCase.want}}; !reflect.DeepEqual(result.Rows, want) {
				t.Fatalf("count result = %#v, want %#v", result.Rows, want)
			}
		})
	}

	result, err = ExecuteSQLQueryParameters(context.Background(), "SELECT COUNT(*) AS rows, COUNT(value) AS total, SUM(value) AS sum FROM CACHE('items')", chu64CountFieldResolver{batch: numeric}, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if want := []SQLRow{{"rows": int64(5), "total": int64(3), "sum": float64(9)}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("mixed count result = %#v, want %#v", result.Rows, want)
	}
}

func TestSQLColumnarCountFieldKeepsFilteringAndFallbackSemantics(t *testing.T) {
	batch := chu64NumericBatch(
		[]int64{1, 2, 3, 4, 5, 6},
		[]bool{true, false, true, true, false, true},
	)
	resolver := chu64CountFieldResolver{batch: batch}
	result, err := ExecuteSQLQueryParameters(context.Background(), "SELECT COUNT(value) AS total FROM CACHE('items') WHERE value >= 3", resolver, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if want := []SQLRow{{"total": int64(3)}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("filtered count result = %#v, want %#v", result.Rows, want)
	}

	plain := chu64CountFieldResolver{batch: ColumnarBatch{
		Columns: map[string][]interface{}{"value": {int64(1), nil, int64(3), nil}},
		Rows:    4,
	}}
	result, err = ExecuteSQLQueryParameters(context.Background(), "SELECT COUNT(value) AS total FROM CACHE('items')", plain, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if want := []SQLRow{{"total": int64(2)}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("plain fallback count result = %#v, want %#v", result.Rows, want)
	}
}

func TestSQLColumnarCountFieldRejectsMalformedValidity(t *testing.T) {
	malformed := ColumnarBatch{
		NumericColumns: map[string]ColumnarNumericColumn{
			"value": {Kind: ColumnarNumericInt64, Data: make([]byte, 8), Validity: []byte{0xff}, Rows: 1},
		},
		Rows: 1,
	}
	if count, ok := sqlColumnarNonNullCount(malformed, "value"); ok || count != 0 {
		t.Fatalf("malformed non-NULL count = %d, %t; want 0, false", count, ok)
	}
}
