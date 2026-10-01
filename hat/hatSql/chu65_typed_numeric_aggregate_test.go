package hatSql

import (
	"context"
	"encoding/binary"
	"errors"
	"math"
	"reflect"
	"testing"
)

type chu65NumericAggregateResolver struct {
	batch ColumnarBatch
}

func (resolver chu65NumericAggregateResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return nil, errors.New("row source must not be resolved for typed numeric aggregate")
}

func (resolver chu65NumericAggregateResolver) ResolveSQLColumnarSource(string, string, []string) (ColumnarBatch, bool, error) {
	return resolver.batch, true, nil
}

func chu65FloatBatch(values []float64, valid []bool) ColumnarBatch {
	data := make([]byte, len(values)*8)
	validity := make([]byte, (len(values)+7)/8)
	allValid := true
	for index, value := range values {
		binary.LittleEndian.PutUint64(data[index*8:], math.Float64bits(value))
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
			"value": {Kind: ColumnarNumericFloat64, Data: data, Validity: validity, Rows: len(values)},
		},
		Rows: len(values),
	}
}

func chu65IntBatch(values []int64, valid []bool) ColumnarBatch {
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

func TestSQLColumnarPackedNumericValue(t *testing.T) {
	batch := chu65FloatBatch([]float64{1.5, 2.5, 3.5}, []bool{true, false, true})
	column := batch.NumericColumns["value"]
	if value, ok := sqlColumnarPackedNumericValue(column, 0); !ok || value != 1.5 {
		t.Fatalf("first packed value = %v, %t; want 1.5, true", value, ok)
	}
	if value, ok := sqlColumnarPackedNumericValue(column, 1); ok || value != 0 {
		t.Fatalf("NULL packed value = %v, %t; want 0, false", value, ok)
	}
	if value, ok := sqlColumnarPackedNumericValue(column, 3); ok || value != 0 {
		t.Fatalf("out-of-range packed value = %v, %t; want 0, false", value, ok)
	}
}

func TestSQLColumnarTypedNumericAggregatesPreserveSemantics(t *testing.T) {
	batch := chu65FloatBatch(
		[]float64{1.5, 2.5, 3.5, 4.5, 5.5},
		[]bool{true, false, true, true, false},
	)
	resolver := chu65NumericAggregateResolver{batch: batch}
	result, err := ExecuteSQLQueryParameters(context.Background(), "SELECT SUM(value) AS sum, AVG(value) AS avg, MIN(value) AS min, MAX(value) AS max FROM CACHE('items')", resolver, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if want := []SQLRow{{"sum": float64(9.5), "avg": float64(9.5 / 3.0), "min": float64(1.5), "max": float64(4.5)}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("aggregate result = %#v, want %#v", result.Rows, want)
	}

	filtered, err := ExecuteSQLQueryParameters(context.Background(), "SELECT SUM(value) AS sum, AVG(value) AS avg, MIN(value) AS min, MAX(value) AS max FROM CACHE('items') WHERE value >= 3", resolver, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if want := []SQLRow{{"sum": float64(8), "avg": float64(4), "min": float64(3.5), "max": float64(4.5)}}; !reflect.DeepEqual(filtered.Rows, want) {
		t.Fatalf("filtered aggregate result = %#v, want %#v", filtered.Rows, want)
	}

	plain := chu65NumericAggregateResolver{batch: ColumnarBatch{
		Columns: map[string][]interface{}{"value": {1.5, nil, 3.5}},
		Rows:    3,
	}}
	fallback, err := ExecuteSQLQueryParameters(context.Background(), "SELECT SUM(value) AS sum, AVG(value) AS avg FROM CACHE('items')", plain, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if want := []SQLRow{{"sum": float64(5), "avg": float64(2.5)}}; !reflect.DeepEqual(fallback.Rows, want) {
		t.Fatalf("fallback aggregate result = %#v, want %#v", fallback.Rows, want)
	}

	integer, err := ExecuteSQLQueryParameters(context.Background(), "SELECT SUM(value) AS sum, MIN(value) AS min, MAX(value) AS max FROM CACHE('items')", chu65NumericAggregateResolver{batch: chu65IntBatch([]int64{-4, 8, 12}, []bool{true, false, true})}, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if want := []SQLRow{{"sum": float64(8), "min": float64(-4), "max": float64(12)}}; !reflect.DeepEqual(integer.Rows, want) {
		t.Fatalf("integer aggregate result = %#v, want %#v", integer.Rows, want)
	}
}
