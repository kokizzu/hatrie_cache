package hatSql

import (
	"context"
	"encoding/binary"
	"errors"
	"math"
	"reflect"
	"testing"
)

type chu66GroupedNumericAggregateResolver struct {
	batch ColumnarBatch
}

func (resolver chu66GroupedNumericAggregateResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return nil, errors.New("row source must not be resolved for grouped numeric aggregate")
}

func (resolver chu66GroupedNumericAggregateResolver) ResolveSQLColumnarSource(string, string, []string) (ColumnarBatch, bool, error) {
	return resolver.batch, true, nil
}

func chu66GroupedBatch(values []float64, valid []bool, codes []uint32, groups []string) ColumnarBatch {
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
		Dictionaries: map[string]DictionaryColumn{
			"group": {Values: groups, Codes: codes},
		},
		NumericColumns: map[string]ColumnarNumericColumn{
			"value": {Kind: ColumnarNumericFloat64, Data: data, Validity: validity, Rows: len(values)},
		},
		Rows: len(values),
	}
}

func TestSQLColumnarGroupedNumericAggregatePreparation(t *testing.T) {
	batch := chu66GroupedBatch([]float64{1, 2}, []bool{true, true}, []uint32{0, 1}, []string{"a", "b"})
	aggregate := sqlColumnarNumericAggregate{name: "SUM", field: "value"}
	sqlColumnarPrepareNumericAggregateColumn(&aggregate, batch)
	if !aggregate.numericPacked {
		t.Fatal("numeric grouped aggregate was not admitted to packed storage")
	}
}

func TestSQLColumnarGroupedNumericAggregatesPreserveSemantics(t *testing.T) {
	batch := chu66GroupedBatch(
		[]float64{1, 2, 3, 4, 5, 6},
		[]bool{true, true, false, true, true, false},
		[]uint32{0, 1, 0, 1, 0, 1},
		[]string{"a", "b"},
	)
	result, err := ExecuteSQLQueryParameters(context.Background(), "SELECT group, SUM(value) AS sum, AVG(value) AS avg, MIN(value) AS min, MAX(value) AS max FROM CACHE('items') GROUP BY group ORDER BY group", chu66GroupedNumericAggregateResolver{batch: batch}, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	want := []SQLRow{
		{"group": "a", "sum": float64(6), "avg": float64(3), "min": float64(1), "max": float64(5)},
		{"group": "b", "sum": float64(6), "avg": float64(3), "min": float64(2), "max": float64(4)},
	}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("grouped aggregate result = %#v, want %#v", result.Rows, want)
	}
}
