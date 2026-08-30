package hatSql

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

type sqlBorrowedColumnarSourceProbe struct {
	batch          ColumnarBatch
	rows           []Row
	borrowedCalls  int
	resolutionCall int
}

func (probe *sqlBorrowedColumnarSourceProbe) ResolveSQLSource(string, string) ([]Row, error) {
	return probe.rows, nil
}

func (probe *sqlBorrowedColumnarSourceProbe) ResolveSQLColumnarSource(string, string, []string) (ColumnarBatch, bool, error) {
	probe.resolutionCall++
	return ColumnarBatch{}, false, errors.New("defensive columnar resolver must not be used")
}

func (probe *sqlBorrowedColumnarSourceProbe) BorrowSQLColumnarSource(string, string, []string) (ColumnarBatch, bool, error) {
	probe.borrowedCalls++
	return probe.batch, true, nil
}

func TestSQLColumnarSourceResolverUsesBorrowedImmutableBatchWhenAvailable(t *testing.T) {
	t.Parallel()
	probe := &sqlBorrowedColumnarSourceProbe{batch: ColumnarBatch{
		Columns: map[string][]interface{}{
			"id":     {int64(1), int64(2)},
			"status": {"queued", "running"},
		},
		Rows: 2,
	}, rows: []Row{{"id": int64(1), "status": "queued"}, {"id": int64(2), "status": "running"}}}
	wantBatch := cloneSQLColumnarBatchForTest(probe.batch)
	result, err := ExecuteSQLQueryParameters(context.Background(), "SELECT event.id, event.status FROM CACHE('events') AS event WHERE event.id >= 2", probe, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQLQuery() error = %v", err)
	}
	if got, want := result.Rows, []Row{{"id": int64(2), "status": "running"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("ExecuteSQLQuery() rows = %#v, want %#v", got, want)
	}
	if probe.borrowedCalls != 1 || probe.resolutionCall != 0 {
		t.Fatalf("borrowed/resolution calls = %d/%d, want 1/0", probe.borrowedCalls, probe.resolutionCall)
	}
	if !reflect.DeepEqual(probe.batch, wantBatch) {
		t.Fatalf("borrowed batch mutated = %#v, want %#v", probe.batch, wantBatch)
	}
}

func cloneSQLColumnarBatchForTest(batch ColumnarBatch) ColumnarBatch {
	clone := ColumnarBatch{Rows: batch.Rows}
	if batch.Columns != nil {
		clone.Columns = make(map[string][]interface{}, len(batch.Columns))
		for field, values := range batch.Columns {
			clone.Columns[field] = append([]interface{}(nil), values...)
		}
	}
	if batch.Dictionaries != nil {
		clone.Dictionaries = make(map[string]DictionaryColumn, len(batch.Dictionaries))
		for field, dictionary := range batch.Dictionaries {
			clone.Dictionaries[field] = DictionaryColumn{
				Values: append([]string(nil), dictionary.Values...),
				Codes:  append([]uint32(nil), dictionary.Codes...),
			}
		}
	}
	return clone
}
