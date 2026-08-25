package hatSql

import (
	"bytes"
	"context"
	stdjson "encoding/json"
)

// CanonicalSnapshot encodes a query result as stable JSON for regression
// fixtures. Rows are represented in declared column order, so map iteration
// cannot affect the resulting bytes. Volatile query statistics are excluded.
func CanonicalSnapshot(result QueryResult) ([]byte, error) {
	columns := append([]string(nil), result.Columns...)
	rows := make([][]interface{}, len(result.Rows))
	for rowIndex, row := range result.Rows {
		values := make([]interface{}, len(columns))
		for columnIndex, column := range columns {
			values[columnIndex] = row[column]
		}
		rows[rowIndex] = values
	}
	var output bytes.Buffer
	encoder := stdjson.NewEncoder(&output)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(struct {
		Columns []string        `json:"columns"`
		Rows    [][]interface{} `json:"rows"`
	}{Columns: columns, Rows: rows}); err != nil {
		return nil, err
	}
	return output.Bytes(), nil
}

// SnapshotQuery executes a query and returns its canonical regression fixture.
func SnapshotQuery(ctx context.Context, source string, resolver SourceResolver, parameters []interface{}, options QueryOptions) ([]byte, error) {
	result, err := ExecuteQueryParameters(ctx, source, resolver, parameters, options)
	if err != nil {
		return nil, err
	}
	return CanonicalSnapshot(result)
}
