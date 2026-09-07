package hatSql

import (
	"bytes"
	"encoding/json"
	"fmt"
	"runtime"
	"sync"
)

// ParseNDJSONParallel parses one JSON object per non-empty line using a
// bounded worker pool. Rows retain input order, and when multiple records are
// invalid the error for the lowest source line is returned deterministically.
// A non-positive workers value uses the current GOMAXPROCS setting.
func ParseNDJSONParallel(data []byte, workers int) ([]Row, error) {
	lines := bytes.Split(data, []byte{'\n'})
	if workers <= 0 {
		workers = runtime.GOMAXPROCS(0)
	}
	if workers < 1 {
		workers = 1
	}
	if workers > len(lines) {
		workers = len(lines)
	}

	rows := make([]Row, len(lines))
	parseErrors := make([]error, len(lines))
	var wait sync.WaitGroup
	wait.Add(workers)
	for worker := range workers {
		start := len(lines) * worker / workers
		end := len(lines) * (worker + 1) / workers
		go func(start, end int) {
			defer wait.Done()
			for lineNumber := start; lineNumber < end; lineNumber++ {
				line := bytes.TrimSpace(lines[lineNumber])
				if len(line) == 0 {
					continue
				}
				var row Row
				if err := json.Unmarshal(line, &row); err != nil {
					parseErrors[lineNumber] = fmt.Errorf("parse NDJSON record %d: %w", lineNumber+1, err)
					continue
				}
				if row == nil {
					parseErrors[lineNumber] = fmt.Errorf("NDJSON record %d must be an object", lineNumber+1)
					continue
				}
				rows[lineNumber] = row
			}
		}(start, end)
	}
	wait.Wait()

	result := make([]Row, 0, len(lines))
	for lineNumber := range lines {
		if err := parseErrors[lineNumber]; err != nil {
			return nil, err
		}
		if rows[lineNumber] != nil {
			result = append(result, rows[lineNumber])
		}
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("NDJSON requires at least one object record")
	}
	return result, nil
}

// ImportNDJSONParallel parses NDJSON concurrently and atomically replaces
// name only after every record has been validated successfully.
func (tables *ExternalTables) ImportNDJSONParallel(name string, data []byte, workers int) error {
	rows, err := ParseNDJSONParallel(data, workers)
	if err != nil {
		return err
	}
	return tables.Register(name, ExternalTable{Columns: externalTableRowColumns(rows), Rows: rows})
}
