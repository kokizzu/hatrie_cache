package hatSql

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
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

// ParseCSVParallel parses RFC 4180 CSV data with a bounded worker pool. The
// header is parsed first, logical records retain input order, and malformed
// records return the lowest source record error deterministically.
func ParseCSVParallel(data []byte, options ExternalImportOptions, workers int) ([]string, []Row, error) {
	var err error
	options, err = options.normalize()
	if err != nil {
		return nil, nil, err
	}
	if options.MaxBytes > 0 && int64(len(data)) > options.MaxBytes {
		return nil, nil, fmt.Errorf("CSV byte limit exceeded: maximum %d bytes", options.MaxBytes)
	}

	var headerStart, headerEnd int
	headerFound := false
	dataRows := 0
	scanCSVRecords(data, func(start, end int) bool {
		if !headerFound {
			headerStart, headerEnd = start, end
			headerFound = true
			return true
		}
		dataRows++
		return true
	})
	if !headerFound {
		return nil, nil, fmt.Errorf("CSV requires a header row")
	}

	header, err := parseCSVRecord(data[headerStart:headerEnd])
	if err != nil {
		return nil, nil, fmt.Errorf("parse CSV header: %w", err)
	}
	if len(header) == 0 {
		return nil, nil, fmt.Errorf("CSV requires a header row")
	}
	columns := append([]string(nil), header...)

	if options.MaxRows > 0 && dataRows > options.MaxRows {
		return nil, nil, fmt.Errorf("CSV row limit exceeded: maximum %d rows", options.MaxRows)
	}
	if dataRows == 0 {
		return columns, []Row{}, nil
	}

	if workers <= 0 {
		workers = runtime.GOMAXPROCS(0)
	}
	if workers < 1 {
		workers = 1
	}
	if workers > dataRows {
		workers = dataRows
	}

	recordLengths := []int(nil)
	if options.MaxRecordBytes > 0 {
		recordLengths = make([]int, dataRows)
	}
	chunks := make([]csvParallelChunk, workers)
	dataIndex := 0
	headerSkipped := false
	scanCSVRecords(data, func(start, end int) bool {
		if !headerSkipped {
			headerSkipped = true
			return true
		}
		if recordLengths != nil {
			recordLengths[dataIndex] = end - start
		}
		worker := dataIndex * workers / dataRows
		if chunks[worker].endIndex == 0 {
			chunks[worker].start = start
			chunks[worker].startIndex = dataIndex
		}
		chunks[worker].end = end
		chunks[worker].endIndex = dataIndex + 1
		dataIndex++
		return true
	})

	rows := make([]Row, dataRows)
	parseErrors := make([]error, dataRows)
	var wait sync.WaitGroup
	wait.Add(workers)
	for worker := range workers {
		chunk := chunks[worker]
		go func(chunk csvParallelChunk) {
			defer wait.Done()
			reader := csv.NewReader(bytes.NewReader(data[chunk.start:chunk.end]))
			reader.ReuseRecord = true
			reader.FieldsPerRecord = -1
			for index := chunk.startIndex; index < chunk.endIndex; index++ {
				recordNumber := index + 2
				if recordLengths != nil && recordLengths[index] > options.MaxRecordBytes {
					parseErrors[index] = fmt.Errorf("CSV record %d exceeds maximum %d bytes", recordNumber, options.MaxRecordBytes)
					return
				}
				values, recordErr := reader.Read()
				if recordErr != nil {
					parseErrors[index] = fmt.Errorf("parse CSV record %d: %w", recordNumber, recordErr)
					return
				}
				if len(values) != len(columns) {
					parseErrors[index] = fmt.Errorf("CSV record %d has %d fields, want %d", recordNumber, len(values), len(columns))
					continue
				}
				row := make(Row, len(columns))
				for columnIndex, column := range columns {
					row[column] = values[columnIndex]
				}
				rows[index] = row
			}
		}(chunk)
	}
	wait.Wait()

	for _, parseErr := range parseErrors {
		if parseErr != nil {
			return nil, nil, parseErr
		}
	}
	return columns, rows, nil
}

// ImportCSVParallel parses CSV concurrently and atomically replaces name only
// after every record has been validated successfully.
func (tables *ExternalTables) ImportCSVParallel(name string, data []byte, options ExternalImportOptions, workers int) error {
	columns, rows, err := ParseCSVParallel(data, options, workers)
	if err != nil {
		return err
	}
	return tables.Register(name, ExternalTable{Columns: columns, Rows: rows})
}

type csvParallelChunk struct {
	start      int
	end        int
	startIndex int
	endIndex   int
}

// scanCSVRecords finds logical record boundaries without interpreting CSV
// fields. encoding/csv still performs the actual RFC 4180 validation for each
// record, including quoted newlines and escaped quotes.
func scanCSVRecords(data []byte, visit func(start, end int) bool) {
	if len(data) == 0 {
		return
	}
	start := 0
	inQuotes := false
	for index := 0; index < len(data); index++ {
		switch data[index] {
		case '"':
			if inQuotes && index+1 < len(data) && data[index+1] == '"' {
				index++
				continue
			}
			inQuotes = !inQuotes
		case '\n':
			if inQuotes {
				continue
			}
			if record := data[start : index+1]; !isCSVBlankRecord(record) {
				if !visit(start, index+1) {
					return
				}
			}
			start = index + 1
		}
	}
	if start < len(data) {
		if record := data[start:]; !isCSVBlankRecord(record) {
			visit(start, len(data))
		}
	}
}

func isCSVBlankRecord(record []byte) bool {
	if len(record) > 0 && record[len(record)-1] == '\n' {
		record = record[:len(record)-1]
	}
	if len(record) > 0 && record[len(record)-1] == '\r' {
		record = record[:len(record)-1]
	}
	return len(record) == 0
}

func parseCSVRecord(data []byte) ([]string, error) {
	reader := csv.NewReader(bytes.NewReader(data))
	reader.ReuseRecord = true
	values, err := reader.Read()
	if err != nil {
		return nil, err
	}
	if _, err := reader.Read(); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("record contains multiple logical rows")
		}
		return nil, err
	}
	return append([]string(nil), values...), nil
}
