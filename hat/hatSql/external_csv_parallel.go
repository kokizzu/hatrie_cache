package hatSql

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"runtime"
	"sync"
)

type csvParallelRecordSpan struct {
	start int
	end   int
}

type csvParallelBlock struct {
	firstRow int
	count    int
	start    int
	end      int
	rows     []Row
	err      error
}

// ParseCSVParallel parses a header-based RFC 4180 CSV document in parallel.
// A bounded serial framing pass keeps quoted newlines inside one record, after
// which complete record blocks are decoded concurrently. Rows retain input
// order and malformed records are reported in deterministic block order.
// A non-positive workers value uses the current GOMAXPROCS setting.
func ParseCSVParallel(reader io.Reader, workers int) ([]Row, error) {
	_, rows, err := parseCSVParallel(reader, workers)
	return rows, err
}

// ImportCSVParallel parses CSV concurrently and atomically replaces name only
// after the header and every record have been validated successfully.
func (tables *ExternalTables) ImportCSVParallel(name string, data []byte, workers int) error {
	if tables == nil {
		return fmt.Errorf("external tables are nil")
	}
	columns, rows, err := parseCSVParallel(bytes.NewReader(data), workers)
	if err != nil {
		return err
	}
	return tables.registerExternalTable(name, ExternalTable{Columns: columns, Rows: rows}, false)
}

func parseCSVParallel(reader io.Reader, workers int) ([]string, []Row, error) {
	if reader == nil {
		return nil, nil, fmt.Errorf("CSV reader is required")
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, nil, fmt.Errorf("read CSV: %w", err)
	}
	spans := frameCSVParallelRecords(data)
	if len(spans) == 0 {
		return nil, nil, fmt.Errorf("CSV requires a header row")
	}
	header, err := parseCSVParallelSingleRecord(data[spans[0].start:spans[0].end])
	if err != nil {
		return nil, nil, fmt.Errorf("parse CSV header: %w", err)
	}
	columns, err := externalTableColumns(header)
	if err != nil {
		return nil, nil, err
	}
	rowSpans := spans[1:]
	if len(rowSpans) == 0 {
		return columns, []Row{}, nil
	}
	if workers <= 0 {
		workers = runtime.GOMAXPROCS(0)
	}
	if workers < 1 {
		workers = 1
	}
	if workers > len(rowSpans) {
		workers = len(rowSpans)
	}

	blocks := make([]csvParallelBlock, workers)
	var wait sync.WaitGroup
	wait.Add(workers)
	for worker := 0; worker < workers; worker++ {
		start := len(rowSpans) * worker / workers
		end := len(rowSpans) * (worker + 1) / workers
		block := &blocks[worker]
		block.firstRow = start + 2
		block.count = end - start
		block.start = rowSpans[start].start
		block.end = rowSpans[end-1].end
		go func(block *csvParallelBlock) {
			defer wait.Done()
			block.rows, block.err = parseCSVParallelBlock(data[block.start:block.end], columns, block.firstRow, block.count)
		}(block)
	}
	wait.Wait()

	rows := make([]Row, 0, len(rowSpans))
	for _, block := range blocks {
		if block.err != nil {
			return nil, nil, block.err
		}
		rows = append(rows, block.rows...)
	}
	return columns, rows, nil
}

func parseCSVParallelBlock(data []byte, columns []string, firstRow, count int) ([]Row, error) {
	reader := csv.NewReader(bytes.NewReader(data))
	reader.ReuseRecord = true
	rows := make([]Row, 0, count)
	for offset := 0; offset < count; offset++ {
		record, err := reader.Read()
		if err == io.EOF {
			return nil, fmt.Errorf("parse CSV record %d: unexpected end of block", firstRow+offset)
		}
		if err != nil {
			return nil, fmt.Errorf("parse CSV record %d: %w", firstRow+offset, err)
		}
		if len(record) != len(columns) {
			return nil, fmt.Errorf("CSV row %d has %d fields, want %d", firstRow+offset, len(record), len(columns))
		}
		row := make(Row, len(columns))
		for column, value := range record {
			row[columns[column]] = value
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func parseCSVParallelSingleRecord(data []byte) ([]string, error) {
	reader := csv.NewReader(bytes.NewReader(data))
	record, err := reader.Read()
	if err != nil {
		return nil, err
	}
	return append([]string(nil), record...), nil
}

func frameCSVParallelRecords(data []byte) []csvParallelRecordSpan {
	spans := make([]csvParallelRecordSpan, 0)
	start := 0
	inQuotes := false
	atFieldStart := true
	for index := 0; index < len(data); index++ {
		value := data[index]
		if inQuotes {
			if value == '"' {
				if index+1 < len(data) && data[index+1] == '"' {
					index++
					continue
				}
				inQuotes = false
				atFieldStart = false
			}
			continue
		}
		if value == '"' && atFieldStart {
			inQuotes = true
			atFieldStart = false
			continue
		}
		switch value {
		case ',':
			atFieldStart = true
		case '\n':
			if !csvParallelBlankRecord(data[start:index]) {
				spans = append(spans, csvParallelRecordSpan{start: start, end: index + 1})
			}
			start = index + 1
			atFieldStart = true
		default:
			atFieldStart = false
		}
	}
	if start < len(data) && !csvParallelBlankRecord(data[start:]) {
		spans = append(spans, csvParallelRecordSpan{start: start, end: len(data)})
	}
	return spans
}

func csvParallelBlankRecord(data []byte) bool {
	for _, value := range data {
		if value != '\r' {
			return false
		}
	}
	return true
}
