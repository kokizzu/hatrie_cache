package hatCache

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"time"

	"hatrie_cache/hat/hatSql"

	json "github.com/goccy/go-json"
)

// SQLRowBinaryImportOptions controls bounded batches used by
// ExecuteSQLRowBinaryInsert. A zero BatchSize uses the largest public atomic
// command batch supported by the cache.
type SQLRowBinaryImportOptions struct {
	BatchSize int `json:"batch_size,omitempty"`
}

// SQLRowBinaryImportResult reports rows applied by a RowBinary import. Each
// completed batch is atomic; a later malformed row or failed batch can leave
// the previously reported batches applied.
type SQLRowBinaryImportResult struct {
	Affected int                  `json:"affected"`
	Batches  int                  `json:"batches"`
	Response CacheCommandResponse `json:"response"`
}

// ExecuteSQLRowBinaryInsert imports a self-describing RowBinary stream into
// cache using the positional target columns from an INSERT target such as
// "INSERT INTO CACHE(key, value)". Supported target columns are key, value,
// counter, ttl_seconds, and unix_seconds. The stream is consumed one row at a
// time and applied in bounded atomic batches.
func ExecuteSQLRowBinaryInsert(ctx context.Context, trie *HatTrie, source string, input io.Reader, options SQLRowBinaryImportOptions) (SQLRowBinaryImportResult, error) {
	if trie == nil {
		return SQLRowBinaryImportResult{}, ErrNilHatTrie
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return SQLRowBinaryImportResult{}, err
	}
	targetColumns, err := parseSQLRowBinaryInsertTarget(source)
	if err != nil {
		return SQLRowBinaryImportResult{}, err
	}
	batchSize, err := normalizeSQLRowBinaryImportBatchSize(options.BatchSize)
	if err != nil {
		return SQLRowBinaryImportResult{}, err
	}
	reader, err := hatSql.NewSQLRowBinaryStreamReader(input)
	if err != nil {
		return SQLRowBinaryImportResult{}, err
	}
	defer reader.Close()
	columns := reader.Columns()
	if len(columns) != len(targetColumns) {
		return SQLRowBinaryImportResult{}, fmt.Errorf("SQL RowBinary import received %d columns for %d target columns", len(columns), len(targetColumns))
	}

	result := SQLRowBinaryImportResult{}
	requests := make([]CacheCommandRequest, 0, batchSize)
	rowsRead := 0
	applyBatch := func() error {
		if len(requests) == 0 {
			return nil
		}
		response := trie.ExecuteCommand(CacheCommandRequest{Command: "BATCH", Atomic: true, Batch: requests})
		if !response.OK {
			return fmt.Errorf("SQL RowBinary import batch %d failed: %s", result.Batches+1, response.Message)
		}
		result.Affected += len(requests)
		result.Batches++
		result.Response = sqlRowBinaryImportResponseSummary(response)
		requests = requests[:0]
		return nil
	}
	for reader.Next() {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		rowsRead++
		request, err := sqlRowBinaryImportRequest(targetColumns, columns, reader.Row())
		if err != nil {
			return result, fmt.Errorf("SQL RowBinary import row %d: %w", rowsRead, err)
		}
		requests = append(requests, request)
		if len(requests) == batchSize {
			if err := applyBatch(); err != nil {
				return result, err
			}
		}
	}
	if err := reader.Err(); err != nil {
		return result, err
	}
	if err := applyBatch(); err != nil {
		return result, err
	}
	if result.Batches == 0 {
		result.Response = CacheCommandResponse{OK: true, Message: "no rows imported"}
	}
	return result, nil
}

func sqlRowBinaryImportResponseSummary(response CacheCommandResponse) CacheCommandResponse {
	return CacheCommandResponse{OK: response.OK, Message: response.Message, Value: response.Value, Code: response.Code}
}

func normalizeSQLRowBinaryImportBatchSize(value int) (int, error) {
	if value == 0 {
		return maxPublicCommandBatchSize, nil
	}
	if value < 1 || value > maxPublicCommandBatchSize {
		return 0, fmt.Errorf("SQL RowBinary import batch size must be between 1 and %d", maxPublicCommandBatchSize)
	}
	return value, nil
}

func parseSQLRowBinaryInsertTarget(source string) ([]string, error) {
	trimmed := strings.TrimSpace(source)
	if trimmed == "" {
		return nil, fmt.Errorf("SQL RowBinary import requires an INSERT target")
	}
	insert, ok, err := parseSQLInsertSelect(trimmed + " FROM ROWBINARY")
	if err != nil {
		return nil, err
	}
	if !ok || !strings.EqualFold(strings.TrimSpace(insert.query), "FROM ROWBINARY") {
		return nil, fmt.Errorf("SQL RowBinary import target must be INSERT INTO CACHE(...) without VALUES or SELECT")
	}
	return insert.columns, nil
}

func sqlRowBinaryImportRequest(targetColumns []string, columns []hatSql.SQLRowBinaryColumn, row hatSql.SQLRow) (CacheCommandRequest, error) {
	fields := make(map[string]sqlValue, len(targetColumns))
	for index, target := range targetColumns {
		column := columns[index]
		value, exists := row[column.Name]
		if !exists {
			return CacheCommandRequest{}, fmt.Errorf("selected column %q is missing", column.Name)
		}
		converted, err := sqlRowBinaryImportValue(value, column)
		if err != nil {
			return CacheCommandRequest{}, fmt.Errorf("column %q: %w", target, err)
		}
		fields[target] = converted
	}
	return compileSQLInsert(fields)
}

func sqlRowBinaryImportValue(value interface{}, column hatSql.SQLRowBinaryColumn) (sqlValue, error) {
	if value == nil {
		return sqlValue{json: true}, nil
	}
	switch typed := value.(type) {
	case string:
		return sqlValue{text: typed}, nil
	case []byte:
		return sqlValue{text: string(typed)}, nil
	case json.RawMessage:
		var decoded interface{}
		if err := json.Unmarshal(typed, &decoded); err != nil {
			return sqlValue{}, fmt.Errorf("invalid JSON value: %w", err)
		}
		return sqlRowBinaryImportValue(decoded, column)
	case bool:
		return sqlValue{text: strconv.FormatBool(typed)}, nil
	case int:
		return sqlValue{text: strconv.Itoa(typed)}, nil
	case int8:
		return sqlValue{text: strconv.FormatInt(int64(typed), 10)}, nil
	case int16:
		return sqlValue{text: strconv.FormatInt(int64(typed), 10)}, nil
	case int32:
		return sqlValue{text: strconv.FormatInt(int64(typed), 10)}, nil
	case int64:
		return sqlValue{text: strconv.FormatInt(typed, 10)}, nil
	case uint:
		return sqlValue{text: strconv.FormatUint(uint64(typed), 10)}, nil
	case uint8:
		return sqlValue{text: strconv.FormatUint(uint64(typed), 10)}, nil
	case uint16:
		return sqlValue{text: strconv.FormatUint(uint64(typed), 10)}, nil
	case uint32:
		return sqlValue{text: strconv.FormatUint(uint64(typed), 10)}, nil
	case uint64:
		return sqlValue{text: strconv.FormatUint(typed, 10)}, nil
	case float32:
		if math.IsNaN(float64(typed)) || math.IsInf(float64(typed), 0) {
			return sqlValue{}, fmt.Errorf("must be a finite scalar")
		}
		return sqlValue{text: strconv.FormatFloat(float64(typed), 'f', -1, 32)}, nil
	case float64:
		if math.IsNaN(typed) || math.IsInf(typed, 0) {
			return sqlValue{}, fmt.Errorf("must be a finite scalar")
		}
		return sqlValue{text: strconv.FormatFloat(typed, 'f', -1, 64)}, nil
	case json.Number:
		return sqlValue{text: typed.String()}, nil
	case time.Time:
		if column.Type == hatSql.SQLRowBinaryDate {
			return sqlValue{text: typed.UTC().Format("2006-01-02")}, nil
		}
		return sqlValue{text: typed.UTC().Format(time.RFC3339Nano)}, nil
	case time.Duration:
		return sqlValue{text: typed.String()}, nil
	case hatSql.SQLDate:
		return sqlValue{text: string(typed)}, nil
	case hatSql.SQLDecimal:
		return sqlValue{text: string(typed)}, nil
	case hatSql.SQLDecimal128:
		formatted, err := typed.Format(column.DecimalScale)
		if err != nil {
			return sqlValue{}, err
		}
		return sqlValue{text: formatted}, nil
	case hatSql.SQLDecimal256:
		formatted, err := typed.Format(column.DecimalScale)
		if err != nil {
			return sqlValue{}, err
		}
		return sqlValue{text: formatted}, nil
	case hatSql.SQLUUID:
		return sqlValue{text: string(typed)}, nil
	case hatSql.SQLDuration:
		return sqlValue{text: string(typed)}, nil
	case hatSql.SQLIPv4:
		return sqlValue{text: typed.String()}, nil
	case hatSql.SQLIPv6:
		return sqlValue{text: typed.String()}, nil
	case hatSql.SQLEnum8:
		if label, ok := typed.Label(column.EnumValues); ok {
			return sqlValue{text: label}, nil
		}
		return sqlValue{text: strconv.FormatUint(uint64(typed), 10)}, nil
	case hatSql.SQLEnum16:
		if label, ok := typed.Label(column.EnumValues); ok {
			return sqlValue{text: label}, nil
		}
		return sqlValue{text: strconv.FormatUint(uint64(typed), 10)}, nil
	case [16]byte:
		var encoded [36]byte
		hex.Encode(encoded[0:8], typed[0:4])
		encoded[8] = '-'
		hex.Encode(encoded[9:13], typed[4:6])
		encoded[13] = '-'
		hex.Encode(encoded[14:18], typed[6:8])
		encoded[18] = '-'
		hex.Encode(encoded[19:23], typed[8:10])
		encoded[23] = '-'
		hex.Encode(encoded[24:36], typed[10:16])
		return sqlValue{text: string(encoded[:])}, nil
	default:
		return sqlValue{}, fmt.Errorf("must be a scalar, got %T", value)
	}
}
