package hatSql

import "context"

// QueryObserver receives one privacy-safe execution summary per query.
type QueryObserver interface {
	ObserveSQLQuery(QueryEvent)
}

// QueryObserverFunc adapts a function into QueryObserver.
type QueryObserverFunc func(QueryEvent)

func (fn QueryObserverFunc) ObserveSQLQuery(event QueryEvent) {
	if fn != nil {
		fn(event)
	}
}

// QueryEvent is an execution summary suitable for a structured log or metric.
// It deliberately excludes SQL text, cache keys, predicates, and row values.
type QueryEvent struct {
	QueryID            string          `json:"query_id"`
	ElapsedNanos       int64           `json:"elapsed_ns"`
	OutputRows         int             `json:"output_rows"`
	OutputColumns      int             `json:"output_columns"`
	ResultBytes        int             `json:"result_bytes"`
	OK                 bool            `json:"ok"`
	Slow               bool            `json:"slow"`
	Canceled           bool            `json:"canceled,omitempty"`
	CancellationReason string          `json:"cancellation_reason,omitempty"`
	Error              string          `json:"error,omitempty"`
	Operators          []QueryOperator `json:"operators,omitempty"`
}

// QueryOperator is a privacy-safe execution counter.
type QueryOperator struct {
	Node                 string   `json:"node"`
	InputRows            int      `json:"input_rows"`
	OutputRows           int      `json:"output_rows"`
	InputBytes           *int     `json:"input_bytes,omitempty"`
	OutputBytes          *int     `json:"output_bytes,omitempty"`
	ElapsedNanos         int64    `json:"elapsed_ns"`
	EstimatedRows        *int     `json:"estimated_rows,omitempty"`
	EstimateErrorPercent *float64 `json:"estimate_error_percent,omitempty"`
}

// SourceResolver supplies relational source rows. Nil rows are an empty source.
type SourceResolver interface {
	ResolveSQLSource(name string, key string) ([]Row, error)
}

// SourceResolverFunc adapts a function into SourceResolver.
type SourceResolverFunc func(name string, key string) ([]Row, error)

func (fn SourceResolverFunc) ResolveSQLSource(name string, key string) ([]Row, error) {
	if fn == nil {
		return nil, nil
	}
	return fn(name, key)
}

// StreamSourceResolver supplies source rows one at a time for stream-compatible queries.
type StreamSourceResolver interface {
	StreamSQLSource(ctx context.Context, name string, key string, visit func(Row) error) error
}

// SnapshotLocker optionally coordinates a consistent source snapshot for a query.
type SnapshotLocker interface{ LockSQLSnapshot() func() }

// IndexedSourceResolver optionally resolves equality predicates through an index.
type IndexedSourceResolver interface {
	ResolveSQLIndexedSource(name, key, field string, value interface{}) ([]Row, bool, error)
}

// RangeIndexedSourceResolver optionally resolves ordered comparisons through an index.
type RangeIndexedSourceResolver interface {
	ResolveSQLIndexedRangeSource(name, key, field, operator string, value interface{}) ([]Row, bool, error)
}

// OrderedSourceResolver optionally reads one source field in SQL ORDER BY order.
type OrderedSourceResolver interface {
	ResolveSQLOrderedSource(name, key, field string, desc, nullsFirst, nullsLast bool) ([]Row, bool, error)
}

// OrderedStreamSourceResolver is the streaming counterpart of OrderedSourceResolver.
type OrderedStreamSourceResolver interface {
	StreamSQLOrderedSource(ctx context.Context, name, key, field string, desc, nullsFirst, nullsLast bool, visit func(Row) error) (bool, error)
}

// CompositeIndexedSourceResolver optionally resolves multi-field equality predicates.
type CompositeIndexedSourceResolver interface {
	ResolveSQLCompositeIndexedSource(name, key string, fields []string, values []interface{}) ([]Row, bool, error)
}

// JSONIndexFrequencyBucket reports a posting-list frequency without exposing values.
type JSONIndexFrequencyBucket struct {
	RowsPerKey   int `json:"rows_per_key"`
	DistinctKeys int `json:"distinct_keys"`
}

// JSONIndexStats describes one materialized JSON index.
type JSONIndexStats struct {
	Key                string
	Fields             []string
	Rows               int
	DistinctKeys       int
	MinRowsPerKey      int
	MaxRowsPerKey      int
	AverageRowsPerKey  float64
	FrequencyHistogram []JSONIndexFrequencyBucket
}
