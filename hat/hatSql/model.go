// Package hatSql provides the portable SQL wire model and HTTP client for a
// hatrie_cache SQL endpoint.
package hatSql

// QueryRequest is accepted by the read-only monitoring SQL endpoint.
type QueryRequest struct {
	Query      string        `json:"query"`
	Parameters []interface{} `json:"parameters,omitempty"`
	PageSize   int           `json:"page_size,omitempty"`
	Cursor     string        `json:"cursor,omitempty"`
	Keyset     bool          `json:"keyset,omitempty"`
	Stream     bool          `json:"stream,omitempty"`
}

// Row is one dynamically shaped row returned by the SQL engine.
type Row map[string]interface{}

// QueryResult is a materialized SQL response. Streaming clients use QueryRows.
type QueryResult struct {
	QueryID    string        `json:"query_id,omitempty"`
	Columns    []string      `json:"columns"`
	Rows       []Row         `json:"rows"`
	Plan       []ExplainStep `json:"plan,omitempty"`
	Stats      *QueryStats   `json:"stats,omitempty"`
	HasMore    bool          `json:"has_more,omitempty"`
	NextCursor string        `json:"next_cursor,omitempty"`
}

// ExplainStep is one stable operation in an EXPLAIN plan.
type ExplainStep struct {
	Node                 string          `json:"node"`
	Detail               string          `json:"detail"`
	Stage                int             `json:"stage,omitempty"`
	Worker               int             `json:"worker,omitempty"`
	Workers              int             `json:"workers,omitempty"`
	Lineage              []ColumnLineage `json:"lineage,omitempty"`
	EstimatedRows        *int            `json:"estimated_rows,omitempty"`
	ActualInputRows      *int            `json:"actual_input_rows,omitempty"`
	ActualOutputRows     *int            `json:"actual_output_rows,omitempty"`
	ActualInputBytes     *int            `json:"actual_input_bytes,omitempty"`
	ActualOutputBytes    *int            `json:"actual_output_bytes,omitempty"`
	EstimateErrorRows    *int            `json:"estimate_error_rows,omitempty"`
	EstimateErrorPercent *float64        `json:"estimate_error_percent,omitempty"`
	ElapsedNanos         *int64          `json:"elapsed_ns,omitempty"`
}

// ColumnLineage identifies the source fields contributing to one projected
// output column. Derived expressions can list more than one source field.
type ColumnLineage struct {
	Output       string   `json:"output"`
	SourceFields []string `json:"source_fields"`
}

// QueryStats describes the measured execution emitted by EXPLAIN ANALYZE.
type QueryStats struct {
	ElapsedNanos  int64 `json:"elapsed_ns"`
	OutputRows    int   `json:"output_rows"`
	OutputColumns int   `json:"output_columns"`
	ResultBytes   int   `json:"result_bytes"`
	PlanSteps     int   `json:"plan_steps"`
}
