// Package hatSql provides the portable SQL wire model and HTTP client for a
// hatrie_cache SQL endpoint.
package hatSql

// QueryRequest is accepted by the monitoring SQL endpoint. A non-empty
// MutationID opts into one journal-backed mutation; requests without it remain
// read-only.
type QueryRequest struct {
	Query      string        `json:"query"`
	Parameters []interface{} `json:"parameters,omitempty"`
	MutationID string        `json:"mutation_id,omitempty"`
	PageSize   int           `json:"page_size,omitempty"`
	Cursor     string        `json:"cursor,omitempty"`
	Keyset     bool          `json:"keyset,omitempty"`
	Stream     bool          `json:"stream,omitempty"`
}

// Row is one dynamically shaped row returned by the SQL engine.
type Row map[string]interface{}

// QueryResult is a materialized SQL response. Streaming clients use QueryRows.
type QueryResult struct {
	QueryID        string                  `json:"query_id,omitempty"`
	Columns        []string                `json:"columns"`
	Rows           []Row                   `json:"rows"`
	Plan           []ExplainStep           `json:"plan,omitempty"`
	OptimizerTrace *SQLQueryOptimizerTrace `json:"optimizer_trace,omitempty"`
	PlanSnapshot   *SQLPlanSnapshot        `json:"plan_snapshot,omitempty"`
	Stats          *QueryStats             `json:"stats,omitempty"`
	HasMore        bool                    `json:"has_more,omitempty"`
	NextCursor     string                  `json:"next_cursor,omitempty"`
}

// ExplainStep is one stable operation in an EXPLAIN plan.
type ExplainStep struct {
	Node                 string                    `json:"node"`
	Detail               string                    `json:"detail"`
	Alternatives         []ExplainAlternative      `json:"alternatives,omitempty"`
	Notices              []ExplainNotice           `json:"notices,omitempty"`
	Index                *SQLIndexDiagnostics      `json:"index,omitempty"`
	Pruning              *ExplainPruning           `json:"pruning,omitempty"`
	Projection           *SQLProjectionDiagnostics `json:"projection,omitempty"`
	Arrangements         []SQLArrangementMetadata  `json:"arrangements,omitempty"`
	Stage                int                       `json:"stage,omitempty"`
	Worker               int                       `json:"worker,omitempty"`
	Workers              int                       `json:"workers,omitempty"`
	Lineage              []ColumnLineage           `json:"lineage,omitempty"`
	EstimatedRows        *int                      `json:"estimated_rows,omitempty"`
	EstimatedCost        *int                      `json:"estimated_cost,omitempty"`
	EstimatedMemoryBytes *int                      `json:"estimated_memory_bytes,omitempty"`
	ActualInputRows      *int                      `json:"actual_input_rows,omitempty"`
	ActualOutputRows     *int                      `json:"actual_output_rows,omitempty"`
	ActualInputBytes     *int                      `json:"actual_input_bytes,omitempty"`
	ActualOutputBytes    *int                      `json:"actual_output_bytes,omitempty"`
	EstimateErrorRows    *int                      `json:"estimate_error_rows,omitempty"`
	EstimateErrorPercent *float64                  `json:"estimate_error_percent,omitempty"`
	ElapsedNanos         *int64                    `json:"elapsed_ns,omitempty"`
}

// ExplainPruning contains machine-readable row counts for an index, mark, or
// segment pruning decision observed by EXPLAIN ANALYZE. It is omitted for
// operators that do not perform pruning.
type ExplainPruning struct {
	TotalRows                 int     `json:"total_rows"`
	SkippedRows               int     `json:"skipped_rows"`
	ScannedRows               int     `json:"scanned_rows"`
	MatchedRows               int     `json:"matched_rows"`
	ResidualRows              int     `json:"residual_rows"`
	ResidualFalsePositiveRate float64 `json:"residual_false_positive_rate"`
}

// ExplainAlternative describes an optimizer strategy considered for one plan
// step. RejectedReason is empty for the selected alternative.
type ExplainAlternative struct {
	Expression     string `json:"expression"`
	EstimatedRows  int    `json:"estimated_rows"`
	EstimatedCost  int    `json:"estimated_cost"`
	Selected       bool   `json:"selected"`
	RejectedReason string `json:"rejected_reason,omitempty"`
}

// SQLQueryOptimizerTraceFormat identifies the stable optimizer-trace wire
// shape returned when SQLQueryOptions.OptimizerTrace is enabled.
const SQLQueryOptimizerTraceFormat = "hatrie-cache-sql-optimizer-trace/v1"

// SQLQueryOptimizerTrace records ordered optimizer rule activity for one
// query. It is opt-in because trace events are diagnostic output, not part of
// the normal execution contract.
type SQLQueryOptimizerTrace struct {
	Format string                        `json:"format"`
	Events []SQLQueryOptimizerTraceEvent `json:"events"`

	rule    int
	pending []SQLQueryOptimizerTraceEvent
}

// SQLQueryOptimizerTraceEvent is one applied rule or rejected alternative.
// Rule is one-based and follows the order supplied to NewSQLQueryOptimizer.
type SQLQueryOptimizerTraceEvent struct {
	Rule       int    `json:"rule"`
	Kind       string `json:"kind"`
	Action     string `json:"action"`
	Expression string `json:"expression,omitempty"`
	Reason     string `json:"reason,omitempty"`
	Changed    bool   `json:"changed,omitempty"`
}

// ExplainNotice is a stable, machine-readable optimizer diagnostic attached
// to an explain step.
type ExplainNotice struct {
	Code   string `json:"code"`
	Detail string `json:"detail"`
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
