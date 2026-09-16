package hatSql

// SQLIndexDiagnostics describes the work performed by one source index for a
// single equality probe. Candidate rows are returned by the index before the
// complete predicate is evaluated; residual exact-check work is reported in
// ExplainPruning on the corresponding plan step.
type SQLIndexDiagnostics struct {
	Kind              string `json:"kind"`
	Field             string `json:"field"`
	IndexBytes        int    `json:"index_bytes"`
	TotalRows         int    `json:"total_rows"`
	CandidateRows     int    `json:"candidate_rows"`
	SkippedRows       int    `json:"skipped_rows"`
	Segments          int    `json:"segments"`
	CandidateSegments int    `json:"candidate_segments"`
	SkippedSegments   int    `json:"skipped_segments"`
}
