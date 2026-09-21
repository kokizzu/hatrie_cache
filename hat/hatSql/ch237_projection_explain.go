package hatSql

import (
	"fmt"
	"sort"
	"strings"
)

// SQLSourceSizeResolver optionally supplies a cheap source-size estimate for
// EXPLAIN projection diagnostics. Implementations should return the current
// logical row count and encoded row bytes without materializing the source.
// available=false keeps the projection choice explainable while omitting the
// source-byte comparison.
type SQLSourceSizeResolver interface {
	SQLSourceSize(name, key string) (rows int, bytes int64, available bool, err error)
}

// SQLProjectionDiagnostics describes one exact materialized-projection choice
// without retaining source rows or query text. Byte fields are logical encoded
// row estimates, not process-RSS measurements.
type SQLProjectionDiagnostics struct {
	Name                string `json:"name,omitempty"`
	Selected            bool   `json:"selected"`
	Reason              string `json:"reason,omitempty"`
	SourceRows          int    `json:"source_rows,omitempty"`
	SourceBytes         int64  `json:"source_bytes,omitempty"`
	ProjectionRows      int    `json:"projection_rows,omitempty"`
	ProjectionBytes     int64  `json:"projection_bytes,omitempty"`
	EstimatedReadBytes  int64  `json:"estimated_read_bytes,omitempty"`
	EstimatedSavedBytes int64  `json:"estimated_saved_bytes,omitempty"`
}

type sqlProjectionExplainCandidate struct {
	name            string
	dependencies    []string
	sourceVersions  map[string]string
	projectionRows  int
	projectionBytes int64
}

func (views *MaterializedViews) explainProjection(query string, resolver SourceResolver, options QueryOptions) (SQLProjectionDiagnostics, bool) {
	if views == nil || resolver == nil || strings.TrimSpace(query) == "" {
		return SQLProjectionDiagnostics{}, false
	}
	if options.IndexHint.Mode != "" {
		return SQLProjectionDiagnostics{Selected: false, Reason: "index hint disables projection selection"}, true
	}
	versions, versioned := resolver.(SourceVersionResolver)
	if !versioned {
		return SQLProjectionDiagnostics{Selected: false, Reason: "source versions unavailable"}, true
	}
	query = strings.TrimSpace(query)
	requestedCollation := normalizedMaterializedViewCollation(options.Collation)
	views.mu.RLock()
	candidates := make([]sqlProjectionExplainCandidate, 0, len(views.views))
	for name, view := range views.views {
		if view.definition.Query != query || view.collation != requestedCollation || len(view.sourceVersions) != len(view.definition.Dependencies) {
			continue
		}
		projectionBytes := view.storedBytes
		if projectionBytes <= 0 {
			projectionBytes = sqlProjectionEstimatedRowsBytes(view.snapshot.Result.Rows)
		}
		candidates = append(candidates, sqlProjectionExplainCandidate{
			name:            name,
			dependencies:    append([]string(nil), view.definition.Dependencies...),
			sourceVersions:  cloneSQLProjectionVersions(view.sourceVersions),
			projectionRows:  len(view.snapshot.Result.Rows),
			projectionBytes: projectionBytes,
		})
	}
	views.mu.RUnlock()
	sort.Slice(candidates, func(left, right int) bool { return candidates[left].name < candidates[right].name })
	if len(candidates) == 0 {
		return SQLProjectionDiagnostics{Selected: false, Reason: "no exact projection candidate"}, true
	}

	var rejected SQLProjectionDiagnostics
	for _, candidate := range candidates {
		diagnostics := SQLProjectionDiagnostics{
			Name:            candidate.name,
			ProjectionRows:  candidate.projectionRows,
			ProjectionBytes: candidate.projectionBytes,
		}
		sqlProjectionExplainSourceSize(&diagnostics, resolver, candidate.dependencies)
		fresh := true
		for _, dependency := range candidate.dependencies {
			version, available, err := versions.SQLSourceVersion("CACHE", dependency)
			if err != nil || !available || version == "" || version != candidate.sourceVersions[dependency] {
				fresh = false
				break
			}
		}
		if !fresh {
			diagnostics.Reason = "projection source version is stale"
			if rejected.Name == "" {
				rejected = diagnostics
			}
			continue
		}
		diagnostics.Selected = true
		diagnostics.Reason = "fresh exact projection"
		diagnostics.EstimatedReadBytes = diagnostics.ProjectionBytes
		if diagnostics.SourceBytes > diagnostics.ProjectionBytes {
			diagnostics.EstimatedSavedBytes = diagnostics.SourceBytes - diagnostics.ProjectionBytes
		}
		return diagnostics, true
	}
	if rejected.Name != "" {
		return rejected, true
	}
	return SQLProjectionDiagnostics{Selected: false, Reason: "no fresh projection candidate"}, true
}

func cloneSQLProjectionVersions(source map[string]string) map[string]string {
	if len(source) == 0 {
		return nil
	}
	copyValues := make(map[string]string, len(source))
	for key, value := range source {
		copyValues[key] = value
	}
	return copyValues
}

// sqlProjectionEstimatedRowsBytes is deliberately allocation-free. The row
// snapshot is immutable while the registry read lock is held, and EXPLAIN only
// needs an order-of-magnitude I/O estimate when exact storage accounting is
// unavailable.
func sqlProjectionEstimatedRowsBytes(rows []SQLRow) int64 {
	var total int64
	for _, row := range rows {
		rowBytes := int64(2)
		first := true
		for key, value := range row {
			if !first {
				rowBytes = saturatingInt64Add(rowBytes, 1)
			}
			first = false
			rowBytes = saturatingInt64Add(rowBytes, int64(len(key)+3))
			rowBytes = saturatingInt64Add(rowBytes, sqlProjectionEstimatedValueBytes(value))
		}
		total = saturatingInt64Add(total, rowBytes)
	}
	return total
}

func sqlProjectionEstimatedValueBytes(value interface{}) int64 {
	switch value := value.(type) {
	case nil:
		return 4
	case string:
		return int64(len(value) + 2)
	case []byte:
		return int64(((len(value)+2)/3)*4 + 2)
	case bool:
		if value {
			return 4
		}
		return 5
	case int, int8, int16, int32, int64:
		return 20
	case uint, uint8, uint16, uint32, uint64, uintptr:
		return 20
	case float32:
		return 16
	case float64:
		return 24
	default:
		return 16
	}
}

func sqlExplainHasProjectionDiagnostics(steps []ExplainStep) bool {
	for _, step := range steps {
		if step.Projection != nil {
			return true
		}
	}
	return false
}

func sqlProjectionExplainSourceSize(diagnostics *SQLProjectionDiagnostics, resolver SourceResolver, dependencies []string) {
	sizer, ok := resolver.(SQLSourceSizeResolver)
	if !ok || diagnostics == nil {
		return
	}
	for _, dependency := range dependencies {
		rows, bytes, available, err := sizer.SQLSourceSize("CACHE", dependency)
		if err != nil || !available || rows < 0 || bytes < 0 {
			return
		}
		diagnostics.SourceRows = saturatingIntAdd(diagnostics.SourceRows, rows)
		diagnostics.SourceBytes = saturatingInt64Add(diagnostics.SourceBytes, bytes)
	}
}

func saturatingIntAdd(left, right int) int {
	if right <= 0 {
		return left
	}
	maxInt := int(^uint(0) >> 1)
	if left > maxInt-right {
		return maxInt
	}
	return left + right
}

func saturatingInt64Add(left, right int64) int64 {
	if right <= 0 {
		return left
	}
	maxInt64 := int64(^uint64(0) >> 1)
	if left > maxInt64-right {
		return maxInt64
	}
	return left + right
}

func explainProjectionSource(source string) string {
	trimmed := strings.TrimSpace(source)
	upper := strings.ToUpper(trimmed)
	for _, prefix := range []string{"EXPLAIN ANALYZE", "EXPLAIN PIPELINE", "EXPLAIN COST", "EXPLAIN"} {
		if strings.HasPrefix(upper, prefix+" ") {
			return strings.TrimSpace(trimmed[len(prefix):])
		}
	}
	if strings.EqualFold(trimmed, "EXPLAIN") {
		return ""
	}
	return trimmed
}

func projectionExplainDetail(diagnostics SQLProjectionDiagnostics) string {
	if diagnostics.Name == "" {
		return diagnostics.Reason
	}
	return fmt.Sprintf("%s: %s", diagnostics.Name, diagnostics.Reason)
}
