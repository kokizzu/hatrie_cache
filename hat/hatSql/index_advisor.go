package hatSql

import (
	"sort"
	"strings"
	"sync"
	"time"
)

// SQLIndexRecommendation is one opt-in recommendation derived from an
// observed slow scan. It retains no SQL text, predicate literal, or row data.
type SQLIndexRecommendation struct {
	Key         string
	Field       string
	SlowQueries uint64
}

// SQLCoveringIndexRecommendation is an opt-in recommendation for an equality
// index that can retain the selected columns without materializing source rows.
// Columns excludes Field, which is always retained by CreateSQLJSONCoveringIndex.
type SQLCoveringIndexRecommendation struct {
	Key         string
	Field       string
	Columns     []string
	SlowQueries uint64
}

// SQLPrimaryOrderRecommendation is a deterministic primary-field ordering
// suggestion derived from the advisor's observed slow scans. Fields are
// ordered by descending frequency, then by name; it contains no query text or
// predicate values.
type SQLPrimaryOrderRecommendation struct {
	Key    string
	Fields []string
}

// SQLIndexAdvisor records bounded candidate fields from observed slow scans.
// It is intended for trusted server-side use, not external telemetry export.
type SQLIndexAdvisor struct {
	mu             sync.RWMutex
	capacity       int
	counts         map[sqlIndexAdvisorKey]uint64
	coveringCounts map[sqlCoveringAdvisorKey]uint64
}

type sqlIndexAdvisorKey struct {
	key   string
	field string
}

type sqlCoveringAdvisorKey struct {
	key     string
	field   string
	columns string
}

func NewSQLIndexAdvisor(capacity int) *SQLIndexAdvisor {
	return &SQLIndexAdvisor{
		capacity: capacity,
		counts:   make(map[sqlIndexAdvisorKey]uint64),
	}
}

func (advisor *SQLIndexAdvisor) Recommendations() []SQLIndexRecommendation {
	if advisor == nil {
		return nil
	}
	advisor.mu.RLock()
	recommendations := make([]SQLIndexRecommendation, 0, len(advisor.counts))
	for key, count := range advisor.counts {
		recommendations = append(recommendations, SQLIndexRecommendation{Key: key.key, Field: key.field, SlowQueries: count})
	}
	advisor.mu.RUnlock()
	sort.Slice(recommendations, func(left, right int) bool {
		if recommendations[left].SlowQueries != recommendations[right].SlowQueries {
			return recommendations[left].SlowQueries > recommendations[right].SlowQueries
		}
		if recommendations[left].Key != recommendations[right].Key {
			return recommendations[left].Key < recommendations[right].Key
		}
		return recommendations[left].Field < recommendations[right].Field
	})
	return recommendations
}

// PrimaryOrderRecommendations groups the advisor's bounded field
// observations by source and returns a candidate order for an ordered primary
// layout. The method is advisory only: it never changes an index or query
// plan, and callers can use the result when creating a new layout.
func (advisor *SQLIndexAdvisor) PrimaryOrderRecommendations() []SQLPrimaryOrderRecommendation {
	if advisor == nil {
		return nil
	}
	type fieldCount struct {
		field string
		count uint64
	}
	advisor.mu.RLock()
	byKey := make(map[string][]fieldCount)
	for key, count := range advisor.counts {
		byKey[key.key] = append(byKey[key.key], fieldCount{field: key.field, count: count})
	}
	recommendations := make([]SQLPrimaryOrderRecommendation, 0, len(byKey))
	for key, fields := range byKey {
		sort.Slice(fields, func(left, right int) bool {
			if fields[left].count != fields[right].count {
				return fields[left].count > fields[right].count
			}
			return fields[left].field < fields[right].field
		})
		ordered := make([]string, len(fields))
		for index, field := range fields {
			ordered[index] = field.field
		}
		recommendations = append(recommendations, SQLPrimaryOrderRecommendation{Key: key, Fields: ordered})
	}
	advisor.mu.RUnlock()
	sort.Slice(recommendations, func(left, right int) bool {
		return recommendations[left].Key < recommendations[right].Key
	})
	return recommendations
}

// CoveringRecommendations returns stable, bounded recommendations for simple
// equality projections that could be served by a covering index. The returned
// columns are copied and sorted so callers can pass them directly to
// CreateSQLJSONCoveringIndex.
func (advisor *SQLIndexAdvisor) CoveringRecommendations() []SQLCoveringIndexRecommendation {
	if advisor == nil {
		return nil
	}
	advisor.mu.RLock()
	recommendations := make([]SQLCoveringIndexRecommendation, 0, len(advisor.coveringCounts))
	for key, count := range advisor.coveringCounts {
		recommendations = append(recommendations, SQLCoveringIndexRecommendation{
			Key:         key.key,
			Field:       key.field,
			Columns:     strings.Split(key.columns, "\x00"),
			SlowQueries: count,
		})
	}
	advisor.mu.RUnlock()
	sort.Slice(recommendations, func(left, right int) bool {
		if recommendations[left].SlowQueries != recommendations[right].SlowQueries {
			return recommendations[left].SlowQueries > recommendations[right].SlowQueries
		}
		if recommendations[left].Key != recommendations[right].Key {
			return recommendations[left].Key < recommendations[right].Key
		}
		if recommendations[left].Field != recommendations[right].Field {
			return recommendations[left].Field < recommendations[right].Field
		}
		return strings.Join(recommendations[left].Columns, "\x00") < strings.Join(recommendations[right].Columns, "\x00")
	})
	return recommendations
}

func (advisor *SQLIndexAdvisor) observeSlowQuery(query *sqlQuery, metrics *sqlExecutionMetrics, elapsed time.Duration, threshold time.Duration, err error) {
	if advisor == nil || advisor.capacity <= 0 || err != nil || threshold <= 0 || elapsed < threshold || query == nil || query.from == nil || query.from.kind != "CACHE" || len(query.joins) != 0 {
		return
	}
	if metrics != nil {
		for _, step := range metrics.steps {
			if strings.Contains(step.Node, "INDEX") {
				return
			}
		}
	}
	fields := sqlIndexAdvisorPredicateFields(query.where, query.from.alias)
	coveringField, coveringColumns, covering := sqlIndexAdvisorCoveringProjection(query)
	if len(fields) == 0 && !covering {
		return
	}
	advisor.mu.Lock()
	defer advisor.mu.Unlock()
	for _, field := range fields {
		if advisor.counts == nil {
			advisor.counts = make(map[sqlIndexAdvisorKey]uint64)
		}
		key := sqlIndexAdvisorKey{key: query.from.key, field: field}
		if _, exists := advisor.counts[key]; !exists && len(advisor.counts) >= advisor.capacity {
			continue
		}
		advisor.counts[key]++
	}
	if covering {
		if advisor.coveringCounts == nil {
			advisor.coveringCounts = make(map[sqlCoveringAdvisorKey]uint64)
		}
		key := sqlCoveringAdvisorKey{key: query.from.key, field: coveringField, columns: strings.Join(coveringColumns, "\x00")}
		if _, exists := advisor.coveringCounts[key]; exists || len(advisor.coveringCounts) < advisor.capacity {
			advisor.coveringCounts[key]++
		}
	}
}

func sqlIndexAdvisorCoveringProjection(query *sqlQuery) (string, []string, bool) {
	if query == nil || query.from == nil {
		return "", nil, false
	}
	fields := sqlCoveringProjectionFields(query)
	if len(fields) < 2 {
		return "", nil, false
	}
	predicateField, _, ok := sqlCoveringIndexedEquality(*query.from, query.where)
	if !ok {
		return "", nil, false
	}
	columns := fields[:0]
	for _, field := range fields {
		if field != predicateField {
			columns = append(columns, field)
		}
	}
	if len(columns) == 0 {
		return "", nil, false
	}
	sort.Strings(columns)
	return predicateField, columns, true
}

func sqlIndexAdvisorPredicateFields(expr sqlExpr, alias string) []string {
	seen := map[string]struct{}{}
	var collect func(sqlExpr)
	collect = func(current sqlExpr) {
		if current.kind == "binary" && current.op == "AND" && current.left != nil && current.right != nil {
			collect(*current.left)
			collect(*current.right)
			return
		}
		if current.kind != "binary" || current.left == nil || current.right == nil {
			return
		}
		left, right := *current.left, *current.right
		if left.kind == "field" && (left.qualifier == "" || left.qualifier == alias) && right.kind == "literal" && sqlColumnarNumericOperator(current.op) {
			seen[left.name] = struct{}{}
		}
		if right.kind == "field" && (right.qualifier == "" || right.qualifier == alias) && left.kind == "literal" && sqlColumnarNumericOperator(current.op) {
			seen[right.name] = struct{}{}
		}
	}
	collect(expr)
	fields := make([]string, 0, len(seen))
	for field := range seen {
		fields = append(fields, field)
	}
	sort.Strings(fields)
	return fields
}
