package hatSql

import (
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// SQLProjectionRecommendation identifies a caller-owned query label and its
// CACHE dependencies. It deliberately excludes SQL text, literals, and rows.
type SQLProjectionRecommendation struct {
	QueryID      string
	Dependencies []string
	// Fields is the sorted union of source fields referenced by the query.
	Fields []string
	// FilterFields contains fields referenced by WHERE and PREWHERE.
	FilterFields []string
	// GroupByFields contains fields referenced by GROUP BY expressions.
	GroupByFields []string
	// OrderByFields contains fields referenced by ORDER BY expressions.
	OrderByFields  []string
	SlowQueries    uint64
	TotalElapsed   time.Duration
	AverageElapsed time.Duration
}

// SQLProjectionAdvisor records bounded candidates for application-managed
// materialized projections. It is opt-in and only records queries with a
// caller-supplied QueryID, avoiding retention of generated or sensitive SQL.
type SQLProjectionAdvisor struct {
	mu       sync.RWMutex
	capacity int
	counts   map[sqlProjectionAdvisorKey]sqlProjectionAdvisorStats
}

type sqlProjectionAdvisorKey struct {
	queryID      string
	dependencies string
	shape        string
}

type sqlProjectionAdvisorShape struct {
	fields        []string
	filterFields  []string
	groupByFields []string
	orderByFields []string
}

type sqlProjectionAdvisorStats struct {
	slowQueries       uint64
	totalElapsedNanos uint64
}

// NewSQLProjectionAdvisor creates an advisor that retains at most capacity
// distinct query/dependency recommendations. A nonpositive capacity is inert.
func NewSQLProjectionAdvisor(capacity int) *SQLProjectionAdvisor {
	return &SQLProjectionAdvisor{capacity: capacity, counts: make(map[sqlProjectionAdvisorKey]sqlProjectionAdvisorStats)}
}

// Recommendations returns deterministic independent copies ordered by the
// observed slow-query count, then caller query ID and dependency list.
func (advisor *SQLProjectionAdvisor) Recommendations() []SQLProjectionRecommendation {
	return advisor.recommendations(false, 0)
}

// CostRecommendations returns at most limit recommendations ordered by total
// observed slow-query latency. A nonpositive limit returns every candidate.
// This is an explicit selection aid; it never changes query execution or
// creates a materialized view.
func (advisor *SQLProjectionAdvisor) CostRecommendations(limit int) []SQLProjectionRecommendation {
	return advisor.recommendations(true, limit)
}

func (advisor *SQLProjectionAdvisor) recommendations(byCost bool, limit int) []SQLProjectionRecommendation {
	if advisor == nil {
		return nil
	}
	advisor.mu.RLock()
	recommendations := make([]SQLProjectionRecommendation, 0, len(advisor.counts))
	for key, stats := range advisor.counts {
		dependencies := sqlProjectionAdvisorDecodeDependencies(key.dependencies)
		shape := sqlProjectionAdvisorDecodeShape(key.shape)
		totalElapsed := sqlProjectionAdvisorDuration(stats.totalElapsedNanos)
		averageElapsed := time.Duration(0)
		if stats.slowQueries > 0 {
			averageElapsed = sqlProjectionAdvisorDuration(stats.totalElapsedNanos / stats.slowQueries)
		}
		recommendations = append(recommendations, SQLProjectionRecommendation{
			QueryID:        key.queryID,
			Dependencies:   dependencies,
			Fields:         shape.fields,
			FilterFields:   shape.filterFields,
			GroupByFields:  shape.groupByFields,
			OrderByFields:  shape.orderByFields,
			SlowQueries:    stats.slowQueries,
			TotalElapsed:   totalElapsed,
			AverageElapsed: averageElapsed,
		})
	}
	advisor.mu.RUnlock()
	sort.Slice(recommendations, func(left, right int) bool {
		if byCost && recommendations[left].TotalElapsed != recommendations[right].TotalElapsed {
			return recommendations[left].TotalElapsed > recommendations[right].TotalElapsed
		}
		if byCost && recommendations[left].AverageElapsed != recommendations[right].AverageElapsed {
			return recommendations[left].AverageElapsed > recommendations[right].AverageElapsed
		}
		if recommendations[left].SlowQueries != recommendations[right].SlowQueries {
			return recommendations[left].SlowQueries > recommendations[right].SlowQueries
		}
		if recommendations[left].QueryID != recommendations[right].QueryID {
			return recommendations[left].QueryID < recommendations[right].QueryID
		}
		leftDependencies := sqlProjectionAdvisorEncodeDependencies(recommendations[left].Dependencies)
		rightDependencies := sqlProjectionAdvisorEncodeDependencies(recommendations[right].Dependencies)
		if leftDependencies != rightDependencies {
			return leftDependencies < rightDependencies
		}
		return sqlProjectionAdvisorEncodeShape(sqlProjectionAdvisorShape{
			fields:        recommendations[left].Fields,
			filterFields:  recommendations[left].FilterFields,
			groupByFields: recommendations[left].GroupByFields,
			orderByFields: recommendations[left].OrderByFields,
		}) < sqlProjectionAdvisorEncodeShape(sqlProjectionAdvisorShape{
			fields:        recommendations[right].Fields,
			filterFields:  recommendations[right].FilterFields,
			groupByFields: recommendations[right].GroupByFields,
			orderByFields: recommendations[right].OrderByFields,
		})
	})
	if limit > 0 && len(recommendations) > limit {
		recommendations = recommendations[:limit]
	}
	return recommendations
}

func (advisor *SQLProjectionAdvisor) observeSlowQuery(query *sqlQuery, queryID string, metrics *sqlExecutionMetrics, elapsed time.Duration, threshold time.Duration, err error) {
	queryID = strings.TrimSpace(queryID)
	if advisor == nil || advisor.capacity <= 0 || queryID == "" || err != nil || threshold <= 0 || elapsed < threshold {
		return
	}
	if metrics != nil {
		for _, step := range metrics.steps {
			if strings.Contains(step.Node, "INDEX") {
				return
			}
		}
	}
	dependencies, ok := sqlProjectionAdvisorDependencies(query)
	if !ok {
		return
	}
	advisor.recordFeedbackWithShape(queryID, dependencies, sqlProjectionAdvisorQueryShape(query), elapsed)
}

func (advisor *SQLProjectionAdvisor) recordFeedback(queryID string, dependencies []string, elapsed time.Duration) {
	advisor.recordFeedbackWithShape(queryID, dependencies, sqlProjectionAdvisorShape{}, elapsed)
}

func (advisor *SQLProjectionAdvisor) recordFeedbackWithShape(queryID string, dependencies []string, shape sqlProjectionAdvisorShape, elapsed time.Duration) {
	queryID = strings.TrimSpace(queryID)
	if advisor == nil || advisor.capacity <= 0 || queryID == "" || len(dependencies) == 0 || elapsed < 0 {
		return
	}
	key := sqlProjectionAdvisorKey{
		queryID:      queryID,
		dependencies: sqlProjectionAdvisorEncodeDependencies(dependencies),
		shape:        sqlProjectionAdvisorEncodeShape(shape),
	}
	advisor.mu.Lock()
	defer advisor.mu.Unlock()
	if _, exists := advisor.counts[key]; !exists && len(advisor.counts) >= advisor.capacity {
		return
	}
	stats := advisor.counts[key]
	stats.slowQueries = sqlProjectionAdvisorSaturatingAdd(stats.slowQueries, 1)
	stats.totalElapsedNanos = sqlProjectionAdvisorSaturatingAdd(stats.totalElapsedNanos, uint64(elapsed))
	advisor.counts[key] = stats
}

func sqlProjectionAdvisorQueryShape(query *sqlQuery) sqlProjectionAdvisorShape {
	if query == nil {
		return sqlProjectionAdvisorShape{}
	}
	fieldCapacity := len(query.selects) + len(query.groupBy) + len(query.orderBy) + 2 + 2*len(query.joins)
	if query.where.kind != "" {
		fieldCapacity += 2
	}
	if query.prewhere.kind != "" {
		fieldCapacity += 2
	}
	shape := sqlProjectionAdvisorShape{fields: make([]string, 0, fieldCapacity)}
	if query.where.kind != "" || query.prewhere.kind != "" {
		shape.filterFields = make([]string, 0, 4)
	}
	if len(query.groupBy) > 0 {
		shape.groupByFields = make([]string, 0, 2*len(query.groupBy))
	}
	if len(query.orderBy) > 0 {
		shape.orderByFields = make([]string, 0, 2*len(query.orderBy))
	}
	for _, item := range query.selects {
		sqlProjectionAdvisorCollectFields(item.expr, &shape.fields)
	}
	for _, expression := range []sqlExpr{query.where, query.prewhere} {
		before := len(shape.filterFields)
		sqlProjectionAdvisorCollectFields(expression, &shape.filterFields)
		shape.fields = append(shape.fields, shape.filterFields[before:]...)
	}
	for _, expression := range query.groupBy {
		before := len(shape.groupByFields)
		sqlProjectionAdvisorCollectFields(expression, &shape.groupByFields)
		shape.fields = append(shape.fields, shape.groupByFields[before:]...)
	}
	for _, order := range query.orderBy {
		before := len(shape.orderByFields)
		sqlProjectionAdvisorCollectFields(order.expr, &shape.orderByFields)
		shape.fields = append(shape.fields, shape.orderByFields[before:]...)
	}
	for _, join := range query.joins {
		sqlProjectionAdvisorCollectFields(join.on, &shape.fields)
	}
	shape.fields = sqlProjectionAdvisorNormalizeFields(shape.fields)
	shape.filterFields = sqlProjectionAdvisorNormalizeFields(shape.filterFields)
	shape.groupByFields = sqlProjectionAdvisorNormalizeFields(shape.groupByFields)
	shape.orderByFields = sqlProjectionAdvisorNormalizeFields(shape.orderByFields)
	return shape
}

func sqlProjectionAdvisorCollectFields(expression sqlExpr, fields *[]string) {
	if expression.kind == "field" && expression.name != "" {
		field := expression.name
		if expression.qualifier != "" {
			field = expression.qualifier + "." + field
		}
		*fields = append(*fields, field)
	}
	if expression.left != nil {
		sqlProjectionAdvisorCollectFields(*expression.left, fields)
	}
	if expression.right != nil {
		sqlProjectionAdvisorCollectFields(*expression.right, fields)
	}
	for _, argument := range expression.args {
		sqlProjectionAdvisorCollectFields(argument, fields)
	}
	for _, branch := range expression.cases {
		sqlProjectionAdvisorCollectFields(branch.when, fields)
		sqlProjectionAdvisorCollectFields(branch.then, fields)
	}
	if expression.filter != nil {
		sqlProjectionAdvisorCollectFields(*expression.filter, fields)
	}
}

func sqlProjectionAdvisorNormalizeFields(fields []string) []string {
	if len(fields) < 2 {
		return fields
	}
	sort.Strings(fields)
	write := 1
	for _, field := range fields[1:] {
		if field == fields[write-1] {
			continue
		}
		fields[write] = field
		write++
	}
	return fields[:write]
}

func sqlProjectionAdvisorDuration(nanos uint64) time.Duration {
	const maxDurationNanos = uint64(1<<63 - 1)
	if nanos > maxDurationNanos {
		return time.Duration(maxDurationNanos)
	}
	return time.Duration(nanos)
}

func sqlProjectionAdvisorSaturatingAdd(current, delta uint64) uint64 {
	if ^uint64(0)-current < delta {
		return ^uint64(0)
	}
	return current + delta
}

func sqlProjectionAdvisorDependencies(query *sqlQuery) ([]string, bool) {
	if query == nil || query.from == nil || query.from.kind != "CACHE" {
		return nil, false
	}
	dependencies := make([]string, 0, len(query.joins)+1)
	seen := make(map[string]struct{}, len(query.joins)+1)
	appendDependency := func(source sqlSource) bool {
		if source.kind != "CACHE" || source.key == "" {
			return false
		}
		if _, exists := seen[source.key]; !exists {
			seen[source.key] = struct{}{}
			dependencies = append(dependencies, source.key)
		}
		return true
	}
	if !appendDependency(*query.from) {
		return nil, false
	}
	for _, join := range query.joins {
		if !appendDependency(join.source) {
			return nil, false
		}
	}
	sort.Strings(dependencies)
	return dependencies, true
}

func sqlProjectionAdvisorEncodeDependencies(dependencies []string) string {
	var builder strings.Builder
	for _, dependency := range dependencies {
		builder.WriteString(strconv.Itoa(len(dependency)))
		builder.WriteByte(':')
		builder.WriteString(dependency)
	}
	return builder.String()
}

func sqlProjectionAdvisorEncodeShape(shape sqlProjectionAdvisorShape) string {
	groups := [...][]string{shape.fields, shape.filterFields, shape.groupByFields, shape.orderByFields}
	var builder strings.Builder
	for _, group := range groups {
		encoded := sqlProjectionAdvisorEncodeDependencies(group)
		builder.WriteString(strconv.Itoa(len(encoded)))
		builder.WriteByte(':')
		builder.WriteString(encoded)
	}
	return builder.String()
}

func sqlProjectionAdvisorDecodeShape(encoded string) sqlProjectionAdvisorShape {
	if encoded == "" {
		return sqlProjectionAdvisorShape{}
	}
	groups := make([][]string, 4)
	for index := range groups {
		separator := strings.IndexByte(encoded, ':')
		if separator < 0 {
			return sqlProjectionAdvisorShape{}
		}
		length, err := strconv.Atoi(encoded[:separator])
		if err != nil || length < 0 || separator+1+length > len(encoded) {
			return sqlProjectionAdvisorShape{}
		}
		start := separator + 1
		group := encoded[start : start+length]
		if group == "" {
			groups[index] = nil
		} else {
			groups[index] = sqlProjectionAdvisorDecodeDependencies(group)
			if groups[index] == nil {
				return sqlProjectionAdvisorShape{}
			}
		}
		encoded = encoded[start+length:]
	}
	if encoded != "" {
		return sqlProjectionAdvisorShape{}
	}
	return sqlProjectionAdvisorShape{
		fields:        groups[0],
		filterFields:  groups[1],
		groupByFields: groups[2],
		orderByFields: groups[3],
	}
}

func sqlProjectionAdvisorDecodeDependencies(encoded string) []string {
	dependencies := make([]string, 0, 1)
	for len(encoded) > 0 {
		separator := strings.IndexByte(encoded, ':')
		if separator <= 0 {
			return nil
		}
		length, err := strconv.Atoi(encoded[:separator])
		if err != nil || length < 0 || separator+1+length > len(encoded) {
			return nil
		}
		start := separator + 1
		dependencies = append(dependencies, encoded[start:start+length])
		encoded = encoded[start+length:]
	}
	return dependencies
}
