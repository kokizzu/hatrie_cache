package hatSql

import (
	"fmt"
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

// SQLProjectionCostModel describes the workload and maintenance assumptions
// used by CostBasedRecommendations. All durations are nonnegative. The model
// is caller-supplied because the advisor deliberately does not retain rows or
// source-write rates.
type SQLProjectionCostModel struct {
	ExpectedQueries   uint64
	ExpectedRefreshes uint64
	QueryHitLatency   time.Duration
	InitialBuildCost  time.Duration
	RefreshCost       time.Duration
}

// SQLProjectionCostRecommendation compares observed query savings with the
// caller's projected build and refresh costs. WorthBuilding is true only when
// the estimated net benefit is positive.
type SQLProjectionCostRecommendation struct {
	SQLProjectionRecommendation
	EstimatedQuerySavings    time.Duration
	EstimatedMaintenanceCost time.Duration
	EstimatedNetBenefit      time.Duration
	WorthBuilding            bool
}

// SQLProjectionAdvisor records bounded candidates for application-managed
// materialized projections. It is opt-in and only records queries with a
// caller-supplied QueryID, avoiding retention of generated or sensitive SQL.
type SQLProjectionAdvisor struct {
	mu              sync.RWMutex
	capacity        int
	counts          map[sqlProjectionAdvisorKey]sqlProjectionAdvisorStats
	forecastEnabled bool
	workloads       map[sqlProjectionAdvisorKey]sqlProjectionAdvisorWorkloadStats
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
	return NewSQLProjectionAdvisorWithOptions(SQLProjectionAdvisorOptions{Capacity: capacity})
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

// CostBasedRecommendations returns deterministic recommendations ordered by
// estimated net benefit. Query savings use each candidate's observed average
// latency minus the projected projection-hit latency, multiplied by the
// expected query count. Maintenance includes one initial build and the
// expected refreshes. This method never changes planning or creates a view.
func (advisor *SQLProjectionAdvisor) CostBasedRecommendations(limit int, model SQLProjectionCostModel) ([]SQLProjectionCostRecommendation, error) {
	if model.ExpectedQueries == 0 {
		return nil, fmt.Errorf("projection cost model expected queries must be positive")
	}
	if model.QueryHitLatency < 0 || model.InitialBuildCost < 0 || model.RefreshCost < 0 {
		return nil, fmt.Errorf("projection cost model durations must not be negative")
	}
	maintenance := sqlProjectionAdvisorDurationSum(
		model.InitialBuildCost,
		sqlProjectionAdvisorDurationProduct(model.RefreshCost, model.ExpectedRefreshes),
	)
	recommendations := make([]SQLProjectionCostRecommendation, 0)
	if advisor != nil {
		advisor.mu.RLock()
		recommendations = make([]SQLProjectionCostRecommendation, 0, len(advisor.counts))
		for key, stats := range advisor.counts {
			recommendation := sqlProjectionAdvisorRecommendation(key, stats)
			savingsPerQuery := recommendation.AverageElapsed - model.QueryHitLatency
			if savingsPerQuery < 0 {
				savingsPerQuery = 0
			}
			savings := sqlProjectionAdvisorDurationProduct(savingsPerQuery, model.ExpectedQueries)
			netBenefit := sqlProjectionAdvisorDurationDifference(savings, maintenance)
			recommendations = append(recommendations, SQLProjectionCostRecommendation{
				SQLProjectionRecommendation: recommendation,
				EstimatedQuerySavings:       savings,
				EstimatedMaintenanceCost:    maintenance,
				EstimatedNetBenefit:         netBenefit,
				WorthBuilding:               netBenefit > 0,
			})
		}
		advisor.mu.RUnlock()
	}
	sort.Slice(recommendations, func(left, right int) bool {
		if recommendations[left].EstimatedNetBenefit != recommendations[right].EstimatedNetBenefit {
			return recommendations[left].EstimatedNetBenefit > recommendations[right].EstimatedNetBenefit
		}
		if recommendations[left].QueryID != recommendations[right].QueryID {
			return recommendations[left].QueryID < recommendations[right].QueryID
		}
		return sqlProjectionAdvisorEncodeDependencies(recommendations[left].Dependencies) < sqlProjectionAdvisorEncodeDependencies(recommendations[right].Dependencies)
	})
	if limit > 0 && len(recommendations) > limit {
		recommendations = recommendations[:limit]
	}
	return recommendations, nil
}

func (advisor *SQLProjectionAdvisor) recommendations(byCost bool, limit int) []SQLProjectionRecommendation {
	if advisor == nil {
		return nil
	}
	advisor.mu.RLock()
	recommendations := make([]SQLProjectionRecommendation, 0, len(advisor.counts))
	for key, stats := range advisor.counts {
		recommendations = append(recommendations, sqlProjectionAdvisorRecommendation(key, stats))
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

func sqlProjectionAdvisorRecommendation(key sqlProjectionAdvisorKey, stats sqlProjectionAdvisorStats) SQLProjectionRecommendation {
	dependencies := sqlProjectionAdvisorDecodeDependencies(key.dependencies)
	shape := sqlProjectionAdvisorDecodeShape(key.shape)
	totalElapsed := sqlProjectionAdvisorDuration(stats.totalElapsedNanos)
	averageElapsed := time.Duration(0)
	if stats.slowQueries > 0 {
		averageElapsed = sqlProjectionAdvisorDuration(stats.totalElapsedNanos / stats.slowQueries)
	}
	return SQLProjectionRecommendation{
		QueryID:        key.queryID,
		Dependencies:   dependencies,
		Fields:         shape.fields,
		FilterFields:   shape.filterFields,
		GroupByFields:  shape.groupByFields,
		OrderByFields:  shape.orderByFields,
		SlowQueries:    stats.slowQueries,
		TotalElapsed:   totalElapsed,
		AverageElapsed: averageElapsed,
	}
}

func (advisor *SQLProjectionAdvisor) observeSlowQuery(query *sqlQuery, queryID string, metrics *sqlExecutionMetrics, elapsed time.Duration, threshold time.Duration, err error) {
	queryID = strings.TrimSpace(queryID)
	if advisor == nil || advisor.capacity <= 0 || queryID == "" || err != nil {
		return
	}
	if !advisor.forecastEnabled && (threshold <= 0 || elapsed < threshold) {
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
	shape := sqlProjectionAdvisorQueryShape(query)
	if advisor.forecastEnabled {
		advisor.recordWorkloadWithShape(queryID, dependencies, shape, time.Now())
	}
	if threshold <= 0 || elapsed < threshold {
		return
	}
	advisor.recordFeedbackWithShape(queryID, dependencies, shape, elapsed)
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

func sqlProjectionAdvisorDurationProduct(value time.Duration, multiplier uint64) time.Duration {
	if value <= 0 || multiplier == 0 {
		return 0
	}
	max := uint64(1<<63 - 1)
	valueNanos := uint64(value)
	if valueNanos > max/multiplier {
		return time.Duration(max)
	}
	return time.Duration(valueNanos * multiplier)
}

func sqlProjectionAdvisorDurationSum(values ...time.Duration) time.Duration {
	max := uint64(1<<63 - 1)
	var total uint64
	for _, value := range values {
		if value <= 0 {
			continue
		}
		valueNanos := uint64(value)
		if total > max-valueNanos {
			return time.Duration(max)
		}
		total += valueNanos
	}
	return time.Duration(total)
}

func sqlProjectionAdvisorDurationDifference(savings, maintenance time.Duration) time.Duration {
	if savings >= maintenance {
		return savings - maintenance
	}
	difference := uint64(maintenance) - uint64(savings)
	max := uint64(1<<63 - 1)
	if difference > max {
		return -time.Duration(max)
	}
	return -time.Duration(difference)
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
