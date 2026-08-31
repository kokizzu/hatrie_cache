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
	SlowQueries  uint64
}

// SQLProjectionAdvisor records bounded candidates for application-managed
// materialized projections. It is opt-in and only records queries with a
// caller-supplied QueryID, avoiding retention of generated or sensitive SQL.
type SQLProjectionAdvisor struct {
	mu       sync.RWMutex
	capacity int
	counts   map[sqlProjectionAdvisorKey]uint64
}

type sqlProjectionAdvisorKey struct {
	queryID      string
	dependencies string
}

// NewSQLProjectionAdvisor creates an advisor that retains at most capacity
// distinct query/dependency recommendations. A nonpositive capacity is inert.
func NewSQLProjectionAdvisor(capacity int) *SQLProjectionAdvisor {
	return &SQLProjectionAdvisor{capacity: capacity, counts: make(map[sqlProjectionAdvisorKey]uint64)}
}

// Recommendations returns deterministic independent copies ordered by the
// observed slow-query count, then caller query ID and dependency list.
func (advisor *SQLProjectionAdvisor) Recommendations() []SQLProjectionRecommendation {
	if advisor == nil {
		return nil
	}
	advisor.mu.RLock()
	recommendations := make([]SQLProjectionRecommendation, 0, len(advisor.counts))
	for key, count := range advisor.counts {
		dependencies := sqlProjectionAdvisorDecodeDependencies(key.dependencies)
		recommendations = append(recommendations, SQLProjectionRecommendation{QueryID: key.queryID, Dependencies: dependencies, SlowQueries: count})
	}
	advisor.mu.RUnlock()
	sort.Slice(recommendations, func(left, right int) bool {
		if recommendations[left].SlowQueries != recommendations[right].SlowQueries {
			return recommendations[left].SlowQueries > recommendations[right].SlowQueries
		}
		if recommendations[left].QueryID != recommendations[right].QueryID {
			return recommendations[left].QueryID < recommendations[right].QueryID
		}
		return sqlProjectionAdvisorEncodeDependencies(recommendations[left].Dependencies) < sqlProjectionAdvisorEncodeDependencies(recommendations[right].Dependencies)
	})
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
	key := sqlProjectionAdvisorKey{queryID: queryID, dependencies: sqlProjectionAdvisorEncodeDependencies(dependencies)}
	advisor.mu.Lock()
	defer advisor.mu.Unlock()
	if _, exists := advisor.counts[key]; !exists && len(advisor.counts) >= advisor.capacity {
		return
	}
	advisor.counts[key]++
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
