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

// SQLIndexAdvisor records bounded candidate fields from observed slow scans.
// It is intended for trusted server-side use, not external telemetry export.
type SQLIndexAdvisor struct {
	mu       sync.RWMutex
	capacity int
	counts   map[sqlIndexAdvisorKey]uint64
}

type sqlIndexAdvisorKey struct {
	key   string
	field string
}

func NewSQLIndexAdvisor(capacity int) *SQLIndexAdvisor {
	return &SQLIndexAdvisor{capacity: capacity, counts: make(map[sqlIndexAdvisorKey]uint64)}
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
	if len(fields) == 0 {
		return
	}
	advisor.mu.Lock()
	defer advisor.mu.Unlock()
	for _, field := range fields {
		key := sqlIndexAdvisorKey{key: query.from.key, field: field}
		if _, exists := advisor.counts[key]; !exists && len(advisor.counts) >= advisor.capacity {
			continue
		}
		advisor.counts[key]++
	}
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
