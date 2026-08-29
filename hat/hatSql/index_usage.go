package hatSql

import (
	"strings"
	"sync"
)

// SQLIndexDefinition identifies one configured index for a use report.
type SQLIndexDefinition struct {
	Key   string
	Field string
	Kind  string
}

// SQLIndexUseReport combines observed execution use with configured index
// topology. Redundant means more than one configured kind shares a key/field.
type SQLIndexUseReport struct {
	SQLIndexDefinition
	Uses      uint64
	Used      bool
	Unused    bool
	Redundant bool
}

// SQLIndexUseRecorder records successful index selections for direct
// single-field predicates. It does not retain SQL text or predicate values.
type SQLIndexUseRecorder struct {
	mu   sync.RWMutex
	uses map[sqlIndexAdvisorKey]uint64
}

func NewSQLIndexUseRecorder() *SQLIndexUseRecorder {
	return &SQLIndexUseRecorder{uses: make(map[sqlIndexAdvisorKey]uint64)}
}

func (recorder *SQLIndexUseRecorder) Report(configured []SQLIndexDefinition) []SQLIndexUseReport {
	if recorder == nil {
		return nil
	}
	recorder.mu.RLock()
	report := make([]SQLIndexUseReport, len(configured))
	counts := make(map[sqlIndexAdvisorKey]int, len(configured))
	for _, definition := range configured {
		counts[sqlIndexAdvisorKey{key: definition.Key, field: definition.Field}]++
	}
	for index, definition := range configured {
		key := sqlIndexAdvisorKey{key: definition.Key, field: definition.Field}
		uses := recorder.uses[key]
		report[index] = SQLIndexUseReport{SQLIndexDefinition: definition, Uses: uses, Used: uses > 0, Unused: uses == 0, Redundant: counts[key] > 1}
	}
	recorder.mu.RUnlock()
	return report
}

func (recorder *SQLIndexUseRecorder) observe(query *sqlQuery, metrics *sqlExecutionMetrics, err error) {
	if recorder == nil || err != nil || query == nil || query.from == nil || query.from.kind != "CACHE" || len(query.joins) != 0 || metrics == nil {
		return
	}
	used := false
	for _, step := range metrics.steps {
		if strings.Contains(step.Node, "INDEX") {
			used = true
			break
		}
	}
	if !used {
		return
	}
	fields := sqlIndexAdvisorPredicateFields(query.where, query.from.alias)
	if len(fields) != 1 {
		return
	}
	recorder.mu.Lock()
	recorder.uses[sqlIndexAdvisorKey{key: query.from.key, field: fields[0]}]++
	recorder.mu.Unlock()
}
