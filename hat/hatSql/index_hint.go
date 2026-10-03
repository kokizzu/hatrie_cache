package hatSql

import (
	"fmt"
	"strings"
	"time"
)

// SQLIndexHintMode controls a diagnostic index-planning override.
type SQLIndexHintMode string

const (
	// SQLIndexHintForce requires one field index for a query source. Execution
	// fails instead of silently falling back to a scan when it is unavailable.
	SQLIndexHintForce SQLIndexHintMode = "FORCE"
	// SQLIndexHintForbid prevents one field index from being selected while
	// leaving other eligible indexes available to the planner.
	SQLIndexHintForbid SQLIndexHintMode = "FORBID"
)

// SQLIndexHint is a per-query diagnostic planner override. Source is an
// optional source alias; empty applies to a matching field in any source.
// Index optionally names one exact embedded index for FORCE or FORBID when the
// resolver implements NamedIndexedSourceResolver. Hints do not create or
// alter indexes and should not be used as a permanent plan-management
// mechanism.
type SQLIndexHint struct {
	Source string
	Field  string
	Index  string
	Mode   SQLIndexHintMode
}

// IndexHint is the package-native name for SQLIndexHint.
type IndexHint = SQLIndexHint

// IndexHintMode is the package-native name for SQLIndexHintMode.
type IndexHintMode = SQLIndexHintMode

func (hint SQLIndexHint) validate() error {
	if hint.Mode == "" && hint.Source == "" && hint.Field == "" && hint.Index == "" {
		return nil
	}
	if strings.TrimSpace(hint.Field) == "" {
		return fmt.Errorf("SQL index hint requires a field")
	}
	if strings.TrimSpace(hint.Index) != "" && hint.Mode == "" {
		return fmt.Errorf("SQL named index hint requires FORCE or FORBID")
	}
	switch hint.Mode {
	case SQLIndexHintForce, SQLIndexHintForbid:
		return nil
	default:
		return fmt.Errorf("SQL index hint mode must be FORCE or FORBID")
	}
}

func (hint SQLIndexHint) applies(source sqlSource) bool {
	return hint.Mode != "" && (hint.Source == "" || strings.EqualFold(hint.Source, source.alias))
}

func (hint SQLIndexHint) allowsField(source sqlSource, field string) bool {
	return !hint.applies(source) || hint.Mode != SQLIndexHintForbid || !strings.EqualFold(hint.Field, field)
}

func (hint SQLIndexHint) allowsIndex(source sqlSource, index string) bool {
	return !hint.applies(source) || hint.Mode != SQLIndexHintForbid || hint.Index == "" || !strings.EqualFold(hint.Index, index)
}

func sqlIndexHintAllowsFields(hint SQLIndexHint, source sqlSource, fields []string) bool {
	for _, field := range fields {
		if !hint.allowsField(source, field) {
			return false
		}
	}
	return true
}

func sqlIndexHintForSource(metrics *sqlExecutionMetrics, source sqlSource) SQLIndexHint {
	if metrics == nil || !metrics.indexHint.applies(source) {
		return SQLIndexHint{}
	}
	return metrics.indexHint
}

func sqlIndexHintComparison(source sqlSource, condition sqlExpr, field string) (string, interface{}, bool) {
	if condition.kind == "binary" && condition.op == "AND" && condition.left != nil && condition.right != nil {
		if operator, value, ok := sqlIndexHintComparison(source, *condition.left, field); ok {
			return operator, value, true
		}
		return sqlIndexHintComparison(source, *condition.right, field)
	}
	if condition.kind != "binary" || condition.left == nil || condition.right == nil {
		return "", nil, false
	}
	left, right := *condition.left, *condition.right
	if left.kind == "field" && left.qualifier == source.alias && strings.EqualFold(left.name, field) && right.kind == "literal" {
		return condition.op, right.value, true
	}
	if right.kind == "field" && right.qualifier == source.alias && strings.EqualFold(right.name, field) && left.kind == "literal" {
		return sqlReverseComparison(condition.op), left.value, true
	}
	return "", nil, false
}

func resolveSQLForcedIndex(source sqlSource, condition sqlExpr, resolver SQLSourceResolver, metrics *sqlExecutionMetrics, hint SQLIndexHint) ([]SQLRow, bool, error) {
	operator, value, matched := sqlIndexHintComparison(source, condition, hint.Field)
	if !matched {
		return nil, false, fmt.Errorf("SQL forced index %q has no compatible predicate", hint.Field)
	}
	started := time.Now()
	rows, available, err := resolveSQLIndexedComparisonWithHint(source, hint.Field, operator, value, resolver, hint)
	if err != nil {
		return nil, false, err
	}
	if !available {
		if hint.Index != "" {
			return nil, false, fmt.Errorf("SQL named index %q is unavailable", hint.Index)
		}
		return nil, false, fmt.Errorf("SQL forced index %q is unavailable", hint.Field)
	}
	detail := sqlExplainSource(source) + " field=" + hint.Field
	if hint.Index != "" {
		detail += " index=" + hint.Index
	}
	metrics.record("FORCED INDEX SCAN", detail, 0, len(rows), started)
	return rows, true, nil
}

func resolveSQLIndexedComparisonWithHint(source sqlSource, field, operator string, value interface{}, resolver SQLSourceResolver, hint SQLIndexHint) ([]SQLRow, bool, error) {
	if hint.Index == "" {
		return resolveSQLIndexedComparison(source, field, operator, value, resolver)
	}
	if !hint.allowsIndex(source, hint.Index) {
		return nil, false, nil
	}
	named, ok := resolver.(NamedIndexedSourceResolver)
	if !ok {
		return nil, false, fmt.Errorf("SQL named index %q is unavailable", hint.Index)
	}
	return named.ResolveSQLNamedIndexedSource(source.kind, source.key, hint.Index, field, value)
}
