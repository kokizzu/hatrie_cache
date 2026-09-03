package hatSql

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"
)

// SQLWhatIfIndexKind describes the operation a hypothetical index is meant to
// accelerate. The advisor never creates or registers an index.
type SQLWhatIfIndexKind string

const (
	SQLWhatIfIndexEquality SQLWhatIfIndexKind = "equality"
	SQLWhatIfIndexRange    SQLWhatIfIndexKind = "range"
	SQLWhatIfIndexOrder    SQLWhatIfIndexKind = "order"
	SQLWhatIfIndexGroup    SQLWhatIfIndexKind = "group"
)

// SQLWhatIfIndex describes one proposed index. Source defaults to the direct
// CACHE source in the query when empty. Fields are ordered for composite and
// ORDER BY candidates.
type SQLWhatIfIndex struct {
	Source  string             `json:"source,omitempty"`
	Fields  []string           `json:"fields"`
	Kind    SQLWhatIfIndexKind `json:"kind"`
	Include []string           `json:"include,omitempty"`
}

// SQLWhatIfRequest asks for a read-only estimate of one proposed index.
type SQLWhatIfRequest struct {
	Query      string         `json:"query"`
	Parameters []interface{}  `json:"parameters,omitempty"`
	Index      SQLWhatIfIndex `json:"index"`
}

// SQLWhatIfFieldStatistics is optional aggregate metadata for one source
// field. Minimum and Maximum are used only for numeric range estimates.
type SQLWhatIfFieldStatistics struct {
	Rows              int         `json:"rows"`
	NullRows          int         `json:"null_rows,omitempty"`
	DistinctValues    int         `json:"distinct_values,omitempty"`
	Minimum           interface{} `json:"minimum,omitempty"`
	Maximum           interface{} `json:"maximum,omitempty"`
	AverageValueBytes int         `json:"average_value_bytes,omitempty"`
}

// SQLWhatIfSourceStatistics contains bounded source metadata used to avoid a
// full row read. A provider may return available=false when it cannot answer
// the requested fields; the advisor then uses the ordinary read-only resolver.
type SQLWhatIfSourceStatistics struct {
	Source string                              `json:"source"`
	Rows   int                                 `json:"rows"`
	Bytes  int                                 `json:"bytes,omitempty"`
	Fields map[string]SQLWhatIfFieldStatistics `json:"fields,omitempty"`
}

// SQLWhatIfSourceStatisticsResolver optionally supplies source metadata for
// the advisor. It has no effect on ordinary query execution.
type SQLWhatIfSourceStatisticsResolver interface {
	SQLWhatIfSourceStatistics(name, key string, fields []string) (SQLWhatIfSourceStatistics, bool, error)
}

// SQLWhatIfReport is a deterministic estimate, not a query result. Estimates
// are never used to change SQL semantics or select a production execution
// path. Bytes and row counts are zero only when the source cannot provide the
// corresponding estimate.
type SQLWhatIfReport struct {
	Source                         string         `json:"source"`
	Index                          SQLWhatIfIndex `json:"index"`
	Supported                      bool           `json:"supported"`
	Beneficial                     bool           `json:"beneficial"`
	ExistingIndex                  bool           `json:"existing_index"`
	SourceRows                     int            `json:"source_rows"`
	BaselineRowsRead               int            `json:"baseline_rows_read"`
	HypotheticalRowsRead           int            `json:"hypothetical_rows_read"`
	RowsSkipped                    int            `json:"rows_skipped"`
	SourceBytes                    int            `json:"source_bytes,omitempty"`
	BaselineBytesRead              int            `json:"baseline_bytes_read,omitempty"`
	HypotheticalBytesRead          int            `json:"hypothetical_bytes_read,omitempty"`
	BytesSkipped                   int            `json:"bytes_skipped,omitempty"`
	EstimatedIndexBytes            int            `json:"estimated_index_bytes,omitempty"`
	EstimatedWriteBytesPerMutation int            `json:"estimated_write_bytes_per_mutation,omitempty"`
	PotentialBenefit               string         `json:"potential_benefit,omitempty"`
	Recommendation                 string         `json:"recommendation"`
	Notes                          []string       `json:"notes,omitempty"`
}

type sqlWhatIfPredicate struct {
	field    string
	operator string
	value    interface{}
}

// ExplainSQLWhatIf estimates the effect of one hypothetical index while
// leaving the normal query path unchanged. It currently accepts a single
// direct CACHE source and simple scalar predicates/order/group fields. A
// resolver implementing SQLWhatIfSourceStatisticsResolver can answer without
// materializing source rows; otherwise the source is read once for exact
// estimates.
func ExplainSQLWhatIf(ctx context.Context, request SQLWhatIfRequest, resolver SQLSourceResolver) (SQLWhatIfReport, error) {
	if resolver == nil {
		return SQLWhatIfReport{}, fmt.Errorf("SQL what-if resolver is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return SQLWhatIfReport{}, err
	}
	query, err := parseSQLQueryParameters(strings.TrimSpace(request.Query), request.Parameters)
	if err != nil {
		return SQLWhatIfReport{}, err
	}
	if query.explain {
		return SQLWhatIfReport{}, fmt.Errorf("SQL what-if query cannot be EXPLAIN")
	}
	if query.from == nil || query.from.kind != "CACHE" {
		return SQLWhatIfReport{}, fmt.Errorf("SQL what-if requires one direct CACHE source")
	}
	if len(query.joins) != 0 || len(query.ctes) != 0 || len(query.unions) != 0 {
		return SQLWhatIfReport{}, fmt.Errorf("SQL what-if supports one direct CACHE source without joins, CTEs, or set operations")
	}
	index, err := normalizeSQLWhatIfIndex(request.Index, *query.from)
	if err != nil {
		return SQLWhatIfReport{}, err
	}
	report := SQLWhatIfReport{Source: query.from.key, Index: index, Recommendation: "unsupported"}
	kind := index.Kind
	if kind == "" {
		kind = inferSQLWhatIfIndexKind(*query)
		index.Kind = kind
		report.Index = index
	}
	shape, notes := analyzeSQLWhatIfShape(*query, index)
	report.Notes = append(report.Notes, notes...)
	if !shape.supported {
		return report, nil
	}
	report.Supported = true
	report.PotentialBenefit = shape.benefit
	report.ExistingIndex, err = sqlWhatIfExistingIndex(resolver, query.from.key, index.Fields)
	if err != nil {
		return SQLWhatIfReport{}, err
	}

	fields := append([]string(nil), shape.fields...)
	fields = append(fields, index.Fields...)
	fields = append(fields, index.Include...)
	fields = uniqueSortedStrings(fields)
	statistics, hasStatistics, err := sqlWhatIfSourceStatistics(resolver, query.from.key, fields)
	if err != nil {
		return SQLWhatIfReport{}, err
	}
	var snapshot sqlWhatIfSnapshot
	if hasStatistics {
		snapshot = sqlWhatIfSnapshotFromStatistics(statistics)
	}
	if hasStatistics && shape.filter {
		matched, estimable := sqlWhatIfEstimateMatchesFromStatistics(resolver, query.from.key, snapshot, shape, report.ExistingIndex)
		if estimable {
			snapshot.matched = matched
		} else {
			hasStatistics = false
		}
	}
	if !hasStatistics || !sqlWhatIfCanEstimateFromStatistics(snapshot, shape) {
		loaded, loadErr := loadSQLWhatIfSnapshot(ctx, resolver, *query.from, fields, shape.predicates)
		if loadErr != nil {
			return SQLWhatIfReport{}, loadErr
		}
		snapshot = loaded
	}

	report.SourceRows = snapshot.rows
	report.BaselineRowsRead = snapshot.rows
	report.SourceBytes = snapshot.bytes
	report.BaselineBytesRead = snapshot.bytes
	report.HypotheticalRowsRead = snapshot.rows
	if shape.filter {
		report.HypotheticalRowsRead = snapshot.matched
	}
	if report.HypotheticalRowsRead < 0 || report.HypotheticalRowsRead > report.BaselineRowsRead {
		report.HypotheticalRowsRead = report.BaselineRowsRead
	}
	report.RowsSkipped = report.BaselineRowsRead - report.HypotheticalRowsRead
	report.HypotheticalBytesRead = sqlWhatIfScaledBytes(snapshot.bytes, snapshot.rows, report.HypotheticalRowsRead)
	report.BytesSkipped = report.BaselineBytesRead - report.HypotheticalBytesRead
	report.EstimatedIndexBytes = estimateSQLWhatIfIndexBytes(snapshot, index)
	report.EstimatedWriteBytesPerMutation = estimateSQLWhatIfWriteBytes(snapshot, index)

	if shape.filter {
		report.Beneficial = report.RowsSkipped > 0
	} else {
		// ORDER/GROUP candidates do not reduce the number of source rows. Their
		// benefit is operator elimination, which is reported explicitly.
		report.Beneficial = report.SourceRows > 1
	}
	switch {
	case report.ExistingIndex:
		report.Recommendation = "keep existing"
	case report.Beneficial:
		report.Recommendation = "build"
	case report.SourceRows == 0:
		report.Recommendation = "insufficient workload"
	default:
		report.Recommendation = "skip"
	}
	if !snapshot.exact {
		report.Notes = append(report.Notes, "row counts are estimated from source statistics")
	}
	return report, nil
}

type sqlWhatIfShape struct {
	supported  bool
	filter     bool
	benefit    string
	predicates []sqlWhatIfPredicate
	fields     []string
}

func normalizeSQLWhatIfIndex(index SQLWhatIfIndex, source sqlSource) (SQLWhatIfIndex, error) {
	index.Source = strings.TrimSpace(index.Source)
	if index.Source == "" {
		index.Source = source.key
	}
	if index.Source != source.key {
		return SQLWhatIfIndex{}, fmt.Errorf("SQL what-if index source %q does not match query source %q", index.Source, source.key)
	}
	fields, err := normalizeSQLWhatIfFields(index.Fields, "index fields")
	if err != nil {
		return SQLWhatIfIndex{}, err
	}
	index.Fields = fields
	included, err := normalizeSQLWhatIfFields(index.Include, "included fields")
	if err != nil {
		return SQLWhatIfIndex{}, err
	}
	index.Include = included
	if len(index.Fields) == 0 {
		return SQLWhatIfIndex{}, fmt.Errorf("SQL what-if index requires at least one field")
	}
	switch index.Kind {
	case "", SQLWhatIfIndexEquality, SQLWhatIfIndexRange, SQLWhatIfIndexOrder, SQLWhatIfIndexGroup:
	default:
		return SQLWhatIfIndex{}, fmt.Errorf("unsupported SQL what-if index kind %q", index.Kind)
	}
	return index, nil
}

func normalizeSQLWhatIfFields(fields []string, label string) ([]string, error) {
	seen := make(map[string]struct{}, len(fields))
	result := make([]string, 0, len(fields))
	for _, field := range fields {
		field = strings.TrimSpace(field)
		if field == "" {
			return nil, fmt.Errorf("SQL what-if %s contain an empty field", label)
		}
		if strings.ContainsAny(field, "'\"`()") {
			return nil, fmt.Errorf("SQL what-if %s contain invalid field %q", label, field)
		}
		if _, exists := seen[field]; exists {
			return nil, fmt.Errorf("SQL what-if %s contain duplicate field %q", label, field)
		}
		seen[field] = struct{}{}
		result = append(result, field)
	}
	return result, nil
}

func inferSQLWhatIfIndexKind(query sqlQuery) SQLWhatIfIndexKind {
	if len(query.orderBy) > 0 {
		return SQLWhatIfIndexOrder
	}
	if len(query.groupBy) > 0 {
		return SQLWhatIfIndexGroup
	}
	if predicates, ok := sqlWhatIfPredicates(query.where, query.from.alias); ok && len(predicates) > 0 {
		for _, predicate := range predicates {
			if predicate.operator != "=" {
				return SQLWhatIfIndexRange
			}
		}
		return SQLWhatIfIndexEquality
	}
	return ""
}

func analyzeSQLWhatIfShape(query sqlQuery, index SQLWhatIfIndex) (sqlWhatIfShape, []string) {
	shape := sqlWhatIfShape{}
	notes := []string{}
	switch index.Kind {
	case SQLWhatIfIndexEquality:
		predicates, ok := sqlWhatIfPredicates(query.where, query.from.alias)
		if !ok || len(predicates) == 0 {
			return shape, []string{"equality indexes require one or more simple field = literal predicates"}
		}
		for _, predicate := range predicates {
			if predicate.operator != "=" || !sqlWhatIfContains(index.Fields, predicate.field) {
				return shape, []string{"index fields must cover every equality predicate field"}
			}
		}
		shape.supported = true
		shape.filter = true
		shape.predicates = predicates
		shape.fields = sqlWhatIfPredicateFields(predicates)
		shape.benefit = "reduces equality-filtered rows read"
	case SQLWhatIfIndexRange:
		predicates, ok := sqlWhatIfPredicates(query.where, query.from.alias)
		if !ok || len(predicates) != 1 || !sqlWhatIfContains(index.Fields, predicates[0].field) || !sqlWhatIfRangeOperator(predicates[0].operator) {
			return shape, []string{"range indexes require one simple numeric field range predicate"}
		}
		shape.supported = true
		shape.filter = true
		shape.predicates = predicates
		shape.fields = []string{predicates[0].field}
		shape.benefit = "reduces range-filtered rows read"
	case SQLWhatIfIndexOrder:
		fields, ok := sqlWhatIfOrderFields(query.orderBy, query.from.alias)
		if !ok || len(fields) == 0 || !sqlWhatIfPrefix(index.Fields, fields) {
			return shape, []string{"order indexes must begin with every simple ORDER BY field in order"}
		}
		shape.supported = true
		shape.fields = fields
		shape.benefit = "can eliminate the requested sort"
	case SQLWhatIfIndexGroup:
		fields, ok := sqlWhatIfExpressionFields(query.groupBy, query.from.alias)
		if !ok || len(fields) == 0 || !sqlWhatIfPrefix(index.Fields, fields) {
			return shape, []string{"group indexes must begin with every simple GROUP BY field in order"}
		}
		shape.supported = true
		shape.fields = fields
		shape.benefit = "can provide grouping order for a streaming aggregate"
		notes = append(notes, "group-order execution is advisory until a compatible ordered aggregate is selected")
	default:
		return shape, []string{"the query has no inferable equality, range, order, or group shape"}
	}
	return shape, notes
}

func sqlWhatIfPredicates(expr sqlExpr, alias string) ([]sqlWhatIfPredicate, bool) {
	if expr.kind == "" {
		return nil, false
	}
	if expr.kind == "binary" && expr.op == "AND" && expr.left != nil && expr.right != nil {
		left, leftOK := sqlWhatIfPredicates(*expr.left, alias)
		right, rightOK := sqlWhatIfPredicates(*expr.right, alias)
		return append(left, right...), leftOK && rightOK
	}
	if expr.kind != "binary" || expr.left == nil || expr.right == nil {
		return nil, false
	}
	left, right := *expr.left, *expr.right
	if left.kind == "field" && (left.qualifier == "" || left.qualifier == alias) && right.kind == "literal" && right.value != nil {
		return []sqlWhatIfPredicate{{field: left.name, operator: expr.op, value: right.value}}, true
	}
	if right.kind == "field" && (right.qualifier == "" || right.qualifier == alias) && left.kind == "literal" && left.value != nil {
		return []sqlWhatIfPredicate{{field: right.name, operator: sqlReverseComparisonOperator(expr.op), value: left.value}}, true
	}
	return nil, false
}

func sqlWhatIfPredicateFields(predicates []sqlWhatIfPredicate) []string {
	fields := make([]string, 0, len(predicates))
	seen := make(map[string]struct{}, len(predicates))
	for _, predicate := range predicates {
		if _, exists := seen[predicate.field]; exists {
			continue
		}
		seen[predicate.field] = struct{}{}
		fields = append(fields, predicate.field)
	}
	sort.Strings(fields)
	return fields
}

func sqlWhatIfOrderFields(orders []sqlOrder, alias string) ([]string, bool) {
	fields := make([]string, 0, len(orders))
	for _, order := range orders {
		if order.expr.kind != "field" || (order.expr.qualifier != "" && order.expr.qualifier != alias) {
			return nil, false
		}
		fields = append(fields, order.expr.name)
	}
	return fields, true
}

func sqlWhatIfExpressionFields(expressions []sqlExpr, alias string) ([]string, bool) {
	fields := make([]string, 0, len(expressions))
	for _, expression := range expressions {
		if expression.kind != "field" || (expression.qualifier != "" && expression.qualifier != alias) {
			return nil, false
		}
		fields = append(fields, expression.name)
	}
	return fields, true
}

func sqlWhatIfRangeOperator(operator string) bool {
	switch operator {
	case "<", "<=", ">", ">=":
		return true
	default:
		return false
	}
}

func sqlWhatIfContains(fields []string, wanted string) bool {
	for _, field := range fields {
		if field == wanted {
			return true
		}
	}
	return false
}

func sqlWhatIfPrefix(fields, prefix []string) bool {
	if len(fields) < len(prefix) {
		return false
	}
	for index, field := range prefix {
		if fields[index] != field {
			return false
		}
	}
	return true
}

func uniqueSortedStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

type sqlWhatIfSnapshot struct {
	rows    int
	bytes   int
	matched int
	exact   bool
	fields  map[string]SQLWhatIfFieldStatistics
}

func sqlWhatIfSourceStatistics(resolver SQLSourceResolver, key string, fields []string) (SQLWhatIfSourceStatistics, bool, error) {
	provider, ok := resolver.(SQLWhatIfSourceStatisticsResolver)
	if !ok {
		return SQLWhatIfSourceStatistics{}, false, nil
	}
	statistics, available, err := provider.SQLWhatIfSourceStatistics("CACHE", key, fields)
	if err != nil || !available {
		return SQLWhatIfSourceStatistics{}, available, err
	}
	if statistics.Rows < 0 || statistics.Bytes < 0 {
		return SQLWhatIfSourceStatistics{}, false, fmt.Errorf("SQL what-if source statistics contain negative values")
	}
	return statistics, true, nil
}

func sqlWhatIfSourceStatisticsFields(statistics SQLWhatIfSourceStatistics) map[string]SQLWhatIfFieldStatistics {
	fields := make(map[string]SQLWhatIfFieldStatistics, len(statistics.Fields))
	for field, value := range statistics.Fields {
		if value.Rows < 0 || value.NullRows < 0 || value.DistinctValues < 0 || value.AverageValueBytes < 0 {
			continue
		}
		fields[field] = value
	}
	return fields
}

func sqlWhatIfSnapshotFromStatistics(statistics SQLWhatIfSourceStatistics) sqlWhatIfSnapshot {
	return sqlWhatIfSnapshot{rows: statistics.Rows, bytes: statistics.Bytes, fields: sqlWhatIfSourceStatisticsFields(statistics)}
}

func sqlWhatIfCanEstimateFromStatistics(snapshot sqlWhatIfSnapshot, shape sqlWhatIfShape) bool {
	if snapshot.rows < 0 {
		return false
	}
	return !shape.filter || snapshot.matched >= 0
}

func sqlWhatIfEstimateMatchesFromStatistics(resolver SQLSourceResolver, key string, snapshot sqlWhatIfSnapshot, shape sqlWhatIfShape, existing bool) (int, bool) {
	if len(shape.predicates) != 1 || snapshot.rows < 0 {
		return 0, false
	}
	predicate := shape.predicates[0]
	if existing && predicate.operator == "=" {
		if estimator, ok := resolver.(IndexValueEstimator); ok {
			rows, exact, available, err := estimator.SQLJSONIndexValueEstimate(key, predicate.field, predicate.value)
			if err == nil && available && exact && rows >= 0 {
				return minSQLWhatIfRows(rows, snapshot.rows), true
			}
		}
	}
	field, ok := snapshot.fields[predicate.field]
	if !ok || field.Rows < 0 {
		return 0, false
	}
	if predicate.operator == "=" {
		nonNull := field.Rows
		if nonNull == 0 && snapshot.rows > field.NullRows {
			nonNull = snapshot.rows - field.NullRows
		}
		if field.DistinctValues <= 0 {
			return 0, false
		}
		return minSQLWhatIfRows((nonNull+field.DistinctValues-1)/field.DistinctValues, snapshot.rows), true
	}
	minimum, minOK := sqlNumber(field.Minimum)
	maximum, maxOK := sqlNumber(field.Maximum)
	target, targetOK := sqlNumber(predicate.value)
	if !minOK || !maxOK || !targetOK || maximum < minimum {
		return 0, false
	}
	if maximum == minimum {
		if sqlWhatIfCompare(field.Minimum, predicate.operator, predicate.value) {
			return minSQLWhatIfRows(field.Rows, snapshot.rows), true
		}
		return 0, true
	}
	fraction := 0.0
	switch predicate.operator {
	case "<", "<=":
		fraction = (target - minimum) / (maximum - minimum)
	case ">", ">=":
		fraction = (maximum - target) / (maximum - minimum)
	default:
		return 0, false
	}
	if fraction < 0 {
		fraction = 0
	}
	if fraction > 1 {
		fraction = 1
	}
	return minSQLWhatIfRows(int(math.Ceil(float64(field.Rows)*fraction)), snapshot.rows), true
}

func minSQLWhatIfRows(rows, maximum int) int {
	if rows < 0 {
		return 0
	}
	if rows > maximum {
		return maximum
	}
	return rows
}

func loadSQLWhatIfSnapshot(ctx context.Context, resolver SQLSourceResolver, source sqlSource, fields []string, predicates []sqlWhatIfPredicate) (sqlWhatIfSnapshot, error) {
	rows, err := resolver.ResolveSQLSource(source.kind, source.key)
	if err != nil {
		return sqlWhatIfSnapshot{}, err
	}
	matched := 0
	for _, row := range rows {
		if err := ctx.Err(); err != nil {
			return sqlWhatIfSnapshot{}, err
		}
		if sqlWhatIfRowMatches(row, predicates) {
			matched++
		}
	}
	_ = fields
	return sqlWhatIfSnapshot{rows: len(rows), bytes: sqlRowsBytes(rows), matched: matched, exact: true}, nil
}

func sqlWhatIfRowMatches(row SQLRow, predicates []sqlWhatIfPredicate) bool {
	for _, predicate := range predicates {
		if !sqlWhatIfCompare(row[predicate.field], predicate.operator, predicate.value) {
			return false
		}
	}
	return true
}

func sqlWhatIfCompare(left interface{}, operator string, right interface{}) bool {
	if leftNumber, leftOK := sqlNumber(left); leftOK {
		if rightNumber, rightOK := sqlNumber(right); rightOK {
			return sqlColumnarNumericMatches(leftNumber, operator, rightNumber)
		}
	}
	if operator == "=" {
		return reflect.DeepEqual(left, right)
	}
	if left == nil || right == nil {
		return false
	}
	leftText, leftOK := left.(string)
	rightText, rightOK := right.(string)
	if leftOK && rightOK {
		switch operator {
		case "<":
			return leftText < rightText
		case "<=":
			return leftText <= rightText
		case ">":
			return leftText > rightText
		case ">=":
			return leftText >= rightText
		}
	}
	return false
}

func sqlWhatIfExistingIndex(resolver SQLSourceResolver, key string, fields []string) (bool, error) {
	provider, ok := resolver.(JSONIndexStatsResolver)
	if !ok {
		return false, nil
	}
	_, available, err := provider.SQLJSONIndexStats(key, fields...)
	return available, err
}

func sqlWhatIfScaledBytes(bytes, rows, selected int) int {
	if bytes <= 0 || rows <= 0 || selected <= 0 {
		return 0
	}
	if selected >= rows {
		return bytes
	}
	scaled := (int64(bytes)*int64(selected) + int64(rows) - 1) / int64(rows)
	if scaled > int64(int(^uint(0)>>1)) {
		return int(^uint(0) >> 1)
	}
	return int(scaled)
}

func estimateSQLWhatIfIndexBytes(snapshot sqlWhatIfSnapshot, index SQLWhatIfIndex) int {
	if snapshot.rows <= 0 {
		return 0
	}
	perRow := 16
	perRow = sqlWhatIfSaturatingAdd(perRow, sqlWhatIfSaturatingMultiply(len(index.Fields), 8))
	perRow = sqlWhatIfSaturatingAdd(perRow, sqlWhatIfSaturatingMultiply(len(index.Include), 8))
	for _, field := range append(append([]string(nil), index.Fields...), index.Include...) {
		if stats, ok := snapshot.fields[field]; ok && stats.AverageValueBytes > 0 {
			perRow = sqlWhatIfSaturatingAdd(perRow, stats.AverageValueBytes)
		}
	}
	return sqlWhatIfSaturatingMultiply(snapshot.rows, perRow)
}

func estimateSQLWhatIfWriteBytes(snapshot sqlWhatIfSnapshot, index SQLWhatIfIndex) int {
	if snapshot.rows <= 0 {
		return 16 + len(index.Fields)*8 + len(index.Include)*8
	}
	bytes := estimateSQLWhatIfIndexBytes(snapshot, index)
	perMutation := bytes / snapshot.rows
	if bytes%snapshot.rows != 0 {
		perMutation++
	}
	return perMutation
}

func sqlWhatIfSaturatingAdd(left, right int) int {
	if left <= 0 {
		return right
	}
	if right <= 0 {
		return left
	}
	maximum := int(^uint(0) >> 1)
	if left > maximum-right {
		return maximum
	}
	return left + right
}

func sqlWhatIfSaturatingMultiply(left, right int) int {
	if left <= 0 || right <= 0 {
		return 0
	}
	maximum := int(^uint(0) >> 1)
	if left > maximum/right {
		return maximum
	}
	return left * right
}
