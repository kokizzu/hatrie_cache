package hatSql

import (
	"container/heap"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"math"
	"math/big"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type sqlToken = Token
type sqlTokenKind = TokenKind

const (
	sqlTokenEOF          = TokenEOF
	sqlTokenIdentifier   = TokenIdentifier
	sqlTokenString       = TokenString
	sqlTokenNumber       = TokenNumber
	sqlTokenComma        = TokenComma
	sqlTokenSemicolon    = TokenSemicolon
	sqlTokenLeftParen    = TokenLeftParen
	sqlTokenRightParen   = TokenRightParen
	sqlTokenEqual        = TokenEqual
	sqlTokenArrow        = TokenArrow
	sqlTokenPlus         = TokenPlus
	sqlTokenMinus        = TokenMinus
	sqlTokenSlash        = TokenSlash
	sqlTokenPercent      = TokenPercent
	sqlTokenBang         = TokenBang
	sqlTokenDot          = TokenDot
	sqlTokenStar         = TokenStar
	sqlTokenLess         = TokenLess
	sqlTokenLessEqual    = TokenLessEqual
	sqlTokenGreater      = TokenGreater
	sqlTokenGreaterEqual = TokenGreaterEqual
	sqlTokenNotEqual     = TokenNotEqual
	sqlTokenParameter    = TokenParameter
)

func lexSQL(source string) ([]sqlToken, error) { return Lex(source) }

func sqlTokenDiagnostic(token sqlToken, message string, suggestion ...string) error {
	value := ""
	if len(suggestion) > 0 {
		value = suggestion[0]
	}
	return &Diagnostic{Message: message, Line: token.line, Column: token.column, EndColumn: token.endColumn, Suggestion: value}
}

func nearestSQLName(value string, candidates []string) string {
	if len(candidates) == 0 {
		return ""
	}
	value = strings.ToUpper(value)
	best, bestDistance, tied := "", 3, false
	for _, candidate := range candidates {
		distance := sqlEditDistance(value, strings.ToUpper(candidate))
		if distance < bestDistance {
			best, bestDistance, tied = candidate, distance, false
		} else if distance == bestDistance {
			tied = true
		}
	}
	if tied || best == "" {
		return ""
	}
	return best
}

func sqlEditDistance(left, right string) int {
	previous := make([]int, len(right)+1)
	current := make([]int, len(right)+1)
	for index := range previous {
		previous[index] = index
	}
	for leftIndex := 1; leftIndex <= len(left); leftIndex++ {
		current[0] = leftIndex
		for rightIndex := 1; rightIndex <= len(right); rightIndex++ {
			cost := 0
			if left[leftIndex-1] != right[rightIndex-1] {
				cost = 1
			}
			current[rightIndex] = minSQLInt(previous[rightIndex]+1, current[rightIndex-1]+1, previous[rightIndex-1]+cost)
		}
		previous, current = current, previous
	}
	return previous[len(right)]
}

func minSQLInt(values ...int) int {
	minimum := values[0]
	for _, value := range values[1:] {
		if value < minimum {
			minimum = value
		}
	}
	return minimum
}

type SQLDiagnostic = Diagnostic
type SQLQueryObserver = QueryObserver
type SQLQueryObserverFunc = QueryObserverFunc
type SQLQueryEvent = QueryEvent
type SQLQueryOperator = QueryOperator
type SQLQueryRequest = QueryRequest
type SQLRow = Row
type SQLQueryResult = QueryResult
type SQLExplainStep = ExplainStep
type SQLQueryStats = QueryStats
type SQLSourceResolver = SourceResolver
type SQLStreamSourceResolver = StreamSourceResolver
type SQLSnapshotLocker = SnapshotLocker
type SQLIndexedSourceResolver = IndexedSourceResolver
type SQLRangeIndexedSourceResolver = RangeIndexedSourceResolver
type SQLOrderedSourceResolver = OrderedSourceResolver
type SQLOrderedStreamSourceResolver = OrderedStreamSourceResolver
type SQLCompositeIndexedSourceResolver = CompositeIndexedSourceResolver
type SQLJSONIndexFrequencyBucket = JSONIndexFrequencyBucket
type SQLJSONIndexStats = JSONIndexStats
type SQLSourceResolverFunc = SourceResolverFunc
type SQLFunctionDefinition = FunctionDefinition
type SQLFunctionCall = FunctionCall
type SQLFunctionError = FunctionError
type SQLFunctionResolver = FunctionResolver

// FunctionRuntime evaluates a vectorized UDF implementation.
type FunctionRuntime interface {
	Evaluate([]FunctionCall) ([]interface{}, error)
}

const maxSQLQueryRows = 100000
const maxSQLPageSize = 10000

// SQLQueryOptions bounds one query. Zero uses the safe default or disables an
// optional byte/work budget; Timeout derives a deadline from ctx.
type SQLQueryOptions struct {
	MaxRows     int
	MaxJoinWork int
	// MaxJoinBytes bounds in-memory hash partitions. Combined with
	// SpillDirectory and MaxSpillBytes it enables a streamed spill hash join
	// for a direct two-source INNER equality join. Zero keeps the existing
	// in-memory join behavior.
	MaxJoinBytes   int
	MaxResultBytes int
	// Workers enables bounded parallel CPU work for eligible query operators.
	// Zero keeps the deterministic sequential default.
	Workers       int
	MaxSortBytes  int
	MaxGroupBytes int
	// MaxSetBytes bounds one in-memory distinct UNION, INTERSECT, or EXCEPT
	// operation.
	// When combined with SpillDirectory and MaxSpillBytes, oversized distinct
	// set membership is merged through temporary runs instead of failing.
	MaxSetBytes int
	// SpillDirectory and MaxSpillBytes opt into bounded temporary files for a
	// sort that exceeds MaxSortBytes. Both must be set; the default remains a
	// safe in-memory budget failure. Temporary files are removed before return.
	SpillDirectory        string
	MaxSpillBytes         int
	MaxRecursionDepth     int
	DetectRecursiveCycles bool
	Timeout               time.Duration
	PreparedCache         *SQLPreparedQueryCache
	// AdaptivePlanner learns index candidate cardinality from prior queries.
	// Nil preserves the deterministic estimate-only planner.
	AdaptivePlanner *AdaptivePlanner
	// QueryID is returned with materialized results and included in every
	// observation event. When Observer is set but QueryID is empty, execution
	// assigns a unique ID.
	QueryID            string
	SlowQueryThreshold time.Duration
	Observer           SQLQueryObserver
}

// QueryOptions bounds one query. It is the package-native name for
// SQLQueryOptions, retained for compatibility with the root package.
type QueryOptions = SQLQueryOptions

// SQLPreparedQueryCacheStats reports immutable parsed-template reuse. Values
// bound to `$n` are never stored in this cache.
type SQLPreparedQueryCacheStats struct {
	Entries int
	Hits    uint64
	Misses  uint64
}

// PreparedQueryCacheStats reports parsed-template cache reuse.
type PreparedQueryCacheStats = SQLPreparedQueryCacheStats

// SQLPreparedQueryCache caches parsed, unbound SQL templates by exact source
// text with least-recently-used eviction. It is safe for concurrent query
// execution.
type SQLPreparedQueryCache struct {
	mu       sync.Mutex
	capacity int
	entries  map[string]*sqlQuery
	order    []string
	hits     uint64
	misses   uint64
}

// PreparedQueryCache caches immutable parsed, unbound SQL templates.
type PreparedQueryCache = SQLPreparedQueryCache

// NewSQLPreparedQueryCache creates a bounded parsed-template cache. A nonpositive
// capacity disables storage while preserving syntax and binding behavior.
func NewSQLPreparedQueryCache(capacity int) *SQLPreparedQueryCache {
	return &SQLPreparedQueryCache{capacity: capacity, entries: map[string]*sqlQuery{}}
}

// NewPreparedQueryCache creates a bounded parsed-template cache.
func NewPreparedQueryCache(capacity int) *PreparedQueryCache {
	return NewSQLPreparedQueryCache(capacity)
}

// Stats returns a stable snapshot of the cache counters.
func (cache *SQLPreparedQueryCache) Stats() SQLPreparedQueryCacheStats {
	if cache == nil {
		return SQLPreparedQueryCacheStats{}
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	return SQLPreparedQueryCacheStats{Entries: len(cache.entries), Hits: cache.hits, Misses: cache.misses}
}

var defaultSQLPreparedQueryCache = NewSQLPreparedQueryCache(256)

// sqlDate is a canonical calendar date. It intentionally remains distinct
// from time.Time so DATE values serialize as YYYY-MM-DD rather than a
// midnight timestamp.
type sqlDate string

// sqlDecimal preserves its user-visible decimal spelling while comparisons use
// arbitrary-precision rational values. It deliberately does not reuse
// float64, which would lose significant digits from financial-style values.
type sqlDecimal string

// sqlSpillOutput is deliberately limited to values needed after projection:
// sort keys, a stable input ordinal, and the user-visible projected row. The
// source/group state is not serialized, which avoids retaining join inputs
// while an external merge is running.
type sqlSpillOutput struct {
	Row     SQLRow
	Keys    []interface{}
	Ordinal int
}

type sqlSpillRun struct {
	path  string
	bytes int64
}

const maxSQLSpillMergeFanIn = 32
const sqlSpillHashPartitions = 64

var errSQLSpillDiskBudget = errors.New("SQL spill disk budget exceeded")

type sqlSpillHashInput struct {
	Row     SQLRow
	Ordinal int
}

type sqlSpillHashPartition struct {
	path    string
	file    *os.File
	encoder *gob.Encoder
}

func init() {
	// Gob requires concrete dynamic interface values to be registered. These
	// cover the SQL value domain, including nested decoded JSON values and the
	// date/decimal types that preserve SQL semantics beyond JSON's defaults.
	gob.Register(SQLRow{})
	gob.Register(map[string]interface{}{})
	gob.Register([]interface{}{})
	gob.Register([]SQLRow{})
	gob.Register([]byte{})
	gob.Register(false)
	gob.Register("")
	gob.Register(int(0))
	gob.Register(int8(0))
	gob.Register(int16(0))
	gob.Register(int32(0))
	gob.Register(int64(0))
	gob.Register(uint(0))
	gob.Register(uint8(0))
	gob.Register(uint16(0))
	gob.Register(uint32(0))
	gob.Register(uint64(0))
	gob.Register(float32(0))
	gob.Register(float64(0))
	gob.Register(json.Number(""))
	gob.Register(sqlDate(""))
	gob.Register(sqlDecimal(""))
	gob.Register(time.Time{})
}

func parseSQLDecimal(value string) (sqlDecimal, bool) {
	if value == "" {
		return "", false
	}
	index := 0
	if value[index] == '+' || value[index] == '-' {
		index++
	}
	digits := 0
	for index < len(value) && value[index] >= '0' && value[index] <= '9' {
		index++
		digits++
	}
	if index < len(value) && value[index] == '.' {
		index++
		fractionStart := index
		for index < len(value) && value[index] >= '0' && value[index] <= '9' {
			index++
		}
		if index == fractionStart {
			return "", false
		}
	}
	if digits == 0 || index != len(value) {
		return "", false
	}
	if _, ok := new(big.Rat).SetString(value); !ok {
		return "", false
	}
	return sqlDecimal(value), true
}

// sqlEvalError carries a dynamic expression failure through the existing
// value-oriented evaluator. Execution checks it at expression boundaries, so
// an invalid CAST is never mistaken for NULL or returned as a result value.
type sqlEvalError struct {
	err   error
	token sqlToken
}

func (e sqlEvalError) Error() string { return e.err.Error() }

func sqlExpressionError(value interface{}) error {
	if failed, ok := value.(sqlEvalError); ok {
		return failed
	}
	return nil
}

func sqlEvaluationFailure(err error) sqlEvalError {
	if failed, ok := err.(sqlEvalError); ok {
		return failed
	}
	return sqlEvalError{err: err}
}

func sqlRuntimeDiagnostic(err error) error {
	if failed, ok := err.(sqlEvalError); ok && failed.token.line > 0 {
		return sqlTokenDiagnostic(failed.token, failed.err.Error())
	}
	return err
}

// ExecuteSQLQuery parses and executes a read-only relational query against a
// snapshot supplied by resolver. It intentionally does not execute cache commands.
func ExecuteSQLQuery(source string, resolver SQLSourceResolver) (SQLQueryResult, error) {
	return ExecuteSQLQueryContext(context.Background(), source, resolver, SQLQueryOptions{})
}

// ExecuteQuery parses and executes a read-only query against resolver.
func ExecuteQuery(source string, resolver SourceResolver) (QueryResult, error) {
	return ExecuteSQLQuery(source, resolver)
}

// ExecuteSQLQueryContext executes a query with cancellation and resource
// budgets. It is the context-aware counterpart of ExecuteSQLQuery.
func ExecuteSQLQueryContext(ctx context.Context, source string, resolver SQLSourceResolver, options SQLQueryOptions) (SQLQueryResult, error) {
	return ExecuteSQLQueryParameters(ctx, source, resolver, nil, options)
}

// ExecuteQueryContext is the context-aware form of ExecuteQuery.
func ExecuteQueryContext(ctx context.Context, source string, resolver SourceResolver, options QueryOptions) (QueryResult, error) {
	return ExecuteSQLQueryContext(ctx, source, resolver, options)
}

var sqlQueryIDSequence atomic.Uint64

type sqlQueryObservation struct {
	id        string
	observer  SQLQueryObserver
	started   time.Time
	threshold time.Duration
}

func newSQLQueryObservation(options SQLQueryOptions) sqlQueryObservation {
	id := strings.TrimSpace(options.QueryID)
	if id == "" && options.Observer != nil {
		id = fmt.Sprintf("sql-%d", sqlQueryIDSequence.Add(1))
	}
	return sqlQueryObservation{
		id:        id,
		observer:  options.Observer,
		started:   time.Now(),
		threshold: options.SlowQueryThreshold,
	}
}

func (observation sqlQueryObservation) finish(result SQLQueryResult, err error, steps []SQLExplainStep) {
	observation.finishSummary(len(result.Rows), len(result.Columns), sqlRowsBytes(result.Rows), err, steps)
}

func (observation sqlQueryObservation) finishSummary(outputRows, outputColumns, resultBytes int, err error, steps []SQLExplainStep) {
	if observation.observer == nil {
		return
	}
	event := SQLQueryEvent{
		QueryID:       observation.id,
		ElapsedNanos:  time.Since(observation.started).Nanoseconds(),
		OutputRows:    outputRows,
		OutputColumns: outputColumns,
		ResultBytes:   resultBytes,
		OK:            err == nil,
	}
	event.Slow = observation.threshold > 0 && time.Duration(event.ElapsedNanos) >= observation.threshold
	if err != nil {
		event.Error = err.Error()
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			event.Canceled = true
			event.CancellationReason = err.Error()
		}
	}
	event.Operators = sqlQueryOperators(steps)
	observation.observer.ObserveSQLQuery(event)
}

func sqlQueryOperators(steps []SQLExplainStep) []SQLQueryOperator {
	operators := make([]SQLQueryOperator, 0, len(steps))
	for _, step := range steps {
		if step.ActualInputRows == nil || step.ActualOutputRows == nil || step.ElapsedNanos == nil {
			continue
		}
		operator := SQLQueryOperator{
			Node:         step.Node,
			InputRows:    *step.ActualInputRows,
			OutputRows:   *step.ActualOutputRows,
			ElapsedNanos: *step.ElapsedNanos,
		}
		if step.EstimatedRows != nil {
			estimate := *step.EstimatedRows
			operator.EstimatedRows = &estimate
		}
		if step.ActualInputBytes != nil {
			bytes := *step.ActualInputBytes
			operator.InputBytes = &bytes
		}
		if step.ActualOutputBytes != nil {
			bytes := *step.ActualOutputBytes
			operator.OutputBytes = &bytes
		}
		if step.EstimateErrorPercent != nil {
			percent := *step.EstimateErrorPercent
			operator.EstimateErrorPercent = &percent
		}
		operators = append(operators, operator)
	}
	return operators
}

// ExecuteSQLQueryParameters executes source with positional $1, $2, ...
// values supplied separately from SQL text.
func ExecuteSQLQueryParameters(ctx context.Context, source string, resolver SQLSourceResolver, parameters []interface{}, options SQLQueryOptions) (result SQLQueryResult, err error) {
	observation := newSQLQueryObservation(options)
	var operatorSteps []SQLExplainStep
	result.QueryID = observation.id
	defer func() { observation.finish(result, err, operatorSteps) }()
	release := lockSQLSnapshot(resolver)
	defer release()
	control, cancel, controlErr := newSQLExecutionControl(ctx, options)
	if controlErr != nil {
		return result, controlErr
	}
	defer cancel()
	if err = control.check(); err != nil {
		return result, err
	}
	query, parseErr := parseSQLQueryWithCache(source, parameters, options.PreparedCache)
	if parseErr != nil {
		return result, parseErr
	}
	if query.explain {
		result, err = explainSQLQuery(query, resolver, control)
		operatorSteps = result.Plan
		result.QueryID = observation.id
		return result, sqlRuntimeDiagnostic(err)
	}
	var metrics *sqlExecutionMetrics
	if observation.observer != nil || options.AdaptivePlanner != nil {
		metrics = &sqlExecutionMetrics{adaptive: options.AdaptivePlanner}
	}
	result, err = executeSQLQueryWithMetrics(query, resolver, nil, metrics, control)
	if metrics != nil {
		operatorSteps = metrics.steps
	}
	result.QueryID = observation.id
	return result, sqlRuntimeDiagnostic(err)
}

// ExecuteQueryParameters executes source with positional parameters.
func ExecuteQueryParameters(ctx context.Context, source string, resolver SourceResolver, parameters []interface{}, options QueryOptions) (QueryResult, error) {
	return ExecuteSQLQueryParameters(ctx, source, resolver, parameters, options)
}

var errSQLStreamLimitReached = fmt.Errorf("SQL stream limit reached")

// ExecuteSQLQueryRows evaluates a stream-compatible query and invokes visit as
// each projected row becomes available. It never builds a result-row slice.
// Queries requiring unsupported global state (grouping, windows, or set
// operations) return an explanatory error instead of silently falling back to
// materialized execution. Compatible indexed orders stream directly; finite
// direct-source orders use a bounded top-N heap; and an unbounded scalar direct
// source order can use bounded external spill runs when MaxSortBytes,
// SpillDirectory, and MaxSpillBytes are configured. A scalar direct-source
// DISTINCT query can similarly use bounded external set runs when MaxSetBytes
// is configured. Scalar custom functions are evaluated one source row at a
// time. A chain of indexed INNER/LEFT CACHE joins is also streamable because
// each next source is probed only for the current row. A direct typed CACHE
// source is also streamed after each row is validated and converted; it drains
// the source after LIMIT so declared-field diagnostics match materialization.
func ExecuteSQLQueryRows(ctx context.Context, source string, resolver SQLSourceResolver, parameters []interface{}, options SQLQueryOptions, visit func(columns []string, row SQLRow) error) (err error) {
	observation := newSQLQueryObservation(options)
	outputRows, outputColumns, resultBytes := 0, 0, 0
	defer func() {
		// A row callback may be backed by NDJSON, an SDK iterator, or an
		// application sink. It is the one operator common to every streamed
		// execution path, so record its exact completed payload counters even
		// when the lower-level path has no materialized plan steps to report.
		elapsed := time.Since(observation.started).Nanoseconds()
		steps := []SQLExplainStep{{
			Node:              "STREAM OUTPUT",
			ActualInputRows:   &outputRows,
			ActualOutputRows:  &outputRows,
			ActualOutputBytes: &resultBytes,
			ElapsedNanos:      &elapsed,
		}}
		observation.finishSummary(outputRows, outputColumns, resultBytes, err, steps)
	}()
	if visit == nil {
		return fmt.Errorf("SQL row callback is required")
	}
	release := lockSQLSnapshot(resolver)
	defer release()
	control, cancel, err := newSQLExecutionControl(ctx, options)
	if err != nil {
		return err
	}
	defer cancel()
	query, err := parseSQLQueryWithCache(source, parameters, options.PreparedCache)
	if err != nil {
		return err
	}
	outputColumns = len(sqlColumns(query.selects))
	return executeSQLQueryRowsParsed(ctx, query, resolver, control, func(columns []string, row SQLRow) error {
		if err := visit(columns, row); err != nil {
			return err
		}
		outputRows++
		resultBytes += sqlRowBytes(row)
		return nil
	})
}

// ExecuteQueryRows invokes visit for each stream-compatible result row.
func ExecuteQueryRows(ctx context.Context, source string, resolver SourceResolver, parameters []interface{}, options QueryOptions, visit func([]string, Row) error) error {
	return ExecuteSQLQueryRows(ctx, source, resolver, parameters, options, visit)
}

func executeSQLQueryRowsParsed(ctx context.Context, query *sqlQuery, resolver SQLSourceResolver, control *sqlExecutionControl, visit func(columns []string, row SQLRow) error) error {
	if sqlExternalSetStreamable(query, control) {
		return executeSQLExternalSetStream(ctx, query, resolver, control, visit)
	}
	if sqlUnionAllStreamable(query) {
		return executeSQLUnionAllStream(ctx, query, resolver, control, visit)
	}
	if sqlLeadWindowStreamable(query) {
		return executeSQLLeadWindowStream(ctx, query, resolver, control, visit)
	}
	if sqlRunningWindowStreamable(query) {
		return executeSQLRunningWindowStream(ctx, query, resolver, control, visit)
	}
	if projections, ok := sqlIndexedGroupStreamable(query, resolver); ok {
		return executeSQLIndexedGroupAggregateStream(ctx, query, resolver, control, visit, projections)
	}
	if _, handled, err := executeSQLStreamedSpilledGroupAggregate(query, resolver, control, nil, visit); handled {
		return err
	}
	if sqlIndexedDistinctStreamable(query, resolver) {
		return executeSQLIndexedDistinctStream(ctx, query, resolver, control, visit)
	}
	if sqlExternalDistinctStreamable(query, control) {
		return executeSQLExternalDistinctStream(ctx, query, resolver, control, visit)
	}
	if sqlIndexedRunningAggregateWindowStreamable(query, resolver) {
		return executeSQLIndexedRunningAggregateWindowStream(ctx, query, resolver, control, visit)
	}
	if sqlIndexedLeadWindowStreamable(query, resolver) {
		return executeSQLIndexedLeadWindowStream(ctx, query, resolver, control, visit)
	}
	if sqlIndexedLagWindowStreamable(query, resolver) {
		return executeSQLIndexedLagWindowStream(ctx, query, resolver, control, visit)
	}
	if sqlIndexedRankWindowStreamable(query, resolver) {
		return executeSQLIndexedRankWindowStream(ctx, query, resolver, control, visit)
	}
	if sqlIndexedOrderStreamable(query, resolver) {
		return executeSQLIndexedOrderStream(ctx, query, resolver, control, visit)
	}
	if sqlExternalSortStreamable(query, control) {
		return executeSQLExternalSortStream(ctx, query, resolver, control, visit)
	}
	if aggregates, ok := sqlGlobalStreamAggregates(query); ok {
		return executeSQLGlobalAggregateStream(ctx, query, resolver, control, visit, aggregates)
	}
	if sqlTopNStreamable(query) {
		return executeSQLTopNStream(ctx, query, resolver, control, visit)
	}
	if err := validateSQLQueryStreamable(query); err != nil {
		return err
	}
	functions, _ := resolver.(SQLFunctionResolver)
	if len(query.joins) > 0 {
		indexed, ok := resolver.(SQLIndexedSourceResolver)
		if !ok {
			return fmt.Errorf("SQL query cannot stream these joins because the resolver has no right-side index")
		}
		aliases := []string{query.from.alias}
		for _, join := range query.joins {
			_, _, rightField, _ := sqlHashJoinFields(join.on, aliases, join.source.alias)
			_, available, err := indexed.ResolveSQLIndexedSource(join.source.kind, join.source.key, rightField, nil)
			if err != nil {
				return sqlRuntimeDiagnostic(err)
			}
			if !available {
				return fmt.Errorf("SQL query cannot stream join %q because its right-side index is unavailable", join.source.alias)
			}
			aliases = append(aliases, join.source.alias)
		}
	}
	columns := sqlColumns(query.selects)
	// A typed source must still be fully validated even when LIMIT 0 would
	// otherwise let the stream stop before reading it. This retains the
	// materialized executor's promise that declared CACHE fields are checked
	// before relational evaluation.
	if query.limit == 0 && len(query.from.fieldTypes) == 0 {
		return nil
	}
	inputRows, expandedRows, seen, emitted, resultBytes := 0, 0, 0, 0, 0
	limitReached := query.limit == 0
	emitRow := func(execRow sqlExecRow) error {
		if err := control.check(); err != nil {
			return err
		}
		if query.where.kind != "" {
			value, err := evalSQLStreamExpr(query.where, execRow, functions)
			if err != nil {
				return err
			}
			if !sqlTruthy(value) {
				return nil
			}
		}
		seen++
		if seen <= query.offset {
			return nil
		}
		if query.limit >= 0 && emitted >= query.limit {
			return errSQLStreamLimitReached
		}
		row := SQLRow{}
		for index, item := range query.selects {
			value, err := evalSQLStreamExpr(item.expr, execRow, functions)
			if err != nil {
				return err
			}
			row[columns[index]] = value
		}
		if control.options.MaxResultBytes > 0 {
			resultBytes += sqlRowBytes(row)
			if resultBytes > control.options.MaxResultBytes {
				return fmt.Errorf("SQL result byte budget exceeded: maximum %d bytes", control.options.MaxResultBytes)
			}
		}
		emitted++
		if err := visit(columns, row); err != nil {
			return err
		}
		if query.limit >= 0 && emitted >= query.limit {
			return errSQLStreamLimitReached
		}
		return nil
	}
	var streamJoined func(int, sqlExecRow) error
	streamJoined = func(joinIndex int, left sqlExecRow) error {
		if joinIndex == len(query.joins) {
			expandedRows++
			if expandedRows > control.maxRows {
				return fmt.Errorf("SQL join exceeds the %d row limit; add a more selective WHERE or ON condition", control.maxRows)
			}
			return emitRow(left)
		}
		join := query.joins[joinIndex]
		aliases := make([]string, 1, joinIndex+1)
		aliases[0] = query.from.alias
		for index := 0; index < joinIndex; index++ {
			aliases = append(aliases, query.joins[index].source.alias)
		}
		leftQualifier, leftField, rightField, _ := sqlHashJoinFields(join.on, aliases, join.source.alias)
		if err := control.addJoinWork(1); err != nil {
			return err
		}
		indexed := resolver.(SQLIndexedSourceResolver)
		value := sqlField(left, leftQualifier, leftField)
		candidates, _, err := indexed.ResolveSQLIndexedSource(join.source.kind, join.source.key, rightField, value)
		if err != nil {
			return err
		}
		if len(candidates) == 0 && join.kind == "LEFT" {
			empty := sqlExecRow{sources: map[string]SQLRow{join.source.alias: {}}, order: []string{join.source.alias}}
			return streamJoined(joinIndex+1, mergeSQLRows(left, empty))
		}
		for _, candidate := range candidates {
			if err := control.addJoinWork(1); err != nil {
				return err
			}
			right := sqlExecRow{sources: map[string]SQLRow{join.source.alias: candidate}, order: []string{join.source.alias}}
			if err := streamJoined(joinIndex+1, mergeSQLRows(left, right)); err != nil {
				return err
			}
		}
		return nil
	}
	streamRow := func(sourceRow SQLRow) error {
		// Keep consuming a typed source after the result limit so a malformed
		// later declared field is reported exactly as in materialized execution.
		// streamSQLSourceRows performs that per-row validation before this call.
		if limitReached {
			return nil
		}
		inputRows++
		if inputRows > control.maxRows {
			return fmt.Errorf("SQL source %q exceeds the %d row limit", query.from.alias, control.maxRows)
		}
		left := sqlExecRow{sources: map[string]SQLRow{query.from.alias: sourceRow}, order: []string{query.from.alias}}
		err := streamJoined(0, left)
		if err == errSQLStreamLimitReached && len(query.from.fieldTypes) > 0 {
			limitReached = true
			return nil
		}
		return err
	}
	if err := streamSQLSourceRows(ctx, *query.from, resolver, streamRow); err != nil && err != errSQLStreamLimitReached {
		return sqlRuntimeDiagnostic(err)
	}
	return nil
}

func sqlExternalSetStreamable(query *sqlQuery, control *sqlExecutionControl) bool {
	if control == nil || control.options.MaxSetBytes <= 0 || strings.TrimSpace(control.options.SpillDirectory) == "" || control.options.MaxSpillBytes <= 0 || query == nil || query.explain || len(query.unions) == 0 {
		return false
	}
	_, ok := sqlExternalSetStreamColumns(query)
	return ok
}

// sqlExternalSetStreamColumns proves the recursively parsed, right-associated
// set tree can be evaluated through bounded spill stages. Each left operand is
// a base query and every right operand is either another stage or a base query.
func sqlExternalSetStreamColumns(query *sqlQuery) ([]string, bool) {
	if query == nil || query.explain {
		return nil, false
	}
	left := *query
	left.unions = nil
	if !sqlQueryRowsBaseStreamable(&left) {
		return nil, false
	}
	columns := sqlColumns(left.selects)
	if len(query.unions) == 0 {
		return columns, true
	}
	if len(query.unions) != 1 {
		return nil, false
	}
	union := query.unions[0]
	if union.all || (union.kind != "UNION" && union.kind != "INTERSECT" && union.kind != "EXCEPT") || union.query == nil {
		return nil, false
	}
	rightColumns, ok := sqlExternalSetStreamColumns(union.query)
	return columns, ok && sameSQLColumns(columns, rightColumns)
}

func sqlExternalSetHasNestedStage(query *sqlQuery) bool {
	return query != nil && len(query.unions) == 1 && query.unions[0].query != nil && len(query.unions[0].query.unions) != 0
}

func executeSQLExternalSetStream(ctx context.Context, query *sqlQuery, resolver SQLSourceResolver, control *sqlExecutionControl, visit func([]string, SQLRow) error) error {
	if sqlExternalSetHasNestedStage(query) {
		return executeSQLExternalSetChainStream(ctx, query, resolver, control, visit)
	}
	left := *query
	left.unions = nil
	union := query.unions[0]
	columns := sqlColumns(left.selects)
	available := int64(control.options.MaxSpillBytes)
	paths := map[string]struct{}{}
	defer func() {
		for path := range paths {
			_ = os.Remove(path)
		}
	}()
	runs := []sqlSpillRun{}
	chunk := []sqlSpillSetRecord{}
	chunkBytes, ordinal := 0, 0
	flush := func() error {
		if len(chunk) == 0 {
			return nil
		}
		sort.SliceStable(chunk, func(i, j int) bool { return sqlSpillSetRecordBefore(chunk[i], chunk[j]) })
		run, err := sqlWriteSpillSetRun(control.options.SpillDirectory, chunk, &available, control)
		if err != nil {
			return err
		}
		paths[run.path] = struct{}{}
		runs = append(runs, run)
		chunk, chunkBytes = []sqlSpillSetRecord{}, 0
		return nil
	}
	appendBranch := func(branch *sqlQuery, right bool) error {
		return executeSQLQueryRowsParsed(ctx, branch, resolver, control, func(branchColumns []string, row SQLRow) error {
			if !sameSQLColumns(columns, branchColumns) {
				return fmt.Errorf("%s queries must project the same column names in the same order", union.kind)
			}
			record := sqlSpillSetRecord{Key: sqlOutputRowKey(row), Row: row, Ordinal: ordinal, Right: right}
			ordinal++
			recordBytes := sqlSpillSetRecordBytes(record)
			if len(chunk) > 0 && chunkBytes+recordBytes > control.options.MaxSetBytes {
				if err := flush(); err != nil {
					return err
				}
			}
			chunk = append(chunk, record)
			chunkBytes += recordBytes
			return nil
		})
	}
	if err := appendBranch(&left, false); err != nil {
		return err
	}
	if err := appendBranch(union.query, true); err != nil {
		return err
	}
	if err := flush(); err != nil {
		return err
	}
	for len(runs) > maxSQLSpillMergeFanIn {
		next := make([]sqlSpillRun, 0, (len(runs)+maxSQLSpillMergeFanIn-1)/maxSQLSpillMergeFanIn)
		for start := 0; start < len(runs); start += maxSQLSpillMergeFanIn {
			end := start + maxSQLSpillMergeFanIn
			if end > len(runs) {
				end = len(runs)
			}
			merged, err := sqlMergeSpillSetRunsToRun(runs[start:end], control.options.SpillDirectory, &available, control)
			if err != nil {
				return err
			}
			paths[merged.path] = struct{}{}
			for _, run := range runs[start:end] {
				if err := os.Remove(run.path); err != nil && !errors.Is(err, os.ErrNotExist) {
					return fmt.Errorf("remove SQL set spill file: %w", err)
				}
				delete(paths, run.path)
				available += run.bytes
			}
			next = append(next, merged)
		}
		runs = next
	}
	readers, err := sqlOpenSpillSetReaders(runs)
	if err != nil {
		return err
	}
	defer closeSQLSpillSetReaders(readers)
	ordinalRuns := []sqlSpillRun{}
	ordinalChunk := []sqlSpillOutput{}
	ordinalBytes := 0
	ordinalOrder := []sqlOrder{{}}
	flushOrdinals := func() error {
		if len(ordinalChunk) == 0 {
			return nil
		}
		sort.SliceStable(ordinalChunk, func(i, j int) bool { return sqlSpillOutputLess(ordinalChunk[i], ordinalChunk[j], ordinalOrder) })
		run, err := sqlWriteSpillRun(control.options.SpillDirectory, ordinalChunk, &available, control)
		if err != nil {
			return err
		}
		paths[run.path] = struct{}{}
		ordinalRuns = append(ordinalRuns, run)
		ordinalChunk, ordinalBytes = []sqlSpillOutput{}, 0
		return nil
	}
	for {
		first, ok, err := sqlNextRawSpillSet(readers, control)
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		hasLeft, hasRight := !first.Right, first.Right
		firstLeft := first
		for {
			best := -1
			for index, reader := range readers {
				if reader.done || reader.current.Key != first.Key || best >= 0 && !sqlSpillSetRecordBefore(reader.current, readers[best].current) {
					continue
				}
				best = index
			}
			if best < 0 {
				break
			}
			record := readers[best].current
			if record.Right {
				hasRight = true
			} else if !hasLeft {
				hasLeft, firstLeft = true, record
			}
			if err := readers[best].next(); err != nil {
				return err
			}
		}
		selected := false
		switch union.kind {
		case "UNION":
			selected = true
		case "INTERSECT":
			selected = hasLeft && hasRight
		case "EXCEPT":
			selected = hasLeft && !hasRight
		}
		if !selected {
			continue
		}
		output := first
		if union.kind != "UNION" {
			output = firstLeft
		}
		record := sqlSpillOutput{Row: output.Row, Keys: []interface{}{int64(output.Ordinal)}, Ordinal: output.Ordinal}
		recordBytes := sqlSpillOutputBytes(record)
		if len(ordinalChunk) > 0 && ordinalBytes+recordBytes > control.options.MaxSetBytes {
			if err := flushOrdinals(); err != nil {
				return err
			}
		}
		ordinalChunk = append(ordinalChunk, record)
		ordinalBytes += recordBytes
	}
	if err := flushOrdinals(); err != nil {
		return err
	}
	closeSQLSpillSetReaders(readers)
	for _, run := range runs {
		if err := os.Remove(run.path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove SQL set spill file: %w", err)
		}
		delete(paths, run.path)
		available += run.bytes
	}
	for len(ordinalRuns) > maxSQLSpillMergeFanIn {
		next := make([]sqlSpillRun, 0, (len(ordinalRuns)+maxSQLSpillMergeFanIn-1)/maxSQLSpillMergeFanIn)
		for start := 0; start < len(ordinalRuns); start += maxSQLSpillMergeFanIn {
			end := start + maxSQLSpillMergeFanIn
			if end > len(ordinalRuns) {
				end = len(ordinalRuns)
			}
			merged, err := sqlMergeSpillRunsToWriter(ordinalRuns[start:end], ordinalOrder, control.options.SpillDirectory, &available, control)
			if err != nil {
				return err
			}
			paths[merged.path] = struct{}{}
			for _, run := range ordinalRuns[start:end] {
				if err := os.Remove(run.path); err != nil && !errors.Is(err, os.ErrNotExist) {
					return fmt.Errorf("remove SQL set ordinal spill file: %w", err)
				}
				delete(paths, run.path)
				available += run.bytes
			}
			next = append(next, merged)
		}
		ordinalRuns = next
	}
	position, emitted, resultBytes := 0, 0, 0
	err = sqlMergeSpillRunsToVisit(ordinalRuns, ordinalOrder, control, func(record sqlSpillOutput) error {
		if position >= query.offset {
			if query.limit >= 0 && emitted >= query.limit {
				return errSQLStreamLimitReached
			}
			if control.options.MaxResultBytes > 0 {
				resultBytes += sqlRowBytes(record.Row)
				if resultBytes > control.options.MaxResultBytes {
					return fmt.Errorf("SQL result byte budget exceeded: maximum %d bytes", control.options.MaxResultBytes)
				}
			}
			if err := visit(columns, record.Row); err != nil {
				return err
			}
			emitted++
		}
		position++
		return nil
	})
	if errors.Is(err, errSQLStreamLimitReached) {
		return nil
	}
	return err
}

// sqlSpillRowStream emits rows without retaining an input slice. It is used to
// connect external set stages: the output of one stage becomes the right input
// of its parent through ordinal-sorted spill runs.
type sqlSpillRowStream func(func(SQLRow) error) error

func executeSQLExternalSetChainStream(ctx context.Context, query *sqlQuery, resolver SQLSourceResolver, control *sqlExecutionControl, visit func([]string, SQLRow) error) error {
	available := int64(control.options.MaxSpillBytes)
	paths := map[string]struct{}{}
	defer func() {
		for path := range paths {
			_ = os.Remove(path)
		}
	}()
	runs, columns, err := sqlBuildExternalSetChainRuns(ctx, query, resolver, control, &available, paths)
	if err != nil {
		return err
	}
	return sqlVisitExternalSetOutputRuns(runs, columns, query.offset, query.limit, control, visit)
}

func sqlBuildExternalSetChainRuns(ctx context.Context, query *sqlQuery, resolver SQLSourceResolver, control *sqlExecutionControl, available *int64, paths map[string]struct{}) ([]sqlSpillRun, []string, error) {
	left := *query
	left.unions = nil
	columns := sqlColumns(left.selects)
	leftStream := func(emit func(SQLRow) error) error {
		return executeSQLQueryRowsParsed(ctx, &left, resolver, control, func(branchColumns []string, row SQLRow) error {
			if !sameSQLColumns(columns, branchColumns) {
				return fmt.Errorf("set queries must project the same column names in the same order")
			}
			return emit(row)
		})
	}
	if len(query.unions) == 0 {
		runs, err := sqlSpillExternalSetInput(leftStream, control, available, paths)
		return runs, columns, err
	}
	union := query.unions[0]
	rightRuns, rightColumns, err := sqlBuildExternalSetChainRuns(ctx, union.query, resolver, control, available, paths)
	if err != nil {
		return nil, nil, err
	}
	if !sameSQLColumns(columns, rightColumns) {
		return nil, nil, fmt.Errorf("%s queries must project the same column names in the same order", union.kind)
	}
	rightStream := func(emit func(SQLRow) error) error {
		return sqlMergeSpillRunsToVisit(rightRuns, []sqlOrder{{}}, control, func(record sqlSpillOutput) error {
			return emit(record.Row)
		})
	}
	cleanRight := func() error { return sqlReleaseSpillRuns(rightRuns, available, paths, "set stage input") }
	runs, err := sqlSpillExternalSetStage(leftStream, rightStream, cleanRight, union.kind, control, available, paths)
	if err != nil {
		return nil, nil, err
	}
	return runs, columns, nil
}

// sqlSpillExternalSetInput persists a base stream in ordinal order so it can
// be consumed by a parent set stage without a result-row slice.
func sqlSpillExternalSetInput(stream sqlSpillRowStream, control *sqlExecutionControl, available *int64, paths map[string]struct{}) ([]sqlSpillRun, error) {
	runs := []sqlSpillRun{}
	chunk := []sqlSpillOutput{}
	chunkBytes, ordinal := 0, 0
	flush := func() error {
		if len(chunk) == 0 {
			return nil
		}
		run, err := sqlWriteSpillRun(control.options.SpillDirectory, chunk, available, control)
		if err != nil {
			return err
		}
		paths[run.path] = struct{}{}
		runs = append(runs, run)
		chunk, chunkBytes = []sqlSpillOutput{}, 0
		return nil
	}
	if err := stream(func(row SQLRow) error {
		record := sqlSpillOutput{Row: row, Keys: []interface{}{int64(ordinal)}, Ordinal: ordinal}
		ordinal++
		recordBytes := sqlSpillOutputBytes(record)
		if len(chunk) > 0 && chunkBytes+recordBytes > control.options.MaxSetBytes {
			if err := flush(); err != nil {
				return err
			}
		}
		chunk = append(chunk, record)
		chunkBytes += recordBytes
		return nil
	}); err != nil {
		return nil, err
	}
	if err := flush(); err != nil {
		return nil, err
	}
	return runs, nil
}

// sqlSpillExternalSetStage creates one bounded binary set stage. The right
// stream may be a nested stage; its input runs are released immediately after
// their records have been absorbed, before this stage's merge needs disk.
func sqlSpillExternalSetStage(left, right sqlSpillRowStream, releaseRight func() error, operation string, control *sqlExecutionControl, available *int64, paths map[string]struct{}) ([]sqlSpillRun, error) {
	runs := []sqlSpillRun{}
	chunk := []sqlSpillSetRecord{}
	chunkBytes, ordinal := 0, 0
	flush := func() error {
		if len(chunk) == 0 {
			return nil
		}
		sort.SliceStable(chunk, func(i, j int) bool { return sqlSpillSetRecordBefore(chunk[i], chunk[j]) })
		run, err := sqlWriteSpillSetRun(control.options.SpillDirectory, chunk, available, control)
		if err != nil {
			return err
		}
		paths[run.path] = struct{}{}
		runs = append(runs, run)
		chunk, chunkBytes = []sqlSpillSetRecord{}, 0
		return nil
	}
	appendStream := func(stream sqlSpillRowStream, rightSide bool) error {
		return stream(func(row SQLRow) error {
			record := sqlSpillSetRecord{Key: sqlOutputRowKey(row), Row: row, Ordinal: ordinal, Right: rightSide}
			ordinal++
			recordBytes := sqlSpillSetRecordBytes(record)
			if len(chunk) > 0 && chunkBytes+recordBytes > control.options.MaxSetBytes {
				if err := flush(); err != nil {
					return err
				}
			}
			chunk = append(chunk, record)
			chunkBytes += recordBytes
			return nil
		})
	}
	if err := appendStream(left, false); err != nil {
		return nil, err
	}
	if err := appendStream(right, true); err != nil {
		return nil, err
	}
	if err := flush(); err != nil {
		return nil, err
	}
	if releaseRight != nil {
		if err := releaseRight(); err != nil {
			return nil, err
		}
	}
	for len(runs) > maxSQLSpillMergeFanIn {
		next := make([]sqlSpillRun, 0, (len(runs)+maxSQLSpillMergeFanIn-1)/maxSQLSpillMergeFanIn)
		for start := 0; start < len(runs); start += maxSQLSpillMergeFanIn {
			end := start + maxSQLSpillMergeFanIn
			if end > len(runs) {
				end = len(runs)
			}
			merged, err := sqlMergeSpillSetRunsToRun(runs[start:end], control.options.SpillDirectory, available, control)
			if err != nil {
				return nil, err
			}
			paths[merged.path] = struct{}{}
			if err := sqlReleaseSpillRuns(runs[start:end], available, paths, "set spill file"); err != nil {
				return nil, err
			}
			next = append(next, merged)
		}
		runs = next
	}
	readers, err := sqlOpenSpillSetReaders(runs)
	if err != nil {
		return nil, err
	}
	defer closeSQLSpillSetReaders(readers)
	ordinalRuns := []sqlSpillRun{}
	ordinalChunk := []sqlSpillOutput{}
	ordinalBytes := 0
	ordinalOrder := []sqlOrder{{}}
	flushOrdinals := func() error {
		if len(ordinalChunk) == 0 {
			return nil
		}
		sort.SliceStable(ordinalChunk, func(i, j int) bool { return sqlSpillOutputLess(ordinalChunk[i], ordinalChunk[j], ordinalOrder) })
		run, err := sqlWriteSpillRun(control.options.SpillDirectory, ordinalChunk, available, control)
		if err != nil {
			return err
		}
		paths[run.path] = struct{}{}
		ordinalRuns = append(ordinalRuns, run)
		ordinalChunk, ordinalBytes = []sqlSpillOutput{}, 0
		return nil
	}
	for {
		first, ok, err := sqlNextRawSpillSet(readers, control)
		if err != nil || !ok {
			if err != nil {
				return nil, err
			}
			break
		}
		hasLeft, hasRight := !first.Right, first.Right
		firstLeft := first
		for {
			best := -1
			for index, reader := range readers {
				if reader.done || reader.current.Key != first.Key || best >= 0 && !sqlSpillSetRecordBefore(reader.current, readers[best].current) {
					continue
				}
				best = index
			}
			if best < 0 {
				break
			}
			record := readers[best].current
			if record.Right {
				hasRight = true
			} else if !hasLeft {
				hasLeft, firstLeft = true, record
			}
			if err := readers[best].next(); err != nil {
				return nil, err
			}
		}
		selected := operation == "UNION" || operation == "INTERSECT" && hasLeft && hasRight || operation == "EXCEPT" && hasLeft && !hasRight
		if !selected {
			continue
		}
		output := first
		if operation != "UNION" {
			output = firstLeft
		}
		record := sqlSpillOutput{Row: output.Row, Keys: []interface{}{int64(output.Ordinal)}, Ordinal: output.Ordinal}
		recordBytes := sqlSpillOutputBytes(record)
		if len(ordinalChunk) > 0 && ordinalBytes+recordBytes > control.options.MaxSetBytes {
			if err := flushOrdinals(); err != nil {
				return nil, err
			}
		}
		ordinalChunk = append(ordinalChunk, record)
		ordinalBytes += recordBytes
	}
	if err := flushOrdinals(); err != nil {
		return nil, err
	}
	closeSQLSpillSetReaders(readers)
	if err := sqlReleaseSpillRuns(runs, available, paths, "set spill file"); err != nil {
		return nil, err
	}
	for len(ordinalRuns) > maxSQLSpillMergeFanIn {
		next := make([]sqlSpillRun, 0, (len(ordinalRuns)+maxSQLSpillMergeFanIn-1)/maxSQLSpillMergeFanIn)
		for start := 0; start < len(ordinalRuns); start += maxSQLSpillMergeFanIn {
			end := start + maxSQLSpillMergeFanIn
			if end > len(ordinalRuns) {
				end = len(ordinalRuns)
			}
			merged, err := sqlMergeSpillRunsToWriter(ordinalRuns[start:end], ordinalOrder, control.options.SpillDirectory, available, control)
			if err != nil {
				return nil, err
			}
			paths[merged.path] = struct{}{}
			if err := sqlReleaseSpillRuns(ordinalRuns[start:end], available, paths, "set ordinal spill file"); err != nil {
				return nil, err
			}
			next = append(next, merged)
		}
		ordinalRuns = next
	}
	return ordinalRuns, nil
}

func sqlReleaseSpillRuns(runs []sqlSpillRun, available *int64, paths map[string]struct{}, label string) error {
	for _, run := range runs {
		if err := os.Remove(run.path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove SQL %s: %w", label, err)
		}
		delete(paths, run.path)
		*available += run.bytes
	}
	return nil
}

func sqlVisitExternalSetOutputRuns(runs []sqlSpillRun, columns []string, offset, limit int, control *sqlExecutionControl, visit func([]string, SQLRow) error) error {
	position, emitted, resultBytes := 0, 0, 0
	err := sqlMergeSpillRunsToVisit(runs, []sqlOrder{{}}, control, func(record sqlSpillOutput) error {
		if position >= offset {
			if limit >= 0 && emitted >= limit {
				return errSQLStreamLimitReached
			}
			if control.options.MaxResultBytes > 0 {
				resultBytes += sqlRowBytes(record.Row)
				if resultBytes > control.options.MaxResultBytes {
					return fmt.Errorf("SQL result byte budget exceeded: maximum %d bytes", control.options.MaxResultBytes)
				}
			}
			if err := visit(columns, record.Row); err != nil {
				return err
			}
			emitted++
		}
		position++
		return nil
	})
	if errors.Is(err, errSQLStreamLimitReached) {
		return nil
	}
	return err
}

// evalSQLStreamExpr evaluates one source row through the same vectorized
// expression implementation as materialized execution. A one-row batch keeps
// custom functions bounded while preserving their typed errors and CASE's lazy
// branch semantics.
func evalSQLStreamExpr(expr sqlExpr, row sqlExecRow, functions SQLFunctionResolver) (interface{}, error) {
	values, err := evalSQLExprBatch(expr, []sqlExecRow{row}, functions)
	if err != nil {
		return nil, err
	}
	if len(values) != 1 {
		return nil, fmt.Errorf("SQL expression produced %d values for one streamed row", len(values))
	}
	if err := sqlExpressionError(values[0]); err != nil {
		return nil, err
	}
	return values[0], nil
}

func sqlQueryRowsBaseStreamable(query *sqlQuery) bool {
	if query == nil || query.explain || len(query.unions) != 0 {
		return false
	}
	if _, ok := sqlGlobalStreamAggregates(query); ok || sqlTopNStreamable(query) {
		return true
	}
	return validateSQLQueryStreamable(query) == nil
}

func sqlUnionAllStreamColumns(query *sqlQuery) ([]string, bool) {
	if query == nil || query.explain {
		return nil, false
	}
	base := *query
	base.unions = nil
	if !sqlQueryRowsBaseStreamable(&base) {
		return nil, false
	}
	columns := sqlColumns(base.selects)
	for _, union := range query.unions {
		if union.kind != "UNION" || !union.all || union.query == nil {
			return nil, false
		}
		rightColumns, ok := sqlUnionAllStreamColumns(union.query)
		if !ok || !sameSQLColumns(columns, rightColumns) {
			return nil, false
		}
	}
	return columns, true
}

// sqlUnionAllStreamable proves the one set operation that needs no global
// membership state. Each branch must be independently streamable and project
// the same columns, matching the materialized executor's contract.
func sqlUnionAllStreamable(query *sqlQuery) bool {
	if query == nil || len(query.unions) == 0 {
		return false
	}
	_, ok := sqlUnionAllStreamColumns(query)
	return ok
}

func executeSQLUnionAllStream(ctx context.Context, query *sqlQuery, resolver SQLSourceResolver, control *sqlExecutionControl, visit func(columns []string, row SQLRow) error) error {
	columns, ok := sqlUnionAllStreamColumns(query)
	if !ok {
		return fmt.Errorf("SQL query cannot stream these set operations because they require materialized membership state")
	}
	emit := func(branchColumns []string, row SQLRow) error {
		if !sameSQLColumns(columns, branchColumns) {
			return fmt.Errorf("UNION queries must project the same column names in the same order")
		}
		return visit(columns, row)
	}
	left := *query
	left.unions = nil
	if err := executeSQLQueryRowsParsed(ctx, &left, resolver, control, emit); err != nil {
		return err
	}
	for _, union := range query.unions {
		if err := executeSQLQueryRowsParsed(ctx, union.query, resolver, control, emit); err != nil {
			return err
		}
	}
	return nil
}

type sqlRunningWindowState struct {
	name        string
	arg         sqlExpr
	count       int64
	sum         float64
	min         float64
	max         float64
	seen        bool
	lagOffset   int
	lagDefault  *sqlExpr
	lagValues   []interface{}
	lagPosition int
	lagSeen     int
}

// sqlRunningWindowStreamable recognizes unpartitioned, unordered windows whose
// default frame is all qualifying preceding rows through the current row, plus
// LAG with a literal offset. They need only constant state or a fixed history,
// never one retained state object per source row.
func sqlRunningWindowStreamable(query *sqlQuery) bool {
	if query == nil || query.explain || query.from == nil || len(query.ctes) != 0 || len(query.unions) != 0 || len(query.joins) != 0 || len(query.groupBy) != 0 || query.having.kind != "" || query.distinct || len(query.orderBy) != 0 || len(query.from.fieldTypes) != 0 || query.from.kind != "CACHE" && query.from.kind != "VALUES" || sqlExprHasWindow(query.where) || sqlExprHasCustomFunction(query.where, nil) {
		return false
	}
	hasWindow := false
	for _, item := range query.selects {
		expr := item.expr
		if expr.window == nil {
			if expr.kind == "star" || sqlExprHasAggregate(expr) || sqlExprHasWindow(expr) || sqlExprHasCustomFunction(expr, nil) {
				return false
			}
			continue
		}
		hasWindow = true
		if expr.kind != "func" || len(expr.window.partition) != 0 || len(expr.window.order) != 0 || expr.window.frame != nil {
			return false
		}
		switch strings.ToUpper(expr.name) {
		case "ROW_NUMBER", "RANK", "DENSE_RANK":
			if len(expr.args) != 0 {
				return false
			}
		case "SUM", "AVG", "MIN", "MAX":
			if len(expr.args) != 1 || sqlExprHasAggregate(expr.args[0]) || sqlExprHasWindow(expr.args[0]) || sqlExprHasCustomFunction(expr.args[0], nil) {
				return false
			}
		case "LAG":
			if _, ok := sqlRunningWindowLagOffset(expr.args); !ok {
				return false
			}
			for _, argument := range expr.args {
				if sqlExprHasAggregate(argument) || sqlExprHasWindow(argument) || sqlExprHasCustomFunction(argument, nil) {
					return false
				}
			}
		default:
			return false
		}
	}
	return hasWindow
}

// sqlRunningWindowLagOffset accepts only a literal offset because streamed LAG
// needs a fixed-size history. Materialized execution remains available for
// dynamic offsets and for LEAD, which requires future rows.
func sqlRunningWindowLagOffset(args []sqlExpr) (int, bool) {
	if len(args) < 1 || len(args) > 3 {
		return 0, false
	}
	if len(args) == 1 {
		return 1, true
	}
	if args[1].kind != "literal" {
		return 0, false
	}
	offset, ok := sqlNumber(args[1].value)
	maxInt := int(^uint(0) >> 1)
	if !ok || offset < 0 || offset >= float64(maxInt) || offset != float64(int(offset)) {
		return 0, false
	}
	return int(offset), true
}

// sqlLeadWindowStreamable recognizes the subset that can delay each output by
// a fixed literal offset. Unlike running windows, LEAD needs future input, but
// the pending queue is bounded by the largest requested offset.
func sqlLeadWindowStreamable(query *sqlQuery) bool {
	if query == nil || query.explain || query.from == nil || len(query.ctes) != 0 || len(query.unions) != 0 || len(query.joins) != 0 || len(query.groupBy) != 0 || query.having.kind != "" || query.distinct || len(query.orderBy) != 0 || len(query.from.fieldTypes) != 0 || query.from.kind != "CACHE" && query.from.kind != "VALUES" || sqlExprHasWindow(query.where) || sqlExprHasCustomFunction(query.where, nil) {
		return false
	}
	hasLead := false
	for _, item := range query.selects {
		expr := item.expr
		if expr.window == nil {
			if expr.kind == "star" || sqlExprHasAggregate(expr) || sqlExprHasWindow(expr) || sqlExprHasCustomFunction(expr, nil) {
				return false
			}
			continue
		}
		if expr.kind != "func" || strings.ToUpper(expr.name) != "LEAD" || len(expr.window.partition) != 0 || len(expr.window.order) != 0 || expr.window.frame != nil {
			return false
		}
		if _, ok := sqlRunningWindowLagOffset(expr.args); !ok {
			return false
		}
		for _, argument := range expr.args {
			if sqlExprHasAggregate(argument) || sqlExprHasWindow(argument) || sqlExprHasCustomFunction(argument, nil) {
				return false
			}
		}
		hasLead = true
	}
	return hasLead
}

type sqlLeadWindowDefinition struct {
	column   int
	argument sqlExpr
	offset   int
	fallback *sqlExpr
}

// executeSQLLeadWindowStream retains only qualifying rows whose fixed-offset
// future value has not arrived. The pending queue is therefore bounded by the
// largest literal LEAD offset rather than the source cardinality.
func executeSQLLeadWindowStream(ctx context.Context, query *sqlQuery, resolver SQLSourceResolver, control *sqlExecutionControl, visit func(columns []string, row SQLRow) error) error {
	columns := sqlColumns(query.selects)
	definitions := make(map[int]sqlLeadWindowDefinition)
	maximumOffset := 0
	for column, item := range query.selects {
		if item.expr.window == nil {
			continue
		}
		offset, _ := sqlRunningWindowLagOffset(item.expr.args)
		definition := sqlLeadWindowDefinition{column: column, argument: item.expr.args[0], offset: offset}
		if len(item.expr.args) == 3 {
			fallback := item.expr.args[2]
			definition.fallback = &fallback
		}
		definitions[column] = definition
		if offset > maximumOffset {
			maximumOffset = offset
		}
	}
	if query.limit == 0 {
		return nil
	}
	inputRows, seen, emitted, resultBytes := 0, 0, 0, 0
	// Do not preallocate from SQL text: a valid but enormous literal offset must
	// still be harmless when the bounded source is small.
	pending := []sqlExecRow{}
	emitAvailable := func(final bool) error {
		for len(pending) > 0 && (final || len(pending) > maximumOffset) {
			current := pending[0]
			row := SQLRow{}
			for column, item := range query.selects {
				if definition, ok := definitions[column]; ok {
					value := interface{}(nil)
					if definition.offset < len(pending) {
						target := pending[definition.offset]
						value = evalSQLExpr(definition.argument, []sqlExecRow{target}, target)
					} else if definition.fallback != nil {
						value = evalSQLExpr(*definition.fallback, []sqlExecRow{current}, current)
					}
					if err := sqlExpressionError(value); err != nil {
						return err
					}
					row[columns[column]] = value
					continue
				}
				value := evalSQLExpr(item.expr, []sqlExecRow{current}, current)
				if err := sqlExpressionError(value); err != nil {
					return err
				}
				row[columns[column]] = value
			}
			pending[0] = sqlExecRow{}
			pending = pending[1:]
			seen++
			if seen <= query.offset {
				continue
			}
			if query.limit >= 0 && emitted >= query.limit {
				return errSQLStreamLimitReached
			}
			if control.options.MaxResultBytes > 0 {
				resultBytes += sqlRowBytes(row)
				if resultBytes > control.options.MaxResultBytes {
					return fmt.Errorf("SQL result byte budget exceeded: maximum %d bytes", control.options.MaxResultBytes)
				}
			}
			emitted++
			if err := visit(columns, row); err != nil {
				return err
			}
			if query.limit >= 0 && emitted >= query.limit {
				return errSQLStreamLimitReached
			}
		}
		return nil
	}
	err := streamSQLSourceRows(ctx, *query.from, resolver, func(sourceRow SQLRow) error {
		if err := control.check(); err != nil {
			return err
		}
		inputRows++
		if inputRows > control.maxRows {
			return fmt.Errorf("SQL source %q exceeds the %d row limit", query.from.alias, control.maxRows)
		}
		current := sqlExecRow{sources: map[string]SQLRow{query.from.alias: sourceRow}, order: []string{query.from.alias}}
		if query.where.kind != "" {
			value := evalSQLExpr(query.where, []sqlExecRow{current}, current)
			if err := sqlExpressionError(value); err != nil {
				return err
			}
			if !sqlTruthy(value) {
				return nil
			}
		}
		pending = append(pending, current)
		return emitAvailable(false)
	})
	if err != nil && err != errSQLStreamLimitReached {
		return sqlRuntimeDiagnostic(err)
	}
	if err == errSQLStreamLimitReached {
		return nil
	}
	if err := emitAvailable(true); err != nil && err != errSQLStreamLimitReached {
		return sqlRuntimeDiagnostic(err)
	}
	return nil
}

func (state *sqlRunningWindowState) add(row sqlExecRow) (interface{}, error) {
	switch state.name {
	case "ROW_NUMBER":
		state.count++
		return state.count, nil
	case "RANK", "DENSE_RANK":
		// With no ORDER BY every row is tied, so both ranks are one.
		return int64(1), nil
	case "LAG":
		value := evalSQLExpr(state.arg, []sqlExecRow{row}, row)
		if err := sqlExpressionError(value); err != nil {
			return nil, err
		}
		defaultValue := interface{}(nil)
		if state.lagDefault != nil {
			defaultValue = evalSQLExpr(*state.lagDefault, []sqlExecRow{row}, row)
			if err := sqlExpressionError(defaultValue); err != nil {
				return nil, err
			}
		}
		result := defaultValue
		if state.lagOffset == 0 {
			result = value
		} else if state.lagSeen >= state.lagOffset {
			result = state.lagValues[state.lagPosition]
		}
		if len(state.lagValues) > 0 {
			state.lagValues[state.lagPosition] = value
			state.lagPosition = (state.lagPosition + 1) % len(state.lagValues)
		}
		state.lagSeen++
		return result, nil
	}
	value := evalSQLExpr(state.arg, []sqlExecRow{row}, row)
	if err := sqlExpressionError(value); err != nil {
		return nil, err
	}
	number, ok := sqlNumber(value)
	if !ok {
		return state.value(), nil
	}
	if !state.seen {
		state.seen = true
		state.count = 1
		state.sum, state.min, state.max = number, number, number
		return state.value(), nil
	}
	state.count++
	switch state.name {
	case "SUM", "AVG":
		state.sum += number
	case "MIN":
		if number < state.min {
			state.min = number
		}
	case "MAX":
		if number > state.max {
			state.max = number
		}
	}
	return state.value(), nil
}

func (state sqlRunningWindowState) value() interface{} {
	if !state.seen {
		return nil
	}
	switch state.name {
	case "SUM":
		return state.sum
	case "AVG":
		return state.sum / float64(state.count)
	case "MIN":
		return state.min
	case "MAX":
		return state.max
	}
	return nil
}

func executeSQLRunningWindowStream(ctx context.Context, query *sqlQuery, resolver SQLSourceResolver, control *sqlExecutionControl, visit func(columns []string, row SQLRow) error) error {
	columns := sqlColumns(query.selects)
	states := make([]*sqlRunningWindowState, len(query.selects))
	for index, item := range query.selects {
		if item.expr.window == nil {
			continue
		}
		state := &sqlRunningWindowState{name: strings.ToUpper(item.expr.name)}
		if len(item.expr.args) == 1 {
			state.arg = item.expr.args[0]
		}
		if state.name == "LAG" {
			offset, _ := sqlRunningWindowLagOffset(item.expr.args)
			state.arg = item.expr.args[0]
			state.lagOffset = offset
			if len(item.expr.args) == 3 {
				defaultExpr := item.expr.args[2]
				state.lagDefault = &defaultExpr
			}
			if offset > 0 && offset < control.maxRows {
				state.lagValues = make([]interface{}, offset)
			}
		}
		states[index] = state
	}
	if query.limit == 0 {
		return nil
	}
	inputRows, seen, emitted, resultBytes := 0, 0, 0, 0
	err := streamSQLSourceRows(ctx, *query.from, resolver, func(sourceRow SQLRow) error {
		if err := control.check(); err != nil {
			return err
		}
		inputRows++
		if inputRows > control.maxRows {
			return fmt.Errorf("SQL source %q exceeds the %d row limit", query.from.alias, control.maxRows)
		}
		execRow := sqlExecRow{sources: map[string]SQLRow{query.from.alias: sourceRow}, order: []string{query.from.alias}}
		if query.where.kind != "" {
			value := evalSQLExpr(query.where, []sqlExecRow{execRow}, execRow)
			if err := sqlExpressionError(value); err != nil {
				return err
			}
			if !sqlTruthy(value) {
				return nil
			}
		}
		row := SQLRow{}
		for index, item := range query.selects {
			if states[index] != nil {
				value, err := states[index].add(execRow)
				if err != nil {
					return err
				}
				row[columns[index]] = value
				continue
			}
			value := evalSQLExpr(item.expr, []sqlExecRow{execRow}, execRow)
			if err := sqlExpressionError(value); err != nil {
				return err
			}
			row[columns[index]] = value
		}
		seen++
		if seen <= query.offset {
			return nil
		}
		if query.limit >= 0 && emitted >= query.limit {
			return errSQLStreamLimitReached
		}
		if control.options.MaxResultBytes > 0 {
			resultBytes += sqlRowBytes(row)
			if resultBytes > control.options.MaxResultBytes {
				return fmt.Errorf("SQL result byte budget exceeded: maximum %d bytes", control.options.MaxResultBytes)
			}
		}
		emitted++
		if err := visit(columns, row); err != nil {
			return err
		}
		if query.limit >= 0 && emitted >= query.limit {
			return errSQLStreamLimitReached
		}
		return nil
	})
	if err != nil && err != errSQLStreamLimitReached {
		return sqlRuntimeDiagnostic(err)
	}
	return nil
}

type sqlTopNStreamItem struct {
	row     SQLRow
	keys    []interface{}
	ordinal int
}

func sqlTopNStreamBefore(left, right sqlTopNStreamItem, order []sqlOrder) bool {
	for index, item := range order {
		if less, decided := sqlOrderLess(item, left.keys[index], right.keys[index]); decided {
			return less
		}
	}
	return left.ordinal < right.ordinal
}

// sqlTopNStreamHeap keeps the worst retained row at index zero. It therefore
// consumes O(LIMIT + OFFSET) memory rather than materializing every source row.
type sqlTopNStreamHeap struct {
	items []sqlTopNStreamItem
	order []sqlOrder
}

func (heap sqlTopNStreamHeap) Len() int { return len(heap.items) }
func (heap sqlTopNStreamHeap) Less(left, right int) bool {
	return sqlTopNStreamBefore(heap.items[right], heap.items[left], heap.order)
}
func (heap sqlTopNStreamHeap) Swap(left, right int) {
	heap.items[left], heap.items[right] = heap.items[right], heap.items[left]
}
func (heap *sqlTopNStreamHeap) Push(value interface{}) {
	heap.items = append(heap.items, value.(sqlTopNStreamItem))
}
func (heap *sqlTopNStreamHeap) Pop() interface{} {
	last := len(heap.items) - 1
	value := heap.items[last]
	heap.items = heap.items[:last]
	return value
}

// sqlTopNStreamable recognizes only the bounded-order subset whose projection
// and ORDER BY expressions can be evaluated per source row. Unsupported SQL
// retains the existing diagnostic instead of silently materializing.
func sqlTopNStreamable(query *sqlQuery) bool {
	if query == nil || query.explain || query.from == nil || query.limit < 0 || len(query.orderBy) == 0 || len(query.ctes) != 0 || len(query.unions) != 0 || len(query.joins) != 0 || len(query.groupBy) != 0 || query.having.kind != "" || query.distinct || sqlQueryHasAggregate(query) || sqlQueryHasWindow(query) || len(query.from.fieldTypes) != 0 || query.from.kind != "CACHE" && query.from.kind != "VALUES" || sqlExprHasWindow(query.where) || sqlExprHasCustomFunction(query.where, nil) {
		return false
	}
	for _, selectItem := range query.selects {
		if selectItem.expr.kind == "star" || sqlExprHasAggregate(selectItem.expr) || sqlExprHasWindow(selectItem.expr) || sqlExprHasCustomFunction(selectItem.expr, nil) {
			return false
		}
	}
	for _, order := range query.orderBy {
		if sqlExprHasAggregate(order.expr) || sqlExprHasWindow(order.expr) || sqlExprHasCustomFunction(order.expr, nil) {
			return false
		}
	}
	return true
}

func sqlTopNStreamCapacity(query *sqlQuery, maxRows int) int {
	if query.limit <= 0 || maxRows <= 0 {
		return 0
	}
	if query.offset >= maxRows || query.limit > maxRows-query.offset {
		return maxRows
	}
	return query.limit + query.offset
}

func executeSQLTopNStream(ctx context.Context, query *sqlQuery, resolver SQLSourceResolver, control *sqlExecutionControl, visit func([]string, SQLRow) error) error {
	if query.limit == 0 {
		return nil
	}
	capacity := sqlTopNStreamCapacity(query, control.maxRows)
	candidates := sqlTopNStreamHeap{items: make([]sqlTopNStreamItem, 0, capacity), order: query.orderBy}
	heap.Init(&candidates)
	columns := sqlColumns(query.selects)
	inputRows := 0
	ordinal := 0
	err := streamSQLSourceRows(ctx, *query.from, resolver, func(sourceRow SQLRow) error {
		if err := control.check(); err != nil {
			return err
		}
		inputRows++
		if inputRows > control.maxRows {
			return fmt.Errorf("SQL source %q exceeds the %d row limit", query.from.alias, control.maxRows)
		}
		execRow := sqlExecRow{sources: map[string]SQLRow{query.from.alias: sourceRow}, order: []string{query.from.alias}}
		if query.where.kind != "" {
			value := evalSQLExpr(query.where, nil, execRow)
			if err := sqlExpressionError(value); err != nil {
				return err
			}
			if !sqlTruthy(value) {
				return nil
			}
		}
		row := SQLRow{}
		for index, item := range query.selects {
			value := evalSQLExpr(item.expr, nil, execRow)
			if err := sqlExpressionError(value); err != nil {
				return err
			}
			row[columns[index]] = value
		}
		candidate := sqlTopNStreamItem{row: row, keys: make([]interface{}, len(query.orderBy)), ordinal: ordinal}
		ordinal++
		for index, order := range query.orderBy {
			value := evalOutputOrder(order.expr, row, []sqlExecRow{execRow})
			if err := sqlExpressionError(value); err != nil {
				return err
			}
			candidate.keys[index] = value
		}
		if candidates.Len() < capacity {
			heap.Push(&candidates, candidate)
			return nil
		}
		if sqlTopNStreamBefore(candidate, candidates.items[0], query.orderBy) {
			candidates.items[0] = candidate
			heap.Fix(&candidates, 0)
		}
		return nil
	})
	if err != nil {
		return sqlRuntimeDiagnostic(err)
	}
	sort.SliceStable(candidates.items, func(left, right int) bool {
		return sqlTopNStreamBefore(candidates.items[left], candidates.items[right], query.orderBy)
	})
	start := query.offset
	if start > len(candidates.items) {
		start = len(candidates.items)
	}
	end := start + query.limit
	if end > len(candidates.items) {
		end = len(candidates.items)
	}
	resultBytes := 0
	for _, candidate := range candidates.items[start:end] {
		if err := control.check(); err != nil {
			return err
		}
		if control.options.MaxResultBytes > 0 {
			resultBytes += sqlRowBytes(candidate.row)
			if resultBytes > control.options.MaxResultBytes {
				return fmt.Errorf("SQL result byte budget exceeded: maximum %d bytes", control.options.MaxResultBytes)
			}
		}
		if err := visit(columns, candidate.row); err != nil {
			return err
		}
	}
	return nil
}

// sqlExternalSortStreamable recognizes an unbounded direct-source ORDER BY
// that can spill sorted projected records and merge them into QueryRows. A
// bounded ORDER BY stays on the cheaper in-memory top-N path, while indexed
// orders remain a direct scan.
func sqlExternalSortStreamable(query *sqlQuery, control *sqlExecutionControl) bool {
	if control == nil || control.options.MaxSortBytes <= 0 || strings.TrimSpace(control.options.SpillDirectory) == "" || control.options.MaxSpillBytes <= 0 || query == nil || query.explain || query.from == nil || query.limit >= 0 || len(query.orderBy) == 0 || len(query.ctes) != 0 || len(query.unions) != 0 || len(query.joins) != 0 || len(query.groupBy) != 0 || query.having.kind != "" || query.distinct || sqlQueryHasAggregate(query) || sqlQueryHasWindow(query) || len(query.from.fieldTypes) != 0 || query.from.kind != "CACHE" && query.from.kind != "VALUES" || sqlExprHasWindow(query.where) {
		return false
	}
	for _, selectItem := range query.selects {
		if selectItem.expr.kind == "star" || sqlExprHasAggregate(selectItem.expr) || sqlExprHasWindow(selectItem.expr) {
			return false
		}
	}
	for _, order := range query.orderBy {
		if sqlExprHasAggregate(order.expr) || sqlExprHasWindow(order.expr) || sqlExprHasCustomFunction(order.expr, nil) {
			return false
		}
	}
	return true
}

// executeSQLExternalSortStream keeps only one bounded input run in memory and
// emits the final merge directly to the row callback. It supports the same
// scalar projection/filter expressions as the ordinary stream, including
// registered scalar functions evaluated one row at a time. Custom ORDER BY
// keys remain excluded until alias/order evaluation has the same proof.
func executeSQLExternalSortStream(ctx context.Context, query *sqlQuery, resolver SQLSourceResolver, control *sqlExecutionControl, visit func([]string, SQLRow) error) error {
	columns := sqlColumns(query.selects)
	functions, _ := resolver.(SQLFunctionResolver)
	available := int64(control.options.MaxSpillBytes)
	allPaths := map[string]struct{}{}
	defer func() {
		for path := range allPaths {
			_ = os.Remove(path)
		}
	}()
	runs := []sqlSpillRun{}
	chunk := make([]sqlSpillOutput, 0)
	chunkBytes := 0
	flush := func() error {
		if len(chunk) == 0 {
			return nil
		}
		sort.SliceStable(chunk, func(left, right int) bool { return sqlSpillOutputLess(chunk[left], chunk[right], query.orderBy) })
		run, err := sqlWriteSpillRun(control.options.SpillDirectory, chunk, &available, control)
		if err != nil {
			return err
		}
		allPaths[run.path] = struct{}{}
		runs = append(runs, run)
		chunk = make([]sqlSpillOutput, 0)
		chunkBytes = 0
		return nil
	}
	inputRows, ordinal := 0, 0
	err := streamSQLSourceRows(ctx, *query.from, resolver, func(sourceRow SQLRow) error {
		if err := control.check(); err != nil {
			return err
		}
		inputRows++
		if inputRows > control.maxRows {
			return fmt.Errorf("SQL source %q exceeds the %d row limit", query.from.alias, control.maxRows)
		}
		execRow := sqlExecRow{sources: map[string]SQLRow{query.from.alias: sourceRow}, order: []string{query.from.alias}}
		if query.where.kind != "" {
			value, err := evalSQLStreamExpr(query.where, execRow, functions)
			if err != nil {
				return err
			}
			if !sqlTruthy(value) {
				return nil
			}
		}
		row := SQLRow{}
		for index, item := range query.selects {
			value, err := evalSQLStreamExpr(item.expr, execRow, functions)
			if err != nil {
				return err
			}
			row[columns[index]] = value
		}
		record := sqlSpillOutput{Row: row, Keys: make([]interface{}, len(query.orderBy)), Ordinal: ordinal}
		ordinal++
		for index, order := range query.orderBy {
			value := evalOutputOrder(order.expr, row, []sqlExecRow{execRow})
			if err := sqlExpressionError(value); err != nil {
				return err
			}
			record.Keys[index] = value
		}
		recordBytes := sqlSpillOutputBytes(record)
		if len(chunk) > 0 && chunkBytes+recordBytes > control.options.MaxSortBytes {
			if err := flush(); err != nil {
				return err
			}
		}
		chunk = append(chunk, record)
		chunkBytes += recordBytes
		return nil
	})
	if err != nil {
		return sqlRuntimeDiagnostic(err)
	}
	if err := flush(); err != nil {
		return err
	}
	for len(runs) > maxSQLSpillMergeFanIn {
		next := make([]sqlSpillRun, 0, (len(runs)+maxSQLSpillMergeFanIn-1)/maxSQLSpillMergeFanIn)
		for start := 0; start < len(runs); start += maxSQLSpillMergeFanIn {
			end := start + maxSQLSpillMergeFanIn
			if end > len(runs) {
				end = len(runs)
			}
			merged, err := sqlMergeSpillRunsToWriter(runs[start:end], query.orderBy, control.options.SpillDirectory, &available, control)
			if err != nil {
				return err
			}
			allPaths[merged.path] = struct{}{}
			for _, run := range runs[start:end] {
				if err := os.Remove(run.path); err != nil && !errors.Is(err, os.ErrNotExist) {
					return fmt.Errorf("remove SQL sort spill file: %w", err)
				}
				delete(allPaths, run.path)
				available += run.bytes
			}
			next = append(next, merged)
		}
		runs = next
	}
	position, resultBytes := 0, 0
	return sqlMergeSpillRunsToVisit(runs, query.orderBy, control, func(record sqlSpillOutput) error {
		if position >= query.offset {
			if control.options.MaxResultBytes > 0 {
				resultBytes += sqlRowBytes(record.Row)
				if resultBytes > control.options.MaxResultBytes {
					return fmt.Errorf("SQL result byte budget exceeded: maximum %d bytes", control.options.MaxResultBytes)
				}
			}
			if err := visit(columns, record.Row); err != nil {
				return err
			}
		}
		position++
		return nil
	})
}

// sqlExternalDistinctStreamable recognizes direct scalar DISTINCT queries for
// which an external key merge can retain only bounded set runs. The final
// ordinal merge restores SQL's first-occurrence result order without retaining
// a result-row slice.
func sqlExternalDistinctStreamable(query *sqlQuery, control *sqlExecutionControl) bool {
	if control == nil || control.options.MaxSetBytes <= 0 || strings.TrimSpace(control.options.SpillDirectory) == "" || control.options.MaxSpillBytes <= 0 || query == nil || query.explain || !query.distinct || query.from == nil || len(query.ctes) != 0 || len(query.unions) != 0 || len(query.joins) != 0 || len(query.groupBy) != 0 || query.having.kind != "" || len(query.orderBy) != 0 || sqlQueryHasAggregate(query) || sqlQueryHasWindow(query) || len(query.from.fieldTypes) != 0 || query.from.kind != "CACHE" && query.from.kind != "VALUES" || sqlExprHasWindow(query.where) {
		return false
	}
	for _, selectItem := range query.selects {
		if selectItem.expr.kind == "star" || sqlExprHasAggregate(selectItem.expr) || sqlExprHasWindow(selectItem.expr) {
			return false
		}
	}
	return true
}

// executeSQLExternalDistinctStream writes bounded key-sorted runs, selects one
// first-occurrence row per key, then externally restores the source ordinal
// before invoking visit. At no point does it build a source, set, or result
// slice.
func executeSQLExternalDistinctStream(ctx context.Context, query *sqlQuery, resolver SQLSourceResolver, control *sqlExecutionControl, visit func([]string, SQLRow) error) error {
	columns := sqlColumns(query.selects)
	functions, _ := resolver.(SQLFunctionResolver)
	available := int64(control.options.MaxSpillBytes)
	allPaths := map[string]struct{}{}
	defer func() {
		for path := range allPaths {
			_ = os.Remove(path)
		}
	}()
	keyRuns := []sqlSpillRun{}
	keyChunk := make([]sqlSpillSetRecord, 0)
	keyChunkBytes := 0
	flushKeys := func() error {
		if len(keyChunk) == 0 {
			return nil
		}
		sort.SliceStable(keyChunk, func(left, right int) bool { return sqlSpillSetRecordBefore(keyChunk[left], keyChunk[right]) })
		run, err := sqlWriteSpillSetRun(control.options.SpillDirectory, keyChunk, &available, control)
		if err != nil {
			return err
		}
		allPaths[run.path] = struct{}{}
		keyRuns = append(keyRuns, run)
		keyChunk = make([]sqlSpillSetRecord, 0)
		keyChunkBytes = 0
		return nil
	}
	inputRows, ordinal := 0, 0
	err := streamSQLSourceRows(ctx, *query.from, resolver, func(sourceRow SQLRow) error {
		if err := control.check(); err != nil {
			return err
		}
		inputRows++
		if inputRows > control.maxRows {
			return fmt.Errorf("SQL source %q exceeds the %d row limit", query.from.alias, control.maxRows)
		}
		execRow := sqlExecRow{sources: map[string]SQLRow{query.from.alias: sourceRow}, order: []string{query.from.alias}}
		if query.where.kind != "" {
			value, err := evalSQLStreamExpr(query.where, execRow, functions)
			if err != nil {
				return err
			}
			if !sqlTruthy(value) {
				return nil
			}
		}
		row := SQLRow{}
		for index, item := range query.selects {
			value, err := evalSQLStreamExpr(item.expr, execRow, functions)
			if err != nil {
				return err
			}
			row[columns[index]] = value
		}
		record := sqlSpillSetRecord{Key: sqlOutputRowKey(row), Row: row, Ordinal: ordinal}
		ordinal++
		recordBytes := sqlSpillSetRecordBytes(record)
		if len(keyChunk) > 0 && keyChunkBytes+recordBytes > control.options.MaxSetBytes {
			if err := flushKeys(); err != nil {
				return err
			}
		}
		keyChunk = append(keyChunk, record)
		keyChunkBytes += recordBytes
		return nil
	})
	if err != nil {
		return sqlRuntimeDiagnostic(err)
	}
	if err := flushKeys(); err != nil {
		return err
	}
	for len(keyRuns) > maxSQLSpillMergeFanIn {
		next := make([]sqlSpillRun, 0, (len(keyRuns)+maxSQLSpillMergeFanIn-1)/maxSQLSpillMergeFanIn)
		for start := 0; start < len(keyRuns); start += maxSQLSpillMergeFanIn {
			end := start + maxSQLSpillMergeFanIn
			if end > len(keyRuns) {
				end = len(keyRuns)
			}
			merged, err := sqlMergeSpillSetRunsToRun(keyRuns[start:end], control.options.SpillDirectory, &available, control)
			if err != nil {
				return err
			}
			allPaths[merged.path] = struct{}{}
			for _, run := range keyRuns[start:end] {
				if err := os.Remove(run.path); err != nil && !errors.Is(err, os.ErrNotExist) {
					return fmt.Errorf("remove SQL DISTINCT spill file: %w", err)
				}
				delete(allPaths, run.path)
				available += run.bytes
			}
			next = append(next, merged)
		}
		keyRuns = next
	}
	readers, err := sqlOpenSpillSetReaders(keyRuns)
	if err != nil {
		return err
	}
	ordinalRuns := []sqlSpillRun{}
	ordinalChunk := make([]sqlSpillOutput, 0)
	ordinalChunkBytes := 0
	ordinalOrder := []sqlOrder{{}}
	flushOrdinals := func() error {
		if len(ordinalChunk) == 0 {
			return nil
		}
		sort.SliceStable(ordinalChunk, func(left, right int) bool {
			return sqlSpillOutputLess(ordinalChunk[left], ordinalChunk[right], ordinalOrder)
		})
		run, err := sqlWriteSpillRun(control.options.SpillDirectory, ordinalChunk, &available, control)
		if err != nil {
			return err
		}
		allPaths[run.path] = struct{}{}
		ordinalRuns = append(ordinalRuns, run)
		ordinalChunk = make([]sqlSpillOutput, 0)
		ordinalChunkBytes = 0
		return nil
	}
	for {
		first, ok, err := sqlNextRawSpillSet(readers, control)
		if err != nil {
			closeSQLSpillSetReaders(readers)
			return err
		}
		if !ok {
			break
		}
		for {
			best := -1
			for index, reader := range readers {
				if reader.done || reader.current.Key != first.Key || best >= 0 && !sqlSpillSetRecordBefore(reader.current, readers[best].current) {
					continue
				}
				best = index
			}
			if best < 0 {
				break
			}
			if err := readers[best].next(); err != nil {
				closeSQLSpillSetReaders(readers)
				return err
			}
		}
		record := sqlSpillOutput{Row: first.Row, Keys: []interface{}{int64(first.Ordinal)}, Ordinal: first.Ordinal}
		recordBytes := sqlSpillOutputBytes(record)
		if len(ordinalChunk) > 0 && ordinalChunkBytes+recordBytes > control.options.MaxSetBytes {
			if err := flushOrdinals(); err != nil {
				closeSQLSpillSetReaders(readers)
				return err
			}
		}
		ordinalChunk = append(ordinalChunk, record)
		ordinalChunkBytes += recordBytes
	}
	closeSQLSpillSetReaders(readers)
	if err := flushOrdinals(); err != nil {
		return err
	}
	for _, run := range keyRuns {
		if err := os.Remove(run.path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove SQL DISTINCT spill file: %w", err)
		}
		delete(allPaths, run.path)
		available += run.bytes
	}
	for len(ordinalRuns) > maxSQLSpillMergeFanIn {
		next := make([]sqlSpillRun, 0, (len(ordinalRuns)+maxSQLSpillMergeFanIn-1)/maxSQLSpillMergeFanIn)
		for start := 0; start < len(ordinalRuns); start += maxSQLSpillMergeFanIn {
			end := start + maxSQLSpillMergeFanIn
			if end > len(ordinalRuns) {
				end = len(ordinalRuns)
			}
			merged, err := sqlMergeSpillRunsToWriter(ordinalRuns[start:end], ordinalOrder, control.options.SpillDirectory, &available, control)
			if err != nil {
				return err
			}
			allPaths[merged.path] = struct{}{}
			for _, run := range ordinalRuns[start:end] {
				if err := os.Remove(run.path); err != nil && !errors.Is(err, os.ErrNotExist) {
					return fmt.Errorf("remove SQL DISTINCT ordinal spill file: %w", err)
				}
				delete(allPaths, run.path)
				available += run.bytes
			}
			next = append(next, merged)
		}
		ordinalRuns = next
	}
	position, emitted, resultBytes := 0, 0, 0
	err = sqlMergeSpillRunsToVisit(ordinalRuns, ordinalOrder, control, func(record sqlSpillOutput) error {
		if position >= query.offset {
			if query.limit >= 0 && emitted >= query.limit {
				return errSQLStreamLimitReached
			}
			if control.options.MaxResultBytes > 0 {
				resultBytes += sqlRowBytes(record.Row)
				if resultBytes > control.options.MaxResultBytes {
					return fmt.Errorf("SQL result byte budget exceeded: maximum %d bytes", control.options.MaxResultBytes)
				}
			}
			if err := visit(columns, record.Row); err != nil {
				return err
			}
			emitted++
		}
		position++
		return nil
	})
	if errors.Is(err, errSQLStreamLimitReached) {
		return nil
	}
	return err
}

type sqlStreamAggregate struct {
	name  string
	arg   *sqlExpr
	count int64
	sum   float64
	value float64
	seen  bool
}

// sqlGlobalStreamAggregates recognizes the constant-state aggregate subset.
// It excludes joins, grouping, HAVING, and expressions around aggregates so
// this path cannot change their materialized-query semantics.
func sqlGlobalStreamAggregates(query *sqlQuery) ([]sqlStreamAggregate, bool) {
	if query == nil || query.explain || query.from == nil || len(query.ctes) != 0 || len(query.unions) != 0 || len(query.joins) != 0 || len(query.groupBy) != 0 || query.having.kind != "" || query.distinct || len(query.orderBy) != 0 || len(query.from.fieldTypes) != 0 || query.from.kind != "CACHE" && query.from.kind != "VALUES" || query.where.window != nil || sqlExprHasCustomFunction(query.where, nil) {
		return nil, false
	}
	aggregates := make([]sqlStreamAggregate, len(query.selects))
	for index, item := range query.selects {
		expr := item.expr
		if expr.kind != "func" || expr.window != nil || sqlExprHasCustomFunction(expr, nil) {
			return nil, false
		}
		aggregate := sqlStreamAggregate{name: expr.name}
		switch expr.name {
		case "COUNT":
			if len(expr.args) > 1 {
				return nil, false
			}
			if len(expr.args) == 1 && expr.args[0].kind != "star" {
				argument := expr.args[0]
				aggregate.arg = &argument
			}
		case "SUM", "AVG", "MIN", "MAX":
			if len(expr.args) != 1 || expr.args[0].kind == "star" {
				return nil, false
			}
			argument := expr.args[0]
			aggregate.arg = &argument
		default:
			return nil, false
		}
		aggregates[index] = aggregate
	}
	return aggregates, true
}

func (aggregate *sqlStreamAggregate) add(row sqlExecRow) error {
	if aggregate.name == "COUNT" && aggregate.arg == nil {
		aggregate.count++
		return nil
	}
	value := evalSQLExpr(*aggregate.arg, []sqlExecRow{row}, row)
	if err := sqlExpressionError(value); err != nil {
		return err
	}
	if aggregate.name == "COUNT" {
		if value != nil {
			aggregate.count++
		}
		return nil
	}
	number, ok := sqlNumber(value)
	if !ok {
		return nil
	}
	if !aggregate.seen {
		aggregate.value, aggregate.sum, aggregate.count, aggregate.seen = number, number, 1, true
		return nil
	}
	aggregate.count++
	switch aggregate.name {
	case "SUM", "AVG":
		aggregate.sum += number
	case "MIN":
		if number < aggregate.value {
			aggregate.value = number
		}
	case "MAX":
		if number > aggregate.value {
			aggregate.value = number
		}
	}
	return nil
}

func (aggregate sqlStreamAggregate) result() interface{} {
	switch aggregate.name {
	case "COUNT":
		return aggregate.count
	case "SUM":
		if aggregate.seen {
			return aggregate.sum
		}
	case "AVG":
		if aggregate.seen {
			return aggregate.sum / float64(aggregate.count)
		}
	case "MIN", "MAX":
		if aggregate.seen {
			return aggregate.value
		}
	}
	return nil
}

func executeSQLGlobalAggregateStream(ctx context.Context, query *sqlQuery, resolver SQLSourceResolver, control *sqlExecutionControl, visit func([]string, SQLRow) error, aggregates []sqlStreamAggregate) error {
	inputRows := 0
	err := streamSQLSourceRows(ctx, *query.from, resolver, func(sourceRow SQLRow) error {
		if err := control.check(); err != nil {
			return err
		}
		inputRows++
		if inputRows > control.maxRows {
			return fmt.Errorf("SQL source %q exceeds the %d row limit", query.from.alias, control.maxRows)
		}
		row := sqlExecRow{sources: map[string]SQLRow{query.from.alias: sourceRow}, order: []string{query.from.alias}}
		if query.where.kind != "" {
			value := evalSQLExpr(query.where, []sqlExecRow{row}, row)
			if err := sqlExpressionError(value); err != nil {
				return err
			}
			if !sqlTruthy(value) {
				return nil
			}
		}
		for index := range aggregates {
			if err := aggregates[index].add(row); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return sqlRuntimeDiagnostic(err)
	}
	if query.offset > 0 || query.limit == 0 {
		return nil
	}
	columns := sqlColumns(query.selects)
	row := SQLRow{}
	for index, aggregate := range aggregates {
		row[columns[index]] = aggregate.result()
	}
	if control.options.MaxResultBytes > 0 && sqlRowBytes(row) > control.options.MaxResultBytes {
		return fmt.Errorf("SQL result byte budget exceeded: maximum %d bytes", control.options.MaxResultBytes)
	}
	return visit(columns, row)
}

// sqlIndexedGroupStreamable proves the narrow aggregate form for which an
// indexed source order makes equal group keys adjacent. It deliberately keeps
// the existing direct-field/aggregate proof shared with the materialized index
// group operator, so grouped QueryRows cannot silently change SQL semantics.
func sqlIndexedGroupStreamable(query *sqlQuery, resolver SQLSourceResolver) ([]sqlOrderedGroupProjection, bool) {
	if query == nil || query.explain || query.from == nil || query.from.kind != "CACHE" || len(query.from.fieldTypes) != 0 || len(query.ctes) != 0 || len(query.unions) != 0 || len(query.joins) != 0 || query.distinct || query.having.kind != "" || query.where.window != nil || len(query.groupBy) != 1 || len(query.orderBy) != 1 {
		return nil, false
	}
	if !sqlSameField(query.groupBy[0], query.orderBy[0].expr) {
		return nil, false
	}
	if _, ok := resolver.(SQLOrderedStreamSourceResolver); !ok {
		return nil, false
	}
	projections, ok := sqlOrderedGroupProjections(query)
	return projections, ok
}

// executeSQLIndexedGroupAggregateStream maintains one active group while an
// ordered JSON index supplies adjacent equal keys. It never creates a source
// row or group slice; only the active aggregate state and one emitted result
// row are retained.
func executeSQLIndexedGroupAggregateStream(ctx context.Context, query *sqlQuery, resolver SQLSourceResolver, control *sqlExecutionControl, visit func([]string, SQLRow) error, projections []sqlOrderedGroupProjection) error {
	streaming, ok := resolver.(SQLOrderedStreamSourceResolver)
	if !ok {
		return fmt.Errorf("SQL query cannot stream this grouped aggregate because the resolver has no ordered index stream")
	}
	functions, _ := resolver.(SQLFunctionResolver)
	columns := sqlColumns(query.selects)
	if query.limit == 0 {
		return nil
	}
	inputRows, groupCount, position, emitted, resultBytes := 0, 0, 0, 0, 0
	var key string
	var groupValue interface{}
	var aggregates []sqlOrderedAggregate
	emit := func() error {
		if groupCount == 0 {
			return nil
		}
		if position < query.offset {
			position++
			return nil
		}
		if query.limit >= 0 && emitted >= query.limit {
			return errSQLStreamLimitReached
		}
		row := SQLRow{}
		for index, projection := range projections {
			if projection.group {
				row[projection.column] = groupValue
			} else {
				row[projection.column] = aggregates[index].value()
			}
		}
		position++
		if control.options.MaxResultBytes > 0 {
			resultBytes += sqlRowBytes(row)
			if resultBytes > control.options.MaxResultBytes {
				return fmt.Errorf("SQL result byte budget exceeded: maximum %d bytes", control.options.MaxResultBytes)
			}
		}
		emitted++
		if err := visit(columns, row); err != nil {
			return err
		}
		if query.limit >= 0 && emitted >= query.limit {
			return errSQLStreamLimitReached
		}
		return nil
	}
	consume := func(sourceRow SQLRow) error {
		if err := control.check(); err != nil {
			return err
		}
		inputRows++
		if inputRows > control.maxRows {
			return fmt.Errorf("SQL source %q exceeds the %d row limit", query.from.alias, control.maxRows)
		}
		execRow := sqlExecRow{sources: map[string]SQLRow{query.from.alias: sourceRow}, order: []string{query.from.alias}}
		if query.where.kind != "" {
			value, err := evalSQLStreamExpr(query.where, execRow, functions)
			if err != nil {
				return err
			}
			if !sqlTruthy(value) {
				return nil
			}
		}
		value, err := evalSQLStreamExpr(query.groupBy[0], execRow, functions)
		if err != nil {
			return err
		}
		currentKey := fmt.Sprintf("%#v", value)
		if groupCount == 0 || currentKey != key {
			if err := emit(); err != nil {
				return err
			}
			key, groupValue = currentKey, value
			aggregates = make([]sqlOrderedAggregate, len(projections))
			for index, projection := range projections {
				if projection.aggregate != nil {
					aggregates[index] = *projection.aggregate
				}
			}
			groupCount++
		}
		for index, projection := range projections {
			if projection.aggregate == nil {
				continue
			}
			if err := aggregates[index].add(execRow); err != nil {
				return err
			}
		}
		return nil
	}
	available, err := streaming.StreamSQLOrderedSource(ctx, query.from.kind, query.from.key, query.groupBy[0].name, query.orderBy[0].desc, query.orderBy[0].nullsFirst, query.orderBy[0].nullsLast, consume)
	if err != nil && err != errSQLStreamLimitReached {
		return sqlRuntimeDiagnostic(err)
	}
	if !available {
		if _, handled, fallbackErr := executeSQLStreamedSpilledGroupAggregate(query, resolver, control, nil, visit); handled {
			return fallbackErr
		}
		return fmt.Errorf("SQL query cannot stream this grouped aggregate because the ordered index is unavailable")
	}
	if err == errSQLStreamLimitReached {
		return nil
	}
	if err := emit(); err != nil && err != errSQLStreamLimitReached {
		return err
	}
	return nil
}

// sqlIndexedOrderStreamable proves the one-field indexed order that can be
// emitted directly. Filtering preserves source order, while joins, DISTINCT,
// grouped aggregates, windows, and expressions used as sort keys need a
// global operator and deliberately remain on their established paths.
func sqlIndexedOrderStreamable(query *sqlQuery, resolver SQLSourceResolver) bool {
	if query == nil || query.explain || query.from == nil || query.from.kind != "CACHE" || len(query.from.fieldTypes) != 0 || len(query.ctes) != 0 || len(query.unions) != 0 || len(query.joins) != 0 || len(query.groupBy) != 0 || query.having.kind != "" || query.distinct || sqlQueryHasAggregate(query) || sqlQueryHasWindow(query) || query.where.window != nil || len(query.orderBy) != 1 {
		return false
	}
	order := query.orderBy[0]
	if order.expr.kind != "field" || order.expr.qualifier != query.from.alias || order.expr.name == "" {
		return false
	}
	if _, ok := resolver.(SQLOrderedStreamSourceResolver); !ok {
		return false
	}
	for _, item := range query.selects {
		if item.expr.kind == "star" || item.expr.window != nil || sqlExprHasAggregate(item.expr) {
			return false
		}
	}
	return true
}

// executeSQLIndexedOrderStream projects directly from an ordered JSON index.
// It keeps neither source rows nor sort candidates, so an unbounded ORDER BY
// can be consumed through QueryRows as long as the index proves its order.
func executeSQLIndexedOrderStream(ctx context.Context, query *sqlQuery, resolver SQLSourceResolver, control *sqlExecutionControl, visit func([]string, SQLRow) error) error {
	streaming, ok := resolver.(SQLOrderedStreamSourceResolver)
	if !ok {
		return fmt.Errorf("SQL query cannot stream this ordered scan because the resolver has no ordered index stream")
	}
	if query.limit == 0 {
		return nil
	}
	functions, _ := resolver.(SQLFunctionResolver)
	columns := sqlColumns(query.selects)
	inputRows, seen, emitted, resultBytes := 0, 0, 0, 0
	emit := func(sourceRow SQLRow) error {
		if err := control.check(); err != nil {
			return err
		}
		inputRows++
		if inputRows > control.maxRows {
			return fmt.Errorf("SQL source %q exceeds the %d row limit", query.from.alias, control.maxRows)
		}
		execRow := sqlExecRow{sources: map[string]SQLRow{query.from.alias: sourceRow}, order: []string{query.from.alias}}
		if query.where.kind != "" {
			value, err := evalSQLStreamExpr(query.where, execRow, functions)
			if err != nil {
				return err
			}
			if !sqlTruthy(value) {
				return nil
			}
		}
		seen++
		if seen <= query.offset {
			return nil
		}
		if query.limit >= 0 && emitted >= query.limit {
			return errSQLStreamLimitReached
		}
		row := SQLRow{}
		for index, item := range query.selects {
			value, err := evalSQLStreamExpr(item.expr, execRow, functions)
			if err != nil {
				return err
			}
			row[columns[index]] = value
		}
		if control.options.MaxResultBytes > 0 {
			resultBytes += sqlRowBytes(row)
			if resultBytes > control.options.MaxResultBytes {
				return fmt.Errorf("SQL result byte budget exceeded: maximum %d bytes", control.options.MaxResultBytes)
			}
		}
		emitted++
		if err := visit(columns, row); err != nil {
			return err
		}
		if query.limit >= 0 && emitted >= query.limit {
			return errSQLStreamLimitReached
		}
		return nil
	}
	order := query.orderBy[0]
	available, err := streaming.StreamSQLOrderedSource(ctx, query.from.kind, query.from.key, order.expr.name, order.desc, order.nullsFirst, order.nullsLast, emit)
	if err != nil && err != errSQLStreamLimitReached {
		return sqlRuntimeDiagnostic(err)
	}
	if !available {
		if sqlExternalSortStreamable(query, control) {
			return executeSQLExternalSortStream(ctx, query, resolver, control, visit)
		}
		return fmt.Errorf("SQL query cannot stream this ordered scan because the ordered index is unavailable")
	}
	return nil
}

// sqlIndexedDistinctStreamable proves DISTINCT for the exact field defining
// one indexed ORDER BY. Every equal projected value is adjacent, allowing the
// executor to retain just the previously emitted canonical row key.
func sqlIndexedDistinctStreamable(query *sqlQuery, resolver SQLSourceResolver) bool {
	if query == nil || query.explain || query.from == nil || query.from.kind != "CACHE" || len(query.from.fieldTypes) != 0 || len(query.ctes) != 0 || len(query.unions) != 0 || len(query.joins) != 0 || len(query.groupBy) != 0 || query.having.kind != "" || !query.distinct || sqlQueryHasAggregate(query) || sqlQueryHasWindow(query) || query.where.window != nil || len(query.orderBy) != 1 || len(query.selects) != 1 {
		return false
	}
	selectExpr, order := query.selects[0].expr, query.orderBy[0]
	if selectExpr.kind != "field" || selectExpr.qualifier != query.from.alias || !sqlSameField(selectExpr, order.expr) {
		return false
	}
	_, ok := resolver.(SQLOrderedStreamSourceResolver)
	return ok
}

// executeSQLIndexedDistinctStream uses index adjacency to emit one direct
// projected field per distinct key without a set map or materialized result.
func executeSQLIndexedDistinctStream(ctx context.Context, query *sqlQuery, resolver SQLSourceResolver, control *sqlExecutionControl, visit func([]string, SQLRow) error) error {
	streaming, ok := resolver.(SQLOrderedStreamSourceResolver)
	if !ok {
		return fmt.Errorf("SQL query cannot stream this DISTINCT scan because the resolver has no ordered index stream")
	}
	if query.limit == 0 {
		return nil
	}
	functions, _ := resolver.(SQLFunctionResolver)
	columns := sqlColumns(query.selects)
	inputRows, seen, emitted, resultBytes := 0, 0, 0, 0
	previousKey, havePrevious := "", false
	emit := func(sourceRow SQLRow) error {
		if err := control.check(); err != nil {
			return err
		}
		inputRows++
		if inputRows > control.maxRows {
			return fmt.Errorf("SQL source %q exceeds the %d row limit", query.from.alias, control.maxRows)
		}
		execRow := sqlExecRow{sources: map[string]SQLRow{query.from.alias: sourceRow}, order: []string{query.from.alias}}
		if query.where.kind != "" {
			value, err := evalSQLStreamExpr(query.where, execRow, functions)
			if err != nil {
				return err
			}
			if !sqlTruthy(value) {
				return nil
			}
		}
		value, err := evalSQLStreamExpr(query.selects[0].expr, execRow, functions)
		if err != nil {
			return err
		}
		row := SQLRow{columns[0]: value}
		key := sqlOutputRowKey(row)
		if havePrevious && key == previousKey {
			return nil
		}
		havePrevious, previousKey = true, key
		seen++
		if seen <= query.offset {
			return nil
		}
		if query.limit >= 0 && emitted >= query.limit {
			return errSQLStreamLimitReached
		}
		if control.options.MaxResultBytes > 0 {
			resultBytes += sqlRowBytes(row)
			if resultBytes > control.options.MaxResultBytes {
				return fmt.Errorf("SQL result byte budget exceeded: maximum %d bytes", control.options.MaxResultBytes)
			}
		}
		emitted++
		if err := visit(columns, row); err != nil {
			return err
		}
		if query.limit >= 0 && emitted >= query.limit {
			return errSQLStreamLimitReached
		}
		return nil
	}
	order := query.orderBy[0]
	available, err := streaming.StreamSQLOrderedSource(ctx, query.from.kind, query.from.key, order.expr.name, order.desc, order.nullsFirst, order.nullsLast, emit)
	if err != nil && err != errSQLStreamLimitReached {
		return sqlRuntimeDiagnostic(err)
	}
	if !available {
		return fmt.Errorf("SQL query cannot stream this DISTINCT scan because the ordered index is unavailable")
	}
	return nil
}

// sqlIndexedRankWindowStreamable recognizes unpartitioned rank windows whose
// one field order is exactly the final index order. The ordered source makes
// peer changes visible one row at a time, so rank state stays constant.
func sqlIndexedRankWindowStreamable(query *sqlQuery, resolver SQLSourceResolver) bool {
	if query == nil || query.explain || query.from == nil || query.from.kind != "CACHE" || len(query.from.fieldTypes) != 0 || len(query.ctes) != 0 || len(query.unions) != 0 || len(query.joins) != 0 || len(query.groupBy) != 0 || query.having.kind != "" || query.distinct || sqlQueryHasAggregate(query) || query.where.window != nil || len(query.orderBy) != 1 {
		return false
	}
	order := query.orderBy[0]
	if order.expr.kind != "field" || order.expr.qualifier != query.from.alias || order.expr.name == "" {
		return false
	}
	if _, ok := resolver.(SQLOrderedStreamSourceResolver); !ok {
		return false
	}
	hasWindow := false
	for _, item := range query.selects {
		expr := item.expr
		if expr.window == nil {
			if expr.kind == "star" || sqlExprHasAggregate(expr) || sqlExprHasWindow(expr) {
				return false
			}
			continue
		}
		if expr.kind != "func" || len(expr.args) != 0 || len(expr.window.partition) != 0 || len(expr.window.order) != 1 || expr.window.frame != nil {
			return false
		}
		windowOrder := expr.window.order[0]
		if !sqlSameField(windowOrder.expr, order.expr) || windowOrder.desc != order.desc || windowOrder.nullsFirst != order.nullsFirst || windowOrder.nullsLast != order.nullsLast {
			return false
		}
		switch strings.ToUpper(expr.name) {
		case "ROW_NUMBER", "RANK", "DENSE_RANK":
			hasWindow = true
		default:
			return false
		}
	}
	return hasWindow
}

// executeSQLIndexedRankWindowStream evaluates rank windows from one ordered
// index scan. Rank and dense-rank need only the current position and prior
// ordering key; no partition, frame, source, or result slice is retained.
func executeSQLIndexedRankWindowStream(ctx context.Context, query *sqlQuery, resolver SQLSourceResolver, control *sqlExecutionControl, visit func([]string, SQLRow) error) error {
	streaming, ok := resolver.(SQLOrderedStreamSourceResolver)
	if !ok {
		return fmt.Errorf("SQL query cannot stream these rank windows because the resolver has no ordered index stream")
	}
	if query.limit == 0 {
		return nil
	}
	functions, _ := resolver.(SQLFunctionResolver)
	columns := sqlColumns(query.selects)
	inputRows, seen, emitted, resultBytes := 0, 0, 0, 0
	position, rank, denseRank := int64(0), int64(0), int64(0)
	var previousOrderValue interface{}
	havePrevious := false
	emit := func(sourceRow SQLRow) error {
		if err := control.check(); err != nil {
			return err
		}
		inputRows++
		if inputRows > control.maxRows {
			return fmt.Errorf("SQL source %q exceeds the %d row limit", query.from.alias, control.maxRows)
		}
		execRow := sqlExecRow{sources: map[string]SQLRow{query.from.alias: sourceRow}, order: []string{query.from.alias}}
		if query.where.kind != "" {
			value, err := evalSQLStreamExpr(query.where, execRow, functions)
			if err != nil {
				return err
			}
			if !sqlTruthy(value) {
				return nil
			}
		}
		orderValue, err := evalSQLStreamExpr(query.orderBy[0].expr, execRow, functions)
		if err != nil {
			return err
		}
		position++
		if !havePrevious || sqlCompare(orderValue, previousOrderValue) != 0 {
			rank, denseRank = position, denseRank+1
			previousOrderValue, havePrevious = orderValue, true
		}
		row := SQLRow{}
		for index, item := range query.selects {
			if item.expr.window != nil {
				switch strings.ToUpper(item.expr.name) {
				case "ROW_NUMBER":
					row[columns[index]] = position
				case "RANK":
					row[columns[index]] = rank
				case "DENSE_RANK":
					row[columns[index]] = denseRank
				}
				continue
			}
			value, err := evalSQLStreamExpr(item.expr, execRow, functions)
			if err != nil {
				return err
			}
			row[columns[index]] = value
		}
		seen++
		if seen <= query.offset {
			return nil
		}
		if query.limit >= 0 && emitted >= query.limit {
			return errSQLStreamLimitReached
		}
		if control.options.MaxResultBytes > 0 {
			resultBytes += sqlRowBytes(row)
			if resultBytes > control.options.MaxResultBytes {
				return fmt.Errorf("SQL result byte budget exceeded: maximum %d bytes", control.options.MaxResultBytes)
			}
		}
		emitted++
		if err := visit(columns, row); err != nil {
			return err
		}
		if query.limit >= 0 && emitted >= query.limit {
			return errSQLStreamLimitReached
		}
		return nil
	}
	order := query.orderBy[0]
	available, err := streaming.StreamSQLOrderedSource(ctx, query.from.kind, query.from.key, order.expr.name, order.desc, order.nullsFirst, order.nullsLast, emit)
	if err != nil && err != errSQLStreamLimitReached {
		return sqlRuntimeDiagnostic(err)
	}
	if !available {
		return fmt.Errorf("SQL query cannot stream these rank windows because the ordered index is unavailable")
	}
	return nil
}

// sqlIndexedLagWindowStreamable recognizes literal-offset LAG windows whose
// ordered index is also the final query order. The existing LAG ring buffer is
// bounded by the literal offset, so no row history slice is needed.
func sqlIndexedLagWindowStreamable(query *sqlQuery, resolver SQLSourceResolver) bool {
	if query == nil || query.explain || query.from == nil || query.from.kind != "CACHE" || len(query.from.fieldTypes) != 0 || len(query.ctes) != 0 || len(query.unions) != 0 || len(query.joins) != 0 || len(query.groupBy) != 0 || query.having.kind != "" || query.distinct || sqlQueryHasAggregate(query) || query.where.window != nil || len(query.orderBy) != 1 {
		return false
	}
	order := query.orderBy[0]
	if order.expr.kind != "field" || order.expr.qualifier != query.from.alias || order.expr.name == "" {
		return false
	}
	if _, ok := resolver.(SQLOrderedStreamSourceResolver); !ok {
		return false
	}
	hasLag := false
	for _, item := range query.selects {
		expr := item.expr
		if expr.window == nil {
			if expr.kind == "star" || sqlExprHasAggregate(expr) || sqlExprHasWindow(expr) {
				return false
			}
			continue
		}
		if expr.kind != "func" || strings.ToUpper(expr.name) != "LAG" || len(expr.window.partition) != 0 || len(expr.window.order) != 1 || expr.window.frame != nil {
			return false
		}
		windowOrder := expr.window.order[0]
		if !sqlSameField(windowOrder.expr, order.expr) || windowOrder.desc != order.desc || windowOrder.nullsFirst != order.nullsFirst || windowOrder.nullsLast != order.nullsLast {
			return false
		}
		if _, ok := sqlRunningWindowLagOffset(expr.args); !ok {
			return false
		}
		for _, argument := range expr.args {
			if sqlExprHasAggregate(argument) || sqlExprHasWindow(argument) || sqlExprHasCustomFunction(argument, nil) {
				return false
			}
		}
		hasLag = true
	}
	return hasLag
}

func sqlIndexedLeadWindowStreamable(query *sqlQuery, resolver SQLSourceResolver) bool {
	if query == nil || query.explain || query.from == nil || query.from.kind != "CACHE" || len(query.from.fieldTypes) != 0 || len(query.ctes) != 0 || len(query.unions) != 0 || len(query.joins) != 0 || len(query.groupBy) != 0 || query.having.kind != "" || query.distinct || sqlQueryHasAggregate(query) || sqlExprHasWindow(query.where) || sqlExprHasCustomFunction(query.where, nil) || len(query.orderBy) != 1 {
		return false
	}
	order := query.orderBy[0]
	if order.expr.kind != "field" || order.expr.qualifier != query.from.alias || order.expr.name == "" {
		return false
	}
	if _, ok := resolver.(SQLOrderedStreamSourceResolver); !ok {
		return false
	}
	hasLead := false
	for _, item := range query.selects {
		expr := item.expr
		if expr.window == nil {
			if expr.kind == "star" || sqlExprHasAggregate(expr) || sqlExprHasWindow(expr) || sqlExprHasCustomFunction(expr, nil) {
				return false
			}
			continue
		}
		if expr.kind != "func" || strings.ToUpper(expr.name) != "LEAD" || len(expr.window.partition) != 0 || len(expr.window.order) != 1 || expr.window.frame != nil {
			return false
		}
		windowOrder := expr.window.order[0]
		if !sqlSameField(windowOrder.expr, order.expr) || windowOrder.desc != order.desc || windowOrder.nullsFirst != order.nullsFirst || windowOrder.nullsLast != order.nullsLast {
			return false
		}
		if _, ok := sqlRunningWindowLagOffset(expr.args); !ok {
			return false
		}
		for _, argument := range expr.args {
			if sqlExprHasAggregate(argument) || sqlExprHasWindow(argument) || sqlExprHasCustomFunction(argument, nil) {
				return false
			}
		}
		hasLead = true
	}
	return hasLead
}

func sqlIndexedRunningAggregateWindowStreamable(query *sqlQuery, resolver SQLSourceResolver) bool {
	if query == nil || query.explain || query.from == nil || query.from.kind != "CACHE" || len(query.from.fieldTypes) != 0 || len(query.ctes) != 0 || len(query.unions) != 0 || len(query.joins) != 0 || len(query.groupBy) != 0 || query.having.kind != "" || query.distinct || sqlQueryHasAggregate(query) || query.where.window != nil || len(query.orderBy) != 1 {
		return false
	}
	order := query.orderBy[0]
	if order.expr.kind != "field" || order.expr.qualifier != query.from.alias || order.expr.name == "" {
		return false
	}
	if _, ok := resolver.(SQLOrderedStreamSourceResolver); !ok {
		return false
	}
	hasAggregate := false
	for _, item := range query.selects {
		expr := item.expr
		if expr.window == nil {
			if expr.kind == "star" || sqlExprHasAggregate(expr) || sqlExprHasWindow(expr) {
				return false
			}
			continue
		}
		if expr.kind != "func" || len(expr.window.partition) != 0 || len(expr.window.order) != 1 || expr.window.frame != nil || len(expr.args) != 1 || sqlExprHasAggregate(expr.args[0]) || sqlExprHasWindow(expr.args[0]) || sqlExprHasCustomFunction(expr.args[0], nil) {
			return false
		}
		windowOrder := expr.window.order[0]
		if !sqlSameField(windowOrder.expr, order.expr) || windowOrder.desc != order.desc || windowOrder.nullsFirst != order.nullsFirst || windowOrder.nullsLast != order.nullsLast {
			return false
		}
		switch strings.ToUpper(expr.name) {
		case "SUM", "AVG", "MIN", "MAX":
			hasAggregate = true
		default:
			return false
		}
	}
	return hasAggregate
}

func executeSQLIndexedRunningAggregateWindowStream(ctx context.Context, query *sqlQuery, resolver SQLSourceResolver, control *sqlExecutionControl, visit func([]string, SQLRow) error) error {
	streaming, ok := resolver.(SQLOrderedStreamSourceResolver)
	if !ok {
		return fmt.Errorf("SQL query cannot stream these running windows because the resolver has no ordered index stream")
	}
	if query.limit == 0 {
		return nil
	}
	states := make([]*sqlRunningWindowState, len(query.selects))
	for index, item := range query.selects {
		if item.expr.window == nil {
			continue
		}
		states[index] = &sqlRunningWindowState{name: strings.ToUpper(item.expr.name), arg: item.expr.args[0]}
	}
	functions, _ := resolver.(SQLFunctionResolver)
	columns := sqlColumns(query.selects)
	inputRows, seen, emitted, resultBytes := 0, 0, 0, 0
	emit := func(sourceRow SQLRow) error {
		if err := control.check(); err != nil {
			return err
		}
		inputRows++
		if inputRows > control.maxRows {
			return fmt.Errorf("SQL source %q exceeds the %d row limit", query.from.alias, control.maxRows)
		}
		execRow := sqlExecRow{sources: map[string]SQLRow{query.from.alias: sourceRow}, order: []string{query.from.alias}}
		if query.where.kind != "" {
			value, err := evalSQLStreamExpr(query.where, execRow, functions)
			if err != nil {
				return err
			}
			if !sqlTruthy(value) {
				return nil
			}
		}
		row := SQLRow{}
		for index, item := range query.selects {
			if states[index] != nil {
				value, err := states[index].add(execRow)
				if err != nil {
					return err
				}
				row[columns[index]] = value
				continue
			}
			value, err := evalSQLStreamExpr(item.expr, execRow, functions)
			if err != nil {
				return err
			}
			row[columns[index]] = value
		}
		seen++
		if seen <= query.offset {
			return nil
		}
		if query.limit >= 0 && emitted >= query.limit {
			return errSQLStreamLimitReached
		}
		if control.options.MaxResultBytes > 0 {
			resultBytes += sqlRowBytes(row)
			if resultBytes > control.options.MaxResultBytes {
				return fmt.Errorf("SQL result byte budget exceeded: maximum %d bytes", control.options.MaxResultBytes)
			}
		}
		emitted++
		if err := visit(columns, row); err != nil {
			return err
		}
		if query.limit >= 0 && emitted >= query.limit {
			return errSQLStreamLimitReached
		}
		return nil
	}
	order := query.orderBy[0]
	available, err := streaming.StreamSQLOrderedSource(ctx, query.from.kind, query.from.key, order.expr.name, order.desc, order.nullsFirst, order.nullsLast, emit)
	if err != nil && err != errSQLStreamLimitReached {
		return sqlRuntimeDiagnostic(err)
	}
	if !available {
		return fmt.Errorf("SQL query cannot stream these running windows because the ordered index is unavailable")
	}
	return nil
}

func executeSQLIndexedLeadWindowStream(ctx context.Context, query *sqlQuery, resolver SQLSourceResolver, control *sqlExecutionControl, visit func([]string, SQLRow) error) error {
	streaming, ok := resolver.(SQLOrderedStreamSourceResolver)
	if !ok {
		return fmt.Errorf("SQL query cannot stream these LEAD windows because the resolver has no ordered index stream")
	}
	if query.limit == 0 {
		return nil
	}
	type definition struct {
		column, offset int
		argument       sqlExpr
		fallback       *sqlExpr
	}
	definitions := map[int]definition{}
	maximumOffset := 0
	for column, item := range query.selects {
		if item.expr.window == nil {
			continue
		}
		offset, _ := sqlRunningWindowLagOffset(item.expr.args)
		definition := definition{column: column, argument: item.expr.args[0], offset: offset}
		if len(item.expr.args) == 3 {
			fallback := item.expr.args[2]
			definition.fallback = &fallback
		}
		definitions[column] = definition
		if offset > maximumOffset {
			maximumOffset = offset
		}
	}
	columns := sqlColumns(query.selects)
	inputRows, seen, emitted, resultBytes := 0, 0, 0, 0
	pending := []sqlExecRow{}
	emitAvailable := func(final bool) error {
		for len(pending) > 0 && (final || len(pending) > maximumOffset) {
			current := pending[0]
			row := SQLRow{}
			for column, item := range query.selects {
				if definition, ok := definitions[column]; ok {
					value := interface{}(nil)
					if definition.offset < len(pending) {
						target := pending[definition.offset]
						value = evalSQLExpr(definition.argument, []sqlExecRow{target}, target)
					} else if definition.fallback != nil {
						value = evalSQLExpr(*definition.fallback, []sqlExecRow{current}, current)
					}
					if err := sqlExpressionError(value); err != nil {
						return err
					}
					row[columns[column]] = value
					continue
				}
				value := evalSQLExpr(item.expr, []sqlExecRow{current}, current)
				if err := sqlExpressionError(value); err != nil {
					return err
				}
				row[columns[column]] = value
			}
			pending[0] = sqlExecRow{}
			pending = pending[1:]
			seen++
			if seen <= query.offset {
				continue
			}
			if query.limit >= 0 && emitted >= query.limit {
				return errSQLStreamLimitReached
			}
			if control.options.MaxResultBytes > 0 {
				resultBytes += sqlRowBytes(row)
				if resultBytes > control.options.MaxResultBytes {
					return fmt.Errorf("SQL result byte budget exceeded: maximum %d bytes", control.options.MaxResultBytes)
				}
			}
			emitted++
			if err := visit(columns, row); err != nil {
				return err
			}
			if query.limit >= 0 && emitted >= query.limit {
				return errSQLStreamLimitReached
			}
		}
		return nil
	}
	emit := func(sourceRow SQLRow) error {
		if err := control.check(); err != nil {
			return err
		}
		inputRows++
		if inputRows > control.maxRows {
			return fmt.Errorf("SQL source %q exceeds the %d row limit", query.from.alias, control.maxRows)
		}
		current := sqlExecRow{sources: map[string]SQLRow{query.from.alias: sourceRow}, order: []string{query.from.alias}}
		if query.where.kind != "" {
			value := evalSQLExpr(query.where, []sqlExecRow{current}, current)
			if err := sqlExpressionError(value); err != nil {
				return err
			}
			if !sqlTruthy(value) {
				return nil
			}
		}
		pending = append(pending, current)
		return emitAvailable(false)
	}
	order := query.orderBy[0]
	available, err := streaming.StreamSQLOrderedSource(ctx, query.from.kind, query.from.key, order.expr.name, order.desc, order.nullsFirst, order.nullsLast, emit)
	if err != nil && err != errSQLStreamLimitReached {
		return sqlRuntimeDiagnostic(err)
	}
	if !available {
		return fmt.Errorf("SQL query cannot stream these LEAD windows because the ordered index is unavailable")
	}
	if err == errSQLStreamLimitReached {
		return nil
	}
	if err := emitAvailable(true); err != nil && err != errSQLStreamLimitReached {
		return sqlRuntimeDiagnostic(err)
	}
	return nil
}

// executeSQLIndexedLagWindowStream projects scalar expressions and bounded
// LAG histories from one ordered index scan. Each LAG retains only its literal
// offset ring; the index supplies the otherwise global order.
func executeSQLIndexedLagWindowStream(ctx context.Context, query *sqlQuery, resolver SQLSourceResolver, control *sqlExecutionControl, visit func([]string, SQLRow) error) error {
	streaming, ok := resolver.(SQLOrderedStreamSourceResolver)
	if !ok {
		return fmt.Errorf("SQL query cannot stream these LAG windows because the resolver has no ordered index stream")
	}
	if query.limit == 0 {
		return nil
	}
	states := make([]*sqlRunningWindowState, len(query.selects))
	for index, item := range query.selects {
		if item.expr.window == nil {
			continue
		}
		offset, _ := sqlRunningWindowLagOffset(item.expr.args)
		state := &sqlRunningWindowState{name: "LAG", arg: item.expr.args[0], lagOffset: offset}
		if len(item.expr.args) == 3 {
			fallback := item.expr.args[2]
			state.lagDefault = &fallback
		}
		if offset > 0 && offset < control.maxRows {
			state.lagValues = make([]interface{}, offset)
		}
		states[index] = state
	}
	functions, _ := resolver.(SQLFunctionResolver)
	columns := sqlColumns(query.selects)
	inputRows, seen, emitted, resultBytes := 0, 0, 0, 0
	emit := func(sourceRow SQLRow) error {
		if err := control.check(); err != nil {
			return err
		}
		inputRows++
		if inputRows > control.maxRows {
			return fmt.Errorf("SQL source %q exceeds the %d row limit", query.from.alias, control.maxRows)
		}
		execRow := sqlExecRow{sources: map[string]SQLRow{query.from.alias: sourceRow}, order: []string{query.from.alias}}
		if query.where.kind != "" {
			value, err := evalSQLStreamExpr(query.where, execRow, functions)
			if err != nil {
				return err
			}
			if !sqlTruthy(value) {
				return nil
			}
		}
		row := SQLRow{}
		for index, item := range query.selects {
			if states[index] != nil {
				value, err := states[index].add(execRow)
				if err != nil {
					return err
				}
				row[columns[index]] = value
				continue
			}
			value, err := evalSQLStreamExpr(item.expr, execRow, functions)
			if err != nil {
				return err
			}
			row[columns[index]] = value
		}
		seen++
		if seen <= query.offset {
			return nil
		}
		if query.limit >= 0 && emitted >= query.limit {
			return errSQLStreamLimitReached
		}
		if control.options.MaxResultBytes > 0 {
			resultBytes += sqlRowBytes(row)
			if resultBytes > control.options.MaxResultBytes {
				return fmt.Errorf("SQL result byte budget exceeded: maximum %d bytes", control.options.MaxResultBytes)
			}
		}
		emitted++
		if err := visit(columns, row); err != nil {
			return err
		}
		if query.limit >= 0 && emitted >= query.limit {
			return errSQLStreamLimitReached
		}
		return nil
	}
	order := query.orderBy[0]
	available, err := streaming.StreamSQLOrderedSource(ctx, query.from.kind, query.from.key, order.expr.name, order.desc, order.nullsFirst, order.nullsLast, emit)
	if err != nil && err != errSQLStreamLimitReached {
		return sqlRuntimeDiagnostic(err)
	}
	if !available {
		return fmt.Errorf("SQL query cannot stream these LAG windows because the ordered index is unavailable")
	}
	return nil
}

func validateSQLQueryStreamable(query *sqlQuery) error {
	if query.distinct {
		return fmt.Errorf("SQL query cannot stream this DISTINCT shape: use a compatible ordered index, or for a direct scalar CACHE/VALUES query configure MaxSetBytes, SpillDirectory, and MaxSpillBytes")
	}
	if len(query.orderBy) > 0 {
		return fmt.Errorf("SQL query cannot stream this ORDER BY: use a compatible ordered index, add a finite LIMIT for bounded top-N, or for a direct scalar CACHE/VALUES query configure MaxSortBytes, SpillDirectory, and MaxSpillBytes")
	}
	if query.explain || len(query.ctes) > 0 || len(query.unions) > 0 || len(query.groupBy) > 0 || query.having.kind != "" || sqlQueryHasAggregate(query) {
		return fmt.Errorf("SQL query cannot stream because it requires materialized grouping, ordering, DISTINCT, EXPLAIN, or set operations")
	}
	aliases := []string{}
	if query.from != nil {
		aliases = append(aliases, query.from.alias)
	}
	for _, join := range query.joins {
		if (join.kind != "INNER" && join.kind != "LEFT") || join.source.kind != "CACHE" {
			return fmt.Errorf("SQL query can stream only indexed INNER or LEFT CACHE joins")
		}
		if _, _, _, ok := sqlHashJoinFields(join.on, aliases, join.source.alias); !ok {
			return fmt.Errorf("SQL query can stream a join only with an equality ON condition")
		}
		aliases = append(aliases, join.source.alias)
	}
	if query.from == nil || query.from.kind != "CACHE" && query.from.kind != "VALUES" {
		if query.from == nil {
			return fmt.Errorf("SQL query cannot stream without a source")
		}
		return fmt.Errorf("SQL source %q cannot stream rows yet; use CACHE or VALUES", query.from.kind)
	}
	if len(query.from.fieldTypes) > 0 && len(query.joins) > 0 {
		return fmt.Errorf("typed CACHE fields can stream only without joins because indexed join probes must preserve declared field validation")
	}
	if query.where.window != nil {
		return fmt.Errorf("SQL query cannot stream window expressions")
	}
	for _, item := range query.selects {
		if item.expr.kind == "star" || item.expr.window != nil || sqlExprHasAggregate(item.expr) {
			return fmt.Errorf("SQL query cannot stream SELECT *, aggregate, or window expressions")
		}
	}
	return nil
}

func streamSQLSourceRows(ctx context.Context, source sqlSource, resolver SQLSourceResolver, visit func(SQLRow) error) error {
	switch source.kind {
	case "VALUES":
		for _, values := range source.values {
			if err := ctx.Err(); err != nil {
				return err
			}
			row := SQLRow{}
			for index, column := range source.columns {
				if index < len(values) {
					row[column] = values[index]
				}
			}
			if err := visit(row); err != nil {
				return err
			}
		}
		return nil
	case "CACHE":
		rowIndex := 0
		validate := func(row SQLRow) (SQLRow, error) {
			rowIndex++
			if len(source.fieldTypes) == 0 {
				return row, nil
			}
			return validateSQLSourceFieldTypeRow(source, row, rowIndex)
		}
		if streaming, ok := resolver.(SQLStreamSourceResolver); ok {
			return streaming.StreamSQLSource(ctx, source.kind, source.key, func(row SQLRow) error {
				validated, err := validate(row)
				if err != nil {
					return err
				}
				return visit(validated)
			})
		}
		rows, err := resolver.ResolveSQLSource(source.kind, source.key)
		if err != nil {
			return err
		}
		for _, row := range rows {
			if err := ctx.Err(); err != nil {
				return err
			}
			validated, err := validate(row)
			if err != nil {
				return err
			}
			if err := visit(validated); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("SQL source %q cannot stream rows yet", source.kind)
	}
}

func sqlSpillHashJoinStreamable(query *sqlQuery, resolver SQLSourceResolver, control *sqlExecutionControl) bool {
	if query == nil || query.from == nil || control == nil || control.options.MaxJoinBytes <= 0 || strings.TrimSpace(control.options.SpillDirectory) == "" || control.options.MaxSpillBytes <= 0 || len(query.ctes) != 0 || len(query.unions) != 0 || len(query.joins) != 1 || query.from.kind != "CACHE" || query.joins[0].source.kind != "CACHE" || query.joins[0].kind != "INNER" || query.where.kind != "" || len(query.groupBy) != 0 || query.having.kind != "" || query.distinct || len(query.orderBy) != 0 || query.offset != 0 || query.limit >= 0 || sqlQueryHasAggregate(query) || sqlQueryHasWindow(query) || len(query.from.fieldTypes) != 0 || len(query.joins[0].source.fieldTypes) != 0 {
		return false
	}
	if _, ok := resolver.(SQLStreamSourceResolver); !ok {
		return false
	}
	_, _, _, ok := sqlHashJoinFields(query.joins[0].on, []string{query.from.alias}, query.joins[0].source.alias)
	if !ok {
		return false
	}
	for _, item := range query.selects {
		if item.expr.kind == "star" || item.expr.window != nil || sqlExprHasAggregate(item.expr) || sqlExprHasFunction(item.expr) {
			return false
		}
	}
	return true
}

// executeSQLSpillHashJoin partitions each streamed input on its equality key,
// then builds at most one right partition at a time. Output is spilled and
// merged by original source ordinal so enabling the fallback does not change
// the established deterministic no-ORDER-BY result order.
func executeSQLSpillHashJoin(query *sqlQuery, resolver SQLSourceResolver, control *sqlExecutionControl, metrics *sqlExecutionMetrics) (SQLQueryResult, bool, error) {
	if !sqlSpillHashJoinStreamable(query, resolver, control) {
		return SQLQueryResult{}, false, nil
	}
	_, leftField, rightField, _ := sqlHashJoinFields(query.joins[0].on, []string{query.from.alias}, query.joins[0].source.alias)
	available := int64(control.options.MaxSpillBytes)
	paths := map[string]struct{}{}
	defer func() {
		for path := range paths {
			_ = os.Remove(path)
		}
	}()
	leftParts, err := newSQLSpillHashPartitions(control.options.SpillDirectory, "hatrie-sql-hash-left-*", &available, paths)
	if err != nil {
		return SQLQueryResult{}, true, err
	}
	rightParts, err := newSQLSpillHashPartitions(control.options.SpillDirectory, "hatrie-sql-hash-right-*", &available, paths)
	if err != nil {
		closeSQLSpillHashPartitions(leftParts)
		return SQLQueryResult{}, true, err
	}
	leftRows, rightRows := 0, 0
	if err := streamSQLSourceRows(control.ctx, *query.from, resolver, func(row SQLRow) error {
		if err := control.check(); err != nil {
			return err
		}
		if leftRows >= control.maxRows {
			return fmt.Errorf("SQL source %q exceeds the %d row limit", query.from.alias, control.maxRows)
		}
		ordinal := leftRows
		leftRows++
		key, ok := sqlHashJoinKey(row[leftField])
		if !ok {
			return nil
		}
		return writeSQLSpillHashInput(leftParts[sqlSpillHashPartitionIndex(key)], sqlSpillHashInput{Row: row, Ordinal: ordinal})
	}); err != nil {
		closeSQLSpillHashPartitions(leftParts)
		closeSQLSpillHashPartitions(rightParts)
		return SQLQueryResult{}, true, err
	}
	if err := streamSQLSourceRows(control.ctx, query.joins[0].source, resolver, func(row SQLRow) error {
		if err := control.check(); err != nil {
			return err
		}
		if rightRows >= control.maxRows {
			return fmt.Errorf("SQL source %q exceeds the %d row limit", query.joins[0].source.alias, control.maxRows)
		}
		ordinal := rightRows
		rightRows++
		key, ok := sqlHashJoinKey(row[rightField])
		if !ok {
			return nil
		}
		return writeSQLSpillHashInput(rightParts[sqlSpillHashPartitionIndex(key)], sqlSpillHashInput{Row: row, Ordinal: ordinal})
	}); err != nil {
		closeSQLSpillHashPartitions(leftParts)
		closeSQLSpillHashPartitions(rightParts)
		return SQLQueryResult{}, true, err
	}
	if err := closeSQLSpillHashPartitions(leftParts); err != nil {
		closeSQLSpillHashPartitions(rightParts)
		return SQLQueryResult{}, true, err
	}
	if err := closeSQLSpillHashPartitions(rightParts); err != nil {
		return SQLQueryResult{}, true, err
	}

	chunkLimit := control.options.MaxJoinBytes
	outputRuns := []sqlSpillRun{}
	outputChunk := make([]sqlSpillOutput, 0)
	outputBytes, outputRows := 0, 0
	flushOutput := func() error {
		if len(outputChunk) == 0 {
			return nil
		}
		sort.SliceStable(outputChunk, func(left, right int) bool {
			return sqlSpillOutputLess(outputChunk[left], outputChunk[right], []sqlOrder{{}})
		})
		run, err := sqlWriteSpillRun(control.options.SpillDirectory, outputChunk, &available, control)
		if err != nil {
			return err
		}
		paths[run.path] = struct{}{}
		outputRuns = append(outputRuns, run)
		outputChunk, outputBytes = make([]sqlSpillOutput, 0), 0
		return nil
	}

	columns := sqlColumns(query.selects)
	for index := range leftParts {
		if err := control.check(); err != nil {
			return SQLQueryResult{}, true, err
		}
		processRightChunk := func(right []sqlSpillHashInput) error {
			buckets := make(map[string][]sqlSpillHashInput, len(right))
			for _, entry := range right {
				key, _ := sqlHashJoinKey(entry.Row[rightField])
				buckets[key] = append(buckets[key], entry)
			}
			return visitSQLSpillHashPartition(leftParts[index].path, control, func(leftEntry sqlSpillHashInput) error {
				key, _ := sqlHashJoinKey(leftEntry.Row[leftField])
				for _, rightEntry := range buckets[key] {
					if err := control.addJoinWork(1); err != nil {
						return err
					}
					combined := mergeSQLRows(
						sqlExecRow{sources: map[string]SQLRow{query.from.alias: leftEntry.Row}, order: []string{query.from.alias}},
						sqlExecRow{sources: map[string]SQLRow{query.joins[0].source.alias: rightEntry.Row}, order: []string{query.joins[0].source.alias}},
					)
					row := SQLRow{}
					for column, item := range query.selects {
						value := evalSQLExpr(item.expr, []sqlExecRow{combined}, combined)
						if err := sqlExpressionError(value); err != nil {
							return err
						}
						row[columns[column]] = value
					}
					if outputRows >= control.maxRows {
						return fmt.Errorf("SQL join exceeds the %d row limit; add a more selective WHERE or ON condition", control.maxRows)
					}
					outputRows++
					ordinal := int64(leftEntry.Ordinal)*int64(control.maxRows+1) + int64(rightEntry.Ordinal)
					record := sqlSpillOutput{Row: row, Keys: []interface{}{ordinal}, Ordinal: outputRows}
					outputChunk = append(outputChunk, record)
					outputBytes += sqlSpillOutputBytes(record)
					if outputBytes >= chunkLimit {
						return flushOutput()
					}
				}
				return nil
			})
		}
		rightChunk := make([]sqlSpillHashInput, 0)
		rightBytes := 0
		err := visitSQLSpillHashPartition(rightParts[index].path, control, func(entry sqlSpillHashInput) error {
			entryBytes := sqlRowBytes(entry.Row) + 16
			if len(rightChunk) > 0 && rightBytes+entryBytes > chunkLimit {
				if err := processRightChunk(rightChunk); err != nil {
					return err
				}
				rightChunk, rightBytes = rightChunk[:0], 0
			}
			rightChunk = append(rightChunk, entry)
			rightBytes += entryBytes
			return nil
		})
		if err != nil {
			return SQLQueryResult{}, true, err
		}
		if err := processRightChunk(rightChunk); err != nil {
			return SQLQueryResult{}, true, err
		}
	}
	if err := flushOutput(); err != nil {
		return SQLQueryResult{}, true, err
	}
	rows, err := sqlMergeSpillRunsToRows(outputRuns, []sqlOrder{{}}, 0, -1, control)
	if err != nil {
		return SQLQueryResult{}, true, err
	}
	if metrics != nil {
		metrics.record("SPILL HASH JOIN", fmt.Sprintf("partitioned equality join (%d partitions)", sqlSpillHashPartitions), leftRows+rightRows, len(rows), time.Now())
	}
	return SQLQueryResult{Columns: columns, Rows: rows}, true, nil
}

func newSQLSpillHashPartitions(directory, pattern string, available *int64, paths map[string]struct{}) ([]sqlSpillHashPartition, error) {
	partitions := make([]sqlSpillHashPartition, sqlSpillHashPartitions)
	for index := range partitions {
		file, err := os.CreateTemp(directory, pattern)
		if err != nil {
			closeSQLSpillHashPartitions(partitions[:index])
			return nil, fmt.Errorf("create SQL hash spill file: %w", err)
		}
		paths[file.Name()] = struct{}{}
		partitions[index] = sqlSpillHashPartition{path: file.Name(), file: file, encoder: gob.NewEncoder(sqlSpillBudgetWriter{writer: file, available: available})}
	}
	return partitions, nil
}

func writeSQLSpillHashInput(partition sqlSpillHashPartition, input sqlSpillHashInput) error {
	if err := partition.encoder.Encode(input); err != nil {
		if errors.Is(err, errSQLSpillDiskBudget) {
			return fmt.Errorf("SQL spill disk budget exceeded while writing hash partitions")
		}
		return fmt.Errorf("write SQL hash spill partition: %w", err)
	}
	return nil
}

func closeSQLSpillHashPartitions(partitions []sqlSpillHashPartition) error {
	var first error
	for index := range partitions {
		if partitions[index].file != nil {
			if err := partitions[index].file.Close(); err != nil && first == nil {
				first = err
			}
			partitions[index].file = nil
		}
	}
	return first
}

func readSQLSpillHashPartition(path string, control *sqlExecutionControl) ([]sqlSpillHashInput, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open SQL hash spill partition: %w", err)
	}
	defer file.Close()
	decoder := gob.NewDecoder(file)
	entries := []sqlSpillHashInput{}
	for {
		if err := control.check(); err != nil {
			return nil, err
		}
		var entry sqlSpillHashInput
		if err := decoder.Decode(&entry); err != nil {
			if errors.Is(err, io.EOF) {
				return entries, nil
			}
			return nil, fmt.Errorf("read SQL hash spill partition: %w", err)
		}
		entries = append(entries, entry)
	}
}

func visitSQLSpillHashPartition(path string, control *sqlExecutionControl, visit func(sqlSpillHashInput) error) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open SQL hash spill partition: %w", err)
	}
	defer file.Close()
	decoder := gob.NewDecoder(file)
	for {
		if err := control.check(); err != nil {
			return err
		}
		var entry sqlSpillHashInput
		if err := decoder.Decode(&entry); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("read SQL hash spill partition: %w", err)
		}
		if err := visit(entry); err != nil {
			return err
		}
	}
}

func sqlSpillHashPartitionIndex(key string) int {
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(key))
	return int(hash.Sum32() % sqlSpillHashPartitions)
}

type sqlCursor struct {
	Fingerprint string `json:"f"`
	Offset      int    `json:"o"`
}

// ExecuteSQLQueryPage executes one bounded page. Cursors are opaque and bound
// to both SQL text and the encoded parameter values.
func ExecuteSQLQueryPage(ctx context.Context, source string, resolver SQLSourceResolver, parameters []interface{}, options SQLQueryOptions, pageSize int, cursor string) (result SQLQueryResult, err error) {
	observation := newSQLQueryObservation(options)
	var operatorSteps []SQLExplainStep
	result.QueryID = observation.id
	defer func() { observation.finish(result, err, operatorSteps) }()
	release := lockSQLSnapshot(resolver)
	defer release()
	if pageSize <= 0 {
		pageSize = 100
	}
	if pageSize > maxSQLPageSize {
		return result, fmt.Errorf("SQL page_size exceeds the maximum %d", maxSQLPageSize)
	}
	control, cancel, controlErr := newSQLExecutionControl(ctx, options)
	if controlErr != nil {
		return result, controlErr
	}
	defer cancel()
	query, parseErr := parseSQLQueryWithCache(source, parameters, options.PreparedCache)
	if parseErr != nil {
		return result, parseErr
	}
	if query.explain {
		return result, fmt.Errorf("EXPLAIN does not support cursor pagination")
	}
	fingerprint, fingerprintErr := sqlCursorFingerprint(source, parameters)
	if fingerprintErr != nil {
		return result, fingerprintErr
	}
	offset := 0
	if cursor != "" {
		value, cursorErr := decodeSQLCursor(cursor)
		if cursorErr != nil {
			return result, cursorErr
		}
		if value.Fingerprint != fingerprint {
			return result, fmt.Errorf("SQL cursor does not match this query and parameters")
		}
		offset = value.Offset
	}
	originalLimit := query.limit
	query.offset += offset
	fetch := pageSize + 1
	if originalLimit >= 0 {
		remaining := originalLimit - offset
		if remaining <= 0 {
			fetch = 0
		} else if remaining < fetch {
			fetch = remaining
		}
	}
	query.limit = fetch
	var metrics *sqlExecutionMetrics
	if observation.observer != nil {
		metrics = &sqlExecutionMetrics{}
	}
	result, err = executeSQLQueryWithMetrics(query, resolver, nil, metrics, control)
	if metrics != nil {
		operatorSteps = metrics.steps
	}
	result.QueryID = observation.id
	if err != nil {
		return result, sqlRuntimeDiagnostic(err)
	}
	if len(result.Rows) > pageSize {
		result.Rows = result.Rows[:pageSize]
		result.HasMore = true
		next, cursorErr := encodeSQLCursor(sqlCursor{Fingerprint: fingerprint, Offset: offset + pageSize})
		if cursorErr != nil {
			return result, cursorErr
		}
		result.NextCursor = next
	}
	return result, nil
}

// ExecuteQueryPage returns one cursor-addressable result page.
func ExecuteQueryPage(ctx context.Context, source string, resolver SourceResolver, parameters []interface{}, options QueryOptions, pageSize int, cursor string) (QueryResult, error) {
	return ExecuteSQLQueryPage(ctx, source, resolver, parameters, options, pageSize, cursor)
}

func lockSQLSnapshot(resolver SQLSourceResolver) func() {
	if locker, ok := resolver.(SQLSnapshotLocker); ok {
		if release := locker.LockSQLSnapshot(); release != nil {
			return release
		}
	}
	return func() {}
}

func sqlCursorFingerprint(source string, parameters []interface{}) (string, error) {
	encoded, err := json.Marshal(parameters)
	if err != nil {
		return "", fmt.Errorf("encode SQL cursor parameters: %w", err)
	}
	sum := sha256.Sum256(append(append([]byte(source), 0), encoded...))
	return base64.RawURLEncoding.EncodeToString(sum[:]), nil
}
func encodeSQLCursor(cursor sqlCursor) (string, error) {
	value, err := json.Marshal(cursor)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(value), nil
}
func decodeSQLCursor(value string) (sqlCursor, error) {
	encoded, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return sqlCursor{}, fmt.Errorf("invalid SQL cursor")
	}
	var cursor sqlCursor
	if json.Unmarshal(encoded, &cursor) != nil || cursor.Offset < 0 || cursor.Fingerprint == "" {
		return sqlCursor{}, fmt.Errorf("invalid SQL cursor")
	}
	return cursor, nil
}

// ValidateSQLQuery verifies syntax without reading any cache source.
func ValidateSQLQuery(source string) error { _, err := parseSQLQuery(source); return err }

// ValidateQuery verifies query syntax without reading a source.
func ValidateQuery(source string) error { return ValidateSQLQuery(source) }

func parseSQLQuery(source string) (*sqlQuery, error) { return parseSQLQueryParameters(source, nil) }

func parseSQLQueryParameters(source string, parameters []interface{}) (*sqlQuery, error) {
	return parseSQLQueryWithCache(source, parameters, nil)
}

func parseSQLQueryWithCache(source string, parameters []interface{}, cache *SQLPreparedQueryCache) (*sqlQuery, error) {
	if cache == nil {
		cache = defaultSQLPreparedQueryCache
	}
	template, err := cache.template(source)
	if err != nil {
		return nil, err
	}
	return bindSQLQueryParameters(template, parameters)
}

func (cache *SQLPreparedQueryCache) template(source string) (*sqlQuery, error) {
	if cache == nil || cache.capacity <= 0 {
		return parseSQLQueryTemplate(source)
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if query := cache.entries[source]; query != nil {
		cache.hits++
		cache.touch(source)
		return query, nil
	}
	query, err := parseSQLQueryTemplate(source)
	if err != nil {
		return nil, err
	}
	cache.misses++
	if len(cache.entries) >= cache.capacity {
		evicted := cache.order[0]
		cache.order = cache.order[1:]
		delete(cache.entries, evicted)
	}
	cache.entries[source] = query
	cache.order = append(cache.order, source)
	return query, nil
}

// touch moves a cached key to the newest position. The caller holds cache.mu.
func (cache *SQLPreparedQueryCache) touch(source string) {
	for index, entry := range cache.order {
		if entry != source {
			continue
		}
		copy(cache.order[index:], cache.order[index+1:])
		cache.order[len(cache.order)-1] = source
		return
	}
}

func parseSQLQueryTemplate(source string) (*sqlQuery, error) {
	tokens, err := lexSQL(source)
	if err != nil {
		return nil, err
	}
	parser := sqlQueryParser{tokens: tokens, allowParameters: true}
	explain := false
	analyze := false
	if parser.keyword("EXPLAIN") {
		explain = true
		parser.next()
		if parser.keyword("ANALYZE") {
			analyze = true
			parser.next()
		}
		if parser.current().kind == sqlTokenEOF || parser.current().kind == sqlTokenSemicolon {
			label := "EXPLAIN"
			if analyze {
				label += " ANALYZE"
			}
			return nil, parser.diagnostic(parser.current(), label+" requires a query after it")
		}
	}
	query, err := parser.parseQuery(false)
	if err != nil {
		return nil, err
	}
	if parser.current().kind == sqlTokenSemicolon {
		parser.next()
	}
	if parser.current().kind != sqlTokenEOF {
		return nil, parser.expected(parser.current(), "end of input", nil)
	}
	query.explain = explain
	query.analyze = analyze
	return query, nil
}

// bindSQLQueryParameters returns a private execution copy of a cached query
// template. Templates are never modified: they may be shared by concurrent
// requests while every request receives its own parameter values, limit, and
// offset state.
func bindSQLQueryParameters(template *sqlQuery, parameters []interface{}) (*sqlQuery, error) {
	query := cloneSQLQuery(template)
	if err := bindSQLQuery(query, parameters); err != nil {
		return nil, err
	}
	return query, nil
}

func bindSQLQuery(query *sqlQuery, parameters []interface{}) error {
	if query == nil {
		return nil
	}
	for index := range query.ctes {
		cte := &query.ctes[index]
		if cte.query != nil {
			if err := bindSQLQuery(cte.query, parameters); err != nil {
				return err
			}
		}
		if err := bindSQLValues(cte.values, parameters); err != nil {
			return err
		}
	}
	if query.from != nil {
		if err := bindSQLSource(query.from, parameters); err != nil {
			return err
		}
	}
	for index := range query.joins {
		join := &query.joins[index]
		if err := bindSQLSource(&join.source, parameters); err != nil {
			return err
		}
		if err := bindSQLExpr(&join.on, parameters); err != nil {
			return err
		}
	}
	for index := range query.selects {
		if err := bindSQLExpr(&query.selects[index].expr, parameters); err != nil {
			return err
		}
	}
	if err := bindSQLExpr(&query.where, parameters); err != nil {
		return err
	}
	for index := range query.groupBy {
		if err := bindSQLExpr(&query.groupBy[index], parameters); err != nil {
			return err
		}
	}
	if err := bindSQLExpr(&query.having, parameters); err != nil {
		return err
	}
	for index := range query.orderBy {
		if err := bindSQLExpr(&query.orderBy[index].expr, parameters); err != nil {
			return err
		}
	}
	for index := range query.unions {
		if err := bindSQLQuery(query.unions[index].query, parameters); err != nil {
			return err
		}
	}
	return nil
}

func bindSQLSource(source *sqlSource, parameters []interface{}) error {
	if source.keyParameter != 0 {
		value, err := sqlParameterValue(source.keyParameter, source.keyToken, parameters)
		if err != nil {
			return err
		}
		source.key = fmt.Sprint(value)
		source.keyParameter = 0
	}
	if source.query != nil {
		if err := bindSQLQuery(source.query, parameters); err != nil {
			return err
		}
	}
	return bindSQLValues(source.values, parameters)
}

func bindSQLValues(values [][]interface{}, parameters []interface{}) error {
	for rowIndex := range values {
		for columnIndex, value := range values[rowIndex] {
			parameter, ok := value.(sqlParameter)
			if !ok {
				continue
			}
			bound, err := sqlParameterValue(parameter.index, parameter.token, parameters)
			if err != nil {
				return err
			}
			values[rowIndex][columnIndex] = bound
		}
	}
	return nil
}

func bindSQLExpr(expr *sqlExpr, parameters []interface{}) error {
	if expr == nil {
		return nil
	}
	if expr.kind == "parameter" {
		index, ok := expr.value.(int)
		if !ok {
			return fmt.Errorf("internal SQL parameter template is invalid")
		}
		value, err := sqlParameterValue(index, expr.token, parameters)
		if err != nil {
			return err
		}
		expr.kind = "literal"
		expr.value = value
	}
	if err := bindSQLExpr(expr.left, parameters); err != nil {
		return err
	}
	if err := bindSQLExpr(expr.right, parameters); err != nil {
		return err
	}
	for index := range expr.args {
		if err := bindSQLExpr(&expr.args[index], parameters); err != nil {
			return err
		}
	}
	for index := range expr.cases {
		if err := bindSQLExpr(&expr.cases[index].when, parameters); err != nil {
			return err
		}
		if err := bindSQLExpr(&expr.cases[index].then, parameters); err != nil {
			return err
		}
	}
	if expr.window != nil {
		for index := range expr.window.partition {
			if err := bindSQLExpr(&expr.window.partition[index], parameters); err != nil {
				return err
			}
		}
		for index := range expr.window.order {
			if err := bindSQLExpr(&expr.window.order[index].expr, parameters); err != nil {
				return err
			}
		}
	}
	return nil
}

func sqlParameterValue(index int, token sqlToken, parameters []interface{}) (interface{}, error) {
	if index < 1 {
		return nil, sqlTokenDiagnostic(token, "parameter indexes start at $1")
	}
	if index > len(parameters) {
		return nil, sqlTokenDiagnostic(token, fmt.Sprintf("parameter $%d has no supplied parameter (received %d)", index, len(parameters)))
	}
	return parameters[index-1], nil
}

func cloneSQLQuery(source *sqlQuery) *sqlQuery {
	if source == nil {
		return nil
	}
	query := *source
	query.ctes = make([]sqlCTE, len(source.ctes))
	for index, cte := range source.ctes {
		query.ctes[index] = cte
		query.ctes[index].columns = append([]string(nil), cte.columns...)
		query.ctes[index].searchBy = append([]string(nil), cte.searchBy...)
		query.ctes[index].cycleBy = append([]string(nil), cte.cycleBy...)
		query.ctes[index].values = cloneSQLValues(cte.values)
		query.ctes[index].query = cloneSQLQuery(cte.query)
	}
	query.selects = make([]sqlSelectItem, len(source.selects))
	for index, item := range source.selects {
		query.selects[index] = item
		query.selects[index].expr = cloneSQLExpr(item.expr)
	}
	query.from = cloneSQLSource(source.from)
	query.joins = make([]sqlJoin, len(source.joins))
	for index, join := range source.joins {
		query.joins[index] = join
		query.joins[index].source = *cloneSQLSource(&join.source)
		query.joins[index].on = cloneSQLExpr(join.on)
	}
	query.where = cloneSQLExpr(source.where)
	query.groupBy = cloneSQLExprs(source.groupBy)
	query.having = cloneSQLExpr(source.having)
	query.orderBy = cloneSQLOrders(source.orderBy)
	query.unions = make([]sqlUnion, len(source.unions))
	for index, union := range source.unions {
		query.unions[index] = union
		query.unions[index].query = cloneSQLQuery(union.query)
	}
	return &query
}

func cloneSQLSource(source *sqlSource) *sqlSource {
	if source == nil {
		return nil
	}
	copy := *source
	copy.values = cloneSQLValues(source.values)
	copy.columns = append([]string(nil), source.columns...)
	copy.query = cloneSQLQuery(source.query)
	if source.fieldTypes != nil {
		copy.fieldTypes = make(map[string]sqlSourceFieldType, len(source.fieldTypes))
		for name, fieldType := range source.fieldTypes {
			copy.fieldTypes[name] = fieldType
		}
	}
	return &copy
}

func cloneSQLValues(values [][]interface{}) [][]interface{} {
	copy := make([][]interface{}, len(values))
	for index, row := range values {
		copy[index] = append([]interface{}(nil), row...)
	}
	return copy
}

func cloneSQLExprs(expressions []sqlExpr) []sqlExpr {
	copy := make([]sqlExpr, len(expressions))
	for index, expression := range expressions {
		copy[index] = cloneSQLExpr(expression)
	}
	return copy
}

func cloneSQLOrders(orders []sqlOrder) []sqlOrder {
	copy := make([]sqlOrder, len(orders))
	for index, order := range orders {
		copy[index] = order
		copy[index].expr = cloneSQLExpr(order.expr)
	}
	return copy
}

func cloneSQLExpr(source sqlExpr) sqlExpr {
	copy := source
	if source.left != nil {
		left := cloneSQLExpr(*source.left)
		copy.left = &left
	}
	if source.right != nil {
		right := cloneSQLExpr(*source.right)
		copy.right = &right
	}
	copy.args = cloneSQLExprs(source.args)
	if source.cases != nil {
		copy.cases = make([]sqlCaseWhen, len(source.cases))
		for index, branch := range source.cases {
			copy.cases[index] = sqlCaseWhen{when: cloneSQLExpr(branch.when), then: cloneSQLExpr(branch.then)}
		}
	}
	if source.window != nil {
		window := *source.window
		window.partition = cloneSQLExprs(source.window.partition)
		window.order = cloneSQLOrders(source.window.order)
		if source.window.frame != nil {
			frame := *source.window.frame
			window.frame = &frame
		}
		copy.window = &window
	}
	return copy
}

type sqlQuery struct {
	ctes     []sqlCTE
	selects  []sqlSelectItem
	from     *sqlSource
	joins    []sqlJoin
	where    sqlExpr
	groupBy  []sqlExpr
	having   sqlExpr
	orderBy  []sqlOrder
	limit    int
	offset   int
	distinct bool
	unions   []sqlUnion
	explain  bool
	analyze  bool
}
type sqlUnion struct {
	kind  string
	all   bool
	query *sqlQuery
}
type sqlCTE struct {
	name      string
	columns   []string
	query     *sqlQuery
	values    [][]interface{}
	recursive bool
	searchBy  []string
	searchSet string
	cycleBy   []string
	cycleSet  string
}
type sqlSource struct {
	kind, key, alias string
	values           [][]interface{}
	columns          []string
	fieldTypes       map[string]sqlSourceFieldType
	query            *sqlQuery
	keyParameter     int
	keyToken         sqlToken
}
type sqlSourceFieldType struct {
	name  string
	token sqlToken
}
type sqlJoin struct {
	kind   string
	source sqlSource
	on     sqlExpr
}
type sqlSelectItem struct {
	expr  sqlExpr
	alias string
}
type sqlOrder struct {
	expr       sqlExpr
	desc       bool
	nullsFirst bool
	nullsLast  bool
}
type sqlCaseWhen struct {
	when sqlExpr
	then sqlExpr
}
type sqlExpr struct {
	kind, name, qualifier, op string
	value                     interface{}
	left, right               *sqlExpr
	args                      []sqlExpr
	cases                     []sqlCaseWhen
	window                    *sqlWindow
	token                     sqlToken
}

// sqlParameter is retained only in an immutable parsed template when a
// placeholder appears in a VALUES source. It is replaced before execution.
type sqlParameter struct {
	index int
	token sqlToken
}
type sqlWindow struct {
	partition []sqlExpr
	order     []sqlOrder
	frame     *sqlWindowFrame
}
type sqlWindowFrame struct {
	kind  string
	start sqlWindowFrameBound
	end   sqlWindowFrameBound
}
type sqlWindowFrameBound struct {
	kind   string
	offset int
}

type sqlQueryParser struct {
	tokens          []sqlToken
	index           int
	parameters      []interface{}
	allowParameters bool
}

func (p *sqlQueryParser) parseQuery(stopRight bool) (*sqlQuery, error) {
	q := &sqlQuery{limit: -1}
	if p.keyword("UNION") || p.keyword("INTERSECT") || p.keyword("EXCEPT") {
		return nil, p.expected(p.current(), "SELECT, FROM, or WITH", []string{"SELECT", "FROM", "WITH"})
	}
	if p.keyword("WITH") {
		p.next()
		withRecursive := p.keyword("RECURSIVE")
		if withRecursive {
			p.next()
		}
		for {
			name, err := p.expectIdentifier("a CTE name", nil)
			if err != nil {
				return nil, err
			}
			cte := sqlCTE{name: strings.ToUpper(name.text)}
			if p.current().kind == sqlTokenLeftParen {
				cols, err := p.parseColumns()
				if err != nil {
					return nil, err
				}
				cte.columns = cols
			}
			if err := p.expectKeyword("AS"); err != nil {
				return nil, err
			}
			if err := p.expectKind(sqlTokenLeftParen, "("); err != nil {
				return nil, err
			}
			if p.keyword("VALUES") {
				values, err := p.parseValues()
				if err != nil {
					return nil, err
				}
				cte.values = values
			} else {
				nested, err := p.parseQuery(true)
				if err != nil {
					return nil, err
				}
				cte.query = nested
			}
			if err := p.expectKind(sqlTokenRightParen, ")"); err != nil {
				return nil, err
			}
			if p.keyword("SEARCH") {
				search := p.current()
				p.next()
				if !p.keyword("BREADTH") {
					return nil, p.expected(p.current(), "BREADTH after SEARCH", []string{"BREADTH"})
				}
				p.next()
				if err := p.expectKeyword("FIRST"); err != nil {
					return nil, err
				}
				if err := p.expectKeyword("BY"); err != nil {
					return nil, err
				}
				for {
					column, err := p.expectIdentifier("a SEARCH BY column", nil)
					if err != nil {
						return nil, err
					}
					cte.searchBy = append(cte.searchBy, column.text)
					if p.current().kind != sqlTokenComma {
						break
					}
					p.next()
				}
				if err := p.expectKeyword("SET"); err != nil {
					return nil, err
				}
				set, err := p.expectIdentifier("a SEARCH output column", nil)
				if err != nil {
					return nil, err
				}
				cte.searchSet = set.text
				_ = search
			}
			if p.keyword("CYCLE") {
				p.next()
				for {
					column, err := p.expectIdentifier("a CYCLE column", nil)
					if err != nil {
						return nil, err
					}
					cte.cycleBy = append(cte.cycleBy, column.text)
					if p.current().kind != sqlTokenComma {
						break
					}
					p.next()
				}
				if err := p.expectKeyword("SET"); err != nil {
					return nil, err
				}
				set, err := p.expectIdentifier("a CYCLE output column", nil)
				if err != nil {
					return nil, err
				}
				cte.cycleSet = set.text
			}
			if cte.query != nil && sqlQueryReferencesCTE(cte.query, cte.name) {
				if !withRecursive {
					return nil, p.diagnostic(name, "recursive CTE "+name.text+" requires WITH RECURSIVE")
				}
				if len(cte.query.unions) != 1 || cte.query.unions[0].kind != "UNION" || cte.query.unions[0].query == nil || len(cte.query.unions[0].query.unions) != 0 {
					return nil, p.diagnostic(name, "recursive CTE "+name.text+" requires exactly one UNION or UNION ALL recursive term")
				}
				cte.recursive = true
			}
			if (cte.searchSet != "" || cte.cycleSet != "") && !cte.recursive {
				return nil, p.diagnostic(name, "SEARCH and CYCLE are only valid for a recursive CTE")
			}
			q.ctes = append(q.ctes, cte)
			if p.current().kind != sqlTokenComma {
				break
			}
			p.next()
		}
	}
	for p.current().kind != sqlTokenEOF && !(stopRight && p.current().kind == sqlTokenRightParen) && !p.keyword("UNION") && !p.keyword("INTERSECT") && !p.keyword("EXCEPT") {
		switch {
		case p.keyword("SELECT"):
			if q.selects != nil {
				return nil, p.diagnostic(p.current(), "SELECT appears more than once")
			}
			p.next()
			if p.keyword("DISTINCT") {
				q.distinct = true
				p.next()
			}
			items, err := p.parseSelect()
			if err != nil {
				return nil, err
			}
			q.selects = items
		case p.keyword("FROM"):
			if q.from != nil {
				return nil, p.diagnostic(p.current(), "FROM appears more than once")
			}
			p.next()
			source, err := p.parseSource()
			if err != nil {
				return nil, err
			}
			q.from = &source
		case p.keyword("JOIN") || p.keyword("INNER") || p.keyword("LEFT") || p.keyword("RIGHT") || p.keyword("FULL") || p.keyword("CROSS"):
			if q.from == nil {
				return nil, p.diagnostic(p.current(), "JOIN requires FROM first")
			}
			join, err := p.parseJoin()
			if err != nil {
				return nil, err
			}
			q.joins = append(q.joins, join)
		case p.keyword("WHERE"):
			if q.where.kind != "" {
				return nil, p.diagnostic(p.current(), "WHERE appears more than once")
			}
			p.next()
			expr, err := p.parseCondition()
			if err != nil {
				return nil, err
			}
			q.where = expr
		case p.keyword("GROUP"):
			if q.groupBy != nil {
				return nil, p.diagnostic(p.current(), "GROUP BY appears more than once")
			}
			p.next()
			if err := p.expectKeyword("BY"); err != nil {
				return nil, err
			}
			values, err := p.parseExprList()
			if err != nil {
				return nil, err
			}
			q.groupBy = values
		case p.keyword("HAVING"):
			if q.having.kind != "" {
				return nil, p.diagnostic(p.current(), "HAVING appears more than once")
			}
			p.next()
			expr, err := p.parseCondition()
			if err != nil {
				return nil, err
			}
			q.having = expr
		case p.keyword("ORDER"):
			if q.orderBy != nil {
				return nil, p.diagnostic(p.current(), "ORDER BY appears more than once")
			}
			p.next()
			if err := p.expectKeyword("BY"); err != nil {
				return nil, err
			}
			order, err := p.parseOrder()
			if err != nil {
				return nil, err
			}
			q.orderBy = order
		case p.keyword("LIMIT"):
			if q.limit >= 0 {
				return nil, p.diagnostic(p.current(), "LIMIT appears more than once")
			}
			p.next()
			value, err := p.parseInteger("LIMIT")
			if err != nil {
				return nil, err
			}
			q.limit = value
		case p.keyword("FETCH"):
			if q.limit >= 0 {
				return nil, p.diagnostic(p.current(), "FETCH cannot be combined with LIMIT")
			}
			p.next()
			if err := p.expectKeyword("FIRST"); err != nil {
				return nil, err
			}
			value, err := p.parseInteger("FETCH FIRST")
			if err != nil {
				return nil, err
			}
			if err := p.expectKeyword("ROWS"); err != nil {
				return nil, err
			}
			if err := p.expectKeyword("ONLY"); err != nil {
				return nil, err
			}
			q.limit = value
		case p.keyword("OFFSET"):
			if q.offset != 0 {
				return nil, p.diagnostic(p.current(), "OFFSET appears more than once")
			}
			p.next()
			value, err := p.parseInteger("OFFSET")
			if err != nil {
				return nil, err
			}
			q.offset = value
		default:
			if strings.EqualFold(p.current().text, "JION") {
				return nil, p.expected(p.current(), "JOIN", []string{"JOIN"})
			}
			return nil, p.expected(p.current(), "SELECT, FROM, JOIN, WHERE, GROUP BY, HAVING, ORDER BY, LIMIT, or OFFSET", []string{"SELECT", "FROM", "JOIN", "LEFT", "CROSS", "WHERE", "GROUP", "HAVING", "ORDER", "LIMIT", "OFFSET"})
		}
	}
	if q.from == nil {
		return nil, p.diagnostic(p.current(), "query requires FROM")
	}
	if q.selects == nil {
		return nil, p.diagnostic(p.current(), "query requires SELECT")
	}
	if p.keyword("UNION") || p.keyword("INTERSECT") || p.keyword("EXCEPT") {
		kind := strings.ToUpper(p.current().text)
		p.next()
		union := sqlUnion{kind: kind}
		if p.keyword("ALL") {
			if kind != "UNION" {
				return nil, p.diagnostic(p.current(), kind+" ALL is not supported")
			}
			union.all = true
			p.next()
		}
		if p.current().kind == sqlTokenEOF || p.current().kind == sqlTokenSemicolon || stopRight && p.current().kind == sqlTokenRightParen {
			label := kind
			if union.all {
				label += " ALL"
			}
			return nil, p.diagnostic(p.current(), label+" requires a query after it")
		}
		right, err := p.parseQuery(stopRight)
		if err != nil {
			return nil, err
		}
		union.query = right
		q.unions = append(q.unions, union)
	}
	return q, nil
}

func sqlQueryReferencesCTE(query *sqlQuery, name string) bool {
	if query == nil {
		return false
	}
	if query.from != nil && sqlSourceReferencesCTE(*query.from, name) {
		return true
	}
	for _, join := range query.joins {
		if sqlSourceReferencesCTE(join.source, name) {
			return true
		}
	}
	for _, union := range query.unions {
		if sqlQueryReferencesCTE(union.query, name) {
			return true
		}
	}
	return false
}

func sqlSourceReferencesCTE(source sqlSource, name string) bool {
	return source.kind == "CTE" && source.key == name || source.kind == "SUBQUERY" && sqlQueryReferencesCTE(source.query, name)
}

func (p *sqlQueryParser) parseColumns() ([]string, error) {
	p.next()
	var out []string
	for {
		tok, err := p.expectIdentifier("a column name", nil)
		if err != nil {
			return nil, err
		}
		out = append(out, tok.text)
		if p.current().kind != sqlTokenComma {
			break
		}
		p.next()
	}
	if err := p.expectKind(sqlTokenRightParen, ")"); err != nil {
		return nil, err
	}
	return out, nil
}
func (p *sqlQueryParser) parseValues() ([][]interface{}, error) {
	if err := p.expectKeyword("VALUES"); err != nil {
		return nil, err
	}
	var rows [][]interface{}
	for {
		if err := p.expectKind(sqlTokenLeftParen, "("); err != nil {
			return nil, err
		}
		var row []interface{}
		for {
			expr, err := p.parsePrimary()
			if err != nil {
				return nil, err
			}
			if expr.kind != "literal" && expr.kind != "parameter" {
				return nil, p.diagnostic(p.previous(), "VALUES accepts literals only")
			}
			if expr.kind == "parameter" {
				row = append(row, sqlParameter{index: expr.value.(int), token: expr.token})
			} else {
				row = append(row, expr.value)
			}
			if p.current().kind != sqlTokenComma {
				break
			}
			p.next()
		}
		if err := p.expectKind(sqlTokenRightParen, ")"); err != nil {
			return nil, err
		}
		if len(rows) > 0 && len(rows[0]) != len(row) {
			return nil, p.diagnostic(p.previous(), "all VALUES rows must have the same number of columns")
		}
		rows = append(rows, row)
		if p.current().kind != sqlTokenComma || p.peek().kind != sqlTokenLeftParen {
			break
		}
		p.next()
	}
	return rows, nil
}
func (p *sqlQueryParser) parseSource() (sqlSource, error) {
	if p.current().kind == sqlTokenLeftParen {
		p.next()
		query, err := p.parseQuery(true)
		if err != nil {
			return sqlSource{}, err
		}
		if err := p.expectKind(sqlTokenRightParen, ")"); err != nil {
			return sqlSource{}, err
		}
		source := sqlSource{kind: "SUBQUERY", query: query}
		if err := p.parseAlias(&source); err != nil {
			return sqlSource{}, err
		}
		return source, nil
	}
	if p.keyword("VALUES") {
		rows, err := p.parseValues()
		if err != nil {
			return sqlSource{}, err
		}
		source := sqlSource{kind: "VALUES", values: rows}
		if err := p.parseAlias(&source); err != nil {
			return sqlSource{}, err
		}
		return source, nil
	}
	if p.keyword("CACHE") {
		p.next()
		if err := p.expectKind(sqlTokenLeftParen, "("); err != nil {
			return sqlSource{}, err
		}
		value, err := p.parsePrimary()
		if err != nil {
			return sqlSource{}, err
		}
		if (value.kind != "literal" && value.kind != "parameter") || p.current().kind != sqlTokenRightParen {
			return sqlSource{}, p.diagnostic(p.current(), "CACHE requires one literal cache key or parameter")
		}
		p.next()
		source := sqlSource{kind: "CACHE"}
		if value.kind == "parameter" {
			source.keyParameter = value.value.(int)
			source.keyToken = value.token
		} else {
			source.key = fmt.Sprint(value.value)
		}
		if err := p.parseAlias(&source); err != nil {
			return sqlSource{}, err
		}
		return source, nil
	}
	if p.keyword("EXTERNAL") {
		p.next()
		if err := p.expectKind(sqlTokenLeftParen, "("); err != nil {
			return sqlSource{}, err
		}
		value, err := p.parsePrimary()
		if err != nil {
			return sqlSource{}, err
		}
		if value.kind != "literal" || p.current().kind != sqlTokenRightParen {
			return sqlSource{}, p.diagnostic(p.current(), "EXTERNAL requires one literal table name")
		}
		name, ok := value.value.(string)
		if !ok || name == "" {
			return sqlSource{}, p.diagnostic(value.token, "EXTERNAL requires a non-empty string table name")
		}
		p.next()
		source := sqlSource{kind: "EXTERNAL", key: name}
		if err := p.parseAlias(&source); err != nil {
			return sqlSource{}, err
		}
		return source, nil
	}
	if p.keyword("KEYS") {
		p.next()
		source := sqlSource{kind: "KEYS"}
		if err := p.parseAlias(&source); err != nil {
			return sqlSource{}, err
		}
		return source, nil
	}
	name, err := p.expectIdentifier("a source name", nil)
	if err != nil {
		return sqlSource{}, err
	}
	source := sqlSource{kind: "CTE", key: strings.ToUpper(name.text)}
	if err := p.parseAlias(&source); err != nil {
		return sqlSource{}, err
	}
	return source, nil
}
func (p *sqlQueryParser) parseAlias(source *sqlSource) error {
	if p.keyword("AS") {
		p.next()
		if p.current().kind == sqlTokenIdentifier {
			source.alias = p.current().text
			p.next()
		}
	} else if p.current().kind == sqlTokenIdentifier && !sqlClauseKeyword(p.current().text) && !strings.EqualFold(p.current().text, "JION") {
		source.alias = p.current().text
		p.next()
	}
	if p.current().kind == sqlTokenLeftParen {
		if source.kind == "CACHE" {
			fieldTypes, err := p.parseSourceFieldTypes()
			if err != nil {
				return err
			}
			source.fieldTypes = fieldTypes
		} else {
			cols, _ := p.parseColumns()
			source.columns = cols
		}
	}
	if source.alias == "" {
		if source.kind == "CTE" {
			source.alias = strings.ToLower(source.key)
		} else {
			source.alias = strings.ToLower(source.kind)
		}
	}
	return nil
}

func (p *sqlQueryParser) parseSourceFieldTypes() (map[string]sqlSourceFieldType, error) {
	p.next()
	fields := map[string]sqlSourceFieldType{}
	for {
		field, err := p.expectIdentifier("a JSON field name", nil)
		if err != nil {
			return nil, err
		}
		typeToken := p.current()
		if typeToken.kind != sqlTokenIdentifier {
			return nil, p.expected(typeToken, "a field type after "+field.text, []string{"TEXT", "NUMBER", "INTEGER", "DECIMAL", "BOOLEAN", "DATE", "TIMESTAMP", "JSON"})
		}
		p.next()
		typeName := strings.ToUpper(typeToken.text)
		switch typeName {
		case "TEXT", "NUMBER", "INTEGER", "DECIMAL", "BOOLEAN", "DATE", "TIMESTAMP", "JSON":
		default:
			return nil, p.diagnostic(typeToken, "unsupported JSON field type "+strconv.Quote(typeToken.text)+"; expected TEXT, NUMBER, INTEGER, DECIMAL, BOOLEAN, DATE, TIMESTAMP, or JSON")
		}
		if _, exists := fields[field.text]; exists {
			return nil, p.diagnostic(field, "JSON field "+strconv.Quote(field.text)+" is declared more than once")
		}
		fields[field.text] = sqlSourceFieldType{name: typeName, token: typeToken}
		if p.current().kind != sqlTokenComma {
			break
		}
		p.next()
	}
	if err := p.expectKind(sqlTokenRightParen, ")"); err != nil {
		return nil, err
	}
	return fields, nil
}
func (p *sqlQueryParser) parseJoin() (sqlJoin, error) {
	kind := "INNER"
	token := p.current()
	if p.keyword("INNER") {
		p.next()
		if err := p.expectKeyword("JOIN"); err != nil {
			return sqlJoin{}, err
		}
	} else if p.keyword("LEFT") {
		kind = "LEFT"
		p.next()
		if p.keyword("OUTER") {
			p.next()
		}
		if err := p.expectKeyword("JOIN"); err != nil {
			return sqlJoin{}, err
		}
	} else if p.keyword("RIGHT") {
		kind = "RIGHT"
		p.next()
		if p.keyword("OUTER") {
			p.next()
		}
		if err := p.expectKeyword("JOIN"); err != nil {
			return sqlJoin{}, err
		}
	} else if p.keyword("FULL") {
		kind = "FULL"
		p.next()
		if p.keyword("OUTER") {
			p.next()
		}
		if err := p.expectKeyword("JOIN"); err != nil {
			return sqlJoin{}, err
		}
	} else if p.keyword("CROSS") {
		kind = "CROSS"
		p.next()
		if err := p.expectKeyword("JOIN"); err != nil {
			return sqlJoin{}, err
		}
	} else {
		if err := p.expectKeyword("JOIN"); err != nil {
			return sqlJoin{}, err
		}
	}
	source, err := p.parseSource()
	if err != nil {
		return sqlJoin{}, err
	}
	join := sqlJoin{kind: kind, source: source}
	if kind != "CROSS" {
		if err := p.expectKeyword("ON"); err != nil {
			return sqlJoin{}, err
		}
		on, err := p.parseCondition()
		if err != nil {
			return sqlJoin{}, err
		}
		join.on = on
	}
	_ = token
	return join, nil
}
func (p *sqlQueryParser) parseSelect() ([]sqlSelectItem, error) {
	var out []sqlSelectItem
	for {
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		item := sqlSelectItem{expr: expr}
		if p.keyword("AS") {
			p.next()
			alias, err := p.expectIdentifier("an alias", nil)
			if err != nil {
				return nil, err
			}
			item.alias = alias.text
		} else if p.current().kind == sqlTokenIdentifier && !sqlClauseKeyword(p.current().text) && !sqlSuspectedClauseTypo(p.current().text) {
			item.alias = p.current().text
			p.next()
		}
		out = append(out, item)
		if p.current().kind != sqlTokenComma {
			break
		}
		p.next()
	}
	return out, nil
}
func (p *sqlQueryParser) parseExprList() ([]sqlExpr, error) {
	var out []sqlExpr
	for {
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		out = append(out, expr)
		if p.current().kind != sqlTokenComma {
			break
		}
		p.next()
	}
	return out, nil
}
func (p *sqlQueryParser) parseOrder() ([]sqlOrder, error) {
	var out []sqlOrder
	for {
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		value := sqlOrder{expr: expr}
		if p.keyword("ASC") {
			p.next()
		} else if p.keyword("DESC") {
			p.next()
			value.desc = true
		}
		if p.keyword("NULLS") {
			p.next()
			if p.keyword("FIRST") {
				value.nullsFirst = true
				p.next()
			} else if p.keyword("LAST") {
				value.nullsLast = true
				p.next()
			} else {
				return nil, p.expected(p.current(), "FIRST or LAST after NULLS", []string{"FIRST", "LAST"})
			}
		}
		out = append(out, value)
		if p.current().kind != sqlTokenComma {
			break
		}
		p.next()
	}
	return out, nil
}
func (p *sqlQueryParser) parseCondition() (sqlExpr, error) {
	return p.parseOrCondition()
}

// SQL evaluates AND before OR. Keeping this split also makes later support for
// parenthesized predicates unambiguous.
func (p *sqlQueryParser) parseOrCondition() (sqlExpr, error) {
	left, err := p.parseAndCondition()
	if err != nil {
		return sqlExpr{}, err
	}
	for p.keyword("OR") {
		op := "OR"
		p.next()
		right, err := p.parseAndCondition()
		if err != nil {
			return sqlExpr{}, err
		}
		l := left
		left = sqlExpr{kind: "binary", op: op, left: &l, right: &right}
	}
	return left, nil
}

func (p *sqlQueryParser) parseAndCondition() (sqlExpr, error) {
	left, err := p.parseNotCondition()
	if err != nil {
		return sqlExpr{}, err
	}
	for p.keyword("AND") {
		p.next()
		right, err := p.parseNotCondition()
		if err != nil {
			return sqlExpr{}, err
		}
		previous := left
		left = sqlExpr{kind: "binary", op: "AND", left: &previous, right: &right}
	}
	return left, nil
}

func (p *sqlQueryParser) parseNotCondition() (sqlExpr, error) {
	if p.keyword("NOT") {
		p.next()
		operand, err := p.parseNotCondition()
		if err != nil {
			return sqlExpr{}, err
		}
		return sqlExpr{kind: "unary", op: "!", left: &operand}, nil
	}
	return p.parseComparison()
}
func (p *sqlQueryParser) parseComparison() (sqlExpr, error) {
	left, err := p.parseExpr()
	if err != nil {
		return sqlExpr{}, err
	}
	if p.keyword("IS") {
		p.next()
		not := false
		if p.keyword("NOT") {
			not = true
			p.next()
		}
		if err := p.expectKeyword("NULL"); err != nil {
			return sqlExpr{}, err
		}
		return sqlExpr{kind: "binary", op: map[bool]string{false: "IS NULL", true: "IS NOT NULL"}[not], left: &left}, nil
	}
	notComparison := false
	if p.keyword("NOT") {
		notComparison = true
		p.next()
	}
	if p.keyword("IN") {
		p.next()
		if err := p.expectKind(sqlTokenLeftParen, "("); err != nil {
			return sqlExpr{}, err
		}
		values, err := p.parseExprList()
		if err != nil {
			return sqlExpr{}, err
		}
		if err := p.expectKind(sqlTokenRightParen, ")"); err != nil {
			return sqlExpr{}, err
		}
		return sqlExpr{kind: "in", op: map[bool]string{false: "IN", true: "NOT IN"}[notComparison], left: &left, args: values}, nil
	}
	if p.keyword("BETWEEN") {
		p.next()
		lower, err := p.parseExpr()
		if err != nil {
			return sqlExpr{}, err
		}
		if err := p.expectKeyword("AND"); err != nil {
			return sqlExpr{}, err
		}
		upper, err := p.parseExpr()
		if err != nil {
			return sqlExpr{}, err
		}
		return sqlExpr{kind: "between", op: map[bool]string{false: "BETWEEN", true: "NOT BETWEEN"}[notComparison], left: &left, args: []sqlExpr{lower, upper}}, nil
	}
	if notComparison {
		return sqlExpr{}, p.expected(p.current(), "IN or BETWEEN after NOT", []string{"IN", "BETWEEN"})
	}
	if p.keyword("LIKE") {
		p.next()
		right, err := p.parseExpr()
		if err != nil {
			return sqlExpr{}, err
		}
		return sqlExpr{kind: "binary", op: "LIKE", left: &left, right: &right}, nil
	}
	switch p.current().kind {
	case sqlTokenEqual, sqlTokenNotEqual, sqlTokenLess, sqlTokenLessEqual, sqlTokenGreater, sqlTokenGreaterEqual:
		op := p.current().text
		p.next()
		right, err := p.parseExpr()
		if err != nil {
			return sqlExpr{}, err
		}
		if err := p.validateSQLLiteralComparison(left, right, p.previous()); err != nil {
			return sqlExpr{}, err
		}
		return sqlExpr{kind: "binary", op: op, left: &left, right: &right}, nil
	}
	return left, nil
}

func (p *sqlQueryParser) validateSQLLiteralComparison(left, right sqlExpr, token sqlToken) error {
	if left.kind != "literal" || right.kind != "literal" || left.value == nil || right.value == nil {
		return nil
	}
	leftType, rightType := sqlLiteralTypeName(left.value), sqlLiteralTypeName(right.value)
	if leftType == rightType || leftType == "NUMBER" && rightType == "NUMBER" {
		return nil
	}
	return p.diagnostic(token, "cannot compare "+leftType+" with "+rightType+"; compare values of the same type or convert the input before binding it")
}

func sqlLiteralTypeName(value interface{}) string {
	if _, ok := sqlNumber(value); ok {
		return "NUMBER"
	}
	switch value.(type) {
	case sqlDecimal:
		return "DECIMAL"
	case sqlDate:
		return "DATE"
	case string:
		return "TEXT"
	case bool:
		return "BOOLEAN"
	case time.Time:
		return "TIMESTAMP"
	}
	return strings.ToUpper(fmt.Sprintf("%T", value))
}
func (p *sqlQueryParser) parseExpr() (sqlExpr, error) { return p.parseAdditiveExpr() }

func (p *sqlQueryParser) parseAdditiveExpr() (sqlExpr, error) {
	left, err := p.parseMultiplicativeExpr()
	if err != nil {
		return sqlExpr{}, err
	}
	for p.current().kind == sqlTokenPlus || p.current().kind == sqlTokenMinus {
		op := p.current().text
		p.next()
		right, err := p.parseMultiplicativeExpr()
		if err != nil {
			return sqlExpr{}, err
		}
		previous := left
		left = sqlExpr{kind: "binary", op: op, left: &previous, right: &right}
	}
	return left, nil
}

func (p *sqlQueryParser) parseMultiplicativeExpr() (sqlExpr, error) {
	left, err := p.parseUnaryExpr()
	if err != nil {
		return sqlExpr{}, err
	}
	for p.current().kind == sqlTokenStar || p.current().kind == sqlTokenSlash || p.current().kind == sqlTokenPercent {
		op := p.current().text
		p.next()
		right, err := p.parseUnaryExpr()
		if err != nil {
			return sqlExpr{}, err
		}
		previous := left
		left = sqlExpr{kind: "binary", op: op, left: &previous, right: &right}
	}
	return left, nil
}

func (p *sqlQueryParser) parseUnaryExpr() (sqlExpr, error) {
	if p.current().kind == sqlTokenBang || p.current().kind == sqlTokenMinus {
		token := p.current()
		p.next()
		operand, err := p.parseUnaryExpr()
		if err != nil {
			return sqlExpr{}, err
		}
		return sqlExpr{kind: "unary", op: token.text, left: &operand}, nil
	}
	return p.parsePrimary()
}

func (p *sqlQueryParser) parseCaseExpression(token sqlToken) (sqlExpr, error) {
	expression := sqlExpr{kind: "case", token: token}
	if !p.keyword("WHEN") {
		operand, err := p.parseExpr()
		if err != nil {
			return sqlExpr{}, err
		}
		expression.left = &operand
	}
	if !p.keyword("WHEN") {
		return sqlExpr{}, p.expected(p.current(), "WHEN in CASE expression", []string{"WHEN"})
	}
	for p.keyword("WHEN") {
		p.next()
		var when sqlExpr
		var err error
		if expression.left == nil {
			when, err = p.parseCondition()
		} else {
			when, err = p.parseExpr()
		}
		if err != nil {
			return sqlExpr{}, err
		}
		if err := p.expectKeyword("THEN"); err != nil {
			return sqlExpr{}, err
		}
		then, err := p.parseExpr()
		if err != nil {
			return sqlExpr{}, err
		}
		expression.cases = append(expression.cases, sqlCaseWhen{when: when, then: then})
	}
	if p.keyword("ELSE") {
		p.next()
		fallback, err := p.parseExpr()
		if err != nil {
			return sqlExpr{}, err
		}
		expression.right = &fallback
	}
	if err := p.expectKeyword("END"); err != nil {
		return sqlExpr{}, err
	}
	return expression, nil
}

func (p *sqlQueryParser) parsePrimary() (sqlExpr, error) {
	token := p.current()
	if token.kind == sqlTokenLeftParen {
		p.next()
		expression, err := p.parseCondition()
		if err != nil {
			return sqlExpr{}, err
		}
		if err := p.expectKind(sqlTokenRightParen, ")"); err != nil {
			return sqlExpr{}, err
		}
		return expression, nil
	}
	if token.kind == sqlTokenStar {
		p.next()
		return sqlExpr{kind: "star"}, nil
	}
	if token.kind == sqlTokenString {
		p.next()
		return sqlExpr{kind: "literal", value: token.text}, nil
	}
	if token.kind == sqlTokenNumber {
		p.next()
		if strings.ContainsAny(token.text, ".eE") {
			v, e := strconv.ParseFloat(token.text, 64)
			if e != nil {
				return sqlExpr{}, p.diagnostic(token, "invalid number")
			}
			return sqlExpr{kind: "literal", value: v}, nil
		}
		v, e := strconv.ParseInt(token.text, 10, 64)
		if e != nil {
			return sqlExpr{}, p.diagnostic(token, "invalid integer")
		}
		return sqlExpr{kind: "literal", value: v}, nil
	}
	if token.kind == sqlTokenParameter {
		p.next()
		index, err := strconv.Atoi(token.text)
		if err != nil || index < 1 {
			return sqlExpr{}, p.diagnostic(token, "parameter indexes start at $1")
		}
		if p.allowParameters {
			return sqlExpr{kind: "parameter", value: index, token: token}, nil
		}
		if index > len(p.parameters) {
			return sqlExpr{}, p.diagnostic(token, fmt.Sprintf("parameter $%d has no supplied parameter (received %d)", index, len(p.parameters)))
		}
		return sqlExpr{kind: "literal", value: p.parameters[index-1]}, nil
	}
	if token.kind == sqlTokenIdentifier {
		p.next()
		upper := strings.ToUpper(token.text)
		if upper == "CASE" {
			return p.parseCaseExpression(token)
		}
		if upper == "DATE" {
			value := p.current()
			if value.kind != sqlTokenString {
				return sqlExpr{}, p.expected(value, "a YYYY-MM-DD date string after DATE", nil)
			}
			p.next()
			parsed, err := time.Parse("2006-01-02", value.text)
			if err != nil || parsed.Format("2006-01-02") != value.text {
				return sqlExpr{}, p.diagnostic(value, "DATE requires a YYYY-MM-DD value such as '2026-08-22'")
			}
			return sqlExpr{kind: "literal", value: sqlDate(value.text)}, nil
		}
		if upper == "DECIMAL" {
			value := p.current()
			if value.kind != sqlTokenString {
				return sqlExpr{}, p.expected(value, "a decimal string after DECIMAL", nil)
			}
			p.next()
			decimal, ok := parseSQLDecimal(value.text)
			if !ok {
				return sqlExpr{}, p.diagnostic(value, "DECIMAL requires digits with an optional fractional part, such as '123.45'")
			}
			return sqlExpr{kind: "literal", value: decimal}, nil
		}
		if upper == "TIMESTAMP" {
			value := p.current()
			if value.kind != sqlTokenString {
				return sqlExpr{}, p.expected(value, "an RFC3339 timestamp string after TIMESTAMP", nil)
			}
			p.next()
			parsed, err := time.Parse(time.RFC3339Nano, value.text)
			if err != nil {
				return sqlExpr{}, p.diagnostic(value, "TIMESTAMP requires an RFC3339 value such as '2026-08-22T09:00:00Z'")
			}
			return sqlExpr{kind: "literal", value: parsed}, nil
		}
		if upper == "CAST" && p.current().kind == sqlTokenLeftParen {
			p.next()
			argument, err := p.parseExpr()
			if err != nil {
				return sqlExpr{}, err
			}
			if err := p.expectKeyword("AS"); err != nil {
				return sqlExpr{}, err
			}
			target := p.current()
			if target.kind != sqlTokenIdentifier {
				return sqlExpr{}, p.expected(target, "a CAST target type (TEXT, NUMBER, DECIMAL, BOOLEAN, DATE, or TIMESTAMP)", nil)
			}
			p.next()
			targetType := strings.ToUpper(target.text)
			switch targetType {
			case "TEXT", "NUMBER", "DECIMAL", "BOOLEAN", "DATE", "TIMESTAMP":
			default:
				return sqlExpr{}, p.diagnostic(target, fmt.Sprintf("unsupported CAST target %q; expected TEXT, NUMBER, DECIMAL, BOOLEAN, DATE, or TIMESTAMP", target.text))
			}
			if err := p.expectKind(sqlTokenRightParen, ")"); err != nil {
				return sqlExpr{}, err
			}
			return sqlExpr{kind: "cast", name: targetType, args: []sqlExpr{argument}, value: token}, nil
		}
		if upper == "NULL" {
			return sqlExpr{kind: "literal", value: nil}, nil
		}
		if upper == "TRUE" {
			return sqlExpr{kind: "literal", value: true}, nil
		}
		if upper == "FALSE" {
			return sqlExpr{kind: "literal", value: false}, nil
		}
		if p.current().kind == sqlTokenLeftParen {
			p.next()
			var args []sqlExpr
			if p.current().kind != sqlTokenRightParen {
				for {
					arg, err := p.parseExpr()
					if err != nil {
						return sqlExpr{}, err
					}
					args = append(args, arg)
					if p.current().kind != sqlTokenComma {
						break
					}
					p.next()
				}
			}
			if err := p.expectKind(sqlTokenRightParen, ")"); err != nil {
				return sqlExpr{}, err
			}
			expr := sqlExpr{kind: "func", name: upper, args: args, token: token}
			if p.keyword("OVER") {
				p.next()
				if err := p.expectKind(sqlTokenLeftParen, "("); err != nil {
					return sqlExpr{}, err
				}
				window := &sqlWindow{}
				if p.keyword("PARTITION") {
					p.next()
					if err := p.expectKeyword("BY"); err != nil {
						return sqlExpr{}, err
					}
					values, err := p.parseExprList()
					if err != nil {
						return sqlExpr{}, err
					}
					window.partition = values
				}
				if p.keyword("ORDER") {
					p.next()
					if err := p.expectKeyword("BY"); err != nil {
						return sqlExpr{}, err
					}
					values, err := p.parseOrder()
					if err != nil {
						return sqlExpr{}, err
					}
					window.order = values
				}
				if p.keyword("ROWS") || p.keyword("RANGE") {
					frame, err := p.parseSQLWindowFrame(strings.ToUpper(p.current().text))
					if err != nil {
						return sqlExpr{}, err
					}
					window.frame = &frame
				}
				if err := p.expectKind(sqlTokenRightParen, ")"); err != nil {
					return sqlExpr{}, err
				}
				expr.window = window
			}
			return expr, nil
		}
		expr := sqlExpr{kind: "field", name: token.text}
		if p.current().kind == sqlTokenDot {
			p.next()
			name, err := p.expectIdentifier("a field name", nil)
			if err != nil {
				return sqlExpr{}, err
			}
			expr.qualifier = token.text
			expr.name = name.text
		}
		return expr, nil
	}
	return sqlExpr{}, p.expected(token, "a column, literal, function, or *", nil)
}

func (p *sqlQueryParser) parseSQLWindowFrame(kind string) (sqlWindowFrame, error) {
	p.next()
	if err := p.expectKeyword("BETWEEN"); err != nil {
		return sqlWindowFrame{}, err
	}
	start, err := p.parseSQLWindowFrameBound("frame start")
	if err != nil {
		return sqlWindowFrame{}, err
	}
	if err := p.expectKeyword("AND"); err != nil {
		return sqlWindowFrame{}, err
	}
	end, err := p.parseSQLWindowFrameBound("frame end")
	if err != nil {
		return sqlWindowFrame{}, err
	}
	if start.kind == "UNBOUNDED FOLLOWING" {
		return sqlWindowFrame{}, p.diagnostic(p.previous(), "a "+kind+" frame cannot start with UNBOUNDED FOLLOWING")
	}
	if end.kind == "UNBOUNDED PRECEDING" {
		return sqlWindowFrame{}, p.diagnostic(p.previous(), "a "+kind+" frame cannot end with UNBOUNDED PRECEDING")
	}
	if sqlWindowFrameBoundPosition(start) > sqlWindowFrameBoundPosition(end) {
		return sqlWindowFrame{}, p.diagnostic(p.previous(), kind+" frame start must not follow its end")
	}
	return sqlWindowFrame{kind: kind, start: start, end: end}, nil
}

func (p *sqlQueryParser) parseSQLWindowFrameBound(name string) (sqlWindowFrameBound, error) {
	if p.keyword("UNBOUNDED") {
		p.next()
		if p.keyword("PRECEDING") {
			p.next()
			return sqlWindowFrameBound{kind: "UNBOUNDED PRECEDING"}, nil
		}
		if p.keyword("FOLLOWING") {
			p.next()
			return sqlWindowFrameBound{kind: "UNBOUNDED FOLLOWING"}, nil
		}
		return sqlWindowFrameBound{}, p.expected(p.current(), "PRECEDING or FOLLOWING after UNBOUNDED", []string{"PRECEDING", "FOLLOWING"})
	}
	if p.keyword("CURRENT") {
		p.next()
		if err := p.expectKeyword("ROW"); err != nil {
			return sqlWindowFrameBound{}, err
		}
		return sqlWindowFrameBound{kind: "CURRENT ROW"}, nil
	}
	offset, err := p.parseInteger(name)
	if err != nil {
		return sqlWindowFrameBound{}, err
	}
	if p.keyword("PRECEDING") {
		p.next()
		return sqlWindowFrameBound{kind: "PRECEDING", offset: offset}, nil
	}
	if p.keyword("FOLLOWING") {
		p.next()
		return sqlWindowFrameBound{kind: "FOLLOWING", offset: offset}, nil
	}
	return sqlWindowFrameBound{}, p.expected(p.current(), "PRECEDING or FOLLOWING after "+name, []string{"PRECEDING", "FOLLOWING"})
}

func sqlWindowFrameBoundPosition(bound sqlWindowFrameBound) int {
	switch bound.kind {
	case "UNBOUNDED PRECEDING":
		return -1 << 30
	case "PRECEDING":
		return -bound.offset
	case "CURRENT ROW":
		return 0
	case "FOLLOWING":
		return bound.offset
	case "UNBOUNDED FOLLOWING":
		return 1 << 30
	}
	return 0
}

func sqlWindowFrameBounds(frame *sqlWindowFrame, position, length int) (int, int) {
	if length == 0 {
		return 0, -1
	}
	if frame == nil {
		return 0, position
	}
	resolve := func(bound sqlWindowFrameBound) int {
		switch bound.kind {
		case "UNBOUNDED PRECEDING":
			return 0
		case "PRECEDING":
			return position - bound.offset
		case "CURRENT ROW":
			return position
		case "FOLLOWING":
			return position + bound.offset
		case "UNBOUNDED FOLLOWING":
			return length - 1
		}
		return position
	}
	start, end := resolve(frame.start), resolve(frame.end)
	if start < 0 {
		start = 0
	}
	if end >= length {
		end = length - 1
	}
	return start, end
}

func sqlRangeWindowFrameBounds(frame *sqlWindowFrame, order sqlOrder, values []interface{}, position int) (int, int, error) {
	if len(values) == 0 {
		return 0, -1, nil
	}
	current, ok := sqlNumber(values[position])
	if !ok {
		return 0, -1, fmt.Errorf("RANGE frame ORDER BY value must be numeric")
	}
	start, err := sqlRangeWindowFrameBoundValue(frame.start, current, order.desc)
	if err != nil {
		return 0, -1, err
	}
	end, err := sqlRangeWindowFrameBoundValue(frame.end, current, order.desc)
	if err != nil {
		return 0, -1, err
	}
	minimum, maximum := start, end
	if minimum > maximum {
		minimum, maximum = maximum, minimum
	}
	first, last := -1, -1
	for index, value := range values {
		number, ok := sqlNumber(value)
		if !ok {
			return 0, -1, fmt.Errorf("RANGE frame ORDER BY value must be numeric")
		}
		if number < minimum || number > maximum {
			continue
		}
		if first < 0 {
			first = index
		}
		last = index
	}
	if first < 0 {
		return 0, -1, nil
	}
	return first, last, nil
}

func sqlRangeWindowFrameBoundValue(bound sqlWindowFrameBound, current float64, descending bool) (float64, error) {
	switch bound.kind {
	case "UNBOUNDED PRECEDING":
		if descending {
			return math.Inf(1), nil
		}
		return math.Inf(-1), nil
	case "UNBOUNDED FOLLOWING":
		if descending {
			return math.Inf(-1), nil
		}
		return math.Inf(1), nil
	case "CURRENT ROW":
		return current, nil
	case "PRECEDING":
		if descending {
			return current + float64(bound.offset), nil
		}
		return current - float64(bound.offset), nil
	case "FOLLOWING":
		if descending {
			return current - float64(bound.offset), nil
		}
		return current + float64(bound.offset), nil
	default:
		return 0, fmt.Errorf("invalid RANGE frame bound %q", bound.kind)
	}
}

func sqlWindowAggregate(name string, values []float64) interface{} {
	if len(values) == 0 {
		return nil
	}
	result := values[0]
	for _, value := range values[1:] {
		switch name {
		case "SUM", "AVG":
			result += value
		case "MIN":
			if value < result {
				result = value
			}
		case "MAX":
			if value > result {
				result = value
			}
		}
	}
	if name == "AVG" {
		return result / float64(len(values))
	}
	return result
}
func (p *sqlQueryParser) parseInteger(name string) (int, error) {
	token := p.current()
	if token.kind != sqlTokenNumber {
		return 0, p.expected(token, name+" integer", nil)
	}
	p.next()
	value, err := strconv.Atoi(token.text)
	if err != nil || value < 0 {
		return 0, p.diagnostic(token, name+" must be a non-negative integer")
	}
	return value, nil
}
func (p *sqlQueryParser) current() sqlToken {
	if p.index >= len(p.tokens) {
		return sqlToken{kind: sqlTokenEOF, line: 1, column: 1, endColumn: 1}
	}
	return p.tokens[p.index]
}
func (p *sqlQueryParser) peek() sqlToken {
	if p.index+1 >= len(p.tokens) {
		return sqlToken{kind: sqlTokenEOF}
	}
	return p.tokens[p.index+1]
}
func (p *sqlQueryParser) previous() sqlToken {
	if p.index == 0 {
		return p.current()
	}
	return p.tokens[p.index-1]
}
func (p *sqlQueryParser) next() { p.index++ }
func (p *sqlQueryParser) keyword(word string) bool {
	return p.current().kind == sqlTokenIdentifier && strings.EqualFold(p.current().text, word)
}
func (p *sqlQueryParser) expectKeyword(word string) error {
	if p.keyword(word) {
		p.next()
		return nil
	}
	return p.expected(p.current(), word, []string{word})
}
func (p *sqlQueryParser) expectIdentifier(expected string, candidates []string) (sqlToken, error) {
	if p.current().kind != sqlTokenIdentifier {
		return sqlToken{}, p.expected(p.current(), expected, candidates)
	}
	v := p.current()
	p.next()
	return v, nil
}
func (p *sqlQueryParser) expectKind(kind sqlTokenKind, expected string) error {
	if p.current().kind == kind {
		p.next()
		return nil
	}
	return p.expected(p.current(), expected, nil)
}
func (p *sqlQueryParser) expected(token sqlToken, expected string, candidates []string) error {
	s := ""
	if token.kind == sqlTokenIdentifier {
		s = nearestSQLName(token.text, candidates)
	}
	return &SQLDiagnostic{Message: "unexpected " + token.Display() + "; expected " + expected, Line: token.line, Column: token.column, EndColumn: token.endColumn, Suggestion: s}
}
func (p *sqlQueryParser) diagnostic(token sqlToken, message string) error {
	return sqlTokenDiagnostic(token, message)
}
func sqlClauseKeyword(value string) bool {
	switch strings.ToUpper(value) {
	case "EXPLAIN", "ANALYZE", "SELECT", "DISTINCT", "FROM", "JOIN", "LEFT", "RIGHT", "FULL", "CROSS", "WHERE", "GROUP", "HAVING", "ORDER", "LIMIT", "FETCH", "OFFSET", "ON", "AS", "INNER", "OUTER", "ASC", "DESC", "UNION", "INTERSECT", "EXCEPT", "ALL", "RECURSIVE", "EXTERNAL":
		return true
	}
	return false
}
func sqlSuspectedClauseTypo(value string) bool {
	return nearestSQLName(value, []string{"SELECT", "FROM", "JOIN", "WHERE", "GROUP", "HAVING", "ORDER", "LIMIT", "OFFSET"}) != ""
}

type sqlExecRow struct {
	sources  map[string]SQLRow
	order    []string
	ordinals map[string]int
}

func executeSQLQuery(q *sqlQuery, resolver SQLSourceResolver, ctes map[string][]SQLRow) (SQLQueryResult, error) {
	return executeSQLQueryWithMetrics(q, resolver, ctes, nil, nil)
}

type sqlExecutionControl struct {
	ctx      context.Context
	maxRows  int
	options  SQLQueryOptions
	joinWork int
	sources  map[string][]SQLRow
}

func newSQLExecutionControl(ctx context.Context, options SQLQueryOptions) (*sqlExecutionControl, context.CancelFunc, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if options.MaxRows < 0 || options.MaxJoinWork < 0 || options.MaxJoinBytes < 0 || options.MaxResultBytes < 0 || options.MaxSortBytes < 0 || options.MaxGroupBytes < 0 || options.MaxSetBytes < 0 || options.MaxSpillBytes < 0 || options.MaxRecursionDepth < 0 || options.Timeout < 0 || options.SlowQueryThreshold < 0 || options.Workers < 0 {
		return nil, func() {}, fmt.Errorf("SQL query budgets cannot be negative")
	}
	if options.Timeout > 0 {
		ctx, cancel := context.WithTimeout(ctx, options.Timeout)
		return &sqlExecutionControl{ctx: ctx, maxRows: sqlQueryMaxRows(options), options: options, sources: map[string][]SQLRow{}}, cancel, nil
	}
	return &sqlExecutionControl{ctx: ctx, maxRows: sqlQueryMaxRows(options), options: options, sources: map[string][]SQLRow{}}, func() {}, nil
}

func sqlQueryMaxRows(options SQLQueryOptions) int {
	if options.MaxRows > 0 {
		return options.MaxRows
	}
	return maxSQLQueryRows
}
func (control *sqlExecutionControl) check() error {
	if control == nil {
		return nil
	}
	return control.ctx.Err()
}
func (control *sqlExecutionControl) addJoinWork(work int) error {
	if control == nil || control.options.MaxJoinWork == 0 {
		return control.check()
	}
	control.joinWork += work
	if control.joinWork > control.options.MaxJoinWork {
		return fmt.Errorf("SQL join work budget exceeded: %d comparisons, maximum %d", control.joinWork, control.options.MaxJoinWork)
	}
	return control.check()
}

type sqlIndexedJoinPlan struct {
	useHash          bool
	probes           int
	estimatedMatches int
	indexCost        int
	hashCost         int
}

func (plan sqlIndexedJoinPlan) detail() string {
	selected := "INDEX JOIN"
	if plan.useHash {
		selected = "HASH JOIN"
	}
	return fmt.Sprintf("exact index postings: probes=%d matches=%d index_cost=%d hash_cost=%d; selected %s", plan.probes, plan.estimatedMatches, plan.indexCost, plan.hashCost, selected)
}

// sqlPlanIndexedJoin compares a cloned posting-list probe with a hash build
// using exact snapshot index cardinalities. Equality output work is common to
// both algorithms; the extra index cloning cost is why a hot key can favor a
// hash join even when an equality index exists.
func sqlPlanIndexedJoin(resolver SQLSourceResolver, source sqlSource, rightField, leftQualifier, leftField string, left []sqlExecRow) (sqlIndexedJoinPlan, bool, error) {
	statsResolver, hasStats := resolver.(JSONIndexStatsResolver)
	valueResolver, hasValues := resolver.(IndexValueEstimator)
	if !hasStats || !hasValues || source.kind != "CACHE" {
		return sqlIndexedJoinPlan{}, false, nil
	}
	stats, available, err := statsResolver.SQLJSONIndexStats(source.key, rightField)
	if err != nil {
		return sqlIndexedJoinPlan{}, false, err
	}
	if !available {
		return sqlIndexedJoinPlan{}, false, nil
	}

	frequencies := make(map[string]int, len(left))
	values := make(map[string]interface{}, len(left))
	probes := 0
	for _, row := range left {
		value := sqlField(row, leftQualifier, leftField)
		key, ok := sqlHashJoinKey(value)
		if !ok {
			continue
		}
		probes = sqlSaturatingAdd(probes, 1)
		frequencies[key] = sqlSaturatingAdd(frequencies[key], 1)
		values[key] = value
	}
	matches := 0
	for key, count := range frequencies {
		rows, exact, available, err := valueResolver.SQLJSONIndexValueEstimate(source.key, rightField, values[key])
		if err != nil {
			return sqlIndexedJoinPlan{}, false, err
		}
		if !available || !exact || rows < 0 {
			return sqlIndexedJoinPlan{}, false, nil
		}
		matches = sqlSaturatingAdd(matches, sqlSaturatingMultiply(count, rows))
	}
	sourceRows := sqlSaturatingAdd(stats.Rows, stats.NullRows)
	plan := sqlIndexedJoinPlan{
		probes:           probes,
		estimatedMatches: matches,
		indexCost:        sqlSaturatingAdd(probes, sqlSaturatingMultiply(matches, 2)),
		hashCost:         sqlSaturatingAdd(sqlSaturatingAdd(sourceRows, probes), matches),
	}
	plan.useHash = plan.hashCost < plan.indexCost
	return plan, true, nil
}

func sqlSaturatingAdd(left, right int) int {
	if left < 0 || right < 0 {
		return int(^uint(0) >> 1)
	}
	maximum := int(^uint(0) >> 1)
	if left > maximum-right {
		return maximum
	}
	return left + right
}

func sqlSaturatingMultiply(left, right int) int {
	if left < 0 || right < 0 {
		return int(^uint(0) >> 1)
	}
	if left == 0 || right == 0 {
		return 0
	}
	maximum := int(^uint(0) >> 1)
	if left > maximum/right {
		return maximum
	}
	return left * right
}

type sqlExecutionMetrics struct {
	steps    []SQLExplainStep
	adaptive *AdaptivePlanner
}

func (metrics *sqlExecutionMetrics) record(node, detail string, inputRows, outputRows int, started time.Time) {
	metrics.recordBytes(node, detail, inputRows, outputRows, -1, -1, started)
}

func (metrics *sqlExecutionMetrics) recordBytes(node, detail string, inputRows, outputRows, inputBytes, outputBytes int, started time.Time) {
	if metrics == nil {
		return
	}
	elapsed := time.Since(started).Nanoseconds()
	step := SQLExplainStep{
		Node:             node,
		Detail:           detail,
		ActualInputRows:  sqlExplainIntPointer(inputRows),
		ActualOutputRows: sqlExplainIntPointer(outputRows),
		ElapsedNanos:     &elapsed,
	}
	if inputBytes >= 0 {
		step.ActualInputBytes = sqlExplainIntPointer(inputBytes)
	}
	if outputBytes >= 0 {
		step.ActualOutputBytes = sqlExplainIntPointer(outputBytes)
	}
	metrics.steps = append(metrics.steps, step)
}

func (metrics *sqlExecutionMetrics) recordEstimated(node, detail string, estimatedRows *int, inputRows, outputRows int, started time.Time) {
	metrics.record(node, detail, inputRows, outputRows, started)
	if metrics != nil && estimatedRows != nil {
		step := &metrics.steps[len(metrics.steps)-1]
		step.EstimatedRows = sqlExplainIntPointer(*estimatedRows)
		step.EstimateErrorRows = sqlExplainIntPointer(outputRows - *estimatedRows)
		if *estimatedRows != 0 {
			percent := float64(outputRows-*estimatedRows) * 100 / float64(*estimatedRows)
			step.EstimateErrorPercent = &percent
		}
	}
}

func sqlExplainIntPointer(value int) *int { return &value }

func (metrics *sqlExecutionMetrics) recordScan(source sqlSource, outputRows int, started time.Time) {
	metrics.record("SCAN", sqlExplainSource(source), 0, outputRows, started)
	if metrics == nil || source.kind != "VALUES" {
		return
	}
	metrics.steps[len(metrics.steps)-1].EstimatedRows = sqlExplainIntPointer(len(source.values))
}

func (metrics *sqlExecutionMetrics) recordScanRows(source sqlSource, rows []SQLRow, started time.Time) {
	metrics.recordBytes("SCAN", sqlExplainSource(source), 0, len(rows), 0, sqlRowsBytes(rows), started)
	if metrics == nil || source.kind != "VALUES" {
		return
	}
	metrics.steps[len(metrics.steps)-1].EstimatedRows = sqlExplainIntPointer(len(source.values))
}

// executeSQLReorderedInnerHashJoins chooses a connected inner-hash-join order
// from the exact cardinalities observed for this snapshot. It deliberately
// leaves outer joins, cross joins, non-equality joins, and base-only WHERE
// pushdown on the established executor: those forms have ordering or null
// preservation semantics that must not be changed by a cost optimization.
func executeSQLReorderedInnerHashJoins(q *sqlQuery, resolver SQLSourceResolver, ctes map[string][]SQLRow, metrics *sqlExecutionMetrics, control *sqlExecutionControl, maxRows int) ([]sqlExecRow, bool, error) {
	if q.from == nil || len(q.joins) < 2 || q.where.kind != "" {
		return nil, false, nil
	}
	sources := []sqlSource{*q.from}
	aliases := []string{q.from.alias}
	for _, join := range q.joins {
		if join.kind != "INNER" || join.source.alias == "" {
			return nil, false, nil
		}
		if _, _, _, ok := sqlHashJoinFields(join.on, aliases, join.source.alias); !ok {
			return nil, false, nil
		}
		sources = append(sources, join.source)
		aliases = append(aliases, join.source.alias)
	}
	for left := range sources {
		for right := left + 1; right < len(sources); right++ {
			if sources[left].alias == sources[right].alias {
				return nil, false, nil
			}
		}
	}

	rowsByAlias := make(map[string][]sqlExecRow, len(sources))
	for _, source := range sources {
		started := time.Now()
		resolved, err := resolveSQLSource(source, resolver, ctes, metrics, control)
		if err != nil {
			return nil, true, err
		}
		if len(resolved) > maxRows {
			return nil, true, fmt.Errorf("SQL source %q exceeds the %d row limit", source.alias, maxRows)
		}
		rowsByAlias[source.alias] = wrapSQLSource(source, resolved)
		metrics.recordScanRows(source, resolved, started)
	}

	start := 0
	for index := 1; index < len(sources); index++ {
		if len(rowsByAlias[sources[index].alias]) < len(rowsByAlias[sources[start].alias]) {
			start = index
		}
	}
	rows := rowsByAlias[sources[start].alias]
	selected := map[string]bool{sources[start].alias: true}
	selectedAliases := []string{sources[start].alias}
	order := []string{fmt.Sprintf("%s (%d rows)", sources[start].alias, len(rows))}
	startedPlan := time.Now()
	for len(selected) < len(sources) {
		bestSource := -1
		bestJoin := -1
		var leftQualifier, leftField, rightField string
		for sourceIndex, source := range sources {
			if selected[source.alias] {
				continue
			}
			for joinIndex, join := range q.joins {
				qualifier, field, targetField, ok := sqlHashJoinFields(join.on, selectedAliases, source.alias)
				if !ok {
					continue
				}
				if bestSource == -1 || len(rowsByAlias[source.alias]) < len(rowsByAlias[sources[bestSource].alias]) || (len(rowsByAlias[source.alias]) == len(rowsByAlias[sources[bestSource].alias]) && sourceIndex < bestSource) {
					bestSource, bestJoin = sourceIndex, joinIndex
					leftQualifier, leftField, rightField = qualifier, field, targetField
				}
			}
		}
		if bestSource == -1 || bestJoin == -1 {
			return nil, false, nil
		}
		join := q.joins[bestJoin]
		right := rowsByAlias[sources[bestSource].alias]
		inputRows := len(rows) + len(right)
		joinStarted := time.Now()
		buckets := make(map[string][]int, len(right))
		for rightIndex, row := range right {
			if err := control.addJoinWork(1); err != nil {
				return nil, true, err
			}
			if key, ok := sqlHashJoinKey(sqlField(row, sources[bestSource].alias, rightField)); ok {
				buckets[key] = append(buckets[key], rightIndex)
			}
		}
		var next []sqlExecRow
		for _, left := range rows {
			key, ok := sqlHashJoinKey(sqlField(left, leftQualifier, leftField))
			if !ok {
				continue
			}
			for _, rightIndex := range buckets[key] {
				if err := control.addJoinWork(1); err != nil {
					return nil, true, err
				}
				next = append(next, mergeSQLRows(left, right[rightIndex]))
				if len(next) > maxRows {
					return nil, true, fmt.Errorf("SQL join exceeds the %d row limit; add a more selective WHERE or ON condition", maxRows)
				}
			}
		}
		detail := "INNER JOIN " + sqlExplainSource(sources[bestSource]) + " ON " + sqlExplainExpression(join.on)
		metrics.record("HASH JOIN", detail, inputRows, len(next), joinStarted)
		rows = next
		selected[sources[bestSource].alias] = true
		selectedAliases = append(selectedAliases, sources[bestSource].alias)
		order = append(order, fmt.Sprintf("%s (%d rows)", sources[bestSource].alias, len(right)))
	}
	// The physical plan may begin with a different source, but relational
	// callers historically receive deterministic nested-source order unless an
	// explicit ORDER BY says otherwise. Restore that order using immutable row
	// ordinals captured while each source was wrapped.
	sort.SliceStable(rows, func(left, right int) bool {
		for _, alias := range aliases {
			leftOrdinal, rightOrdinal := rows[left].ordinals[alias], rows[right].ordinals[alias]
			if leftOrdinal != rightOrdinal {
				return leftOrdinal < rightOrdinal
			}
		}
		return false
	})
	metrics.record("JOIN REORDER", "cardinality order: "+strings.Join(order, " -> "), len(sources), len(rows), startedPlan)
	return rows, true, nil
}

func executeSQLQueryWithMetrics(q *sqlQuery, resolver SQLSourceResolver, ctes map[string][]SQLRow, metrics *sqlExecutionMetrics, control *sqlExecutionControl) (SQLQueryResult, error) {
	if err := control.check(); err != nil {
		return SQLQueryResult{}, err
	}
	maxRows := maxSQLQueryRows
	if control != nil {
		maxRows = control.maxRows
	}
	if ctes == nil {
		ctes = map[string][]SQLRow{}
	}
	for _, cte := range q.ctes {
		var rows []SQLRow
		var err error
		if cte.recursive {
			rows, err = executeSQLRecursiveCTE(cte, resolver, ctes, metrics, control, maxRows)
		} else if cte.query != nil {
			r, e := executeSQLQueryWithMetrics(cte.query, resolver, ctes, metrics, control)
			err = e
			if err == nil {
				rows, err = sqlCTEOutputRows(cte, r)
			}
		} else {
			rows = valuesSQLRows(cte.values, cte.columns)
		}
		if err != nil {
			return SQLQueryResult{}, err
		}
		ctes[cte.name] = rows
	}
	if result, handled, spillErr := executeSQLSpillHashJoin(q, resolver, control, metrics); handled {
		return result, spillErr
	}
	if result, handled, streamErr := executeSQLStreamedSpilledGroupAggregate(q, resolver, control, metrics, nil); handled {
		return result, streamErr
	}
	functions, _ := resolver.(SQLFunctionResolver)
	var started time.Time
	rows, reordered, err := executeSQLReorderedInnerHashJoins(q, resolver, ctes, metrics, control, maxRows)
	if err != nil {
		return SQLQueryResult{}, err
	}
	pushedWhere := false
	indexOrdered := false
	if !reordered {
		started = time.Now()
		base, ordered, err := resolveSQLOrderedSource(q, resolver)
		indexed := ordered
		if !indexed {
			base, indexed, err = resolveSQLIndexedSource(*q.from, q.where, resolver, metrics)
		}
		if !indexed {
			base, err = resolveSQLSource(*q.from, resolver, ctes, metrics, control)
		}
		if err != nil {
			return SQLQueryResult{}, err
		}
		if len(base) > maxRows {
			return SQLQueryResult{}, fmt.Errorf("SQL source %q exceeds the %d row limit", q.from.alias, maxRows)
		}
		if ordered {
			metrics.record("INDEX ORDER SCAN", sqlExplainSource(*q.from)+" ORDER BY "+sqlExplainOrders(q.orderBy), 0, len(base), started)
			indexOrdered = true
		} else if indexed {
			estimatedRows, err := sqlIndexedEqualityEstimate(*q.from, q.where, resolver)
			if err != nil {
				return SQLQueryResult{}, err
			}
			metrics.recordEstimated("INDEX SCAN", sqlExplainSource(*q.from), estimatedRows, 0, len(base), started)
		} else {
			metrics.recordScanRows(*q.from, base, started)
		}
		rows = wrapSQLSourceWorkers(*q.from, base, control.options.Workers)
		pushedWhere = q.where.kind != "" && sqlCanPushBaseWhere(q)
		if pushedWhere {
			started = time.Now()
			inputRows := len(rows)
			inputBytes := sqlExecRowsBytes(rows)
			values, err := evalSQLExprBatch(q.where, rows, functions)
			if err != nil {
				return SQLQueryResult{}, err
			}
			filtered := rows[:0]
			for index, row := range rows {
				if err := sqlExpressionError(values[index]); err != nil {
					return SQLQueryResult{}, err
				}
				if sqlTruthy(values[index]) {
					filtered = append(filtered, row)
				}
			}
			rows = filtered
			metrics.recordBytes("FILTER", sqlExplainExpression(q.where), inputRows, len(rows), inputBytes, sqlExecRowsBytes(rows), started)
		}
		leftAliases := []string{q.from.alias}
		for _, join := range q.joins {
			started = time.Now()
			leftQualifier, leftField, rightField, hashJoin := sqlHashJoinFields(join.on, leftAliases, join.source.alias)
			rangeLeftQualifier, rangeLeftField, rangeRightField, rangeOperator, rangeJoin := sqlRangeJoinFields(join.on, leftAliases, join.source.alias)
			leftQualifiers, leftFields, rightFields, compositeIndexJoin := sqlCompositeJoinFields(join.on, leftAliases, join.source.alias)
			indexJoin := hashJoin && (join.kind == "INNER" || join.kind == "LEFT")
			rightIndexJoin := hashJoin && join.kind == "RIGHT"
			rightCondition := sqlInnerJoinPushdownCondition(q.where, join)
			rightPushed := rightCondition.kind != ""
			var pushedRight []SQLRow
			if rightPushed {
				var err error
				pushedRight, err = resolveSQLJoinPushedSource(join.source, rightCondition, resolver, ctes, metrics, control, maxRows)
				if err != nil {
					return SQLQueryResult{}, err
				}
			}
			forceHashJoin := false
			if indexJoin && !rightPushed && join.source.kind == "CACHE" {
				plan, planned, err := sqlPlanIndexedJoin(resolver, join.source, rightField, leftQualifier, leftField, rows)
				if err != nil {
					return SQLQueryResult{}, err
				}
				if planned {
					metrics.record("JOIN PLAN", plan.detail(), len(rows), len(rows), time.Now())
					forceHashJoin = plan.useHash
				}
			}
			if compositeIndexJoin && !rightPushed && (join.kind == "INNER" || join.kind == "LEFT") && join.source.kind == "CACHE" {
				if indexed, ok := resolver.(SQLCompositeIndexedSourceResolver); ok {
					// Resolve once with NULLs to establish index availability and to
					// surface malformed JSON even when all left-side keys are NULL.
					_, available, err := indexed.ResolveSQLCompositeIndexedSource(join.source.kind, join.source.key, rightFields, make([]interface{}, len(rightFields)))
					if err != nil {
						return SQLQueryResult{}, err
					}
					if available {
						inputRows := len(rows)
						var next []sqlExecRow
						for _, left := range rows {
							values := make([]interface{}, len(leftFields))
							valid := true
							for index := range leftFields {
								value := sqlField(left, leftQualifiers[index], leftFields[index])
								if _, ok := sqlHashJoinKey(value); !ok {
									valid = false
									break
								}
								values[index] = value
							}
							matched := false
							if valid {
								if err := control.addJoinWork(1); err != nil {
									return SQLQueryResult{}, err
								}
								candidates, _, err := indexed.ResolveSQLCompositeIndexedSource(join.source.kind, join.source.key, rightFields, values)
								if err != nil {
									return SQLQueryResult{}, err
								}
								for _, candidate := range candidates {
									if err := control.addJoinWork(1); err != nil {
										return SQLQueryResult{}, err
									}
									matched = true
									right := sqlExecRow{sources: map[string]SQLRow{join.source.alias: candidate}, order: []string{join.source.alias}}
									next = append(next, mergeSQLRows(left, right))
									if len(next) > maxRows {
										return SQLQueryResult{}, fmt.Errorf("SQL join exceeds the %d row limit; add a more selective WHERE or ON condition", maxRows)
									}
								}
							}
							if join.kind == "LEFT" && !matched {
								empty := sqlExecRow{sources: map[string]SQLRow{join.source.alias: {}}, order: []string{join.source.alias}}
								next = append(next, mergeSQLRows(left, empty))
								if len(next) > maxRows {
									return SQLQueryResult{}, fmt.Errorf("SQL join exceeds the %d row limit; add a more selective WHERE or ON condition", maxRows)
								}
							}
						}
						metrics.record("COMPOSITE INDEX JOIN", join.kind+" JOIN "+sqlExplainSource(join.source)+" ON "+sqlExplainExpression(join.on), inputRows, len(next), started)
						rows = next
						leftAliases = append(leftAliases, join.source.alias)
						continue
					}
				}
			}
			if rangeJoin && !rightPushed && (join.kind == "INNER" || join.kind == "LEFT") && join.source.kind == "CACHE" {
				if indexed, ok := resolver.(SQLRangeIndexedSourceResolver); ok {
					var probe interface{}
					for _, left := range rows {
						if value := sqlField(left, rangeLeftQualifier, rangeLeftField); value != nil {
							probe = value
							break
						}
					}
					if probe != nil {
						_, available, err := indexed.ResolveSQLIndexedRangeSource(join.source.kind, join.source.key, rangeRightField, rangeOperator, probe)
						if err != nil {
							return SQLQueryResult{}, err
						}
						if available {
							inputRows := len(rows)
							var next []sqlExecRow
							for _, left := range rows {
								value := sqlField(left, rangeLeftQualifier, rangeLeftField)
								matched := false
								if value != nil {
									candidates, _, err := indexed.ResolveSQLIndexedRangeSource(join.source.kind, join.source.key, rangeRightField, rangeOperator, value)
									if err != nil {
										return SQLQueryResult{}, err
									}
									for _, candidate := range candidates {
										if err := control.addJoinWork(1); err != nil {
											return SQLQueryResult{}, err
										}
										combined := mergeSQLRows(left, sqlExecRow{sources: map[string]SQLRow{join.source.alias: candidate}, order: []string{join.source.alias}})
										on := evalSQLExpr(join.on, []sqlExecRow{combined}, combined)
										if err := sqlExpressionError(on); err != nil {
											return SQLQueryResult{}, err
										}
										if !sqlTruthy(on) {
											continue
										}
										matched = true
										next = append(next, combined)
										if len(next) > maxRows {
											return SQLQueryResult{}, fmt.Errorf("SQL join exceeds the %d row limit; add a more selective WHERE or ON condition", maxRows)
										}
									}
								}
								if join.kind == "LEFT" && !matched {
									next = append(next, mergeSQLRows(left, sqlExecRow{sources: map[string]SQLRow{join.source.alias: {}}, order: []string{join.source.alias}}))
								}
								if len(next) > maxRows {
									return SQLQueryResult{}, fmt.Errorf("SQL join exceeds the %d row limit; add a more selective WHERE or ON condition", maxRows)
								}
							}
							metrics.record("RANGE INDEX JOIN", join.kind+" JOIN "+sqlExplainSource(join.source)+" ON "+sqlExplainExpression(join.on), inputRows, len(next), started)
							rows = next
							leftAliases = append(leftAliases, join.source.alias)
							continue
						}
					}
				}
			}
			if indexJoin && !rightPushed && !forceHashJoin && join.source.kind == "CACHE" {
				if indexed, ok := resolver.(SQLIndexedSourceResolver); ok {
					// Probe once with NULL to verify that the optional index exists and
					// to surface malformed JSON even when every left key is NULL.
					_, available, err := indexed.ResolveSQLIndexedSource(join.source.kind, join.source.key, rightField, nil)
					if err != nil {
						return SQLQueryResult{}, err
					}
					if available {
						inputRows := len(rows)
						var next []sqlExecRow
						for _, left := range rows {
							value := sqlField(left, leftQualifier, leftField)
							matched := false
							if _, ok := sqlHashJoinKey(value); ok {
								if err := control.addJoinWork(1); err != nil {
									return SQLQueryResult{}, err
								}
								candidates, _, err := indexed.ResolveSQLIndexedSource(join.source.kind, join.source.key, rightField, value)
								if err != nil {
									return SQLQueryResult{}, err
								}
								for _, candidate := range candidates {
									if err := control.addJoinWork(1); err != nil {
										return SQLQueryResult{}, err
									}
									matched = true
									wrappedCandidate := sqlExecRow{sources: map[string]SQLRow{join.source.alias: candidate}, order: []string{join.source.alias}}
									next = append(next, mergeSQLRows(left, wrappedCandidate))
									if len(next) > maxRows {
										return SQLQueryResult{}, fmt.Errorf("SQL join exceeds the %d row limit; add a more selective WHERE or ON condition", maxRows)
									}
								}
							}
							if join.kind == "LEFT" && !matched {
								empty := sqlExecRow{sources: map[string]SQLRow{join.source.alias: {}}, order: []string{join.source.alias}}
								next = append(next, mergeSQLRows(left, empty))
							}
						}
						detail := join.kind + " JOIN " + sqlExplainSource(join.source) + " ON " + sqlExplainExpression(join.on)
						metrics.record("INDEX JOIN", detail, inputRows, len(next), started)
						rows = next
						leftAliases = append(leftAliases, join.source.alias)
						continue
					}
				}
			}
			// A base-only WHERE is applied before this loop. Re-probing the raw
			// CACHE index would otherwise put filtered left rows back as unmatched
			// right rows, so preserve SQL's filter semantics by using the general
			// join path in that case.
			if rightIndexJoin && !pushedWhere && len(leftAliases) == 1 && q.from.kind == "CACHE" {
				if indexed, ok := resolver.(SQLIndexedSourceResolver); ok {
					_, available, err := indexed.ResolveSQLIndexedSource(q.from.kind, q.from.key, leftField, nil)
					if err != nil {
						return SQLQueryResult{}, err
					}
					if available {
						right, err := resolveSQLSource(join.source, resolver, ctes, metrics, control)
						if err != nil {
							return SQLQueryResult{}, err
						}
						if len(right) > maxRows {
							return SQLQueryResult{}, fmt.Errorf("SQL source %q exceeds the %d row limit", join.source.alias, maxRows)
						}
						inputRows := len(rows) + len(right)
						var next []sqlExecRow
						for _, candidateRight := range wrapSQLSource(join.source, right) {
							if err := control.addJoinWork(1); err != nil {
								return SQLQueryResult{}, err
							}
							value := sqlField(candidateRight, join.source.alias, rightField)
							candidates, _, err := indexed.ResolveSQLIndexedSource(q.from.kind, q.from.key, leftField, value)
							if err != nil {
								return SQLQueryResult{}, err
							}
							if len(candidates) == 0 {
								empty := sqlExecRow{sources: map[string]SQLRow{q.from.alias: {}}, order: []string{q.from.alias}}
								next = append(next, mergeSQLRows(empty, candidateRight))
							}
							for _, candidateLeft := range candidates {
								if err := control.addJoinWork(1); err != nil {
									return SQLQueryResult{}, err
								}
								wrappedLeft := sqlExecRow{sources: map[string]SQLRow{q.from.alias: candidateLeft}, order: []string{q.from.alias}}
								next = append(next, mergeSQLRows(wrappedLeft, candidateRight))
							}
							if len(next) > maxRows {
								return SQLQueryResult{}, fmt.Errorf("SQL join exceeds the %d row limit; add a more selective WHERE or ON condition", maxRows)
							}
						}
						metrics.record("INDEX RIGHT JOIN", "RIGHT JOIN "+sqlExplainSource(join.source)+" ON "+sqlExplainExpression(join.on), inputRows, len(next), started)
						rows = next
						leftAliases = append(leftAliases, join.source.alias)
						continue
					}
				}
			}
			right := pushedRight
			if !rightPushed {
				var err error
				right, err = resolveSQLSource(join.source, resolver, ctes, metrics, control)
				if err != nil {
					return SQLQueryResult{}, err
				}
				if len(right) > maxRows {
					return SQLQueryResult{}, fmt.Errorf("SQL source %q exceeds the %d row limit", join.source.alias, maxRows)
				}
			}
			wrapped := wrapSQLSource(join.source, right)
			inputRows := len(rows) + len(wrapped)
			var next []sqlExecRow
			matchedRight := make([]bool, len(wrapped))
			if hashJoin {
				buckets := make(map[string][]int, len(wrapped))
				for rightIndex, row := range wrapped {
					if err := control.addJoinWork(1); err != nil {
						return SQLQueryResult{}, err
					}
					if key, ok := sqlHashJoinKey(sqlField(row, join.source.alias, rightField)); ok {
						buckets[key] = append(buckets[key], rightIndex)
					}
				}
				for _, left := range rows {
					matched := false
					key, ok := sqlHashJoinKey(sqlField(left, leftQualifier, leftField))
					if ok {
						for _, rightIndex := range buckets[key] {
							if err := control.addJoinWork(1); err != nil {
								return SQLQueryResult{}, err
							}
							matched = true
							next = append(next, mergeSQLRows(left, wrapped[rightIndex]))
							matchedRight[rightIndex] = true
							if len(next) > maxRows {
								return SQLQueryResult{}, fmt.Errorf("SQL join exceeds the %d row limit; add a more selective WHERE or ON condition", maxRows)
							}
						}
					}
					if (join.kind == "LEFT" || join.kind == "FULL") && !matched {
						empty := sqlExecRow{sources: map[string]SQLRow{join.source.alias: {}}, order: []string{join.source.alias}}
						next = append(next, mergeSQLRows(left, empty))
						if len(next) > maxRows {
							return SQLQueryResult{}, fmt.Errorf("SQL join exceeds the %d row limit; add a more selective WHERE or ON condition", maxRows)
						}
					}
				}
			} else {
				for _, left := range rows {
					matched := false
					for rightIndex, r := range wrapped {
						if err := control.addJoinWork(1); err != nil {
							return SQLQueryResult{}, err
						}
						combined := mergeSQLRows(left, r)
						value := evalSQLExpr(join.on, []sqlExecRow{combined}, combined)
						if err := sqlExpressionError(value); err != nil {
							return SQLQueryResult{}, err
						}
						ok := join.kind == "CROSS" || sqlTruthy(value)
						if ok {
							matched = true
							matchedRight[rightIndex] = true
							next = append(next, combined)
							if len(next) > maxRows {
								return SQLQueryResult{}, fmt.Errorf("SQL join exceeds the %d row limit; add a more selective WHERE or ON condition", maxRows)
							}
						}
					}
					if (join.kind == "LEFT" || join.kind == "FULL") && !matched {
						empty := sqlExecRow{sources: map[string]SQLRow{join.source.alias: {}}, order: []string{join.source.alias}}
						next = append(next, mergeSQLRows(left, empty))
						if len(next) > maxRows {
							return SQLQueryResult{}, fmt.Errorf("SQL join exceeds the %d row limit; add a more selective WHERE or ON condition", maxRows)
						}
					}
				}
			}
			if join.kind == "RIGHT" || join.kind == "FULL" {
				for rightIndex, right := range wrapped {
					if matchedRight[rightIndex] {
						continue
					}
					emptySources := make(map[string]SQLRow, len(leftAliases))
					for _, alias := range leftAliases {
						emptySources[alias] = SQLRow{}
					}
					emptyLeft := sqlExecRow{sources: emptySources, order: append([]string{}, leftAliases...)}
					next = append(next, mergeSQLRows(emptyLeft, right))
					if len(next) > maxRows {
						return SQLQueryResult{}, fmt.Errorf("SQL join exceeds the %d row limit; add a more selective WHERE or ON condition", maxRows)
					}
				}
			}
			detail := join.kind + " JOIN " + sqlExplainSource(join.source)
			if join.kind != "CROSS" {
				detail += " ON " + sqlExplainExpression(join.on)
			}
			node := "JOIN"
			if hashJoin {
				node = "HASH JOIN"
			}
			metrics.record(node, detail, inputRows, len(next), started)
			rows = next
			leftAliases = append(leftAliases, join.source.alias)
		}
	}
	if q.where.kind != "" && !pushedWhere {
		started = time.Now()
		inputRows := len(rows)
		inputBytes := sqlExecRowsBytes(rows)
		values, err := evalSQLExprBatch(q.where, rows, functions)
		if err != nil {
			return SQLQueryResult{}, err
		}
		filtered := rows[:0]
		for index, row := range rows {
			if err := sqlExpressionError(values[index]); err != nil {
				return SQLQueryResult{}, err
			}
			if sqlTruthy(values[index]) {
				filtered = append(filtered, row)
			}
		}
		rows = filtered
		metrics.recordBytes("FILTER", sqlExplainExpression(q.where), inputRows, len(rows), inputBytes, sqlExecRowsBytes(rows), started)
	}
	if indexOrdered {
		result, handled, err := executeSQLOrderedGroupAggregate(q, rows, control, metrics)
		if err != nil {
			return SQLQueryResult{}, err
		}
		if handled {
			return result, nil
		}
	}
	if control != nil && control.options.MaxGroupBytes > 0 && control.options.SpillDirectory != "" && control.options.MaxSpillBytes > 0 {
		result, handled, err := executeSQLSpilledGroupAggregate(q, rows, control, metrics)
		if err != nil {
			return SQLQueryResult{}, err
		}
		if handled {
			return result, nil
		}
	}
	started = time.Now()
	inputRows := len(rows)
	var groups [][]sqlExecRow
	if indexOrdered && len(q.groupBy) > 0 {
		groups, err = groupSQLRowsOrdered(rows, q.groupBy, q)
	} else {
		groups, err = groupSQLRows(rows, q.groupBy, q)
	}
	if err != nil {
		return SQLQueryResult{}, err
	}
	if control != nil && control.options.MaxGroupBytes > 0 && sqlGroupedRowsBytes(groups) > control.options.MaxGroupBytes {
		return SQLQueryResult{}, fmt.Errorf("SQL group memory budget exceeded: maximum %d bytes", control.options.MaxGroupBytes)
	}
	if len(q.groupBy) > 0 || sqlQueryHasAggregate(q) {
		node := "AGGREGATE"
		if indexOrdered && len(q.groupBy) > 0 {
			node = "INDEX GROUP AGGREGATE"
		}
		metrics.record(node, sqlExplainExpressions(q.groupBy), inputRows, len(groups), started)
	}
	started = time.Now()
	result := SQLQueryResult{Columns: sqlColumns(q.selects), Rows: make([]SQLRow, 0, len(groups))}
	type output struct {
		row   SQLRow
		group []sqlExecRow
	}
	projectGroup := func(group []sqlExecRow) (output, bool, error) {
		representative := sqlExecRow{}
		if len(group) > 0 {
			representative = group[0]
		}
		if q.having.kind != "" {
			value := evalSQLExpr(q.having, group, representative)
			if err := sqlExpressionError(value); err != nil {
				return output{}, false, err
			}
			if !sqlTruthy(value) {
				return output{}, false, nil
			}
		}
		row := SQLRow{}
		for idx, item := range q.selects {
			if item.expr.kind == "star" {
				for _, source := range representative.order {
					for key, value := range representative.sources[source] {
						row[key] = value
					}
				}
				continue
			}
			value := evalSQLExpr(item.expr, group, representative)
			if err := sqlExpressionError(value); err != nil {
				return output{}, false, err
			}
			row[result.Columns[idx]] = value
		}
		return output{row: row, group: group}, true, nil
	}
	out := make([]output, 0, len(groups))
	parallelGroups := control.options.Workers > 1 && len(groups) > 1 && !sqlExprHasCustomFunction(q.having, functions)
	if parallelGroups {
		for _, item := range q.selects {
			if sqlExprHasCustomFunction(item.expr, functions) {
				parallelGroups = false
				break
			}
		}
	}
	if !parallelGroups {
		for _, group := range groups {
			projected, include, err := projectGroup(group)
			if err != nil {
				return SQLQueryResult{}, err
			}
			if include {
				out = append(out, projected)
			}
		}
	} else {
		projected := make([]output, len(groups))
		included := make([]bool, len(groups))
		errs := make([]error, len(groups))
		workers := control.options.Workers
		if workers > len(groups) {
			workers = len(groups)
		}
		jobs := make(chan int)
		var groupWait sync.WaitGroup
		for range workers {
			groupWait.Add(1)
			go func() {
				defer groupWait.Done()
				for index := range jobs {
					projected[index], included[index], errs[index] = projectGroup(groups[index])
				}
			}()
		}
		for index := range groups {
			jobs <- index
		}
		close(jobs)
		groupWait.Wait()
		for index := range groups {
			if errs[index] != nil {
				return SQLQueryResult{}, errs[index]
			}
			if included[index] {
				out = append(out, projected[index])
			}
		}
	}
	for column, item := range q.selects {
		if item.expr.window == nil {
			continue
		}
		if item.expr.name != "ROW_NUMBER" && item.expr.name != "RANK" && item.expr.name != "DENSE_RANK" && item.expr.name != "SUM" && item.expr.name != "AVG" && item.expr.name != "MIN" && item.expr.name != "MAX" && item.expr.name != "LAG" && item.expr.name != "LEAD" {
			return SQLQueryResult{}, fmt.Errorf("SQL window function %q is not supported", item.expr.name)
		}
		for _, output := range out {
			row := sqlExecRow{}
			if len(output.group) > 0 {
				row = output.group[0]
			}
			for _, expression := range item.expr.window.partition {
				if err := sqlExpressionError(evalSQLExpr(expression, output.group, row)); err != nil {
					return SQLQueryResult{}, err
				}
			}
			for _, order := range item.expr.window.order {
				if err := sqlExpressionError(evalSQLExpr(order.expr, output.group, row)); err != nil {
					return SQLQueryResult{}, err
				}
			}
			for _, argument := range item.expr.args {
				if err := sqlExpressionError(evalSQLExpr(argument, output.group, row)); err != nil {
					return SQLQueryResult{}, err
				}
			}
		}
		partitions := map[string][]int{}
		for index := range out {
			row := sqlExecRow{}
			if len(out[index].group) > 0 {
				row = out[index].group[0]
			}
			parts := make([]string, len(item.expr.window.partition))
			for partIndex, expression := range item.expr.window.partition {
				parts[partIndex] = fmt.Sprintf("%#v", evalSQLExpr(expression, out[index].group, row))
			}
			key := strings.Join(parts, "\x00")
			partitions[key] = append(partitions[key], index)
		}
		for _, indexes := range partitions {
			sort.SliceStable(indexes, func(a, b int) bool {
				left, right := out[indexes[a]], out[indexes[b]]
				leftRow, rightRow := sqlExecRow{}, sqlExecRow{}
				if len(left.group) > 0 {
					leftRow = left.group[0]
				}
				if len(right.group) > 0 {
					rightRow = right.group[0]
				}
				for _, order := range item.expr.window.order {
					if less, decided := sqlOrderLess(order, evalSQLExpr(order.expr, left.group, leftRow), evalSQLExpr(order.expr, right.group, rightRow)); decided {
						return less
					}
				}
				return false
			})
			var rangeValues []interface{}
			if item.expr.window.frame != nil && item.expr.window.frame.kind == "RANGE" {
				if len(item.expr.window.order) != 1 {
					return SQLQueryResult{}, fmt.Errorf("RANGE frame requires exactly one ORDER BY expression")
				}
				rangeValues = make([]interface{}, len(indexes))
				for position, index := range indexes {
					row := sqlExecRow{}
					if len(out[index].group) > 0 {
						row = out[index].group[0]
					}
					value := evalSQLExpr(item.expr.window.order[0].expr, out[index].group, row)
					if err := sqlExpressionError(value); err != nil {
						return SQLQueryResult{}, err
					}
					rangeValues[position] = value
				}
			}
			rank := int64(1)
			denseRank := int64(1)
			for position, index := range indexes {
				row := sqlExecRow{}
				if len(out[index].group) > 0 {
					row = out[index].group[0]
				}
				if position > 0 && len(item.expr.window.order) > 0 {
					previous := indexes[position-1]
					previousRow := sqlExecRow{}
					if len(out[previous].group) > 0 {
						previousRow = out[previous].group[0]
					}
					changed := false
					for _, order := range item.expr.window.order {
						if sqlCompare(evalSQLExpr(order.expr, out[index].group, row), evalSQLExpr(order.expr, out[previous].group, previousRow)) != 0 {
							changed = true
							break
						}
					}
					if changed {
						rank = int64(position + 1)
						denseRank++
					}
				}
				switch item.expr.name {
				case "ROW_NUMBER":
					out[index].row[result.Columns[column]] = int64(position + 1)
				case "RANK":
					out[index].row[result.Columns[column]] = rank
				case "DENSE_RANK":
					out[index].row[result.Columns[column]] = denseRank
				case "SUM", "AVG", "MIN", "MAX":
					if len(item.expr.args) != 1 {
						return SQLQueryResult{}, fmt.Errorf("%s window function expects one argument", item.expr.name)
					}
					start, end := sqlWindowFrameBounds(item.expr.window.frame, position, len(indexes))
					if item.expr.window.frame != nil && item.expr.window.frame.kind == "RANGE" {
						var err error
						start, end, err = sqlRangeWindowFrameBounds(item.expr.window.frame, item.expr.window.order[0], rangeValues, position)
						if err != nil {
							return SQLQueryResult{}, err
						}
					}
					var values []float64
					for framePosition := start; framePosition <= end; framePosition++ {
						frameRow := sqlExecRow{}
						if len(out[indexes[framePosition]].group) > 0 {
							frameRow = out[indexes[framePosition]].group[0]
						}
						value := evalSQLExpr(item.expr.args[0], out[indexes[framePosition]].group, frameRow)
						if err := sqlExpressionError(value); err != nil {
							return SQLQueryResult{}, err
						}
						if number, ok := sqlNumber(value); ok {
							values = append(values, number)
						}
					}
					out[index].row[result.Columns[column]] = sqlWindowAggregate(item.expr.name, values)
				case "LAG", "LEAD":
					if len(item.expr.args) < 1 || len(item.expr.args) > 3 {
						return SQLQueryResult{}, fmt.Errorf("%s window function expects one to three arguments", item.expr.name)
					}
					offset := 1
					if len(item.expr.args) >= 2 {
						value, ok := sqlNumber(evalSQLExpr(item.expr.args[1], out[index].group, row))
						if !ok || value < 0 || value != float64(int(value)) {
							return SQLQueryResult{}, fmt.Errorf("%s window offset must be a non-negative integer", item.expr.name)
						}
						offset = int(value)
					}
					target := position - offset
					if item.expr.name == "LEAD" {
						target = position + offset
					}
					if target >= 0 && target < len(indexes) {
						targetRow := sqlExecRow{}
						if len(out[indexes[target]].group) > 0 {
							targetRow = out[indexes[target]].group[0]
						}
						out[index].row[result.Columns[column]] = evalSQLExpr(item.expr.args[0], out[indexes[target]].group, targetRow)
					} else if len(item.expr.args) == 3 {
						out[index].row[result.Columns[column]] = evalSQLExpr(item.expr.args[2], out[index].group, row)
					} else {
						out[index].row[result.Columns[column]] = nil
					}
				}
			}
		}
	}
	for column, item := range q.selects {
		if item.expr.window != nil {
			continue
		}
		if !sqlExprHasCustomFunction(item.expr, functions) {
			continue
		}
		calls := make([]sqlExecRow, len(out))
		for index := range out {
			if len(out[index].group) != 1 {
				return SQLQueryResult{}, fmt.Errorf("SQL function %q cannot be combined with grouped or aggregate results", item.expr.name)
			}
			calls[index] = out[index].group[0]
		}
		values, err := evalSQLExprBatch(item.expr, calls, functions)
		if err != nil {
			return SQLQueryResult{}, err
		}
		for index := range out {
			out[index].row[result.Columns[column]] = values[index]
		}
	}
	projectedBytes := 0
	for _, output := range out {
		projectedBytes += sqlRowBytes(output.row)
	}
	metrics.recordBytes("PROJECT", sqlExplainSelects(q.selects), len(groups), len(out), sqlGroupedRowsBytes(groups), projectedBytes, started)
	if q.distinct {
		started = time.Now()
		inputRows := len(out)
		seen := make(map[string]struct{}, len(out))
		filtered := out[:0]
		for _, item := range out {
			key := sqlOutputRowKey(item.row)
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			filtered = append(filtered, item)
		}
		out = filtered
		metrics.record("DISTINCT", "deduplicate projected rows", inputRows, len(out), started)
	}
	externallySorted := false
	sortInputRows := 0
	if len(q.orderBy) > 0 && !indexOrdered {
		spillRecords := make([]sqlSpillOutput, 0, len(out))
		for _, output := range out {
			if err := control.check(); err != nil {
				return SQLQueryResult{}, err
			}
			record := sqlSpillOutput{Row: output.row, Keys: make([]interface{}, len(q.orderBy)), Ordinal: len(spillRecords)}
			for index, item := range q.orderBy {
				value := evalOutputOrder(item.expr, output.row, output.group)
				if err := sqlExpressionError(value); err != nil {
					return SQLQueryResult{}, err
				}
				record.Keys[index] = value
			}
			spillRecords = append(spillRecords, record)
		}
		if control != nil && control.options.MaxSortBytes > 0 {
			sortBytes := 0
			for _, item := range out {
				sortBytes += sqlRowBytes(item.row)
			}
			if sortBytes > control.options.MaxSortBytes {
				if control.options.SpillDirectory == "" || control.options.MaxSpillBytes <= 0 {
					return SQLQueryResult{}, fmt.Errorf("SQL sort memory budget exceeded: maximum %d bytes", control.options.MaxSortBytes)
				}
				started = time.Now()
				rows, spillBytes, runs, err := sqlExternalSortRows(spillRecords, q.orderBy, control.options.SpillDirectory, control.options.MaxSortBytes, control.options.MaxSpillBytes, q.offset, q.limit, control)
				if err != nil {
					return SQLQueryResult{}, err
				}
				result.Rows = rows
				groups = nil
				out = nil
				externallySorted = true
				sortInputRows = len(spillRecords)
				metrics.record("EXTERNAL SORT", fmt.Sprintf("%s spill_bytes=%d runs=%d", sqlExplainOrders(q.orderBy), spillBytes, runs), sortInputRows, sortInputRows, started)
			}
		}
		if !externallySorted {
			started = time.Now()
			inputRows := len(out)
			sort.SliceStable(out, func(i, j int) bool {
				for _, item := range q.orderBy {
					a := evalOutputOrder(item.expr, out[i].row, out[i].group)
					b := evalOutputOrder(item.expr, out[j].row, out[j].group)
					if less, decided := sqlOrderLess(item, a, b); decided {
						return less
					}
				}
				return false
			})
			metrics.record("SORT", sqlExplainOrders(q.orderBy), inputRows, len(out), started)
		}
	}
	if !externallySorted {
		started = time.Now()
		inputRows = len(out)
		start := q.offset
		if start > len(out) {
			start = len(out)
		}
		end := len(out)
		if q.limit >= 0 && start+q.limit < end {
			end = start + q.limit
		}
		for _, item := range out[start:end] {
			result.Rows = append(result.Rows, item.row)
		}
		if q.limit >= 0 || q.offset > 0 {
			metrics.record("LIMIT", fmt.Sprintf("limit=%d offset=%d", q.limit, q.offset), inputRows, len(result.Rows), started)
		}
	}
	if externallySorted && (q.limit >= 0 || q.offset > 0) {
		started = time.Now()
		metrics.record("LIMIT", fmt.Sprintf("limit=%d offset=%d", q.limit, q.offset), sortInputRows, len(result.Rows), started)
	}
	if control != nil && control.options.MaxResultBytes > 0 && sqlRowsBytes(result.Rows) > control.options.MaxResultBytes {
		return SQLQueryResult{}, fmt.Errorf("SQL result byte budget exceeded: maximum %d bytes", control.options.MaxResultBytes)
	}
	for _, union := range q.unions {
		right, err := executeSQLQueryWithMetrics(union.query, resolver, ctes, metrics, control)
		if err != nil {
			return SQLQueryResult{}, err
		}
		if !sameSQLColumns(result.Columns, right.Columns) {
			return SQLQueryResult{}, fmt.Errorf("%s queries must project the same column names in the same order", union.kind)
		}
		started = time.Now()
		inputRows = len(result.Rows) + len(right.Rows)
		setBytes := sqlRowsBytes(result.Rows) + sqlRowsBytes(right.Rows)
		if control != nil && !union.all && control.options.MaxSetBytes > 0 && setBytes > control.options.MaxSetBytes {
			if control.options.SpillDirectory != "" && control.options.MaxSpillBytes > 0 {
				rows, spillBytes, runs, err := sqlExternalSetRows(result.Rows, right.Rows, union.kind, control.options.SpillDirectory, control.options.MaxSetBytes, control.options.MaxSpillBytes, control)
				if err != nil {
					return SQLQueryResult{}, err
				}
				result.Rows = rows
				metrics.record("EXTERNAL SET", fmt.Sprintf("%s spill_bytes=%d runs=%d", union.kind, spillBytes, runs), inputRows, len(result.Rows), started)
				continue
			}
			return SQLQueryResult{}, fmt.Errorf("SQL set memory budget exceeded: maximum %d bytes", control.options.MaxSetBytes)
		}
		switch union.kind {
		case "UNION":
			if union.all {
				result.Rows = append(result.Rows, right.Rows...)
				break
			}
			seen := make(map[string]struct{}, len(result.Rows)+len(right.Rows))
			unique := result.Rows[:0]
			for _, row := range result.Rows {
				key := sqlOutputRowKey(row)
				if _, exists := seen[key]; exists {
					continue
				}
				seen[key] = struct{}{}
				unique = append(unique, row)
			}
			for _, row := range right.Rows {
				key := sqlOutputRowKey(row)
				if _, exists := seen[key]; exists {
					continue
				}
				seen[key] = struct{}{}
				unique = append(unique, row)
			}
			result.Rows = unique
		case "INTERSECT":
			available := make(map[string]struct{}, len(right.Rows))
			for _, row := range right.Rows {
				available[sqlOutputRowKey(row)] = struct{}{}
			}
			filtered := result.Rows[:0]
			for _, row := range result.Rows {
				if _, exists := available[sqlOutputRowKey(row)]; exists {
					filtered = append(filtered, row)
				}
			}
			result.Rows = distinctSQLQueryRows(filtered)
		case "EXCEPT":
			excluded := make(map[string]struct{}, len(right.Rows))
			for _, row := range right.Rows {
				excluded[sqlOutputRowKey(row)] = struct{}{}
			}
			filtered := result.Rows[:0]
			for _, row := range result.Rows {
				if _, exists := excluded[sqlOutputRowKey(row)]; !exists {
					filtered = append(filtered, row)
				}
			}
			result.Rows = distinctSQLQueryRows(filtered)
		default:
			return SQLQueryResult{}, fmt.Errorf("unsupported SQL set operation %q", union.kind)
		}
		kind := union.kind
		if union.all {
			kind += " ALL"
		} else if union.kind == "UNION" {
			kind += " early duplicate elimination"
		}
		metrics.record("SET", kind, inputRows, len(result.Rows), started)
	}
	return result, nil
}

// executeSQLRecursiveCTE evaluates the non-recursive seed once and then
// repeatedly evaluates the recursive UNION term against the previous working
// table. This mirrors SQL recursive-CTE semantics and prevents a term from
// observing rows produced earlier in the same iteration.
func executeSQLRecursiveCTE(cte sqlCTE, resolver SQLSourceResolver, ctes map[string][]SQLRow, metrics *sqlExecutionMetrics, control *sqlExecutionControl, maxRows int) ([]SQLRow, error) {
	if cte.query == nil || len(cte.query.unions) != 1 {
		return nil, fmt.Errorf("recursive CTE %q has no recursive UNION term", cte.name)
	}
	started := time.Now()
	seedQuery := *cte.query
	union := seedQuery.unions[0]
	seedQuery.unions = nil
	seed, err := executeSQLQueryWithMetrics(&seedQuery, resolver, ctes, metrics, control)
	if err != nil {
		return nil, err
	}
	seedRows, err := sqlCTEOutputRows(cte, seed)
	if err != nil {
		return nil, err
	}
	if len(seedRows) > maxRows {
		return nil, fmt.Errorf("recursive CTE %q exceeds the %d row limit", cte.name, maxRows)
	}
	total := cloneSQLRows(seedRows)
	frontier := cloneSQLRows(seedRows)
	depth := 0
	seen := map[string]struct{}{}
	detectCycles := control != nil && control.options.DetectRecursiveCycles
	if !union.all || detectCycles {
		for _, row := range total {
			seen[sqlOutputRowKey(row)] = struct{}{}
		}
	}
	cycleSeen := map[string]struct{}{}
	if cte.cycleSet != "" {
		for _, row := range total {
			key, err := sqlRecursiveCycleKey(row, cte.cycleBy)
			if err != nil {
				return nil, fmt.Errorf("recursive CTE %q CYCLE: %w", cte.name, err)
			}
			cycleSeen[key] = struct{}{}
			row[cte.cycleSet] = false
		}
		frontier = cloneSQLRows(total)
	}
	searchOrder := int64(0)
	if cte.searchSet != "" {
		if err := sortSQLRecursiveRows(total, cte.searchBy); err != nil {
			return nil, fmt.Errorf("recursive CTE %q SEARCH: %w", cte.name, err)
		}
		for _, row := range total {
			searchOrder++
			row[cte.searchSet] = searchOrder
		}
		frontier = cloneSQLRows(total)
	}
	for len(frontier) > 0 {
		if err := control.check(); err != nil {
			return nil, err
		}
		if control != nil && control.options.MaxRecursionDepth > 0 && depth >= control.options.MaxRecursionDepth {
			return nil, fmt.Errorf("recursive CTE %q recursion depth %d exceeds maximum %d", cte.name, depth+1, control.options.MaxRecursionDepth)
		}
		depth++
		ctes[cte.name] = cloneSQLRows(frontier)
		nextResult, err := executeSQLQueryWithMetrics(union.query, resolver, ctes, metrics, control)
		if err != nil {
			return nil, err
		}
		next, err := sqlCTEOutputRows(cte, nextResult)
		if err != nil {
			return nil, err
		}
		if len(cte.columns) == 0 && !sameSQLColumns(seed.Columns, nextResult.Columns) {
			return nil, fmt.Errorf("recursive CTE %q seed and recursive terms must project the same column names in the same order", cte.name)
		}
		if !union.all {
			filtered := next[:0]
			for _, row := range next {
				key := sqlOutputRowKey(row)
				if _, exists := seen[key]; exists {
					continue
				}
				seen[key] = struct{}{}
				filtered = append(filtered, row)
			}
			next = filtered
		} else if detectCycles {
			for _, row := range next {
				key := sqlOutputRowKey(row)
				if _, exists := seen[key]; exists {
					return nil, fmt.Errorf("recursive CTE %q detected a cycle at depth %d; use UNION for deduplication or disable cycle detection", cte.name, depth)
				}
				seen[key] = struct{}{}
			}
		}
		frontierNext := next
		if cte.cycleSet != "" {
			frontierNext = make([]SQLRow, 0, len(next))
			for _, row := range next {
				key, err := sqlRecursiveCycleKey(row, cte.cycleBy)
				if err != nil {
					return nil, fmt.Errorf("recursive CTE %q CYCLE: %w", cte.name, err)
				}
				_, cycle := cycleSeen[key]
				row[cte.cycleSet] = cycle
				if !cycle {
					cycleSeen[key] = struct{}{}
					frontierNext = append(frontierNext, row)
				}
			}
		}
		if cte.searchSet != "" {
			if err := sortSQLRecursiveRows(next, cte.searchBy); err != nil {
				return nil, fmt.Errorf("recursive CTE %q SEARCH: %w", cte.name, err)
			}
			for _, row := range next {
				searchOrder++
				row[cte.searchSet] = searchOrder
			}
		}
		if len(total)+len(next) > maxRows {
			return nil, fmt.Errorf("recursive CTE %q exceeds the %d row limit; add a terminating condition", cte.name, maxRows)
		}
		total = append(total, next...)
		frontier = cloneSQLRows(frontierNext)
	}
	metrics.record("RECURSIVE CTE", cte.name, len(seedRows), len(total), started)
	return total, nil
}

func sqlRecursiveCycleKey(row SQLRow, columns []string) (string, error) {
	key := make(SQLRow, len(columns))
	for _, column := range columns {
		value, exists := row[column]
		if !exists {
			return "", fmt.Errorf("column %q is not projected by the recursive term", column)
		}
		key[column] = value
	}
	return sqlOutputRowKey(key), nil
}

func sortSQLRecursiveRows(rows []SQLRow, columns []string) error {
	for _, row := range rows {
		for _, column := range columns {
			if _, exists := row[column]; !exists {
				return fmt.Errorf("column %q is not projected by the recursive term", column)
			}
		}
	}
	sort.SliceStable(rows, func(left, right int) bool {
		for _, column := range columns {
			if comparison := sqlCompare(rows[left][column], rows[right][column]); comparison != 0 {
				return comparison < 0
			}
		}
		return false
	})
	return nil
}

// sqlCTEOutputRows applies the optional CTE column list to a query result.
// The declared names are the only names visible to later CTE terms and to the
// outer query, matching regular SQL CTE scoping.
func sqlCTEOutputRows(cte sqlCTE, result SQLQueryResult) ([]SQLRow, error) {
	if len(cte.columns) == 0 {
		return result.Rows, nil
	}
	if len(cte.columns) != len(result.Columns) {
		return nil, fmt.Errorf("CTE %q declares %d columns but its query returns %d", cte.name, len(cte.columns), len(result.Columns))
	}
	rows := make([]SQLRow, len(result.Rows))
	for index, row := range result.Rows {
		mapped := make(SQLRow, len(cte.columns))
		for column, name := range cte.columns {
			mapped[name] = row[result.Columns[column]]
		}
		rows[index] = mapped
	}
	return rows, nil
}

func explainSQLQuery(query *sqlQuery, resolver SQLSourceResolver, control *sqlExecutionControl) (SQLQueryResult, error) {
	steps := sqlExplainSteps(query)
	result := SQLQueryResult{
		Columns: []string{"node", "detail", "estimated_rows"},
		Rows:    make([]SQLRow, 0, len(steps)+1),
		Plan:    steps,
	}
	for _, step := range steps {
		row := SQLRow{"node": step.Node, "detail": step.Detail}
		if step.EstimatedRows != nil {
			row["estimated_rows"] = *step.EstimatedRows
		}
		result.Rows = append(result.Rows, row)
	}
	if !query.analyze {
		return result, nil
	}
	started := time.Now()
	metrics := &sqlExecutionMetrics{}
	executed, err := executeSQLQueryWithMetrics(query, resolver, nil, metrics, control)
	if err != nil {
		return SQLQueryResult{}, err
	}
	result.Stats = &SQLQueryStats{
		ElapsedNanos:  time.Since(started).Nanoseconds(),
		OutputRows:    len(executed.Rows),
		OutputColumns: len(executed.Columns),
		ResultBytes:   sqlRowsBytes(executed.Rows),
		PlanSteps:     len(metrics.steps),
	}
	result.Plan = metrics.steps
	result.Rows = result.Rows[:0]
	for _, step := range result.Plan {
		row := SQLRow{"node": step.Node, "detail": step.Detail, "actual_input_rows": *step.ActualInputRows, "actual_output_rows": *step.ActualOutputRows, "elapsed_ns": *step.ElapsedNanos}
		if step.ActualInputBytes != nil {
			row["actual_input_bytes"] = *step.ActualInputBytes
		}
		if step.ActualOutputBytes != nil {
			row["actual_output_bytes"] = *step.ActualOutputBytes
		}
		if step.EstimatedRows != nil {
			row["estimated_rows"] = *step.EstimatedRows
		}
		if step.EstimateErrorRows != nil {
			row["estimate_error_rows"] = *step.EstimateErrorRows
		}
		if step.EstimateErrorPercent != nil {
			row["estimate_error_percent"] = *step.EstimateErrorPercent
		}
		result.Rows = append(result.Rows, row)
	}
	result.Columns = append(result.Columns, "actual_rows", "estimate_error_rows", "estimate_error_percent", "actual_input_bytes", "actual_output_bytes", "result_bytes", "elapsed_ns")
	result.Rows = append(result.Rows, SQLRow{
		"node":         "ANALYZE",
		"detail":       "execution summary",
		"actual_rows":  result.Stats.OutputRows,
		"result_bytes": result.Stats.ResultBytes,
		"elapsed_ns":   result.Stats.ElapsedNanos,
	})
	return result, nil
}

func sqlExplainSteps(query *sqlQuery) []SQLExplainStep {
	steps := make([]SQLExplainStep, 0, 8+len(query.ctes)+len(query.joins)+len(query.unions))
	sqlAppendExplainSteps(&steps, query, "")
	return steps
}

func sqlAppendExplainSteps(steps *[]SQLExplainStep, query *sqlQuery, prefix string) {
	for _, cte := range query.ctes {
		*steps = append(*steps, SQLExplainStep{Node: prefix + "CTE", Detail: cte.name})
		if cte.query != nil {
			sqlAppendExplainSteps(steps, cte.query, prefix+"  ")
		} else {
			estimate := len(cte.values)
			*steps = append(*steps, SQLExplainStep{Node: prefix + "  VALUES", Detail: "CTE " + cte.name, EstimatedRows: &estimate})
		}
	}
	*steps = append(*steps, sqlExplainSourceStep(prefix+"SCAN", *query.from))
	if query.from.kind == "SUBQUERY" && query.from.query != nil {
		sqlAppendExplainSteps(steps, query.from.query, prefix+"  ")
	}
	leftAliases := []string{}
	if query.from != nil && query.from.alias != "" {
		leftAliases = append(leftAliases, query.from.alias)
	}
	for _, join := range query.joins {
		detail := join.kind + " JOIN " + sqlExplainSource(join.source)
		if join.kind != "CROSS" {
			detail += " ON " + sqlExplainExpression(join.on)
		}
		node := "JOIN"
		if join.kind == "CROSS" {
			node = "CROSS JOIN"
		} else if _, _, _, ok := sqlHashJoinFields(join.on, leftAliases, join.source.alias); ok {
			node = "EQUALITY JOIN"
			detail += "; eligible for HASH JOIN"
			if _, _, _, ok := sqlCompositeJoinFields(join.on, leftAliases, join.source.alias); ok {
				detail += " or COMPOSITE INDEX JOIN"
			}
		}
		*steps = append(*steps, SQLExplainStep{Node: prefix + node, Detail: detail})
		if join.source.kind == "SUBQUERY" && join.source.query != nil {
			sqlAppendExplainSteps(steps, join.source.query, prefix+"  ")
		}
		if join.source.alias != "" {
			leftAliases = append(leftAliases, join.source.alias)
		}
	}
	if query.where.kind != "" {
		*steps = append(*steps, SQLExplainStep{Node: prefix + "FILTER", Detail: sqlExplainExpression(query.where)})
	}
	if len(query.groupBy) > 0 || sqlQueryHasAggregate(query) {
		*steps = append(*steps, SQLExplainStep{Node: prefix + "AGGREGATE", Detail: sqlExplainExpressions(query.groupBy)})
	}
	if query.having.kind != "" {
		*steps = append(*steps, SQLExplainStep{Node: prefix + "HAVING", Detail: sqlExplainExpression(query.having)})
	}
	*steps = append(*steps, SQLExplainStep{Node: prefix + "PROJECT", Detail: sqlExplainSelects(query.selects)})
	if query.distinct {
		*steps = append(*steps, SQLExplainStep{Node: prefix + "DISTINCT", Detail: "deduplicate projected rows"})
	}
	if len(query.orderBy) > 0 {
		*steps = append(*steps, SQLExplainStep{Node: prefix + "SORT", Detail: sqlExplainOrders(query.orderBy)})
	}
	if query.limit >= 0 || query.offset > 0 {
		*steps = append(*steps, SQLExplainStep{Node: prefix + "LIMIT", Detail: fmt.Sprintf("limit=%d offset=%d", query.limit, query.offset)})
	}
	for _, union := range query.unions {
		kind := union.kind
		if union.all {
			kind += " ALL"
		}
		*steps = append(*steps, SQLExplainStep{Node: prefix + "SET", Detail: kind})
		sqlAppendExplainSteps(steps, union.query, prefix+"  ")
	}
}

func sqlExplainSourceStep(node string, source sqlSource) SQLExplainStep {
	step := SQLExplainStep{Node: node, Detail: sqlExplainSource(source)}
	if source.kind == "VALUES" {
		estimate := len(source.values)
		step.EstimatedRows = &estimate
	}
	return step
}

func sqlExplainSource(source sqlSource) string {
	var detail string
	switch source.kind {
	case "CACHE":
		detail = "CACHE(" + strconv.Quote(source.key) + ")"
	case "EXTERNAL":
		detail = "EXTERNAL(" + strconv.Quote(source.key) + ")"
	case "VALUES":
		detail = "VALUES"
	case "CTE":
		detail = "CTE " + source.key
	case "KEYS":
		detail = "KEYS"
	case "SUBQUERY":
		detail = "derived query"
	default:
		detail = source.kind
	}
	if source.alias != "" {
		detail += " AS " + source.alias
	}
	return detail
}

func sqlOrderLess(order sqlOrder, left, right interface{}) (bool, bool) {
	if (left == nil || right == nil) && left != right && (order.nullsFirst || order.nullsLast) {
		return (left == nil) == order.nullsFirst, true
	}
	cmp := sqlCompare(left, right)
	if cmp == 0 {
		return false, false
	}
	if order.desc {
		return cmp > 0, true
	}
	return cmp < 0, true
}

func sqlExplainExpression(expression sqlExpr) string {
	switch expression.kind {
	case "field":
		if expression.qualifier != "" {
			return expression.qualifier + "." + expression.name
		}
		return expression.name
	case "literal":
		return fmt.Sprintf("%#v", expression.value)
	case "star":
		return "*"
	case "cast":
		if len(expression.args) == 1 {
			return "CAST(" + sqlExplainExpression(expression.args[0]) + " AS " + expression.name + ")"
		}
		return "CAST(<invalid>)"
	case "func":
		return expression.name + "(" + sqlExplainExpressions(expression.args) + ")"
	case "case":
		parts := make([]string, 0, len(expression.cases)*2+2)
		parts = append(parts, "CASE")
		if expression.left != nil {
			parts = append(parts, sqlExplainExpression(*expression.left))
		}
		for _, branch := range expression.cases {
			parts = append(parts, "WHEN", sqlExplainExpression(branch.when), "THEN", sqlExplainExpression(branch.then))
		}
		if expression.right != nil {
			parts = append(parts, "ELSE", sqlExplainExpression(*expression.right))
		}
		parts = append(parts, "END")
		return strings.Join(parts, " ")
	case "unary":
		return expression.op + " " + sqlExplainExpression(*expression.left)
	case "in":
		return sqlExplainExpression(*expression.left) + " " + expression.op + " (" + sqlExplainExpressions(expression.args) + ")"
	case "between":
		if len(expression.args) == 2 {
			return sqlExplainExpression(*expression.left) + " " + expression.op + " " + sqlExplainExpression(expression.args[0]) + " AND " + sqlExplainExpression(expression.args[1])
		}
		return "<invalid BETWEEN>"
	case "binary":
		if expression.op == "IS NULL" || expression.op == "IS NOT NULL" {
			return sqlExplainExpression(*expression.left) + " " + expression.op
		}
		return sqlExplainExpression(*expression.left) + " " + expression.op + " " + sqlExplainExpression(*expression.right)
	}
	return "<unknown expression>"
}

func sqlExplainExpressions(expressions []sqlExpr) string {
	values := make([]string, len(expressions))
	for index, expression := range expressions {
		values[index] = sqlExplainExpression(expression)
	}
	return strings.Join(values, ", ")
}

func sqlExplainSelects(items []sqlSelectItem) string {
	values := make([]string, len(items))
	for index, item := range items {
		values[index] = sqlExplainExpression(item.expr)
		if item.alias != "" {
			values[index] += " AS " + item.alias
		}
	}
	return strings.Join(values, ", ")
}

func sqlExplainOrders(orders []sqlOrder) string {
	values := make([]string, len(orders))
	for index, order := range orders {
		values[index] = sqlExplainExpression(order.expr)
		if order.desc {
			values[index] += " DESC"
		} else {
			values[index] += " ASC"
		}
		if order.nullsFirst {
			values[index] += " NULLS FIRST"
		} else if order.nullsLast {
			values[index] += " NULLS LAST"
		}
	}
	return strings.Join(values, ", ")
}

func sameSQLColumns(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if !strings.EqualFold(left[index], right[index]) {
			return false
		}
	}
	return true
}

func distinctSQLQueryRows(rows []SQLRow) []SQLRow {
	seen := make(map[string]struct{}, len(rows))
	out := rows[:0]
	for _, row := range rows {
		key := sqlOutputRowKey(row)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, row)
	}
	return out
}

func sqlOutputRowKey(row SQLRow) string {
	if encoded, err := json.Marshal(row); err == nil {
		return string(encoded)
	}
	keys := make([]string, 0, len(row))
	for key := range row {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var builder strings.Builder
	for _, key := range keys {
		fmt.Fprintf(&builder, "%q=%#v;", key, row[key])
	}
	return builder.String()
}
func resolveSQLSource(source sqlSource, resolver SQLSourceResolver, ctes map[string][]SQLRow, metrics *sqlExecutionMetrics, control *sqlExecutionControl) ([]SQLRow, error) {
	switch source.kind {
	case "VALUES":
		return valuesSQLRows(source.values, source.columns), nil
	case "CTE":
		return ctes[source.key], nil
	case "SUBQUERY":
		result, err := executeSQLQueryWithMetrics(source.query, resolver, ctes, metrics, control)
		if err != nil {
			return nil, err
		}
		return result.Rows, nil
	case "CACHE", "KEYS":
		if resolver == nil {
			return nil, nil
		}
		cacheKey := source.kind + "\x00" + source.key
		if control != nil {
			if rows, ok := control.sources[cacheKey]; ok {
				return validateSQLSourceFieldTypes(source, cloneSQLRows(rows))
			}
		}
		rows, err := resolver.ResolveSQLSource(source.kind, source.key)
		if err != nil {
			return nil, err
		}
		if control != nil {
			control.sources[cacheKey] = cloneSQLRows(rows)
		}
		return validateSQLSourceFieldTypes(source, rows)
	case "EXTERNAL":
		if resolver == nil {
			return nil, fmt.Errorf("EXTERNAL(%q) requires an external source resolver", source.key)
		}
		cacheKey := source.kind + "\x00" + source.key
		if control != nil {
			if rows, ok := control.sources[cacheKey]; ok {
				return cloneSQLRows(rows), nil
			}
		}
		external, ok := resolver.(ExternalSourceResolver)
		if !ok {
			return nil, fmt.Errorf("EXTERNAL(%q) requires an external source resolver", source.key)
		}
		rows, err := external.ResolveSQLExternalSource(source.key)
		if err != nil {
			return nil, err
		}
		if control != nil {
			control.sources[cacheKey] = cloneSQLRows(rows)
		}
		return rows, nil
	}
	return nil, nil
}

func validateSQLSourceFieldTypes(source sqlSource, rows []SQLRow) ([]SQLRow, error) {
	if len(source.fieldTypes) == 0 {
		return rows, nil
	}
	validated := make([]SQLRow, len(rows))
	for rowIndex, row := range rows {
		converted, err := validateSQLSourceFieldTypeRow(source, row, rowIndex+1)
		if err != nil {
			return nil, err
		}
		validated[rowIndex] = converted
	}
	return validated, nil
}

// validateSQLSourceFieldTypeRow is the row-at-a-time form used by both
// materialized and streamed CACHE sources. Keeping the conversion and the
// source-spanned error in one place prevents the two executors from drifting.
func validateSQLSourceFieldTypeRow(source sqlSource, row SQLRow, rowIndex int) (SQLRow, error) {
	validated := cloneSQLRows([]SQLRow{row})[0]
	for field, fieldType := range source.fieldTypes {
		value, exists := validated[field]
		if !exists || value == nil {
			continue
		}
		converted, ok := sqlTypedJSONFieldValue(value, fieldType.name)
		if !ok {
			return nil, sqlEvalError{err: fmt.Errorf("CACHE(%q) row %d field %q expects %s, got %s", source.key, rowIndex, field, fieldType.name, sqlLiteralTypeName(value)), token: fieldType.token}
		}
		validated[field] = converted
	}
	return validated, nil
}

func sqlTypedJSONFieldValue(value interface{}, typeName string) (interface{}, bool) {
	switch typeName {
	case "TEXT":
		text, ok := value.(string)
		return text, ok
	case "BOOLEAN":
		boolean, ok := value.(bool)
		return boolean, ok
	case "NUMBER":
		_, ok := sqlNumber(value)
		return value, ok
	case "INTEGER":
		number, ok := sqlNumber(value)
		if !ok || math.Trunc(number) != number || number < float64(-1<<63) || number >= float64(1<<63) {
			return nil, false
		}
		return int64(number), true
	case "DECIMAL":
		if decimal, ok := value.(sqlDecimal); ok {
			return decimal, true
		}
		if text, ok := value.(string); ok {
			decimal, ok := parseSQLDecimal(text)
			return decimal, ok
		}
		if number, ok := sqlNumber(value); ok {
			decimal, ok := parseSQLDecimal(strconv.FormatFloat(number, 'f', -1, 64))
			return decimal, ok
		}
		return nil, false
	case "DATE":
		text, ok := value.(string)
		if !ok {
			return nil, false
		}
		parsed, err := time.Parse("2006-01-02", text)
		if err != nil || parsed.Format("2006-01-02") != text {
			return nil, false
		}
		return sqlDate(text), true
	case "TIMESTAMP":
		text, ok := value.(string)
		if !ok {
			return nil, false
		}
		parsed, err := time.Parse(time.RFC3339Nano, text)
		if err != nil {
			return nil, false
		}
		return parsed, true
	case "JSON":
		switch value.(type) {
		case map[string]interface{}, []interface{}:
			return value, true
		}
	}
	return nil, false
}

func resolveSQLIndexedSource(source sqlSource, condition sqlExpr, resolver SQLSourceResolver, metrics *sqlExecutionMetrics) ([]SQLRow, bool, error) {
	if source.kind != "CACHE" || len(source.fieldTypes) != 0 {
		return nil, false, nil
	}
	if rows, indexed, err := resolveSQLTextIndexedSource(source, condition, resolver); indexed || err != nil {
		return rows, indexed, err
	}
	if condition.kind != "binary" || condition.left == nil || condition.right == nil {
		return nil, false, nil
	}
	if indexed, ok := resolver.(SQLCompositeIndexedSourceResolver); ok {
		fields, values := sqlCompositeIndexedEqualities(source, condition)
		if len(fields) >= 2 {
			rows, available, err := indexed.ResolveSQLCompositeIndexedSource(source.kind, source.key, fields, values)
			if available || err != nil {
				return rows, available, err
			}
		}
	}
	if condition.op == "AND" {
		if rows, indexed, err := resolveSQLMostSelectiveIndexedConjunct(source, condition, resolver, metrics); indexed || err != nil {
			return rows, indexed, err
		}
		if rows, indexed, err := resolveSQLIndexedSource(source, *condition.left, resolver, metrics); indexed || err != nil {
			return rows, indexed, err
		}
		return resolveSQLIndexedSource(source, *condition.right, resolver, metrics)
	}
	left, right := *condition.left, *condition.right
	var adaptiveKey string
	var adaptiveEstimate *int
	if metrics != nil && metrics.adaptive != nil {
		adaptiveKey = sqlAdaptiveIndexKey(source, condition)
		var estimateErr error
		adaptiveEstimate, estimateErr = sqlIndexedEqualityEstimate(source, condition, resolver)
		if estimateErr != nil {
			return nil, false, estimateErr
		}
		if adaptiveEstimate != nil && !metrics.adaptive.ShouldUseIndex(adaptiveKey) {
			return nil, false, nil
		}
	}
	var rows []SQLRow
	var indexed bool
	var err error
	if left.kind == "field" && left.qualifier == source.alias && right.kind == "literal" {
		rows, indexed, err = resolveSQLIndexedComparison(source, left.name, condition.op, right.value, resolver)
	} else if right.kind == "field" && right.qualifier == source.alias && left.kind == "literal" {
		rows, indexed, err = resolveSQLIndexedComparison(source, right.name, sqlReverseComparison(condition.op), left.value, resolver)
	} else {
		return nil, false, nil
	}
	if err == nil && indexed && adaptiveEstimate != nil && metrics != nil && metrics.adaptive != nil {
		metrics.adaptive.ObserveIndex(adaptiveKey, *adaptiveEstimate, len(rows))
	}
	return rows, indexed, err
}

func resolveSQLTextIndexedSource(source sqlSource, condition sqlExpr, resolver SQLSourceResolver) ([]SQLRow, bool, error) {
	if condition.kind != "func" || !strings.EqualFold(condition.name, "CONTAINS") || len(condition.args) != 2 {
		return nil, false, nil
	}
	field, query := condition.args[0], condition.args[1]
	if field.kind != "field" || field.qualifier != source.alias || query.kind != "literal" {
		return nil, false, nil
	}
	text, ok := query.value.(string)
	if !ok {
		return nil, false, nil
	}
	indexed, ok := resolver.(TextIndexedSourceResolver)
	if !ok {
		return nil, false, nil
	}
	return indexed.ResolveSQLTextSource(source.kind, source.key, field.name, text)
}

// resolveSQLJoinPushedSource resolves and filters an INNER JOIN input before
// hash construction. The original WHERE remains in the plan and is evaluated
// again after the join, making this a work-only optimization rather than a
// semantic shortcut.
func resolveSQLJoinPushedSource(source sqlSource, condition sqlExpr, resolver SQLSourceResolver, ctes map[string][]SQLRow, metrics *sqlExecutionMetrics, control *sqlExecutionControl, maxRows int) ([]SQLRow, error) {
	started := time.Now()
	rows, indexed, err := resolveSQLIndexedSource(source, condition, resolver, metrics)
	if err != nil {
		return nil, err
	}
	if !indexed {
		rows, err = resolveSQLSource(source, resolver, ctes, metrics, control)
		if err != nil {
			return nil, err
		}
	}
	if len(rows) > maxRows {
		return nil, fmt.Errorf("SQL source %q exceeds the %d row limit", source.alias, maxRows)
	}
	if indexed {
		metrics.record("JOIN INDEX SCAN", sqlExplainSource(source), 0, len(rows), started)
	} else {
		metrics.recordScanRows(source, rows, started)
	}

	filterStarted := time.Now()
	filtered := make([]SQLRow, 0, len(rows))
	for _, row := range rows {
		if err := control.check(); err != nil {
			return nil, err
		}
		execRow := sqlExecRow{sources: map[string]SQLRow{source.alias: row}, order: []string{source.alias}}
		value := evalSQLExpr(condition, []sqlExecRow{execRow}, execRow)
		if err := sqlExpressionError(value); err != nil {
			return nil, err
		}
		if sqlTruthy(value) {
			filtered = append(filtered, row)
		}
	}
	metrics.record("JOIN FILTER PUSH DOWN", "right predicate pushed: "+sqlExplainExpression(condition), len(rows), len(filtered), filterStarted)
	return filtered, nil
}

// resolveSQLOrderedSource chooses the narrow order-preserving scan that can
// replace both a source scan and the final SORT. It intentionally excludes
// filters, joins, unions, typed sources, aliases, and composite order keys:
// those forms need the established executor until their ordering proof is as
// direct as this one-field case.
func resolveSQLOrderedSource(q *sqlQuery, resolver SQLSourceResolver) ([]SQLRow, bool, error) {
	if q == nil || q.from == nil || q.from.kind != "CACHE" || len(q.from.fieldTypes) != 0 || q.where.kind != "" || q.distinct || sqlQueryHasWindow(q) || len(q.joins) != 0 || len(q.unions) != 0 || len(q.orderBy) != 1 {
		return nil, false, nil
	}
	order := q.orderBy[0]
	if order.expr.kind != "field" || order.expr.qualifier != q.from.alias || order.expr.name == "" {
		return nil, false, nil
	}
	if len(q.groupBy) > 0 {
		if len(q.groupBy) != 1 || q.groupBy[0].kind != "field" || q.groupBy[0].qualifier != order.expr.qualifier || q.groupBy[0].name != order.expr.name {
			return nil, false, nil
		}
	} else if sqlQueryHasAggregate(q) {
		return nil, false, nil
	}
	indexed, ok := resolver.(SQLOrderedSourceResolver)
	if !ok {
		return nil, false, nil
	}
	return indexed.ResolveSQLOrderedSource(q.from.kind, q.from.key, order.expr.name, order.desc, order.nullsFirst, order.nullsLast)
}

// resolveSQLMostSelectiveIndexedConjunct uses available equality-index
// cardinality estimates to choose an AND term before the historical
// left-to-right fallback. The complete predicate is still evaluated after the
// probe, so estimates affect work only, never correctness.
func resolveSQLMostSelectiveIndexedConjunct(source sqlSource, condition sqlExpr, resolver SQLSourceResolver, metrics *sqlExecutionMetrics) ([]SQLRow, bool, error) {
	type candidate struct {
		condition sqlExpr
		estimate  int
		cost      int
	}
	conjuncts := []sqlExpr{}
	var collect func(sqlExpr)
	collect = func(expression sqlExpr) {
		if expression.kind == "binary" && expression.op == "AND" && expression.left != nil && expression.right != nil {
			collect(*expression.left)
			collect(*expression.right)
			return
		}
		conjuncts = append(conjuncts, expression)
	}
	collect(condition)
	candidates := make([]candidate, 0, len(conjuncts))
	for _, conjunct := range conjuncts {
		estimate, err := sqlIndexedEqualityEstimate(source, conjunct, resolver)
		if err != nil {
			return nil, false, err
		}
		if estimate != nil {
			candidates = append(candidates, candidate{condition: conjunct, estimate: *estimate, cost: sqlIndexedEqualityProbeCost(*estimate)})
		}
	}
	sort.SliceStable(candidates, func(left, right int) bool { return candidates[left].estimate < candidates[right].estimate })
	started := time.Now()
	details := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		adaptiveKey := sqlAdaptiveIndexKey(source, candidate.condition)
		if metrics != nil && metrics.adaptive != nil && !metrics.adaptive.ShouldUseIndex(adaptiveKey) {
			details = append(details, fmt.Sprintf("%s estimated_rows=%d adaptive: scan selected after prior underestimate", sqlExplainExpression(candidate.condition), candidate.estimate))
			continue
		}
		rows, indexed, err := resolveSQLIndexedSource(source, candidate.condition, resolver, metrics)
		if indexed || err != nil {
			if err == nil {
				details = append(details, fmt.Sprintf("%s estimated_rows=%d estimated_cost=%d selected", sqlExplainExpression(candidate.condition), candidate.estimate, candidate.cost))
				metrics.record("INDEX CANDIDATES", strings.Join(details, "; "), len(candidates), 1, started)
			}
			return rows, indexed, err
		}
		details = append(details, fmt.Sprintf("%s estimated_rows=%d estimated_cost=%d rejected: index unavailable", sqlExplainExpression(candidate.condition), candidate.estimate, candidate.cost))
	}
	if len(candidates) > 0 {
		metrics.record("INDEX CANDIDATES", strings.Join(details, "; "), len(candidates), 0, started)
	}
	return nil, false, nil
}

func sqlAdaptiveIndexKey(source sqlSource, condition sqlExpr) string {
	return source.kind + "\x00" + source.key + "\x00" + sqlExplainExpression(condition)
}

// sqlIndexedEqualityProbeCost models one indexed lookup plus the estimated
// posting-list rows that must be checked by the complete predicate. It is used
// only to explain a candidate decision; rows remain the stable sort key, so an
// estimate can never change query results or the written-order tie break.
func sqlIndexedEqualityProbeCost(estimatedRows int) int {
	if estimatedRows < 0 {
		return 0
	}
	if estimatedRows == int(^uint(0)>>1) {
		return estimatedRows
	}
	return estimatedRows + 1
}

// sqlIndexedEqualityEstimate reports the average posting-list cardinality for
// a simple indexed equality predicate. It is deliberately an estimate rather
// than the exact lookup length so EXPLAIN ANALYZE can surface skew by comparing
// this number with ActualOutputRows. Composite and range predicates keep a nil
// estimate until their individual distributions are available.
func sqlIndexedEqualityEstimate(source sqlSource, condition sqlExpr, resolver SQLSourceResolver) (*int, error) {
	if source.kind != "CACHE" || condition.kind != "binary" || condition.op != "=" || condition.left == nil || condition.right == nil {
		return nil, nil
	}
	left, right := *condition.left, *condition.right
	field := ""
	var value interface{}
	if left.kind == "field" && left.qualifier == source.alias && right.kind == "literal" {
		field = left.name
		value = right.value
	}
	if right.kind == "field" && right.qualifier == source.alias && left.kind == "literal" {
		field = right.name
		value = left.value
	}
	if field == "" {
		return nil, nil
	}
	if valueResolver, ok := resolver.(IndexValueEstimator); ok {
		rows, exact, available, err := valueResolver.SQLJSONIndexValueEstimate(source.key, field, value)
		if err != nil {
			return nil, err
		}
		if available && exact {
			return &rows, nil
		}
	}
	statsResolver, ok := resolver.(JSONIndexStatsResolver)
	if !ok {
		return nil, nil
	}
	stats, available, err := statsResolver.SQLJSONIndexStats(source.key, field)
	if err != nil || !available || stats.DistinctKeys == 0 {
		return nil, err
	}
	estimate := (stats.Rows + stats.DistinctKeys - 1) / stats.DistinctKeys
	return &estimate, nil
}

func sqlCompositeIndexedEqualities(source sqlSource, condition sqlExpr) ([]string, []interface{}) {
	values := map[string]interface{}{}
	var collect func(sqlExpr)
	collect = func(expression sqlExpr) {
		if expression.kind != "binary" {
			return
		}
		if expression.op == "AND" && expression.left != nil && expression.right != nil {
			collect(*expression.left)
			collect(*expression.right)
			return
		}
		if expression.op != "=" || expression.left == nil || expression.right == nil {
			return
		}
		left, right := *expression.left, *expression.right
		if left.kind == "field" && left.qualifier == source.alias && right.kind == "literal" {
			values[left.name] = right.value
		}
		if right.kind == "field" && right.qualifier == source.alias && left.kind == "literal" {
			values[right.name] = left.value
		}
	}
	collect(condition)
	fields := make([]string, 0, len(values))
	for field := range values {
		fields = append(fields, field)
	}
	sort.Strings(fields)
	orderedValues := make([]interface{}, len(fields))
	for index, field := range fields {
		orderedValues[index] = values[field]
	}
	return fields, orderedValues
}

func resolveSQLIndexedComparison(source sqlSource, field, operator string, value interface{}, resolver SQLSourceResolver) ([]SQLRow, bool, error) {
	if operator == "=" {
		indexed, ok := resolver.(SQLIndexedSourceResolver)
		if !ok {
			return nil, false, nil
		}
		return indexed.ResolveSQLIndexedSource(source.kind, source.key, field, value)
	}
	indexed, ok := resolver.(SQLRangeIndexedSourceResolver)
	if !ok {
		return nil, false, nil
	}
	return indexed.ResolveSQLIndexedRangeSource(source.kind, source.key, field, operator, value)
}

func sqlReverseComparison(operator string) string {
	switch operator {
	case "<":
		return ">"
	case "<=":
		return ">="
	case ">":
		return "<"
	case ">=":
		return "<="
	}
	return operator
}

func cloneSQLRows(rows []SQLRow) []SQLRow {
	if rows == nil {
		return nil
	}
	cloned := make([]SQLRow, len(rows))
	for index, row := range rows {
		cloned[index] = make(SQLRow, len(row))
		for key, value := range row {
			cloned[index][key] = value
		}
	}
	return cloned
}
func valuesSQLRows(values [][]interface{}, columns []string) []SQLRow {
	if len(columns) == 0 && len(values) > 0 {
		columns = make([]string, len(values[0]))
		for i := range columns {
			columns[i] = "column" + strconv.Itoa(i+1)
		}
	}
	out := make([]SQLRow, 0, len(values))
	for _, source := range values {
		row := SQLRow{}
		for i, value := range source {
			if i < len(columns) {
				row[columns[i]] = value
			}
		}
		out = append(out, row)
	}
	return out
}
func wrapSQLSource(source sqlSource, rows []SQLRow) []sqlExecRow {
	return wrapSQLSourceWorkers(source, rows, 0)
}

// wrapSQLSourceWorkers parallelizes independent scan-row envelopes while each
// output slot keeps the original source ordinal for deterministic joins.
func wrapSQLSourceWorkers(source sqlSource, rows []SQLRow, workers int) []sqlExecRow {
	out := make([]sqlExecRow, len(rows))
	if workers < 2 || len(rows) < 2 {
		for i, row := range rows {
			out[i] = sqlExecRow{sources: map[string]SQLRow{source.alias: row}, order: []string{source.alias}, ordinals: map[string]int{source.alias: i}}
		}
		return out
	}
	if workers > len(rows) {
		workers = len(rows)
	}
	jobs := make(chan int)
	var group sync.WaitGroup
	for range workers {
		group.Add(1)
		go func() {
			defer group.Done()
			for index := range jobs {
				out[index] = sqlExecRow{sources: map[string]SQLRow{source.alias: rows[index]}, order: []string{source.alias}, ordinals: map[string]int{source.alias: index}}
			}
		}()
	}
	for index := range rows {
		jobs <- index
	}
	close(jobs)
	group.Wait()
	return out
}
func mergeSQLRows(left, right sqlExecRow) sqlExecRow {
	out := sqlExecRow{sources: map[string]SQLRow{}, order: append(append([]string{}, left.order...), right.order...), ordinals: map[string]int{}}
	for k, v := range left.sources {
		out.sources[k] = v
	}
	for k, v := range right.sources {
		out.sources[k] = v
	}
	for alias, ordinal := range left.ordinals {
		out.ordinals[alias] = ordinal
	}
	for alias, ordinal := range right.ordinals {
		out.ordinals[alias] = ordinal
	}
	return out
}

func sqlRowBytes(row SQLRow) int {
	encoded, err := json.Marshal(row)
	if err != nil {
		return len(fmt.Sprintf("%#v", row))
	}
	return len(encoded)
}

func sqlRowsBytes(rows []SQLRow) int {
	total := 0
	for _, row := range rows {
		total += sqlRowBytes(row)
	}
	return total
}

// sqlExecRowsBytes measures the logical source payload carried by execution
// rows. Join aliases are intentionally not serialized: bytes describe values,
// not plan metadata.
func sqlExecRowsBytes(rows []sqlExecRow) int {
	total := 0
	for _, row := range rows {
		for _, source := range row.sources {
			total += sqlRowBytes(source)
		}
	}
	return total
}

func sqlGroupedRowsBytes(groups [][]sqlExecRow) int {
	total := 0
	for _, group := range groups {
		for _, row := range group {
			for _, source := range row.sources {
				total += sqlRowBytes(source)
			}
		}
	}
	return total
}

type sqlSpillBudgetWriter struct {
	writer    io.Writer
	available *int64
}

func (writer sqlSpillBudgetWriter) Write(data []byte) (int, error) {
	if int64(len(data)) > *writer.available {
		return 0, errSQLSpillDiskBudget
	}
	written, err := writer.writer.Write(data)
	*writer.available -= int64(written)
	return written, err
}

func sqlSpillOutputLess(left, right sqlSpillOutput, order []sqlOrder) bool {
	for index, item := range order {
		if less, decided := sqlOrderLess(item, left.Keys[index], right.Keys[index]); decided {
			return less
		}
	}
	return left.Ordinal < right.Ordinal
}

func sqlSpillOutputBytes(record sqlSpillOutput) int {
	bytes := sqlRowBytes(record.Row)
	for _, key := range record.Keys {
		bytes += len(fmt.Sprintf("%#v", key))
	}
	return bytes
}

func sqlWriteSpillRun(directory string, records []sqlSpillOutput, available *int64, control *sqlExecutionControl) (sqlSpillRun, error) {
	file, err := os.CreateTemp(directory, "hatrie-sql-sort-*")
	if err != nil {
		return sqlSpillRun{}, fmt.Errorf("create SQL sort spill file: %w", err)
	}
	run := sqlSpillRun{path: file.Name()}
	remove := true
	defer func() {
		if remove {
			_ = file.Close()
			_ = os.Remove(run.path)
		}
	}()
	encoder := gob.NewEncoder(sqlSpillBudgetWriter{writer: file, available: available})
	for _, record := range records {
		if err := control.check(); err != nil {
			return sqlSpillRun{}, err
		}
		if err := encoder.Encode(record); err != nil {
			if errors.Is(err, errSQLSpillDiskBudget) {
				return sqlSpillRun{}, fmt.Errorf("SQL spill disk budget exceeded while writing sort runs")
			}
			return sqlSpillRun{}, fmt.Errorf("write SQL sort spill file: %w", err)
		}
	}
	if err := file.Close(); err != nil {
		return sqlSpillRun{}, fmt.Errorf("close SQL sort spill file: %w", err)
	}
	info, err := os.Stat(run.path)
	if err != nil {
		return sqlSpillRun{}, fmt.Errorf("inspect SQL sort spill file: %w", err)
	}
	run.bytes = info.Size()
	remove = false
	return run, nil
}

type sqlSpillReader struct {
	file    *os.File
	decoder *gob.Decoder
	current sqlSpillOutput
	done    bool
}

func openSQLSpillReader(run sqlSpillRun) (*sqlSpillReader, error) {
	file, err := os.Open(run.path)
	if err != nil {
		return nil, fmt.Errorf("open SQL sort spill file: %w", err)
	}
	reader := &sqlSpillReader{file: file, decoder: gob.NewDecoder(file)}
	if err := reader.next(); err != nil {
		_ = file.Close()
		return nil, err
	}
	return reader, nil
}

func (reader *sqlSpillReader) next() error {
	if reader.done {
		return nil
	}
	var record sqlSpillOutput
	if err := reader.decoder.Decode(&record); err != nil {
		if errors.Is(err, io.EOF) {
			reader.done = true
			return nil
		}
		return fmt.Errorf("read SQL sort spill file: %w", err)
	}
	reader.current = record
	return nil
}

func closeSQLSpillReaders(readers []*sqlSpillReader) {
	for _, reader := range readers {
		if reader != nil && reader.file != nil {
			_ = reader.file.Close()
		}
	}
}

func sqlMergeSpillRunsToWriter(runs []sqlSpillRun, order []sqlOrder, directory string, available *int64, control *sqlExecutionControl) (sqlSpillRun, error) {
	readers := make([]*sqlSpillReader, len(runs))
	for index, run := range runs {
		reader, err := openSQLSpillReader(run)
		if err != nil {
			closeSQLSpillReaders(readers)
			return sqlSpillRun{}, err
		}
		readers[index] = reader
	}
	defer closeSQLSpillReaders(readers)
	file, err := os.CreateTemp(directory, "hatrie-sql-sort-merge-*")
	if err != nil {
		return sqlSpillRun{}, fmt.Errorf("create SQL sort merge file: %w", err)
	}
	run := sqlSpillRun{path: file.Name()}
	remove := true
	defer func() {
		if remove {
			_ = file.Close()
			_ = os.Remove(run.path)
		}
	}()
	encoder := gob.NewEncoder(sqlSpillBudgetWriter{writer: file, available: available})
	for {
		if err := control.check(); err != nil {
			return sqlSpillRun{}, err
		}
		best := -1
		for index, reader := range readers {
			if reader.done || best >= 0 && !sqlSpillOutputLess(reader.current, readers[best].current, order) {
				continue
			}
			best = index
		}
		if best < 0 {
			break
		}
		if err := encoder.Encode(readers[best].current); err != nil {
			if errors.Is(err, errSQLSpillDiskBudget) {
				return sqlSpillRun{}, fmt.Errorf("SQL spill disk budget exceeded while merging sort runs")
			}
			return sqlSpillRun{}, fmt.Errorf("write SQL sort merge file: %w", err)
		}
		if err := readers[best].next(); err != nil {
			return sqlSpillRun{}, err
		}
	}
	if err := file.Close(); err != nil {
		return sqlSpillRun{}, fmt.Errorf("close SQL sort merge file: %w", err)
	}
	info, err := os.Stat(run.path)
	if err != nil {
		return sqlSpillRun{}, fmt.Errorf("inspect SQL sort merge file: %w", err)
	}
	run.bytes = info.Size()
	remove = false
	return run, nil
}

func sqlMergeSpillRunsToRows(runs []sqlSpillRun, order []sqlOrder, offset, limit int, control *sqlExecutionControl) ([]SQLRow, error) {
	rows := []SQLRow{}
	position := 0
	err := sqlMergeSpillRunsToVisit(runs, order, control, func(record sqlSpillOutput) error {
		if position >= offset && (limit < 0 || len(rows) < limit) {
			rows = append(rows, record.Row)
		}
		position++
		return nil
	})
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func sqlMergeSpillRunsToVisit(runs []sqlSpillRun, order []sqlOrder, control *sqlExecutionControl, visit func(sqlSpillOutput) error) error {
	if visit == nil {
		return fmt.Errorf("SQL spill merge callback is required")
	}
	readers := make([]*sqlSpillReader, len(runs))
	for index, run := range runs {
		reader, err := openSQLSpillReader(run)
		if err != nil {
			closeSQLSpillReaders(readers)
			return err
		}
		readers[index] = reader
	}
	defer closeSQLSpillReaders(readers)
	for {
		if err := control.check(); err != nil {
			return err
		}
		best := -1
		for index, reader := range readers {
			if reader.done || best >= 0 && !sqlSpillOutputLess(reader.current, readers[best].current, order) {
				continue
			}
			best = index
		}
		if best < 0 {
			break
		}
		if err := visit(readers[best].current); err != nil {
			return err
		}
		if err := readers[best].next(); err != nil {
			return err
		}
	}
	return nil
}

func sqlExternalSortRows(records []sqlSpillOutput, order []sqlOrder, directory string, maxRunBytes, maxSpillBytes, offset, limit int, control *sqlExecutionControl) ([]SQLRow, int64, int, error) {
	if directory == "" || maxSpillBytes <= 0 {
		return nil, 0, 0, fmt.Errorf("SQL external sort requires SpillDirectory and MaxSpillBytes")
	}
	available := int64(maxSpillBytes)
	allPaths := map[string]struct{}{}
	defer func() {
		for path := range allPaths {
			_ = os.Remove(path)
		}
	}()
	runs := []sqlSpillRun{}
	chunks := [][]sqlSpillOutput{}
	chunk := make([]sqlSpillOutput, 0)
	chunkBytes := 0
	flush := func() error {
		if len(chunk) == 0 {
			return nil
		}
		chunks = append(chunks, chunk)
		chunk = make([]sqlSpillOutput, 0)
		chunkBytes = 0
		return nil
	}
	for _, record := range records {
		if err := control.check(); err != nil {
			return nil, 0, 0, err
		}
		recordBytes := sqlSpillOutputBytes(record)
		if len(chunk) > 0 && chunkBytes+recordBytes > maxRunBytes {
			if err := flush(); err != nil {
				return nil, 0, 0, err
			}
		}
		chunk = append(chunk, record)
		chunkBytes += recordBytes
	}
	if err := flush(); err != nil {
		return nil, 0, 0, err
	}
	sqlSortSpillChunks(chunks, order, control)
	for _, chunk := range chunks {
		if err := control.check(); err != nil {
			return nil, 0, 0, err
		}
		run, err := sqlWriteSpillRun(directory, chunk, &available, control)
		if err != nil {
			return nil, 0, 0, err
		}
		allPaths[run.path] = struct{}{}
		runs = append(runs, run)
	}
	for len(runs) > maxSQLSpillMergeFanIn {
		next := make([]sqlSpillRun, 0, (len(runs)+maxSQLSpillMergeFanIn-1)/maxSQLSpillMergeFanIn)
		for start := 0; start < len(runs); start += maxSQLSpillMergeFanIn {
			end := start + maxSQLSpillMergeFanIn
			if end > len(runs) {
				end = len(runs)
			}
			merged, err := sqlMergeSpillRunsToWriter(runs[start:end], order, directory, &available, control)
			if err != nil {
				return nil, 0, 0, err
			}
			allPaths[merged.path] = struct{}{}
			for _, run := range runs[start:end] {
				if err := os.Remove(run.path); err != nil && !errors.Is(err, os.ErrNotExist) {
					return nil, 0, 0, fmt.Errorf("remove SQL sort spill file: %w", err)
				}
				delete(allPaths, run.path)
				available += run.bytes
			}
			next = append(next, merged)
		}
		runs = next
	}
	rows, err := sqlMergeSpillRunsToRows(runs, order, offset, limit, control)
	if err != nil {
		return nil, 0, 0, err
	}
	return rows, int64(maxSpillBytes) - available, len(runs), nil
}

func sqlSortSpillChunks(chunks [][]sqlSpillOutput, order []sqlOrder, control *sqlExecutionControl) {
	workers := 1
	if control != nil && control.options.Workers > 1 {
		workers = control.options.Workers
	}
	if workers > len(chunks) {
		workers = len(chunks)
	}
	if workers <= 1 {
		for _, chunk := range chunks {
			sort.SliceStable(chunk, func(left, right int) bool { return sqlSpillOutputLess(chunk[left], chunk[right], order) })
		}
		return
	}
	jobs := make(chan int)
	var group sync.WaitGroup
	for range workers {
		group.Add(1)
		go func() {
			defer group.Done()
			for index := range jobs {
				sort.SliceStable(chunks[index], func(left, right int) bool {
					return sqlSpillOutputLess(chunks[index][left], chunks[index][right], order)
				})
			}
		}()
	}
	for index := range chunks {
		jobs <- index
	}
	close(jobs)
	group.Wait()
}

// sqlSpillSetRecord retains only what a DISTINCT set operator needs while it
// is on disk. Ordinal is the concatenated left/right input position, so the
// final result can keep the engine's existing first-occurrence ordering.
type sqlSpillSetRecord struct {
	Key     string
	Row     SQLRow
	Ordinal int
	Right   bool
}

type sqlSpillSetReader struct {
	file    *os.File
	decoder *gob.Decoder
	current sqlSpillSetRecord
	done    bool
}

func sqlSpillSetRecordBefore(left, right sqlSpillSetRecord) bool {
	if left.Key != right.Key {
		return left.Key < right.Key
	}
	return left.Ordinal < right.Ordinal
}

func sqlSpillSetRecordBytes(record sqlSpillSetRecord) int {
	return len(record.Key) + sqlRowBytes(record.Row) + 32
}

func sqlWriteSpillSetRun(directory string, records []sqlSpillSetRecord, available *int64, control *sqlExecutionControl) (sqlSpillRun, error) {
	file, err := os.CreateTemp(directory, "hatrie-sql-set-*")
	if err != nil {
		return sqlSpillRun{}, fmt.Errorf("create SQL set spill file: %w", err)
	}
	run := sqlSpillRun{path: file.Name()}
	remove := true
	defer func() {
		if remove {
			_ = file.Close()
			_ = os.Remove(run.path)
		}
	}()
	encoder := gob.NewEncoder(sqlSpillBudgetWriter{writer: file, available: available})
	for _, record := range records {
		if err := control.check(); err != nil {
			return sqlSpillRun{}, err
		}
		if err := encoder.Encode(record); err != nil {
			if errors.Is(err, errSQLSpillDiskBudget) {
				return sqlSpillRun{}, fmt.Errorf("SQL spill disk budget exceeded while writing set runs")
			}
			return sqlSpillRun{}, fmt.Errorf("write SQL set spill file: %w", err)
		}
	}
	if err := file.Close(); err != nil {
		return sqlSpillRun{}, fmt.Errorf("close SQL set spill file: %w", err)
	}
	info, err := os.Stat(run.path)
	if err != nil {
		return sqlSpillRun{}, fmt.Errorf("inspect SQL set spill file: %w", err)
	}
	run.bytes = info.Size()
	remove = false
	return run, nil
}

func openSQLSpillSetReader(run sqlSpillRun) (*sqlSpillSetReader, error) {
	file, err := os.Open(run.path)
	if err != nil {
		return nil, fmt.Errorf("open SQL set spill file: %w", err)
	}
	reader := &sqlSpillSetReader{file: file, decoder: gob.NewDecoder(file)}
	if err := reader.next(); err != nil {
		_ = file.Close()
		return nil, err
	}
	return reader, nil
}

func (reader *sqlSpillSetReader) next() error {
	if reader.done {
		return nil
	}
	var record sqlSpillSetRecord
	if err := reader.decoder.Decode(&record); err != nil {
		if errors.Is(err, io.EOF) {
			reader.done = true
			return nil
		}
		return fmt.Errorf("read SQL set spill file: %w", err)
	}
	reader.current = record
	return nil
}

func closeSQLSpillSetReaders(readers []*sqlSpillSetReader) {
	for _, reader := range readers {
		if reader != nil && reader.file != nil {
			_ = reader.file.Close()
		}
	}
}

func sqlOpenSpillSetReaders(runs []sqlSpillRun) ([]*sqlSpillSetReader, error) {
	readers := make([]*sqlSpillSetReader, len(runs))
	for index, run := range runs {
		reader, err := openSQLSpillSetReader(run)
		if err != nil {
			closeSQLSpillSetReaders(readers)
			return nil, err
		}
		readers[index] = reader
	}
	return readers, nil
}

func sqlNextRawSpillSet(readers []*sqlSpillSetReader, control *sqlExecutionControl) (sqlSpillSetRecord, bool, error) {
	if err := control.check(); err != nil {
		return sqlSpillSetRecord{}, false, err
	}
	best := -1
	for index, reader := range readers {
		if reader.done || best >= 0 && !sqlSpillSetRecordBefore(reader.current, readers[best].current) {
			continue
		}
		best = index
	}
	if best < 0 {
		return sqlSpillSetRecord{}, false, nil
	}
	record := readers[best].current
	if err := readers[best].next(); err != nil {
		return sqlSpillSetRecord{}, false, err
	}
	return record, true, nil
}

func sqlMergeSpillSetRunsToRun(runs []sqlSpillRun, directory string, available *int64, control *sqlExecutionControl) (sqlSpillRun, error) {
	readers, err := sqlOpenSpillSetReaders(runs)
	if err != nil {
		return sqlSpillRun{}, err
	}
	defer closeSQLSpillSetReaders(readers)
	file, err := os.CreateTemp(directory, "hatrie-sql-set-merge-*")
	if err != nil {
		return sqlSpillRun{}, fmt.Errorf("create SQL set merge file: %w", err)
	}
	run := sqlSpillRun{path: file.Name()}
	remove := true
	defer func() {
		if remove {
			_ = file.Close()
			_ = os.Remove(run.path)
		}
	}()
	encoder := gob.NewEncoder(sqlSpillBudgetWriter{writer: file, available: available})
	for {
		record, ok, err := sqlNextRawSpillSet(readers, control)
		if err != nil {
			return sqlSpillRun{}, err
		}
		if !ok {
			break
		}
		if err := encoder.Encode(record); err != nil {
			if errors.Is(err, errSQLSpillDiskBudget) {
				return sqlSpillRun{}, fmt.Errorf("SQL spill disk budget exceeded while merging set runs")
			}
			return sqlSpillRun{}, fmt.Errorf("write SQL set spill file: %w", err)
		}
	}
	if err := file.Close(); err != nil {
		return sqlSpillRun{}, fmt.Errorf("close SQL set spill file: %w", err)
	}
	info, err := os.Stat(run.path)
	if err != nil {
		return sqlSpillRun{}, fmt.Errorf("inspect SQL set spill file: %w", err)
	}
	run.bytes = info.Size()
	remove = false
	return run, nil
}

type sqlSetOutput struct {
	row     SQLRow
	ordinal int
}

func sqlExternalSetRows(left, right []SQLRow, operation, directory string, maxRunBytes, maxSpillBytes int, control *sqlExecutionControl) ([]SQLRow, int64, int, error) {
	if directory == "" || maxSpillBytes <= 0 {
		return nil, 0, 0, fmt.Errorf("SQL external set requires SpillDirectory and MaxSpillBytes")
	}
	available := int64(maxSpillBytes)
	allPaths := map[string]struct{}{}
	defer func() {
		for path := range allPaths {
			_ = os.Remove(path)
		}
	}()
	runs := []sqlSpillRun{}
	chunk := make([]sqlSpillSetRecord, 0)
	chunkBytes := 0
	flush := func() error {
		if len(chunk) == 0 {
			return nil
		}
		sort.SliceStable(chunk, func(left, right int) bool { return sqlSpillSetRecordBefore(chunk[left], chunk[right]) })
		run, err := sqlWriteSpillSetRun(directory, chunk, &available, control)
		if err != nil {
			return err
		}
		allPaths[run.path] = struct{}{}
		runs = append(runs, run)
		chunk = make([]sqlSpillSetRecord, 0)
		chunkBytes = 0
		return nil
	}
	appendRows := func(rows []SQLRow, rightSide bool, ordinalStart int) error {
		for index, row := range rows {
			if err := control.check(); err != nil {
				return err
			}
			record := sqlSpillSetRecord{Key: sqlOutputRowKey(row), Row: row, Ordinal: ordinalStart + index, Right: rightSide}
			recordBytes := sqlSpillSetRecordBytes(record)
			if len(chunk) > 0 && chunkBytes+recordBytes > maxRunBytes {
				if err := flush(); err != nil {
					return err
				}
			}
			chunk = append(chunk, record)
			chunkBytes += recordBytes
		}
		return nil
	}
	if err := appendRows(left, false, 0); err != nil {
		return nil, 0, 0, err
	}
	if err := appendRows(right, true, len(left)); err != nil {
		return nil, 0, 0, err
	}
	if err := flush(); err != nil {
		return nil, 0, 0, err
	}
	for len(runs) > maxSQLSpillMergeFanIn {
		next := make([]sqlSpillRun, 0, (len(runs)+maxSQLSpillMergeFanIn-1)/maxSQLSpillMergeFanIn)
		for start := 0; start < len(runs); start += maxSQLSpillMergeFanIn {
			end := start + maxSQLSpillMergeFanIn
			if end > len(runs) {
				end = len(runs)
			}
			merged, err := sqlMergeSpillSetRunsToRun(runs[start:end], directory, &available, control)
			if err != nil {
				return nil, 0, 0, err
			}
			allPaths[merged.path] = struct{}{}
			for _, run := range runs[start:end] {
				if err := os.Remove(run.path); err != nil && !errors.Is(err, os.ErrNotExist) {
					return nil, 0, 0, fmt.Errorf("remove SQL set spill file: %w", err)
				}
				delete(allPaths, run.path)
				available += run.bytes
			}
			next = append(next, merged)
		}
		runs = next
	}
	readers, err := sqlOpenSpillSetReaders(runs)
	if err != nil {
		return nil, 0, 0, err
	}
	defer closeSQLSpillSetReaders(readers)
	output := make([]sqlSetOutput, 0)
	for {
		first, ok, err := sqlNextRawSpillSet(readers, control)
		if err != nil {
			return nil, 0, 0, err
		}
		if !ok {
			break
		}
		hasLeft, hasRight := !first.Right, first.Right
		firstLeft := first
		for {
			best := -1
			for index, reader := range readers {
				if reader.done || reader.current.Key != first.Key || best >= 0 && !sqlSpillSetRecordBefore(reader.current, readers[best].current) {
					continue
				}
				best = index
			}
			if best < 0 {
				break
			}
			record := readers[best].current
			if record.Right {
				hasRight = true
			} else if !hasLeft {
				hasLeft = true
				firstLeft = record
			}
			if err := readers[best].next(); err != nil {
				return nil, 0, 0, err
			}
		}
		switch operation {
		case "UNION":
			output = append(output, sqlSetOutput{row: first.Row, ordinal: first.Ordinal})
		case "INTERSECT":
			if hasLeft && hasRight {
				output = append(output, sqlSetOutput{row: firstLeft.Row, ordinal: firstLeft.Ordinal})
			}
		case "EXCEPT":
			if hasLeft && !hasRight {
				output = append(output, sqlSetOutput{row: firstLeft.Row, ordinal: firstLeft.Ordinal})
			}
		default:
			return nil, 0, 0, fmt.Errorf("unsupported SQL set operation %q", operation)
		}
	}
	sort.SliceStable(output, func(left, right int) bool { return output[left].ordinal < output[right].ordinal })
	rows := make([]SQLRow, len(output))
	for index, record := range output {
		rows[index] = record.row
	}
	return rows, int64(maxSpillBytes) - available, len(runs), nil
}

type sqlSpillGroupAggregate struct {
	Name  string
	Count int64
	Sum   float64
	Seen  bool
	Min   float64
	Max   float64
}

type sqlSpillGroupRecord struct {
	Key        string
	Value      interface{}
	Ordinal    int
	Aggregates []sqlSpillGroupAggregate
}

type sqlSpillGroupRun struct {
	path  string
	bytes int64
}

type sqlSpillGroupReader struct {
	file    *os.File
	decoder *gob.Decoder
	current sqlSpillGroupRecord
	done    bool
}

func sqlSpillGroupRecordBefore(left, right sqlSpillGroupRecord, order sqlOrder) bool {
	if less, decided := sqlOrderLess(order, left.Value, right.Value); decided {
		return less
	}
	// Group identity must be the secondary ordering so every partial state for
	// one key is adjacent across independently sorted runs. SQL does not define
	// an order among otherwise equal ORDER BY keys.
	if left.Key != right.Key {
		return left.Key < right.Key
	}
	return left.Ordinal < right.Ordinal
}

func sqlSpillGroupRecordBytes(record sqlSpillGroupRecord) int {
	return len(record.Key) + sqlRowBytes(SQLRow{"group": record.Value}) + len(record.Aggregates)*64 + 32
}

func sqlSpillGroupAggregateFromOrdered(aggregate sqlOrderedAggregate) sqlSpillGroupAggregate {
	return sqlSpillGroupAggregate{Name: aggregate.name, Count: aggregate.count, Sum: aggregate.sum, Seen: aggregate.seen, Min: aggregate.min, Max: aggregate.max}
}

func (aggregate sqlSpillGroupAggregate) value() interface{} {
	if aggregate.Name == "COUNT" {
		return aggregate.Count
	}
	if !aggregate.Seen {
		return nil
	}
	switch aggregate.Name {
	case "SUM":
		return aggregate.Sum
	case "AVG":
		return aggregate.Sum / float64(aggregate.Count)
	case "MIN":
		return aggregate.Min
	case "MAX":
		return aggregate.Max
	}
	return nil
}

func sqlMergeSpillGroupRecord(left, right sqlSpillGroupRecord) sqlSpillGroupRecord {
	if right.Ordinal < left.Ordinal {
		left.Ordinal = right.Ordinal
		left.Value = right.Value
	}
	for index := range left.Aggregates {
		other := right.Aggregates[index]
		current := &left.Aggregates[index]
		if current.Name == "COUNT" {
			current.Count += other.Count
			continue
		}
		if !other.Seen {
			continue
		}
		if !current.Seen {
			*current = other
			continue
		}
		current.Count += other.Count
		switch current.Name {
		case "SUM", "AVG":
			current.Sum += other.Sum
		case "MIN":
			if other.Min < current.Min {
				current.Min = other.Min
			}
		case "MAX":
			if other.Max > current.Max {
				current.Max = other.Max
			}
		}
	}
	return left
}

func sqlWriteSpillGroupRun(directory string, records []sqlSpillGroupRecord, available *int64, control *sqlExecutionControl) (sqlSpillGroupRun, error) {
	file, err := os.CreateTemp(directory, "hatrie-sql-group-*")
	if err != nil {
		return sqlSpillGroupRun{}, fmt.Errorf("create SQL group spill file: %w", err)
	}
	run := sqlSpillGroupRun{path: file.Name()}
	remove := true
	defer func() {
		if remove {
			_ = file.Close()
			_ = os.Remove(run.path)
		}
	}()
	encoder := gob.NewEncoder(sqlSpillBudgetWriter{writer: file, available: available})
	for _, record := range records {
		if err := control.check(); err != nil {
			return sqlSpillGroupRun{}, err
		}
		if err := encoder.Encode(record); err != nil {
			if errors.Is(err, errSQLSpillDiskBudget) {
				return sqlSpillGroupRun{}, fmt.Errorf("SQL spill disk budget exceeded while writing aggregate runs")
			}
			return sqlSpillGroupRun{}, fmt.Errorf("write SQL group spill file: %w", err)
		}
	}
	if err := file.Close(); err != nil {
		return sqlSpillGroupRun{}, fmt.Errorf("close SQL group spill file: %w", err)
	}
	info, err := os.Stat(run.path)
	if err != nil {
		return sqlSpillGroupRun{}, fmt.Errorf("inspect SQL group spill file: %w", err)
	}
	run.bytes = info.Size()
	remove = false
	return run, nil
}

func openSQLSpillGroupReader(run sqlSpillGroupRun) (*sqlSpillGroupReader, error) {
	file, err := os.Open(run.path)
	if err != nil {
		return nil, fmt.Errorf("open SQL group spill file: %w", err)
	}
	reader := &sqlSpillGroupReader{file: file, decoder: gob.NewDecoder(file)}
	if err := reader.next(); err != nil {
		_ = file.Close()
		return nil, err
	}
	return reader, nil
}

func (reader *sqlSpillGroupReader) next() error {
	if reader.done {
		return nil
	}
	var record sqlSpillGroupRecord
	if err := reader.decoder.Decode(&record); err != nil {
		if errors.Is(err, io.EOF) {
			reader.done = true
			return nil
		}
		return fmt.Errorf("read SQL group spill file: %w", err)
	}
	reader.current = record
	return nil
}

func closeSQLSpillGroupReaders(readers []*sqlSpillGroupReader) {
	for _, reader := range readers {
		if reader != nil && reader.file != nil {
			_ = reader.file.Close()
		}
	}
}

func sqlOpenSpillGroupReaders(runs []sqlSpillGroupRun) ([]*sqlSpillGroupReader, error) {
	readers := make([]*sqlSpillGroupReader, len(runs))
	for index, run := range runs {
		reader, err := openSQLSpillGroupReader(run)
		if err != nil {
			closeSQLSpillGroupReaders(readers)
			return nil, err
		}
		readers[index] = reader
	}
	return readers, nil
}

func sqlNextRawSpillGroup(readers []*sqlSpillGroupReader, order sqlOrder, control *sqlExecutionControl) (sqlSpillGroupRecord, bool, error) {
	if err := control.check(); err != nil {
		return sqlSpillGroupRecord{}, false, err
	}
	best := -1
	for index, reader := range readers {
		if reader.done || best >= 0 && !sqlSpillGroupRecordBefore(reader.current, readers[best].current, order) {
			continue
		}
		best = index
	}
	if best < 0 {
		return sqlSpillGroupRecord{}, false, nil
	}
	record := readers[best].current
	if err := readers[best].next(); err != nil {
		return sqlSpillGroupRecord{}, false, err
	}
	return record, true, nil
}

func sqlNextSpillGroup(readers []*sqlSpillGroupReader, order sqlOrder, control *sqlExecutionControl) (sqlSpillGroupRecord, bool, error) {
	record, ok, err := sqlNextRawSpillGroup(readers, order, control)
	if err != nil || !ok {
		return record, ok, err
	}
	for {
		best := -1
		for index, reader := range readers {
			if reader.done || reader.current.Key != record.Key || best >= 0 && !sqlSpillGroupRecordBefore(reader.current, readers[best].current, order) {
				continue
			}
			best = index
		}
		if best < 0 {
			return record, true, nil
		}
		record = sqlMergeSpillGroupRecord(record, readers[best].current)
		if err := readers[best].next(); err != nil {
			return sqlSpillGroupRecord{}, false, err
		}
	}
}

func sqlMergeSpillGroupRunsToRun(runs []sqlSpillGroupRun, order sqlOrder, directory string, available *int64, control *sqlExecutionControl) (sqlSpillGroupRun, error) {
	readers, err := sqlOpenSpillGroupReaders(runs)
	if err != nil {
		return sqlSpillGroupRun{}, err
	}
	defer closeSQLSpillGroupReaders(readers)
	file, err := os.CreateTemp(directory, "hatrie-sql-group-merge-*")
	if err != nil {
		return sqlSpillGroupRun{}, fmt.Errorf("create SQL group merge file: %w", err)
	}
	run := sqlSpillGroupRun{path: file.Name()}
	remove := true
	defer func() {
		if remove {
			_ = file.Close()
			_ = os.Remove(run.path)
		}
	}()
	encoder := gob.NewEncoder(sqlSpillBudgetWriter{writer: file, available: available})
	for {
		record, ok, err := sqlNextRawSpillGroup(readers, order, control)
		if err != nil {
			return sqlSpillGroupRun{}, err
		}
		if !ok {
			break
		}
		if err := encoder.Encode(record); err != nil {
			if errors.Is(err, errSQLSpillDiskBudget) {
				return sqlSpillGroupRun{}, fmt.Errorf("SQL spill disk budget exceeded while merging aggregate runs")
			}
			return sqlSpillGroupRun{}, fmt.Errorf("write SQL group merge file: %w", err)
		}
	}
	if err := file.Close(); err != nil {
		return sqlSpillGroupRun{}, fmt.Errorf("close SQL group merge file: %w", err)
	}
	info, err := os.Stat(run.path)
	if err != nil {
		return sqlSpillGroupRun{}, fmt.Errorf("inspect SQL group merge file: %w", err)
	}
	run.bytes = info.Size()
	remove = false
	return run, nil
}

func sqlCanPushBaseWhere(query *sqlQuery) bool {
	for _, join := range query.joins {
		if join.kind != "INNER" && join.kind != "CROSS" {
			return false
		}
	}
	return sqlExprReferencesOnlyAlias(query.where, query.from.alias)
}

// sqlInnerJoinPushdownCondition extracts deterministic AND terms that inspect
// only one new INNER JOIN source. Outer joins are intentionally excluded: a
// right predicate can change whether an unmatched null-extended row exists.
func sqlInnerJoinPushdownCondition(condition sqlExpr, join sqlJoin) sqlExpr {
	if join.kind != "INNER" || condition.kind == "" {
		return sqlExpr{}
	}
	terms := []sqlExpr{}
	var collect func(sqlExpr)
	collect = func(current sqlExpr) {
		if current.kind == "binary" && current.op == "AND" && current.left != nil && current.right != nil {
			collect(*current.left)
			collect(*current.right)
			return
		}
		if sqlExprReferencesOnlyAlias(current, join.source.alias) && !sqlExprHasWindow(current) && !sqlExprHasAggregate(current) && !sqlExprHasFunction(current) {
			terms = append(terms, current)
		}
	}
	collect(condition)
	if len(terms) == 0 {
		return sqlExpr{}
	}
	result := terms[0]
	for _, term := range terms[1:] {
		left, right := result, term
		result = sqlExpr{kind: "binary", op: "AND", left: &left, right: &right}
	}
	return result
}

func sqlExprHasFunction(expression sqlExpr) bool {
	if expression.kind == "func" {
		return true
	}
	if expression.left != nil && sqlExprHasFunction(*expression.left) {
		return true
	}
	if expression.right != nil && sqlExprHasFunction(*expression.right) {
		return true
	}
	for _, argument := range expression.args {
		if sqlExprHasFunction(argument) {
			return true
		}
	}
	for _, branch := range expression.cases {
		if sqlExprHasFunction(branch.when) || sqlExprHasFunction(branch.then) {
			return true
		}
	}
	return false
}

func sqlExprReferencesOnlyAlias(expression sqlExpr, alias string) bool {
	switch expression.kind {
	case "literal":
		return true
	case "field":
		return expression.qualifier != "" && expression.qualifier == alias
	case "unary":
		return expression.left != nil && sqlExprReferencesOnlyAlias(*expression.left, alias)
	case "binary":
		return expression.left != nil && sqlExprReferencesOnlyAlias(*expression.left, alias) && (expression.right == nil || sqlExprReferencesOnlyAlias(*expression.right, alias))
	case "cast":
		return len(expression.args) == 1 && sqlExprReferencesOnlyAlias(expression.args[0], alias)
	case "case":
		if expression.left != nil && !sqlExprReferencesOnlyAlias(*expression.left, alias) {
			return false
		}
		for _, branch := range expression.cases {
			if !sqlExprReferencesOnlyAlias(branch.when, alias) || !sqlExprReferencesOnlyAlias(branch.then, alias) {
				return false
			}
		}
		return expression.right == nil || sqlExprReferencesOnlyAlias(*expression.right, alias)
	case "func":
		for _, argument := range expression.args {
			if !sqlExprReferencesOnlyAlias(argument, alias) {
				return false
			}
		}
		return true
	}
	return false
}

func sqlHashJoinFields(expression sqlExpr, leftAliases []string, rightAlias string) (string, string, string, bool) {
	if expression.kind != "binary" || expression.op != "=" || expression.left == nil || expression.right == nil || expression.left.kind != "field" || expression.right.kind != "field" {
		return "", "", "", false
	}
	left := *expression.left
	right := *expression.right
	isLeftAlias := func(alias string) bool {
		for _, candidate := range leftAliases {
			if alias == candidate {
				return true
			}
		}
		return false
	}
	if isLeftAlias(left.qualifier) && right.qualifier == rightAlias {
		return left.qualifier, left.name, right.name, true
	}
	if isLeftAlias(right.qualifier) && left.qualifier == rightAlias {
		return right.qualifier, right.name, left.name, true
	}
	return "", "", "", false
}

func sqlRangeJoinFields(expression sqlExpr, leftAliases []string, rightAlias string) (string, string, string, string, bool) {
	if expression.kind == "binary" && expression.op == "AND" && expression.left != nil && expression.right != nil {
		if qualifier, leftField, rightField, operator, ok := sqlRangeJoinFields(*expression.left, leftAliases, rightAlias); ok {
			return qualifier, leftField, rightField, operator, true
		}
		return sqlRangeJoinFields(*expression.right, leftAliases, rightAlias)
	}
	if expression.kind != "binary" || (expression.op != "<" && expression.op != "<=" && expression.op != ">" && expression.op != ">=") || expression.left == nil || expression.right == nil || expression.left.kind != "field" || expression.right.kind != "field" {
		return "", "", "", "", false
	}
	left, right := *expression.left, *expression.right
	isLeft := func(alias string) bool {
		for _, candidate := range leftAliases {
			if alias == candidate {
				return true
			}
		}
		return false
	}
	if isLeft(left.qualifier) && right.qualifier == rightAlias {
		return left.qualifier, left.name, right.name, sqlReverseComparison(expression.op), true
	}
	if isLeft(right.qualifier) && left.qualifier == rightAlias {
		return right.qualifier, right.name, left.name, expression.op, true
	}
	return "", "", "", "", false
}

// sqlCompositeJoinFields accepts only a pure AND of two or more field-equality
// terms that each connect an already-joined source to the new right source.
// Returning fields in right-field order makes the mapping deterministic while
// leaving the resolver free to select any compatible configured composite index.
func sqlCompositeJoinFields(expression sqlExpr, leftAliases []string, rightAlias string) (leftQualifiers, leftFields, rightFields []string, ok bool) {
	type pair struct{ qualifier, leftField, rightField string }
	pairs := []pair{}
	seenRight := map[string]bool{}
	var collect func(sqlExpr) bool
	collect = func(current sqlExpr) bool {
		if current.kind == "binary" && current.op == "AND" && current.left != nil && current.right != nil {
			return collect(*current.left) && collect(*current.right)
		}
		qualifier, leftField, rightField, matched := sqlHashJoinFields(current, leftAliases, rightAlias)
		if !matched || seenRight[rightField] {
			return false
		}
		seenRight[rightField] = true
		pairs = append(pairs, pair{qualifier: qualifier, leftField: leftField, rightField: rightField})
		return true
	}
	if !collect(expression) || len(pairs) < 2 {
		return nil, nil, nil, false
	}
	sort.Slice(pairs, func(left, right int) bool { return pairs[left].rightField < pairs[right].rightField })
	for _, pair := range pairs {
		leftQualifiers = append(leftQualifiers, pair.qualifier)
		leftFields = append(leftFields, pair.leftField)
		rightFields = append(rightFields, pair.rightField)
	}
	return leftQualifiers, leftFields, rightFields, true
}

func sqlHashJoinKey(value interface{}) (string, bool) {
	if value == nil {
		return "", false
	}
	if number, ok := sqlNumber(value); ok {
		return "number:" + strconv.FormatFloat(number, 'g', -1, 64), true
	}
	switch value := value.(type) {
	case string:
		return "string:" + value, true
	case bool:
		return "bool:" + strconv.FormatBool(value), true
	}
	return "", false
}

func groupSQLRows(rows []sqlExecRow, by []sqlExpr, q *sqlQuery) ([][]sqlExecRow, error) {
	if len(by) == 0 {
		if !sqlQueryHasAggregate(q) {
			out := make([][]sqlExecRow, len(rows))
			for i, row := range rows {
				out[i] = []sqlExecRow{row}
			}
			return out, nil
		}
		if len(rows) == 0 {
			return [][]sqlExecRow{{}}, nil
		}
		return [][]sqlExecRow{rows}, nil
	}
	groups := map[string][]sqlExecRow{}
	order := []string{}
	for _, row := range rows {
		parts := make([]string, len(by))
		for i, expr := range by {
			value := evalSQLExpr(expr, []sqlExecRow{row}, row)
			if err := sqlExpressionError(value); err != nil {
				return nil, err
			}
			parts[i] = fmt.Sprintf("%#v", value)
		}
		key := strings.Join(parts, "\x00")
		if _, ok := groups[key]; !ok {
			order = append(order, key)
		}
		groups[key] = append(groups[key], row)
	}
	out := make([][]sqlExecRow, 0, len(order))
	for _, key := range order {
		out = append(out, groups[key])
	}
	return out, nil
}

// groupSQLRowsOrdered groups adjacent equal keys from an index-ordered source.
// Its caller proves that the one GROUP BY expression is exactly the indexed
// ORDER BY field, so a key cannot reappear after a different key. This avoids
// the grouping hash table while retaining all group rows for aggregate
// semantics and existing group-memory accounting.
func groupSQLRowsOrdered(rows []sqlExecRow, by []sqlExpr, q *sqlQuery) ([][]sqlExecRow, error) {
	if len(by) == 0 {
		return groupSQLRows(rows, by, q)
	}
	out := make([][]sqlExecRow, 0, len(rows))
	previousKey := ""
	for index, row := range rows {
		parts := make([]string, len(by))
		for expressionIndex, expr := range by {
			value := evalSQLExpr(expr, []sqlExecRow{row}, row)
			if err := sqlExpressionError(value); err != nil {
				return nil, err
			}
			parts[expressionIndex] = fmt.Sprintf("%#v", value)
		}
		key := strings.Join(parts, "\x00")
		if index == 0 || key != previousKey {
			out = append(out, []sqlExecRow{row})
			previousKey = key
			continue
		}
		out[len(out)-1] = append(out[len(out)-1], row)
	}
	return out, nil
}

type sqlOrderedGroupProjection struct {
	column    string
	group     bool
	aggregate *sqlOrderedAggregate
}

type sqlOrderedAggregate struct {
	name  string
	field sqlExpr
	count int64
	sum   float64
	seen  bool
	min   float64
	max   float64
}

func sqlSameField(left, right sqlExpr) bool {
	return left.kind == "field" && right.kind == "field" && left.qualifier == right.qualifier && left.name == right.name
}

// sqlOrderedGroupProjections recognizes only aggregates whose state can be
// updated one source row at a time. More expressive grouped queries continue
// through the established materialized evaluator rather than approximating its
// representative-row, HAVING, window, or function semantics.
func sqlOrderedGroupProjections(q *sqlQuery) ([]sqlOrderedGroupProjection, bool) {
	if q == nil || len(q.groupBy) != 1 || q.groupBy[0].kind != "field" || q.having.kind != "" || q.distinct || len(q.unions) != 0 || sqlQueryHasWindow(q) {
		return nil, false
	}
	columns := sqlColumns(q.selects)
	projections := make([]sqlOrderedGroupProjection, len(q.selects))
	for index, item := range q.selects {
		projection := sqlOrderedGroupProjection{column: columns[index]}
		if sqlSameField(item.expr, q.groupBy[0]) {
			projection.group = true
			projections[index] = projection
			continue
		}
		if item.expr.kind != "func" {
			return nil, false
		}
		name := strings.ToUpper(item.expr.name)
		aggregate := &sqlOrderedAggregate{name: name}
		switch name {
		case "COUNT":
			if len(item.expr.args) == 0 || len(item.expr.args) == 1 && item.expr.args[0].kind == "star" {
				projection.aggregate = aggregate
				projections[index] = projection
				continue
			}
			if len(item.expr.args) != 1 || item.expr.args[0].kind != "field" {
				return nil, false
			}
			aggregate.field = item.expr.args[0]
		case "SUM", "AVG", "MIN", "MAX":
			if len(item.expr.args) != 1 || item.expr.args[0].kind != "field" {
				return nil, false
			}
			aggregate.field = item.expr.args[0]
		default:
			return nil, false
		}
		projection.aggregate = aggregate
		projections[index] = projection
	}
	return projections, true
}

func (aggregate *sqlOrderedAggregate) add(row sqlExecRow) error {
	if aggregate.name == "COUNT" && aggregate.field.kind == "" {
		aggregate.count++
		return nil
	}
	value := evalSQLExpr(aggregate.field, []sqlExecRow{row}, row)
	if err := sqlExpressionError(value); err != nil {
		return err
	}
	if aggregate.name == "COUNT" {
		if value != nil {
			aggregate.count++
		}
		return nil
	}
	number, ok := sqlNumber(value)
	if !ok {
		return nil
	}
	if !aggregate.seen {
		aggregate.seen = true
		aggregate.sum = number
		aggregate.min = number
		aggregate.max = number
		aggregate.count = 1
		return nil
	}
	aggregate.count++
	switch aggregate.name {
	case "SUM", "AVG":
		aggregate.sum += number
	case "MIN":
		if number < aggregate.min {
			aggregate.min = number
		}
	case "MAX":
		if number > aggregate.max {
			aggregate.max = number
		}
	}
	return nil
}

func (aggregate *sqlOrderedAggregate) value() interface{} {
	if aggregate.name == "COUNT" {
		return aggregate.count
	}
	if !aggregate.seen {
		return nil
	}
	switch aggregate.name {
	case "SUM":
		return aggregate.sum
	case "AVG":
		return aggregate.sum / float64(aggregate.count)
	case "MIN":
		return aggregate.min
	case "MAX":
		return aggregate.max
	}
	return nil
}

// executeSQLOrderedGroupAggregate consumes an index-ordered GROUP BY stream
// with constant state per aggregate. resolveSQLOrderedSource has already
// proved that equal group keys are adjacent and that the final ORDER BY uses
// the same field, so no grouping hash map or final sort is needed.
func executeSQLOrderedGroupAggregate(q *sqlQuery, rows []sqlExecRow, control *sqlExecutionControl, metrics *sqlExecutionMetrics) (SQLQueryResult, bool, error) {
	projections, ok := sqlOrderedGroupProjections(q)
	if !ok {
		return SQLQueryResult{}, false, nil
	}
	result := SQLQueryResult{Columns: sqlColumns(q.selects), Rows: []SQLRow{}}
	started := time.Now()
	groupCount := 0
	position := 0
	var key string
	var groupValue interface{}
	var aggregates []sqlOrderedAggregate
	emit := func() error {
		if groupCount == 0 {
			return nil
		}
		row := SQLRow{}
		for index, projection := range projections {
			if projection.group {
				row[projection.column] = groupValue
				continue
			}
			row[projection.column] = aggregates[index].value()
		}
		if position >= q.offset && (q.limit < 0 || len(result.Rows) < q.limit) {
			result.Rows = append(result.Rows, row)
		}
		position++
		return nil
	}
	for _, sourceRow := range rows {
		if err := control.check(); err != nil {
			return SQLQueryResult{}, true, err
		}
		value := evalSQLExpr(q.groupBy[0], []sqlExecRow{sourceRow}, sourceRow)
		if err := sqlExpressionError(value); err != nil {
			return SQLQueryResult{}, true, err
		}
		currentKey := fmt.Sprintf("%#v", value)
		if groupCount == 0 || currentKey != key {
			if err := emit(); err != nil {
				return SQLQueryResult{}, true, err
			}
			key = currentKey
			groupValue = value
			aggregates = make([]sqlOrderedAggregate, len(projections))
			for index, projection := range projections {
				if projection.aggregate != nil {
					aggregates[index] = *projection.aggregate
				}
			}
			groupCount++
		}
		for index, projection := range projections {
			if projection.aggregate == nil {
				continue
			}
			if err := aggregates[index].add(sourceRow); err != nil {
				return SQLQueryResult{}, true, err
			}
		}
	}
	if err := emit(); err != nil {
		return SQLQueryResult{}, true, err
	}
	if control != nil && control.options.MaxResultBytes > 0 && sqlRowsBytes(result.Rows) > control.options.MaxResultBytes {
		return SQLQueryResult{}, true, fmt.Errorf("SQL result byte budget exceeded: maximum %d bytes", control.options.MaxResultBytes)
	}
	metrics.record("INDEX GROUP AGGREGATE", "streaming "+sqlExplainExpressions(q.groupBy), len(rows), groupCount, started)
	metrics.record("PROJECT", sqlExplainSelects(q.selects), groupCount, groupCount, started)
	if q.limit >= 0 || q.offset > 0 {
		metrics.record("LIMIT", fmt.Sprintf("limit=%d offset=%d", q.limit, q.offset), groupCount, len(result.Rows), started)
	}
	return result, true, nil
}

func sqlSpilledGroupAggregateProjections(q *sqlQuery) ([]sqlOrderedGroupProjection, sqlOrder, bool) {
	projections, ok := sqlOrderedGroupProjections(q)
	if !ok || len(q.orderBy) != 1 || !sqlSameField(q.orderBy[0].expr, q.groupBy[0]) {
		return nil, sqlOrder{}, false
	}
	return projections, q.orderBy[0], true
}

func sqlAddSpillGroupAggregate(state *sqlSpillGroupAggregate, definition *sqlOrderedAggregate, row sqlExecRow) error {
	if definition.name == "COUNT" && definition.field.kind == "" {
		state.Count++
		return nil
	}
	value := evalSQLExpr(definition.field, []sqlExecRow{row}, row)
	if err := sqlExpressionError(value); err != nil {
		return err
	}
	if definition.name == "COUNT" {
		if value != nil {
			state.Count++
		}
		return nil
	}
	number, ok := sqlNumber(value)
	if !ok {
		return nil
	}
	if !state.Seen {
		state.Seen = true
		state.Count = 1
		state.Sum = number
		state.Min = number
		state.Max = number
		return nil
	}
	state.Count++
	switch state.Name {
	case "SUM", "AVG":
		state.Sum += number
	case "MIN":
		if number < state.Min {
			state.Min = number
		}
	case "MAX":
		if number > state.Max {
			state.Max = number
		}
	}
	return nil
}

// executeSQLSpilledGroupAggregate implements the direct aggregate subset with
// bounded contribution runs. It deliberately requires ORDER BY the same single
// group field, allowing sorted spill-run merging to preserve query order and
// source-order floating-point accumulation without retaining group rows.
func sqlCanStreamSpilledGroupAggregate(q *sqlQuery, resolver SQLSourceResolver, control *sqlExecutionControl) bool {
	if q == nil || q.from == nil || control == nil || control.options.MaxGroupBytes <= 0 || control.options.SpillDirectory == "" || control.options.MaxSpillBytes <= 0 || len(q.ctes) != 0 || len(q.joins) != 0 || len(q.from.fieldTypes) != 0 {
		return false
	}
	if q.from.kind == "CACHE" {
		if _, ok := resolver.(SQLStreamSourceResolver); !ok {
			return false
		}
	} else if q.from.kind != "VALUES" {
		return false
	}
	if q.where.kind != "" && (!sqlExprReferencesOnlyAlias(q.where, q.from.alias) || q.where.window != nil || sqlExprHasAggregate(q.where) || sqlExprHasCustomFunction(q.where, nil)) {
		return false
	}
	_, _, ok := sqlSpilledGroupAggregateProjections(q)
	return ok
}

// executeSQLStreamedSpilledGroupAggregate streams a single CACHE or VALUES
// source directly into the bounded external GROUP BY operator. The narrow
// eligibility proof intentionally keeps the established materialized executor
// for joins, typed fields, CTEs, custom functions, and other shapes whose
// per-row semantics are not yet equivalent here.
func executeSQLStreamedSpilledGroupAggregate(q *sqlQuery, resolver SQLSourceResolver, control *sqlExecutionControl, metrics *sqlExecutionMetrics, visit func([]string, SQLRow) error) (SQLQueryResult, bool, error) {
	if !sqlCanStreamSpilledGroupAggregate(q, resolver, control) {
		return SQLQueryResult{}, false, nil
	}
	streamStarted := time.Now()
	inputRows, filteredRows := 0, 0
	streamElapsed := int64(0)
	filterElapsed := int64(0)
	if metrics != nil {
		metrics.steps = append(metrics.steps, SQLExplainStep{
			Node:             "STREAM SCAN",
			Detail:           sqlExplainSource(*q.from),
			ActualInputRows:  sqlExplainIntPointer(0),
			ActualOutputRows: &inputRows,
			ElapsedNanos:     &streamElapsed,
		})
		if q.where.kind != "" {
			metrics.steps = append(metrics.steps, SQLExplainStep{
				Node:             "FILTER",
				Detail:           sqlExplainExpression(q.where),
				ActualInputRows:  &inputRows,
				ActualOutputRows: &filteredRows,
				ElapsedNanos:     &filterElapsed,
			})
		}
	}
	filterStarted := time.Now()
	result, handled, err := executeSQLSpilledGroupAggregateRows(q, func(visit func(sqlExecRow) error) error {
		return streamSQLSourceRows(control.ctx, *q.from, resolver, func(sourceRow SQLRow) error {
			if err := control.check(); err != nil {
				return err
			}
			inputRows++
			if inputRows > control.maxRows {
				return fmt.Errorf("SQL source %q exceeds the %d row limit", q.from.alias, control.maxRows)
			}
			row := sqlExecRow{sources: map[string]SQLRow{q.from.alias: sourceRow}, order: []string{q.from.alias}, ordinals: map[string]int{q.from.alias: inputRows - 1}}
			if q.where.kind != "" {
				value := evalSQLExpr(q.where, []sqlExecRow{row}, row)
				if evalErr := sqlExpressionError(value); evalErr != nil {
					return evalErr
				}
				if !sqlTruthy(value) {
					return nil
				}
			}
			filteredRows++
			return visit(row)
		})
	}, control, metrics, visit)
	filterElapsed = time.Since(filterStarted).Nanoseconds()
	streamElapsed = time.Since(streamStarted).Nanoseconds()
	return result, handled, err
}

func executeSQLSpilledGroupAggregate(q *sqlQuery, rows []sqlExecRow, control *sqlExecutionControl, metrics *sqlExecutionMetrics) (SQLQueryResult, bool, error) {
	return executeSQLSpilledGroupAggregateRows(q, func(visit func(sqlExecRow) error) error {
		for _, row := range rows {
			if err := visit(row); err != nil {
				return err
			}
		}
		return nil
	}, control, metrics, nil)
}

func executeSQLSpilledGroupAggregateRows(q *sqlQuery, stream func(func(sqlExecRow) error) error, control *sqlExecutionControl, metrics *sqlExecutionMetrics, visit func([]string, SQLRow) error) (SQLQueryResult, bool, error) {
	projections, order, ok := sqlSpilledGroupAggregateProjections(q)
	if !ok {
		return SQLQueryResult{}, false, nil
	}
	available := int64(control.options.MaxSpillBytes)
	paths := map[string]struct{}{}
	defer func() {
		for path := range paths {
			_ = os.Remove(path)
		}
	}()
	buffer := []sqlSpillGroupRecord{}
	bufferBytes := 0
	runs := []sqlSpillGroupRun{}
	flush := func() error {
		if len(buffer) == 0 {
			return nil
		}
		sort.SliceStable(buffer, func(left, right int) bool { return sqlSpillGroupRecordBefore(buffer[left], buffer[right], order) })
		run, err := sqlWriteSpillGroupRun(control.options.SpillDirectory, buffer, &available, control)
		if err != nil {
			return err
		}
		paths[run.path] = struct{}{}
		runs = append(runs, run)
		buffer = []sqlSpillGroupRecord{}
		bufferBytes = 0
		return nil
	}
	started := time.Now()
	inputRows := 0
	err := stream(func(row sqlExecRow) error {
		if err := control.check(); err != nil {
			return err
		}
		value := evalSQLExpr(q.groupBy[0], []sqlExecRow{row}, row)
		if err := sqlExpressionError(value); err != nil {
			return err
		}
		key := fmt.Sprintf("%#v", value)
		record := sqlSpillGroupRecord{Key: key, Value: value, Ordinal: inputRows, Aggregates: make([]sqlSpillGroupAggregate, len(projections))}
		for index, projection := range projections {
			if projection.aggregate != nil {
				record.Aggregates[index] = sqlSpillGroupAggregateFromOrdered(*projection.aggregate)
			}
		}
		for index, projection := range projections {
			if projection.aggregate == nil {
				continue
			}
			if err := sqlAddSpillGroupAggregate(&record.Aggregates[index], projection.aggregate, row); err != nil {
				return err
			}
		}
		inputRows++
		buffer = append(buffer, record)
		bufferBytes += sqlSpillGroupRecordBytes(record)
		if bufferBytes > control.options.MaxGroupBytes {
			if err := flush(); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return SQLQueryResult{}, true, err
	}
	if err := flush(); err != nil {
		return SQLQueryResult{}, true, err
	}
	for len(runs) > maxSQLSpillMergeFanIn {
		next := make([]sqlSpillGroupRun, 0, (len(runs)+maxSQLSpillMergeFanIn-1)/maxSQLSpillMergeFanIn)
		for start := 0; start < len(runs); start += maxSQLSpillMergeFanIn {
			end := start + maxSQLSpillMergeFanIn
			if end > len(runs) {
				end = len(runs)
			}
			merged, err := sqlMergeSpillGroupRunsToRun(runs[start:end], order, control.options.SpillDirectory, &available, control)
			if err != nil {
				return SQLQueryResult{}, true, err
			}
			paths[merged.path] = struct{}{}
			for _, run := range runs[start:end] {
				if err := os.Remove(run.path); err != nil && !errors.Is(err, os.ErrNotExist) {
					return SQLQueryResult{}, true, fmt.Errorf("remove SQL group spill file: %w", err)
				}
				delete(paths, run.path)
				available += run.bytes
			}
			next = append(next, merged)
		}
		runs = next
	}
	readers, err := sqlOpenSpillGroupReaders(runs)
	if err != nil {
		return SQLQueryResult{}, true, err
	}
	defer closeSQLSpillGroupReaders(readers)
	result := SQLQueryResult{Columns: sqlColumns(q.selects), Rows: []SQLRow{}}
	groupCount := 0
	position, emitted, resultBytes := 0, 0, 0
	for {
		record, ok, err := sqlNextSpillGroup(readers, order, control)
		if err != nil {
			return SQLQueryResult{}, true, err
		}
		if !ok {
			break
		}
		groupCount++
		if position >= q.offset && (q.limit < 0 || emitted < q.limit) {
			row := SQLRow{}
			for index, projection := range projections {
				if projection.group {
					row[projection.column] = record.Value
				} else {
					row[projection.column] = record.Aggregates[index].value()
				}
			}
			if control.options.MaxResultBytes > 0 {
				resultBytes += sqlRowBytes(row)
				if resultBytes > control.options.MaxResultBytes {
					return SQLQueryResult{}, true, fmt.Errorf("SQL result byte budget exceeded: maximum %d bytes", control.options.MaxResultBytes)
				}
			}
			if visit != nil {
				if err := visit(result.Columns, row); err != nil {
					return SQLQueryResult{}, true, err
				}
			} else {
				result.Rows = append(result.Rows, row)
			}
			emitted++
		}
		position++
	}
	spillBytes := int64(control.options.MaxSpillBytes) - available
	metrics.record("EXTERNAL GROUP AGGREGATE", fmt.Sprintf("%s spill_bytes=%d runs=%d", sqlExplainExpressions(q.groupBy), spillBytes, len(runs)), inputRows, groupCount, started)
	metrics.record("PROJECT", sqlExplainSelects(q.selects), groupCount, emitted, started)
	if q.limit >= 0 || q.offset > 0 {
		metrics.record("LIMIT", fmt.Sprintf("limit=%d offset=%d", q.limit, q.offset), groupCount, emitted, started)
	}
	return result, true, nil
}

func sqlQueryHasAggregate(q *sqlQuery) bool {
	for _, item := range q.selects {
		if sqlExprHasAggregate(item.expr) {
			return true
		}
	}
	return sqlExprHasAggregate(q.having)
}

func sqlQueryHasWindow(q *sqlQuery) bool {
	if q == nil {
		return false
	}
	for _, item := range q.selects {
		if sqlExprHasWindow(item.expr) {
			return true
		}
	}
	if sqlExprHasWindow(q.having) || sqlExprHasWindow(q.where) {
		return true
	}
	for _, item := range q.groupBy {
		if sqlExprHasWindow(item) {
			return true
		}
	}
	for _, item := range q.orderBy {
		if sqlExprHasWindow(item.expr) {
			return true
		}
	}
	return false
}

func sqlExprHasWindow(expr sqlExpr) bool {
	if expr.window != nil {
		return true
	}
	for _, arg := range expr.args {
		if sqlExprHasWindow(arg) {
			return true
		}
	}
	for _, branch := range expr.cases {
		if sqlExprHasWindow(branch.when) || sqlExprHasWindow(branch.then) {
			return true
		}
	}
	return expr.left != nil && sqlExprHasWindow(*expr.left) || expr.right != nil && sqlExprHasWindow(*expr.right)
}

func sqlExprHasAggregate(expr sqlExpr) bool {
	if expr.window != nil {
		return false
	}
	if expr.kind == "func" {
		switch expr.name {
		case "COUNT", "SUM", "AVG", "MIN", "MAX":
			return true
		}
		for _, arg := range expr.args {
			if sqlExprHasAggregate(arg) {
				return true
			}
		}
	}
	if expr.kind == "cast" {
		for _, arg := range expr.args {
			if sqlExprHasAggregate(arg) {
				return true
			}
		}
	}
	if expr.kind == "case" {
		for _, branch := range expr.cases {
			if sqlExprHasAggregate(branch.when) || sqlExprHasAggregate(branch.then) {
				return true
			}
		}
	}
	if expr.left != nil && sqlExprHasAggregate(*expr.left) {
		return true
	}
	return expr.right != nil && sqlExprHasAggregate(*expr.right)
}
func sqlColumns(items []sqlSelectItem) []string {
	out := make([]string, len(items))
	for i, item := range items {
		if item.alias != "" {
			out[i] = item.alias
		} else if item.expr.kind == "field" {
			out[i] = item.expr.name
		} else if item.expr.kind == "func" {
			out[i] = strings.ToLower(item.expr.name)
		} else {
			out[i] = "column" + strconv.Itoa(i+1)
		}
	}
	return out
}
func evalSQLExpr(expr sqlExpr, group []sqlExecRow, row sqlExecRow) interface{} {
	switch expr.kind {
	case "literal":
		return expr.value
	case "field":
		return sqlField(row, expr.qualifier, expr.name)
	case "cast":
		if len(expr.args) != 1 {
			token, _ := expr.value.(sqlToken)
			return sqlEvalError{err: fmt.Errorf("CAST expects exactly one expression"), token: token}
		}
		value := evalSQLExpr(expr.args[0], group, row)
		if err := sqlExpressionError(value); err != nil {
			return sqlEvaluationFailure(err)
		}
		converted, err := sqlCastValue(value, expr.name)
		if err != nil {
			token, _ := expr.value.(sqlToken)
			return sqlEvalError{err: err, token: token}
		}
		return converted
	case "case":
		var operand interface{}
		if expr.left != nil {
			operand = evalSQLExpr(*expr.left, group, row)
			if err := sqlExpressionError(operand); err != nil {
				return sqlEvaluationFailure(err)
			}
		}
		for _, branch := range expr.cases {
			condition := evalSQLExpr(branch.when, group, row)
			if err := sqlExpressionError(condition); err != nil {
				return sqlEvaluationFailure(err)
			}
			if expr.left != nil {
				condition = sqlBinaryValue("=", operand, condition)
			}
			if sqlTruthy(condition) {
				value := evalSQLExpr(branch.then, group, row)
				if err := sqlExpressionError(value); err != nil {
					return sqlEvaluationFailure(err)
				}
				return value
			}
		}
		if expr.right != nil {
			value := evalSQLExpr(*expr.right, group, row)
			if err := sqlExpressionError(value); err != nil {
				return sqlEvaluationFailure(err)
			}
			return value
		}
		return nil
	case "func":
		switch expr.name {
		case "CONTAINS":
			if len(expr.args) != 2 {
				return sqlEvalError{err: fmt.Errorf("CONTAINS expects exactly two arguments"), token: expr.token}
			}
			value := evalSQLExpr(expr.args[0], group, row)
			if err := sqlExpressionError(value); err != nil {
				return sqlEvaluationFailure(err)
			}
			query := evalSQLExpr(expr.args[1], group, row)
			if err := sqlExpressionError(query); err != nil {
				return sqlEvaluationFailure(err)
			}
			if value == nil || query == nil {
				return nil
			}
			text, textOK := value.(string)
			search, searchOK := query.(string)
			if !textOK || !searchOK {
				return sqlEvalError{err: fmt.Errorf("CONTAINS expects TEXT arguments"), token: expr.token}
			}
			return textContains(text, search)
		case "COUNT":
			if len(expr.args) == 0 || expr.args[0].kind == "star" {
				return int64(len(group))
			}
			var n int64
			for _, r := range group {
				value := evalSQLExpr(expr.args[0], []sqlExecRow{r}, r)
				if err := sqlExpressionError(value); err != nil {
					return sqlEvaluationFailure(err)
				}
				if value != nil {
					n++
				}
			}
			return n
		case "SUM", "AVG", "MIN", "MAX":
			var values []float64
			for _, r := range group {
				value := evalSQLExpr(expr.args[0], []sqlExecRow{r}, r)
				if err := sqlExpressionError(value); err != nil {
					return sqlEvaluationFailure(err)
				}
				if n, ok := sqlNumber(value); ok {
					values = append(values, n)
				}
			}
			if len(values) == 0 {
				return nil
			}
			result := values[0]
			for _, v := range values[1:] {
				if expr.name == "SUM" || expr.name == "AVG" {
					result += v
				} else if expr.name == "MIN" && v < result {
					result = v
				} else if expr.name == "MAX" && v > result {
					result = v
				}
			}
			if expr.name == "AVG" {
				result /= float64(len(values))
			}
			return result
		}
	case "unary":
		value := evalSQLExpr(*expr.left, group, row)
		if err := sqlExpressionError(value); err != nil {
			return sqlEvaluationFailure(err)
		}
		switch expr.op {
		case "!":
			return !sqlTruthy(value)
		case "-":
			switch number := value.(type) {
			case int64:
				return -number
			case int:
				return -number
			case float64:
				return -number
			case float32:
				return -number
			}
			return nil
		}
	case "in":
		left := evalSQLExpr(*expr.left, group, row)
		if err := sqlExpressionError(left); err != nil {
			return sqlEvaluationFailure(err)
		}
		values := make([]interface{}, len(expr.args))
		for index, argument := range expr.args {
			values[index] = evalSQLExpr(argument, group, row)
			if err := sqlExpressionError(values[index]); err != nil {
				return sqlEvaluationFailure(err)
			}
		}
		return sqlInValue(expr.op, left, values)
	case "between":
		left := evalSQLExpr(*expr.left, group, row)
		if err := sqlExpressionError(left); err != nil {
			return sqlEvaluationFailure(err)
		}
		lower := evalSQLExpr(expr.args[0], group, row)
		upper := evalSQLExpr(expr.args[1], group, row)
		if err := sqlExpressionError(lower); err != nil {
			return sqlEvaluationFailure(err)
		}
		if err := sqlExpressionError(upper); err != nil {
			return sqlEvaluationFailure(err)
		}
		return sqlBetweenValue(expr.op, left, lower, upper)
	case "binary":
		left := evalSQLExpr(*expr.left, group, row)
		if err := sqlExpressionError(left); err != nil {
			return sqlEvaluationFailure(err)
		}
		if expr.op == "IS NULL" {
			return left == nil
		}
		if expr.op == "IS NOT NULL" {
			return left != nil
		}
		right := evalSQLExpr(*expr.right, group, row)
		if err := sqlExpressionError(right); err != nil {
			return sqlEvaluationFailure(err)
		}
		return sqlBinaryValue(expr.op, left, right)
	}
	return nil
}

func sqlInValue(op string, left interface{}, values []interface{}) interface{} {
	if left == nil {
		return nil
	}
	unknown := false
	for _, value := range values {
		comparison := sqlBinaryValue("=", left, value)
		if comparison == true {
			return op != "NOT IN"
		}
		if comparison == nil {
			unknown = true
		}
	}
	if unknown {
		return nil
	}
	return op == "NOT IN"
}

func sqlBetweenValue(op string, left, lower, upper interface{}) interface{} {
	value := sqlBinaryValue("AND", sqlBinaryValue(">=", left, lower), sqlBinaryValue("<=", left, upper))
	if value == nil || op != "NOT BETWEEN" {
		return value
	}
	return !sqlTruthy(value)
}

func sqlCastValue(value interface{}, target string) (interface{}, error) {
	if value == nil {
		return nil, nil
	}
	fail := func() (interface{}, error) {
		return nil, fmt.Errorf("CAST cannot convert %s value %q to %s", sqlLiteralTypeName(value), fmt.Sprint(value), target)
	}
	switch target {
	case "TEXT":
		if timestamp, ok := value.(time.Time); ok {
			return timestamp.Format(time.RFC3339Nano), nil
		}
		return fmt.Sprint(value), nil
	case "NUMBER":
		if number, ok := sqlNumber(value); ok {
			return number, nil
		}
		text, ok := value.(string)
		if !ok {
			return fail()
		}
		number, err := strconv.ParseFloat(text, 64)
		if err != nil || math.IsNaN(number) || math.IsInf(number, 0) {
			return fail()
		}
		return number, nil
	case "DECIMAL":
		if decimal, ok := value.(sqlDecimal); ok {
			return decimal, nil
		}
		if text, ok := value.(string); ok {
			if decimal, ok := parseSQLDecimal(text); ok {
				return decimal, nil
			}
			return fail()
		}
		if number, ok := sqlNumber(value); ok {
			if decimal, ok := parseSQLDecimal(strconv.FormatFloat(number, 'f', -1, 64)); ok {
				return decimal, nil
			}
		}
		return fail()
	case "BOOLEAN":
		switch typed := value.(type) {
		case bool:
			return typed, nil
		case string:
			switch strings.ToLower(typed) {
			case "true":
				return true, nil
			case "false":
				return false, nil
			}
		case int, int64, float32, float64:
			number, _ := sqlNumber(typed)
			if number == 0 {
				return false, nil
			}
			if number == 1 {
				return true, nil
			}
		}
		return fail()
	case "DATE":
		switch typed := value.(type) {
		case sqlDate:
			return typed, nil
		case time.Time:
			return sqlDate(typed.Format("2006-01-02")), nil
		case string:
			parsed, err := time.Parse("2006-01-02", typed)
			if err == nil && parsed.Format("2006-01-02") == typed {
				return sqlDate(typed), nil
			}
		}
		return fail()
	case "TIMESTAMP":
		switch typed := value.(type) {
		case time.Time:
			return typed, nil
		case sqlDate:
			parsed, err := time.Parse("2006-01-02", string(typed))
			if err == nil {
				return parsed.UTC(), nil
			}
		case string:
			parsed, err := time.Parse(time.RFC3339Nano, typed)
			if err == nil {
				return parsed, nil
			}
		}
		return fail()
	}
	return nil, fmt.Errorf("unsupported CAST target %q", target)
}
func sqlField(row sqlExecRow, qualifier, name string) interface{} {
	if qualifier != "" {
		return row.sources[qualifier][name]
	}
	for _, source := range row.order {
		if value, ok := row.sources[source][name]; ok {
			return value
		}
	}
	return nil
}
func evalOutputOrder(expr sqlExpr, out SQLRow, group []sqlExecRow) interface{} {
	if expr.kind == "field" && expr.qualifier == "" {
		if value, ok := out[expr.name]; ok {
			return value
		}
	}
	row := sqlExecRow{}
	if len(group) > 0 {
		row = group[0]
	}
	return evalSQLExpr(expr, group, row)
}
func sqlTruthy(value interface{}) bool {
	b, ok := value.(bool)
	if ok {
		return b
	}
	return value != nil && value != false
}
func sqlNumber(value interface{}) (float64, bool) {
	switch v := value.(type) {
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	case float64:
		return v, true
	case float32:
		return float64(v), true
	}
	return 0, false
}
func sqlCompare(left, right interface{}) int {
	if left == nil && right == nil {
		return 0
	}
	if left == nil {
		return -1
	}
	if right == nil {
		return 1
	}
	if a, ok := left.(time.Time); ok {
		if b, ok := right.(time.Time); ok {
			if a.Before(b) {
				return -1
			}
			if a.After(b) {
				return 1
			}
			return 0
		}
	}
	if a, ok := left.(sqlDecimal); ok {
		if b, ok := right.(sqlDecimal); ok {
			leftValue, leftOK := new(big.Rat).SetString(string(a))
			rightValue, rightOK := new(big.Rat).SetString(string(b))
			if leftOK && rightOK {
				return leftValue.Cmp(rightValue)
			}
		}
	}
	if a, ok := sqlNumber(left); ok {
		if b, ok := sqlNumber(right); ok {
			if a < b {
				return -1
			}
			if a > b {
				return 1
			}
			return 0
		}
	}
	a, b := fmt.Sprint(left), fmt.Sprint(right)
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}
func sqlLike(value, pattern string) bool {
	parts := strings.Split(pattern, "%")
	if len(parts) == 1 {
		return value == pattern
	}
	if !strings.HasPrefix(pattern, "%") && !strings.HasPrefix(value, parts[0]) {
		return false
	}
	if !strings.HasSuffix(pattern, "%") && !strings.HasSuffix(value, parts[len(parts)-1]) {
		return false
	}
	position := 0
	for _, part := range parts {
		if part == "" {
			continue
		}
		index := strings.Index(value[position:], part)
		if index < 0 {
			return false
		}
		position += index + len(part)
	}
	return true
}

func sqlExprHasCustomFunction(expr sqlExpr, functions SQLFunctionResolver) bool {
	_ = functions
	if expr.kind == "func" && !sqlBuiltinFunction(expr.name) {
		return true
	}
	if expr.left != nil && sqlExprHasCustomFunction(*expr.left, functions) {
		return true
	}
	if expr.right != nil && sqlExprHasCustomFunction(*expr.right, functions) {
		return true
	}
	for _, argument := range expr.args {
		if sqlExprHasCustomFunction(argument, functions) {
			return true
		}
	}
	for _, branch := range expr.cases {
		if sqlExprHasCustomFunction(branch.when, functions) || sqlExprHasCustomFunction(branch.then, functions) {
			return true
		}
	}
	return false
}
func sqlBuiltinFunction(name string) bool {
	switch strings.ToUpper(name) {
	case "CONTAINS", "COUNT", "SUM", "AVG", "MIN", "MAX":
		return true
	}
	return false
}
func evalSQLExprBatch(expr sqlExpr, rows []sqlExecRow, functions SQLFunctionResolver) ([]interface{}, error) {
	if expr.kind == "func" && !sqlBuiltinFunction(expr.name) {
		if functions == nil {
			return nil, fmt.Errorf("unknown SQL function %q", expr.name)
		}
		calls := make([]SQLFunctionCall, len(rows))
		for index, row := range rows {
			call := SQLFunctionCall{Arguments: make([]interface{}, len(expr.args))}
			for argIndex, arg := range expr.args {
				value, err := evalSQLExprBatch(arg, []sqlExecRow{row}, functions)
				if err != nil {
					return nil, err
				}
				call.Arguments[argIndex] = value[0]
			}
			calls[index] = call
		}
		values, err := functions.EvaluateSQLFunction(expr.name, calls)
		if err != nil {
			return nil, err
		}
		if len(values) != len(rows) {
			return nil, fmt.Errorf("SQL function %q returned %d values for %d rows", expr.name, len(values), len(rows))
		}
		return values, nil
	}
	if expr.kind == "case" {
		out := make([]interface{}, len(rows))
		resolved := make([]bool, len(rows))
		var operand []interface{}
		if expr.left != nil {
			values, err := evalSQLExprBatch(*expr.left, rows, functions)
			if err != nil {
				return nil, err
			}
			operand = values
			for index, value := range values {
				if err := sqlExpressionError(value); err != nil {
					out[index] = sqlEvaluationFailure(err)
					resolved[index] = true
				}
			}
		}
		for _, branch := range expr.cases {
			conditions, err := evalSQLExprBatch(branch.when, rows, functions)
			if err != nil {
				return nil, err
			}
			indexes := make([]int, 0, len(rows))
			for index, condition := range conditions {
				if resolved[index] {
					continue
				}
				if err := sqlExpressionError(condition); err != nil {
					out[index] = sqlEvaluationFailure(err)
					resolved[index] = true
					continue
				}
				if operand != nil {
					condition = sqlBinaryValue("=", operand[index], condition)
				}
				if sqlTruthy(condition) {
					indexes = append(indexes, index)
				}
			}
			if len(indexes) == 0 {
				continue
			}
			selectedRows := make([]sqlExecRow, len(indexes))
			for selectedIndex, index := range indexes {
				selectedRows[selectedIndex] = rows[index]
			}
			values, err := evalSQLExprBatch(branch.then, selectedRows, functions)
			if err != nil {
				return nil, err
			}
			for selectedIndex, index := range indexes {
				out[index] = values[selectedIndex]
				resolved[index] = true
			}
		}
		indexes := make([]int, 0, len(rows))
		for index := range rows {
			if !resolved[index] {
				indexes = append(indexes, index)
			}
		}
		if expr.right == nil || len(indexes) == 0 {
			return out, nil
		}
		selectedRows := make([]sqlExecRow, len(indexes))
		for selectedIndex, index := range indexes {
			selectedRows[selectedIndex] = rows[index]
		}
		values, err := evalSQLExprBatch(*expr.right, selectedRows, functions)
		if err != nil {
			return nil, err
		}
		for selectedIndex, index := range indexes {
			out[index] = values[selectedIndex]
		}
		return out, nil
	}
	if expr.kind == "binary" {
		left, err := evalSQLExprBatch(*expr.left, rows, functions)
		if err != nil {
			return nil, err
		}
		if expr.op == "IS NULL" || expr.op == "IS NOT NULL" {
			out := make([]interface{}, len(rows))
			for i := range rows {
				if err := sqlExpressionError(left[i]); err != nil {
					out[i] = sqlEvaluationFailure(err)
					continue
				}
				if expr.op == "IS NULL" {
					out[i] = left[i] == nil
				} else {
					out[i] = left[i] != nil
				}
			}
			return out, nil
		}
		right, err := evalSQLExprBatch(*expr.right, rows, functions)
		if err != nil {
			return nil, err
		}
		out := make([]interface{}, len(rows))
		for i := range rows {
			if err := sqlExpressionError(left[i]); err != nil {
				out[i] = sqlEvaluationFailure(err)
				continue
			}
			if err := sqlExpressionError(right[i]); err != nil {
				out[i] = sqlEvaluationFailure(err)
				continue
			}
			out[i] = sqlBinaryValue(expr.op, left[i], right[i])
		}
		return out, nil
	}
	out := make([]interface{}, len(rows))
	for index, row := range rows {
		out[index] = evalSQLExpr(expr, []sqlExecRow{row}, row)
	}
	return out, nil
}
func sqlBinaryValue(op string, left, right interface{}) interface{} {
	switch op {
	case "AND":
		if left == nil || right == nil {
			if (left != nil && !sqlTruthy(left)) || (right != nil && !sqlTruthy(right)) {
				return false
			}
			return nil
		}
		return sqlTruthy(left) && sqlTruthy(right)
	case "OR":
		if left == nil || right == nil {
			if (left != nil && sqlTruthy(left)) || (right != nil && sqlTruthy(right)) {
				return true
			}
			return nil
		}
		return sqlTruthy(left) || sqlTruthy(right)
	case "LIKE", "=", "!=", "<>", "<", "<=", ">", ">=":
		if left == nil || right == nil {
			return nil
		}
	}
	switch op {
	case "LIKE":
		return sqlLike(fmt.Sprint(left), fmt.Sprint(right))
	case "=":
		return sqlCompare(left, right) == 0
	case "!=", "<>":
		return sqlCompare(left, right) != 0
	case "<":
		return sqlCompare(left, right) < 0
	case "<=":
		return sqlCompare(left, right) <= 0
	case ">":
		return sqlCompare(left, right) > 0
	case ">=":
		return sqlCompare(left, right) >= 0
	case "+", "-", "*", "/", "%":
		return sqlArithmeticValue(op, left, right)
	}
	return nil
}

func sqlArithmeticValue(op string, left, right interface{}) interface{} {
	leftInteger, leftIsInteger := sqlInteger(left)
	rightInteger, rightIsInteger := sqlInteger(right)
	if leftIsInteger && rightIsInteger {
		switch op {
		case "+":
			return leftInteger + rightInteger
		case "-":
			return leftInteger - rightInteger
		case "*":
			return leftInteger * rightInteger
		case "/":
			if rightInteger == 0 {
				return nil
			}
			return leftInteger / rightInteger
		case "%":
			if rightInteger == 0 {
				return nil
			}
			return leftInteger % rightInteger
		}
	}
	leftNumber, leftOK := sqlNumber(left)
	rightNumber, rightOK := sqlNumber(right)
	if !leftOK || !rightOK || (op == "/" && rightNumber == 0) || op == "%" {
		return nil
	}
	switch op {
	case "+":
		return leftNumber + rightNumber
	case "-":
		return leftNumber - rightNumber
	case "*":
		return leftNumber * rightNumber
	case "/":
		return leftNumber / rightNumber
	}
	return nil
}

func sqlInteger(value interface{}) (int64, bool) {
	switch number := value.(type) {
	case int:
		return int64(number), true
	case int64:
		return number, true
	}
	return 0, false
}

// ParsedQuery is an immutable parsed SQL template. It is intentionally opaque:
// callers execute it through the query entry points rather than mutating plan
// nodes directly.
type ParsedQuery = sqlQuery

// SQLDate is the canonical SQL DATE value, serialized as YYYY-MM-DD.
type SQLDate = sqlDate

// SQLDecimal preserves decimal spelling while comparisons use exact rational
// arithmetic.
type SQLDecimal = sqlDecimal

// ParseSQLQueryWithCache parses source and binds positional parameters using
// cache when non-nil.
func ParseSQLQueryWithCache(source string, parameters []interface{}, cache *SQLPreparedQueryCache) (*ParsedQuery, error) {
	return parseSQLQueryWithCache(source, parameters, cache)
}

// ParseQueryWithCache parses source and binds positional parameters using cache.
func ParseQueryWithCache(source string, parameters []interface{}, cache *PreparedQueryCache) (*ParsedQuery, error) {
	return ParseSQLQueryWithCache(source, parameters, cache)
}

// ParseSQLQueryParameters parses source and binds positional parameters.
func ParseSQLQueryParameters(source string, parameters []interface{}) (*ParsedQuery, error) {
	return parseSQLQueryParameters(source, parameters)
}

// ParseQueryParameters parses source and binds positional parameters.
func ParseQueryParameters(source string, parameters []interface{}) (*ParsedQuery, error) {
	return ParseSQLQueryParameters(source, parameters)
}

// QuerySourceNames parses source and returns each physical CACHE or EXTERNAL
// source name in first-use order. CTEs and derived queries are traversed so
// callers can apply authorization before executing the query.
func QuerySourceNames(source string) ([]string, error) {
	return QuerySourceNamesParameters(source, nil)
}

// QuerySourceNamesParameters is QuerySourceNames with positional parameters
// bound before source discovery.
func QuerySourceNamesParameters(source string, parameters []interface{}) ([]string, error) {
	query, err := parseSQLQueryParameters(source, parameters)
	if err != nil {
		return nil, err
	}
	seen := make(map[string]struct{})
	names := make([]string, 0)
	appendSource := func(candidate string) {
		if candidate == "" {
			return
		}
		if _, ok := seen[candidate]; ok {
			return
		}
		seen[candidate] = struct{}{}
		names = append(names, candidate)
	}
	var visitSource func(sqlSource)
	var visitQuery func(*sqlQuery)
	visitSource = func(candidate sqlSource) {
		switch candidate.kind {
		case "CACHE", "EXTERNAL":
			appendSource(candidate.key)
		case "SUBQUERY":
			visitQuery(candidate.query)
		}
	}
	visitQuery = func(candidate *sqlQuery) {
		if candidate == nil {
			return
		}
		for _, cte := range candidate.ctes {
			visitQuery(cte.query)
		}
		if candidate.from != nil {
			visitSource(*candidate.from)
		}
		for _, join := range candidate.joins {
			visitSource(join.source)
		}
		for _, union := range candidate.unions {
			visitQuery(union.query)
		}
	}
	visitQuery(query)
	return names, nil
}

// ValidateSQLQueryStreamable reports whether query can emit rows incrementally.
func ValidateSQLQueryStreamable(query *ParsedQuery) error {
	return validateSQLQueryStreamable(query)
}

// SQLQueryColumns returns the output columns selected by query.
func SQLQueryColumns(query *ParsedQuery) []string {
	if query == nil {
		return nil
	}
	return sqlColumns(query.selects)
}

// QueryColumns returns the output columns selected by query.
func QueryColumns(query *ParsedQuery) []string { return SQLQueryColumns(query) }

// Compare applies the SQL value ordering used by queries and indexes.
func Compare(left, right interface{}) int { return sqlCompare(left, right) }

// OrderLess applies SQL NULL and direction ordering without exposing internal
// query-plan order nodes.
func OrderLess(desc, nullsFirst, nullsLast bool, left, right interface{}) (bool, bool) {
	return sqlOrderLess(sqlOrder{desc: desc, nullsFirst: nullsFirst, nullsLast: nullsLast}, left, right)
}

// CloneRows returns independent row maps suitable for resolver boundaries.
func CloneRows(rows []SQLRow) []SQLRow { return cloneSQLRows(rows) }

// IndexedEqualityProbeCost returns the planner cost for one indexed equality
// candidate with estimatedRows matching values.
func IndexedEqualityProbeCost(estimatedRows int) int {
	return sqlIndexedEqualityProbeCost(estimatedRows)
}

// Number converts a SQL numeric value to float64.
func Number(value interface{}) (float64, bool) { return sqlNumber(value) }

// Integer converts a SQL integer value to int64.
func Integer(value interface{}) (int64, bool) { return sqlInteger(value) }

// BinaryValue evaluates one SQL binary operation using the query value rules.
func BinaryValue(op string, left, right interface{}) interface{} {
	return sqlBinaryValue(op, left, right)
}
