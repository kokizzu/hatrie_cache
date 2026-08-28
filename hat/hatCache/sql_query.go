package hatCache

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"

	"hatrie_cache/hat/hatDataStructure"
	"hatrie_cache/hat/hatSql"

	jsonfast "github.com/goccy/go-json"
)

type SQLQueryOptions = hatSql.SQLQueryOptions
type SQLAdaptivePlanner = hatSql.AdaptivePlanner
type SQLAdaptivePlannerOptions = hatSql.AdaptivePlannerOptions
type SQLPreparedQueryCache = hatSql.SQLPreparedQueryCache
type SQLPreparedQueryCacheStats = hatSql.SQLPreparedQueryCacheStats
type SQLPreparedQuery = hatSql.SQLPreparedQuery
type SQLParameterType = hatSql.ParameterType
type SQLParameterSpec = hatSql.ParameterSpec
type SQLSpillFaults = hatSql.SQLSpillFaults
type SQLCollation = hatSql.SQLCollation
type SQLErrorCode = hatSql.ErrorCode
type SQLCodedError = hatSql.CodedError
type SQLDate = hatSql.SQLDate
type SQLDecimal = hatSql.SQLDecimal
type SQLUUID = hatSql.SQLUUID
type SQLDuration = hatSql.SQLDuration
type SQLQueryRequest = hatSql.QueryRequest
type SQLRow = hatSql.Row
type SQLQueryResult = hatSql.QueryResult
type SQLExplainStep = hatSql.ExplainStep
type SQLQueryStats = hatSql.QueryStats
type SQLQueryObserver = hatSql.QueryObserver
type SQLQueryObserverFunc = hatSql.QueryObserverFunc
type SQLQueryEvent = hatSql.QueryEvent
type SQLQueryOperator = hatSql.QueryOperator
type SQLSourceResolver = hatSql.SourceResolver
type SQLStreamSourceResolver = hatSql.StreamSourceResolver
type SQLSnapshotLocker = hatSql.SnapshotLocker
type SQLIndexedSourceResolver = hatSql.IndexedSourceResolver
type SQLRangeIndexedSourceResolver = hatSql.RangeIndexedSourceResolver
type SQLTextIndexedSourceResolver = hatSql.TextIndexedSourceResolver
type SQLOrderedSourceResolver = hatSql.OrderedSourceResolver
type SQLOrderedStreamSourceResolver = hatSql.OrderedStreamSourceResolver
type SQLCompositeIndexedSourceResolver = hatSql.CompositeIndexedSourceResolver
type SQLJSONIndexStatsResolver = hatSql.JSONIndexStatsResolver
type SQLIndexValueEstimator = hatSql.IndexValueEstimator
type SQLJSONIndexFrequencyBucket = hatSql.JSONIndexFrequencyBucket
type SQLJSONIndexStats = hatSql.JSONIndexStats
type SQLSourceResolverFunc = hatSql.SourceResolverFunc
type SQLConn = hatSql.Conn
type SQLTimeSeriesOptions = hatSql.TimeSeriesOptions
type SQLTimeSeriesResult = hatSql.TimeSeriesResult
type SQLVectorMatch = hatSql.VectorMatch

// SQLResultCache retains the root API while hatSql owns the portable cache
// core. Its HatTrie adapter supplies the mutation epoch for invalidation.
type SQLResultCache struct {
	cache *hatSql.ResultCache
}

// NewSQLResultCache creates a bounded cache for default-option read queries.
func NewSQLResultCache(capacity int) *SQLResultCache {
	return &SQLResultCache{cache: hatSql.NewResultCache(capacity)}
}

// Execute runs a default-option read query and returns a cached result only if
// the trie has not been mutated since that result was computed.
func (cache *SQLResultCache) Execute(ctx context.Context, trie *HatTrie, source string, parameters []interface{}) (SQLQueryResult, error) {
	if trie == nil {
		return SQLQueryResult{}, ErrNilHatTrie
	}
	key, err := sqlResultCacheKey(source, parameters)
	if err != nil {
		return SQLQueryResult{}, err
	}
	var core *hatSql.ResultCache
	if cache != nil {
		core = cache.cache
	}
	return core.Execute(ctx, key, func() uint64 {
		return atomic.LoadUint64(&trie.mutationEpoch)
	}, func(ctx context.Context) (hatSql.QueryResult, error) {
		return ExecuteSQLQueryParameters(ctx, source, trie, parameters, SQLQueryOptions{})
	})
}

func sqlResultCacheKey(source string, parameters []interface{}) (string, error) {
	encoded, err := jsonfast.Marshal(parameters)
	if err != nil {
		return "", errors.New("hatriecache: SQL result cache parameters are not serializable")
	}
	return source + "\x00" + string(encoded), nil
}

// NewSQLConn creates a connection to a hatrie-cache monitoring endpoint.
func NewSQLConn(baseURL string, token string) *SQLConn {
	return hatSql.NewConn(baseURL, token)
}

// QueryRows invokes visit once for each streamed SQL row.
func QueryRows[T any](ctx context.Context, conn *SQLConn, query string, visit func(T) error) (int, error) {
	return hatSql.QueryRows(ctx, conn, query, visit)
}

// QuerySQLTimeSeries evaluates SQL once, then returns gap-aware buckets and
// optional rolling means.
func QuerySQLTimeSeries(ctx context.Context, source string, resolver SQLSourceResolver, parameters []interface{}, queryOptions SQLQueryOptions, options SQLTimeSeriesOptions) (SQLTimeSeriesResult, error) {
	return hatSql.QueryTimeSeries(ctx, source, resolver, parameters, queryOptions, options)
}

// SearchSQLVectorHybrid evaluates the SQL filter first, then ranks only the
// vectors admitted by the filtered result.
func SearchSQLVectorHybrid(ctx context.Context, source string, resolver SQLSourceResolver, parameters []interface{}, options SQLQueryOptions, index *hatDataStructure.VectorIndex, query []float32, limit int, idField string) ([]SQLVectorMatch, error) {
	return hatSql.SearchVectorHybrid(ctx, source, resolver, parameters, options, index, query, limit, idField)
}

// CanonicalSQLSnapshot encodes a query result as stable JSON for regression
// fixtures without volatile execution statistics.
func CanonicalSQLSnapshot(result SQLQueryResult) ([]byte, error) {
	return hatSql.CanonicalSnapshot(result)
}

// SnapshotSQLQuery executes a query and returns its stable regression fixture.
func SnapshotSQLQuery(ctx context.Context, query string, resolver SQLSourceResolver, parameters []interface{}, options SQLQueryOptions) ([]byte, error) {
	return hatSql.SnapshotQuery(ctx, query, resolver, parameters, options)
}

// SQLQuerySourceNames returns the physical CACHE and EXTERNAL source names
// referenced by query after binding positional parameters.
func SQLQuerySourceNames(query string, parameters []interface{}) ([]string, error) {
	return hatSql.QuerySourceNamesParameters(query, parameters)
}

// SQLPartitionPruningPlan reports whether a SQL source can be routed to one
// local partition. CACHE(key) is prunable because its namespace key is known;
// KEYS intentionally scans every partition.
type SQLPartitionPruningPlan struct {
	Source     string `json:"source"`
	Key        string `json:"key,omitempty"`
	Pruned     bool   `json:"pruned"`
	Partition  int    `json:"partition,omitempty"`
	Partitions int    `json:"partitions"`
}

// SQLPartitionPruningPlan returns the local-partition routing decision for a
// supported SQL source. Partitioning is disabled by default, in which case
// Pruned is false and Partitions is zero.
func (ht *HatTrie) SQLPartitionPruningPlan(source string, key string) (SQLPartitionPruningPlan, error) {
	plan := SQLPartitionPruningPlan{Source: strings.ToUpper(strings.TrimSpace(source)), Key: strings.TrimSpace(key)}
	set := ht.localPartitionSet()
	if set == nil {
		return plan, nil
	}
	plan.Partitions = len(set.tries)
	if plan.Source != "CACHE" {
		return plan, nil
	}
	partition, enabled, err := ht.LocalPartitionForKey(plan.Key)
	if err != nil {
		return SQLPartitionPruningPlan{}, err
	}
	if enabled {
		plan.Pruned = true
		plan.Partition = partition
	}
	return plan, nil
}

// SQLJSONIndexHealth reports the current coverage of one lazily refreshed
// JSON field index. Refreshed is true when this inspection rebuilt the index
// from a changed CACHE value.
type SQLJSONIndexHealth struct {
	Key          string
	Field        string
	Rows         int
	IndexedRows  int
	NullRows     int
	DistinctKeys int
	SourceBytes  int
	Current      bool
	Refreshed    bool
}

// SQLJSONRangeHistogramBucket is one contiguous quantile bucket from an
// ordered JSON field index. Lower and Upper are inclusive observed values.
type SQLJSONRangeHistogramBucket struct {
	Lower interface{}
	Upper interface{}
	Rows  int
}

// SQLJSONRangeStats describes ordered-index coverage for range planning.
// Buckets are equal-depth contiguous buckets rather than value-width buckets,
// which keeps skewed distributions useful without retaining another index.
type SQLJSONRangeStats struct {
	Key      string
	Field    string
	Rows     int
	NullRows int
	Buckets  []SQLJSONRangeHistogramBucket
}

const (
	SQLErrorUnknown  = hatSql.ErrorUnknown
	SQLErrorSyntax   = hatSql.ErrorSyntax
	SQLErrorType     = hatSql.ErrorType
	SQLErrorCapacity = hatSql.ErrorCapacity
	SQLErrorConflict = hatSql.ErrorConflict
	SQLErrorCanceled = hatSql.ErrorCanceled

	SQLCollationBinary    = hatSql.SQLCollationBinary
	SQLCollationUnicodeCI = hatSql.SQLCollationUnicodeCI

	SQLParameterAny       = hatSql.ParameterAny
	SQLParameterText      = hatSql.ParameterText
	SQLParameterNumber    = hatSql.ParameterNumber
	SQLParameterInteger   = hatSql.ParameterInteger
	SQLParameterDecimal   = hatSql.ParameterDecimal
	SQLParameterBoolean   = hatSql.ParameterBoolean
	SQLParameterDate      = hatSql.ParameterDate
	SQLParameterTimestamp = hatSql.ParameterTimestamp
	SQLParameterUUID      = hatSql.ParameterUUID
	SQLParameterDuration  = hatSql.ParameterDuration
	SQLParameterBinary    = hatSql.ParameterBinary
	SQLParameterJSON      = hatSql.ParameterJSON
)

// SQLErrorCodeOf returns the stable class for an SQL API error.
func SQLErrorCodeOf(err error) SQLErrorCode { return hatSql.ErrorCodeOf(err) }

// WithSQLErrorCode preserves an error chain while attaching a stable class.
func WithSQLErrorCode(code SQLErrorCode, err error) error { return hatSql.WithErrorCode(code, err) }

func NewSQLPreparedQueryCache(capacity int) *SQLPreparedQueryCache {
	return hatSql.NewSQLPreparedQueryCache(capacity)
}

func NewSQLAdaptivePlanner(options SQLAdaptivePlannerOptions) *SQLAdaptivePlanner {
	return hatSql.NewAdaptivePlanner(options)
}

// PrepareSQLQuery parses source once and binds a typed positional-parameter
// schema to its immutable cached template.
func PrepareSQLQuery(source string, parameters []SQLParameterSpec, cache *SQLPreparedQueryCache) (*SQLPreparedQuery, error) {
	return hatSql.PrepareSQLQuery(source, parameters, cache)
}

func ExecuteSQLQuery(source string, resolver SQLSourceResolver) (SQLQueryResult, error) {
	return hatSql.ExecuteSQLQuery(source, resolver)
}

func ExecuteSQLQueryContext(ctx context.Context, source string, resolver SQLSourceResolver, options SQLQueryOptions) (SQLQueryResult, error) {
	return hatSql.ExecuteSQLQueryContext(ctx, source, resolver, options)
}

func ExecuteSQLQueryParameters(ctx context.Context, source string, resolver SQLSourceResolver, parameters []interface{}, options SQLQueryOptions) (SQLQueryResult, error) {
	return hatSql.ExecuteSQLQueryParameters(ctx, source, resolver, parameters, options)
}

func ExecuteSQLQueryRows(ctx context.Context, source string, resolver SQLSourceResolver, parameters []interface{}, options SQLQueryOptions, visit func([]string, SQLRow) error) error {
	return hatSql.ExecuteSQLQueryRows(ctx, source, resolver, parameters, options, visit)
}

func ExecuteSQLQueryPage(ctx context.Context, source string, resolver SQLSourceResolver, parameters []interface{}, options SQLQueryOptions, pageSize int, cursor string) (SQLQueryResult, error) {
	return hatSql.ExecuteSQLQueryPage(ctx, source, resolver, parameters, options, pageSize, cursor)
}

func ValidateSQLQuery(source string) error { return hatSql.ValidateSQLQuery(source) }

func parseSQLQueryWithCache(source string, parameters []interface{}, cache *SQLPreparedQueryCache) (*hatSql.ParsedQuery, error) {
	return hatSql.ParseSQLQueryWithCache(source, parameters, cache)
}

func parseSQLQueryParameters(source string, parameters []interface{}) (*hatSql.ParsedQuery, error) {
	return hatSql.ParseSQLQueryParameters(source, parameters)
}

func validateSQLQueryStreamable(query *hatSql.ParsedQuery) error {
	return hatSql.ValidateSQLQueryStreamable(query)
}

func SQLQueryColumns(query *hatSql.ParsedQuery) []string { return hatSql.SQLQueryColumns(query) }

func cloneSQLRows(rows []SQLRow) []SQLRow { return hatSql.CloneRows(rows) }

func sqlIndexedEqualityProbeCost(estimatedRows int) int {
	return hatSql.IndexedEqualityProbeCost(estimatedRows)
}

func sqlNumber(value interface{}) (float64, bool) { return hatSql.Number(value) }

func sqlInteger(value interface{}) (int64, bool) { return hatSql.Integer(value) }

type sqlDate = SQLDate
type sqlDecimal = SQLDecimal
type sqlUUID = SQLUUID
type sqlDuration = SQLDuration

func sqlBinaryValue(op string, left, right interface{}) interface{} {
	return hatSql.BinaryValue(op, left, right)
}

type sqlJSONFieldIndex struct {
	raw     string
	rows    map[string][]SQLRow
	ordered []sqlJSONFieldIndexEntry
	nulls   []SQLRow
}
type sqlJSONFieldIndexEntry struct {
	value interface{}
	row   SQLRow
}
type sqlJSONTextIndex struct {
	raw    string
	rows   []SQLRow
	tokens map[string][]int
}
type sqlJSONCompositeIndex struct {
	raw    string
	fields []string
	rows   map[string][]SQLRow
}
type sqlJSONBitmapIndex struct {
	raw      string
	rows     []SQLRow
	postings map[string]hatDataStructure.RoaringBitmap
	nulls    []SQLRow
}
type sqlJSONCoveringIndex struct {
	raw     string
	columns map[string]struct{}
	rows    map[string][]SQLRow
}
type sqlJSONIndexMaintenance struct {
	scheduled uint64
	runs      uint64
	rebuilds  uint64
}
type sqlJSONIndexRebuildRequest struct {
	key   string
	field string
}

// SQLJSONIndexMaintenanceStats reports lifecycle state without exposing source
// values or indexed keys.
type SQLJSONIndexMaintenanceStats struct {
	Key         string `json:"key"`
	Field       string `json:"field"`
	Configured  int    `json:"configured"`
	SourceBytes int    `json:"source_bytes"`
	Current     bool   `json:"current"`
	Pending     bool   `json:"pending"`
	Scheduled   uint64 `json:"scheduled"`
	Runs        uint64 `json:"runs"`
	Rebuilds    uint64 `json:"rebuilds"`
}

// SQLJSONBitmapIndexHealth reports the current in-memory size and shape of an
// opt-in low-cardinality JSON bitmap index.
type SQLJSONBitmapIndexHealth struct {
	Key          string `json:"key"`
	Field        string `json:"field"`
	Rows         int    `json:"rows"`
	IndexedRows  int    `json:"indexed_rows"`
	NullRows     int    `json:"null_rows"`
	DistinctKeys int    `json:"distinct_keys"`
	SourceBytes  int    `json:"source_bytes"`
	EncodedBytes uint64 `json:"encoded_bytes"`
	Current      bool   `json:"current"`
	Refreshed    bool   `json:"refreshed"`
}

func (ht *HatTrie) CreateSQLJSONFieldIndex(key, field string) error {
	if ht == nil || key == "" || field == "" {
		return fmt.Errorf("SQL JSON index requires a cache key and field")
	}
	ht.sqlIndexMu.Lock()
	defer ht.sqlIndexMu.Unlock()
	if ht.sqlJSONIndexes == nil {
		ht.sqlJSONIndexes = map[string]map[string]*sqlJSONFieldIndex{}
	}
	if ht.sqlJSONIndexes[key] == nil {
		ht.sqlJSONIndexes[key] = map[string]*sqlJSONFieldIndex{}
	}
	ht.sqlJSONIndexes[key][field] = &sqlJSONFieldIndex{}
	return nil
}

// CreateSQLJSONBitmapIndex configures an online equality index for a
// low-cardinality JSON field. Each distinct value owns a compact Roaring bitmap
// of source-row ordinals; range and ordered scans remain the responsibility of
// CreateSQLJSONFieldIndex.
func (ht *HatTrie) CreateSQLJSONBitmapIndex(key, field string) error {
	if ht == nil || key == "" || field == "" {
		return fmt.Errorf("SQL JSON bitmap index requires a cache key and field")
	}
	ht.sqlIndexMu.Lock()
	defer ht.sqlIndexMu.Unlock()
	if ht.sqlJSONBitmapIndexes == nil {
		ht.sqlJSONBitmapIndexes = map[string]map[string]*sqlJSONBitmapIndex{}
	}
	if ht.sqlJSONBitmapIndexes[key] == nil {
		ht.sqlJSONBitmapIndexes[key] = map[string]*sqlJSONBitmapIndex{}
	}
	ht.sqlJSONBitmapIndexes[key][field] = &sqlJSONBitmapIndex{}
	return nil
}

// CreateSQLJSONCoveringIndex configures an equality index that retains only
// field and columns. Queries whose predicate and projection are covered can
// avoid materializing the source row.
func (ht *HatTrie) CreateSQLJSONCoveringIndex(key, field string, columns ...string) error {
	if ht == nil || key == "" || field == "" || len(columns) == 0 {
		return fmt.Errorf("SQL JSON covering index requires a cache key, field, and columns")
	}
	covered := map[string]struct{}{field: {}}
	for _, column := range columns {
		if column == "" {
			return fmt.Errorf("SQL JSON covering index columns must not be empty")
		}
		covered[column] = struct{}{}
	}
	ht.sqlIndexMu.Lock()
	defer ht.sqlIndexMu.Unlock()
	if ht.sqlJSONCoveringIndexes == nil {
		ht.sqlJSONCoveringIndexes = map[string]map[string]*sqlJSONCoveringIndex{}
	}
	if ht.sqlJSONCoveringIndexes[key] == nil {
		ht.sqlJSONCoveringIndexes[key] = map[string]*sqlJSONCoveringIndex{}
	}
	ht.sqlJSONCoveringIndexes[key][field] = &sqlJSONCoveringIndex{columns: covered}
	return nil
}

// ScheduleSQLJSONIndexRebuild queues one configured field for an explicit
// rebuild. Duplicate requests are coalesced until the queued request runs.
func (ht *HatTrie) ScheduleSQLJSONIndexRebuild(key, field string) error {
	if ht == nil || key == "" || field == "" {
		return fmt.Errorf("SQL JSON index rebuild requires a cache key and field")
	}
	ht.sqlIndexMu.Lock()
	defer ht.sqlIndexMu.Unlock()
	if !ht.sqlJSONIndexConfiguredLocked(key, field) {
		return fmt.Errorf("SQL JSON index %q on %q is not configured", field, key)
	}
	if ht.sqlJSONIndexRebuildPending == nil {
		ht.sqlJSONIndexRebuildPending = map[string]map[string]bool{}
	}
	if ht.sqlJSONIndexRebuildPending[key] == nil {
		ht.sqlJSONIndexRebuildPending[key] = map[string]bool{}
	}
	if ht.sqlJSONIndexRebuildPending[key][field] {
		return nil
	}
	ht.sqlJSONIndexRebuildPending[key][field] = true
	ht.sqlJSONIndexRebuildQueue = append(ht.sqlJSONIndexRebuildQueue, sqlJSONIndexRebuildRequest{key: key, field: field})
	ht.sqlJSONIndexMaintenanceLocked(key, field).scheduled++
	return nil
}

// RunScheduledSQLJSONIndexRebuilds processes up to limit queued requests. A
// non-positive limit processes every currently queued request. Failed requests
// remain pending so an operator can retry after fixing the source issue.
func (ht *HatTrie) RunScheduledSQLJSONIndexRebuilds(limit int) (int, error) {
	if ht == nil {
		return 0, ErrNilHatTrie
	}
	processed := 0
	for limit <= 0 || processed < limit {
		ht.sqlIndexMu.Lock()
		if len(ht.sqlJSONIndexRebuildQueue) == 0 {
			ht.sqlIndexMu.Unlock()
			return processed, nil
		}
		request := ht.sqlJSONIndexRebuildQueue[0]
		ht.sqlJSONIndexRebuildQueue[0] = sqlJSONIndexRebuildRequest{}
		ht.sqlJSONIndexRebuildQueue = ht.sqlJSONIndexRebuildQueue[1:]
		delete(ht.sqlJSONIndexRebuildPending[request.key], request.field)
		ht.sqlIndexMu.Unlock()

		data, err := ht.GetBytesChecked(request.key)
		if err != nil {
			ht.sqlIndexMu.Lock()
			ht.sqlJSONIndexRebuildQueue = append([]sqlJSONIndexRebuildRequest{request}, ht.sqlJSONIndexRebuildQueue...)
			if ht.sqlJSONIndexRebuildPending[request.key] == nil {
				ht.sqlJSONIndexRebuildPending[request.key] = map[string]bool{}
			}
			ht.sqlJSONIndexRebuildPending[request.key][request.field] = true
			ht.sqlIndexMu.Unlock()
			return processed, err
		}

		ht.sqlIndexMu.Lock()
		rebuilt, err := ht.refreshSQLJSONIndexesLocked(request.key, request.field, data)
		if err != nil {
			ht.sqlJSONIndexRebuildQueue = append([]sqlJSONIndexRebuildRequest{request}, ht.sqlJSONIndexRebuildQueue...)
			if ht.sqlJSONIndexRebuildPending[request.key] == nil {
				ht.sqlJSONIndexRebuildPending[request.key] = map[string]bool{}
			}
			ht.sqlJSONIndexRebuildPending[request.key][request.field] = true
			ht.sqlIndexMu.Unlock()
			return processed, err
		}
		maintenance := ht.sqlJSONIndexMaintenanceLocked(request.key, request.field)
		maintenance.runs++
		maintenance.rebuilds += uint64(rebuilt)
		ht.sqlIndexMu.Unlock()
		processed++
	}
	return processed, nil
}

// SQLJSONIndexMaintenanceStats reports whether the configured indexes are
// current with the CACHE value and whether an explicit rebuild is pending.
func (ht *HatTrie) SQLJSONIndexMaintenanceStats(key, field string) (SQLJSONIndexMaintenanceStats, bool, error) {
	if ht == nil || key == "" || field == "" {
		return SQLJSONIndexMaintenanceStats{}, false, nil
	}
	data, err := ht.GetBytesChecked(key)
	if err != nil {
		return SQLJSONIndexMaintenanceStats{}, false, err
	}
	raw := string(data)
	ht.sqlIndexMu.Lock()
	defer ht.sqlIndexMu.Unlock()
	configured, current := ht.sqlJSONIndexCurrentLocked(key, field, raw)
	if configured == 0 {
		return SQLJSONIndexMaintenanceStats{}, false, nil
	}
	maintenance := ht.sqlJSONIndexMaintenanceLocked(key, field)
	return SQLJSONIndexMaintenanceStats{
		Key: key, Field: field, Configured: configured, SourceBytes: len(data), Current: current,
		Pending: ht.sqlJSONIndexRebuildPending[key][field], Scheduled: maintenance.scheduled,
		Runs: maintenance.runs, Rebuilds: maintenance.rebuilds,
	}, true, nil
}

func (ht *HatTrie) sqlJSONIndexMaintenanceLocked(key, field string) *sqlJSONIndexMaintenance {
	if ht.sqlJSONIndexMaintenance == nil {
		ht.sqlJSONIndexMaintenance = map[string]map[string]*sqlJSONIndexMaintenance{}
	}
	if ht.sqlJSONIndexMaintenance[key] == nil {
		ht.sqlJSONIndexMaintenance[key] = map[string]*sqlJSONIndexMaintenance{}
	}
	if ht.sqlJSONIndexMaintenance[key][field] == nil {
		ht.sqlJSONIndexMaintenance[key][field] = &sqlJSONIndexMaintenance{}
	}
	return ht.sqlJSONIndexMaintenance[key][field]
}

func sqlJSONCompositeIndexContains(index *sqlJSONCompositeIndex, field string) bool {
	for _, candidate := range index.fields {
		if candidate == field {
			return true
		}
	}
	return false
}

func (ht *HatTrie) sqlJSONIndexConfiguredLocked(key, field string) bool {
	configured, _ := ht.sqlJSONIndexCurrentLocked(key, field, "")
	return configured > 0
}

func (ht *HatTrie) sqlJSONIndexCurrentLocked(key, field, raw string) (int, bool) {
	configured, current := 0, true
	if index := ht.sqlJSONIndexes[key][field]; index != nil {
		configured++
		current = current && index.raw == raw
	}
	if index := ht.sqlJSONBitmapIndexes[key][field]; index != nil {
		configured++
		current = current && index.raw == raw
	}
	if index := ht.sqlJSONCoveringIndexes[key][field]; index != nil {
		configured++
		current = current && index.raw == raw
	}
	if index := ht.sqlJSONTextIndexes[key][field]; index != nil {
		configured++
		current = current && index.raw == raw
	}
	for _, index := range ht.sqlJSONCompositeIndexes[key] {
		if sqlJSONCompositeIndexContains(index, field) {
			configured++
			current = current && index.raw == raw
		}
	}
	return configured, current
}

func (ht *HatTrie) refreshSQLJSONIndexesLocked(key, field string, data []byte) (int, error) {
	raw := string(data)
	rebuilt := 0
	if index := ht.sqlJSONIndexes[key][field]; index != nil {
		changed := index.raw != raw
		if err := refreshSQLJSONFieldIndex(index, key, field, data); err != nil {
			return rebuilt, err
		}
		if changed {
			rebuilt++
		}
	}
	if index := ht.sqlJSONBitmapIndexes[key][field]; index != nil {
		changed := index.raw != raw
		if err := refreshSQLJSONBitmapIndex(index, key, field, data); err != nil {
			return rebuilt, err
		}
		if changed {
			rebuilt++
		}
	}
	if index := ht.sqlJSONCoveringIndexes[key][field]; index != nil {
		changed := index.raw != raw
		if err := refreshSQLJSONCoveringIndex(index, key, field, data); err != nil {
			return rebuilt, err
		}
		if changed {
			rebuilt++
		}
	}
	if index := ht.sqlJSONTextIndexes[key][field]; index != nil {
		changed := index.raw != raw
		if err := refreshSQLJSONTextIndex(index, key, field, data); err != nil {
			return rebuilt, err
		}
		if changed {
			rebuilt++
		}
	}
	for _, index := range ht.sqlJSONCompositeIndexes[key] {
		if !sqlJSONCompositeIndexContains(index, field) {
			continue
		}
		changed := index.raw != raw
		if err := refreshSQLJSONCompositeIndex(index, key, data); err != nil {
			return rebuilt, err
		}
		if changed {
			rebuilt++
		}
	}
	return rebuilt, nil
}

// SQLJSONBitmapIndexHealth refreshes and reports one configured bitmap index.
func (ht *HatTrie) SQLJSONBitmapIndexHealth(key, field string) (SQLJSONBitmapIndexHealth, bool, error) {
	if ht == nil || key == "" || field == "" {
		return SQLJSONBitmapIndexHealth{}, false, nil
	}
	data, err := ht.GetBytesChecked(key)
	if err != nil {
		return SQLJSONBitmapIndexHealth{}, false, err
	}
	ht.sqlIndexMu.Lock()
	defer ht.sqlIndexMu.Unlock()
	index := ht.sqlJSONBitmapIndexes[key][field]
	if index == nil {
		return SQLJSONBitmapIndexHealth{}, false, nil
	}
	refreshed := index.raw != string(data)
	if err := refreshSQLJSONBitmapIndex(index, key, field, data); err != nil {
		return SQLJSONBitmapIndexHealth{}, false, err
	}
	health := SQLJSONBitmapIndexHealth{
		Key: key, Field: field, Rows: len(index.rows), NullRows: len(index.nulls), DistinctKeys: len(index.postings),
		SourceBytes: len(data), Current: index.raw == string(data), Refreshed: refreshed,
	}
	for _, bitmap := range index.postings {
		health.IndexedRows += int(bitmap.Count())
		health.EncodedBytes += uint64(bitmap.EncodedSize())
	}
	return health, true, nil
}

// CreateSQLJSONPathIndex configures an online equality and range index for a
// nested JSON path in a CACHE value, for example $.profile.city.
func (ht *HatTrie) CreateSQLJSONPathIndex(key, path string) error {
	normalized, err := hatSql.NormalizeJSONPath(path)
	if err != nil {
		return err
	}
	return ht.CreateSQLJSONFieldIndex(key, normalized)
}

// CreateSQLJSONTextIndex configures a token index for one string field in a
// JSON object or array stored at key. Creation is online: source parsing and
// tokenization are deferred until the first matching CONTAINS query.
func (ht *HatTrie) CreateSQLJSONTextIndex(key, field string) error {
	if ht == nil || key == "" || field == "" {
		return fmt.Errorf("SQL JSON text index requires a cache key and field")
	}
	ht.sqlIndexMu.Lock()
	defer ht.sqlIndexMu.Unlock()
	if ht.sqlJSONTextIndexes == nil {
		ht.sqlJSONTextIndexes = map[string]map[string]*sqlJSONTextIndex{}
	}
	if ht.sqlJSONTextIndexes[key] == nil {
		ht.sqlJSONTextIndexes[key] = map[string]*sqlJSONTextIndex{}
	}
	ht.sqlJSONTextIndexes[key][field] = &sqlJSONTextIndex{}
	return nil
}

// ResolveSQLTextSource resolves the candidates for an AND token query against
// an opt-in text index. The SQL executor re-evaluates CONTAINS for every row
// returned here before publishing a result.
func (ht *HatTrie) ResolveSQLTextSource(name, key, field, query string) ([]SQLRow, bool, error) {
	if name != "CACHE" {
		return nil, false, nil
	}
	data, err := ht.GetBytesChecked(key)
	if err != nil {
		return nil, false, err
	}
	ht.sqlIndexMu.Lock()
	defer ht.sqlIndexMu.Unlock()
	index := ht.sqlJSONTextIndexes[key][field]
	if index == nil {
		return nil, false, nil
	}
	if err := refreshSQLJSONTextIndex(index, key, field, data); err != nil {
		return nil, false, err
	}
	tokens := hatSql.TextTokens(query)
	if len(tokens) == 0 {
		return []SQLRow{}, true, nil
	}
	postings := make([][]int, len(tokens))
	for tokenIndex, token := range tokens {
		posting := index.tokens[token]
		if len(posting) == 0 {
			return []SQLRow{}, true, nil
		}
		postings[tokenIndex] = posting
	}
	sort.Slice(postings, func(left, right int) bool { return len(postings[left]) < len(postings[right]) })
	matched := append([]int(nil), postings[0]...)
	for _, posting := range postings[1:] {
		matched = intersectSQLTextPostings(matched, posting)
		if len(matched) == 0 {
			return []SQLRow{}, true, nil
		}
	}
	rows := make([]SQLRow, len(matched))
	for rowIndex, sourceIndex := range matched {
		rows[rowIndex] = index.rows[sourceIndex]
	}
	return hatSql.CloneRows(rows), true, nil
}

func intersectSQLTextPostings(left, right []int) []int {
	result := left[:0]
	leftIndex, rightIndex := 0, 0
	for leftIndex < len(left) && rightIndex < len(right) {
		switch {
		case left[leftIndex] < right[rightIndex]:
			leftIndex++
		case left[leftIndex] > right[rightIndex]:
			rightIndex++
		default:
			result = append(result, left[leftIndex])
			leftIndex++
			rightIndex++
		}
	}
	return result
}

// SQLJSONIndexHealth refreshes and reports one configured field index. It is
// safe to call while the cache is being queried; refresh remains serialized by
// the existing SQL-index mutex.
func (ht *HatTrie) SQLJSONIndexHealth(key, field string) (SQLJSONIndexHealth, bool, error) {
	if ht == nil || key == "" || field == "" {
		return SQLJSONIndexHealth{}, false, nil
	}
	data, err := ht.GetBytesChecked(key)
	if err != nil {
		return SQLJSONIndexHealth{}, false, err
	}
	ht.sqlIndexMu.Lock()
	defer ht.sqlIndexMu.Unlock()
	index := ht.sqlJSONIndexes[key][field]
	if index == nil {
		return SQLJSONIndexHealth{}, false, nil
	}
	refreshed := index.raw != string(data)
	if err := refreshSQLJSONFieldIndex(index, key, field, data); err != nil {
		return SQLJSONIndexHealth{}, false, err
	}
	indexedRows := 0
	for _, rows := range index.rows {
		indexedRows += len(rows)
	}
	return SQLJSONIndexHealth{
		Key: key, Field: field, Rows: indexedRows + len(index.nulls), IndexedRows: indexedRows,
		NullRows: len(index.nulls), DistinctKeys: len(index.rows), SourceBytes: len(data),
		Current: index.raw == string(data), Refreshed: refreshed,
	}, true, nil
}

// CreateSQLJSONCompositeIndex creates an optional equality index over two or
// more JSON object fields in one CACHE value. Field order is significant and
// is retained in reported statistics and index keys.
func (ht *HatTrie) CreateSQLJSONCompositeIndex(key string, fields ...string) error {
	if ht == nil || key == "" || len(fields) < 2 {
		return fmt.Errorf("SQL composite JSON index requires a cache key and at least two fields")
	}
	seen := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		if field == "" {
			return fmt.Errorf("SQL composite JSON index fields must not be empty")
		}
		if _, exists := seen[field]; exists {
			return fmt.Errorf("SQL composite JSON index field %q is repeated", field)
		}
		seen[field] = struct{}{}
	}
	copyFields := append([]string(nil), fields...)
	identifier := sqlJSONCompositeIndexIdentifier(copyFields)
	ht.sqlIndexMu.Lock()
	defer ht.sqlIndexMu.Unlock()
	if ht.sqlJSONCompositeIndexes == nil {
		ht.sqlJSONCompositeIndexes = map[string]map[string]*sqlJSONCompositeIndex{}
	}
	if ht.sqlJSONCompositeIndexes[key] == nil {
		ht.sqlJSONCompositeIndexes[key] = map[string]*sqlJSONCompositeIndex{}
	}
	ht.sqlJSONCompositeIndexes[key][identifier] = &sqlJSONCompositeIndex{fields: copyFields}
	return nil
}

func sqlJSONCompositeIndexIdentifier(fields []string) string { return strings.Join(fields, "\x00") }
func (ht *HatTrie) ResolveSQLIndexedSource(name, key, field string, value interface{}) ([]SQLRow, bool, error) {
	if name != "CACHE" {
		return nil, false, nil
	}
	data, err := ht.GetBytesChecked(key)
	if err != nil {
		return nil, false, err
	}
	ht.sqlIndexMu.Lock()
	defer ht.sqlIndexMu.Unlock()
	if bitmap := ht.sqlJSONBitmapIndexes[key][field]; bitmap != nil {
		if err := refreshSQLJSONBitmapIndex(bitmap, key, field, data); err != nil {
			return nil, false, err
		}
		valueKey, ok := sqlIndexValueKey(value)
		if !ok {
			return []SQLRow{}, true, nil
		}
		ordinals := bitmap.postings[valueKey].Values()
		rows := make([]SQLRow, 0, len(ordinals))
		for _, ordinal := range ordinals {
			if int(ordinal) < len(bitmap.rows) {
				rows = append(rows, bitmap.rows[ordinal])
			}
		}
		return hatSql.CloneRows(rows), true, nil
	}
	index := ht.sqlJSONIndexes[key][field]
	if index == nil {
		return nil, false, nil
	}
	if err := refreshSQLJSONFieldIndex(index, key, field, data); err != nil {
		return nil, false, err
	}
	valueKey, ok := sqlIndexValueKey(value)
	if !ok {
		return []SQLRow{}, true, nil
	}
	return hatSql.CloneRows(index.rows[valueKey]), true, nil
}

// ResolveSQLCoveringSource returns equality candidates containing only fields
// explicitly configured for a covering index. The SQL executor still evaluates
// the predicate before publishing results.
func (ht *HatTrie) ResolveSQLCoveringSource(name, key, field string, value interface{}, fields []string) ([]SQLRow, bool, error) {
	if name != "CACHE" || len(fields) == 0 {
		return nil, false, nil
	}
	data, err := ht.GetBytesChecked(key)
	if err != nil {
		return nil, false, err
	}
	ht.sqlIndexMu.Lock()
	defer ht.sqlIndexMu.Unlock()
	index := ht.sqlJSONCoveringIndexes[key][field]
	if index == nil {
		return nil, false, nil
	}
	for _, column := range fields {
		if _, ok := index.columns[column]; !ok {
			return nil, false, nil
		}
	}
	if err := refreshSQLJSONCoveringIndex(index, key, field, data); err != nil {
		return nil, false, err
	}
	valueKey, ok := sqlIndexValueKey(value)
	if !ok {
		return []SQLRow{}, true, nil
	}
	return hatSql.CloneRows(index.rows[valueKey]), true, nil
}

// ResolveSQLSecondaryIndexedSource combines exact bitmap postings for a
// homogeneous equality predicate. It returns source-order candidate rows; the
// SQL executor evaluates the original predicate again before returning data.
func (ht *HatTrie) ResolveSQLSecondaryIndexedSource(name, key, operation string, fields []string, values []interface{}) ([]SQLRow, bool, error) {
	if name != "CACHE" || len(fields) != len(values) || len(fields) < 2 || operation != "AND" && operation != "OR" {
		return nil, false, nil
	}
	data, err := ht.GetBytesChecked(key)
	if err != nil {
		return nil, false, err
	}
	ht.sqlIndexMu.Lock()
	defer ht.sqlIndexMu.Unlock()
	indexes := make([]*sqlJSONBitmapIndex, len(fields))
	postings := make([]hatDataStructure.RoaringBitmap, len(fields))
	for index, field := range fields {
		bitmap := ht.sqlJSONBitmapIndexes[key][field]
		if bitmap == nil {
			return nil, false, nil
		}
		if err := refreshSQLJSONBitmapIndex(bitmap, key, field, data); err != nil {
			return nil, false, err
		}
		valueKey, ok := sqlIndexValueKey(values[index])
		if !ok {
			return []SQLRow{}, true, nil
		}
		indexes[index] = bitmap
		postings[index] = bitmap.postings[valueKey]
	}
	ordinals := sqlSecondaryBitmapOrdinals(operation, postings)
	rows := make([]SQLRow, 0, len(ordinals))
	for _, ordinal := range ordinals {
		if int(ordinal) < len(indexes[0].rows) {
			rows = append(rows, indexes[0].rows[ordinal])
		}
	}
	return hatSql.CloneRows(rows), true, nil
}

func sqlSecondaryBitmapOrdinals(operation string, postings []hatDataStructure.RoaringBitmap) []uint32 {
	if len(postings) == 0 {
		return []uint32{}
	}
	if operation == "AND" {
		base := 0
		for index := 1; index < len(postings); index++ {
			if postings[index].Count() < postings[base].Count() {
				base = index
			}
		}
		candidates := postings[base].Values()
		matched := candidates[:0]
		for _, ordinal := range candidates {
			present := true
			for index := range postings {
				if index != base && !postings[index].Contains(ordinal) {
					present = false
					break
				}
			}
			if present {
				matched = append(matched, ordinal)
			}
		}
		return matched
	}
	merged := hatDataStructure.NewRoaringBitmap()
	for _, posting := range postings {
		for _, ordinal := range posting.Values() {
			merged.Add(ordinal)
		}
	}
	return merged.Values()
}

// ResolveSQLCompositeIndexedSource uses the longest configured composite
// index whose fields are all present in the supplied equality predicates.
func (ht *HatTrie) ResolveSQLCompositeIndexedSource(name, key string, fields []string, values []interface{}) ([]SQLRow, bool, error) {
	if name != "CACHE" || len(fields) != len(values) || len(fields) < 2 {
		return nil, false, nil
	}
	data, err := ht.GetBytesChecked(key)
	if err != nil {
		return nil, false, err
	}
	provided := make(map[string]interface{}, len(fields))
	for index, field := range fields {
		provided[field] = values[index]
	}
	ht.sqlIndexMu.Lock()
	defer ht.sqlIndexMu.Unlock()
	var selected *sqlJSONCompositeIndex
	for _, candidate := range ht.sqlJSONCompositeIndexes[key] {
		if len(candidate.fields) <= 1 || selected != nil && len(candidate.fields) <= len(selected.fields) {
			continue
		}
		available := true
		for _, field := range candidate.fields {
			if _, ok := provided[field]; !ok {
				available = false
				break
			}
		}
		if available {
			selected = candidate
		}
	}
	if selected == nil {
		return nil, false, nil
	}
	if err := refreshSQLJSONCompositeIndex(selected, key, data); err != nil {
		return nil, false, err
	}
	lookup := make([]interface{}, len(selected.fields))
	for index, field := range selected.fields {
		lookup[index] = provided[field]
	}
	valueKey, ok := sqlJSONCompositeIndexValueKey(lookup)
	if !ok {
		return []SQLRow{}, true, nil
	}
	return hatSql.CloneRows(selected.rows[valueKey]), true, nil
}

// SQLJSONIndexStats returns fresh cardinality statistics for an optional
// single-field or composite JSON index.
func (ht *HatTrie) SQLJSONIndexStats(key string, fields ...string) (SQLJSONIndexStats, bool, error) {
	if ht == nil || key == "" || len(fields) == 0 {
		return SQLJSONIndexStats{}, false, nil
	}
	data, err := ht.GetBytesChecked(key)
	if err != nil {
		return SQLJSONIndexStats{}, false, err
	}
	ht.sqlIndexMu.Lock()
	defer ht.sqlIndexMu.Unlock()
	if len(fields) == 1 {
		index := ht.sqlJSONIndexes[key][fields[0]]
		if index == nil {
			return SQLJSONIndexStats{}, false, nil
		}
		if err := refreshSQLJSONFieldIndex(index, key, fields[0], data); err != nil {
			return SQLJSONIndexStats{}, false, err
		}
		return sqlJSONIndexStats(key, fields, index.rows, len(index.nulls)), true, nil
	}
	index := ht.sqlJSONCompositeIndexes[key][sqlJSONCompositeIndexIdentifier(fields)]
	if index == nil {
		return SQLJSONIndexStats{}, false, nil
	}
	if err := refreshSQLJSONCompositeIndex(index, key, data); err != nil {
		return SQLJSONIndexStats{}, false, err
	}
	return sqlJSONIndexStats(key, index.fields, index.rows), true, nil
}

// SQLJSONIndexValueEstimate returns the exact posting-list size for one value
// supplied by the caller. It does not expose or enumerate any indexed values.
// exact is false only when the value cannot be represented by this index;
// available is false when the requested field index does not exist.
func (ht *HatTrie) SQLJSONIndexValueEstimate(key, field string, value interface{}) (rows int, exact bool, available bool, err error) {
	if ht == nil || key == "" || field == "" {
		return 0, false, false, nil
	}
	data, err := ht.GetBytesChecked(key)
	if err != nil {
		return 0, false, false, err
	}
	ht.sqlIndexMu.Lock()
	defer ht.sqlIndexMu.Unlock()
	if bitmap := ht.sqlJSONBitmapIndexes[key][field]; bitmap != nil {
		if err := refreshSQLJSONBitmapIndex(bitmap, key, field, data); err != nil {
			return 0, false, true, err
		}
		valueKey, ok := sqlIndexValueKey(value)
		if !ok {
			return 0, true, true, nil
		}
		return int(bitmap.postings[valueKey].Count()), true, true, nil
	}
	index := ht.sqlJSONIndexes[key][field]
	if index == nil {
		return 0, false, false, nil
	}
	if err := refreshSQLJSONFieldIndex(index, key, field, data); err != nil {
		return 0, false, true, err
	}
	valueKey, ok := sqlIndexValueKey(value)
	if !ok {
		return 0, true, true, nil
	}
	return len(index.rows[valueKey]), true, true, nil
}

// SQLJSONRangeStats returns fresh equal-depth histogram buckets for one
// optional ordered field index. A nonpositive bucket count uses 16 buckets;
// the result never has more buckets than indexed rows.
func (ht *HatTrie) SQLJSONRangeStats(key, field string, bucketCount int) (SQLJSONRangeStats, bool, error) {
	if ht == nil || key == "" || field == "" {
		return SQLJSONRangeStats{}, false, nil
	}
	data, err := ht.GetBytesChecked(key)
	if err != nil {
		return SQLJSONRangeStats{}, false, err
	}
	ht.sqlIndexMu.Lock()
	defer ht.sqlIndexMu.Unlock()
	index := ht.sqlJSONIndexes[key][field]
	if index == nil {
		return SQLJSONRangeStats{}, false, nil
	}
	if err := refreshSQLJSONFieldIndex(index, key, field, data); err != nil {
		return SQLJSONRangeStats{}, false, err
	}
	stats := SQLJSONRangeStats{Key: key, Field: field, Rows: len(index.ordered), NullRows: len(index.nulls)}
	if len(index.ordered) == 0 {
		return stats, true, nil
	}
	if bucketCount <= 0 {
		bucketCount = 16
	}
	if bucketCount > len(index.ordered) {
		bucketCount = len(index.ordered)
	}
	bucketSize := (len(index.ordered) + bucketCount - 1) / bucketCount
	stats.Buckets = make([]SQLJSONRangeHistogramBucket, 0, bucketCount)
	for start := 0; start < len(index.ordered); start += bucketSize {
		end := start + bucketSize
		if end > len(index.ordered) {
			end = len(index.ordered)
		}
		stats.Buckets = append(stats.Buckets, SQLJSONRangeHistogramBucket{
			Lower: index.ordered[start].value,
			Upper: index.ordered[end-1].value,
			Rows:  end - start,
		})
	}
	return stats, true, nil
}

// SQLJSONRangeEstimate returns the exact number of indexed rows matching one
// SQL range comparison. It shares the range scan's ordering and NULL rules.
func (ht *HatTrie) SQLJSONRangeEstimate(key, field, operator string, value interface{}) (rows int, exact bool, available bool, err error) {
	if ht == nil || key == "" || field == "" {
		return 0, false, false, nil
	}
	data, err := ht.GetBytesChecked(key)
	if err != nil {
		return 0, false, false, err
	}
	ht.sqlIndexMu.Lock()
	defer ht.sqlIndexMu.Unlock()
	index := ht.sqlJSONIndexes[key][field]
	if index == nil {
		return 0, false, false, nil
	}
	if value == nil {
		return 0, true, true, nil
	}
	if err := refreshSQLJSONFieldIndex(index, key, field, data); err != nil {
		return 0, false, true, err
	}
	start, end, ok := sqlJSONRangeBounds(index.ordered, operator, value)
	if !ok {
		return 0, false, true, fmt.Errorf("unsupported SQL range operator %q", operator)
	}
	return end - start, true, true, nil
}

func sqlJSONIndexStats(key string, fields []string, postings map[string][]SQLRow, nullRows ...int) SQLJSONIndexStats {
	stats := SQLJSONIndexStats{Key: key, Fields: append([]string(nil), fields...), DistinctKeys: len(postings)}
	if len(nullRows) > 0 {
		stats.NullRows = nullRows[0]
	}
	frequencies := make(map[int]int, len(postings))
	for _, posting := range postings {
		count := len(posting)
		stats.Rows += count
		if stats.MinRowsPerKey == 0 || count < stats.MinRowsPerKey {
			stats.MinRowsPerKey = count
		}
		if count > stats.MaxRowsPerKey {
			stats.MaxRowsPerKey = count
		}
		frequencies[count]++
	}
	if stats.DistinctKeys == 0 {
		return stats
	}
	stats.AverageRowsPerKey = float64(stats.Rows) / float64(stats.DistinctKeys)
	counts := make([]int, 0, len(frequencies))
	for count := range frequencies {
		counts = append(counts, count)
	}
	sort.Ints(counts)
	stats.FrequencyHistogram = make([]SQLJSONIndexFrequencyBucket, 0, len(counts))
	for _, count := range counts {
		stats.FrequencyHistogram = append(stats.FrequencyHistogram, SQLJSONIndexFrequencyBucket{RowsPerKey: count, DistinctKeys: frequencies[count]})
	}
	return stats
}

// ResolveSQLIndexedRangeSource uses the ordered representation of an opt-in
// JSON field index. Missing and null fields are absent because ordinary SQL
// comparisons with NULL are unknown and therefore never pass WHERE.
func (ht *HatTrie) ResolveSQLIndexedRangeSource(name, key, field, operator string, value interface{}) ([]SQLRow, bool, error) {
	if name != "CACHE" || value == nil {
		return nil, false, nil
	}
	data, err := ht.GetBytesChecked(key)
	if err != nil {
		return nil, false, err
	}
	ht.sqlIndexMu.Lock()
	defer ht.sqlIndexMu.Unlock()
	index := ht.sqlJSONIndexes[key][field]
	if index == nil {
		return nil, false, nil
	}
	if err := refreshSQLJSONFieldIndex(index, key, field, data); err != nil {
		return nil, false, err
	}
	start, end, ok := sqlJSONRangeBounds(index.ordered, operator, value)
	if !ok {
		return nil, false, nil
	}
	rows := make([]SQLRow, end-start)
	for i, entry := range index.ordered[start:end] {
		rows[i] = entry.row
	}
	return hatSql.CloneRows(rows), true, nil
}

func sqlJSONRangeBounds(ordered []sqlJSONFieldIndexEntry, operator string, value interface{}) (start, end int, ok bool) {
	start, end = 0, len(ordered)
	switch operator {
	case "<":
		end = sort.Search(len(ordered), func(index int) bool { return hatSql.Compare(ordered[index].value, value) >= 0 })
	case "<=":
		end = sort.Search(len(ordered), func(index int) bool { return hatSql.Compare(ordered[index].value, value) > 0 })
	case ">":
		start = sort.Search(len(ordered), func(index int) bool { return hatSql.Compare(ordered[index].value, value) > 0 })
	case ">=":
		start = sort.Search(len(ordered), func(index int) bool { return hatSql.Compare(ordered[index].value, value) >= 0 })
	default:
		return 0, 0, false
	}
	return start, end, true
}

// ResolveSQLOrderedSource returns every JSON source row in the exact order of
// one opt-in indexed field. It is used only for a compatible ORDER BY plan;
// callers outside the SQL executor may use it as an optional resolver method.
func (ht *HatTrie) ResolveSQLOrderedSource(name, key, field string, desc, nullsFirst, nullsLast bool) ([]SQLRow, bool, error) {
	if name != "CACHE" {
		return nil, false, nil
	}
	data, err := ht.GetBytesChecked(key)
	if err != nil {
		return nil, false, err
	}
	ht.sqlIndexMu.Lock()
	defer ht.sqlIndexMu.Unlock()
	index := ht.sqlJSONIndexes[key][field]
	if index == nil {
		return nil, false, nil
	}
	if err := refreshSQLJSONFieldIndex(index, key, field, data); err != nil {
		return nil, false, err
	}
	rows := make([]SQLRow, 0, len(index.ordered)+len(index.nulls))
	if desc {
		for end := len(index.ordered); end > 0; {
			start := end - 1
			for start > 0 && hatSql.Compare(index.ordered[start-1].value, index.ordered[end-1].value) == 0 {
				start--
			}
			for _, entry := range index.ordered[start:end] {
				rows = append(rows, entry.row)
			}
			end = start
		}
	} else {
		for _, entry := range index.ordered {
			rows = append(rows, entry.row)
		}
	}
	placeNullsFirst := false
	if len(index.ordered) > 0 {
		placeNullsFirst, _ = hatSql.OrderLess(desc, nullsFirst, nullsLast, nil, index.ordered[0].value)
	}
	if placeNullsFirst {
		rows = append(append([]SQLRow{}, index.nulls...), rows...)
	} else {
		rows = append(rows, index.nulls...)
	}
	return hatSql.CloneRows(rows), true, nil
}

// StreamSQLOrderedSource visits an indexed CACHE source in one-field SQL
// ORDER BY order. It captures immutable index slice headers while locked, then
// releases the index before calling visit; refreshes replace slices instead of
// mutating their backing arrays, so this remains a stable query snapshot.
func (ht *HatTrie) StreamSQLOrderedSource(ctx context.Context, name, key, field string, desc, nullsFirst, nullsLast bool, visit func(SQLRow) error) (bool, error) {
	if ht == nil {
		return false, ErrNilHatTrie
	}
	if visit == nil {
		return false, fmt.Errorf("SQL row callback is required")
	}
	if name != "CACHE" {
		return false, nil
	}
	data, err := ht.GetBytesChecked(key)
	if err != nil {
		return false, err
	}
	ht.sqlIndexMu.Lock()
	index := ht.sqlJSONIndexes[key][field]
	if index == nil {
		ht.sqlIndexMu.Unlock()
		return false, nil
	}
	if err := refreshSQLJSONFieldIndex(index, key, field, data); err != nil {
		ht.sqlIndexMu.Unlock()
		return false, err
	}
	ordered, nulls := index.ordered, index.nulls
	placeNullsFirst := false
	if len(ordered) > 0 {
		placeNullsFirst, _ = hatSql.OrderLess(desc, nullsFirst, nullsLast, nil, ordered[0].value)
	}
	ht.sqlIndexMu.Unlock()
	clone := func(row SQLRow) SQLRow {
		copy := make(SQLRow, len(row))
		for name, value := range row {
			copy[name] = value
		}
		return copy
	}
	emit := func(row SQLRow) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		return visit(clone(row))
	}
	emitNulls := func() error {
		for _, row := range nulls {
			if err := emit(row); err != nil {
				return err
			}
		}
		return nil
	}
	emitOrdered := func() error {
		if desc {
			for end := len(ordered); end > 0; {
				start := end - 1
				for start > 0 && hatSql.Compare(ordered[start-1].value, ordered[end-1].value) == 0 {
					start--
				}
				for _, entry := range ordered[start:end] {
					if err := emit(entry.row); err != nil {
						return err
					}
				}
				end = start
			}
			return nil
		}
		for _, entry := range ordered {
			if err := emit(entry.row); err != nil {
				return err
			}
		}
		return nil
	}
	if placeNullsFirst {
		if err := emitNulls(); err != nil {
			return true, err
		}
		return true, emitOrdered()
	}
	if err := emitOrdered(); err != nil {
		return true, err
	}
	return true, emitNulls()
}

func refreshSQLJSONBitmapIndex(index *sqlJSONBitmapIndex, key, field string, data []byte) error {
	if index.raw == string(data) {
		return nil
	}
	rows, err := sqlJSONRows(key, data)
	if err != nil {
		return err
	}
	if uint64(len(rows)) > uint64(^uint32(0)) {
		return fmt.Errorf("SQL JSON bitmap index supports at most %d rows", ^uint32(0))
	}
	index.raw, index.rows, index.postings, index.nulls = string(data), rows, map[string]hatDataStructure.RoaringBitmap{}, nil
	for rowIndex, row := range rows {
		value, exists, err := sqlJSONIndexRowValue(row, field)
		if err != nil {
			return err
		}
		if !exists {
			index.nulls = append(index.nulls, row)
			continue
		}
		valueKey, ok := sqlIndexValueKey(value)
		if !ok {
			index.nulls = append(index.nulls, row)
			continue
		}
		bitmap := index.postings[valueKey]
		bitmap.Add(uint32(rowIndex))
		index.postings[valueKey] = bitmap
	}
	return nil
}

func refreshSQLJSONCoveringIndex(index *sqlJSONCoveringIndex, key, field string, data []byte) error {
	if index.raw == string(data) {
		return nil
	}
	rows, err := sqlJSONRows(key, data)
	if err != nil {
		return err
	}
	index.raw, index.rows = string(data), map[string][]SQLRow{}
	for _, row := range rows {
		value, exists, err := sqlJSONIndexRowValue(row, field)
		if err != nil {
			return err
		}
		if !exists {
			continue
		}
		valueKey, ok := sqlIndexValueKey(value)
		if !ok {
			continue
		}
		covered := make(SQLRow, len(index.columns))
		for column := range index.columns {
			value, exists, err := sqlJSONIndexRowValue(row, column)
			if err != nil {
				return err
			}
			if exists {
				covered[column] = value
			}
		}
		index.rows[valueKey] = append(index.rows[valueKey], covered)
	}
	return nil
}

func sqlJSONIndexRowValue(row SQLRow, field string) (interface{}, bool, error) {
	if strings.HasPrefix(field, "$") {
		return hatSql.JSONPathValue(row, field)
	}
	value, exists := row[field]
	return value, exists, nil
}

func refreshSQLJSONFieldIndex(index *sqlJSONFieldIndex, key, field string, data []byte) error {
	if index.raw == string(data) {
		return nil
	}
	rows, err := sqlJSONRows(key, data)
	if err != nil {
		return err
	}
	index.raw, index.rows, index.ordered, index.nulls = string(data), map[string][]SQLRow{}, nil, nil
	for _, row := range rows {
		value, exists, err := sqlJSONIndexRowValue(row, field)
		if err != nil {
			return err
		}
		if !exists {
			index.nulls = append(index.nulls, row)
			continue
		}
		if valueKey, ok := sqlIndexValueKey(value); ok {
			index.rows[valueKey] = append(index.rows[valueKey], row)
			index.ordered = append(index.ordered, sqlJSONFieldIndexEntry{value: value, row: row})
		} else {
			index.nulls = append(index.nulls, row)
		}
	}
	sort.SliceStable(index.ordered, func(i, j int) bool {
		return hatSql.Compare(index.ordered[i].value, index.ordered[j].value) < 0
	})
	return nil
}

func refreshSQLJSONTextIndex(index *sqlJSONTextIndex, key, field string, data []byte) error {
	if index.raw == string(data) {
		return nil
	}
	rows, err := sqlJSONRows(key, data)
	if err != nil {
		return err
	}
	index.raw, index.rows, index.tokens = string(data), rows, map[string][]int{}
	for rowIndex, row := range rows {
		text, ok := row[field].(string)
		if !ok {
			continue
		}
		for _, token := range hatSql.TextTokens(text) {
			index.tokens[token] = append(index.tokens[token], rowIndex)
		}
	}
	return nil
}

func refreshSQLJSONCompositeIndex(index *sqlJSONCompositeIndex, key string, data []byte) error {
	if index.raw == string(data) {
		return nil
	}
	rows, err := sqlJSONRows(key, data)
	if err != nil {
		return err
	}
	index.raw, index.rows = string(data), map[string][]SQLRow{}
	for _, row := range rows {
		values := make([]interface{}, len(index.fields))
		for fieldIndex, field := range index.fields {
			values[fieldIndex] = row[field]
		}
		if valueKey, ok := sqlJSONCompositeIndexValueKey(values); ok {
			index.rows[valueKey] = append(index.rows[valueKey], row)
		}
	}
	return nil
}

func sqlJSONCompositeIndexValueKey(values []interface{}) (string, bool) {
	keys := make([]string, len(values))
	for index, value := range values {
		key, ok := sqlIndexValueKey(value)
		if !ok {
			return "", false
		}
		keys[index] = key
	}
	return strings.Join(keys, "\x00"), true
}
func sqlJSONRows(key string, data []byte) ([]SQLRow, error) {
	if len(data) == 0 {
		return []SQLRow{}, nil
	}
	var rows []SQLRow
	if json.Unmarshal(data, &rows) == nil {
		return rows, nil
	}
	var row SQLRow
	if json.Unmarshal(data, &row) == nil {
		return []SQLRow{row}, nil
	}
	return nil, fmt.Errorf("CACHE(%q) must contain a JSON object or an array of JSON objects", key)
}
func sqlIndexValueKey(value interface{}) (string, bool) {
	if value == nil {
		return "", false
	}
	encoded, err := json.Marshal(value)
	return string(encoded), err == nil
}

// ResolveSQLSource exposes a stable, read-only snapshot of cache data to SQL.
// CACHE(key) requires a JSON object or array of JSON objects. KEYS returns the
// same metadata fields exposed by the monitoring entries endpoint.
func (ht *HatTrie) ResolveSQLSource(name string, key string) ([]SQLRow, error) {
	if name == "CACHE" {
		plan, err := ht.SQLPartitionPruningPlan(name, key)
		if err != nil {
			return nil, err
		}
		if plan.Pruned {
			return ht.localPartitionSet().tries[plan.Partition].ResolveSQLSource(name, key)
		}
	}
	switch name {
	case "KEYS":
		entries := ht.monitoringEntries("")
		rows := make([]SQLRow, 0, len(entries))
		for _, entry := range entries {
			rows = append(rows, SQLRow{"key": entry.Key, "type": entry.Type, "ttl_ms": entry.TTLMillis, "on_disk": entry.OnDisk, "size_bytes": entry.SizeBytes, "value_preview": entry.ValuePreview})
		}
		return rows, nil
	case "CACHE":
		data, err := ht.GetBytesChecked(key)
		if err != nil {
			return nil, err
		}
		if len(data) == 0 {
			return []SQLRow{}, nil
		}
		var array []SQLRow
		if err := json.Unmarshal(data, &array); err == nil {
			return array, nil
		}
		var object SQLRow
		if err := json.Unmarshal(data, &object); err == nil {
			return []SQLRow{object}, nil
		}
		return nil, fmt.Errorf("CACHE(%q) must contain a JSON object or an array of JSON objects", key)
	default:
		return nil, fmt.Errorf("unknown SQL source %q", name)
	}
}

// StreamSQLSource visits CACHE JSON object rows incrementally. KEYS is not
// streamable yet because its monitoring metadata scan has different expiration
// maintenance semantics.
func (ht *HatTrie) StreamSQLSource(ctx context.Context, name string, key string, visit func(SQLRow) error) error {
	if ht == nil {
		return ErrNilHatTrie
	}
	if visit == nil {
		return fmt.Errorf("SQL row callback is required")
	}
	if name != "CACHE" {
		return fmt.Errorf("SQL source %q does not support row streaming", name)
	}
	data, err := ht.GetBytesChecked(key)
	if err != nil {
		return err
	}
	data = bytes.TrimSpace(data)
	if len(data) == 0 {
		return nil
	}
	if !json.Valid(data) {
		return fmt.Errorf("CACHE(%q) must contain a JSON object or an array of JSON objects", key)
	}
	if data[0] != '[' {
		var row SQLRow
		if err := json.Unmarshal(data, &row); err != nil {
			return fmt.Errorf("CACHE(%q) must contain a JSON object or an array of JSON objects", key)
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		return visit(row)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	token, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("CACHE(%q) must contain a JSON object or an array of JSON objects", key)
	}
	if delimiter, ok := token.(json.Delim); !ok || delimiter != '[' {
		return fmt.Errorf("CACHE(%q) must contain a JSON object or an array of JSON objects", key)
	}
	for decoder.More() {
		if err := ctx.Err(); err != nil {
			return err
		}
		var row SQLRow
		if err := decoder.Decode(&row); err != nil {
			return fmt.Errorf("CACHE(%q) must contain a JSON object or an array of JSON objects", key)
		}
		if err := visit(row); err != nil {
			return err
		}
	}
	if _, err := decoder.Token(); err != nil {
		return fmt.Errorf("CACHE(%q) must contain a JSON object or an array of JSON objects", key)
	}
	return nil
}

// LockSQLSnapshot exists for resolvers that coordinate snapshot lifetime. A
// HatTrie source copies and memoizes each resolved source per query; it must not
// hold ht.mu across execution because normal source readers acquire the same
// lock and KEYS may need its exclusive maintenance path.
func (ht *HatTrie) LockSQLSnapshot() func() {
	return func() {}
}
