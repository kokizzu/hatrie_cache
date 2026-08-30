package hatCache

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unsafe"

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
type SQLColumnarBatch = hatSql.ColumnarBatch
type SQLColumnarSourceResolver = hatSql.ColumnarSourceResolver
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
	if trie, ok := resolver.(*HatTrie); ok {
		resolver = sqlTimePartitionResolver{base: resolver, trie: trie, start: options.Start, end: options.End}
	}
	return hatSql.QueryTimeSeries(ctx, source, resolver, parameters, queryOptions, options)
}

// SQLTimePartition maps one logical CACHE source to a physical cache key for
// a half-open UTC time range.
type SQLTimePartition struct {
	Key   string    `json:"key"`
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
}

// SQLTimePartitionPruningPlan reports the physical CACHE keys selected for a
// time-series query. A configured source with no overlapping keys is still a
// successful pruning decision and resolves to an empty source.
type SQLTimePartitionPruningPlan struct {
	Key        string    `json:"key"`
	Start      time.Time `json:"start"`
	End        time.Time `json:"end"`
	Partitions int       `json:"partitions"`
	Keys       []string  `json:"keys"`
}

// ConfigureSQLTimePartitions configures non-overlapping physical CACHE keys
// for one logical time-series source. It is in-memory configuration, like SQL
// JSON indexes, and is intentionally opt-in.
func (ht *HatTrie) ConfigureSQLTimePartitions(key string, partitions []SQLTimePartition) error {
	if ht == nil {
		return ErrNilHatTrie
	}
	key = strings.TrimSpace(key)
	if key == "" {
		return fmt.Errorf("SQL time partitions require a logical cache key")
	}
	configured := make([]SQLTimePartition, len(partitions))
	for index, partition := range partitions {
		partition.Key = strings.TrimSpace(partition.Key)
		partition.Start = partition.Start.UTC()
		partition.End = partition.End.UTC()
		if partition.Key == "" || partition.Start.IsZero() || partition.End.IsZero() || !partition.Start.Before(partition.End) {
			return fmt.Errorf("SQL time partition %d requires a key and increasing start/end range", index)
		}
		configured[index] = partition
	}
	sort.Slice(configured, func(left, right int) bool { return configured[left].Start.Before(configured[right].Start) })
	for index := 1; index < len(configured); index++ {
		if configured[index].Start.Before(configured[index-1].End) {
			return fmt.Errorf("SQL time partitions %q and %q overlap", configured[index-1].Key, configured[index].Key)
		}
	}
	ht.sqlIndexMu.Lock()
	defer ht.sqlIndexMu.Unlock()
	if ht.sqlTimePartitions == nil {
		ht.sqlTimePartitions = map[string][]SQLTimePartition{}
	}
	ht.sqlTimePartitions[key] = configured
	return nil
}

// SQLTimePartitionPruningPlan returns physical cache keys whose configured
// ranges overlap [start,end). It reports available=false for unconfigured
// logical keys and invalid windows.
func (ht *HatTrie) SQLTimePartitionPruningPlan(key string, start, end time.Time) (SQLTimePartitionPruningPlan, bool) {
	if ht == nil || start.IsZero() || end.IsZero() || !start.Before(end) {
		return SQLTimePartitionPruningPlan{}, false
	}
	key = strings.TrimSpace(key)
	start, end = start.UTC(), end.UTC()
	ht.sqlIndexMu.RLock()
	partitions, available := ht.sqlTimePartitions[key]
	plan := SQLTimePartitionPruningPlan{Key: key, Start: start, End: end, Partitions: len(partitions)}
	if available {
		for _, partition := range partitions {
			if partition.Start.Before(end) && start.Before(partition.End) {
				plan.Keys = append(plan.Keys, partition.Key)
			}
		}
	}
	ht.sqlIndexMu.RUnlock()
	return plan, available
}

type sqlTimePartitionResolver struct {
	base       SQLSourceResolver
	trie       *HatTrie
	start, end time.Time
}

func (resolver sqlTimePartitionResolver) ResolveSQLSource(name, key string) ([]SQLRow, error) {
	if resolver.base == nil || resolver.trie == nil || !strings.EqualFold(strings.TrimSpace(name), "CACHE") {
		if resolver.base == nil {
			return nil, nil
		}
		return resolver.base.ResolveSQLSource(name, key)
	}
	plan, available := resolver.trie.SQLTimePartitionPruningPlan(key, resolver.start, resolver.end)
	if !available {
		return resolver.base.ResolveSQLSource(name, key)
	}
	rows := []SQLRow{}
	for _, partitionKey := range plan.Keys {
		partitionRows, err := resolver.base.ResolveSQLSource(name, partitionKey)
		if err != nil {
			return nil, err
		}
		rows = append(rows, partitionRows...)
	}
	return rows, nil
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
	sqlJSONIndexState
	rows    map[string][]SQLRow
	ordered []sqlJSONFieldIndexEntry
	nulls   []SQLRow
}
type sqlJSONFieldIndexEntry struct {
	value interface{}
	row   SQLRow
}
type sqlJSONSourceSnapshot struct {
	sqlJSONIndexState
	rows []SQLRow
}

// SQLIndexType declares the value representation for an opt-in typed JSON index.
type SQLIndexType string

const (
	// SQLIndexInt64 stores exact integral JSON numbers in a compact ordered index.
	SQLIndexInt64 SQLIndexType = "int64"
)

// DefaultSQLJSONIndexMaxSourceBytes bounds one automatic index rebuild before
// decoding JSON. SetSQLJSONIndexAdmissionBudget can raise, lower, or disable it.
const DefaultSQLJSONIndexMaxSourceBytes = 64 << 20

// SQLJSONIndexAdmissionBudget bounds automatic work retained by an opt-in SQL
// JSON index. A zero MaxSourceBytes explicitly disables the source-size gate.
type SQLJSONIndexAdmissionBudget struct {
	MaxSourceBytes int
}

// SQLJSONIndexSpec configures one opt-in typed JSON index. Type inference is
// intentionally absent: callers must declare the one supported representation.
type SQLJSONIndexSpec struct {
	CacheKey string
	Fields   []string
	Type     SQLIndexType
}

type sqlJSONTypedInt64Index struct {
	sqlJSONIndexState
	rows     []SQLRow
	postings map[int64][]uint32
	ordered  []sqlJSONTypedInt64Entry
	nulls    []uint32
	complete bool
}
type sqlJSONTypedInt64Entry struct {
	value   int64
	ordinal uint32
}
type sqlJSONTextIndex struct {
	sqlJSONIndexState
	rows   []SQLRow
	tokens map[string][]int
}
type sqlJSONCompositeIndex struct {
	sqlJSONIndexState
	fields []string
	rows   map[string][]SQLRow
}
type sqlJSONBitmapIndex struct {
	sqlJSONIndexState
	rows     []SQLRow
	postings map[string]hatDataStructure.RoaringBitmap
	nulls    []SQLRow
}
type sqlJSONCoveringIndex struct {
	sqlJSONIndexState
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

type sqlJSONIndexState struct {
	raw        string
	generation uint64
	ready      bool
}

type sqlJSONSource struct {
	raw        string
	generation uint64
	tracked    bool
}

var errSQLJSONIndexAdmissionDenied = fmt.Errorf("SQL JSON index source exceeds admission budget")

// SetSQLJSONIndexAdmissionBudget changes the source-size gate for future
// index refreshes. A zero MaxSourceBytes disables the gate explicitly.
func (ht *HatTrie) SetSQLJSONIndexAdmissionBudget(budget SQLJSONIndexAdmissionBudget) error {
	if ht == nil {
		return ErrNilHatTrie
	}
	if budget.MaxSourceBytes < 0 {
		return fmt.Errorf("SQL JSON index admission max source bytes must not be negative")
	}
	ht.sqlIndexMu.Lock()
	ht.sqlJSONIndexAdmissionBudget = budget
	ht.sqlJSONIndexAdmissionConfigured = true
	ht.sqlIndexMu.Unlock()
	return nil
}

func (ht *HatTrie) sqlJSONIndexSourceAdmittedLocked(source sqlJSONSource) bool {
	maxBytes := DefaultSQLJSONIndexMaxSourceBytes
	if ht.sqlJSONIndexAdmissionConfigured {
		maxBytes = ht.sqlJSONIndexAdmissionBudget.MaxSourceBytes
	}
	return maxBytes == 0 || len(source.raw) <= maxBytes
}

func (source sqlJSONSource) current(state sqlJSONIndexState) bool {
	if !state.ready {
		return false
	}
	if source.tracked {
		return state.generation == source.generation
	}
	return state.raw == source.raw
}

func (ht *HatTrie) registerSQLJSONIndexSource(key string) {
	ht.mu.Lock()
	if ht.sqlJSONIndexSourceGenerations == nil {
		ht.sqlJSONIndexSourceGenerations = make(map[string]uint64)
	}
	if _, exists := ht.sqlJSONIndexSourceGenerations[key]; !exists {
		ht.sqlJSONIndexSourceGenerations[key] = 0
	}
	ht.mu.Unlock()
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
	ht.registerSQLJSONIndexSource(key)
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

// CreateSQLTypedJSONIndex configures a compact opt-in typed JSON index. It is
// disabled by default and currently supports one exact integral field.
func (ht *HatTrie) CreateSQLTypedJSONIndex(spec SQLJSONIndexSpec) error {
	if ht == nil || spec.CacheKey == "" || len(spec.Fields) != 1 || spec.Fields[0] == "" {
		return fmt.Errorf("typed SQL JSON index requires one cache key and field")
	}
	if spec.Type != SQLIndexInt64 {
		return fmt.Errorf("unsupported typed SQL JSON index type %q", spec.Type)
	}
	ht.registerSQLJSONIndexSource(spec.CacheKey)
	ht.sqlIndexMu.Lock()
	defer ht.sqlIndexMu.Unlock()
	if ht.sqlJSONTypedInt64Indexes == nil {
		ht.sqlJSONTypedInt64Indexes = make(map[string]map[string]*sqlJSONTypedInt64Index)
	}
	if ht.sqlJSONTypedInt64Indexes[spec.CacheKey] == nil {
		ht.sqlJSONTypedInt64Indexes[spec.CacheKey] = make(map[string]*sqlJSONTypedInt64Index)
	}
	ht.sqlJSONTypedInt64Indexes[spec.CacheKey][spec.Fields[0]] = &sqlJSONTypedInt64Index{}
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
	ht.registerSQLJSONIndexSource(key)
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
	ht.registerSQLJSONIndexSource(key)
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

		source, err := ht.sqlJSONSource(request.key)
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
		rebuilt, err := ht.refreshSQLJSONIndexesLocked(request.key, request.field, source)
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
	source, err := ht.sqlJSONSource(key)
	if err != nil {
		return SQLJSONIndexMaintenanceStats{}, false, err
	}
	ht.sqlIndexMu.Lock()
	defer ht.sqlIndexMu.Unlock()
	configured, current := ht.sqlJSONIndexCurrentLocked(key, field, source)
	if configured == 0 {
		return SQLJSONIndexMaintenanceStats{}, false, nil
	}
	maintenance := ht.sqlJSONIndexMaintenanceLocked(key, field)
	return SQLJSONIndexMaintenanceStats{
		Key: key, Field: field, Configured: configured, SourceBytes: len(source.raw), Current: current,
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
	configured, _ := ht.sqlJSONIndexCurrentLocked(key, field, sqlJSONSource{})
	return configured > 0
}

func (ht *HatTrie) sqlJSONIndexCurrentLocked(key, field string, source sqlJSONSource) (int, bool) {
	configured, current := 0, true
	if index := ht.sqlJSONTypedInt64Indexes[key][field]; index != nil {
		configured++
		current = current && source.current(index.sqlJSONIndexState)
	}
	if index := ht.sqlJSONIndexes[key][field]; index != nil {
		configured++
		current = current && source.current(index.sqlJSONIndexState)
	}
	if index := ht.sqlJSONBitmapIndexes[key][field]; index != nil {
		configured++
		current = current && source.current(index.sqlJSONIndexState)
	}
	if index := ht.sqlJSONCoveringIndexes[key][field]; index != nil {
		configured++
		current = current && source.current(index.sqlJSONIndexState)
	}
	if index := ht.sqlJSONTextIndexes[key][field]; index != nil {
		configured++
		current = current && source.current(index.sqlJSONIndexState)
	}
	for _, index := range ht.sqlJSONCompositeIndexes[key] {
		if sqlJSONCompositeIndexContains(index, field) {
			configured++
			current = current && source.current(index.sqlJSONIndexState)
		}
	}
	return configured, current
}

func (ht *HatTrie) refreshSQLJSONIndexesLocked(key, field string, source sqlJSONSource) (int, error) {
	rebuilt := 0
	var snapshot *sqlJSONSourceSnapshot
	loadSnapshot := func() (*sqlJSONSourceSnapshot, error) {
		if snapshot != nil {
			return snapshot, nil
		}
		var err error
		snapshot, err = ht.sqlJSONIndexSnapshotForSourceLocked(key, source)
		return snapshot, err
	}
	if index := ht.sqlJSONTypedInt64Indexes[key][field]; index != nil {
		changed := !source.current(index.sqlJSONIndexState)
		snapshot, err := loadSnapshot()
		if err != nil {
			return rebuilt, err
		}
		refreshSQLJSONTypedInt64IndexSource(index, field, source, snapshot.rows)
		if changed {
			rebuilt++
		}
	}
	if index := ht.sqlJSONIndexes[key][field]; index != nil {
		changed := !source.current(index.sqlJSONIndexState)
		snapshot, err := loadSnapshot()
		if err != nil {
			return rebuilt, err
		}
		if err := refreshSQLJSONFieldIndexSourceRows(index, field, source, snapshot.rows); err != nil {
			return rebuilt, err
		}
		if changed {
			rebuilt++
		}
	}
	if index := ht.sqlJSONBitmapIndexes[key][field]; index != nil {
		changed := !source.current(index.sqlJSONIndexState)
		snapshot, err := loadSnapshot()
		if err != nil {
			return rebuilt, err
		}
		if err := refreshSQLJSONBitmapIndexSourceRows(index, field, source, snapshot.rows); err != nil {
			return rebuilt, err
		}
		if changed {
			rebuilt++
		}
	}
	if index := ht.sqlJSONCoveringIndexes[key][field]; index != nil {
		changed := !source.current(index.sqlJSONIndexState)
		if err := refreshSQLJSONCoveringIndexSource(index, key, field, source); err != nil {
			return rebuilt, err
		}
		if changed {
			rebuilt++
		}
	}
	if index := ht.sqlJSONTextIndexes[key][field]; index != nil {
		changed := !source.current(index.sqlJSONIndexState)
		snapshot, err := loadSnapshot()
		if err != nil {
			return rebuilt, err
		}
		if err := refreshSQLJSONTextIndexSourceRows(index, field, source, snapshot.rows); err != nil {
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
		changed := !source.current(index.sqlJSONIndexState)
		snapshot, err := loadSnapshot()
		if err != nil {
			return rebuilt, err
		}
		if err := refreshSQLJSONCompositeIndexSourceRows(index, source, snapshot.rows); err != nil {
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
	source, err := ht.sqlJSONSource(key)
	if err != nil {
		return SQLJSONBitmapIndexHealth{}, false, err
	}
	ht.sqlIndexMu.Lock()
	defer ht.sqlIndexMu.Unlock()
	index := ht.sqlJSONBitmapIndexes[key][field]
	if index == nil {
		return SQLJSONBitmapIndexHealth{}, false, nil
	}
	refreshed := !source.current(index.sqlJSONIndexState)
	snapshot, err := ht.sqlJSONIndexSnapshotForSourceLocked(key, source)
	if err != nil {
		return SQLJSONBitmapIndexHealth{}, false, err
	}
	if err := refreshSQLJSONBitmapIndexSourceRows(index, field, source, snapshot.rows); err != nil {
		return SQLJSONBitmapIndexHealth{}, false, err
	}
	health := SQLJSONBitmapIndexHealth{
		Key: key, Field: field, Rows: len(index.rows), NullRows: len(index.nulls), DistinctKeys: len(index.postings),
		SourceBytes: len(source.raw), Current: source.current(index.sqlJSONIndexState), Refreshed: refreshed,
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
	ht.registerSQLJSONIndexSource(key)
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
	source, err := ht.sqlJSONSource(key)
	if err != nil {
		return nil, false, err
	}
	ht.sqlIndexMu.Lock()
	defer ht.sqlIndexMu.Unlock()
	index := ht.sqlJSONTextIndexes[key][field]
	if index == nil {
		return nil, false, nil
	}
	snapshot, err := ht.sqlJSONIndexSnapshotForSourceLocked(key, source)
	if err != nil {
		if err == errSQLJSONIndexAdmissionDenied {
			return nil, false, nil
		}
		return nil, false, err
	}
	if err := refreshSQLJSONTextIndexSourceRows(index, field, source, snapshot.rows); err != nil {
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
	source, err := ht.sqlJSONSource(key)
	if err != nil {
		return SQLJSONIndexHealth{}, false, err
	}
	ht.sqlIndexMu.Lock()
	defer ht.sqlIndexMu.Unlock()
	index := ht.sqlJSONIndexes[key][field]
	if index == nil {
		return SQLJSONIndexHealth{}, false, nil
	}
	refreshed := !source.current(index.sqlJSONIndexState)
	snapshot, err := ht.sqlJSONIndexSnapshotForSourceLocked(key, source)
	if err != nil {
		return SQLJSONIndexHealth{}, false, err
	}
	if err := refreshSQLJSONFieldIndexSourceRows(index, field, source, snapshot.rows); err != nil {
		return SQLJSONIndexHealth{}, false, err
	}
	indexedRows := 0
	for _, rows := range index.rows {
		indexedRows += len(rows)
	}
	return SQLJSONIndexHealth{
		Key: key, Field: field, Rows: indexedRows + len(index.nulls), IndexedRows: indexedRows,
		NullRows: len(index.nulls), DistinctKeys: len(index.rows), SourceBytes: len(source.raw),
		Current: source.current(index.sqlJSONIndexState), Refreshed: refreshed,
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
	ht.registerSQLJSONIndexSource(key)
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
	ht.sqlIndexMu.Lock()
	typed := ht.sqlJSONTypedInt64Indexes[key][field]
	bitmap := ht.sqlJSONBitmapIndexes[key][field]
	index := ht.sqlJSONIndexes[key][field]
	ht.sqlIndexMu.Unlock()
	if typed != nil {
		source, err := ht.sqlJSONSource(key)
		if err != nil {
			return nil, false, err
		}
		ht.sqlIndexMu.Lock()
		defer ht.sqlIndexMu.Unlock()
		typed = ht.sqlJSONTypedInt64Indexes[key][field]
		if typed == nil {
			return nil, false, nil
		}
		snapshot, err := ht.sqlJSONIndexSnapshotForSourceLocked(key, source)
		if err != nil {
			if err == errSQLJSONIndexAdmissionDenied {
				return nil, false, nil
			}
			return nil, false, err
		}
		refreshSQLJSONTypedInt64IndexSource(typed, field, source, snapshot.rows)
		value, ok := sqlJSONTypedInt64Value(value)
		if !ok {
			return []SQLRow{}, true, nil
		}
		return sqlJSONTypedInt64Rows(typed, typed.postings[value]), true, nil
	}
	if bitmap != nil {
		source, err := ht.sqlJSONSource(key)
		if err != nil {
			return nil, false, err
		}
		ht.sqlIndexMu.Lock()
		defer ht.sqlIndexMu.Unlock()
		bitmap = ht.sqlJSONBitmapIndexes[key][field]
		if bitmap == nil {
			return nil, false, nil
		}
		snapshot, err := ht.sqlJSONIndexSnapshotForSourceLocked(key, source)
		if err != nil {
			if err == errSQLJSONIndexAdmissionDenied {
				return nil, false, nil
			}
			return nil, false, err
		}
		if err := refreshSQLJSONBitmapIndexSourceRows(bitmap, field, source, snapshot.rows); err != nil {
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
	if index == nil {
		return nil, false, nil
	}
	source, err := ht.sqlJSONSource(key)
	if err != nil {
		return nil, false, err
	}
	ht.sqlIndexMu.Lock()
	defer ht.sqlIndexMu.Unlock()
	index = ht.sqlJSONIndexes[key][field]
	if index == nil {
		return nil, false, nil
	}
	snapshot, err := ht.sqlJSONIndexSnapshotForSourceLocked(key, source)
	if err != nil {
		if err == errSQLJSONIndexAdmissionDenied {
			return nil, false, nil
		}
		return nil, false, err
	}
	if err := refreshSQLJSONFieldIndexSourceRows(index, field, source, snapshot.rows); err != nil {
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
	source, err := ht.sqlJSONSource(key)
	if err != nil {
		return nil, false, err
	}
	ht.sqlIndexMu.Lock()
	defer ht.sqlIndexMu.Unlock()
	index := ht.sqlJSONCoveringIndexes[key][field]
	if index == nil {
		return nil, false, nil
	}
	if !ht.sqlJSONIndexSourceAdmittedLocked(source) {
		return nil, false, nil
	}
	for _, column := range fields {
		if _, ok := index.columns[column]; !ok {
			return nil, false, nil
		}
	}
	if err := refreshSQLJSONCoveringIndexSource(index, key, field, source); err != nil {
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
	source, err := ht.sqlJSONSource(key)
	if err != nil {
		return nil, false, err
	}
	ht.sqlIndexMu.Lock()
	defer ht.sqlIndexMu.Unlock()
	indexes := make([]*sqlJSONBitmapIndex, len(fields))
	postings := make([]hatDataStructure.RoaringBitmap, len(fields))
	snapshot, err := ht.sqlJSONIndexSnapshotForSourceLocked(key, source)
	if err != nil {
		if err == errSQLJSONIndexAdmissionDenied {
			return nil, false, nil
		}
		return nil, false, err
	}
	for index, field := range fields {
		bitmap := ht.sqlJSONBitmapIndexes[key][field]
		if bitmap == nil {
			return nil, false, nil
		}
		if err := refreshSQLJSONBitmapIndexSourceRows(bitmap, field, source, snapshot.rows); err != nil {
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
	source, err := ht.sqlJSONSource(key)
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
	snapshot, err := ht.sqlJSONIndexSnapshotForSourceLocked(key, source)
	if err != nil {
		if err == errSQLJSONIndexAdmissionDenied {
			return nil, false, nil
		}
		return nil, false, err
	}
	if err := refreshSQLJSONCompositeIndexSourceRows(selected, source, snapshot.rows); err != nil {
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
	if len(fields) == 1 {
		source, err := ht.sqlJSONSource(key)
		if err != nil {
			return SQLJSONIndexStats{}, false, err
		}
		ht.sqlIndexMu.Lock()
		defer ht.sqlIndexMu.Unlock()
		if typed := ht.sqlJSONTypedInt64Indexes[key][fields[0]]; typed != nil {
			snapshot, err := ht.sqlJSONIndexSnapshotForSourceLocked(key, source)
			if err != nil {
				return SQLJSONIndexStats{}, false, err
			}
			refreshSQLJSONTypedInt64IndexSource(typed, fields[0], source, snapshot.rows)
			return sqlJSONTypedInt64IndexStats(key, fields, typed), true, nil
		}
		index := ht.sqlJSONIndexes[key][fields[0]]
		if index == nil {
			return SQLJSONIndexStats{}, false, nil
		}
		snapshot, err := ht.sqlJSONIndexSnapshotForSourceLocked(key, source)
		if err != nil {
			return SQLJSONIndexStats{}, false, err
		}
		if err := refreshSQLJSONFieldIndexSourceRows(index, fields[0], source, snapshot.rows); err != nil {
			return SQLJSONIndexStats{}, false, err
		}
		return sqlJSONIndexStats(key, fields, index.rows, len(index.nulls)), true, nil
	}
	source, err := ht.sqlJSONSource(key)
	if err != nil {
		return SQLJSONIndexStats{}, false, err
	}
	ht.sqlIndexMu.Lock()
	defer ht.sqlIndexMu.Unlock()
	index := ht.sqlJSONCompositeIndexes[key][sqlJSONCompositeIndexIdentifier(fields)]
	if index == nil {
		return SQLJSONIndexStats{}, false, nil
	}
	snapshot, err := ht.sqlJSONIndexSnapshotForSourceLocked(key, source)
	if err != nil {
		return SQLJSONIndexStats{}, false, err
	}
	if err := refreshSQLJSONCompositeIndexSourceRows(index, source, snapshot.rows); err != nil {
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
	ht.sqlIndexMu.Lock()
	typed := ht.sqlJSONTypedInt64Indexes[key][field]
	bitmap := ht.sqlJSONBitmapIndexes[key][field]
	index := ht.sqlJSONIndexes[key][field]
	ht.sqlIndexMu.Unlock()
	if typed != nil {
		source, err := ht.sqlJSONSource(key)
		if err != nil {
			return 0, false, false, err
		}
		ht.sqlIndexMu.Lock()
		defer ht.sqlIndexMu.Unlock()
		typed = ht.sqlJSONTypedInt64Indexes[key][field]
		if typed == nil {
			return 0, false, false, nil
		}
		snapshot, err := ht.sqlJSONIndexSnapshotForSourceLocked(key, source)
		if err != nil {
			return 0, false, true, err
		}
		refreshSQLJSONTypedInt64IndexSource(typed, field, source, snapshot.rows)
		value, ok := sqlJSONTypedInt64Value(value)
		if !ok {
			return 0, true, true, nil
		}
		return len(typed.postings[value]), true, true, nil
	}
	if bitmap != nil {
		source, err := ht.sqlJSONSource(key)
		if err != nil {
			return 0, false, false, err
		}
		ht.sqlIndexMu.Lock()
		defer ht.sqlIndexMu.Unlock()
		bitmap = ht.sqlJSONBitmapIndexes[key][field]
		if bitmap == nil {
			return 0, false, false, nil
		}
		snapshot, err := ht.sqlJSONIndexSnapshotForSourceLocked(key, source)
		if err != nil {
			return 0, false, true, err
		}
		if err := refreshSQLJSONBitmapIndexSourceRows(bitmap, field, source, snapshot.rows); err != nil {
			return 0, false, true, err
		}
		valueKey, ok := sqlIndexValueKey(value)
		if !ok {
			return 0, true, true, nil
		}
		return int(bitmap.postings[valueKey].Count()), true, true, nil
	}
	if index == nil {
		return 0, false, false, nil
	}
	source, err := ht.sqlJSONSource(key)
	if err != nil {
		return 0, false, false, err
	}
	ht.sqlIndexMu.Lock()
	defer ht.sqlIndexMu.Unlock()
	index = ht.sqlJSONIndexes[key][field]
	if index == nil {
		return 0, false, false, nil
	}
	snapshot, err := ht.sqlJSONIndexSnapshotForSourceLocked(key, source)
	if err != nil {
		return 0, false, true, err
	}
	if err := refreshSQLJSONFieldIndexSourceRows(index, field, source, snapshot.rows); err != nil {
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
	source, err := ht.sqlJSONSource(key)
	if err != nil {
		return SQLJSONRangeStats{}, false, err
	}
	ht.sqlIndexMu.Lock()
	defer ht.sqlIndexMu.Unlock()
	typed := ht.sqlJSONTypedInt64Indexes[key][field]
	index := ht.sqlJSONIndexes[key][field]
	if typed == nil && index == nil {
		return SQLJSONRangeStats{}, false, nil
	}
	snapshot, err := ht.sqlJSONIndexSnapshotForSourceLocked(key, source)
	if err != nil {
		return SQLJSONRangeStats{}, false, err
	}
	if typed != nil {
		refreshSQLJSONTypedInt64IndexSource(typed, field, source, snapshot.rows)
		return sqlJSONTypedInt64RangeStats(key, field, bucketCount, typed), true, nil
	}
	if err := refreshSQLJSONFieldIndexSourceRows(index, field, source, snapshot.rows); err != nil {
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
	source, err := ht.sqlJSONSource(key)
	if err != nil {
		return 0, false, false, err
	}
	ht.sqlIndexMu.Lock()
	defer ht.sqlIndexMu.Unlock()
	typed := ht.sqlJSONTypedInt64Indexes[key][field]
	index := ht.sqlJSONIndexes[key][field]
	if typed == nil && index == nil {
		return 0, false, false, nil
	}
	if value == nil {
		return 0, true, true, nil
	}
	snapshot, err := ht.sqlJSONIndexSnapshotForSourceLocked(key, source)
	if err != nil {
		return 0, false, true, err
	}
	if typed != nil {
		refreshSQLJSONTypedInt64IndexSource(typed, field, source, snapshot.rows)
		needle, ok := sqlJSONTypedInt64Value(value)
		if !ok {
			return 0, true, true, nil
		}
		start, end := 0, len(typed.ordered)
		switch operator {
		case "<":
			end = sort.Search(len(typed.ordered), func(index int) bool { return typed.ordered[index].value >= needle })
		case "<=":
			end = sort.Search(len(typed.ordered), func(index int) bool { return typed.ordered[index].value > needle })
		case ">":
			start = sort.Search(len(typed.ordered), func(index int) bool { return typed.ordered[index].value > needle })
		case ">=":
			start = sort.Search(len(typed.ordered), func(index int) bool { return typed.ordered[index].value >= needle })
		default:
			return 0, false, true, fmt.Errorf("unsupported SQL range operator %q", operator)
		}
		return end - start, true, true, nil
	}
	if err := refreshSQLJSONFieldIndexSourceRows(index, field, source, snapshot.rows); err != nil {
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

func sqlJSONTypedInt64IndexStats(key string, fields []string, index *sqlJSONTypedInt64Index) SQLJSONIndexStats {
	stats := SQLJSONIndexStats{
		Key:          key,
		Fields:       append([]string(nil), fields...),
		DistinctKeys: len(index.postings),
		NullRows:     len(index.nulls),
	}
	frequencies := make(map[int]int, len(index.postings))
	for _, posting := range index.postings {
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

func sqlJSONTypedInt64RangeStats(key, field string, bucketCount int, index *sqlJSONTypedInt64Index) SQLJSONRangeStats {
	stats := SQLJSONRangeStats{Key: key, Field: field, Rows: len(index.ordered), NullRows: len(index.nulls)}
	if len(index.ordered) == 0 {
		return stats
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
	return stats
}

// ResolveSQLIndexedRangeSource uses the ordered representation of an opt-in
// JSON field index. Missing and null fields are absent because ordinary SQL
// comparisons with NULL are unknown and therefore never pass WHERE.
func (ht *HatTrie) ResolveSQLIndexedRangeSource(name, key, field, operator string, value interface{}) ([]SQLRow, bool, error) {
	if name != "CACHE" || value == nil {
		return nil, false, nil
	}
	source, err := ht.sqlJSONSource(key)
	if err != nil {
		return nil, false, err
	}
	ht.sqlIndexMu.Lock()
	defer ht.sqlIndexMu.Unlock()
	if typed := ht.sqlJSONTypedInt64Indexes[key][field]; typed != nil {
		snapshot, err := ht.sqlJSONIndexSnapshotForSourceLocked(key, source)
		if err != nil {
			if err == errSQLJSONIndexAdmissionDenied {
				return nil, false, nil
			}
			return nil, false, err
		}
		refreshSQLJSONTypedInt64IndexSource(typed, field, source, snapshot.rows)
		needle, ok := sqlJSONTypedInt64Value(value)
		if !ok {
			return []SQLRow{}, true, nil
		}
		start, end := 0, len(typed.ordered)
		switch operator {
		case "<":
			end = sort.Search(len(typed.ordered), func(index int) bool { return typed.ordered[index].value >= needle })
		case "<=":
			end = sort.Search(len(typed.ordered), func(index int) bool { return typed.ordered[index].value > needle })
		case ">":
			start = sort.Search(len(typed.ordered), func(index int) bool { return typed.ordered[index].value > needle })
		case ">=":
			start = sort.Search(len(typed.ordered), func(index int) bool { return typed.ordered[index].value >= needle })
		default:
			return nil, false, nil
		}
		ordinals := make([]uint32, end-start)
		for index, entry := range typed.ordered[start:end] {
			ordinals[index] = entry.ordinal
		}
		return sqlJSONTypedInt64Rows(typed, ordinals), true, nil
	}
	index := ht.sqlJSONIndexes[key][field]
	if index == nil {
		return nil, false, nil
	}
	snapshot, err := ht.sqlJSONIndexSnapshotForSourceLocked(key, source)
	if err != nil {
		if err == errSQLJSONIndexAdmissionDenied {
			return nil, false, nil
		}
		return nil, false, err
	}
	if err := refreshSQLJSONFieldIndexSourceRows(index, field, source, snapshot.rows); err != nil {
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
	source, err := ht.sqlJSONSource(key)
	if err != nil {
		return nil, false, err
	}
	ht.sqlIndexMu.Lock()
	defer ht.sqlIndexMu.Unlock()
	if typed := ht.sqlJSONTypedInt64Indexes[key][field]; typed != nil {
		snapshot, err := ht.sqlJSONIndexSnapshotForSourceLocked(key, source)
		if err != nil {
			if err == errSQLJSONIndexAdmissionDenied {
				return nil, false, nil
			}
			return nil, false, err
		}
		refreshSQLJSONTypedInt64IndexSource(typed, field, source, snapshot.rows)
		if !typed.complete {
			return nil, false, nil
		}
		return sqlJSONTypedInt64OrderedRows(typed, desc, nullsFirst, nullsLast), true, nil
	}
	index := ht.sqlJSONIndexes[key][field]
	if index == nil {
		return nil, false, nil
	}
	snapshot, err := ht.sqlJSONIndexSnapshotForSourceLocked(key, source)
	if err != nil {
		if err == errSQLJSONIndexAdmissionDenied {
			return nil, false, nil
		}
		return nil, false, err
	}
	if err := refreshSQLJSONFieldIndexSourceRows(index, field, source, snapshot.rows); err != nil {
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
	source, err := ht.sqlJSONSource(key)
	if err != nil {
		return false, err
	}
	ht.sqlIndexMu.Lock()
	if typed := ht.sqlJSONTypedInt64Indexes[key][field]; typed != nil {
		snapshot, err := ht.sqlJSONIndexSnapshotForSourceLocked(key, source)
		if err != nil {
			ht.sqlIndexMu.Unlock()
			if err == errSQLJSONIndexAdmissionDenied {
				return false, nil
			}
			return false, err
		}
		refreshSQLJSONTypedInt64IndexSource(typed, field, source, snapshot.rows)
		if !typed.complete {
			ht.sqlIndexMu.Unlock()
			return false, nil
		}
		rows := typed.rows
		ordinals := sqlJSONTypedInt64OrderOrdinals(typed, desc, nullsFirst, nullsLast)
		ht.sqlIndexMu.Unlock()
		for _, ordinal := range ordinals {
			if int(ordinal) >= len(rows) {
				continue
			}
			if err := ctx.Err(); err != nil {
				return true, err
			}
			copy := make(SQLRow, len(rows[ordinal]))
			for name, value := range rows[ordinal] {
				copy[name] = value
			}
			if err := visit(copy); err != nil {
				return true, err
			}
		}
		return true, nil
	}
	index := ht.sqlJSONIndexes[key][field]
	if index == nil {
		ht.sqlIndexMu.Unlock()
		return false, nil
	}
	snapshot, err := ht.sqlJSONIndexSnapshotForSourceLocked(key, source)
	if err != nil {
		ht.sqlIndexMu.Unlock()
		if err == errSQLJSONIndexAdmissionDenied {
			return false, nil
		}
		return false, err
	}
	if err := refreshSQLJSONFieldIndexSourceRows(index, field, source, snapshot.rows); err != nil {
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

func refreshSQLJSONTypedInt64Index(index *sqlJSONTypedInt64Index, field, data string, rows []SQLRow) {
	refreshSQLJSONTypedInt64IndexSource(index, field, sqlJSONSource{raw: data}, rows)
}

func refreshSQLJSONTypedInt64IndexSource(index *sqlJSONTypedInt64Index, field string, source sqlJSONSource, rows []SQLRow) {
	if source.current(index.sqlJSONIndexState) {
		return
	}
	postings := make(map[int64][]uint32)
	ordered := make([]sqlJSONTypedInt64Entry, 0, len(rows))
	nulls := make([]uint32, 0)
	complete := true
	for ordinal, row := range rows {
		if uint64(ordinal) > uint64(^uint32(0)) {
			break
		}
		raw, exists := row[field]
		if !exists || raw == nil {
			nulls = append(nulls, uint32(ordinal))
			continue
		}
		value, ok := sqlJSONTypedInt64Value(raw)
		if !ok {
			complete = false
			continue
		}
		postings[value] = append(postings[value], uint32(ordinal))
		ordered = append(ordered, sqlJSONTypedInt64Entry{value: value, ordinal: uint32(ordinal)})
	}
	sort.SliceStable(ordered, func(left, right int) bool { return ordered[left].value < ordered[right].value })
	index.sqlJSONIndexState = sqlJSONIndexState{raw: source.raw, generation: source.generation, ready: true}
	index.rows, index.postings, index.ordered, index.nulls, index.complete = rows, postings, ordered, nulls, complete
}

func sqlJSONTypedInt64Value(value interface{}) (int64, bool) {
	switch value := value.(type) {
	case int:
		return int64(value), true
	case int8:
		return int64(value), true
	case int16:
		return int64(value), true
	case int32:
		return int64(value), true
	case int64:
		return value, true
	case uint:
		if uint64(value) <= math.MaxInt64 {
			return int64(value), true
		}
	case uint8:
		return int64(value), true
	case uint16:
		return int64(value), true
	case uint32:
		return int64(value), true
	case uint64:
		if value <= math.MaxInt64 {
			return int64(value), true
		}
	case float64:
		if !math.IsNaN(value) && !math.IsInf(value, 0) && math.Trunc(value) == value && value >= math.MinInt64 && value <= math.MaxInt64 {
			return int64(value), true
		}
	}
	return 0, false
}

func sqlJSONTypedInt64Rows(index *sqlJSONTypedInt64Index, ordinals []uint32) []SQLRow {
	rows := make([]SQLRow, 0, len(ordinals))
	for _, ordinal := range ordinals {
		if int(ordinal) < len(index.rows) {
			rows = append(rows, index.rows[ordinal])
		}
	}
	return hatSql.CloneRows(rows)
}

func sqlJSONTypedInt64OrderedRows(index *sqlJSONTypedInt64Index, desc, nullsFirst, nullsLast bool) []SQLRow {
	return sqlJSONTypedInt64Rows(index, sqlJSONTypedInt64OrderOrdinals(index, desc, nullsFirst, nullsLast))
}

func sqlJSONTypedInt64OrderOrdinals(index *sqlJSONTypedInt64Index, desc, nullsFirst, nullsLast bool) []uint32 {
	ordinals := make([]uint32, 0, len(index.ordered)+len(index.nulls))
	forward := func() {
		for _, entry := range index.ordered {
			ordinals = append(ordinals, entry.ordinal)
		}
	}
	backward := func() {
		for end := len(index.ordered); end > 0; {
			start := end - 1
			for start > 0 && index.ordered[start-1].value == index.ordered[end-1].value {
				start--
			}
			for _, entry := range index.ordered[start:end] {
				ordinals = append(ordinals, entry.ordinal)
			}
			end = start
		}
	}
	placeNullsFirst, _ := hatSql.OrderLess(desc, nullsFirst, nullsLast, nil, int64(0))
	if placeNullsFirst {
		ordinals = append(ordinals, index.nulls...)
	}
	if desc {
		backward()
	} else {
		forward()
	}
	if !placeNullsFirst {
		ordinals = append(ordinals, index.nulls...)
	}
	return ordinals
}

func refreshSQLJSONBitmapIndex(index *sqlJSONBitmapIndex, key, field string, data []byte) error {
	return refreshSQLJSONBitmapIndexString(index, key, field, string(data))
}

func refreshSQLJSONBitmapIndexString(index *sqlJSONBitmapIndex, key, field, data string) error {
	if index.raw == data {
		return nil
	}
	rows, err := sqlJSONRowsString(key, data)
	if err != nil {
		return err
	}
	return refreshSQLJSONBitmapIndexRows(index, field, data, rows)
}

func refreshSQLJSONBitmapIndexRows(index *sqlJSONBitmapIndex, field, data string, rows []SQLRow) error {
	return refreshSQLJSONBitmapIndexSourceRows(index, field, sqlJSONSource{raw: data}, rows)
}

func refreshSQLJSONBitmapIndexSourceRows(index *sqlJSONBitmapIndex, field string, source sqlJSONSource, rows []SQLRow) error {
	if source.current(index.sqlJSONIndexState) {
		return nil
	}
	if uint64(len(rows)) > uint64(^uint32(0)) {
		return fmt.Errorf("SQL JSON bitmap index supports at most %d rows", ^uint32(0))
	}
	postings := make(map[string]hatDataStructure.RoaringBitmap)
	var nulls []SQLRow
	for rowIndex, row := range rows {
		value, exists, err := sqlJSONIndexRowValue(row, field)
		if err != nil {
			return err
		}
		if !exists {
			nulls = append(nulls, row)
			continue
		}
		valueKey, ok := sqlIndexValueKey(value)
		if !ok {
			nulls = append(nulls, row)
			continue
		}
		bitmap := postings[valueKey]
		bitmap.Add(uint32(rowIndex))
		postings[valueKey] = bitmap
	}
	index.sqlJSONIndexState = sqlJSONIndexState{raw: source.raw, generation: source.generation, ready: true}
	index.rows, index.postings, index.nulls = rows, postings, nulls
	return nil
}

func refreshSQLJSONCoveringIndex(index *sqlJSONCoveringIndex, key, field string, data []byte) error {
	return refreshSQLJSONCoveringIndexString(index, key, field, string(data))
}

func refreshSQLJSONCoveringIndexString(index *sqlJSONCoveringIndex, key, field, data string) error {
	return refreshSQLJSONCoveringIndexSource(index, key, field, sqlJSONSource{raw: data})
}

func refreshSQLJSONCoveringIndexSource(index *sqlJSONCoveringIndex, key, field string, source sqlJSONSource) error {
	if source.current(index.sqlJSONIndexState) {
		return nil
	}
	rows, err := sqlJSONRowsString(key, source.raw)
	if err != nil {
		return err
	}
	index.sqlJSONIndexState = sqlJSONIndexState{raw: source.raw, generation: source.generation, ready: true}
	index.rows = map[string][]SQLRow{}
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
	return refreshSQLJSONFieldIndexString(index, key, field, string(data))
}

func refreshSQLJSONFieldIndexString(index *sqlJSONFieldIndex, key, field, data string) error {
	if index.raw == data {
		return nil
	}
	rows, err := sqlJSONRowsString(key, data)
	if err != nil {
		return err
	}
	return refreshSQLJSONFieldIndexRows(index, field, data, rows)
}

func refreshSQLJSONFieldIndexRows(index *sqlJSONFieldIndex, field, data string, rows []SQLRow) error {
	return refreshSQLJSONFieldIndexSourceRows(index, field, sqlJSONSource{raw: data}, rows)
}

func refreshSQLJSONFieldIndexSourceRows(index *sqlJSONFieldIndex, field string, source sqlJSONSource, rows []SQLRow) error {
	if source.current(index.sqlJSONIndexState) {
		return nil
	}
	postings := make(map[string][]SQLRow)
	ordered := make([]sqlJSONFieldIndexEntry, 0, len(rows))
	var nulls []SQLRow
	for _, row := range rows {
		value, exists, err := sqlJSONIndexRowValue(row, field)
		if err != nil {
			return err
		}
		if !exists {
			nulls = append(nulls, row)
			continue
		}
		if valueKey, ok := sqlIndexValueKey(value); ok {
			postings[valueKey] = append(postings[valueKey], row)
			ordered = append(ordered, sqlJSONFieldIndexEntry{value: value, row: row})
		} else {
			nulls = append(nulls, row)
		}
	}
	sort.SliceStable(ordered, func(i, j int) bool {
		return hatSql.Compare(ordered[i].value, ordered[j].value) < 0
	})
	index.sqlJSONIndexState = sqlJSONIndexState{raw: source.raw, generation: source.generation, ready: true}
	index.rows, index.ordered, index.nulls = postings, ordered, nulls
	return nil
}

func refreshSQLJSONTextIndex(index *sqlJSONTextIndex, key, field string, data []byte) error {
	return refreshSQLJSONTextIndexString(index, key, field, string(data))
}

func refreshSQLJSONTextIndexString(index *sqlJSONTextIndex, key, field, data string) error {
	if index.raw == data {
		return nil
	}
	rows, err := sqlJSONRowsString(key, data)
	if err != nil {
		return err
	}
	return refreshSQLJSONTextIndexRows(index, field, data, rows)
}

func refreshSQLJSONTextIndexRows(index *sqlJSONTextIndex, field, data string, rows []SQLRow) error {
	return refreshSQLJSONTextIndexSourceRows(index, field, sqlJSONSource{raw: data}, rows)
}

func refreshSQLJSONTextIndexSourceRows(index *sqlJSONTextIndex, field string, source sqlJSONSource, rows []SQLRow) error {
	if source.current(index.sqlJSONIndexState) {
		return nil
	}
	tokens := make(map[string][]int)
	for rowIndex, row := range rows {
		text, ok := row[field].(string)
		if !ok {
			continue
		}
		for _, token := range hatSql.TextTokens(text) {
			tokens[token] = append(tokens[token], rowIndex)
		}
	}
	index.sqlJSONIndexState = sqlJSONIndexState{raw: source.raw, generation: source.generation, ready: true}
	index.rows, index.tokens = rows, tokens
	return nil
}

func refreshSQLJSONCompositeIndex(index *sqlJSONCompositeIndex, key string, data []byte) error {
	return refreshSQLJSONCompositeIndexString(index, key, string(data))
}

func refreshSQLJSONCompositeIndexString(index *sqlJSONCompositeIndex, key, data string) error {
	if index.raw == data {
		return nil
	}
	rows, err := sqlJSONRowsString(key, data)
	if err != nil {
		return err
	}
	return refreshSQLJSONCompositeIndexRows(index, data, rows)
}

func refreshSQLJSONCompositeIndexRows(index *sqlJSONCompositeIndex, data string, rows []SQLRow) error {
	return refreshSQLJSONCompositeIndexSourceRows(index, sqlJSONSource{raw: data}, rows)
}

func refreshSQLJSONCompositeIndexSourceRows(index *sqlJSONCompositeIndex, source sqlJSONSource, rows []SQLRow) error {
	if source.current(index.sqlJSONIndexState) {
		return nil
	}
	postings := make(map[string][]SQLRow)
	for _, row := range rows {
		values := make([]interface{}, len(index.fields))
		for fieldIndex, field := range index.fields {
			values[fieldIndex] = row[field]
		}
		if valueKey, ok := sqlJSONCompositeIndexValueKey(values); ok {
			postings[valueKey] = append(postings[valueKey], row)
		}
	}
	index.sqlJSONIndexState = sqlJSONIndexState{raw: source.raw, generation: source.generation, ready: true}
	index.rows = postings
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

// sqlJSONSourceString returns an immutable source snapshot for SQL indexes.
// String values are immutable cache storage, so retaining their string header
// avoids copying a potentially large JSON document on every indexed query.
// Other value representations preserve the checked public byte-read fallback.
func (ht *HatTrie) sqlJSONSourceString(key string) (string, error) {
	source, err := ht.sqlJSONSource(key)
	return source.raw, err
}

func (ht *HatTrie) sqlJSONSource(key string) (sqlJSONSource, error) {
	if ht == nil {
		return sqlJSONSource{}, ErrNilHatTrie
	}
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.sqlJSONSource(key)
	}
	ht.mu.RLock()
	hval, fallback, err := ht.readValueRLockedChecked(key, true)
	if !fallback && err == nil && hval.IsStringAtRaws() {
		value := ht.strings.Get(hval.Index)
		generation, tracked := ht.sqlJSONIndexSourceGenerations[key]
		ht.recordReadLocked(true, key)
		ht.mu.RUnlock()
		return sqlJSONSource{raw: value, generation: generation, tracked: tracked}, nil
	}
	ht.mu.RUnlock()
	data, err := ht.GetBytesChecked(key)
	return sqlJSONSource{raw: string(data)}, err
}

// sqlJSONIndexSnapshotLocked returns the one decoded JSON generation shared by
// indexes that retain complete source rows. The caller must hold sqlIndexMu.
// A replacement publishes a new snapshot, preserving rows still referenced by
// indexes serving the preceding source generation.
func (ht *HatTrie) sqlJSONIndexSnapshotLocked(key, data string) (*sqlJSONSourceSnapshot, error) {
	return ht.sqlJSONIndexSnapshotForSourceLocked(key, sqlJSONSource{raw: data})
}

func (ht *HatTrie) sqlJSONIndexSnapshotForSourceLocked(key string, source sqlJSONSource) (*sqlJSONSourceSnapshot, error) {
	if !ht.sqlJSONIndexSourceAdmittedLocked(source) {
		return nil, errSQLJSONIndexAdmissionDenied
	}
	if snapshot := ht.sqlJSONIndexSnapshots[key]; snapshot != nil && source.current(snapshot.sqlJSONIndexState) {
		return snapshot, nil
	}
	rows, err := sqlJSONRowsString(key, source.raw)
	if err != nil {
		return nil, err
	}
	if ht.sqlJSONIndexSnapshots == nil {
		ht.sqlJSONIndexSnapshots = make(map[string]*sqlJSONSourceSnapshot)
	}
	snapshot := &sqlJSONSourceSnapshot{sqlJSONIndexState: sqlJSONIndexState{raw: source.raw, generation: source.generation, ready: true}, rows: rows}
	ht.sqlJSONIndexSnapshots[key] = snapshot
	return snapshot, nil
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

// sqlJSONRowsString exposes immutable string storage to the JSON decoder
// without a transient full-document byte copy. json.Unmarshal treats its input
// as read-only, and the cache string remains referenced by the caller.
func sqlJSONRowsString(key, data string) ([]SQLRow, error) {
	return sqlJSONRows(key, unsafe.Slice(unsafe.StringData(data), len(data)))
}

func sqlJSONColumnarBatch(key string, data []byte, fields []string) (hatSql.ColumnarBatch, error) {
	batch := hatSql.ColumnarBatch{Columns: make(map[string][]interface{}, len(fields))}
	unique := make([]string, 0, len(fields))
	for _, field := range fields {
		if field == "" {
			return hatSql.ColumnarBatch{}, fmt.Errorf("CACHE(%q) columnar field cannot be empty", key)
		}
		if _, exists := batch.Columns[field]; !exists {
			batch.Columns[field] = make([]interface{}, 0)
			unique = append(unique, field)
		}
	}
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 {
		return batch, nil
	}
	appendRow := func(row SQLRow) {
		for _, field := range unique {
			batch.Columns[field] = append(batch.Columns[field], row[field])
		}
		batch.Rows++
	}
	switch trimmed[0] {
	case '{':
		var row SQLRow
		if err := json.Unmarshal(trimmed, &row); err != nil {
			return hatSql.ColumnarBatch{}, fmt.Errorf("CACHE(%q) must contain a JSON object or an array of JSON objects", key)
		}
		appendRow(row)
		batch.EncodeRepeatedStrings()
		return batch, nil
	case '[':
		decoder := json.NewDecoder(bytes.NewReader(trimmed))
		if _, err := decoder.Token(); err != nil {
			return hatSql.ColumnarBatch{}, fmt.Errorf("CACHE(%q) must contain a JSON object or an array of JSON objects", key)
		}
		for decoder.More() {
			var row SQLRow
			if err := decoder.Decode(&row); err != nil {
				return hatSql.ColumnarBatch{}, fmt.Errorf("CACHE(%q) must contain a JSON object or an array of JSON objects", key)
			}
			appendRow(row)
		}
		if _, err := decoder.Token(); err != nil {
			return hatSql.ColumnarBatch{}, fmt.Errorf("CACHE(%q) must contain a JSON object or an array of JSON objects", key)
		}
		if err := decoder.Decode(&struct{}{}); err != io.EOF {
			return hatSql.ColumnarBatch{}, fmt.Errorf("CACHE(%q) must contain a JSON object or an array of JSON objects", key)
		}
		batch.EncodeRepeatedStrings()
		return batch, nil
	default:
		return hatSql.ColumnarBatch{}, fmt.Errorf("CACHE(%q) must contain a JSON object or an array of JSON objects", key)
	}
}

func sqlJSONColumnarBatchString(key, data string, fields []string) (hatSql.ColumnarBatch, error) {
	return sqlJSONColumnarBatch(key, unsafe.Slice(unsafe.StringData(data), len(data)), fields)
}

func sqlIndexValueKey(value interface{}) (string, bool) {
	switch value := value.(type) {
	case nil:
		return "", false
	case string:
		return "s:" + value, true
	case bool:
		if value {
			return "b:1", true
		}
		return "b:0", true
	case float64:
		return sqlIndexFloatValueKey(value)
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return sqlIndexIntegerValueKey(value)
	}
	encoded, err := json.Marshal(value)
	return string(encoded), err == nil
}

func sqlIndexFloatValueKey(value float64) (string, bool) {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return "", false
	}
	format := byte('f')
	abs := math.Abs(value)
	if abs != 0 && (abs < 1e-6 || abs >= 1e21) {
		format = 'e'
	}
	encoded := strconv.FormatFloat(value, format, -1, 64)
	if format == 'e' && len(encoded) >= 4 {
		zero := len(encoded) - 2
		if encoded[zero] == '0' && (encoded[zero-1] == '+' || encoded[zero-1] == '-') {
			encoded = encoded[:zero] + encoded[zero+1:]
		}
	}
	return encoded, true
}

func sqlIndexIntegerValueKey(value interface{}) (string, bool) {
	switch value := value.(type) {
	case int:
		return strconv.FormatInt(int64(value), 10), true
	case int8:
		return strconv.FormatInt(int64(value), 10), true
	case int16:
		return strconv.FormatInt(int64(value), 10), true
	case int32:
		return strconv.FormatInt(int64(value), 10), true
	case int64:
		return strconv.FormatInt(value, 10), true
	case uint:
		return strconv.FormatUint(uint64(value), 10), true
	case uint8:
		return strconv.FormatUint(uint64(value), 10), true
	case uint16:
		return strconv.FormatUint(uint64(value), 10), true
	case uint32:
		return strconv.FormatUint(uint64(value), 10), true
	case uint64:
		return strconv.FormatUint(value, 10), true
	default:
		return "", false
	}
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

// ResolveSQLColumnarSource exposes only requested CACHE JSON fields in
// field-aligned slices. It avoids retaining full source rows for simple
// analytics scans; unsupported sources return available=false.
func (ht *HatTrie) ResolveSQLColumnarSource(name, key string, fields []string) (hatSql.ColumnarBatch, bool, error) {
	if ht == nil {
		return hatSql.ColumnarBatch{}, false, ErrNilHatTrie
	}
	if name != "CACHE" {
		return hatSql.ColumnarBatch{}, false, nil
	}
	layoutKey := newSQLColumnarLayoutCacheKey(key, fields)
	if batch, cached := ht.sqlColumnarLayouts.lookup(layoutKey); cached {
		return batch, true, nil
	}
	plan, err := ht.SQLPartitionPruningPlan(name, key)
	if err != nil {
		return hatSql.ColumnarBatch{}, false, err
	}
	if plan.Pruned {
		return ht.localPartitionSet().tries[plan.Partition].ResolveSQLColumnarSource(name, key, fields)
	}
	if batch, handled, err := ht.sqlColumnarRawBytesBatch(key, fields); handled {
		if err == nil {
			ht.sqlColumnarLayouts.observe(layoutKey, batch)
		}
		return batch, true, err
	}
	data, err := ht.sqlJSONSourceString(key)
	if err != nil {
		return hatSql.ColumnarBatch{}, false, err
	}
	batch, err := sqlJSONColumnarBatchString(key, data, fields)
	if err == nil {
		ht.sqlColumnarLayouts.observe(layoutKey, batch)
	}
	return batch, true, err
}

// sqlColumnarRawBytesBatch decodes an in-memory raw value while its backing
// storage is protected by ht.mu. The returned columnar batch is detached from
// the raw bytes, so the lock is not retained after this method returns.
func (ht *HatTrie) sqlColumnarRawBytesBatch(key string, fields []string) (hatSql.ColumnarBatch, bool, error) {
	ht.mu.RLock()
	hval, fallback, err := ht.readValueRLockedChecked(key, true)
	if !fallback && err != nil {
		ht.recordReadLocked(false, key)
		ht.mu.RUnlock()
		return hatSql.ColumnarBatch{}, true, err
	}
	if !fallback && hval.IsBytesAtRaws() && !hval.OnDisk() {
		batch, decodeErr := sqlJSONColumnarBatch(key, ht.raws.array[hval.Index], fields)
		ht.recordReadLocked(true, key)
		ht.mu.RUnlock()
		return batch, true, decodeErr
	}
	ht.mu.RUnlock()
	return hatSql.ColumnarBatch{}, false, nil
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
