package hatSql

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// ErrSQLExternalDictionaryNotReady means that no successful refresh has
	// populated the dictionary yet.
	ErrSQLExternalDictionaryNotReady = errors.New("SQL external dictionary is not ready")
	// ErrSQLExternalDictionaryExpired means that the last successful snapshot
	// is older than the configured stale-value lifetime.
	ErrSQLExternalDictionaryExpired = errors.New("SQL external dictionary snapshot expired")
	// ErrSQLExternalDictionaryClosed means that the dictionary has been closed.
	ErrSQLExternalDictionaryClosed = errors.New("SQL external dictionary is closed")
	// ErrSQLExternalDictionaryNotFound means that a registry has no dictionary
	// with the requested name.
	ErrSQLExternalDictionaryNotFound = errors.New("SQL external dictionary was not found")
	// ErrSQLFunctionNotHandled allows a function resolver chain to try the next
	// resolver without turning an extension miss into a query error.
	ErrSQLFunctionNotHandled = errors.New("SQL function was not handled")
)

// SQLExternalDictionaryOptions configures a refreshable scalar lookup
// dictionary. Load owns the source I/O and returns a complete replacement
// snapshot. RefreshInterval enables the background refresher; zero selects
// explicit Refresh calls only. MaxStale bounds how long the last good
// snapshot may be served after a failed refresh; zero keeps it indefinitely.
type SQLExternalDictionaryOptions struct {
	CollectStats    bool
	Name            string
	Load            func(context.Context) (map[string]interface{}, error)
	RefreshInterval time.Duration
	MaxStale        time.Duration
}

// SQLExternalDictionaryStats reports refresh and lookup health without
// exposing dictionary values.
type SQLExternalDictionaryStats struct {
	Name             string
	Entries          int
	Generation       uint64
	LoadedAt         time.Time
	Refreshes        uint64
	RefreshFailures  uint64
	Lookups          uint64
	Hits             uint64
	Misses           uint64
	StaleHits        uint64
	Expired          uint64
	LastRefreshError string
}

type sqlExternalDictionarySnapshot struct {
	values     map[string]interface{}
	loadedAt   time.Time
	generation uint64
}

// SQLExternalDictionary is a concurrently readable, source-replaceable
// lookup dictionary. Lookups never wait for a refresh or hold a mutex.
type SQLExternalDictionary struct {
	collectStats    bool
	name            string
	load            func(context.Context) (map[string]interface{}, error)
	refreshInterval time.Duration
	maxStale        time.Duration
	now             func() time.Time

	snapshot        atomic.Pointer[sqlExternalDictionarySnapshot]
	generation      atomic.Uint64
	refreshes       atomic.Uint64
	refreshFailures atomic.Uint64
	lookups         atomic.Uint64
	hits            atomic.Uint64
	misses          atomic.Uint64
	staleHits       atomic.Uint64
	expired         atomic.Uint64
	refreshFailed   atomic.Bool

	refreshMu   sync.Mutex
	lifecycleMu sync.Mutex
	cancel      context.CancelFunc
	done        chan struct{}
	closed      atomic.Bool

	lastErrorMu sync.RWMutex
	lastError   string
}

// NewSQLExternalDictionary validates and creates a dictionary. It does not
// invoke Load; callers choose when the first snapshot is admitted.
func NewSQLExternalDictionary(options SQLExternalDictionaryOptions) (*SQLExternalDictionary, error) {
	name := strings.TrimSpace(options.Name)
	if name == "" {
		return nil, errors.New("SQL external dictionary name is required")
	}
	if options.Load == nil {
		return nil, fmt.Errorf("SQL external dictionary %q load function is required", name)
	}
	if options.RefreshInterval < 0 {
		return nil, fmt.Errorf("SQL external dictionary %q refresh interval must not be negative", name)
	}
	if options.MaxStale < 0 {
		return nil, fmt.Errorf("SQL external dictionary %q max stale lifetime must not be negative", name)
	}
	return &SQLExternalDictionary{
		collectStats:    options.CollectStats,
		name:            name,
		load:            options.Load,
		refreshInterval: options.RefreshInterval,
		maxStale:        options.MaxStale,
		now:             time.Now,
	}, nil
}

// Name returns the stable dictionary name.
func (dictionary *SQLExternalDictionary) Name() string {
	if dictionary == nil {
		return ""
	}
	return dictionary.name
}

// Refresh loads and atomically publishes one complete dictionary snapshot.
// A failed load leaves the previous snapshot untouched.
func (dictionary *SQLExternalDictionary) Refresh(ctx context.Context) error {
	if dictionary == nil {
		return ErrSQLExternalDictionaryClosed
	}
	if dictionary.closed.Load() {
		return ErrSQLExternalDictionaryClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	dictionary.refreshMu.Lock()
	defer dictionary.refreshMu.Unlock()
	if dictionary.closed.Load() {
		return ErrSQLExternalDictionaryClosed
	}
	values, err := dictionary.load(ctx)
	if err != nil {
		dictionary.recordRefreshFailure(err)
		return err
	}
	snapshotValues, err := cloneSQLExternalDictionaryValues(values)
	if err != nil {
		dictionary.recordRefreshFailure(err)
		return err
	}
	snapshot := &sqlExternalDictionarySnapshot{
		values:     snapshotValues,
		loadedAt:   dictionary.now(),
		generation: dictionary.generation.Add(1),
	}
	dictionary.snapshot.Store(snapshot)
	dictionary.refreshes.Add(1)
	dictionary.refreshFailed.Store(false)
	dictionary.recordRefreshError("")
	return nil
}

// Lookup returns one scalar dictionary value. The returned byte slice is
// copied; scalar values are returned without per-lookup heap allocation.
func (dictionary *SQLExternalDictionary) Lookup(key string) (interface{}, bool, error) {
	if dictionary == nil {
		return nil, false, ErrSQLExternalDictionaryClosed
	}
	if dictionary.closed.Load() {
		return nil, false, ErrSQLExternalDictionaryClosed
	}
	collectStats := dictionary.collectStats
	if collectStats {
		dictionary.lookups.Add(1)
	}
	snapshot := dictionary.snapshot.Load()
	if snapshot == nil {
		return nil, false, ErrSQLExternalDictionaryNotReady
	}
	age := dictionary.now().Sub(snapshot.loadedAt)
	if age < 0 {
		age = 0
	}
	if dictionary.refreshFailed.Load() {
		if dictionary.maxStale > 0 && age > dictionary.maxStale {
			if collectStats {
				dictionary.expired.Add(1)
			}
			return nil, false, ErrSQLExternalDictionaryExpired
		}
		if collectStats {
			dictionary.staleHits.Add(1)
		}
	}
	value, found := snapshot.values[key]
	if !found {
		if collectStats {
			dictionary.misses.Add(1)
		}
		return nil, false, nil
	}
	if collectStats {
		dictionary.hits.Add(1)
	}
	cloned, err := cloneSQLExternalDictionaryValue(value)
	return cloned, true, err
}

// Stats returns a stable health snapshot. It never includes dictionary values.
func (dictionary *SQLExternalDictionary) Stats() SQLExternalDictionaryStats {
	if dictionary == nil {
		return SQLExternalDictionaryStats{}
	}
	var entries int
	var generation uint64
	var loadedAt time.Time
	if snapshot := dictionary.snapshot.Load(); snapshot != nil {
		entries = len(snapshot.values)
		generation = snapshot.generation
		loadedAt = snapshot.loadedAt
	}
	dictionary.lastErrorMu.RLock()
	lastError := dictionary.lastError
	dictionary.lastErrorMu.RUnlock()
	return SQLExternalDictionaryStats{
		Name:             dictionary.name,
		Entries:          entries,
		Generation:       generation,
		LoadedAt:         loadedAt,
		Refreshes:        dictionary.refreshes.Load(),
		RefreshFailures:  dictionary.refreshFailures.Load(),
		Lookups:          dictionary.lookups.Load(),
		Hits:             dictionary.hits.Load(),
		Misses:           dictionary.misses.Load(),
		StaleHits:        dictionary.staleHits.Load(),
		Expired:          dictionary.expired.Load(),
		LastRefreshError: lastError,
	}
}

// Start performs an initial synchronous refresh and then refreshes at the
// configured interval. Refresh failures retain the last good snapshot.
func (dictionary *SQLExternalDictionary) Start(ctx context.Context) error {
	if dictionary == nil {
		return ErrSQLExternalDictionaryClosed
	}
	if dictionary.refreshInterval <= 0 {
		return errors.New("SQL external dictionary refresh interval is not configured")
	}
	if dictionary.closed.Load() {
		return ErrSQLExternalDictionaryClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := dictionary.Refresh(ctx); err != nil {
		return err
	}
	dictionary.lifecycleMu.Lock()
	defer dictionary.lifecycleMu.Unlock()
	if dictionary.closed.Load() {
		return ErrSQLExternalDictionaryClosed
	}
	if dictionary.cancel != nil {
		return errors.New("SQL external dictionary refresher is already started")
	}
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	dictionary.cancel = cancel
	dictionary.done = done
	go dictionary.runRefreshLoop(runCtx, done)
	return nil
}

func (dictionary *SQLExternalDictionary) runRefreshLoop(ctx context.Context, done chan struct{}) {
	ticker := time.NewTicker(dictionary.refreshInterval)
	defer ticker.Stop()
	defer close(done)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = dictionary.Refresh(ctx)
		}
	}
}

// Close stops background refresh and rejects future refresh or lookup calls.
func (dictionary *SQLExternalDictionary) Close() error {
	if dictionary == nil {
		return nil
	}
	dictionary.closed.Store(true)
	dictionary.lifecycleMu.Lock()
	cancel, done := dictionary.cancel, dictionary.done
	dictionary.cancel, dictionary.done = nil, nil
	dictionary.lifecycleMu.Unlock()
	if cancel != nil {
		cancel()
		<-done
	}
	return nil
}

func (dictionary *SQLExternalDictionary) recordRefreshFailure(err error) {
	dictionary.refreshFailures.Add(1)
	dictionary.refreshFailed.Store(true)
	if err == nil {
		dictionary.recordRefreshError("")
		return
	}
	dictionary.recordRefreshError(err.Error())
}

func (dictionary *SQLExternalDictionary) recordRefreshError(message string) {
	dictionary.lastErrorMu.Lock()
	dictionary.lastError = message
	dictionary.lastErrorMu.Unlock()
}

func cloneSQLExternalDictionaryValues(values map[string]interface{}) (map[string]interface{}, error) {
	cloned := make(map[string]interface{}, len(values))
	for key, value := range values {
		clonedValue, err := cloneSQLExternalDictionaryValue(value)
		if err != nil {
			return nil, fmt.Errorf("SQL external dictionary key %q: %w", key, err)
		}
		cloned[key] = clonedValue
	}
	return cloned, nil
}

func cloneSQLExternalDictionaryValue(value interface{}) (interface{}, error) {
	switch typed := value.(type) {
	case nil, bool, string,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64, time.Time:
		return value, nil
	case []byte:
		return append([]byte(nil), typed...), nil
	default:
		return nil, fmt.Errorf("unsupported value type %T; only SQL scalar values are supported", value)
	}
}

func normalizeSQLExternalDictionaryName(name string) string {
	return strings.ToLower(strings.TrimSpace(name))
}

// SQLExternalDictionaryRegistry provides named dictionaries to SQL function
// evaluation. Registry reads use an immutable atomic map; registration is
// infrequent and copies only the small name-to-dictionary directory.
type SQLExternalDictionaryRegistry struct {
	mu           sync.Mutex
	dictionaries atomic.Pointer[map[string]*SQLExternalDictionary]
}

// NewSQLExternalDictionaryRegistry creates an empty dictionary registry.
func NewSQLExternalDictionaryRegistry() *SQLExternalDictionaryRegistry {
	registry := &SQLExternalDictionaryRegistry{}
	dictionaries := map[string]*SQLExternalDictionary{}
	registry.dictionaries.Store(&dictionaries)
	return registry
}

// Register adds one named dictionary. Names are case-insensitive.
func (registry *SQLExternalDictionaryRegistry) Register(dictionary *SQLExternalDictionary) error {
	if registry == nil {
		return errors.New("SQL external dictionary registry is nil")
	}
	if dictionary == nil || dictionary.Name() == "" {
		return errors.New("SQL external dictionary is required")
	}
	name := normalizeSQLExternalDictionaryName(dictionary.Name())
	registry.mu.Lock()
	defer registry.mu.Unlock()
	current := registry.dictionaries.Load()
	currentValues := map[string]*SQLExternalDictionary{}
	if current != nil {
		currentValues = *current
	}
	next := make(map[string]*SQLExternalDictionary, len(currentValues)+1)
	for key, value := range currentValues {
		next[key] = value
	}
	if _, exists := next[name]; exists {
		return fmt.Errorf("SQL external dictionary %q is already registered", dictionary.Name())
	}
	next[name] = dictionary
	registry.dictionaries.Store(&next)
	return nil
}

// Unregister removes a dictionary by name.
func (registry *SQLExternalDictionaryRegistry) Unregister(name string) bool {
	if registry == nil {
		return false
	}
	name = normalizeSQLExternalDictionaryName(name)
	registry.mu.Lock()
	defer registry.mu.Unlock()
	current := registry.dictionaries.Load()
	if current == nil {
		return false
	}
	if _, exists := (*current)[name]; !exists {
		return false
	}
	next := make(map[string]*SQLExternalDictionary, len(*current)-1)
	for key, value := range *current {
		if key != name {
			next[key] = value
		}
	}
	registry.dictionaries.Store(&next)
	return true
}

func (registry *SQLExternalDictionaryRegistry) dictionary(name string) (*SQLExternalDictionary, bool) {
	if registry == nil {
		return nil, false
	}
	dictionaries := registry.dictionaries.Load()
	if dictionaries == nil {
		return nil, false
	}
	dictionary, exists := (*dictionaries)[normalizeSQLExternalDictionaryName(name)]
	return dictionary, exists
}

// Lookup resolves a named dictionary and then performs a lock-free value
// lookup.
func (registry *SQLExternalDictionaryRegistry) Lookup(name, key string) (interface{}, bool, error) {
	dictionary, exists := registry.dictionary(name)
	if !exists {
		return nil, false, fmt.Errorf("%w: %q", ErrSQLExternalDictionaryNotFound, name)
	}
	return dictionary.Lookup(key)
}

// Stats returns all dictionary health records in deterministic name order.
func (registry *SQLExternalDictionaryRegistry) Stats() []SQLExternalDictionaryStats {
	if registry == nil {
		return nil
	}
	dictionaries := registry.dictionaries.Load()
	if dictionaries == nil {
		return nil
	}
	stats := make([]SQLExternalDictionaryStats, 0, len(*dictionaries))
	for _, dictionary := range *dictionaries {
		stats = append(stats, dictionary.Stats())
	}
	sort.Slice(stats, func(left, right int) bool { return stats[left].Name < stats[right].Name })
	return stats
}

// EvaluateSQLFunction implements FunctionResolver for the dictionary
// functions DICT_GET, DICT_GET_OR_DEFAULT, and DICT_HAS.
func (registry *SQLExternalDictionaryRegistry) EvaluateSQLFunction(name string, calls []FunctionCall) ([]interface{}, error) {
	switch strings.ToUpper(strings.TrimSpace(name)) {
	case "DICT_GET":
		return registry.evaluateDictionaryFunction(calls, false, false)
	case "DICT_GET_OR_DEFAULT":
		return registry.evaluateDictionaryFunction(calls, true, false)
	case "DICT_HAS":
		return registry.evaluateDictionaryFunction(calls, false, true)
	default:
		return nil, ErrSQLFunctionNotHandled
	}
}

func (registry *SQLExternalDictionaryRegistry) evaluateDictionaryFunction(calls []FunctionCall, withDefault, hasOnly bool) ([]interface{}, error) {
	argumentCount := 2
	if withDefault {
		argumentCount = 3
	}
	values := make([]interface{}, len(calls))
	for index, call := range calls {
		if len(call.Arguments) != argumentCount {
			return nil, fmt.Errorf("SQL dictionary function requires %d arguments, got %d", argumentCount, len(call.Arguments))
		}
		dictionaryName, ok := call.Arguments[0].(string)
		if !ok {
			return nil, fmt.Errorf("SQL dictionary name must be a string, got %T", call.Arguments[0])
		}
		key, ok := call.Arguments[1].(string)
		if !ok {
			return nil, fmt.Errorf("SQL dictionary key must be a string, got %T", call.Arguments[1])
		}
		value, found, err := registry.Lookup(dictionaryName, key)
		if err != nil {
			return nil, err
		}
		if hasOnly {
			values[index] = found
		} else if found {
			values[index] = value
		} else if withDefault {
			values[index] = call.Arguments[2]
		}
	}
	return values, nil
}

// SQLFunctionResolverFunc adapts a function to FunctionResolver.
type SQLFunctionResolverFunc func(name string, calls []FunctionCall) ([]interface{}, error)

// EvaluateSQLFunction implements FunctionResolver.
func (function SQLFunctionResolverFunc) EvaluateSQLFunction(name string, calls []FunctionCall) ([]interface{}, error) {
	if function == nil {
		return nil, ErrSQLFunctionNotHandled
	}
	return function(name, calls)
}

// FunctionResolverFunc is the package-neutral alias for
// SQLFunctionResolverFunc.
type FunctionResolverFunc = SQLFunctionResolverFunc

// SQLFunctionResolverChain tries resolvers in order. A resolver that returns
// ErrSQLFunctionNotHandled yields to the next resolver.
type SQLFunctionResolverChain struct {
	resolvers []SQLFunctionResolver
}

// NewSQLFunctionResolverChain creates an immutable function-resolver chain.
func NewSQLFunctionResolverChain(resolvers ...SQLFunctionResolver) *SQLFunctionResolverChain {
	return &SQLFunctionResolverChain{resolvers: append([]SQLFunctionResolver(nil), resolvers...)}
}

// EvaluateSQLFunction implements FunctionResolver.
func (chain *SQLFunctionResolverChain) EvaluateSQLFunction(name string, calls []FunctionCall) ([]interface{}, error) {
	if chain == nil {
		return nil, ErrSQLFunctionNotHandled
	}
	for _, resolver := range chain.resolvers {
		if resolver == nil {
			continue
		}
		values, err := resolver.EvaluateSQLFunction(name, calls)
		if errors.Is(err, ErrSQLFunctionNotHandled) {
			continue
		}
		return values, err
	}
	return nil, ErrSQLFunctionNotHandled
}
