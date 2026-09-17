package hatSql

import (
	"context"
	"errors"
	"runtime"
	"sort"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"
)

const (
	// DefaultSQLClusterAdmissionMaxClusters bounds lazily created cluster
	// ledgers when no maximum is configured.
	DefaultSQLClusterAdmissionMaxClusters = 256
	// MaxSQLClusterAdmissionMaxClusters prevents untrusted configuration from
	// creating an unbounded number of cluster policies and ledgers.
	MaxSQLClusterAdmissionMaxClusters = 4096
	// DefaultSQLClusterAdmissionQueueCapacity bounds waiters in each workload
	// class when no queue capacity is configured.
	DefaultSQLClusterAdmissionQueueCapacity = 64
	// MaxSQLClusterAdmissionQueueCapacity bounds retained waiter requests.
	MaxSQLClusterAdmissionQueueCapacity    = 100000
	maxSQLClusterAdmissionClusterNameBytes = 128
)

var (
	// ErrSQLClusterAdmissionNil reports a nil admission controller or lease
	// receiver where an operation cannot proceed.
	ErrSQLClusterAdmissionNil = errors.New("SQL cluster admission is nil")
	// ErrSQLClusterAdmissionInvalid reports malformed options or requests.
	ErrSQLClusterAdmissionInvalid = errors.New("SQL cluster admission request is invalid")
	// ErrSQLClusterAdmissionClosed reports admission after controller shutdown.
	ErrSQLClusterAdmissionClosed = errors.New("SQL cluster admission is closed")
	// ErrSQLClusterAdmissionQueueFull reports a bounded class queue with no
	// available waiter slot.
	ErrSQLClusterAdmissionQueueFull = errors.New("SQL cluster admission queue is full")
	// ErrSQLClusterAdmissionRequestTooLarge reports a request that can never fit
	// in its selected cluster/class resource pool.
	ErrSQLClusterAdmissionRequestTooLarge = errors.New("SQL cluster admission request is too large")
	// ErrSQLClusterAdmissionClusterLimit reports that the active cluster ledger
	// bound has been reached.
	ErrSQLClusterAdmissionClusterLimit = errors.New("SQL cluster admission cluster limit reached")
)

// SQLClusterWorkClass selects an independently reserved resource pool.
type SQLClusterWorkClass string

const (
	// SQLClusterWorkServing is the latency-sensitive query pool.
	SQLClusterWorkServing SQLClusterWorkClass = "serving"
	// SQLClusterWorkMaintenance is the background/backfill/compaction pool.
	SQLClusterWorkMaintenance SQLClusterWorkClass = "maintenance"
)

// SQLClusterAdmissionPool configures one class-specific resource pool. CPUUnits
// and MemoryBytes are request/capacity units chosen by the caller; a zero
// MemoryBytes means memory accounting is disabled for that pool. Zero CPUUnits,
// MaxRunning, and MaxQueued select safe defaults.
type SQLClusterAdmissionPool struct {
	CPUUnits    int64 `json:"cpu_units"`
	MemoryBytes int64 `json:"memory_bytes"`
	MaxRunning  int   `json:"max_running"`
	MaxQueued   int   `json:"max_queued"`
}

// SQLClusterAdmissionPolicy reserves independent pools for serving and
// maintenance work in one cluster.
type SQLClusterAdmissionPolicy struct {
	Serving     SQLClusterAdmissionPool `json:"serving"`
	Maintenance SQLClusterAdmissionPool `json:"maintenance"`
}

// SQLClusterAdmissionOptions configures default and named cluster policies.
// Named policies are applied when the corresponding cluster is first used.
// The controller itself is opt-in and does not modify existing SQL execution.
type SQLClusterAdmissionOptions struct {
	Default     SQLClusterAdmissionPolicy            `json:"default"`
	Clusters    map[string]SQLClusterAdmissionPolicy `json:"clusters,omitempty"`
	MaxClusters int                                  `json:"max_clusters,omitempty"`
}

// SQLClusterAdmissionRequest describes one resource reservation.
type SQLClusterAdmissionRequest struct {
	Cluster     string              `json:"cluster"`
	Class       SQLClusterWorkClass `json:"class"`
	CPUUnits    int64               `json:"cpu_units"`
	MemoryBytes int64               `json:"memory_bytes"`
}

// SQLClusterAdmissionPoolStats reports current usage and queue depth for one
// class-specific pool.
type SQLClusterAdmissionPoolStats struct {
	Limits     SQLClusterAdmissionPool `json:"limits"`
	CPUUsed    int64                   `json:"cpu_used"`
	MemoryUsed int64                   `json:"memory_used"`
	Running    int                     `json:"running"`
	Queued     int                     `json:"queued"`
}

// SQLClusterAdmissionClusterStats is a consistent point-in-time view of both
// class pools for one active cluster.
type SQLClusterAdmissionClusterStats struct {
	Cluster     string                       `json:"cluster"`
	Serving     SQLClusterAdmissionPoolStats `json:"serving"`
	Maintenance SQLClusterAdmissionPoolStats `json:"maintenance"`
}

// SQLClusterAdmissionLease owns one reservation. Release is idempotent and is
// safe to call after Close.
type SQLClusterAdmissionLease struct {
	pool    *sqlClusterAdmissionPoolState
	request SQLClusterAdmissionRequest
	once    sync.Once
}

// Release returns the reservation to its class-specific pool.
func (lease *SQLClusterAdmissionLease) Release() {
	if lease == nil || lease.pool == nil {
		return
	}
	lease.once.Do(func() {
		lease.pool.release(lease.request)
	})
}

// SQLClusterAdmission bounds concurrent and queued work independently for
// serving and maintenance classes on each named cluster. It is safe for
// concurrent Acquire, Execute, Stats, Snapshot, and Close calls.
type SQLClusterAdmission struct {
	mu            sync.Mutex
	defaultPolicy SQLClusterAdmissionPolicy
	policies      map[string]SQLClusterAdmissionPolicy
	clusters      map[string]*sqlClusterAdmissionClusterState
	maxClusters   int
	closed        bool
}

type sqlClusterAdmissionClusterState struct {
	serving     *sqlClusterAdmissionPoolState
	maintenance *sqlClusterAdmissionPoolState
}

type sqlClusterAdmissionPoolState struct {
	mu         sync.Mutex
	limits     SQLClusterAdmissionPool
	cpuUsed    int64
	memoryUsed int64
	running    int
	waiters    []*sqlClusterAdmissionWaiter
	closed     bool
}

type sqlClusterAdmissionWaiter struct {
	ready     chan struct{}
	request   SQLClusterAdmissionRequest
	lease     *SQLClusterAdmissionLease
	err       error
	granted   bool
	cancelled bool
}

// NewSQLClusterAdmission creates an opt-in cluster admission controller.
// Default serving capacity follows GOMAXPROCS; default maintenance capacity is
// one CPU unit. Memory accounting remains disabled until a pool gets a
// positive MemoryBytes limit.
func NewSQLClusterAdmission(options SQLClusterAdmissionOptions) (*SQLClusterAdmission, error) {
	maxClusters := options.MaxClusters
	if maxClusters == 0 {
		maxClusters = DefaultSQLClusterAdmissionMaxClusters
	}
	if maxClusters < 1 || maxClusters > MaxSQLClusterAdmissionMaxClusters {
		return nil, ErrSQLClusterAdmissionInvalid
	}
	if len(options.Clusters) > maxClusters {
		return nil, ErrSQLClusterAdmissionInvalid
	}
	defaultPolicy, err := normalizeSQLClusterAdmissionPolicy(options.Default)
	if err != nil {
		return nil, err
	}
	policies := make(map[string]SQLClusterAdmissionPolicy, len(options.Clusters))
	for name, policy := range options.Clusters {
		normalizedName, err := normalizeSQLClusterAdmissionClusterName(name)
		if err != nil {
			return nil, err
		}
		if _, exists := policies[normalizedName]; exists {
			return nil, ErrSQLClusterAdmissionInvalid
		}
		policy, err = normalizeSQLClusterAdmissionPolicy(policy)
		if err != nil {
			return nil, err
		}
		policies[normalizedName] = policy
	}
	return &SQLClusterAdmission{
		defaultPolicy: defaultPolicy,
		policies:      policies,
		clusters:      make(map[string]*sqlClusterAdmissionClusterState),
		maxClusters:   maxClusters,
	}, nil
}

// Acquire waits for a class-specific reservation, observing context
// cancellation. Serving and maintenance never consume each other's reserved
// CPU or memory capacity.
func (admission *SQLClusterAdmission) Acquire(ctx context.Context, request SQLClusterAdmissionRequest) (*SQLClusterAdmissionLease, error) {
	if admission == nil {
		return nil, ErrSQLClusterAdmissionNil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	normalized, err := normalizeSQLClusterAdmissionRequest(request)
	if err != nil {
		return nil, err
	}
	pool, err := admission.poolFor(normalized)
	if err != nil {
		return nil, err
	}
	return pool.acquire(ctx, normalized)
}

// Execute acquires a reservation, invokes fn, and always releases the lease.
// The callback receives a non-nil context and may call the existing SQL
// execution entry points inside the reservation.
func (admission *SQLClusterAdmission) Execute(ctx context.Context, request SQLClusterAdmissionRequest, fn func(context.Context) error) error {
	if admission == nil {
		return ErrSQLClusterAdmissionNil
	}
	if fn == nil {
		return ErrSQLClusterAdmissionInvalid
	}
	if ctx == nil {
		ctx = context.Background()
	}
	lease, err := admission.Acquire(ctx, request)
	if err != nil {
		return err
	}
	defer lease.Release()
	return fn(ctx)
}

// Stats returns the current class-pool state for an active cluster.
func (admission *SQLClusterAdmission) Stats(cluster string) (SQLClusterAdmissionClusterStats, bool) {
	if admission == nil {
		return SQLClusterAdmissionClusterStats{}, false
	}
	cluster, err := normalizeSQLClusterAdmissionClusterName(cluster)
	if err != nil {
		return SQLClusterAdmissionClusterStats{}, false
	}
	admission.mu.Lock()
	state := admission.clusters[cluster]
	admission.mu.Unlock()
	if state == nil {
		return SQLClusterAdmissionClusterStats{}, false
	}
	return SQLClusterAdmissionClusterStats{
		Cluster:     cluster,
		Serving:     state.serving.stats(),
		Maintenance: state.maintenance.stats(),
	}, true
}

// Snapshot returns deterministic statistics for all active clusters.
func (admission *SQLClusterAdmission) Snapshot() []SQLClusterAdmissionClusterStats {
	if admission == nil {
		return nil
	}
	admission.mu.Lock()
	clusters := make([]string, 0, len(admission.clusters))
	for cluster := range admission.clusters {
		clusters = append(clusters, cluster)
	}
	states := make(map[string]*sqlClusterAdmissionClusterState, len(clusters))
	for _, cluster := range clusters {
		states[cluster] = admission.clusters[cluster]
	}
	admission.mu.Unlock()
	sort.Strings(clusters)
	snapshot := make([]SQLClusterAdmissionClusterStats, 0, len(clusters))
	for _, cluster := range clusters {
		state := states[cluster]
		snapshot = append(snapshot, SQLClusterAdmissionClusterStats{
			Cluster:     cluster,
			Serving:     state.serving.stats(),
			Maintenance: state.maintenance.stats(),
		})
	}
	return snapshot
}

// Close rejects new reservations and wakes queued callers with
// ErrSQLClusterAdmissionClosed. Existing leases remain valid until released.
func (admission *SQLClusterAdmission) Close() error {
	if admission == nil {
		return nil
	}
	admission.mu.Lock()
	if admission.closed {
		admission.mu.Unlock()
		return nil
	}
	admission.closed = true
	states := make([]*sqlClusterAdmissionClusterState, 0, len(admission.clusters))
	for _, state := range admission.clusters {
		states = append(states, state)
	}
	admission.mu.Unlock()
	for _, state := range states {
		state.serving.close()
		state.maintenance.close()
	}
	return nil
}

func (admission *SQLClusterAdmission) poolFor(request SQLClusterAdmissionRequest) (*sqlClusterAdmissionPoolState, error) {
	admission.mu.Lock()
	defer admission.mu.Unlock()
	if admission.closed {
		return nil, ErrSQLClusterAdmissionClosed
	}
	state := admission.clusters[request.Cluster]
	if state == nil {
		if len(admission.clusters) >= admission.maxClusters {
			return nil, ErrSQLClusterAdmissionClusterLimit
		}
		policy := admission.defaultPolicy
		if configured, found := admission.policies[request.Cluster]; found {
			policy = configured
		}
		state = &sqlClusterAdmissionClusterState{
			serving:     newSQLClusterAdmissionPoolState(policy.Serving),
			maintenance: newSQLClusterAdmissionPoolState(policy.Maintenance),
		}
		admission.clusters[request.Cluster] = state
	}
	if request.Class == SQLClusterWorkMaintenance {
		return state.maintenance, nil
	}
	return state.serving, nil
}

func newSQLClusterAdmissionPoolState(limits SQLClusterAdmissionPool) *sqlClusterAdmissionPoolState {
	return &sqlClusterAdmissionPoolState{
		limits:  limits,
		waiters: make([]*sqlClusterAdmissionWaiter, 0, limits.MaxQueued),
	}
}

func (pool *sqlClusterAdmissionPoolState) acquire(ctx context.Context, request SQLClusterAdmissionRequest) (*SQLClusterAdmissionLease, error) {
	pool.mu.Lock()
	if pool.closed {
		pool.mu.Unlock()
		return nil, ErrSQLClusterAdmissionClosed
	}
	if !sqlClusterAdmissionRequestFitsCapacity(request, pool.limits) {
		pool.mu.Unlock()
		return nil, ErrSQLClusterAdmissionRequestTooLarge
	}
	if len(pool.waiters) == 0 && pool.canStartLocked(request) {
		lease := pool.reserveLocked(request)
		pool.mu.Unlock()
		return lease, nil
	}
	if pool.limits.MaxQueued > 0 && len(pool.waiters) >= pool.limits.MaxQueued {
		pool.mu.Unlock()
		return nil, ErrSQLClusterAdmissionQueueFull
	}
	waiter := &sqlClusterAdmissionWaiter{
		ready:   make(chan struct{}),
		request: request,
	}
	pool.waiters = append(pool.waiters, waiter)
	pool.mu.Unlock()

	select {
	case <-waiter.ready:
		pool.mu.Lock()
		granted, lease, waitErr := waiter.granted, waiter.lease, waiter.err
		pool.mu.Unlock()
		if granted {
			if err := ctx.Err(); err != nil {
				lease.Release()
				return nil, err
			}
			return lease, nil
		}
		if waitErr != nil {
			return nil, waitErr
		}
		return nil, ErrSQLClusterAdmissionClosed
	case <-ctx.Done():
		pool.mu.Lock()
		if waiter.granted {
			lease := waiter.lease
			pool.mu.Unlock()
			lease.Release()
			return nil, ctx.Err()
		}
		if waiter.err != nil {
			waitErr := waiter.err
			pool.mu.Unlock()
			return nil, waitErr
		}
		waiter.cancelled = true
		pool.removeWaiterLocked(waiter)
		pool.mu.Unlock()
		return nil, ctx.Err()
	}
}

func (pool *sqlClusterAdmissionPoolState) canStartLocked(request SQLClusterAdmissionRequest) bool {
	if pool.limits.MaxRunning > 0 && pool.running >= pool.limits.MaxRunning {
		return false
	}
	if pool.limits.CPUUnits > 0 && request.CPUUnits > pool.limits.CPUUnits-pool.cpuUsed {
		return false
	}
	if pool.limits.MemoryBytes > 0 && request.MemoryBytes > pool.limits.MemoryBytes-pool.memoryUsed {
		return false
	}
	return true
}

func (pool *sqlClusterAdmissionPoolState) reserveLocked(request SQLClusterAdmissionRequest) *SQLClusterAdmissionLease {
	pool.running++
	pool.cpuUsed += request.CPUUnits
	pool.memoryUsed += request.MemoryBytes
	return &SQLClusterAdmissionLease{pool: pool, request: request}
}

func (pool *sqlClusterAdmissionPoolState) release(request SQLClusterAdmissionRequest) {
	pool.mu.Lock()
	if pool.running > 0 {
		pool.running--
	}
	if pool.cpuUsed >= request.CPUUnits {
		pool.cpuUsed -= request.CPUUnits
	} else {
		pool.cpuUsed = 0
	}
	if pool.memoryUsed >= request.MemoryBytes {
		pool.memoryUsed -= request.MemoryBytes
	} else {
		pool.memoryUsed = 0
	}
	if !pool.closed {
		pool.dispatchLocked()
	}
	pool.mu.Unlock()
}

func (pool *sqlClusterAdmissionPoolState) dispatchLocked() {
	for {
		selected := -1
		for index, waiter := range pool.waiters {
			if !waiter.cancelled && pool.canStartLocked(waiter.request) {
				selected = index
				break
			}
		}
		if selected < 0 {
			return
		}
		waiter := pool.waiters[selected]
		copy(pool.waiters[selected:], pool.waiters[selected+1:])
		pool.waiters[len(pool.waiters)-1] = nil
		pool.waiters = pool.waiters[:len(pool.waiters)-1]
		waiter.granted = true
		waiter.lease = pool.reserveLocked(waiter.request)
		close(waiter.ready)
	}
}

func (pool *sqlClusterAdmissionPoolState) close() {
	pool.mu.Lock()
	if pool.closed {
		pool.mu.Unlock()
		return
	}
	pool.closed = true
	for _, waiter := range pool.waiters {
		waiter.cancelled = true
		waiter.err = ErrSQLClusterAdmissionClosed
		close(waiter.ready)
	}
	pool.waiters = nil
	pool.mu.Unlock()
}

func (pool *sqlClusterAdmissionPoolState) removeWaiterLocked(target *sqlClusterAdmissionWaiter) {
	for index, waiter := range pool.waiters {
		if waiter != target {
			continue
		}
		copy(pool.waiters[index:], pool.waiters[index+1:])
		pool.waiters[len(pool.waiters)-1] = nil
		pool.waiters = pool.waiters[:len(pool.waiters)-1]
		return
	}
}

func (pool *sqlClusterAdmissionPoolState) stats() SQLClusterAdmissionPoolStats {
	pool.mu.Lock()
	defer pool.mu.Unlock()
	return SQLClusterAdmissionPoolStats{
		Limits:     pool.limits,
		CPUUsed:    pool.cpuUsed,
		MemoryUsed: pool.memoryUsed,
		Running:    pool.running,
		Queued:     len(pool.waiters),
	}
}

func normalizeSQLClusterAdmissionPolicy(policy SQLClusterAdmissionPolicy) (SQLClusterAdmissionPolicy, error) {
	serving, err := normalizeSQLClusterAdmissionPool(policy.Serving, int64(runtime.GOMAXPROCS(0)), runtime.GOMAXPROCS(0))
	if err != nil {
		return SQLClusterAdmissionPolicy{}, err
	}
	maintenance, err := normalizeSQLClusterAdmissionPool(policy.Maintenance, 1, 1)
	if err != nil {
		return SQLClusterAdmissionPolicy{}, err
	}
	return SQLClusterAdmissionPolicy{Serving: serving, Maintenance: maintenance}, nil
}

func normalizeSQLClusterAdmissionPool(pool SQLClusterAdmissionPool, defaultCPU int64, defaultRunning int) (SQLClusterAdmissionPool, error) {
	if pool.CPUUnits < 0 || pool.MemoryBytes < 0 || pool.MaxRunning < 0 || pool.MaxQueued < 0 || pool.MaxQueued > MaxSQLClusterAdmissionQueueCapacity {
		return SQLClusterAdmissionPool{}, ErrSQLClusterAdmissionInvalid
	}
	if pool.CPUUnits == 0 {
		pool.CPUUnits = defaultCPU
	}
	if pool.MaxRunning == 0 {
		pool.MaxRunning = defaultRunning
	}
	if pool.MaxQueued == 0 {
		pool.MaxQueued = DefaultSQLClusterAdmissionQueueCapacity
	}
	return pool, nil
}

func normalizeSQLClusterAdmissionRequest(request SQLClusterAdmissionRequest) (SQLClusterAdmissionRequest, error) {
	cluster, err := normalizeSQLClusterAdmissionClusterName(request.Cluster)
	if err != nil {
		return SQLClusterAdmissionRequest{}, err
	}
	class := request.Class
	if class == "" {
		class = SQLClusterWorkServing
	}
	if class != SQLClusterWorkServing && class != SQLClusterWorkMaintenance {
		return SQLClusterAdmissionRequest{}, ErrSQLClusterAdmissionInvalid
	}
	if request.CPUUnits < 0 || request.MemoryBytes < 0 {
		return SQLClusterAdmissionRequest{}, ErrSQLClusterAdmissionInvalid
	}
	if request.CPUUnits == 0 {
		request.CPUUnits = 1
	}
	request.Cluster = cluster
	request.Class = class
	return request, nil
}

func normalizeSQLClusterAdmissionClusterName(cluster string) (string, error) {
	if !utf8.ValidString(cluster) || len(cluster) > maxSQLClusterAdmissionClusterNameBytes {
		return "", ErrSQLClusterAdmissionInvalid
	}
	for _, character := range cluster {
		if unicode.IsControl(character) || unicode.Is(unicode.Cf, character) {
			return "", ErrSQLClusterAdmissionInvalid
		}
	}
	cluster = strings.TrimSpace(cluster)
	if cluster == "" {
		return "", ErrSQLClusterAdmissionInvalid
	}
	return cluster, nil
}

func sqlClusterAdmissionRequestFitsCapacity(request SQLClusterAdmissionRequest, limits SQLClusterAdmissionPool) bool {
	return request.CPUUnits <= limits.CPUUnits && (limits.MemoryBytes == 0 || request.MemoryBytes <= limits.MemoryBytes)
}
