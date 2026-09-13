package hatSql

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"hatrie_cache/hat/hatPipeline"
)

// ErrNamespaceQueryQueueFull indicates that a namespace admission queue has
// reached its configured MaxQueuedQueries limit.
var ErrNamespaceQueryQueueFull = errors.New("namespace query queue is full")

// ErrNamespaceQueryRateLimited indicates that a namespace has exhausted its
// configured MaxQueriesPerWindow allowance.
var ErrNamespaceQueryRateLimited = errors.New("namespace query rate limit exceeded")

// ErrNamespaceQueryGovernorClosed identifies execution after governor
// shutdown.
var ErrNamespaceQueryGovernorClosed = errors.New("namespace query governor is closed")

const defaultNamespaceQueryWindow = time.Minute

// NamespaceResourceLimits caps resources available to queries in one namespace.
// Zero leaves a limit unset. Timeout is wall-clock time, which bounds CPU work
// performed by the cooperative SQL executor; MaxWorkers caps parallel operators.
type NamespaceResourceLimits struct {
	MaxConcurrentQueries int
	// MaxQueuedQueries bounds waiters behind MaxConcurrentQueries. Zero keeps
	// the existing unlimited-waiter behavior.
	MaxQueuedQueries int
	// ComputeWorkers enables a named, independently scheduled compute pool for
	// this namespace. Zero keeps the existing caller-goroutine execution path.
	ComputeWorkers int
	// ComputeQueueCapacity bounds queries waiting for a namespace compute
	// worker. Zero selects the SQL compute-pool default when workers are on.
	ComputeQueueCapacity int
	// MaxQueriesPerWindow limits admitted executions in QueryWindow. Zero
	// disables the quota. A zero QueryWindow uses one minute when the quota is
	// enabled.
	MaxQueriesPerWindow int
	QueryWindow         time.Duration
	MaxRows             int
	MaxIntermediateRows int
	MaxJoinWork         int
	MaxJoinBytes        int
	MaxResultBytes      int
	MaxWorkers          int
	MaxSortBytes        int
	MaxGroupBytes       int
	MaxGroupKeys        int
	MaxSetBytes         int
	MaxSpillBytes       int
	MaxRecursionDepth   int
	Timeout             time.Duration
	SpillDirectory      string
}

// NamespaceResourceProfile separates advisory soft limits from enforced hard
// limits. Soft limits are reported by DryRun; hard limits retain the existing
// Execute behavior and tighten requested SQL options.
type NamespaceResourceProfile struct {
	Soft NamespaceResourceLimits `json:"soft"`
	Hard NamespaceResourceLimits `json:"hard"`
}

// NamespaceQueryAdmission describes the static policy result for one query.
// DryRun never reserves a concurrency slot, consumes quota, or starts query
// work, so callers can use it for admission explanations and dashboards.
type NamespaceQueryAdmission struct {
	Namespace        string                  `json:"namespace"`
	EffectiveOptions SQLQueryOptions         `json:"-"`
	HardLimits       NamespaceResourceLimits `json:"hard_limits"`
	SoftLimits       NamespaceResourceLimits `json:"soft_limits"`
	HardClamps       []string                `json:"hard_clamps,omitempty"`
	SoftWarnings     []string                `json:"soft_warnings,omitempty"`
}

// Apply tightens options to this namespace policy. Positive policies are upper
// bounds; a caller's stricter value remains intact. A zero MaxRows is the
// executor's safe default, so a policy larger than that default cannot loosen it.
func (limits NamespaceResourceLimits) Apply(options SQLQueryOptions) SQLQueryOptions {
	options.MaxRows = applyRowLimit(options.MaxRows, limits.MaxRows)
	options.MaxIntermediateRows = applyPositiveLimit(options.MaxIntermediateRows, limits.MaxIntermediateRows)
	options.MaxJoinWork = applyPositiveLimit(options.MaxJoinWork, limits.MaxJoinWork)
	options.MaxJoinBytes = applyPositiveLimit(options.MaxJoinBytes, limits.MaxJoinBytes)
	options.MaxResultBytes = applyPositiveLimit(options.MaxResultBytes, limits.MaxResultBytes)
	options.Workers = applyPositiveLimit(options.Workers, limits.MaxWorkers)
	options.MaxSortBytes = applyPositiveLimit(options.MaxSortBytes, limits.MaxSortBytes)
	options.MaxGroupBytes = applyPositiveLimit(options.MaxGroupBytes, limits.MaxGroupBytes)
	options.MaxGroupKeys = applyPositiveLimit(options.MaxGroupKeys, limits.MaxGroupKeys)
	options.MaxSetBytes = applyPositiveLimit(options.MaxSetBytes, limits.MaxSetBytes)
	options.MaxSpillBytes = applyPositiveLimit(options.MaxSpillBytes, limits.MaxSpillBytes)
	options.MaxRecursionDepth = applyPositiveLimit(options.MaxRecursionDepth, limits.MaxRecursionDepth)
	options.Timeout = applyDurationLimit(options.Timeout, limits.Timeout)
	if limits.SpillDirectory != "" {
		options.SpillDirectory = limits.SpillDirectory
	}
	return options
}

func applyRowLimit(requested, maximum int) int {
	if maximum <= 0 {
		return requested
	}
	if requested == 0 {
		if maximum < maxSQLQueryRows {
			return maximum
		}
		return 0
	}
	return applyPositiveLimit(requested, maximum)
}

func applyPositiveLimit(requested, maximum int) int {
	if maximum > 0 && (requested == 0 || requested > maximum) {
		return maximum
	}
	return requested
}

func applyDurationLimit(requested, maximum time.Duration) time.Duration {
	if maximum > 0 && (requested == 0 || requested > maximum) {
		return maximum
	}
	return requested
}

func (limits NamespaceResourceLimits) validate() error {
	values := []struct {
		name  string
		value int
	}{
		{"max concurrent queries", limits.MaxConcurrentQueries},
		{"max queued queries", limits.MaxQueuedQueries},
		{"compute workers", limits.ComputeWorkers},
		{"compute queue capacity", limits.ComputeQueueCapacity},
		{"max queries per window", limits.MaxQueriesPerWindow},
		{"max rows", limits.MaxRows},
		{"max intermediate rows", limits.MaxIntermediateRows},
		{"max join work", limits.MaxJoinWork},
		{"max join bytes", limits.MaxJoinBytes},
		{"max result bytes", limits.MaxResultBytes},
		{"max workers", limits.MaxWorkers},
		{"max sort bytes", limits.MaxSortBytes},
		{"max group bytes", limits.MaxGroupBytes},
		{"max group keys", limits.MaxGroupKeys},
		{"max set bytes", limits.MaxSetBytes},
		{"max spill bytes", limits.MaxSpillBytes},
		{"max recursion depth", limits.MaxRecursionDepth},
	}
	for _, value := range values {
		if value.value < 0 {
			return fmt.Errorf("namespace resource limit %s must not be negative", value.name)
		}
	}
	if limits.ComputeWorkers > MaxSQLQueryManagerComputeWorkers {
		return fmt.Errorf("namespace resource limit compute workers exceed %d", MaxSQLQueryManagerComputeWorkers)
	}
	if limits.ComputeQueueCapacity > MaxSQLQueryManagerComputeQueueCapacity {
		return fmt.Errorf("namespace resource limit compute queue capacity exceed %d", MaxSQLQueryManagerComputeQueueCapacity)
	}
	if limits.Timeout < 0 {
		return fmt.Errorf("namespace resource limit timeout must not be negative")
	}
	if limits.QueryWindow < 0 {
		return fmt.Errorf("namespace resource limit query window must not be negative")
	}
	return nil
}

// NamespaceQueryGovernor applies immutable default and per-namespace resource
// limits before delegating to the package's single SQL execution path.
type NamespaceQueryGovernor struct {
	defaults       NamespaceResourceLimits
	defaultSoft    NamespaceResourceLimits
	namespaces     map[string]NamespaceResourceLimits
	softNamespaces map[string]NamespaceResourceLimits

	mu                 sync.Mutex
	gates              map[string]*namespaceQueryGate
	quotas             map[string]*namespaceQueryQuota
	computePools       map[string]*hatPipeline.WorkStealingPool
	defaultComputePool *hatPipeline.WorkStealingPool
	closed             bool
}

// NewNamespaceQueryGovernor validates and copies the supplied static policies.
// Per-namespace values only tighten defaults, which prevents accidental policy
// escalation in configuration overlays.
func NewNamespaceQueryGovernor(defaults NamespaceResourceLimits, namespaces map[string]NamespaceResourceLimits) (*NamespaceQueryGovernor, error) {
	profiles := make(map[string]NamespaceResourceProfile, len(namespaces))
	for namespace, limits := range namespaces {
		profiles[namespace] = NamespaceResourceProfile{Hard: limits}
	}
	return NewNamespaceQueryGovernorWithProfiles(NamespaceResourceProfile{Hard: defaults}, profiles)
}

// NewNamespaceQueryGovernorWithProfiles validates and copies hard and soft
// namespace policies. Hard policies are enforced by Execute; soft policies
// are advisory and appear only in DryRun results.
func NewNamespaceQueryGovernorWithProfiles(defaults NamespaceResourceProfile, namespaces map[string]NamespaceResourceProfile) (*NamespaceQueryGovernor, error) {
	defaults, err := normalizeNamespaceResourceProfile(defaults)
	if err != nil {
		return nil, err
	}
	copyNamespaces := make(map[string]NamespaceResourceProfile, len(namespaces))
	for namespace, profile := range namespaces {
		if strings.TrimSpace(namespace) == "" {
			return nil, fmt.Errorf("namespace resource policy name is required")
		}
		profile, err = normalizeNamespaceResourceProfile(profile)
		if err != nil {
			return nil, fmt.Errorf("namespace %q: %w", namespace, err)
		}
		profile.Hard = tightenNamespaceLimits(defaults.Hard, profile.Hard)
		profile.Soft = capNamespaceSoftLimits(tightenNamespaceLimits(defaults.Soft, profile.Soft), profile.Hard)
		copyNamespaces[namespace] = profile
	}
	governor := &NamespaceQueryGovernor{
		defaults:       defaults.Hard,
		defaultSoft:    defaults.Soft,
		namespaces:     make(map[string]NamespaceResourceLimits, len(copyNamespaces)),
		softNamespaces: make(map[string]NamespaceResourceLimits, len(copyNamespaces)),
		gates:          make(map[string]*namespaceQueryGate),
		quotas:         make(map[string]*namespaceQueryQuota),
	}
	for namespace, profile := range copyNamespaces {
		governor.namespaces[namespace] = profile.Hard
		governor.softNamespaces[namespace] = profile.Soft
	}
	if defaults.Hard.ComputeWorkers > 0 {
		pool, err := newNamespaceComputePool(defaults.Hard)
		if err != nil {
			return nil, err
		}
		governor.defaultComputePool = pool
	}
	for namespace, profile := range copyNamespaces {
		if profile.Hard.ComputeWorkers <= 0 {
			continue
		}
		pool, err := newNamespaceComputePool(profile.Hard)
		if err != nil {
			_ = governor.Close()
			return nil, fmt.Errorf("namespace %q: %w", namespace, err)
		}
		if governor.computePools == nil {
			governor.computePools = make(map[string]*hatPipeline.WorkStealingPool)
		}
		governor.computePools[namespace] = pool
	}
	return governor, nil
}

func newNamespaceComputePool(limits NamespaceResourceLimits) (*hatPipeline.WorkStealingPool, error) {
	queueCapacity := limits.ComputeQueueCapacity
	if queueCapacity == 0 {
		queueCapacity = DefaultSQLQueryManagerComputeQueueCapacity
	}
	return hatPipeline.NewWorkStealingPool(context.Background(), limits.ComputeWorkers, queueCapacity)
}

func normalizeNamespaceResourceLimits(limits NamespaceResourceLimits) NamespaceResourceLimits {
	if limits.MaxQueriesPerWindow > 0 && limits.QueryWindow == 0 {
		limits.QueryWindow = defaultNamespaceQueryWindow
	}
	return limits
}

func normalizeNamespaceResourceProfile(profile NamespaceResourceProfile) (NamespaceResourceProfile, error) {
	if err := profile.Soft.validate(); err != nil {
		return NamespaceResourceProfile{}, fmt.Errorf("soft profile: %w", err)
	}
	if err := profile.Hard.validate(); err != nil {
		return NamespaceResourceProfile{}, fmt.Errorf("hard profile: %w", err)
	}
	profile.Soft = normalizeNamespaceResourceLimits(profile.Soft)
	profile.Hard = normalizeNamespaceResourceLimits(profile.Hard)
	profile.Soft = capNamespaceSoftLimits(profile.Soft, profile.Hard)
	return profile, nil
}

func capNamespaceSoftLimits(soft, hard NamespaceResourceLimits) NamespaceResourceLimits {
	soft.MaxConcurrentQueries = capNamespaceLimit(soft.MaxConcurrentQueries, hard.MaxConcurrentQueries)
	soft.MaxQueuedQueries = capNamespaceLimit(soft.MaxQueuedQueries, hard.MaxQueuedQueries)
	soft.ComputeWorkers = capNamespaceLimit(soft.ComputeWorkers, hard.ComputeWorkers)
	soft.ComputeQueueCapacity = capNamespaceLimit(soft.ComputeQueueCapacity, hard.ComputeQueueCapacity)
	soft.MaxQueriesPerWindow = capNamespaceLimit(soft.MaxQueriesPerWindow, hard.MaxQueriesPerWindow)
	soft.MaxRows = capNamespaceLimit(soft.MaxRows, hard.MaxRows)
	soft.MaxIntermediateRows = capNamespaceLimit(soft.MaxIntermediateRows, hard.MaxIntermediateRows)
	soft.MaxJoinWork = capNamespaceLimit(soft.MaxJoinWork, hard.MaxJoinWork)
	soft.MaxJoinBytes = capNamespaceLimit(soft.MaxJoinBytes, hard.MaxJoinBytes)
	soft.MaxResultBytes = capNamespaceLimit(soft.MaxResultBytes, hard.MaxResultBytes)
	soft.MaxWorkers = capNamespaceLimit(soft.MaxWorkers, hard.MaxWorkers)
	soft.MaxSortBytes = capNamespaceLimit(soft.MaxSortBytes, hard.MaxSortBytes)
	soft.MaxGroupBytes = capNamespaceLimit(soft.MaxGroupBytes, hard.MaxGroupBytes)
	soft.MaxGroupKeys = capNamespaceLimit(soft.MaxGroupKeys, hard.MaxGroupKeys)
	soft.MaxSetBytes = capNamespaceLimit(soft.MaxSetBytes, hard.MaxSetBytes)
	soft.MaxSpillBytes = capNamespaceLimit(soft.MaxSpillBytes, hard.MaxSpillBytes)
	soft.MaxRecursionDepth = capNamespaceLimit(soft.MaxRecursionDepth, hard.MaxRecursionDepth)
	soft.Timeout = capNamespaceDuration(soft.Timeout, hard.Timeout)
	return soft
}

func capNamespaceLimit(soft, hard int) int {
	if soft > 0 && hard > 0 && soft > hard {
		return hard
	}
	return soft
}

func capNamespaceDuration(soft, hard time.Duration) time.Duration {
	if soft > 0 && hard > 0 && soft > hard {
		return hard
	}
	return soft
}

func tightenNamespaceLimits(defaults, override NamespaceResourceLimits) NamespaceResourceLimits {
	return NamespaceResourceLimits{
		MaxConcurrentQueries: applyPositiveLimit(defaults.MaxConcurrentQueries, override.MaxConcurrentQueries),
		MaxQueuedQueries:     applyPositiveLimit(defaults.MaxQueuedQueries, override.MaxQueuedQueries),
		ComputeWorkers:       applyPositiveLimit(defaults.ComputeWorkers, override.ComputeWorkers),
		ComputeQueueCapacity: applyPositiveLimit(defaults.ComputeQueueCapacity, override.ComputeQueueCapacity),
		MaxQueriesPerWindow:  applyPositiveLimit(defaults.MaxQueriesPerWindow, override.MaxQueriesPerWindow),
		QueryWindow:          tightenQueryWindow(defaults.QueryWindow, override.QueryWindow),
		MaxRows:              applyPositiveLimit(defaults.MaxRows, override.MaxRows),
		MaxIntermediateRows:  applyPositiveLimit(defaults.MaxIntermediateRows, override.MaxIntermediateRows),
		MaxJoinWork:          applyPositiveLimit(defaults.MaxJoinWork, override.MaxJoinWork),
		MaxJoinBytes:         applyPositiveLimit(defaults.MaxJoinBytes, override.MaxJoinBytes),
		MaxResultBytes:       applyPositiveLimit(defaults.MaxResultBytes, override.MaxResultBytes),
		MaxWorkers:           applyPositiveLimit(defaults.MaxWorkers, override.MaxWorkers),
		MaxSortBytes:         applyPositiveLimit(defaults.MaxSortBytes, override.MaxSortBytes),
		MaxGroupBytes:        applyPositiveLimit(defaults.MaxGroupBytes, override.MaxGroupBytes),
		MaxGroupKeys:         applyPositiveLimit(defaults.MaxGroupKeys, override.MaxGroupKeys),
		MaxSetBytes:          applyPositiveLimit(defaults.MaxSetBytes, override.MaxSetBytes),
		MaxSpillBytes:        applyPositiveLimit(defaults.MaxSpillBytes, override.MaxSpillBytes),
		MaxRecursionDepth:    applyPositiveLimit(defaults.MaxRecursionDepth, override.MaxRecursionDepth),
		Timeout:              applyDurationLimit(defaults.Timeout, override.Timeout),
		SpillDirectory:       firstNonEmpty(defaults.SpillDirectory, override.SpillDirectory),
	}
}

func tightenQueryWindow(defaultWindow, overrideWindow time.Duration) time.Duration {
	if overrideWindow <= 0 {
		return defaultWindow
	}
	if defaultWindow <= 0 || overrideWindow < defaultWindow {
		return overrideWindow
	}
	return defaultWindow
}

func firstNonEmpty(first, second string) string {
	if first != "" {
		return first
	}
	return second
}

func (governor *NamespaceQueryGovernor) limitsFor(namespace string) NamespaceResourceLimits {
	if limits, ok := governor.namespaces[namespace]; ok {
		return limits
	}
	return governor.defaults
}

func (governor *NamespaceQueryGovernor) softLimitsFor(namespace string) NamespaceResourceLimits {
	if limits, ok := governor.softNamespaces[namespace]; ok {
		return limits
	}
	return governor.defaultSoft
}

// DryRun returns the static namespace policy that would be applied to a query.
// It does not reserve concurrency, consume quota, submit compute work, or
// otherwise change governor state.
func (governor *NamespaceQueryGovernor) DryRun(namespace string, options SQLQueryOptions) (NamespaceQueryAdmission, error) {
	if governor == nil {
		return NamespaceQueryAdmission{}, fmt.Errorf("namespace query governor is required")
	}
	if strings.TrimSpace(namespace) == "" {
		return NamespaceQueryAdmission{}, fmt.Errorf("namespace is required")
	}
	governor.mu.Lock()
	closed := governor.closed
	governor.mu.Unlock()
	if closed {
		return NamespaceQueryAdmission{}, ErrNamespaceQueryGovernorClosed
	}
	hard := governor.limitsFor(namespace)
	soft := governor.softLimitsFor(namespace)
	effective := hard.Apply(options)
	hardClamps, softWarnings := namespaceQueryAdmissionChanges(options, effective, soft)
	return NamespaceQueryAdmission{
		Namespace:        namespace,
		EffectiveOptions: effective,
		HardLimits:       hard,
		SoftLimits:       soft,
		HardClamps:       hardClamps,
		SoftWarnings:     softWarnings,
	}, nil
}

func namespaceQueryAdmissionChanges(requested, effective SQLQueryOptions, soft NamespaceResourceLimits) ([]string, []string) {
	fields := [...]struct {
		name                       string
		requested, effective, soft int64
	}{
		{"max_rows", int64(requested.MaxRows), int64(effective.MaxRows), int64(soft.MaxRows)},
		{"max_intermediate_rows", int64(requested.MaxIntermediateRows), int64(effective.MaxIntermediateRows), int64(soft.MaxIntermediateRows)},
		{"max_join_work", int64(requested.MaxJoinWork), int64(effective.MaxJoinWork), int64(soft.MaxJoinWork)},
		{"max_join_bytes", int64(requested.MaxJoinBytes), int64(effective.MaxJoinBytes), int64(soft.MaxJoinBytes)},
		{"max_result_bytes", int64(requested.MaxResultBytes), int64(effective.MaxResultBytes), int64(soft.MaxResultBytes)},
		{"workers", int64(requested.Workers), int64(effective.Workers), int64(soft.MaxWorkers)},
		{"max_sort_bytes", int64(requested.MaxSortBytes), int64(effective.MaxSortBytes), int64(soft.MaxSortBytes)},
		{"max_group_bytes", int64(requested.MaxGroupBytes), int64(effective.MaxGroupBytes), int64(soft.MaxGroupBytes)},
		{"max_group_keys", int64(requested.MaxGroupKeys), int64(effective.MaxGroupKeys), int64(soft.MaxGroupKeys)},
		{"max_set_bytes", int64(requested.MaxSetBytes), int64(effective.MaxSetBytes), int64(soft.MaxSetBytes)},
		{"max_spill_bytes", int64(requested.MaxSpillBytes), int64(effective.MaxSpillBytes), int64(soft.MaxSpillBytes)},
		{"max_recursion_depth", int64(requested.MaxRecursionDepth), int64(effective.MaxRecursionDepth), int64(soft.MaxRecursionDepth)},
		{"timeout", int64(requested.Timeout), int64(effective.Timeout), int64(soft.Timeout)},
	}
	var hardClamps, softWarnings []string
	for _, field := range fields {
		if field.requested != field.effective {
			hardClamps = append(hardClamps, field.name)
		}
		if field.requested > 0 && field.soft > 0 && field.requested > field.soft {
			softWarnings = append(softWarnings, field.name)
		}
	}
	if requested.SpillDirectory != effective.SpillDirectory {
		hardClamps = append(hardClamps, "spill_directory")
	}
	return hardClamps, softWarnings
}

func (governor *NamespaceQueryGovernor) gateFor(namespace string, limits NamespaceResourceLimits) *namespaceQueryGate {
	if limits.MaxConcurrentQueries == 0 {
		return nil
	}
	governor.mu.Lock()
	defer governor.mu.Unlock()
	if gate := governor.gates[namespace]; gate != nil {
		return gate
	}
	gate := newNamespaceQueryGate(limits.MaxConcurrentQueries, limits.MaxQueuedQueries)
	governor.gates[namespace] = gate
	return gate
}

func (governor *NamespaceQueryGovernor) computePoolFor(namespace string, limits NamespaceResourceLimits) *hatPipeline.WorkStealingPool {
	if governor == nil || limits.ComputeWorkers <= 0 {
		return nil
	}
	governor.mu.Lock()
	defer governor.mu.Unlock()
	if pool := governor.computePools[namespace]; pool != nil {
		return pool
	}
	return governor.defaultComputePool
}

// Close stops new namespace executions and drains all owned compute pools. It
// is idempotent. A running query must observe its request context for prompt
// shutdown because close drains rather than cancels work.
func (governor *NamespaceQueryGovernor) Close() error {
	if governor == nil {
		return nil
	}
	governor.mu.Lock()
	governor.closed = true
	pools := make([]*hatPipeline.WorkStealingPool, 0, len(governor.computePools)+1)
	if governor.defaultComputePool != nil {
		pools = append(pools, governor.defaultComputePool)
	}
	for _, pool := range governor.computePools {
		if pool != nil {
			pools = append(pools, pool)
		}
	}
	governor.mu.Unlock()
	var firstErr error
	for _, pool := range pools {
		if err := pool.Wait(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Execute runs source under the named namespace's policy. Waiting for a busy
// namespace observes ctx cancellation and never starts a second SQL path.
func (governor *NamespaceQueryGovernor) Execute(ctx context.Context, namespace, source string, resolver SQLSourceResolver, parameters []interface{}, options SQLQueryOptions) (SQLQueryResult, error) {
	if governor == nil {
		return SQLQueryResult{}, fmt.Errorf("namespace query governor is required")
	}
	if strings.TrimSpace(namespace) == "" {
		return SQLQueryResult{}, fmt.Errorf("namespace is required")
	}
	governor.mu.Lock()
	closed := governor.closed
	governor.mu.Unlock()
	if closed {
		return SQLQueryResult{}, ErrNamespaceQueryGovernorClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	limits := governor.limitsFor(namespace)
	gate := governor.gateFor(namespace, limits)
	if gate != nil {
		if err := gate.acquire(ctx); err != nil {
			return SQLQueryResult{}, err
		}
		defer gate.release()
	}
	if quota := governor.quotaFor(namespace, limits); quota != nil && !quota.allow(time.Now()) {
		return SQLQueryResult{}, ErrNamespaceQueryRateLimited
	}
	options = limits.Apply(options)
	if pool := governor.computePoolFor(namespace, limits); pool != nil {
		result, err := executeSQLQueryOnComputePool(pool, ctx, source, resolver, parameters, options)
		if errors.Is(err, hatPipeline.ErrWorkStealingPoolClosed) {
			return SQLQueryResult{}, ErrNamespaceQueryGovernorClosed
		}
		return result, err
	}
	return ExecuteSQLQueryParameters(ctx, source, resolver, parameters, options)
}

func (governor *NamespaceQueryGovernor) quotaFor(namespace string, limits NamespaceResourceLimits) *namespaceQueryQuota {
	if governor == nil || limits.MaxQueriesPerWindow <= 0 {
		return nil
	}
	governor.mu.Lock()
	defer governor.mu.Unlock()
	if quota := governor.quotas[namespace]; quota != nil {
		return quota
	}
	quota := newNamespaceQueryQuota(limits.MaxQueriesPerWindow, limits.QueryWindow)
	governor.quotas[namespace] = quota
	return quota
}

type namespaceQueryQuota struct {
	mu     sync.Mutex
	limit  int
	window time.Duration
	start  time.Time
	used   int
}

func newNamespaceQueryQuota(limit int, window time.Duration) *namespaceQueryQuota {
	if window <= 0 {
		window = defaultNamespaceQueryWindow
	}
	return &namespaceQueryQuota{limit: limit, window: window}
}

func (quota *namespaceQueryQuota) allow(now time.Time) bool {
	if quota == nil || quota.limit <= 0 {
		return true
	}
	quota.mu.Lock()
	defer quota.mu.Unlock()
	if quota.start.IsZero() || now.Before(quota.start) || now.Sub(quota.start) >= quota.window {
		quota.start = now
		quota.used = 0
	}
	if quota.used >= quota.limit {
		return false
	}
	quota.used++
	return true
}

// namespaceQueryGate bounds one namespace while admitting waiters in arrival
// order. A canceled waiter is removed before it can consume a released slot.
type namespaceQueryGate struct {
	mu        sync.Mutex
	capacity  int
	maxQueued int
	running   int
	waiters   []*namespaceQueryWaiter
}

type namespaceQueryWaiter struct {
	ready     chan struct{}
	granted   bool
	cancelled bool
}

func newNamespaceQueryGate(capacity int, maxQueued ...int) *namespaceQueryGate {
	queueLimit := 0
	if len(maxQueued) > 0 {
		queueLimit = maxQueued[0]
	}
	return &namespaceQueryGate{capacity: capacity, maxQueued: queueLimit}
}

func (gate *namespaceQueryGate) acquire(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	gate.mu.Lock()
	if gate.running < gate.capacity && len(gate.waiters) == 0 {
		gate.running++
		gate.mu.Unlock()
		return nil
	}
	if gate.maxQueued > 0 && len(gate.waiters) >= gate.maxQueued {
		gate.mu.Unlock()
		return ErrNamespaceQueryQueueFull
	}
	waiter := &namespaceQueryWaiter{ready: make(chan struct{})}
	gate.waiters = append(gate.waiters, waiter)
	gate.mu.Unlock()

	select {
	case <-waiter.ready:
		if err := ctx.Err(); err != nil {
			gate.release()
			return err
		}
		return nil
	case <-ctx.Done():
		gate.mu.Lock()
		if !waiter.granted {
			waiter.cancelled = true
			gate.removeWaiterLocked(waiter)
			gate.mu.Unlock()
			return ctx.Err()
		}
		gate.mu.Unlock()
		gate.release()
		return ctx.Err()
	}
}

func (gate *namespaceQueryGate) release() {
	gate.mu.Lock()
	defer gate.mu.Unlock()
	for len(gate.waiters) > 0 {
		waiter := gate.waiters[0]
		gate.waiters[0] = nil
		gate.waiters = gate.waiters[1:]
		if waiter.cancelled {
			continue
		}
		waiter.granted = true
		close(waiter.ready)
		return
	}
	gate.running--
}

func (gate *namespaceQueryGate) removeWaiterLocked(target *namespaceQueryWaiter) {
	for index, waiter := range gate.waiters {
		if waiter != target {
			continue
		}
		copy(gate.waiters[index:], gate.waiters[index+1:])
		gate.waiters[len(gate.waiters)-1] = nil
		gate.waiters = gate.waiters[:len(gate.waiters)-1]
		return
	}
}
