package hatSql

import (
	"errors"
	"math"
	"sync"
)

const (
	// DefaultGlobalJoinBroadcastMaxRows is the default row threshold for one
	// materialized global subquery result.
	DefaultGlobalJoinBroadcastMaxRows = 65536
	// DefaultGlobalJoinBroadcastMaxBytes is the default payload threshold for
	// one materialized global subquery result.
	DefaultGlobalJoinBroadcastMaxBytes int64 = 64 << 20
	// DefaultGlobalJoinBroadcastMaxCachedPlans bounds reusable result metadata.
	DefaultGlobalJoinBroadcastMaxCachedPlans = 4096
)

var (
	ErrGlobalJoinBroadcastPlannerNil             = errors.New("hatSql: global join broadcast planner is nil")
	ErrGlobalJoinBroadcastPlannerWorkersInvalid  = errors.New("hatSql: global join broadcast planner worker count is invalid")
	ErrGlobalJoinBroadcastPlannerOptionsInvalid  = errors.New("hatSql: global join broadcast planner options are invalid")
	ErrGlobalJoinBroadcastPlannerRequestInvalid  = errors.New("hatSql: global join broadcast planner request is invalid")
	ErrGlobalJoinBroadcastPlannerPayloadOverflow = errors.New("hatSql: global join broadcast planner fanout bytes overflow")
)

// GlobalJoinBroadcastPlannerOptions bounds global subquery materialization.
// Workers is required. Zero thresholds select conservative defaults.
type GlobalJoinBroadcastPlannerOptions struct {
	Workers           int
	MaxBroadcastRows  int
	MaxBroadcastBytes int64
	MaxCachedPlans    int
}

// GlobalJoinBroadcastRequest identifies one snapshot-consistent global
// subquery result. Fingerprint should identify the normalized subquery and
// Epoch must change whenever the source snapshot can change.
type GlobalJoinBroadcastRequest struct {
	Fingerprint string
	Epoch       uint64
	Rows        int
	Bytes       int64
}

// GlobalJoinBroadcastMode is the placement strategy for a global result.
type GlobalJoinBroadcastMode uint8

const (
	// GlobalJoinBroadcastModePerWorker asks each worker to execute or fetch the
	// subquery locally because the result is outside the broadcast bounds.
	GlobalJoinBroadcastModePerWorker GlobalJoinBroadcastMode = iota
	// GlobalJoinBroadcastModeBroadcast materializes one result and fans it out.
	GlobalJoinBroadcastModeBroadcast
)

// GlobalJoinBroadcastPlan is a caller-facing accounting decision. It does not
// perform network I/O or retain the result payload.
type GlobalJoinBroadcastPlan struct {
	Mode                     GlobalJoinBroadcastMode
	CacheHit                 bool
	RemoteSubqueryExecutions int
	WorkerCopies             int
	Rows                     int
	Bytes                    int64
	BroadcastBytes           int64
}

// GlobalJoinBroadcastPlanner plans bounded reuse of GLOBAL IN/GLOBAL JOIN
// subquery results. It is safe for concurrent callers and keeps only bounded
// fingerprint/epoch metadata, never the result payload itself.
type GlobalJoinBroadcastPlanner struct {
	mu                sync.Mutex
	workers           int
	maxBroadcastRows  int
	maxBroadcastBytes int64
	maxCachedPlans    int
	cache             map[globalJoinBroadcastCacheKey]struct{}
	order             []globalJoinBroadcastCacheKey
	head              int
}

type globalJoinBroadcastCacheKey struct {
	fingerprint string
	epoch       uint64
	rows        int
	bytes       int64
}

// NewGlobalJoinBroadcastPlanner creates a bounded planner.
func NewGlobalJoinBroadcastPlanner(options GlobalJoinBroadcastPlannerOptions) (*GlobalJoinBroadcastPlanner, error) {
	if options.Workers <= 0 {
		return nil, ErrGlobalJoinBroadcastPlannerWorkersInvalid
	}
	if options.MaxBroadcastRows < 0 || options.MaxBroadcastBytes < 0 || options.MaxCachedPlans < 0 {
		return nil, ErrGlobalJoinBroadcastPlannerOptionsInvalid
	}
	maxRows := options.MaxBroadcastRows
	if maxRows == 0 {
		maxRows = DefaultGlobalJoinBroadcastMaxRows
	}
	maxBytes := options.MaxBroadcastBytes
	if maxBytes == 0 {
		maxBytes = DefaultGlobalJoinBroadcastMaxBytes
	}
	maxCachedPlans := options.MaxCachedPlans
	if maxCachedPlans == 0 {
		maxCachedPlans = DefaultGlobalJoinBroadcastMaxCachedPlans
	}
	return &GlobalJoinBroadcastPlanner{
		workers:           options.Workers,
		maxBroadcastRows:  maxRows,
		maxBroadcastBytes: maxBytes,
		maxCachedPlans:    maxCachedPlans,
		cache:             make(map[globalJoinBroadcastCacheKey]struct{}),
	}, nil
}

// Plan decides whether one source result can be reused by all workers. A
// cache miss on an eligible request accounts for one remote subquery execution;
// a cache hit accounts for none. The caller remains responsible for fetching,
// publishing, and invalidating the actual result.
func (planner *GlobalJoinBroadcastPlanner) Plan(request GlobalJoinBroadcastRequest) (GlobalJoinBroadcastPlan, error) {
	if planner == nil {
		return GlobalJoinBroadcastPlan{}, ErrGlobalJoinBroadcastPlannerNil
	}
	if request.Fingerprint == "" || request.Rows < 0 || request.Bytes < 0 {
		return GlobalJoinBroadcastPlan{}, ErrGlobalJoinBroadcastPlannerRequestInvalid
	}
	plan := GlobalJoinBroadcastPlan{
		Mode:  GlobalJoinBroadcastModePerWorker,
		Rows:  request.Rows,
		Bytes: request.Bytes,
	}
	if request.Rows > planner.maxBroadcastRows || request.Bytes > planner.maxBroadcastBytes {
		plan.RemoteSubqueryExecutions = planner.workers
		return plan, nil
	}
	if request.Bytes > math.MaxInt64/int64(planner.workers) {
		return GlobalJoinBroadcastPlan{}, ErrGlobalJoinBroadcastPlannerPayloadOverflow
	}
	plan.Mode = GlobalJoinBroadcastModeBroadcast
	plan.WorkerCopies = planner.workers
	plan.BroadcastBytes = request.Bytes * int64(planner.workers)
	cacheKey := globalJoinBroadcastCacheKey{
		fingerprint: request.Fingerprint,
		epoch:       request.Epoch,
		rows:        request.Rows,
		bytes:       request.Bytes,
	}
	planner.mu.Lock()
	defer planner.mu.Unlock()
	if _, ok := planner.cache[cacheKey]; ok {
		plan.CacheHit = true
		return plan, nil
	}
	plan.RemoteSubqueryExecutions = 1
	planner.cache[cacheKey] = struct{}{}
	planner.order = append(planner.order, cacheKey)
	planner.evictLocked()
	return plan, nil
}

// Invalidate removes all cached epochs and shapes for fingerprint and returns
// the number of metadata entries removed. It does not touch caller payloads.
func (planner *GlobalJoinBroadcastPlanner) Invalidate(fingerprint string) int {
	if planner == nil || fingerprint == "" {
		return 0
	}
	planner.mu.Lock()
	defer planner.mu.Unlock()
	removed := 0
	for key := range planner.cache {
		if key.fingerprint == fingerprint {
			delete(planner.cache, key)
			removed++
		}
	}
	planner.compactOrderLocked()
	return removed
}

// CachedPlans reports the number of retained broadcast metadata entries.
func (planner *GlobalJoinBroadcastPlanner) CachedPlans() int {
	if planner == nil {
		return 0
	}
	planner.mu.Lock()
	defer planner.mu.Unlock()
	return len(planner.cache)
}

func (planner *GlobalJoinBroadcastPlanner) evictLocked() {
	for len(planner.cache) > planner.maxCachedPlans && planner.head < len(planner.order) {
		key := planner.order[planner.head]
		planner.head++
		if _, ok := planner.cache[key]; ok {
			delete(planner.cache, key)
		}
	}
	planner.compactOrderLocked()
}

func (planner *GlobalJoinBroadcastPlanner) compactOrderLocked() {
	if planner.head == 0 && len(planner.order) < 1024 {
		return
	}
	if planner.head > 0 && planner.head < 1024 && planner.head*2 < len(planner.order) {
		return
	}
	order := planner.order[:0]
	for index := planner.head; index < len(planner.order); index++ {
		key := planner.order[index]
		if _, ok := planner.cache[key]; ok {
			order = append(order, key)
		}
	}
	planner.order = order
	planner.head = 0
}
