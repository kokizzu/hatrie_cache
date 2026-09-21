package hatReplication

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// DefaultFailoverRouteCacheMaxRoutes bounds the default route snapshot.
	DefaultFailoverRouteCacheMaxRoutes = 1024
	// MaxFailoverRouteCacheMaxRoutes prevents unbounded route snapshots.
	MaxFailoverRouteCacheMaxRoutes = 65536
	// DefaultFailoverRouteCacheFailureThreshold suppresses a route after one
	// reported failure unless the caller configures a higher threshold.
	DefaultFailoverRouteCacheFailureThreshold = 1
	// MaxFailoverRouteCacheFailureThreshold bounds repeated failure state.
	MaxFailoverRouteCacheFailureThreshold = 1 << 20
	// DefaultFailoverRouteCacheCooldown is the health suppression interval.
	DefaultFailoverRouteCacheCooldown = 5 * time.Second
	// MaxFailoverRouteCacheCooldown prevents an accidental permanent route ban.
	MaxFailoverRouteCacheCooldown = 7 * 24 * time.Hour
	// MaxFailoverRouteCacheNodeBytes bounds one node ID.
	MaxFailoverRouteCacheNodeBytes = 256
	// MaxFailoverRouteCacheAddressBytes bounds one endpoint address.
	MaxFailoverRouteCacheAddressBytes = 1024
)

var (
	// ErrFailoverRouteCacheNil reports a nil cache receiver.
	ErrFailoverRouteCacheNil = errors.New("hatReplication: failover route cache is nil")
	// ErrFailoverRouteCacheInvalid reports malformed options or routes.
	ErrFailoverRouteCacheInvalid = errors.New("hatReplication: failover route cache input is invalid")
	// ErrFailoverRouteCacheGeneration reports a stale topology replacement.
	ErrFailoverRouteCacheGeneration = errors.New("hatReplication: failover route cache generation is stale")
	// ErrFailoverRouteCacheEmpty reports a lookup with no configured routes.
	ErrFailoverRouteCacheEmpty = errors.New("hatReplication: failover route cache is empty")
	// ErrFailoverRouteCacheNoHealthyRoute reports that every route is suppressed.
	ErrFailoverRouteCacheNoHealthyRoute = errors.New("hatReplication: failover route cache has no healthy route")
	// ErrFailoverRouteCacheNodeNotFound reports an update for an unknown node.
	ErrFailoverRouteCacheNodeNotFound = errors.New("hatReplication: failover route cache node is not found")
)

// FailoverRouteCacheOptions bounds an in-memory route cache. Zero values use
// the documented defaults. Route lookup is always opt-in; existing callers
// that do not construct this cache retain their current routing behavior.
type FailoverRouteCacheOptions struct {
	MaxRoutes        int
	FailureThreshold int
	Cooldown         time.Duration
}

type failoverRouteCacheOptions struct {
	maxRoutes        int
	failureThreshold int
	cooldown         time.Duration
}

// FailoverRoute identifies one client/router destination. NodeID is the
// stable topology identity; Address is the transport endpoint.
type FailoverRoute struct {
	NodeID  string
	Address string
}

// FailoverRouteStatus is a monitoring snapshot for one route.
type FailoverRouteStatus struct {
	Route          FailoverRoute
	Healthy        bool
	Failures       int
	UnhealthyUntil time.Time
}

// FailoverRouteCacheSnapshot is a detached route and health snapshot.
type FailoverRouteCacheSnapshot struct {
	Generation uint64
	Routes     []FailoverRouteStatus
}

type failoverRouteCacheEntry struct {
	route          FailoverRoute
	failures       int
	unhealthyUntil time.Time
}

type failoverRouteCacheState struct {
	generation uint64
	routes     []failoverRouteCacheEntry
}

// FailoverRouteCache is a health-aware client/router route cache. Lookups use
// an immutable atomic snapshot and do not take a mutex or allocate. Topology
// replacement and health reports copy the bounded snapshot on the control
// path, so a slow or failing endpoint cannot block normal route selection.
type FailoverRouteCache struct {
	options  failoverRouteCacheOptions
	updateMu sync.Mutex
	state    atomic.Pointer[failoverRouteCacheState]
}

// NewFailoverRouteCache creates an empty route cache.
func NewFailoverRouteCache(options FailoverRouteCacheOptions) (*FailoverRouteCache, error) {
	normalized, err := normalizeFailoverRouteCacheOptions(options)
	if err != nil {
		return nil, err
	}
	cache := &FailoverRouteCache{options: normalized}
	cache.state.Store(&failoverRouteCacheState{})
	return cache, nil
}

// Replace atomically publishes a strictly newer topology generation. Routes
// with the same node and address retain their current health state; changed
// endpoints start healthy. A caller can publish an empty route set while a
// topology update is in progress.
func (cache *FailoverRouteCache) Replace(generation uint64, routes []FailoverRoute) error {
	if cache == nil {
		return ErrFailoverRouteCacheNil
	}
	normalized, err := normalizeFailoverRoutes(routes, cache.options.maxRoutes)
	if err != nil {
		return err
	}
	cache.updateMu.Lock()
	defer cache.updateMu.Unlock()
	current := cache.state.Load()
	if generation <= current.generation {
		return fmt.Errorf("%w: current=%d received=%d", ErrFailoverRouteCacheGeneration, current.generation, generation)
	}
	next := &failoverRouteCacheState{generation: generation, routes: normalized}
	for index := range next.routes {
		for _, previous := range current.routes {
			if next.routes[index].route == previous.route {
				next.routes[index].failures = previous.failures
				next.routes[index].unhealthyUntil = previous.unhealthyUntil
				break
			}
		}
	}
	cache.state.Store(next)
	return nil
}

// Lookup selects a stable route for key, skipping routes currently inside
// their failure cooldown. A zero time uses the local clock; callers that need
// deterministic behavior should pass an explicit time.
func (cache *FailoverRouteCache) Lookup(key string, now time.Time) (FailoverRoute, error) {
	if cache == nil {
		return FailoverRoute{}, ErrFailoverRouteCacheNil
	}
	if now.IsZero() {
		now = time.Now()
	}
	state := cache.state.Load()
	if len(state.routes) == 0 {
		return FailoverRoute{}, ErrFailoverRouteCacheEmpty
	}
	start := int(failoverRouteHash(key) % uint64(len(state.routes)))
	for offset := 0; offset < len(state.routes); offset++ {
		entry := state.routes[(start+offset)%len(state.routes)]
		if failoverRouteHealthy(entry, now) {
			return entry.route, nil
		}
	}
	return FailoverRoute{}, ErrFailoverRouteCacheNoHealthyRoute
}

// ReportFailure suppresses nodeID after the configured failure threshold.
// The update is copy-on-write and safe to call concurrently with lookups.
func (cache *FailoverRouteCache) ReportFailure(nodeID string, now time.Time) error {
	if now.IsZero() {
		now = time.Now()
	}
	return cache.updateNode(nodeID, func(entry *failoverRouteCacheEntry) {
		if entry.failures < cache.options.failureThreshold {
			entry.failures++
		}
		if entry.failures >= cache.options.failureThreshold {
			entry.unhealthyUntil = now.Add(cache.options.cooldown)
		}
	})
}

// ReportSuccess clears failure state for nodeID and makes it immediately
// eligible for subsequent lookups.
func (cache *FailoverRouteCache) ReportSuccess(nodeID string) error {
	return cache.updateNode(nodeID, func(entry *failoverRouteCacheEntry) {
		entry.failures = 0
		entry.unhealthyUntil = time.Time{}
	})
}

// Invalidate immediately suppresses nodeID for the configured cooldown.
func (cache *FailoverRouteCache) Invalidate(nodeID string, now time.Time) error {
	if now.IsZero() {
		now = time.Now()
	}
	return cache.updateNode(nodeID, func(entry *failoverRouteCacheEntry) {
		entry.failures = cache.options.failureThreshold
		entry.unhealthyUntil = now.Add(cache.options.cooldown)
	})
}

// Generation returns the latest published topology generation.
func (cache *FailoverRouteCache) Generation() uint64 {
	if cache == nil {
		return 0
	}
	return cache.state.Load().generation
}

// Snapshot returns detached route health state at now. A zero time uses the
// local clock.
func (cache *FailoverRouteCache) Snapshot(now time.Time) FailoverRouteCacheSnapshot {
	if cache == nil {
		return FailoverRouteCacheSnapshot{}
	}
	if now.IsZero() {
		now = time.Now()
	}
	state := cache.state.Load()
	snapshot := FailoverRouteCacheSnapshot{
		Generation: state.generation,
		Routes:     make([]FailoverRouteStatus, len(state.routes)),
	}
	for index, entry := range state.routes {
		snapshot.Routes[index] = FailoverRouteStatus{
			Route:          entry.route,
			Healthy:        failoverRouteHealthy(entry, now),
			Failures:       entry.failures,
			UnhealthyUntil: entry.unhealthyUntil,
		}
	}
	return snapshot
}

func (cache *FailoverRouteCache) updateNode(nodeID string, update func(*failoverRouteCacheEntry)) error {
	if cache == nil {
		return ErrFailoverRouteCacheNil
	}
	nodeID = strings.TrimSpace(nodeID)
	if nodeID == "" {
		return ErrFailoverRouteCacheNodeNotFound
	}
	cache.updateMu.Lock()
	defer cache.updateMu.Unlock()
	current := cache.state.Load()
	index := -1
	for routeIndex := range current.routes {
		if current.routes[routeIndex].route.NodeID == nodeID {
			index = routeIndex
			break
		}
	}
	if index < 0 {
		return ErrFailoverRouteCacheNodeNotFound
	}
	next := cloneFailoverRouteCacheState(current)
	update(&next.routes[index])
	cache.state.Store(next)
	return nil
}

func normalizeFailoverRouteCacheOptions(options FailoverRouteCacheOptions) (failoverRouteCacheOptions, error) {
	if options.MaxRoutes == 0 {
		options.MaxRoutes = DefaultFailoverRouteCacheMaxRoutes
	}
	if options.FailureThreshold == 0 {
		options.FailureThreshold = DefaultFailoverRouteCacheFailureThreshold
	}
	if options.Cooldown == 0 {
		options.Cooldown = DefaultFailoverRouteCacheCooldown
	}
	if options.MaxRoutes < 1 || options.MaxRoutes > MaxFailoverRouteCacheMaxRoutes || options.FailureThreshold < 1 || options.FailureThreshold > MaxFailoverRouteCacheFailureThreshold || options.Cooldown < 0 || options.Cooldown > MaxFailoverRouteCacheCooldown {
		return failoverRouteCacheOptions{}, ErrFailoverRouteCacheInvalid
	}
	return failoverRouteCacheOptions{
		maxRoutes:        options.MaxRoutes,
		failureThreshold: options.FailureThreshold,
		cooldown:         options.Cooldown,
	}, nil
}

func normalizeFailoverRoutes(routes []FailoverRoute, maxRoutes int) ([]failoverRouteCacheEntry, error) {
	if len(routes) > maxRoutes {
		return nil, fmt.Errorf("%w: route count exceeds %d", ErrFailoverRouteCacheInvalid, maxRoutes)
	}
	entries := make([]failoverRouteCacheEntry, len(routes))
	seen := make(map[string]struct{}, len(routes))
	for index, route := range routes {
		nodeID := strings.TrimSpace(route.NodeID)
		address := strings.TrimSpace(route.Address)
		if nodeID == "" || len(nodeID) > MaxFailoverRouteCacheNodeBytes || strings.IndexByte(nodeID, 0) >= 0 || address == "" || len(address) > MaxFailoverRouteCacheAddressBytes || strings.IndexByte(address, 0) >= 0 {
			return nil, fmt.Errorf("%w: route %d is invalid", ErrFailoverRouteCacheInvalid, index)
		}
		if _, exists := seen[nodeID]; exists {
			return nil, fmt.Errorf("%w: duplicate node %q", ErrFailoverRouteCacheInvalid, nodeID)
		}
		seen[nodeID] = struct{}{}
		entries[index].route = FailoverRoute{NodeID: nodeID, Address: address}
	}
	sort.Slice(entries, func(left, right int) bool {
		return entries[left].route.NodeID < entries[right].route.NodeID
	})
	return entries, nil
}

func cloneFailoverRouteCacheState(state *failoverRouteCacheState) *failoverRouteCacheState {
	clone := &failoverRouteCacheState{
		generation: state.generation,
		routes:     make([]failoverRouteCacheEntry, len(state.routes)),
	}
	copy(clone.routes, state.routes)
	return clone
}

func failoverRouteHealthy(entry failoverRouteCacheEntry, now time.Time) bool {
	return entry.unhealthyUntil.IsZero() || !now.Before(entry.unhealthyUntil)
}

func failoverRouteHash(key string) uint64 {
	const offset = uint64(14695981039346656037)
	const prime = uint64(1099511628211)
	hash := offset
	for index := 0; index < len(key); index++ {
		hash ^= uint64(key[index])
		hash *= prime
	}
	return hash
}
