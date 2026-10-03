package hatReplication

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

const (
	// DefaultPeerRouteCacheMaxRoutes bounds the route list retained for one key.
	DefaultPeerRouteCacheMaxRoutes = 32
	// DefaultPeerRouteCacheFailureCooldown keeps a failed route out of rotation
	// long enough to avoid retrying a known-unhealthy peer on every request.
	DefaultPeerRouteCacheFailureCooldown = 2 * time.Second
)

var (
	ErrPeerRouteCacheInvalidOptions = errors.New("hatReplication: invalid peer route cache options")
	ErrPeerRouteCacheInvalidRoute   = errors.New("hatReplication: invalid peer route")
	ErrPeerRouteUnavailable         = errors.New("hatReplication: no healthy peer route")
)

// PeerRoute identifies one address that can serve a logical peer target.
// Node must be stable across route refreshes; Address is the dialable endpoint.
type PeerRoute struct {
	Node    string
	Address string
}

// PeerRouteCacheOptions bounds route and retry state. Zero values select the
// bounded defaults. MaxAttempts limits one Do call; zero means all configured
// routes may be attempted once.
type PeerRouteCacheOptions struct {
	MaxRoutes       int
	MaxAttempts     int
	FailureCooldown time.Duration
}

type peerRouteHealth struct {
	failures uint32
	retryAt  time.Time
}

// PeerRouteCache is a concurrency-safe, bounded route cache for caller-owned
// peer requests. It does not open connections or infer health; callers report
// the result of each attempt through Do or ReportSuccess/ReportFailure.
type PeerRouteCache struct {
	mu              sync.Mutex
	maxRoutes       int
	maxAttempts     int
	failureCooldown time.Duration
	routes          map[string][]PeerRoute
	health          map[string]map[string]peerRouteHealth
	cursors         map[string]int
}

// NewPeerRouteCache creates a bounded peer route cache.
func NewPeerRouteCache(options PeerRouteCacheOptions) (*PeerRouteCache, error) {
	if options.MaxRoutes == 0 {
		options.MaxRoutes = DefaultPeerRouteCacheMaxRoutes
	}
	if options.MaxRoutes < 1 || options.MaxRoutes > 4096 {
		return nil, fmt.Errorf("max routes %d: %w", options.MaxRoutes, ErrPeerRouteCacheInvalidOptions)
	}
	if options.MaxAttempts < 0 || options.MaxAttempts > options.MaxRoutes {
		return nil, fmt.Errorf("max attempts %d: %w", options.MaxAttempts, ErrPeerRouteCacheInvalidOptions)
	}
	if options.FailureCooldown == 0 {
		options.FailureCooldown = DefaultPeerRouteCacheFailureCooldown
	}
	if options.FailureCooldown < 0 {
		return nil, fmt.Errorf("failure cooldown %s: %w", options.FailureCooldown, ErrPeerRouteCacheInvalidOptions)
	}
	if options.MaxAttempts == 0 {
		options.MaxAttempts = options.MaxRoutes
	}
	return &PeerRouteCache{
		maxRoutes:       options.MaxRoutes,
		maxAttempts:     options.MaxAttempts,
		failureCooldown: options.FailureCooldown,
		routes:          make(map[string][]PeerRoute),
		health:          make(map[string]map[string]peerRouteHealth),
		cursors:         make(map[string]int),
	}, nil
}

// Replace atomically publishes routes for key and resets health for removed or
// replaced endpoints. A copied route list prevents callers from mutating the
// cache after publication.
func (cache *PeerRouteCache) Replace(key string, routes []PeerRoute) error {
	if cache == nil {
		return fmt.Errorf("nil route cache: %w", ErrPeerRouteCacheInvalidOptions)
	}
	key = strings.TrimSpace(key)
	if key == "" {
		return fmt.Errorf("empty route key: %w", ErrPeerRouteCacheInvalidRoute)
	}
	if len(routes) == 0 || len(routes) > cache.maxRoutes {
		return fmt.Errorf("route count %d for %q: %w", len(routes), key, ErrPeerRouteCacheInvalidRoute)
	}
	cloned := make([]PeerRoute, len(routes))
	seen := make(map[string]struct{}, len(routes))
	for index, route := range routes {
		route.Node = strings.TrimSpace(route.Node)
		route.Address = strings.TrimSpace(route.Address)
		if route.Node == "" || route.Address == "" {
			return fmt.Errorf("route %d for %q: %w", index, key, ErrPeerRouteCacheInvalidRoute)
		}
		if _, exists := seen[route.Node]; exists {
			return fmt.Errorf("duplicate node %q for %q: %w", route.Node, key, ErrPeerRouteCacheInvalidRoute)
		}
		seen[route.Node] = struct{}{}
		cloned[index] = route
	}

	cache.mu.Lock()
	cache.routes[key] = cloned
	delete(cache.health, key)
	cache.cursors[key] = 0
	cache.mu.Unlock()
	return nil
}

// Invalidate removes all routes and health state for key. It reports whether a
// route set existed.
func (cache *PeerRouteCache) Invalidate(key string) bool {
	if cache == nil {
		return false
	}
	key = strings.TrimSpace(key)
	cache.mu.Lock()
	_, existed := cache.routes[key]
	delete(cache.routes, key)
	delete(cache.health, key)
	delete(cache.cursors, key)
	cache.mu.Unlock()
	return existed
}

// Next returns the healthiest available route. A route remains unavailable
// until its failure cooldown expires; ties rotate through the published list.
func (cache *PeerRouteCache) Next(key string, now time.Time) (PeerRoute, bool) {
	return cache.next(key, now, nil)
}

// ReportSuccess clears a route's accumulated failure penalty.
func (cache *PeerRouteCache) ReportSuccess(key, node string) {
	if cache == nil {
		return
	}
	key = strings.TrimSpace(key)
	node = strings.TrimSpace(node)
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if _, ok := cache.routeIndexLocked(key, node); !ok {
		return
	}
	if states := cache.health[key]; states != nil {
		delete(states, node)
		if len(states) == 0 {
			delete(cache.health, key)
		}
	}
}

// ReportFailure marks a route unavailable until its configured cooldown
// expires. Repeated failures retain a penalty so healthy alternatives win ties.
func (cache *PeerRouteCache) ReportFailure(key, node string, now time.Time) {
	if cache == nil {
		return
	}
	key = strings.TrimSpace(key)
	node = strings.TrimSpace(node)
	if now.IsZero() {
		now = time.Now()
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if _, ok := cache.routeIndexLocked(key, node); !ok {
		return
	}
	states := cache.health[key]
	if states == nil {
		states = make(map[string]peerRouteHealth)
		cache.health[key] = states
	}
	state := states[node]
	if state.failures < ^uint32(0) {
		state.failures++
	}
	state.retryAt = now.Add(cache.failureCooldown)
	states[node] = state
}

// Do attempts each currently eligible route at most once, in health order. A
// transport error cools the route and advances to the next candidate. Caller
// cancellation and deadlines are returned without poisoning route health.
func (cache *PeerRouteCache) Do(ctx context.Context, key string, attempt func(context.Context, PeerRoute) error) (PeerRoute, error) {
	if cache == nil {
		return PeerRoute{}, fmt.Errorf("nil route cache: %w", ErrPeerRouteUnavailable)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if attempt == nil {
		return PeerRoute{}, fmt.Errorf("nil route attempt: %w", ErrPeerRouteCacheInvalidOptions)
	}
	attempted := make([]string, 0, cache.maxAttempts)
	var lastErr error
	for len(attempted) < cache.maxAttempts {
		if err := ctx.Err(); err != nil {
			return PeerRoute{}, err
		}
		route, ok := cache.next(key, time.Now(), attempted)
		if !ok {
			break
		}
		attempted = append(attempted, route.Node)
		if err := attempt(ctx, route); err == nil {
			cache.ReportSuccess(key, route.Node)
			return route, nil
		} else {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return PeerRoute{}, err
			}
			lastErr = err
			cache.ReportFailure(key, route.Node, time.Now())
		}
	}
	if lastErr != nil {
		return PeerRoute{}, fmt.Errorf("peer route %q exhausted after %d attempts: %w", strings.TrimSpace(key), len(attempted), lastErr)
	}
	return PeerRoute{}, fmt.Errorf("peer route %q: %w", strings.TrimSpace(key), ErrPeerRouteUnavailable)
}

func (cache *PeerRouteCache) next(key string, now time.Time, attempted []string) (PeerRoute, bool) {
	if cache == nil {
		return PeerRoute{}, false
	}
	key = strings.TrimSpace(key)
	if key == "" {
		return PeerRoute{}, false
	}
	if now.IsZero() {
		now = time.Now()
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	routes := cache.routes[key]
	if len(routes) == 0 {
		return PeerRoute{}, false
	}
	start := cache.cursors[key] % len(routes)
	bestIndex := -1
	var bestHealth peerRouteHealth
	for offset := 0; offset < len(routes); offset++ {
		index := (start + offset) % len(routes)
		route := routes[index]
		if peerRouteSeen(attempted, route.Node) {
			continue
		}
		health := cache.health[key][route.Node]
		if !health.retryAt.IsZero() {
			if now.Before(health.retryAt) {
				continue
			}
			// A cooled route gets one fresh probe instead of carrying a
			// permanent penalty after the peer has had time to recover.
			health.failures = 0
			health.retryAt = time.Time{}
			if states := cache.health[key]; states != nil {
				delete(states, route.Node)
				if len(states) == 0 {
					delete(cache.health, key)
				}
			}
		}
		if bestIndex < 0 || health.failures < bestHealth.failures {
			bestIndex = index
			bestHealth = health
		}
	}
	if bestIndex < 0 {
		return PeerRoute{}, false
	}
	cache.cursors[key] = (bestIndex + 1) % len(routes)
	return routes[bestIndex], true
}

func (cache *PeerRouteCache) routeIndexLocked(key, node string) (int, bool) {
	for index, route := range cache.routes[key] {
		if route.Node == node {
			return index, true
		}
	}
	return 0, false
}

func peerRouteSeen(attempted []string, node string) bool {
	for _, attemptedNode := range attempted {
		if attemptedNode == node {
			return true
		}
	}
	return false
}
