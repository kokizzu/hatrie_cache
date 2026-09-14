package hatSql

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

const (
	// DefaultSkewAwareJoinExchangeHotKeyThreshold is the observed weight at
	// which a join key is promoted to skew-aware routing.
	DefaultSkewAwareJoinExchangeHotKeyThreshold uint64 = 1024
	// DefaultSkewAwareJoinExchangeMaxTrackedKeys bounds cold frequency state.
	DefaultSkewAwareJoinExchangeMaxTrackedKeys = 65536
)

var (
	ErrSkewAwareJoinExchangeNil              = errors.New("hatSql: skew-aware join exchange is nil")
	ErrSkewAwareJoinExchangeWorkersInvalid   = errors.New("hatSql: skew-aware join exchange worker count is invalid")
	ErrSkewAwareJoinExchangeThresholdInvalid = errors.New("hatSql: skew-aware join exchange hot-key threshold is invalid")
	ErrSkewAwareJoinExchangeTrackedInvalid   = errors.New("hatSql: skew-aware join exchange tracked-key limit is invalid")
	ErrSkewAwareJoinExchangeSideInvalid      = errors.New("hatSql: skew-aware join exchange side is invalid")
	ErrSkewAwareJoinExchangeKeyRequired      = errors.New("hatSql: skew-aware join exchange key is required")
	ErrSkewAwareJoinExchangeKeyInvalid       = errors.New("hatSql: skew-aware join exchange key is invalid")
	ErrSkewAwareJoinExchangeSourceRequired   = errors.New("hatSql: skew-aware join exchange source key is required")
	ErrSkewAwareJoinExchangeWeightInvalid    = errors.New("hatSql: skew-aware join exchange observation weight is invalid")
	ErrSkewAwareJoinExchangeRebalanceMissing = errors.New("hatSql: skew-aware join exchange rebalance is missing")
	ErrSkewAwareJoinExchangeGenerationStale  = errors.New("hatSql: skew-aware join exchange generation is stale")
)

// SkewAwareJoinExchangeOptions configures deterministic join-key routing.
// BroadcastSide is the side that is replicated to every worker after a key is
// promoted; zero defaults to IncrementalJoinRight. MaxTrackedKeys bounds cold
// frequency state and does not limit already-promoted hot keys.
type SkewAwareJoinExchangeOptions struct {
	Workers         int
	HotKeyThreshold uint64
	BroadcastSide   IncrementalJoinSide
	MaxTrackedKeys  int
}

// SkewAwareJoinExchangeRoute is the placement decision for one source update.
// Worker is -1 when Broadcast is true. Workers returns an independent worker
// list for callers that need to fan out a broadcast record.
type SkewAwareJoinExchangeRoute struct {
	Worker            int
	Broadcast         bool
	Hot               bool
	Generation        uint64
	RebalanceRequired bool
}

// Workers returns the worker set represented by route. A non-broadcast route
// returns one worker; a broadcast route returns [0, workerCount).
func (route SkewAwareJoinExchangeRoute) Workers(workerCount int) []int {
	if workerCount <= 0 {
		return nil
	}
	if !route.Broadcast {
		if route.Worker < 0 || route.Worker >= workerCount {
			return nil
		}
		return []int{route.Worker}
	}
	workers := make([]int, workerCount)
	for worker := range workers {
		workers[worker] = worker
	}
	return workers
}

// SkewAwareJoinExchangeHotKey is the immutable status of one promoted key.
type SkewAwareJoinExchangeHotKey struct {
	Key               string
	Weight            uint64
	Generation        uint64
	RebalanceRequired bool
}

// SkewAwareJoinExchangeSnapshot is a deterministic operator snapshot.
type SkewAwareJoinExchangeSnapshot struct {
	Generation uint64
	HotKeys    []SkewAwareJoinExchangeHotKey
}

type skewAwareJoinExchangeState struct {
	weight            uint64
	hot               bool
	generation        uint64
	rebalanceRequired bool
}

type skewAwareJoinExchangeHotRoute struct {
	generation        uint64
	rebalanceRequired bool
}

// SkewAwareJoinExchange detects heavy join keys and routes their build-side
// rows to every worker while partitioning probe-side rows by source key. Cold
// keys continue to use one deterministic join-key owner. It is a placement
// primitive; callers must rehydrate a promoted key at the returned generation
// before relying on the new route.
type SkewAwareJoinExchange struct {
	mu              sync.RWMutex
	workers         int
	hotKeyThreshold uint64
	broadcastSide   IncrementalJoinSide
	maxTrackedKeys  int
	generation      uint64
	states          map[string]*skewAwareJoinExchangeState
	hotRouting      atomic.Value
}

// NewSkewAwareJoinExchange creates a deterministic exchange policy.
func NewSkewAwareJoinExchange(options SkewAwareJoinExchangeOptions) (*SkewAwareJoinExchange, error) {
	if options.Workers <= 0 {
		return nil, ErrSkewAwareJoinExchangeWorkersInvalid
	}
	threshold := options.HotKeyThreshold
	if threshold == 0 {
		threshold = DefaultSkewAwareJoinExchangeHotKeyThreshold
	}
	if threshold == 0 {
		return nil, ErrSkewAwareJoinExchangeThresholdInvalid
	}
	maxTrackedKeys := options.MaxTrackedKeys
	if maxTrackedKeys == 0 {
		maxTrackedKeys = DefaultSkewAwareJoinExchangeMaxTrackedKeys
	}
	if maxTrackedKeys < 0 {
		return nil, ErrSkewAwareJoinExchangeTrackedInvalid
	}
	broadcastSide := options.BroadcastSide
	if broadcastSide == 0 {
		broadcastSide = IncrementalJoinRight
	}
	if broadcastSide != IncrementalJoinLeft && broadcastSide != IncrementalJoinRight {
		return nil, ErrSkewAwareJoinExchangeSideInvalid
	}
	exchange := &SkewAwareJoinExchange{
		workers:         options.Workers,
		hotKeyThreshold: threshold,
		broadcastSide:   broadcastSide,
		maxTrackedKeys:  maxTrackedKeys,
		states:          make(map[string]*skewAwareJoinExchangeState),
	}
	exchange.hotRouting.Store(map[string]skewAwareJoinExchangeHotRoute{})
	return exchange, nil
}

// Observe adds weight to a join-key frequency. It returns true only when the
// key crosses the threshold and a new routing generation is created. Cold
// keys beyond MaxTrackedKeys are ignored unless one observation already
// reaches the threshold; this keeps the detector bounded without making the
// route function allocate or scan all known keys.
func (exchange *SkewAwareJoinExchange) Observe(joinKey string, weight uint64) (bool, error) {
	if exchange == nil {
		return false, ErrSkewAwareJoinExchangeNil
	}
	if err := validateSkewAwareJoinExchangeKey(joinKey); err != nil {
		return false, err
	}
	if weight == 0 {
		return false, ErrSkewAwareJoinExchangeWeightInvalid
	}
	exchange.mu.Lock()
	defer exchange.mu.Unlock()
	state := exchange.states[joinKey]
	if state == nil {
		if len(exchange.states) >= exchange.maxTrackedKeys && weight < exchange.hotKeyThreshold {
			return false, nil
		}
		state = &skewAwareJoinExchangeState{}
		exchange.states[joinKey] = state
	}
	if math.MaxUint64-state.weight < weight {
		state.weight = math.MaxUint64
	} else {
		state.weight += weight
	}
	if state.hot || state.weight < exchange.hotKeyThreshold {
		return false, nil
	}
	state.hot = true
	exchange.generation++
	state.generation = exchange.generation
	state.rebalanceRequired = true
	exchange.publishHotRoutingLocked()
	return true, nil
}

// Route returns a placement decision for one source row without allocating.
// Both keys are required so hot probe rows can spread by source identity while
// cold rows remain owned by their equality key.
func (exchange *SkewAwareJoinExchange) Route(side IncrementalJoinSide, joinKey, sourceKey string) (SkewAwareJoinExchangeRoute, error) {
	if exchange == nil {
		return SkewAwareJoinExchangeRoute{}, ErrSkewAwareJoinExchangeNil
	}
	if side != IncrementalJoinLeft && side != IncrementalJoinRight {
		return SkewAwareJoinExchangeRoute{}, ErrSkewAwareJoinExchangeSideInvalid
	}
	if err := validateSkewAwareJoinExchangeKey(joinKey); err != nil {
		return SkewAwareJoinExchangeRoute{}, err
	}
	if sourceKey == "" {
		return SkewAwareJoinExchangeRoute{}, ErrSkewAwareJoinExchangeSourceRequired
	}
	if strings.IndexByte(sourceKey, 0) >= 0 {
		return SkewAwareJoinExchangeRoute{}, ErrSkewAwareJoinExchangeKeyInvalid
	}
	hotRouting := exchange.hotRouting.Load().(map[string]skewAwareJoinExchangeHotRoute)
	state, hot := hotRouting[joinKey]
	route := SkewAwareJoinExchangeRoute{Worker: int(skewAwareJoinExchangeHash(joinKey) % uint64(exchange.workers))}
	if !hot {
		return route, nil
	}
	route.Hot = true
	route.Generation = state.generation
	route.RebalanceRequired = state.rebalanceRequired
	if side == exchange.broadcastSide {
		route.Worker = -1
		route.Broadcast = true
	} else {
		route.Worker = int(skewAwareJoinExchangeHash(sourceKey) % uint64(exchange.workers))
	}
	return route, nil
}

// AcknowledgeRebalance clears the promotion fence for joinKey at its exact
// generation. A stale acknowledgement cannot clear a newer placement plan.
func (exchange *SkewAwareJoinExchange) AcknowledgeRebalance(joinKey string, generation uint64) error {
	if exchange == nil {
		return ErrSkewAwareJoinExchangeNil
	}
	if err := validateSkewAwareJoinExchangeKey(joinKey); err != nil {
		return err
	}
	exchange.mu.Lock()
	defer exchange.mu.Unlock()
	state := exchange.states[joinKey]
	if state == nil || !state.hot {
		return fmt.Errorf("%w: key %q", ErrSkewAwareJoinExchangeRebalanceMissing, joinKey)
	}
	if generation != state.generation {
		return fmt.Errorf("%w: key %q got %d want %d", ErrSkewAwareJoinExchangeGenerationStale, joinKey, generation, state.generation)
	}
	state.rebalanceRequired = false
	exchange.publishHotRoutingLocked()
	return nil
}

// Snapshot returns hot-key status in key order and copies no internal slices.
func (exchange *SkewAwareJoinExchange) Snapshot() SkewAwareJoinExchangeSnapshot {
	if exchange == nil {
		return SkewAwareJoinExchangeSnapshot{}
	}
	exchange.mu.RLock()
	defer exchange.mu.RUnlock()
	keys := make([]string, 0)
	for key, state := range exchange.states {
		if state.hot {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	snapshot := SkewAwareJoinExchangeSnapshot{Generation: exchange.generation, HotKeys: make([]SkewAwareJoinExchangeHotKey, 0, len(keys))}
	for _, key := range keys {
		state := exchange.states[key]
		snapshot.HotKeys = append(snapshot.HotKeys, SkewAwareJoinExchangeHotKey{
			Key: key, Weight: state.weight, Generation: state.generation, RebalanceRequired: state.rebalanceRequired,
		})
	}
	return snapshot
}

func (exchange *SkewAwareJoinExchange) publishHotRoutingLocked() {
	routing := make(map[string]skewAwareJoinExchangeHotRoute)
	for key, state := range exchange.states {
		if state.hot {
			routing[key] = skewAwareJoinExchangeHotRoute{generation: state.generation, rebalanceRequired: state.rebalanceRequired}
		}
	}
	exchange.hotRouting.Store(routing)
}

func validateSkewAwareJoinExchangeKey(key string) error {
	if key == "" {
		return ErrSkewAwareJoinExchangeKeyRequired
	}
	if strings.IndexByte(key, 0) >= 0 {
		return ErrSkewAwareJoinExchangeKeyInvalid
	}
	return nil
}

func skewAwareJoinExchangeHash(value string) uint64 {
	hash := uint64(14695981039346656037)
	for index := 0; index < len(value); index++ {
		hash ^= uint64(value[index])
		hash *= 1099511628211
	}
	return hash
}
