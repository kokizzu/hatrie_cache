package hatDataStructure

import (
	"errors"
	"sync"
)

var (
	// ErrFrontierReadHoldInvalidBounds reports a since frontier above its upper.
	ErrFrontierReadHoldInvalidBounds = errors.New("frontier read hold has invalid bounds")
	// ErrFrontierReadHoldRegression reports an attempt to move a hold backwards.
	ErrFrontierReadHoldRegression = errors.New("frontier read hold moved backwards")
	// ErrFrontierReadHoldReleased reports use of a released or uninitialized hold.
	ErrFrontierReadHoldReleased = errors.New("frontier read hold is released")
	// ErrFrontierReadHoldSetNil reports an operation on a nil hold set.
	ErrFrontierReadHoldSetNil = errors.New("frontier read hold set is nil")
	// ErrFrontierReadHoldIDExhausted reports exhaustion of hold identifiers.
	ErrFrontierReadHoldIDExhausted = errors.New("frontier read hold identifiers exhausted")
)

// FrontierReadHoldSet tracks scalar since/upper bounds for readers that need
// a stable logical view. Since is the inclusive oldest point a reader may
// still request; upper is the inclusive newest point currently visible to it.
// The set does not own the underlying data and can be used by storage or
// incremental-query layers independently.
type FrontierReadHoldSet struct {
	mu     sync.RWMutex
	nextID uint64
	holds  map[uint64]uint64
}

// FrontierReadHold is an independent lease registered in a hold set. Always
// release a hold when its reader is finished; Release is idempotent.
type FrontierReadHold struct {
	set      *FrontierReadHoldSet
	id       uint64
	upper    uint64
	released bool
}

// NewFrontierReadHoldSet returns an empty hold set. The map is allocated only
// when the first hold is acquired.
func NewFrontierReadHoldSet() *FrontierReadHoldSet {
	return &FrontierReadHoldSet{}
}

// Acquire registers an inclusive [since, upper] read interval.
func (set *FrontierReadHoldSet) Acquire(since, upper uint64) (*FrontierReadHold, error) {
	if set == nil {
		return nil, ErrFrontierReadHoldSetNil
	}
	if since > upper {
		return nil, ErrFrontierReadHoldInvalidBounds
	}
	set.mu.Lock()
	defer set.mu.Unlock()
	if set.nextID == ^uint64(0) {
		return nil, ErrFrontierReadHoldIDExhausted
	}
	set.nextID++
	if set.holds == nil {
		set.holds = make(map[uint64]uint64)
	}
	hold := &FrontierReadHold{set: set, id: set.nextID, upper: upper}
	set.holds[hold.id] = since
	return hold, nil
}

// ID returns the stable identifier assigned to the hold. It remains useful in
// logs after Release.
func (hold *FrontierReadHold) ID() uint64 {
	if hold == nil {
		return 0
	}
	return hold.id
}

// Since returns the current inclusive lower bound. A released hold returns 0.
func (hold *FrontierReadHold) Since() uint64 {
	if hold == nil || hold.set == nil {
		return 0
	}
	hold.set.mu.RLock()
	defer hold.set.mu.RUnlock()
	if hold.released {
		return 0
	}
	return hold.set.holds[hold.id]
}

// Upper returns the current inclusive upper bound. A released hold returns 0.
func (hold *FrontierReadHold) Upper() uint64 {
	if hold == nil || hold.set == nil {
		return 0
	}
	hold.set.mu.RLock()
	defer hold.set.mu.RUnlock()
	if hold.released {
		return 0
	}
	return hold.upper
}

// Allows reports whether sequence is inside the current read interval.
func (hold *FrontierReadHold) Allows(sequence uint64) bool {
	if hold == nil || hold.set == nil {
		return false
	}
	hold.set.mu.RLock()
	defer hold.set.mu.RUnlock()
	if hold.released {
		return false
	}
	since := hold.set.holds[hold.id]
	return since <= sequence && sequence <= hold.upper
}

// Advance moves both frontiers forward. Neither bound may regress, and the
// interval must remain valid. It is safe to call concurrently with queries of
// the hold or with other holds in the same set.
func (hold *FrontierReadHold) Advance(since, upper uint64) error {
	if hold == nil || hold.set == nil {
		return ErrFrontierReadHoldReleased
	}
	if since > upper {
		return ErrFrontierReadHoldInvalidBounds
	}
	hold.set.mu.Lock()
	defer hold.set.mu.Unlock()
	if hold.released {
		return ErrFrontierReadHoldReleased
	}
	currentSince, ok := hold.set.holds[hold.id]
	if !ok {
		hold.released = true
		return ErrFrontierReadHoldReleased
	}
	if since < currentSince || upper < hold.upper {
		return ErrFrontierReadHoldRegression
	}
	hold.set.holds[hold.id] = since
	hold.upper = upper
	return nil
}

// Release removes the hold from its set. Releasing the same hold more than
// once is a no-op, which makes defer-based cleanup safe on error paths.
func (hold *FrontierReadHold) Release() error {
	if hold == nil || hold.set == nil {
		return ErrFrontierReadHoldReleased
	}
	hold.set.mu.Lock()
	defer hold.set.mu.Unlock()
	if hold.released {
		return nil
	}
	delete(hold.set.holds, hold.id)
	hold.released = true
	return nil
}

// Active returns the number of currently registered holds.
func (set *FrontierReadHoldSet) Active() int {
	if set == nil {
		return 0
	}
	set.mu.RLock()
	active := len(set.holds)
	set.mu.RUnlock()
	return active
}

// SafeSince returns the earliest since frontier that must be retained. The
// fallback is returned when no hold is active or when it is already earlier
// than every hold.
func (set *FrontierReadHoldSet) SafeSince(fallback uint64) uint64 {
	if set == nil {
		return fallback
	}
	set.mu.RLock()
	defer set.mu.RUnlock()
	safe := fallback
	for _, since := range set.holds {
		if since < safe {
			safe = since
		}
	}
	return safe
}

// CanCompactThrough reports whether inclusive compaction through sequence is
// safe. A hold at since N pins N and later history, so only values below N can
// be discarded while that hold is active.
func (set *FrontierReadHoldSet) CanCompactThrough(sequence uint64) bool {
	if set == nil {
		return true
	}
	set.mu.RLock()
	defer set.mu.RUnlock()
	for _, since := range set.holds {
		if sequence >= since {
			return false
		}
	}
	return true
}
