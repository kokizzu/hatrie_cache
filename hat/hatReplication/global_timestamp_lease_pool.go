package hatReplication

import (
	"errors"
	"strings"
	"sync"
)

const (
	// DefaultGlobalTimestampLeaseBatchSize bounds reservation overhead while
	// keeping unused timestamps after a node restart reasonably small.
	DefaultGlobalTimestampLeaseBatchSize uint64 = 64
	// MaxGlobalTimestampLeaseBatchSize prevents one caller from accidentally
	// retaining an unbounded local timestamp range.
	MaxGlobalTimestampLeaseBatchSize uint64 = 1 << 20
)

var (
	// ErrGlobalTimestampLeasePoolInvalid reports a nil callback or invalid
	// node, term, epoch, observation, or batch configuration.
	ErrGlobalTimestampLeasePoolInvalid = errors.New("hatriecache: global timestamp lease pool input is invalid")
	// ErrGlobalTimestampLeasePoolGrantMismatch reports a reservation response
	// that is not bound to the exact request issued by the pool.
	ErrGlobalTimestampLeasePoolGrantMismatch = errors.New("hatriecache: global timestamp lease pool grant does not match request")
	// ErrGlobalTimestampLeasePoolSequenceOverflow reports that the per-node
	// request sequence cannot advance safely.
	ErrGlobalTimestampLeasePoolSequenceOverflow = errors.New("hatriecache: global timestamp lease pool sequence overflow")
)

// GlobalTimestampReserveFunc reserves one globally ordered range. A callback
// may be backed by the local coordinator, authenticated gRPC, or another
// caller-owned consensus transport. The pool retries the same request
// sequence after an error so an idempotent coordinator can safely recover from
// a lost response.
type GlobalTimestampReserveFunc func(GlobalTimestampRequest) (GlobalTimestampGrant, error)

// GlobalTimestampLeasePoolOptions configures one node's local range cache.
// Term, NodeID, and NodeEpoch are copied into every reservation request.
type GlobalTimestampLeasePoolOptions struct {
	Term      uint64
	NodeID    string
	NodeEpoch uint64
	BatchSize uint64
	Reserve   GlobalTimestampReserveFunc
}

// GlobalTimestampLeasePoolStats reports reservation amortization and unused
// range state. The snapshot is detached and safe to retain.
type GlobalTimestampLeasePoolStats struct {
	ReservationAttempts uint64
	Reservations        uint64
	Issued              uint64
	Discarded           uint64
	Remaining           uint64
	NextSequence        uint64
}

// GlobalTimestampLeasePool obtains bounded ranges from a global coordinator
// and serves individual timestamps locally. Calls are serialized only around
// the pool state; the underlying GlobalTimestampLease remains allocation-free
// after a successful refill.
type GlobalTimestampLeasePool struct {
	mu        sync.Mutex
	term      uint64
	nodeID    string
	nodeEpoch uint64
	batchSize uint64
	reserve   GlobalTimestampReserveFunc
	sequence  uint64
	observed  int64
	lease     *GlobalTimestampLease
	stats     GlobalTimestampLeasePoolStats
}

// NewGlobalTimestampLeasePool creates a bounded local timestamp allocator.
// A zero BatchSize uses DefaultGlobalTimestampLeaseBatchSize.
func NewGlobalTimestampLeasePool(options GlobalTimestampLeasePoolOptions) (*GlobalTimestampLeasePool, error) {
	options.NodeID = strings.TrimSpace(options.NodeID)
	if options.Term == 0 || options.NodeID == "" || options.NodeEpoch == 0 || options.Reserve == nil {
		return nil, ErrGlobalTimestampLeasePoolInvalid
	}
	if options.BatchSize == 0 {
		options.BatchSize = DefaultGlobalTimestampLeaseBatchSize
	}
	if options.BatchSize > MaxGlobalTimestampLeaseBatchSize {
		return nil, ErrGlobalTimestampLeasePoolInvalid
	}
	return &GlobalTimestampLeasePool{
		term:      options.Term,
		nodeID:    options.NodeID,
		nodeEpoch: options.NodeEpoch,
		batchSize: options.BatchSize,
		reserve:   options.Reserve,
		sequence:  1,
	}, nil
}

// Next returns the next globally ordered timestamp. A local lease is consumed
// without a reservation until it is exhausted; a refill is requested with the
// same sequence again if the callback returns an error.
func (pool *GlobalTimestampLeasePool) Next() (int64, error) {
	if pool == nil {
		return 0, ErrGlobalTimestampLeasePoolInvalid
	}
	pool.mu.Lock()
	defer pool.mu.Unlock()
	if pool.lease != nil {
		if timestamp, ok := pool.lease.Next(); ok {
			pool.noteIssuedLocked(timestamp)
			return timestamp, nil
		}
		pool.lease = nil
	}
	if pool.sequence == 0 {
		return 0, ErrGlobalTimestampLeasePoolSequenceOverflow
	}
	request := GlobalTimestampRequest{
		Term:      pool.term,
		NodeID:    pool.nodeID,
		NodeEpoch: pool.nodeEpoch,
		Sequence:  pool.sequence,
		Observed:  pool.observed,
		Count:     pool.batchSize,
	}
	pool.stats.ReservationAttempts++
	grant, err := pool.reserve(request)
	if err != nil {
		return 0, err
	}
	if err := validateGlobalTimestampLeasePoolGrant(request, grant); err != nil {
		return 0, err
	}
	lease, err := NewGlobalTimestampLease(grant)
	if err != nil {
		return 0, err
	}
	pool.lease = lease
	pool.sequence++
	pool.stats.Reservations++
	timestamp, ok := pool.lease.Next()
	if !ok {
		return 0, ErrGlobalTimestampLeasePoolGrantMismatch
	}
	pool.noteIssuedLocked(timestamp)
	return timestamp, nil
}

// Observe incorporates a timestamp received from another node. A higher
// observation retires any unused local range so future timestamps remain
// strictly newer; retired values are intentionally left as gaps.
func (pool *GlobalTimestampLeasePool) Observe(timestamp int64) error {
	if pool == nil || timestamp < 0 {
		return ErrGlobalTimestampLeasePoolInvalid
	}
	pool.mu.Lock()
	defer pool.mu.Unlock()
	if timestamp <= pool.observed {
		return nil
	}
	if pool.lease != nil {
		pool.stats.Discarded += pool.lease.Remaining()
		pool.lease = nil
	}
	pool.observed = timestamp
	return nil
}

// Current returns the greatest timestamp observed or issued by the pool.
func (pool *GlobalTimestampLeasePool) Current() int64 {
	if pool == nil {
		return 0
	}
	pool.mu.Lock()
	defer pool.mu.Unlock()
	return pool.observed
}

// Stats returns a detached snapshot of pool activity.
func (pool *GlobalTimestampLeasePool) Stats() GlobalTimestampLeasePoolStats {
	if pool == nil {
		return GlobalTimestampLeasePoolStats{}
	}
	pool.mu.Lock()
	defer pool.mu.Unlock()
	stats := pool.stats
	if pool.lease != nil {
		stats.Remaining = pool.lease.Remaining()
	}
	stats.NextSequence = pool.sequence
	return stats
}

func (pool *GlobalTimestampLeasePool) noteIssuedLocked(timestamp int64) {
	pool.stats.Issued++
	if timestamp > pool.observed {
		pool.observed = timestamp
	}
}

func validateGlobalTimestampLeasePoolGrant(request GlobalTimestampRequest, grant GlobalTimestampGrant) error {
	if grant.Term != request.Term || grant.NodeID != request.NodeID || grant.NodeEpoch != request.NodeEpoch || grant.Sequence != request.Sequence || grant.Count != request.Count {
		return ErrGlobalTimestampLeasePoolGrantMismatch
	}
	if err := validateGlobalTimestampGrant(grant); err != nil {
		return ErrGlobalTimestampLeasePoolGrantMismatch
	}
	return nil
}
