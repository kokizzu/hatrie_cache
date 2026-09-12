package hatReplication

import (
	"context"
	"errors"
	"sync"
)

var (
	// ErrConnectionPoolClosed is returned after the pool has been closed.
	ErrConnectionPoolClosed = errors.New("connection pool is closed")
	// ErrConnectionPoolInvalid is returned for unusable pool options.
	ErrConnectionPoolInvalid = errors.New("invalid connection pool options")
)

// ConnectionPoolOptions configures a ConnectionPool.
//
// MaxOpen is required and bounds the number of connections created by the
// pool. MaxIdle controls how many released connections remain reusable; zero
// disables idle retention. Dial receives the caller's context so it can apply
// its own timeout, authentication, and transport settings. Close may be nil
// for values that do not need cleanup.
type ConnectionPoolOptions[T any] struct {
	MaxOpen int
	MaxIdle int
	Dial    func(context.Context) (T, error)
	Close   func(T) error
}

// ConnectionPoolStats is a point-in-time pool state snapshot.
type ConnectionPoolStats struct {
	MaxOpen int
	MaxIdle int
	Open    int
	Idle    int
	InUse   int
	Closed  bool
}

// ConnectionPool bounds and reuses caller-owned connections. It is safe for
// concurrent use. Close does not wait for active connections; active callers
// must still Release or Discard them, after which the connection is closed.
type ConnectionPool[T any] struct {
	dial  func(context.Context) (T, error)
	close func(T) error

	slots  chan struct{}
	idle   chan T
	closed chan struct{}

	mu          sync.Mutex
	closedState bool
	open        int
	maxOpen     int
	maxIdle     int
}

// NewConnectionPool creates a bounded reusable connection pool. MaxIdle values
// greater than MaxOpen are reduced to MaxOpen.
func NewConnectionPool[T any](options ConnectionPoolOptions[T]) (*ConnectionPool[T], error) {
	if options.MaxOpen <= 0 || options.MaxIdle < 0 || options.Dial == nil {
		return nil, ErrConnectionPoolInvalid
	}
	if options.MaxIdle > options.MaxOpen {
		options.MaxIdle = options.MaxOpen
	}
	pool := &ConnectionPool[T]{
		dial:    options.Dial,
		close:   options.Close,
		slots:   make(chan struct{}, options.MaxOpen),
		idle:    make(chan T, options.MaxIdle),
		closed:  make(chan struct{}),
		maxOpen: options.MaxOpen,
		maxIdle: options.MaxIdle,
	}
	for i := 0; i < options.MaxOpen; i++ {
		pool.slots <- struct{}{}
	}
	return pool, nil
}

// Acquire returns an idle connection or dials a new one when capacity is
// available. It waits for an idle connection, capacity, cancellation, or
// Close. A nil context is treated as context.Background.
func (pool *ConnectionPool[T]) Acquire(ctx context.Context) (T, error) {
	var zero T
	if pool == nil {
		return zero, ErrConnectionPoolClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	if pool.closedNow() {
		return zero, ErrConnectionPoolClosed
	}
	if connection, ok := pool.tryIdle(); ok {
		if pool.closedNow() {
			pool.discardAcquired(connection)
			return zero, ErrConnectionPoolClosed
		}
		return connection, nil
	}

	select {
	case <-ctx.Done():
		return zero, ctx.Err()
	case <-pool.closed:
		return zero, ErrConnectionPoolClosed
	case connection := <-pool.idle:
		if pool.closedNow() {
			pool.discardAcquired(connection)
			return zero, ErrConnectionPoolClosed
		}
		return connection, nil
	case <-pool.slots:
		if pool.closedNow() {
			pool.returnSlot()
			return zero, ErrConnectionPoolClosed
		}
		connection, err := pool.dial(ctx)
		if err != nil {
			pool.returnSlot()
			return zero, err
		}
		pool.mu.Lock()
		if pool.closedState {
			pool.mu.Unlock()
			pool.closeConnection(connection)
			pool.returnSlot()
			return zero, ErrConnectionPoolClosed
		}
		pool.open++
		pool.mu.Unlock()
		return connection, nil
	}
}

// Release returns a healthy connection to the idle pool. If the pool is
// closed or its idle bound is full, the connection is closed instead.
func (pool *ConnectionPool[T]) Release(connection T) {
	pool.release(connection, true)
}

// Discard closes a connection and returns its capacity to the pool. Use it
// after a transport error or any other indication that the connection is not
// reusable.
func (pool *ConnectionPool[T]) Discard(connection T) {
	pool.release(connection, false)
}

func (pool *ConnectionPool[T]) release(connection T, healthy bool) {
	if pool == nil {
		return
	}
	pool.mu.Lock()
	if !healthy || pool.closedState {
		pool.decrementOpenLocked()
		pool.mu.Unlock()
		pool.closeConnection(connection)
		pool.returnSlot()
		return
	}
	select {
	case pool.idle <- connection:
		pool.mu.Unlock()
		return
	default:
		pool.decrementOpenLocked()
		pool.mu.Unlock()
		pool.closeConnection(connection)
		pool.returnSlot()
	}
}

// Stats returns a point-in-time snapshot of pool capacity and usage.
func (pool *ConnectionPool[T]) Stats() ConnectionPoolStats {
	if pool == nil {
		return ConnectionPoolStats{Closed: true}
	}
	pool.mu.Lock()
	defer pool.mu.Unlock()
	idle := len(pool.idle)
	inUse := pool.open - idle
	if inUse < 0 {
		inUse = 0
	}
	return ConnectionPoolStats{
		MaxOpen: pool.maxOpen,
		MaxIdle: pool.maxIdle,
		Open:    pool.open,
		Idle:    idle,
		InUse:   inUse,
		Closed:  pool.closedState,
	}
}

// Close prevents new acquisitions, wakes waiters, and closes every idle
// connection. Active connections are closed when their owners Release or
// Discard them. Close is idempotent and returns the first idle-close error.
func (pool *ConnectionPool[T]) Close() error {
	if pool == nil {
		return nil
	}
	pool.mu.Lock()
	if pool.closedState {
		pool.mu.Unlock()
		return nil
	}
	pool.closedState = true
	close(pool.closed)
	idle := make([]T, 0, len(pool.idle))
	for {
		select {
		case connection := <-pool.idle:
			idle = append(idle, connection)
		default:
			pool.open -= len(idle)
			pool.mu.Unlock()
			var firstErr error
			for _, connection := range idle {
				if err := pool.closeConnection(connection); err != nil && firstErr == nil {
					firstErr = err
				}
			}
			return firstErr
		}
	}
}

func (pool *ConnectionPool[T]) tryIdle() (T, bool) {
	var zero T
	pool.mu.Lock()
	defer pool.mu.Unlock()
	if pool.closedState {
		return zero, false
	}
	select {
	case connection := <-pool.idle:
		return connection, true
	default:
		return zero, false
	}
}

func (pool *ConnectionPool[T]) closedNow() bool {
	select {
	case <-pool.closed:
		return true
	default:
		return false
	}
}

func (pool *ConnectionPool[T]) discardAcquired(connection T) {
	pool.closeConnection(connection)
	pool.mu.Lock()
	pool.decrementOpenLocked()
	pool.mu.Unlock()
	pool.returnSlot()
}

func (pool *ConnectionPool[T]) decrementOpenLocked() {
	if pool.open > 0 {
		pool.open--
	}
}

func (pool *ConnectionPool[T]) returnSlot() {
	pool.slots <- struct{}{}
}

func (pool *ConnectionPool[T]) closeConnection(connection T) error {
	if pool.close == nil {
		return nil
	}
	return pool.close(connection)
}
