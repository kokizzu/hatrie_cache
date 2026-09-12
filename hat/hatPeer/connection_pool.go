// Package hatPeer contains reusable peer-connection primitives.
package hatPeer

import (
	"context"
	"errors"
	"sync"
	"time"
)

var (
	ErrConnectionPoolNil                  = errors.New("hatPeer: connection pool is nil")
	ErrConnectionPoolDialRequired         = errors.New("hatPeer: connection pool dial function is required")
	ErrConnectionPoolMaxOpenInvalid       = errors.New("hatPeer: connection pool max open is invalid")
	ErrConnectionPoolMaxIdleInvalid       = errors.New("hatPeer: connection pool max idle is invalid")
	ErrConnectionPoolDialAttemptsInvalid  = errors.New("hatPeer: connection pool max dial attempts is invalid")
	ErrConnectionPoolRetryDelayInvalid    = errors.New("hatPeer: connection pool retry delay is invalid")
	ErrConnectionPoolRetryMaxDelayInvalid = errors.New("hatPeer: connection pool maximum retry delay is invalid")
	ErrConnectionPoolHandlerRequired      = errors.New("hatPeer: connection pool handler is required")
	ErrConnectionPoolContextRequired      = errors.New("hatPeer: connection pool context is required")
	ErrConnectionPoolClosed               = errors.New("hatPeer: connection pool is closed")
	ErrConnectionPoolConnectionNil        = errors.New("hatPeer: dial returned a nil connection")
)

const (
	DefaultConnectionPoolMaxOpen         = 8
	DefaultConnectionPoolMaxIdle         = 8
	DefaultConnectionPoolMaxDialAttempts = 3
	MaxConnectionPoolMaxOpen             = 4096
	MaxConnectionPoolDialAttempts        = 8
)

const (
	DefaultConnectionPoolDialRetryDelay    = 10 * time.Millisecond
	DefaultConnectionPoolDialRetryMaxDelay = 250 * time.Millisecond
)

// Connection is the minimal lifecycle contract required by ConnectionPool.
// Protocol-specific peer clients can wrap their native connection type.
type Connection interface {
	Close() error
}

// DialFunc establishes one peer connection. It must honor ctx so that pool
// shutdown and request cancellation can interrupt slow connection attempts.
type DialFunc func(ctx context.Context) (Connection, error)

// ConnectionPoolOptions configures a ConnectionPool. Zero values use sane
// bounded defaults; negative values are rejected.
type ConnectionPoolOptions struct {
	MaxOpen           int
	MaxIdle           int
	MaxDialAttempts   int
	DialRetryDelay    time.Duration
	DialRetryMaxDelay time.Duration
	Dial              DialFunc
}

// ConnectionPoolStats is a point-in-time pool snapshot.
type ConnectionPoolStats struct {
	Active       int
	Open         int
	Idle         int
	Acquires     uint64
	DialAttempts uint64
	DialFailures uint64
}

// ConnectionPool bounds peer connections and reuses successful connections.
// A handler error closes that connection rather than retrying the handler,
// because the pool cannot know whether a peer operation is idempotent.
type ConnectionPool struct {
	dial              DialFunc
	maxOpen           int
	maxIdle           int
	maxDialAttempts   int
	dialRetryDelay    time.Duration
	dialRetryMaxDelay time.Duration

	slots   chan struct{}
	idle    chan Connection
	closed  chan struct{}
	done    chan struct{}
	poolCtx context.Context
	cancel  context.CancelFunc

	mu           sync.Mutex
	closedState  bool
	active       int
	open         int
	acquires     uint64
	dialAttempts uint64
	dialFailures uint64
}

// NewConnectionPool creates a bounded reusable connection pool.
func NewConnectionPool(options ConnectionPoolOptions) (*ConnectionPool, error) {
	if options.Dial == nil {
		return nil, ErrConnectionPoolDialRequired
	}
	if options.MaxOpen < 0 || options.MaxOpen > MaxConnectionPoolMaxOpen {
		return nil, ErrConnectionPoolMaxOpenInvalid
	}
	if options.MaxOpen == 0 {
		options.MaxOpen = DefaultConnectionPoolMaxOpen
	}
	if options.MaxIdle < 0 || options.MaxIdle > options.MaxOpen {
		return nil, ErrConnectionPoolMaxIdleInvalid
	}
	if options.MaxIdle == 0 {
		options.MaxIdle = min(DefaultConnectionPoolMaxIdle, options.MaxOpen)
	}
	if options.MaxDialAttempts < 0 || options.MaxDialAttempts > MaxConnectionPoolDialAttempts {
		return nil, ErrConnectionPoolDialAttemptsInvalid
	}
	if options.MaxDialAttempts == 0 {
		options.MaxDialAttempts = DefaultConnectionPoolMaxDialAttempts
	}
	if options.DialRetryDelay < 0 {
		return nil, ErrConnectionPoolRetryDelayInvalid
	}
	if options.DialRetryDelay == 0 {
		options.DialRetryDelay = DefaultConnectionPoolDialRetryDelay
	}
	if options.DialRetryMaxDelay < 0 {
		return nil, ErrConnectionPoolRetryMaxDelayInvalid
	}
	if options.DialRetryMaxDelay == 0 {
		options.DialRetryMaxDelay = DefaultConnectionPoolDialRetryMaxDelay
	}
	if options.DialRetryMaxDelay < options.DialRetryDelay {
		return nil, ErrConnectionPoolRetryMaxDelayInvalid
	}

	poolContext, cancel := context.WithCancel(context.Background())
	return &ConnectionPool{
		dial:              options.Dial,
		maxOpen:           options.MaxOpen,
		maxIdle:           options.MaxIdle,
		maxDialAttempts:   options.MaxDialAttempts,
		dialRetryDelay:    options.DialRetryDelay,
		dialRetryMaxDelay: options.DialRetryMaxDelay,
		slots:             make(chan struct{}, options.MaxOpen),
		idle:              make(chan Connection, options.MaxIdle),
		closed:            make(chan struct{}),
		done:              make(chan struct{}),
		poolCtx:           poolContext,
		cancel:            cancel,
	}, nil
}

// Do acquires one connection, invokes fn, and returns the connection to the
// idle pool when fn succeeds. Failed operations discard the connection.
func (pool *ConnectionPool) Do(ctx context.Context, fn func(context.Context, Connection) error) error {
	if pool == nil {
		return ErrConnectionPoolNil
	}
	if fn == nil {
		return ErrConnectionPoolHandlerRequired
	}
	if ctx == nil {
		return ErrConnectionPoolContextRequired
	}
	if err := pool.begin(); err != nil {
		return err
	}
	defer pool.end()

	connection, err := pool.acquire(ctx)
	if err != nil {
		return err
	}

	reusable := false
	defer func() {
		_ = pool.release(connection, reusable)
	}()
	err = fn(ctx, connection)
	reusable = err == nil
	return err
}

// Close stops new acquisitions, closes idle connections, and waits for
// active handlers to release their connections. It continues closing in the
// background when ctx expires and may be called more than once.
func (pool *ConnectionPool) Close(ctx context.Context) error {
	if pool == nil {
		return ErrConnectionPoolNil
	}
	if ctx == nil {
		return ErrConnectionPoolContextRequired
	}

	pool.mu.Lock()
	if !pool.closedState {
		pool.closedState = true
		close(pool.closed)
		pool.cancel()
		if pool.active == 0 {
			close(pool.done)
		}
	}
	pool.mu.Unlock()

	pool.closeIdle()
	select {
	case <-pool.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Stats reports current activity and cumulative pool counters.
func (pool *ConnectionPool) Stats() ConnectionPoolStats {
	if pool == nil {
		return ConnectionPoolStats{}
	}
	pool.mu.Lock()
	defer pool.mu.Unlock()
	return ConnectionPoolStats{
		Active:       pool.active,
		Open:         pool.open,
		Idle:         len(pool.idle),
		Acquires:     pool.acquires,
		DialAttempts: pool.dialAttempts,
		DialFailures: pool.dialFailures,
	}
}

func (pool *ConnectionPool) begin() error {
	pool.mu.Lock()
	defer pool.mu.Unlock()
	if pool.closedState {
		return ErrConnectionPoolClosed
	}
	pool.active++
	pool.acquires++
	return nil
}

func (pool *ConnectionPool) end() {
	pool.mu.Lock()
	pool.active--
	if pool.closedState && pool.active == 0 {
		close(pool.done)
	}
	pool.mu.Unlock()
}

func (pool *ConnectionPool) acquire(ctx context.Context) (Connection, error) {
	if connection, ok := pool.tryIdle(); ok {
		return pool.acceptIdle(connection)
	}

	select {
	case connection := <-pool.idle:
		return pool.acceptIdle(connection)
	case pool.slots <- struct{}{}:
		if pool.isClosed() {
			pool.releaseSlot()
			return nil, ErrConnectionPoolClosed
		}
		connection, err := pool.dialWithRetry(ctx)
		if err != nil {
			pool.releaseSlot()
			return nil, err
		}
		pool.mu.Lock()
		if pool.closedState {
			pool.mu.Unlock()
			_ = pool.closeConnection(connection)
			pool.releaseSlot()
			return nil, ErrConnectionPoolClosed
		}
		pool.open++
		pool.mu.Unlock()
		return connection, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-pool.closed:
		return nil, ErrConnectionPoolClosed
	}
}

func (pool *ConnectionPool) tryIdle() (Connection, bool) {
	select {
	case connection := <-pool.idle:
		return connection, true
	default:
		return nil, false
	}
}

func (pool *ConnectionPool) acceptIdle(connection Connection) (Connection, error) {
	if connection == nil {
		return nil, ErrConnectionPoolConnectionNil
	}
	if pool.isClosed() {
		_ = pool.closeConnection(connection)
		pool.releaseSlot()
		return nil, ErrConnectionPoolClosed
	}
	return connection, nil
}

func (pool *ConnectionPool) dialWithRetry(ctx context.Context) (Connection, error) {
	dialContext, cancel := context.WithCancel(ctx)
	stopPoolCancel := context.AfterFunc(pool.poolCtx, cancel)
	defer func() {
		stopPoolCancel()
		cancel()
	}()

	var lastErr error
	delay := pool.dialRetryDelay
	for attempt := 0; attempt < pool.maxDialAttempts; attempt++ {
		if pool.isClosed() {
			return nil, ErrConnectionPoolClosed
		}
		pool.mu.Lock()
		pool.dialAttempts++
		pool.mu.Unlock()
		connection, err := pool.dial(dialContext)
		if err == nil && connection != nil {
			return connection, nil
		}
		if err == nil {
			err = ErrConnectionPoolConnectionNil
		}
		lastErr = err
		pool.mu.Lock()
		pool.dialFailures++
		pool.mu.Unlock()
		if attempt+1 == pool.maxDialAttempts {
			break
		}
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-dialContext.Done():
			if !timer.Stop() {
				<-timer.C
			}
			if pool.isClosed() {
				return nil, ErrConnectionPoolClosed
			}
			return nil, dialContext.Err()
		case <-pool.closed:
			if !timer.Stop() {
				<-timer.C
			}
			return nil, ErrConnectionPoolClosed
		}
		if delay < pool.dialRetryMaxDelay {
			delay *= 2
			if delay > pool.dialRetryMaxDelay || delay < 0 {
				delay = pool.dialRetryMaxDelay
			}
		}
	}
	return nil, lastErr
}

func (pool *ConnectionPool) release(connection Connection, reusable bool) error {
	if connection == nil {
		return ErrConnectionPoolConnectionNil
	}
	pool.mu.Lock()
	if reusable && !pool.closedState {
		select {
		case pool.idle <- connection:
			pool.mu.Unlock()
			return nil
		default:
		}
	}
	pool.mu.Unlock()
	return pool.closeConnectionAndReleaseSlot(connection)
}

func (pool *ConnectionPool) closeIdle() {
	for {
		select {
		case connection := <-pool.idle:
			_ = pool.closeConnectionAndReleaseSlot(connection)
		default:
			return
		}
	}
}

func (pool *ConnectionPool) closeConnection(connection Connection) error {
	if connection == nil {
		return nil
	}
	pool.mu.Lock()
	if pool.open > 0 {
		pool.open--
	}
	pool.mu.Unlock()
	return connection.Close()
}

func (pool *ConnectionPool) closeConnectionAndReleaseSlot(connection Connection) error {
	err := pool.closeConnection(connection)
	pool.releaseSlot()
	return err
}

func (pool *ConnectionPool) releaseSlot() {
	<-pool.slots
}

func (pool *ConnectionPool) isClosed() bool {
	pool.mu.Lock()
	closed := pool.closedState
	pool.mu.Unlock()
	return closed
}
