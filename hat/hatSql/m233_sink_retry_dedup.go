package hatSql

import (
	"context"
	"errors"
	"io"
	"net"
	"time"
)

const (
	// DefaultSQLSinkRetryAttempts is the maximum number of delivery attempts
	// used when no explicit retry bound is configured.
	DefaultSQLSinkRetryAttempts = 3
	// MaxSQLSinkRetryAttempts prevents accidental unbounded retry loops.
	MaxSQLSinkRetryAttempts = 64
	// DefaultSQLSinkRetryInitialBackoff is the first delay between attempts.
	DefaultSQLSinkRetryInitialBackoff = 10 * time.Millisecond
	// DefaultSQLSinkRetryMaxBackoff caps exponential retry delay.
	DefaultSQLSinkRetryMaxBackoff = time.Second
)

var (
	// ErrSQLSinkRetryExecutorNil reports a method call on a nil executor.
	ErrSQLSinkRetryExecutorNil = errors.New("SQL sink retry executor is nil")
	// ErrSQLSinkRetryLedgerRequired reports construction without an exactly-once
	// ledger.
	ErrSQLSinkRetryLedgerRequired = errors.New("SQL sink retry ledger is required")
	// ErrSQLSinkRetryApplyRequired reports a nil delivery callback.
	ErrSQLSinkRetryApplyRequired = errors.New("SQL sink retry delivery callback is required")
	// ErrSQLSinkRetryOptionsInvalid reports an invalid retry bound or delay.
	ErrSQLSinkRetryOptionsInvalid = errors.New("SQL sink retry options are invalid")
)

// SQLSinkRetryOptions bounds retries around an exactly-once sink ledger.
// Retryable may narrow the default network/disconnected error classifier.
// Sleep is injectable for deterministic tests and should honor ctx.
type SQLSinkRetryOptions struct {
	MaxAttempts    int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	Retryable      func(error) bool
	Sleep          func(context.Context, time.Duration) error
}

// SQLSinkRetryResult describes a successful or failed delivery attempt set.
// Committed is true for a newly committed or already committed transaction;
// Deduplicated distinguishes the latter case.
type SQLSinkRetryResult struct {
	Attempts     int
	Committed    bool
	Deduplicated bool
}

// SQLSinkRetryExecutor retries disconnected delivery failures through a
// SQLSinkExactlyOnceLedger. The caller must pass the supplied idempotency key
// to the external sink so a connection loss after apply cannot duplicate the
// output.
type SQLSinkRetryExecutor struct {
	ledger         *SQLSinkExactlyOnceLedger
	maxAttempts    int
	initialBackoff time.Duration
	maxBackoff     time.Duration
	retryable      func(error) bool
	sleep          func(context.Context, time.Duration) error
}

// NewSQLSinkRetryExecutor creates a bounded retry executor around ledger.
func NewSQLSinkRetryExecutor(ledger *SQLSinkExactlyOnceLedger, options SQLSinkRetryOptions) (*SQLSinkRetryExecutor, error) {
	if ledger == nil {
		return nil, ErrSQLSinkRetryLedgerRequired
	}
	maxAttempts := options.MaxAttempts
	if maxAttempts == 0 {
		maxAttempts = DefaultSQLSinkRetryAttempts
	}
	if maxAttempts < 1 || maxAttempts > MaxSQLSinkRetryAttempts {
		return nil, ErrSQLSinkRetryOptionsInvalid
	}
	initialBackoff := options.InitialBackoff
	if initialBackoff == 0 {
		initialBackoff = DefaultSQLSinkRetryInitialBackoff
	}
	maxBackoff := options.MaxBackoff
	if maxBackoff == 0 {
		maxBackoff = DefaultSQLSinkRetryMaxBackoff
	}
	if initialBackoff < 0 || maxBackoff < initialBackoff {
		return nil, ErrSQLSinkRetryOptionsInvalid
	}
	retryable := options.Retryable
	if retryable == nil {
		retryable = DefaultSQLSinkRetryable
	}
	sleep := options.Sleep
	if sleep == nil {
		sleep = sleepSQLSinkRetry
	}
	return &SQLSinkRetryExecutor{
		ledger:         ledger,
		maxAttempts:    maxAttempts,
		initialBackoff: initialBackoff,
		maxBackoff:     maxBackoff,
		retryable:      retryable,
		sleep:          sleep,
	}, nil
}

// Commit retries one sink transaction. apply receives the same normalized
// idempotency key on every attempt. A nil error after a prior committed
// transaction is reported as Deduplicated without invoking apply.
func (executor *SQLSinkRetryExecutor) Commit(ctx context.Context, commit SQLSinkCommit, apply func(string) error) (SQLSinkRetryResult, error) {
	if executor == nil {
		return SQLSinkRetryResult{}, ErrSQLSinkRetryExecutorNil
	}
	if apply == nil {
		return SQLSinkRetryResult{}, ErrSQLSinkRetryApplyRequired
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return SQLSinkRetryResult{}, err
	}
	var result SQLSinkRetryResult
	for attempt := 1; attempt <= executor.maxAttempts; attempt++ {
		result.Attempts = attempt
		committed, err := executor.ledger.CommitContext(ctx, commit, apply)
		if err == nil {
			result.Committed = true
			result.Deduplicated = !committed
			return result, nil
		}
		if attempt == executor.maxAttempts || !executor.retryable(err) {
			return result, err
		}
		if err := executor.sleep(ctx, sqlSinkRetryBackoff(executor.initialBackoff, executor.maxBackoff, attempt)); err != nil {
			return result, err
		}
	}
	return result, ErrSQLSinkRetryOptionsInvalid
}

// DefaultSQLSinkRetryable reports errors commonly caused by a disconnected
// output stream. Permanent application errors are not retried by default;
// callers can provide a classifier for their transport's error type.
func DefaultSQLSinkRetryable(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var networkError net.Error
	if errors.As(err, &networkError) {
		return networkError.Timeout() || networkError.Temporary()
	}
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, net.ErrClosed)
}

func sleepSQLSinkRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func sqlSinkRetryBackoff(initial, maximum time.Duration, attempt int) time.Duration {
	delay := initial
	for index := 1; index < attempt; index++ {
		if delay >= maximum/2 {
			return maximum
		}
		delay *= 2
	}
	if delay > maximum {
		return maximum
	}
	return delay
}
