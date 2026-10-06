package hatSql

import (
	"context"
	"errors"
	"sync"
	"time"
)

var (
	ErrDifferentialTemporalJoinCompactionSchedulerInvalid           = errors.New("hatSql: differential temporal join compaction scheduler options are invalid")
	ErrDifferentialTemporalJoinCompactionSchedulerDisabled          = errors.New("hatSql: differential temporal join compaction scheduler requires a compaction policy")
	ErrDifferentialTemporalJoinCompactionSchedulerFrontiersRequired = errors.New("hatSql: differential temporal join compaction scheduler frontier provider is required")
	ErrDifferentialTemporalJoinCompactionSchedulerAlreadyRunning    = errors.New("hatSql: differential temporal join compaction scheduler is already running")
)

const DefaultDifferentialTemporalJoinCompactionSchedulerInterval = time.Second

// DifferentialTemporalJoinCompactionFrontierFunc supplies the current sealed
// frontiers. The scheduler passes its context so a slow provider can stop
// promptly during shutdown.
type DifferentialTemporalJoinCompactionFrontierFunc func(context.Context) (leftFrontier, rightFrontier uint64, err error)

// DifferentialTemporalJoinCompactionSchedulerOptions enables the explicit
// background scheduler. A zero interval uses a conservative one-second tick;
// the scheduler is never started by NewDifferentialTemporalJoin or Apply.
type DifferentialTemporalJoinCompactionSchedulerOptions struct {
	Context   context.Context
	Interval  time.Duration
	Frontiers DifferentialTemporalJoinCompactionFrontierFunc
	// OnResult runs synchronously on the scheduler goroutine after an attempted
	// compaction. It should return promptly and must not call Stop.
	OnResult func(DifferentialTemporalJoinCompactionResult)
}

// DifferentialTemporalJoinCompactionResult reports one scheduler attempt.
// Err is reported to OnResult while the scheduler remains active, except when
// the supplied context has been canceled.
type DifferentialTemporalJoinCompactionResult struct {
	Attempted      bool
	Stats          DifferentialTemporalJoinCompactionStats
	Recommendation DifferentialTemporalJoinCompactionRecommendation
	Err            error
}

// DifferentialTemporalJoinCompactionScheduler owns one opt-in compaction
// goroutine. Stop is idempotent and waits until the goroutine has exited.
type DifferentialTemporalJoinCompactionScheduler struct {
	join      *DifferentialTemporalJoin
	cancel    context.CancelFunc
	done      chan struct{}
	stopOnce  sync.Once
	interval  time.Duration
	frontiers DifferentialTemporalJoinCompactionFrontierFunc
	onResult  func(DifferentialTemporalJoinCompactionResult)
}

// StartCompactionScheduler starts one bounded, opt-in compaction loop. It
// checks the policy before asking for frontiers, so idle joins do not invoke a
// potentially expensive frontier provider on every tick.
func (join *DifferentialTemporalJoin) StartCompactionScheduler(options DifferentialTemporalJoinCompactionSchedulerOptions) (*DifferentialTemporalJoinCompactionScheduler, error) {
	if join == nil {
		return nil, ErrDifferentialTemporalJoinNil
	}
	if join.compactionPolicy == nil {
		return nil, ErrDifferentialTemporalJoinCompactionSchedulerDisabled
	}
	if options.Interval < 0 {
		return nil, ErrDifferentialTemporalJoinCompactionSchedulerInvalid
	}
	if options.Frontiers == nil {
		return nil, ErrDifferentialTemporalJoinCompactionSchedulerFrontiersRequired
	}
	if options.Interval == 0 {
		options.Interval = DefaultDifferentialTemporalJoinCompactionSchedulerInterval
	}
	baseContext := options.Context
	if baseContext == nil {
		baseContext = context.Background()
	}
	ctx, cancel := context.WithCancel(baseContext)
	scheduler := &DifferentialTemporalJoinCompactionScheduler{
		join:      join,
		cancel:    cancel,
		done:      make(chan struct{}),
		interval:  options.Interval,
		frontiers: options.Frontiers,
		onResult:  options.OnResult,
	}
	join.mu.Lock()
	if join.compactionScheduler != nil {
		join.mu.Unlock()
		cancel()
		return nil, ErrDifferentialTemporalJoinCompactionSchedulerAlreadyRunning
	}
	join.compactionScheduler = scheduler
	join.mu.Unlock()
	go scheduler.run(ctx)
	return scheduler, nil
}

// Stop cancels the scheduler and waits for its goroutine to exit. Calling it
// repeatedly is safe.
func (scheduler *DifferentialTemporalJoinCompactionScheduler) Stop() {
	if scheduler == nil {
		return
	}
	scheduler.stopOnce.Do(scheduler.cancel)
	<-scheduler.done
}

// Done returns a channel closed when the scheduler exits.
func (scheduler *DifferentialTemporalJoinCompactionScheduler) Done() <-chan struct{} {
	if scheduler == nil {
		return nil
	}
	return scheduler.done
}

func (scheduler *DifferentialTemporalJoinCompactionScheduler) run(ctx context.Context) {
	defer func() {
		scheduler.join.mu.Lock()
		if scheduler.join.compactionScheduler == scheduler {
			scheduler.join.compactionScheduler = nil
		}
		scheduler.join.mu.Unlock()
		close(scheduler.done)
	}()

	ticker := time.NewTicker(scheduler.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			scheduler.tick(ctx, now.UTC())
		}
	}
}

func (scheduler *DifferentialTemporalJoinCompactionScheduler) tick(ctx context.Context, now time.Time) {
	recommendation := scheduler.join.CompactionRecommendation(now)
	if !recommendation.ShouldCompact {
		return
	}
	leftFrontier, rightFrontier, err := scheduler.frontiers(ctx)
	if err != nil {
		scheduler.report(ctx, DifferentialTemporalJoinCompactionResult{
			Attempted:      true,
			Recommendation: recommendation,
			Err:            err,
		})
		return
	}
	stats, err := scheduler.join.compactDifferentialTemporalJoinContext(ctx, leftFrontier, rightFrontier, now)
	scheduler.report(ctx, DifferentialTemporalJoinCompactionResult{
		Attempted:      true,
		Stats:          stats,
		Recommendation: recommendation,
		Err:            err,
	})
}

func (scheduler *DifferentialTemporalJoinCompactionScheduler) report(ctx context.Context, result DifferentialTemporalJoinCompactionResult) {
	if scheduler.onResult == nil || ctx.Err() != nil {
		return
	}
	scheduler.onResult(result)
}
