package hatCache

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// DefaultSQLJSONIndexRebuildWorkerInterval is used when a worker is started
// with a non-positive interval. The worker remains opt-in.
const DefaultSQLJSONIndexRebuildWorkerInterval = 100 * time.Millisecond

// SQLJSONIndexRebuildWorker owns one background queue consumer. Multiple
// workers may safely share a HatTrie because queue claims are synchronized.
type SQLJSONIndexRebuildWorker struct {
	cancel   context.CancelFunc
	done     chan struct{}
	stopOnce sync.Once
}

// StartSQLJSONIndexRebuildWorker starts an opt-in background consumer for
// queued SQL JSON index rebuilds. It processes at most one completed rebuild
// per interval, reports through the existing progress callback, and retries a
// failed request on a later tick. The first poll happens immediately.
func (ht *HatTrie) StartSQLJSONIndexRebuildWorker(ctx context.Context, interval time.Duration, report func(SQLJSONIndexRebuildProgress)) (*SQLJSONIndexRebuildWorker, error) {
	if ht == nil {
		return nil, ErrNilHatTrie
	}
	if ctx == nil {
		return nil, fmt.Errorf("SQL JSON index rebuild worker context is nil")
	}
	if interval <= 0 {
		interval = DefaultSQLJSONIndexRebuildWorkerInterval
	}
	workerContext, cancel := context.WithCancel(ctx)
	worker := &SQLJSONIndexRebuildWorker{cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(worker.done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			_, err := ht.RunScheduledSQLJSONIndexRebuildsWithProgress(workerContext, 1, report)
			if err != nil && workerContext.Err() != nil {
				return
			}
			select {
			case <-workerContext.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	return worker, nil
}

// Stop requests that the worker stop after its current atomic rebuild unit.
// It is safe to call Stop more than once or concurrently with Wait.
func (worker *SQLJSONIndexRebuildWorker) Stop() {
	if worker == nil || worker.cancel == nil {
		return
	}
	worker.stopOnce.Do(worker.cancel)
}

// Wait blocks until the worker has stopped.
func (worker *SQLJSONIndexRebuildWorker) Wait() {
	if worker == nil || worker.done == nil {
		return
	}
	<-worker.done
}

// Done returns a channel closed when the worker has stopped.
func (worker *SQLJSONIndexRebuildWorker) Done() <-chan struct{} {
	if worker == nil {
		return nil
	}
	return worker.done
}
