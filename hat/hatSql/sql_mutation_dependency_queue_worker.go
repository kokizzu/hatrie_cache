package hatSql

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

var (
	// ErrSQLMutationDependencyQueueContextNil reports a nil worker context.
	ErrSQLMutationDependencyQueueContextNil = errors.New("SQL mutation dependency queue worker context is nil")
	// ErrSQLMutationDependencyQueueHandlerNil reports a missing task handler.
	ErrSQLMutationDependencyQueueHandlerNil = errors.New("SQL mutation dependency queue worker handler is nil")
)

// SQLMutationDependencyQueueRunStats summarizes one RunReady batch.
type SQLMutationDependencyQueueRunStats struct {
	Claimed   int
	Completed int
	Failed    int
}

// RunReady claims up to limit dependency-ready tasks and executes them in the
// queue's deterministic task-ID order. A non-positive limit processes every
// task that is ready when the batch is claimed. Successful handlers are
// durably completed; handler errors and cancellation between claimed tasks are
// durably marked failed so callers can inspect or Retry them later.
//
// RunReady continues the claimed batch after a handler error and returns the
// first error after all transitions have been attempted. The callback must not
// retain or mutate the task record after it returns.
func (queue *SQLMutationDependencyQueue) RunReady(ctx context.Context, limit int, handler func(context.Context, SQLMutationTaskRecord) error) (SQLMutationDependencyQueueRunStats, error) {
	var stats SQLMutationDependencyQueueRunStats
	if queue == nil {
		return stats, ErrSQLMutationDependencyQueueNil
	}
	if ctx == nil {
		return stats, ErrSQLMutationDependencyQueueContextNil
	}
	if handler == nil {
		return stats, ErrSQLMutationDependencyQueueHandlerNil
	}
	if err := ctx.Err(); err != nil {
		return stats, err
	}

	claimed, err := queue.ClaimReady(limit)
	if err != nil {
		return stats, err
	}
	stats.Claimed = len(claimed)
	var firstErr error
	for _, task := range claimed {
		taskErr := ctx.Err()
		if taskErr == nil {
			taskErr = handler(ctx, task)
		}
		if taskErr != nil {
			if failErr := queue.Fail(task.ID, task.Attempt, sqlMutationDependencyQueueWorkerReason(taskErr)); failErr != nil {
				if firstErr == nil {
					firstErr = fmt.Errorf("fail mutation task %q: %w", task.ID, failErr)
				}
				continue
			}
			stats.Failed++
			if firstErr == nil {
				firstErr = fmt.Errorf("execute mutation task %q: %w", task.ID, taskErr)
			}
			continue
		}

		if completeErr := queue.Complete(task.ID, task.Attempt); completeErr != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("complete mutation task %q: %w", task.ID, completeErr)
			}
			continue
		}
		stats.Completed++
	}
	return stats, firstErr
}

func sqlMutationDependencyQueueWorkerReason(err error) string {
	reason := strings.TrimSpace(err.Error())
	if len(reason) > maxSQLMutationTaskErrorBytes {
		return reason[:maxSQLMutationTaskErrorBytes]
	}
	return reason
}
