package hatPipeline

import (
	"context"
	"errors"
	"time"
)

var (
	// ErrSchedulerTaskOptionsInvalid reports a negative task timeout.
	ErrSchedulerTaskOptionsInvalid = errors.New("hatPipeline: invalid task options")
)

// TaskOptions configures the opt-in context policy for one submitted task.
// Timeout starts when SubmitWithOptions is called, including queue wait. A
// Deadline is absolute, and the earlier of Deadline and Timeout wins. Set
// PropagateCaller to forward cancellation from the SubmitWithOptions context
// into the task after admission.
type TaskOptions struct {
	Deadline        time.Time
	Timeout         time.Duration
	PropagateCaller bool
}

func wrapTaskWithOptions(ctx context.Context, options TaskOptions, task Task) (Task, error) {
	if options.Timeout < 0 {
		return nil, ErrSchedulerTaskOptionsInvalid
	}
	if options.Deadline.IsZero() && options.Timeout == 0 && !options.PropagateCaller {
		return task, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	deadline := options.Deadline
	if options.Timeout > 0 {
		timeoutDeadline := time.Now().Add(options.Timeout)
		if deadline.IsZero() || timeoutDeadline.Before(deadline) {
			deadline = timeoutDeadline
		}
	}
	return func(schedulerContext context.Context) error {
		taskContext := schedulerContext
		var cancel context.CancelFunc
		if options.PropagateCaller {
			taskContext, cancel = context.WithCancel(taskContext)
			defer cancel()
			stopCaller := context.AfterFunc(ctx, cancel)
			defer stopCaller()
		}
		if !deadline.IsZero() {
			var stopDeadline context.CancelFunc
			taskContext, stopDeadline = context.WithDeadline(taskContext, deadline)
			defer stopDeadline()
		}
		if err := task(taskContext); err != nil {
			return err
		}
		return taskContext.Err()
	}, nil
}
