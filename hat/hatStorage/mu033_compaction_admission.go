package hatStorage

import (
	"context"
	"errors"
)

var (
	// ErrCompactionBoundaryAdmissionNil reports a missing retention admission
	// implementation.
	ErrCompactionBoundaryAdmissionNil = errors.New("hatriecache: compaction boundary admission is nil")
	// ErrCompactionBoundaryPermitNil reports an admission implementation that
	// returned success without a release handle.
	ErrCompactionBoundaryPermitNil = errors.New("hatriecache: compaction boundary permit is nil")
)

// CompactionBoundaryPermit releases a storage engine's temporary reservation
// over one compaction boundary. Implementations should make Release idempotent
// so callers can use it from a deferred cleanup path.
type CompactionBoundaryPermit interface {
	Release() error
}

// CompactionBoundaryAdmission reserves a boundary immediately before the
// storage callback runs. It closes the race between a safe-boundary check and
// the callback's first delete.
type CompactionBoundaryAdmission interface {
	BeginCompaction(context.Context, string, uint64) (CompactionBoundaryPermit, error)
}

// SubmitWithBoundaryAdmission submits a compaction whose callback is admitted
// against a retention boundary at execution time. The supplied context is
// checked before queueing; the context passed to Run controls the actual
// admission wait. The permit is always released after the callback returns,
// including when the callback returns an error or panics.
func (controller *CompactionController) SubmitWithBoundaryAdmission(
	ctx context.Context,
	request CompactionRequest,
	admission CompactionBoundaryAdmission,
	frontierID string,
	boundary uint64,
) (CompactionJob, bool, error) {
	if controller == nil {
		return CompactionJob{}, false, ErrCompactionControllerNil
	}
	if admission == nil {
		return CompactionJob{}, false, ErrCompactionBoundaryAdmissionNil
	}
	if request.Run == nil {
		return CompactionJob{}, false, ErrCompactionRequestInvalid
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return CompactionJob{}, false, err
	}

	original := request.Run
	request.Run = func(runCtx context.Context) (runErr error) {
		permit, err := admission.BeginCompaction(runCtx, frontierID, boundary)
		if err != nil {
			return err
		}
		if permit == nil {
			return ErrCompactionBoundaryPermitNil
		}
		defer func() {
			releaseErr := permit.Release()
			if runErr == nil {
				runErr = releaseErr
			}
		}()
		return original(runCtx)
	}
	return controller.Submit(request)
}
