package hatReplication

import (
	"context"
	"errors"
	"fmt"
)

const (
	// DefaultSpaceChangefeedInspectMaxEvents bounds a one-shot inspection when
	// the caller does not provide an event limit.
	DefaultSpaceChangefeedInspectMaxEvents = 128
	// DefaultSpaceChangefeedInspectMaxBytes bounds retained payload bytes for a
	// one-shot inspection when the caller does not provide a byte limit.
	DefaultSpaceChangefeedInspectMaxBytes int64 = 1 << 20
	MaxSpaceChangefeedInspectMaxEvents          = 4096
	MaxSpaceChangefeedInspectMaxBytes    int64 = 16 << 20
)

var ErrSpaceChangefeedInspectLimit = errors.New("hatriecache: space changefeed inspection limit is invalid or too small")

// SpaceChangefeedInspectOptions bounds a read-only one-shot history inspection.
// AfterSequence is exclusive; zero starts at the oldest retained event.
// Zero MaxEvents and MaxBytes select the bounded defaults above.
type SpaceChangefeedInspectOptions struct {
	AfterSequence uint64
	MaxEvents     int
	MaxBytes      int64
}

// SpaceChangefeedInspection is a bounded snapshot of retained feed history.
// NextSequence is the sequence that would be assigned to the next published
// event. More reports that retained events were omitted by a bound.
type SpaceChangefeedInspection struct {
	Events                 []SpaceChangefeedEvent
	FirstAvailableSequence uint64
	NextSequence           uint64
	More                   bool
}

// Inspect returns retained events after an offset without creating a consumer
// or changing a checkpoint. Returned events own their payload byte slices.
func (feed *SpaceChangefeed) Inspect(ctx context.Context, options SpaceChangefeedInspectOptions) (SpaceChangefeedInspection, error) {
	if feed == nil {
		return SpaceChangefeedInspection{}, ErrSpaceChangefeedNil
	}
	if ctx == nil {
		return SpaceChangefeedInspection{}, ErrSpaceChangefeedOptionsInvalid
	}
	if err := ctx.Err(); err != nil {
		return SpaceChangefeedInspection{}, err
	}

	maxEvents, maxBytes, err := normalizeSpaceChangefeedInspectOptions(options)
	if err != nil {
		return SpaceChangefeedInspection{}, err
	}

	feed.mu.Lock()
	defer feed.mu.Unlock()
	if feed.closed {
		return SpaceChangefeedInspection{}, ErrSpaceChangefeedClosed
	}
	if err := ctx.Err(); err != nil {
		return SpaceChangefeedInspection{}, err
	}
	if options.AfterSequence > feed.nextSequence {
		return SpaceChangefeedInspection{}, ErrSpaceChangefeedCheckpointAhead
	}

	inspection := SpaceChangefeedInspection{
		NextSequence: nextSpaceChangefeedSequence(feed.nextSequence),
	}
	if feed.eventCount == 0 {
		return inspection, nil
	}
	inspection.FirstAvailableSequence = feed.eventAtLocked(0).Sequence
	if options.AfterSequence < inspection.FirstAvailableSequence-1 {
		return SpaceChangefeedInspection{}, fmt.Errorf(
			"%w: checkpoint=%d oldest=%d",
			ErrSpaceChangefeedHistoryGap,
			options.AfterSequence,
			inspection.FirstAvailableSequence,
		)
	}

	var selectedBytes int64
	for index := 0; index < feed.eventCount; index++ {
		if err := ctx.Err(); err != nil {
			return SpaceChangefeedInspection{}, err
		}
		event := feed.eventAtLocked(index)
		if event.Sequence <= options.AfterSequence {
			continue
		}
		if len(inspection.Events) >= maxEvents {
			inspection.More = true
			break
		}
		eventBytes := spaceChangefeedEventBytesUnchecked(event)
		if eventBytes > maxBytes || inspectionBytesWouldExceed(selectedBytes, eventBytes, maxBytes) {
			if len(inspection.Events) == 0 {
				return SpaceChangefeedInspection{}, fmt.Errorf(
					"%w: event bytes=%d max=%d",
					ErrSpaceChangefeedInspectLimit,
					eventBytes,
					maxBytes,
				)
			}
			inspection.More = true
			break
		}
		if inspection.Events == nil {
			capacity := feed.eventCount - index
			if capacity > maxEvents {
				capacity = maxEvents
			}
			inspection.Events = make([]SpaceChangefeedEvent, 0, capacity)
		}
		inspection.Events = append(inspection.Events, cloneSpaceChangefeedEvent(event))
		selectedBytes += eventBytes
	}
	return inspection, nil
}

func normalizeSpaceChangefeedInspectOptions(options SpaceChangefeedInspectOptions) (int, int64, error) {
	maxEvents := options.MaxEvents
	if maxEvents == 0 {
		maxEvents = DefaultSpaceChangefeedInspectMaxEvents
	}
	maxBytes := options.MaxBytes
	if maxBytes == 0 {
		maxBytes = DefaultSpaceChangefeedInspectMaxBytes
	}
	if maxEvents < 1 || maxEvents > MaxSpaceChangefeedInspectMaxEvents || maxBytes < 1 || maxBytes > MaxSpaceChangefeedInspectMaxBytes {
		return 0, 0, ErrSpaceChangefeedInspectLimit
	}
	return maxEvents, maxBytes, nil
}

func nextSpaceChangefeedSequence(sequence uint64) uint64 {
	if sequence == ^uint64(0) {
		return sequence
	}
	return sequence + 1
}

func inspectionBytesWouldExceed(current, next, max int64) bool {
	return next > max || current > max-next
}
