package hatReplication

import (
	"errors"
	"fmt"
	"sync/atomic"
)

var (
	ErrChangefeedFrontierNil       = errors.New("hatriecache: changefeed frontier is nil")
	ErrChangefeedFrontierRegressed = errors.New("hatriecache: changefeed frontier regressed")
)

// ChangefeedProgress is a progress-only changefeed message. Sequence means
// that all updates through that sequence have been observed by the producer.
type ChangefeedProgress struct {
	Sequence   uint64 `json:"sequence"`
	Progressed bool   `json:"progressed"`
}

// ChangefeedFrontier tracks the greatest sequence that a producer has
// completely observed. It is safe for concurrent producers and consumers.
type ChangefeedFrontier struct {
	sequence uint64
}

// NewChangefeedFrontier creates a frontier at initial. Sequence zero is a
// valid empty frontier and can be advanced normally.
func NewChangefeedFrontier(initial uint64) *ChangefeedFrontier {
	return &ChangefeedFrontier{sequence: initial}
}

// Current returns the greatest observed sequence. A nil frontier returns zero.
func (frontier *ChangefeedFrontier) Current() uint64 {
	if frontier == nil {
		return 0
	}
	return atomic.LoadUint64(&frontier.sequence)
}

// Progress returns a progress message for the current frontier.
func (frontier *ChangefeedFrontier) Progress() (ChangefeedProgress, error) {
	if frontier == nil {
		return ChangefeedProgress{}, ErrChangefeedFrontierNil
	}
	return ChangefeedProgress{Sequence: atomic.LoadUint64(&frontier.sequence), Progressed: true}, nil
}

// Advance publishes a progress message at sequence. Equal advances are
// idempotent. A lower sequence is rejected so a consumer cannot checkpoint a
// stale watermark after a newer one has already been published.
func (frontier *ChangefeedFrontier) Advance(sequence uint64) (ChangefeedProgress, error) {
	if frontier == nil {
		return ChangefeedProgress{}, ErrChangefeedFrontierNil
	}
	for {
		current := atomic.LoadUint64(&frontier.sequence)
		if sequence < current {
			return ChangefeedProgress{Sequence: current, Progressed: true}, fmt.Errorf("%w: current=%d requested=%d", ErrChangefeedFrontierRegressed, current, sequence)
		}
		if sequence == current || atomic.CompareAndSwapUint64(&frontier.sequence, current, sequence) {
			return ChangefeedProgress{Sequence: sequence, Progressed: true}, nil
		}
	}
}
