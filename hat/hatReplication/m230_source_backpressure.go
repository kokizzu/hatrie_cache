package hatReplication

import "errors"

const (
	// MaxSpaceChangefeedMaxLag bounds the number of unpublished acknowledgements
	// a backpressure-enabled feed may allow a subscriber to trail.
	MaxSpaceChangefeedMaxLag = 1 << 20
)

var (
	// ErrSpaceChangefeedBackpressure means the next event would exceed the
	// configured lag from the slowest active subscriber. The caller may retry
	// after advancing a subscription or reducing the write rate.
	ErrSpaceChangefeedBackpressure = errors.New("hatriecache: space changefeed downstream backpressure")
)

// SpaceChangefeedBackpressureOptions enables caller-visible, retryable
// backpressure based on acknowledged subscriber frontiers. It is disabled by
// default so existing feeds retain their nonblocking overflow behavior.
type SpaceChangefeedBackpressureOptions struct {
	Enabled bool
	MaxLag  uint64
}
