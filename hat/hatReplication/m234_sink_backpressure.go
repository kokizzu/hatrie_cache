package hatReplication

import "errors"

const (
	defaultSinkRetryHighWatermarkNumerator   = int64(4)
	defaultSinkRetryHighWatermarkDenominator = int64(5)
)

var ErrSinkRetryBackpressure = errors.New("hatriecache: sink retry queue is applying backpressure")

// SinkRetryBackpressureOptions enables hysteresis before the hard retry queue
// bound. It is disabled by default. High watermarks enter throttling; all
// dimensions must fall below their low watermarks before admission reopens.
type SinkRetryBackpressureOptions struct {
	Enabled     bool
	HighPending int
	LowPending  int
	HighBytes   int64
	LowBytes    int64
}

// SinkRetryBackpressureStatus exposes the configured and current admission
// state for monitoring and upstream flow control.
type SinkRetryBackpressureStatus struct {
	Enabled       bool
	Backpressured bool
	HighPending   int
	LowPending    int
	HighBytes     int64
	LowBytes      int64
}

func normalizeSinkRetryBackpressureOptions(options SinkRetryBackpressureOptions, maxPending int, maxBytes int64) (SinkRetryBackpressureOptions, error) {
	if !options.Enabled {
		return SinkRetryBackpressureOptions{}, nil
	}
	if options.HighPending == 0 {
		options.HighPending = watermarkInt(maxPending, defaultSinkRetryHighWatermarkNumerator, defaultSinkRetryHighWatermarkDenominator)
	}
	if options.LowPending == 0 {
		options.LowPending = options.HighPending / 2
	}
	if options.HighBytes == 0 {
		options.HighBytes = watermarkBytes(maxBytes, defaultSinkRetryHighWatermarkNumerator, defaultSinkRetryHighWatermarkDenominator)
	}
	if options.LowBytes == 0 {
		options.LowBytes = options.HighBytes / 2
	}
	if options.HighPending < 1 || options.HighPending > maxPending || options.LowPending < 0 || options.LowPending >= options.HighPending || options.HighBytes < 1 || options.HighBytes > maxBytes || options.LowBytes < 0 || options.LowBytes >= options.HighBytes {
		return SinkRetryBackpressureOptions{}, ErrSinkRetryInvalid
	}
	return options, nil
}

func watermarkInt(value int, numerator, denominator int64) int {
	watermark := int((int64(value) * numerator) / denominator)
	if watermark < 1 {
		return 1
	}
	return watermark
}

func watermarkBytes(value, numerator, denominator int64) int64 {
	watermark := (value * numerator) / denominator
	if watermark < 1 {
		return 1
	}
	return watermark
}

func (queue *SinkRetryQueue) updateBackpressureLocked() {
	options := queue.options.Backpressure
	if !options.Enabled {
		queue.backpressured = false
		return
	}
	if queue.backpressured {
		if queue.bytes <= options.LowBytes && len(queue.items) <= options.LowPending {
			queue.backpressured = false
		}
		return
	}
	if queue.bytes >= options.HighBytes || len(queue.items) >= options.HighPending {
		queue.backpressured = true
	}
}

// BackpressureStatus returns the configured watermarks and current state.
func (queue *SinkRetryQueue) BackpressureStatus() SinkRetryBackpressureStatus {
	if queue == nil {
		return SinkRetryBackpressureStatus{}
	}
	queue.mu.Lock()
	defer queue.mu.Unlock()
	options := queue.options.Backpressure
	return SinkRetryBackpressureStatus{
		Enabled:       options.Enabled,
		Backpressured: queue.backpressured,
		HighPending:   options.HighPending,
		LowPending:    options.LowPending,
		HighBytes:     options.HighBytes,
		LowBytes:      options.LowBytes,
	}
}
