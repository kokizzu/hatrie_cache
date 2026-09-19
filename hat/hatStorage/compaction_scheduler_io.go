package hatStorage

import (
	"context"
	"math/bits"
	"sync"
	"time"
)

type compactionIOThrottle struct {
	mu              sync.Mutex
	bytesPerSecond  uint64
	nextAvailable   time.Time
	throttledTasks  uint64
	throttledBytes  uint64
	waitNanoseconds uint64
}

type compactionIOThrottleStats struct {
	bytesPerSecond  uint64
	throttledTasks  uint64
	throttledBytes  uint64
	waitNanoseconds uint64
}

type compactionSchedulerIOState struct {
	throttle  *compactionIOThrottle
	estimates map[string]uint64
}

func newCompactionSchedulerIOState(bytesPerSecond uint64) *compactionSchedulerIOState {
	if bytesPerSecond == 0 {
		return nil
	}
	return &compactionSchedulerIOState{
		throttle: newCompactionIOThrottle(bytesPerSecond),
	}
}

func newCompactionIOThrottle(bytesPerSecond uint64) *compactionIOThrottle {
	if bytesPerSecond == 0 {
		return nil
	}
	return &compactionIOThrottle{bytesPerSecond: bytesPerSecond}
}

func (throttle *compactionIOThrottle) wait(ctx context.Context, bytes uint64) error {
	if throttle == nil || bytes == 0 {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	now := time.Now()
	duration := compactionIODuration(bytes, throttle.bytesPerSecond)
	throttle.mu.Lock()
	start := throttle.nextAvailable
	if start.IsZero() || start.Before(now) {
		start = now
	}
	wait := start.Sub(now)
	throttle.nextAvailable = start.Add(duration)
	if wait > 0 {
		throttle.throttledTasks++
		throttle.throttledBytes = saturatingCompactionAdd(throttle.throttledBytes, bytes)
		throttle.waitNanoseconds = saturatingCompactionAdd(throttle.waitNanoseconds, uint64(wait))
	}
	throttle.mu.Unlock()

	if wait <= 0 {
		return nil
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (throttle *compactionIOThrottle) stats() compactionIOThrottleStats {
	if throttle == nil {
		return compactionIOThrottleStats{}
	}
	throttle.mu.Lock()
	defer throttle.mu.Unlock()
	return compactionIOThrottleStats{
		bytesPerSecond:  throttle.bytesPerSecond,
		throttledTasks:  throttle.throttledTasks,
		throttledBytes:  throttle.throttledBytes,
		waitNanoseconds: throttle.waitNanoseconds,
	}
}

func compactionIODuration(bytes, bytesPerSecond uint64) time.Duration {
	if bytes == 0 || bytesPerSecond == 0 {
		return 0
	}
	const nanosPerSecond = uint64(time.Second)
	const maxDurationNanos = uint64(^uint64(0) >> 1)
	seconds := bytes / bytesPerSecond
	if seconds > maxDurationNanos/nanosPerSecond {
		return time.Duration(maxDurationNanos)
	}
	nanos := seconds * nanosPerSecond
	remainder := bytes % bytesPerSecond
	if remainder != 0 {
		hi, lo := bits.Mul64(remainder, nanosPerSecond)
		extra, fraction := bits.Div64(hi, lo, bytesPerSecond)
		if fraction != 0 {
			extra++
		}
		if extra > maxDurationNanos-nanos {
			return time.Duration(maxDurationNanos)
		}
		nanos += extra
	}
	return time.Duration(nanos)
}

func saturatingCompactionAdd(left, right uint64) uint64 {
	if ^uint64(0)-left < right {
		return ^uint64(0)
	}
	return left + right
}
