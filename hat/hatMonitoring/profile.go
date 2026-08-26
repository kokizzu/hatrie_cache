package hatMonitoring

import (
	"context"
	"errors"
	"fmt"
	"io"
	"runtime/pprof"
	"strings"
	"sync/atomic"
	"time"
)

const (
	MinCPUProfileDuration = time.Second
	MaxCPUProfileDuration = 30 * time.Second
	MaxProfileBytes       = 256 << 20
)

// ErrProfileTooLarge reports that profile output exceeded MaxProfileBytes.
// Its text is retained for compatibility with existing monitoring responses.
var ErrProfileTooLarge = errors.New("hatriecache: profile exceeds 256 MiB response limit")

// ProfileCapture serializes process-wide profile collection.
type ProfileCapture struct{ active atomic.Bool }

// TryStart marks profile capture active when no other capture is running.
func (capture *ProfileCapture) TryStart() bool {
	return capture != nil && capture.active.CompareAndSwap(false, true)
}

// Stop releases a previously acquired capture guard.
func (capture *ProfileCapture) Stop() {
	if capture != nil {
		capture.active.Store(false)
	}
}

// Active reports whether a profile capture is running.
func (capture *ProfileCapture) Active() bool {
	return capture != nil && capture.active.Load()
}

// ProfileRequest selects one runtime profile and an optional CPU duration.
type ProfileRequest struct {
	Type           string `json:"type"`
	DurationMillis int64  `json:"duration_millis,omitempty"`
}

// ProfileLimitedWriter stops output at a fixed response limit.
type ProfileLimitedWriter struct {
	writer    io.Writer
	remaining int64
	err       error
}

// NewProfileLimitedWriter constructs a bounded profile writer.
func NewProfileLimitedWriter(writer io.Writer, limit int64) *ProfileLimitedWriter {
	if limit < 0 {
		limit = 0
	}
	return &ProfileLimitedWriter{writer: writer, remaining: limit}
}

// Write implements io.Writer and returns ErrProfileTooLarge after writing the
// remaining permitted bytes.
func (writer *ProfileLimitedWriter) Write(data []byte) (int, error) {
	if writer == nil {
		return 0, errors.New("hatMonitoring: profile writer is nil")
	}
	if writer.err != nil {
		return 0, writer.err
	}
	if writer.writer == nil {
		writer.err = errors.New("hatMonitoring: profile writer target is nil")
		return 0, writer.err
	}
	if int64(len(data)) > writer.remaining {
		data = data[:writer.remaining]
		writer.err = ErrProfileTooLarge
	}
	written, err := writer.writer.Write(data)
	writer.remaining -= int64(written)
	if err != nil {
		writer.err = err
		return written, err
	}
	if writer.err != nil {
		return written, writer.err
	}
	return written, nil
}

// Remaining reports the remaining writable response bytes.
func (writer *ProfileLimitedWriter) Remaining() int64 {
	if writer == nil {
		return 0
	}
	return writer.remaining
}

// Err returns the first writer failure, including ErrProfileTooLarge.
func (writer *ProfileLimitedWriter) Err() error {
	if writer == nil {
		return nil
	}
	return writer.err
}

// ValidateProfileRequest normalizes and validates a profile request.
func ValidateProfileRequest(request ProfileRequest) (string, time.Duration, error) {
	profileType := strings.ToLower(strings.TrimSpace(request.Type))
	switch profileType {
	case "cpu":
		if request.DurationMillis < MinCPUProfileDuration.Milliseconds() || request.DurationMillis > MaxCPUProfileDuration.Milliseconds() {
			return "", 0, fmt.Errorf("CPU profile duration_millis must be between %d and %d", MinCPUProfileDuration.Milliseconds(), MaxCPUProfileDuration.Milliseconds())
		}
		return profileType, time.Duration(request.DurationMillis) * time.Millisecond, nil
	case "heap", "goroutine":
		if request.DurationMillis != 0 {
			return "", 0, fmt.Errorf("%s profile does not accept duration_millis", profileType)
		}
		return profileType, 0, nil
	default:
		return "", 0, errors.New("profile type must be cpu, heap, or goroutine")
	}
}

// CaptureProfile writes a runtime profile and stops CPU collection when ctx is
// canceled. The supplied writer must be a ProfileLimitedWriter.
func CaptureProfile(ctx context.Context, writer *ProfileLimitedWriter, profileType string, duration time.Duration) error {
	if writer == nil {
		return errors.New("hatMonitoring: profile writer is nil")
	}
	switch profileType {
	case "cpu":
		if err := pprof.StartCPUProfile(writer); err != nil {
			return err
		}
		timer := time.NewTimer(duration)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			pprof.StopCPUProfile()
			if writer.Err() != nil {
				return writer.Err()
			}
			return ctx.Err()
		}
		pprof.StopCPUProfile()
	case "heap", "goroutine":
		profile := pprof.Lookup(profileType)
		if profile == nil {
			return fmt.Errorf("runtime profile %q is unavailable", profileType)
		}
		if err := profile.WriteTo(writer, 0); err != nil {
			return err
		}
	}
	return writer.Err()
}
