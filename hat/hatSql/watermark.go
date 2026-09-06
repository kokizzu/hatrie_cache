package hatSql

import (
	"errors"
	"time"
)

var ErrWatermarkInvalid = errors.New("hatriecache: watermark input is invalid")

// MergeWatermarks returns the slowest source watermark. A downstream
// operator must wait for every source, so the minimum is the safe propagated
// frontier.
func MergeWatermarks(watermarks []time.Time) (time.Time, error) {
	if len(watermarks) == 0 {
		return time.Time{}, ErrWatermarkInvalid
	}
	slowest := watermarks[0]
	for _, watermark := range watermarks[1:] {
		if watermark.Before(slowest) {
			slowest = watermark
		}
	}
	return slowest, nil
}

// AdvanceWatermark applies monotonicity to one source frontier. A delayed or
// restarted source cannot move an already-published watermark backward.
func AdvanceWatermark(previous, candidate time.Time) time.Time {
	if candidate.Before(previous) {
		return previous
	}
	return candidate
}
