package hatCache

import (
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	// DefaultMonitoringSlowCommandCaptureCapacity is used when capture is
	// enabled without an explicit capacity.
	DefaultMonitoringSlowCommandCaptureCapacity = 128
	MaxMonitoringSlowCommandCaptureCapacity     = 4096
	maxMonitoringSlowCommandFieldBytes          = 256
)

// SlowCommandRecord is a bounded, value-free record of one slow command.
// DurationNS and StartedAt are suitable for machine-readable diagnostics.
type SlowCommandRecord struct {
	StartedAt  time.Time `json:"started_at"`
	DurationNS int64     `json:"duration_ns"`
	Command    string    `json:"command"`
	Key        string    `json:"key,omitempty"`
	OK         bool      `json:"ok"`
	Status     int       `json:"status"`
}

// SlowCommandReport is returned by the read-only slow-command endpoint.
type SlowCommandReport struct {
	Enabled     bool                `json:"enabled"`
	ThresholdNS int64               `json:"threshold_ns"`
	Capacity    int                 `json:"capacity"`
	Entries     []SlowCommandRecord `json:"entries"`
}

type monitoringSlowCommandCapture struct {
	threshold time.Duration
	capacity  int

	mu      sync.RWMutex
	next    uint64
	entries []SlowCommandRecord
}

func normalizeMonitoringSlowCommandCaptureCapacity(value int) int {
	if value <= 0 {
		return DefaultMonitoringSlowCommandCaptureCapacity
	}
	if value > MaxMonitoringSlowCommandCaptureCapacity {
		return MaxMonitoringSlowCommandCaptureCapacity
	}
	return value
}

func newMonitoringSlowCommandCapture(threshold time.Duration, capacity int) *monitoringSlowCommandCapture {
	capacity = normalizeMonitoringSlowCommandCaptureCapacity(capacity)
	return &monitoringSlowCommandCapture{
		threshold: threshold,
		capacity:  capacity,
		entries:   make([]SlowCommandRecord, 0, capacity),
	}
}

func (capture *monitoringSlowCommandCapture) add(startedAt time.Time, request CacheCommandRequest, response CacheCommandResponse, status int) {
	if capture == nil {
		return
	}
	duration := time.Since(startedAt)
	if duration < capture.threshold {
		return
	}
	record := SlowCommandRecord{
		StartedAt:  startedAt.UTC(),
		DurationNS: duration.Nanoseconds(),
		Command:    normalizedCommand(request.Command),
		Key:        truncateMonitoringSlowCommandField(strings.TrimSpace(request.Key)),
		OK:         response.OK,
		Status:     status,
	}
	capture.mu.Lock()
	if len(capture.entries) < capture.capacity {
		capture.entries = append(capture.entries, record)
	} else {
		capture.entries[capture.next%uint64(capture.capacity)] = record
	}
	capture.next++
	capture.mu.Unlock()
}

func (capture *monitoringSlowCommandCapture) report() SlowCommandReport {
	if capture == nil {
		return SlowCommandReport{}
	}
	capture.mu.RLock()
	defer capture.mu.RUnlock()
	report := SlowCommandReport{
		Enabled:     true,
		ThresholdNS: capture.threshold.Nanoseconds(),
		Capacity:    capture.capacity,
		Entries:     make([]SlowCommandRecord, len(capture.entries)),
	}
	for index := range report.Entries {
		recordIndex := (capture.next - 1 - uint64(index)) % uint64(capture.capacity)
		if len(capture.entries) < capture.capacity {
			recordIndex = uint64(len(capture.entries) - 1 - index)
		}
		report.Entries[index] = capture.entries[recordIndex]
	}
	return report
}

func truncateMonitoringSlowCommandField(value string) string {
	if len(value) <= maxMonitoringSlowCommandFieldBytes {
		return value
	}
	return value[:maxMonitoringSlowCommandFieldBytes]
}

func (handler *MonitoringHandler) captureSlowCommand(startedAt time.Time, request CacheCommandRequest, response CacheCommandResponse, status int) {
	if handler == nil {
		return
	}
	handler.slowCommands.add(startedAt, request, response, status)
}

func (handler *MonitoringHandler) handleSlowCommands(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w)
		return
	}
	if requestContextDone(w, r) {
		return
	}
	if handler == nil || handler.slowCommands == nil {
		writeJSON(w, SlowCommandReport{})
		return
	}
	writeJSON(w, handler.slowCommands.report())
}
