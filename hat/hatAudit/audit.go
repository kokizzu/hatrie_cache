// Package hatAudit provides concurrency-safe JSONL audit logging.
package hatAudit

import (
	"errors"
	"io"
	"math"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	json "github.com/goccy/go-json"
)

type AuditEvent struct {
	Time       string                 `json:"time"`
	Node       string                 `json:"node,omitempty"`
	Protocol   string                 `json:"protocol,omitempty"`
	RemoteAddr string                 `json:"remote_addr,omitempty"`
	Method     string                 `json:"method,omitempty"`
	Path       string                 `json:"path,omitempty"`
	Action     string                 `json:"action"`
	Command    string                 `json:"command,omitempty"`
	Key        string                 `json:"key,omitempty"`
	OK         bool                   `json:"ok"`
	Status     int                    `json:"status,omitempty"`
	Message    string                 `json:"message,omitempty"`
	Details    map[string]interface{} `json:"details,omitempty"`
}

// AuditSink receives accepted audit events synchronously from AuditLogger.
// Implementations should avoid calling the same logger from WriteAuditEvent.
type AuditSink interface {
	WriteAuditEvent(AuditEvent) error
}

// AuditSinkFunc adapts a function into an AuditSink.
type AuditSinkFunc func(AuditEvent) error

// WriteAuditEvent implements AuditSink.
func (sink AuditSinkFunc) WriteAuditEvent(event AuditEvent) error {
	return sink(event)
}

// AuditLoggerOptions configures optional success-event sampling and export
// sinks. SuccessSampleRate == 0 disables sampling and preserves every event;
// values between 0 and 1 sample successful events, while failures are always
// retained. A rate of 1 retains every successful event.
type AuditLoggerOptions struct {
	SuccessSampleRate float64
	Sinks             []AuditSink
}

type AuditLogger struct {
	mu                   sync.Mutex
	writer               io.Writer
	closer               io.Closer
	now                  func() time.Time
	recent               []AuditEvent
	recentStart          int
	sinks                []AuditSink
	successSampleRate    float64
	sampleSequence       uint64
	sampledSuccessEvents uint64
}

const MaxRecentAuditEvents = 128

func NewAuditLogger(writer io.Writer) *AuditLogger {
	return &AuditLogger{writer: writer, now: time.Now}
}

// NewAuditLoggerWithOptions creates an audit logger with optional success
// sampling and structured export sinks.
func NewAuditLoggerWithOptions(writer io.Writer, options AuditLoggerOptions) (*AuditLogger, error) {
	if err := validateAuditLoggerOptions(options); err != nil {
		return nil, err
	}
	return &AuditLogger{
		writer:            writer,
		now:               time.Now,
		sinks:             append([]AuditSink(nil), options.Sinks...),
		successSampleRate: options.SuccessSampleRate,
	}, nil
}

func OpenAuditLogger(path string) (*AuditLogger, error) {
	return OpenAuditLoggerWithOptions(path, AuditLoggerOptions{})
}

// OpenAuditLoggerWithOptions opens a mode-0600 append-only audit log with
// optional success sampling and structured export sinks.
func OpenAuditLoggerWithOptions(path string, options AuditLoggerOptions) (*AuditLogger, error) {
	if err := validateAuditLoggerOptions(options); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	logger, err := NewAuditLoggerWithOptions(file, options)
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	logger.closer = file
	return logger, nil
}

func (logger *AuditLogger) Log(event AuditEvent) error {
	if logger == nil {
		return nil
	}
	logger.mu.Lock()
	defer logger.mu.Unlock()
	if event.OK && logger.shouldSampleSuccess() {
		logger.sampledSuccessEvents++
		return nil
	}
	if event.Time == "" {
		event.Time = logger.now().UTC().Format(time.RFC3339Nano)
	}
	logger.appendRecent(event)
	var firstErr error
	if logger.writer != nil {
		data, err := json.Marshal(event)
		if err != nil {
			return err
		}
		if _, err := logger.writer.Write(append(data, '\n')); err != nil {
			firstErr = err
		}
	}
	for _, sink := range logger.sinks {
		if err := sink.WriteAuditEvent(event); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (logger *AuditLogger) appendRecent(event AuditEvent) {
	if len(logger.recent) < MaxRecentAuditEvents {
		logger.recent = append(logger.recent, event)
		return
	}
	logger.recent[logger.recentStart] = event
	logger.recentStart++
	if logger.recentStart == MaxRecentAuditEvents {
		logger.recentStart = 0
	}
}

func (logger *AuditLogger) recentEventAt(oldestOffset int) AuditEvent {
	index := oldestOffset
	if len(logger.recent) == MaxRecentAuditEvents {
		index += logger.recentStart
		if index >= len(logger.recent) {
			index -= len(logger.recent)
		}
	}
	return logger.recent[index]
}

// SampledSuccessEvents reports how many successful events were omitted by the
// configured sample rate.
func (logger *AuditLogger) SampledSuccessEvents() uint64 {
	if logger == nil {
		return 0
	}
	logger.mu.Lock()
	defer logger.mu.Unlock()
	return logger.sampledSuccessEvents
}

func validateAuditLoggerOptions(options AuditLoggerOptions) error {
	if math.IsNaN(options.SuccessSampleRate) || options.SuccessSampleRate < 0 || options.SuccessSampleRate > 1 {
		return errors.New("hatriecache: audit success sample rate must be between 0 and 1")
	}
	for _, sink := range options.Sinks {
		if sink == nil {
			return errors.New("hatriecache: audit sink must not be nil")
		}
	}
	return nil
}

func (logger *AuditLogger) shouldSampleSuccess() bool {
	if logger.successSampleRate <= 0 || logger.successSampleRate >= 1 {
		return false
	}
	logger.sampleSequence++
	const sampleScale = uint64(1_000_000)
	threshold := uint64(logger.successSampleRate * float64(sampleScale))
	return auditSampleHash(logger.sampleSequence)%sampleScale >= threshold
}

func auditSampleHash(value uint64) uint64 {
	value += 0x9e3779b97f4a7c15
	value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9
	value = (value ^ (value >> 27)) * 0x94d049bb133111eb
	return value ^ (value >> 31)
}

func (logger *AuditLogger) Recent(limit int) []AuditEvent {
	if logger == nil {
		return nil
	}
	logger.mu.Lock()
	defer logger.mu.Unlock()
	if limit <= 0 || limit > len(logger.recent) {
		limit = len(logger.recent)
	}
	out := make([]AuditEvent, limit)
	start := len(logger.recent) - limit
	for index := range out {
		out[index] = logger.recentEventAt(start + index)
	}
	sort.SliceStable(out, func(i, j int) bool {
		return out[i].Time > out[j].Time
	})
	return out
}

// Query returns a newest-first independent copy of retained events that match
// query. It never reads historical disk logs, keeping request latency bounded.
func (logger *AuditLogger) Query(query Query) ([]AuditEvent, error) {
	if logger == nil {
		return nil, nil
	}
	if query.Limit < 0 || query.Limit > MaxRecentAuditEvents {
		return nil, errors.New("hatriecache: audit query limit is invalid")
	}
	query.Action = strings.TrimSpace(query.Action)
	query.Command = strings.TrimSpace(query.Command)
	query.KeyPrefix = strings.TrimSpace(query.KeyPrefix)
	logger.mu.Lock()
	defer logger.mu.Unlock()
	out := make([]AuditEvent, 0, len(logger.recent))
	for index := len(logger.recent) - 1; index >= 0; index-- {
		event := logger.recentEventAt(index)
		if !matchesQuery(event, query) {
			continue
		}
		out = append(out, event)
		if query.Limit > 0 && len(out) == query.Limit {
			break
		}
	}
	return out, nil
}

func (logger *AuditLogger) Close() error {
	if logger == nil || logger.closer == nil {
		return nil
	}
	return logger.closer.Close()
}
