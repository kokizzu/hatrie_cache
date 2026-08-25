// Package hatAudit provides concurrency-safe JSONL audit logging.
package hatAudit

import (
	"errors"
	"io"
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

type AuditLogger struct {
	mu     sync.Mutex
	writer io.Writer
	closer io.Closer
	now    func() time.Time
	recent []AuditEvent
}

const MaxRecentAuditEvents = 128

func NewAuditLogger(writer io.Writer) *AuditLogger {
	return &AuditLogger{writer: writer, now: time.Now}
}

func OpenAuditLogger(path string) (*AuditLogger, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	logger := NewAuditLogger(file)
	logger.closer = file
	return logger, nil
}

func (logger *AuditLogger) Log(event AuditEvent) error {
	if logger == nil {
		return nil
	}
	logger.mu.Lock()
	defer logger.mu.Unlock()
	if event.Time == "" {
		event.Time = logger.now().UTC().Format(time.RFC3339Nano)
	}
	logger.recent = append(logger.recent, event)
	if len(logger.recent) > MaxRecentAuditEvents {
		copy(logger.recent, logger.recent[len(logger.recent)-MaxRecentAuditEvents:])
		logger.recent = logger.recent[:MaxRecentAuditEvents]
	}
	if logger.writer != nil {
		data, err := json.Marshal(event)
		if err != nil {
			return err
		}
		if _, err := logger.writer.Write(append(data, '\n')); err != nil {
			return err
		}
	}
	return nil
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
	copy(out, logger.recent[len(logger.recent)-limit:])
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
		event := logger.recent[index]
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
