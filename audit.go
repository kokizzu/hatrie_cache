package hatriecache

import (
	"io"
	"os"
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
}

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
	if logger == nil || logger.writer == nil {
		return nil
	}
	logger.mu.Lock()
	defer logger.mu.Unlock()
	if event.Time == "" {
		event.Time = logger.now().UTC().Format(time.RFC3339Nano)
	}
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}
	if _, err := logger.writer.Write(append(data, '\n')); err != nil {
		return err
	}
	return nil
}

func (logger *AuditLogger) Close() error {
	if logger == nil || logger.closer == nil {
		return nil
	}
	return logger.closer.Close()
}
