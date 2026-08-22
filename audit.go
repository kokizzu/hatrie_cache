package hatriecache

import (
	"io"

	"hatrie_cache/hat/hatAudit"
)

// AuditEvent is retained at the root API for compatibility.
type AuditEvent = hatAudit.AuditEvent

// AuditLogger is retained at the root API for compatibility.
type AuditLogger = hatAudit.AuditLogger

const maxRecentAuditEvents = hatAudit.MaxRecentAuditEvents

func NewAuditLogger(writer io.Writer) *AuditLogger {
	return hatAudit.NewAuditLogger(writer)
}

func OpenAuditLogger(path string) (*AuditLogger, error) {
	return hatAudit.OpenAuditLogger(path)
}
