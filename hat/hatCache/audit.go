package hatCache

import (
	"errors"
	"io"

	"hatrie_cache/hat/hatAudit"
)

// AuditEvent is retained at the root API for compatibility.
type AuditEvent = hatAudit.AuditEvent

// AuditLogger is retained at the root API for compatibility.
type AuditLogger = hatAudit.AuditLogger
type AuditQuery = hatAudit.Query
type WorkloadTrace = hatAudit.Trace
type WorkloadTraceRecorder = hatAudit.TraceRecorder
type WorkloadReplayReport = hatAudit.ReplayReport

const maxRecentAuditEvents = hatAudit.MaxRecentAuditEvents

func NewAuditLogger(writer io.Writer) *AuditLogger {
	return hatAudit.NewAuditLogger(writer)
}

func OpenAuditLogger(path string) (*AuditLogger, error) {
	return hatAudit.OpenAuditLogger(path)
}

// NewWorkloadTraceRecorder records an ordered replay workload. The optional
// writer receives JSONL trace records, and a non-positive limit retains all
// in-memory records.
func NewWorkloadTraceRecorder(writer io.Writer, limit int) *WorkloadTraceRecorder {
	return hatAudit.NewTraceRecorder(writer, limit)
}

// ExecuteTracedCommand executes one command and durably records its exact
// input and response when a recorder is supplied.
func (ht *HatTrie) ExecuteTracedCommand(recorder *WorkloadTraceRecorder, request CacheCommandRequest) (CacheCommandResponse, error) {
	response := ht.ExecuteCommand(request)
	if recorder == nil {
		return response, nil
	}
	if err := recorder.Record(request, response); err != nil {
		return response, err
	}
	return response, nil
}

// ReplayWorkloadTrace re-executes a trace on trie and rejects the first
// response mismatch, making it suitable for deterministic regression checks.
func ReplayWorkloadTrace(trie *HatTrie, traces []WorkloadTrace) (WorkloadReplayReport, error) {
	if trie == nil {
		return WorkloadReplayReport{}, errors.New("hatriecache: trace replay trie is nil")
	}
	return hatAudit.Replay(traces, trie.ExecuteCommand)
}
