package hatCache

import (
	"context"
	"errors"
	"net/http"
)

// AsyncCommandQueueResponse is returned by the authenticated queue status
// endpoint.
type AsyncCommandQueueResponse struct {
	Queue AsyncCommandQueueStats `json:"queue"`
}

// AsyncCommandQueueFlushResponse is returned after the current async command
// admission set has drained successfully.
type AsyncCommandQueueFlushResponse struct {
	Queue AsyncCommandQueueStats `json:"queue"`
}

func (handler *MonitoringHandler) handleAsyncCommandQueue(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w)
		return
	}
	if requestContextDone(w, r) {
		return
	}
	if handler.options.Journal == nil {
		writeJSONStatus(w, http.StatusConflict, commandError("journal is not configured"))
		return
	}
	response := AsyncCommandQueueResponse{Queue: handler.options.Journal.AsyncCommandQueueStats()}
	handler.auditHTTP(r, AuditEvent{Action: "async.command.queue", OK: true, Status: http.StatusOK, Details: map[string]interface{}{
		"pending":     response.Queue.Pending,
		"queue_depth": response.Queue.QueueDepth,
	}})
	writeJSON(w, response)
}

func (handler *MonitoringHandler) handleAsyncCommandQueueFlush(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeMethodNotAllowed(w)
		return
	}
	if requestContextDone(w, r) {
		return
	}
	if handler.options.Journal == nil {
		writeJSONStatus(w, http.StatusConflict, commandError("journal is not configured"))
		return
	}
	if err := handler.options.Journal.FlushAsyncCommands(r.Context()); err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, ErrCommandJournalAsyncUnsupported) {
			status = http.StatusConflict
		} else if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			status = http.StatusRequestTimeout
		}
		handler.auditHTTP(r, AuditEvent{Action: "async.command.queue.flush", OK: false, Status: status, Message: err.Error()})
		writeJSONStatus(w, status, commandError(err.Error()))
		return
	}
	response := AsyncCommandQueueFlushResponse{Queue: handler.options.Journal.AsyncCommandQueueStats()}
	handler.auditHTTP(r, AuditEvent{Action: "async.command.queue.flush", OK: true, Status: http.StatusOK, Details: map[string]interface{}{
		"pending":     response.Queue.Pending,
		"queue_depth": response.Queue.QueueDepth,
	}})
	writeJSON(w, response)
}
