package hatCache

import (
	"context"
	"errors"
	"net/http"
	"strings"
)

func (handler *MonitoringHandler) handleAsyncInsertQueues(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w)
		return
	}
	registry := handler.options.AsyncInsertQueues
	if registry == nil {
		writeJSONStatus(w, http.StatusServiceUnavailable, commandError("async insert queue monitoring is not configured"))
		return
	}
	writeJSON(w, AsyncInsertQueuesResponse{Queues: registry.Stats()})
}

func (handler *MonitoringHandler) handleAsyncInsertQueueFlush(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeMethodNotAllowed(w)
		return
	}
	registry := handler.options.AsyncInsertQueues
	if registry == nil {
		writeJSONStatus(w, http.StatusServiceUnavailable, commandError("async insert queue monitoring is not configured"))
		return
	}

	name := strings.TrimSpace(r.URL.Query().Get("name"))
	var err error
	if name == "" {
		err = registry.FlushAll(r.Context())
	} else {
		err = registry.Flush(r.Context(), name)
	}
	if err != nil {
		status := http.StatusServiceUnavailable
		switch {
		case errors.Is(err, ErrAsyncInsertQueueNotFound):
			status = http.StatusNotFound
		case errors.Is(err, ErrAsyncInsertQueueInvalidName):
			status = http.StatusBadRequest
		case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
			status = http.StatusRequestTimeout
		}
		handler.auditHTTP(r, AuditEvent{Action: "async_insert.flush", Key: name, OK: false, Status: status, Message: err.Error()})
		writeJSONStatus(w, status, commandError(err.Error()))
		return
	}

	handler.auditHTTP(r, AuditEvent{Action: "async_insert.flush", Key: name, OK: true, Status: http.StatusOK})
	writeJSON(w, AsyncInsertQueueFlushResponse{
		Flushed: true,
		Name:    name,
		Queues:  registry.Stats(),
	})
}
