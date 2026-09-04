package hatCache

import (
	"errors"
	"net/http"
	"strings"
	"time"
)

const (
	// DefaultMonitoringAsyncCommandStatusCapacity bounds the in-memory HTTP
	// receipt registry when async command submission is enabled.
	DefaultMonitoringAsyncCommandStatusCapacity = 1024
	MaxMonitoringAsyncCommandStatusCapacity     = 1 << 16
)

var (
	ErrMonitoringAsyncCommandStatusFull          = errors.New("hatriecache: async command status registry is full")
	ErrMonitoringAsyncCommandRequiresJournal     = errors.New("hatriecache: async HTTP commands require a journal")
	ErrMonitoringAsyncCommandRequiresIdempotency = errors.New("hatriecache: async HTTP commands require journal idempotency")
	ErrMonitoringAsyncCommandIncompatible        = errors.New("hatriecache: async HTTP commands are incompatible with leader enforcement or replication")
)

// AsyncCommandAcceptedResponse is returned by the opt-in HTTP async command
// admission path. Completion is available from the status endpoint using the
// same idempotency key.
type AsyncCommandAcceptedResponse struct {
	Accepted       bool                  `json:"accepted"`
	Status         string                `json:"status"`
	IdempotencyKey string                `json:"idempotency_key"`
	Response       *CacheCommandResponse `json:"response,omitempty"`
}

// AsyncCommandStatusResponse reports the bounded HTTP async command receipt.
// A completed response is retained in memory until the status registry evicts
// it, and successful responses are also recoverable from the journal.
type AsyncCommandStatusResponse struct {
	IdempotencyKey string                `json:"idempotency_key"`
	Status         string                `json:"status"`
	Response       *CacheCommandResponse `json:"response,omitempty"`
}

type monitoringAsyncCommandEntry struct {
	check      commandIdempotencyCheck
	submission *CommandJournalSubmission
	request    CacheCommandRequest
	startedAt  time.Time
	completed  bool
	response   CacheCommandResponse
}

func normalizeMonitoringAsyncCommandStatusCapacity(value int) int {
	if value <= 0 {
		return DefaultMonitoringAsyncCommandStatusCapacity
	}
	if value > MaxMonitoringAsyncCommandStatusCapacity {
		return MaxMonitoringAsyncCommandStatusCapacity
	}
	return value
}

func monitoringAsyncCommandRequested(r *http.Request) bool {
	if r == nil {
		return false
	}
	for _, value := range r.Header.Values("X-Hatrie-Async") {
		if strings.EqualFold(strings.TrimSpace(value), "true") || strings.TrimSpace(value) == "1" {
			return true
		}
	}
	for _, value := range strings.Split(r.Header.Get("Prefer"), ",") {
		token := strings.TrimSpace(strings.SplitN(value, "=", 2)[0])
		if strings.EqualFold(token, "respond-async") {
			return true
		}
	}
	return false
}

func (handler *MonitoringHandler) handleAsyncCommandSubmission(w http.ResponseWriter, r *http.Request, request CacheCommandRequest) {
	accepted, err := handler.admitAsyncCommand(request)
	if err != nil {
		status := monitoringAsyncCommandErrorStatus(err)
		handler.auditHTTP(r, AuditEvent{Action: "command.async", Command: normalizedCommand(request.Command), Key: strings.TrimSpace(request.Key), OK: false, Status: status, Message: err.Error()})
		writeJSONStatus(w, status, commandError(err.Error()))
		return
	}
	status := http.StatusAccepted
	if accepted.Status == "completed" {
		status = http.StatusOK
	}
	handler.auditHTTP(r, AuditEvent{Action: "command.async", Command: normalizedCommand(request.Command), Key: strings.TrimSpace(request.Key), OK: true, Status: status, Message: accepted.Status})
	writeJSONStatus(w, status, accepted)
}

func monitoringAsyncCommandErrorStatus(err error) int {
	if errors.Is(err, ErrCommandJournalAsyncQueueFull) || errors.Is(err, ErrMonitoringAsyncCommandStatusFull) {
		return http.StatusTooManyRequests
	}
	if errors.Is(err, ErrCommandJournalAsyncUnsupported) || errors.Is(err, ErrCommandJournalClosed) {
		return http.StatusServiceUnavailable
	}
	return http.StatusConflict
}

func (handler *MonitoringHandler) admitAsyncCommand(request CacheCommandRequest) (AsyncCommandAcceptedResponse, error) {
	if handler == nil || handler.trie == nil {
		return AsyncCommandAcceptedResponse{}, ErrNilHatTrie
	}
	journal := handler.options.Journal
	if journal == nil {
		return AsyncCommandAcceptedResponse{}, ErrMonitoringAsyncCommandRequiresJournal
	}
	if !commandShouldJournal(request) {
		return AsyncCommandAcceptedResponse{}, ErrCommandJournalAsyncWriteOnly
	}
	if asyncCommandContainsInternal(request) {
		return AsyncCommandAcceptedResponse{}, ErrMonitoringAsyncCommandIncompatible
	}
	if handler.options.EnforceLeaderWrites || handler.options.Replicator != nil {
		return AsyncCommandAcceptedResponse{}, ErrMonitoringAsyncCommandIncompatible
	}
	key := strings.TrimSpace(request.IdempotencyKey)
	if key == "" {
		return AsyncCommandAcceptedResponse{}, errors.New("hatriecache: async HTTP commands require idempotency_key")
	}
	if err := validateCommandIdempotencyKey(key); err != nil {
		return AsyncCommandAcceptedResponse{}, err
	}
	request.IdempotencyKey = key
	check, err := journal.idempotencyCheck(request)
	if err != nil {
		return AsyncCommandAcceptedResponse{}, err
	}
	if !check.enabled {
		return AsyncCommandAcceptedResponse{}, ErrMonitoringAsyncCommandRequiresIdempotency
	}
	if response, duplicate, err := journal.lookupAsyncCommandIdempotency(check); err != nil {
		return AsyncCommandAcceptedResponse{}, err
	} else if duplicate {
		return AsyncCommandAcceptedResponse{
			Accepted:       true,
			Status:         "completed",
			IdempotencyKey: key,
			Response:       responsePointer(response),
		}, nil
	}

	handler.asyncCommandsMu.Lock()
	defer handler.asyncCommandsMu.Unlock()
	if entry, ok := handler.asyncCommands[key]; ok {
		if entry.check.fingerprint != check.fingerprint {
			return AsyncCommandAcceptedResponse{}, errors.New("hatriecache: idempotency key was reused with a different command")
		}
		if entry.completed {
			return AsyncCommandAcceptedResponse{
				Accepted:       true,
				Status:         "completed",
				IdempotencyKey: key,
				Response:       responsePointer(entry.response),
			}, nil
		}
		return AsyncCommandAcceptedResponse{Accepted: true, Status: "pending", IdempotencyKey: key}, nil
	}
	if len(handler.asyncCommands) >= handler.options.AsyncCommandStatusCapacity && !handler.evictCompletedAsyncCommandLocked() {
		return AsyncCommandAcceptedResponse{}, ErrMonitoringAsyncCommandStatusFull
	}
	handler.asyncCommands[key] = monitoringAsyncCommandEntry{
		check:     check,
		request:   cloneAsyncCommandRequest(request),
		startedAt: time.Now(),
	}
	handler.asyncCommandOrder = append(handler.asyncCommandOrder, key)
	callbackRequest := cloneAsyncCommandRequest(request)
	submission, err := journal.submitAsyncCommand(handler.trie, request, func(response CacheCommandResponse) {
		if response.OK && handler.options.LevelDBDirtyTracker != nil {
			handler.options.LevelDBDirtyTracker.markCommand(callbackRequest)
		}
		handler.completeAsyncCommand(key, response)
	})
	if err != nil {
		handler.removeAsyncCommandLocked(key)
		return AsyncCommandAcceptedResponse{}, err
	}
	entry := handler.asyncCommands[key]
	entry.submission = submission
	handler.asyncCommands[key] = entry
	return AsyncCommandAcceptedResponse{Accepted: true, Status: "pending", IdempotencyKey: key}, nil
}

func asyncCommandContainsInternal(request CacheCommandRequest) bool {
	if normalizedCommand(request.Command) == "BATCH" {
		for _, nested := range request.Batch {
			if asyncCommandContainsInternal(nested) {
				return true
			}
		}
		return false
	}
	return isInternalReplicationCommand(request)
}

func (handler *MonitoringHandler) completeAsyncCommand(key string, response CacheCommandResponse) {
	handler.asyncCommandsMu.Lock()
	entry, ok := handler.asyncCommands[key]
	if !ok || entry.completed {
		handler.asyncCommandsMu.Unlock()
		return
	}
	entry.response = cloneCacheCommandResponse(response)
	entry.completed = true
	handler.asyncCommands[key] = entry
	handler.asyncCommandsMu.Unlock()
	handler.captureSlowCommand(entry.startedAt, entry.request, response, http.StatusOK)
}

func (handler *MonitoringHandler) evictCompletedAsyncCommandLocked() bool {
	for index, key := range handler.asyncCommandOrder {
		entry, ok := handler.asyncCommands[key]
		if !ok {
			continue
		}
		if !entry.completed {
			continue
		}
		delete(handler.asyncCommands, key)
		copy(handler.asyncCommandOrder[index:], handler.asyncCommandOrder[index+1:])
		handler.asyncCommandOrder = handler.asyncCommandOrder[:len(handler.asyncCommandOrder)-1]
		return true
	}
	return false
}

func (handler *MonitoringHandler) removeAsyncCommandLocked(key string) {
	delete(handler.asyncCommands, key)
	for index, existing := range handler.asyncCommandOrder {
		if existing != key {
			continue
		}
		copy(handler.asyncCommandOrder[index:], handler.asyncCommandOrder[index+1:])
		handler.asyncCommandOrder = handler.asyncCommandOrder[:len(handler.asyncCommandOrder)-1]
		return
	}
}

func (handler *MonitoringHandler) handleAsyncCommandStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w)
		return
	}
	key := strings.TrimSpace(r.URL.Query().Get("idempotency_key"))
	if key == "" {
		writeJSONStatus(w, http.StatusBadRequest, commandError("idempotency_key is required"))
		return
	}
	if err := validateCommandIdempotencyKey(key); err != nil {
		writeJSONStatus(w, http.StatusBadRequest, commandError(err.Error()))
		return
	}
	handler.asyncCommandsMu.Lock()
	entry, ok := handler.asyncCommands[key]
	if ok {
		status := "pending"
		var response *CacheCommandResponse
		if entry.completed {
			status = "completed"
			response = responsePointer(entry.response)
		}
		handler.asyncCommandsMu.Unlock()
		writeJSON(w, AsyncCommandStatusResponse{IdempotencyKey: key, Status: status, Response: response})
		return
	}
	handler.asyncCommandsMu.Unlock()
	if response, found := handler.options.Journal.lookupAsyncCommandKey(key); found {
		writeJSON(w, AsyncCommandStatusResponse{IdempotencyKey: key, Status: "completed", Response: responsePointer(response)})
		return
	}
	writeJSON(w, AsyncCommandStatusResponse{IdempotencyKey: key, Status: "unknown"})
}

func responsePointer(response CacheCommandResponse) *CacheCommandResponse {
	response = cloneCacheCommandResponse(response)
	return &response
}

func (journal *CommandJournal) lookupAsyncCommandIdempotency(check commandIdempotencyCheck) (CacheCommandResponse, bool, error) {
	if journal == nil {
		return CacheCommandResponse{}, false, ErrNilCommandJournal
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	return journal.idempotency.lookup(check)
}

func (journal *CommandJournal) lookupAsyncCommandKey(key string) (CacheCommandResponse, bool) {
	if journal == nil {
		return CacheCommandResponse{}, false
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if !journal.idempotency.enabled() {
		return CacheCommandResponse{}, false
	}
	record, ok := journal.idempotency.entries[key]
	if !ok {
		return CacheCommandResponse{}, false
	}
	return cloneCacheCommandResponse(record.response), true
}
