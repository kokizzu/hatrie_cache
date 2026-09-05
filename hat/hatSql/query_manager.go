package hatSql

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	// DefaultSQLQueryManagerHistoryCapacity bounds completed status retained by
	// a new manager. Query text and parameter values are never retained.
	DefaultSQLQueryManagerHistoryCapacity = 256
	maxSQLQueryManagerIDBytes             = 256
	maxSQLQueryManagerReasonBytes         = 512
)

// SQLQueryState describes one managed query's lifecycle.
type SQLQueryState string

const (
	SQLQueryStateRunning         SQLQueryState = "running"
	SQLQueryStateCancelRequested SQLQueryState = "cancel_requested"
	SQLQueryStateSucceeded       SQLQueryState = "succeeded"
	SQLQueryStateFailed          SQLQueryState = "failed"
	SQLQueryStateCanceled        SQLQueryState = "canceled"
)

// SQLQueryStatus is a privacy-safe operator view of one managed query. It
// contains no SQL text, source names, parameters, or result rows.
type SQLQueryStatus struct {
	QueryID      string        `json:"query_id"`
	State        SQLQueryState `json:"state"`
	StartedAt    time.Time     `json:"started_at"`
	FinishedAt   time.Time     `json:"finished_at,omitempty"`
	CancelReason string        `json:"cancel_reason,omitempty"`
	ErrorCode    ErrorCode     `json:"error_code,omitempty"`
}

// SQLQueryCanceledError identifies an operator-requested cancellation while
// preserving errors.Is(err, context.Canceled) for existing callers.
type SQLQueryCanceledError struct {
	QueryID string
	Reason  string
}

func (err *SQLQueryCanceledError) Error() string {
	if err == nil {
		return ""
	}
	if err.Reason == "" {
		return fmt.Sprintf("SQL query %q canceled", err.QueryID)
	}
	return fmt.Sprintf("SQL query %q canceled: %s", err.QueryID, err.Reason)
}

func (err *SQLQueryCanceledError) Unwrap() error { return context.Canceled }

// SQLQueryManagerOptions configures the bounded operator status history.
type SQLQueryManagerOptions struct {
	HistoryCapacity int
}

// SQLQueryManager owns cancellation contexts for opt-in SQL executions. It
// is safe for concurrent Execute, Cancel, Status, Active, and History calls.
type SQLQueryManager struct {
	mu              sync.Mutex
	nextID          uint64
	historyCapacity int
	active          map[string]*managedSQLQuery
	history         []SQLQueryStatus
	historyStart    int
}

type managedSQLQuery struct {
	status SQLQueryStatus
	cancel context.CancelFunc
}

// NewSQLQueryManager creates a manager with bounded completed-query history.
// A nonpositive capacity selects DefaultSQLQueryManagerHistoryCapacity.
func NewSQLQueryManager(historyCapacity int) *SQLQueryManager {
	if historyCapacity <= 0 {
		historyCapacity = DefaultSQLQueryManagerHistoryCapacity
	}
	return &SQLQueryManager{
		historyCapacity: historyCapacity,
		active:          make(map[string]*managedSQLQuery),
	}
}

// NewSQLQueryManagerWithOptions creates a manager from explicit options.
func NewSQLQueryManagerWithOptions(options SQLQueryManagerOptions) *SQLQueryManager {
	return NewSQLQueryManager(options.HistoryCapacity)
}

// Execute runs one query under a manager-owned cancellation context. When
// options.QueryID is empty, a bounded generated ID is returned in the result.
func (manager *SQLQueryManager) Execute(ctx context.Context, source string, resolver SQLSourceResolver, parameters []interface{}, options SQLQueryOptions) (result SQLQueryResult, err error) {
	if manager == nil {
		return SQLQueryResult{}, errors.New("SQL query manager is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	queryID, err := manager.queryID(options.QueryID)
	if err != nil {
		return SQLQueryResult{}, err
	}
	queryContext, cancel := context.WithCancel(ctx)
	started := time.Now().UTC()
	entry := &managedSQLQuery{
		status: SQLQueryStatus{
			QueryID:   queryID,
			State:     SQLQueryStateRunning,
			StartedAt: started,
		},
		cancel: cancel,
	}
	manager.mu.Lock()
	if _, exists := manager.active[queryID]; exists {
		manager.mu.Unlock()
		cancel()
		return SQLQueryResult{}, WithErrorCode(ErrorConflict, fmt.Errorf("SQL query %q is already running", queryID))
	}
	manager.active[queryID] = entry
	manager.mu.Unlock()

	options.QueryID = queryID
	result, err = ExecuteSQLQueryParameters(queryContext, source, resolver, parameters, options)
	manager.mu.Lock()
	status := entry.status
	if queryContext.Err() != nil && (err == nil || status.State == SQLQueryStateCancelRequested) {
		err = queryContext.Err()
	}
	status.FinishedAt = time.Now().UTC()
	if isSQLQueryManagerCancellation(err) {
		status.State = SQLQueryStateCanceled
	} else if err != nil {
		status.State = SQLQueryStateFailed
	} else {
		status.State = SQLQueryStateSucceeded
	}
	if status.CancelReason != "" && isSQLQueryManagerCancellation(err) {
		err = &SQLQueryCanceledError{QueryID: queryID, Reason: status.CancelReason}
	}
	if err != nil {
		status.ErrorCode = ErrorCodeOf(err)
	}
	delete(manager.active, queryID)
	manager.appendHistoryLocked(status)
	manager.mu.Unlock()
	cancel()
	return result, err
}

// Cancel requests cooperative cancellation of one active query. The first
// accepted reason is retained and included in the returned final status and
// SQLQueryCanceledError. Repeating Cancel for the same active query is safe.
func (manager *SQLQueryManager) Cancel(queryID, reason string) (SQLQueryStatus, error) {
	if manager == nil {
		return SQLQueryStatus{}, errors.New("SQL query manager is required")
	}
	queryID, err := normalizeSQLQueryManagerID(queryID)
	if err != nil {
		return SQLQueryStatus{}, err
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		return SQLQueryStatus{}, errors.New("SQL query cancellation reason is required")
	}
	if len(reason) > maxSQLQueryManagerReasonBytes {
		return SQLQueryStatus{}, fmt.Errorf("SQL query cancellation reason exceeds %d bytes", maxSQLQueryManagerReasonBytes)
	}
	manager.mu.Lock()
	entry, ok := manager.active[queryID]
	if !ok {
		manager.mu.Unlock()
		return SQLQueryStatus{}, fmt.Errorf("SQL query %q is not active", queryID)
	}
	if entry.status.State == SQLQueryStateRunning {
		entry.status.State = SQLQueryStateCancelRequested
		entry.status.CancelReason = reason
	}
	status := entry.status
	cancel := entry.cancel
	manager.mu.Unlock()
	cancel()
	return status, nil
}

// Status returns the active or most recently retained completed status.
func (manager *SQLQueryManager) Status(queryID string) (SQLQueryStatus, bool) {
	if manager == nil {
		return SQLQueryStatus{}, false
	}
	queryID = strings.TrimSpace(queryID)
	if queryID == "" {
		return SQLQueryStatus{}, false
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if entry, ok := manager.active[queryID]; ok {
		return entry.status, true
	}
	for offset := 0; offset < len(manager.history); offset++ {
		index := manager.historyIndexLocked(len(manager.history) - 1 - offset)
		if manager.history[index].QueryID == queryID {
			return manager.history[index], true
		}
	}
	return SQLQueryStatus{}, false
}

// Active returns a deterministic snapshot of currently active queries.
func (manager *SQLQueryManager) Active() []SQLQueryStatus {
	if manager == nil {
		return nil
	}
	manager.mu.Lock()
	statuses := make([]SQLQueryStatus, 0, len(manager.active))
	for _, entry := range manager.active {
		statuses = append(statuses, entry.status)
	}
	manager.mu.Unlock()
	sort.Slice(statuses, func(left, right int) bool {
		return statuses[left].QueryID < statuses[right].QueryID
	})
	return statuses
}

// History returns an oldest-first snapshot of bounded completed-query status.
func (manager *SQLQueryManager) History() []SQLQueryStatus {
	if manager == nil {
		return nil
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if len(manager.history) == 0 {
		return nil
	}
	history := make([]SQLQueryStatus, len(manager.history))
	for index := range history {
		history[index] = manager.history[manager.historyIndexLocked(index)]
	}
	return history
}

func (manager *SQLQueryManager) appendHistoryLocked(status SQLQueryStatus) {
	if len(manager.history) < manager.historyCapacity {
		manager.history = append(manager.history, status)
		return
	}
	manager.history[manager.historyStart] = status
	manager.historyStart = (manager.historyStart + 1) % manager.historyCapacity
}

func (manager *SQLQueryManager) historyIndexLocked(offset int) int {
	return (manager.historyStart + offset) % len(manager.history)
}

func (manager *SQLQueryManager) queryID(requested string) (string, error) {
	requested = strings.TrimSpace(requested)
	if requested != "" {
		return normalizeSQLQueryManagerID(requested)
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	manager.nextID++
	return fmt.Sprintf("query-%d", manager.nextID), nil
}

func normalizeSQLQueryManagerID(queryID string) (string, error) {
	queryID = strings.TrimSpace(queryID)
	if queryID == "" {
		return "", errors.New("SQL query ID is required")
	}
	if len(queryID) > maxSQLQueryManagerIDBytes {
		return "", fmt.Errorf("SQL query ID exceeds %d bytes", maxSQLQueryManagerIDBytes)
	}
	return queryID, nil
}

func isSQLQueryManagerCancellation(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}
