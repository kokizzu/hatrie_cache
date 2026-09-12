package hatPipeline

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"
)

const (
	defaultConnectorHistoryLimit = 32
	maxConnectorHistoryLimit     = 4096
)

var (
	ErrConnectorIDEmpty             = errors.New("connector ID is empty")
	ErrConnectorNil                 = errors.New("connector is nil")
	ErrConnectorAlreadyRegistered   = errors.New("connector is already registered")
	ErrConnectorNotFound            = errors.New("connector is not registered")
	ErrConnectorInvalidTransition   = errors.New("connector lifecycle transition is invalid")
	ErrConnectorRegistryClosed      = errors.New("connector registry is closed")
	ErrConnectorHistoryLimitInvalid = errors.New("connector history limit is invalid")
)

// ConnectorState is the externally visible lifecycle state of a connector.
type ConnectorState uint8

const (
	ConnectorCreated ConnectorState = iota
	ConnectorRunning
	ConnectorPaused
	ConnectorFailed
	ConnectorStopped
)

func (s ConnectorState) String() string {
	switch s {
	case ConnectorCreated:
		return "created"
	case ConnectorRunning:
		return "running"
	case ConnectorPaused:
		return "paused"
	case ConnectorFailed:
		return "failed"
	case ConnectorStopped:
		return "stopped"
	default:
		return "unknown"
	}
}

// Connector owns the work performed by each lifecycle transition. The
// registry serializes calls for one connector but permits different
// connectors to transition concurrently.
type Connector interface {
	Start(context.Context) error
	Pause(context.Context) error
	Resume(context.Context) error
	Stop(context.Context) error
}

// ConnectorRegistryOptions controls the lifecycle control plane.
type ConnectorRegistryOptions struct {
	// HistoryLimit is the number of recent transition events retained per
	// connector. Zero selects the default of 32.
	HistoryLimit int
}

// ConnectorStatus is a point-in-time lifecycle snapshot.
type ConnectorStatus struct {
	ID         string
	State      ConnectorState
	Generation uint64
	UpdatedAt  time.Time
	LastError  string
}

// ConnectorEvent records one successful or failed lifecycle transition.
type ConnectorEvent struct {
	Sequence uint64
	ID       string
	From     ConnectorState
	To       ConnectorState
	At       time.Time
	Error    string
}

type managedConnector struct {
	mu        sync.Mutex
	id        string
	connector Connector
	status    ConnectorStatus
	events    []ConnectorEvent
	eventNext int
	removed   bool
}

// ConnectorRegistry owns connector registration, lifecycle transitions, and
// bounded operator-visible status history. It has no effect on data-plane
// operations unless a caller explicitly uses it.
type ConnectorRegistry struct {
	mu           sync.RWMutex
	connectors   map[string]*managedConnector
	historyLimit int
	closed       bool
}

// NewConnectorRegistry creates a connector lifecycle registry.
func NewConnectorRegistry(options ConnectorRegistryOptions) (*ConnectorRegistry, error) {
	historyLimit := options.HistoryLimit
	if historyLimit == 0 {
		historyLimit = defaultConnectorHistoryLimit
	}
	if historyLimit < 0 || historyLimit > maxConnectorHistoryLimit {
		return nil, ErrConnectorHistoryLimitInvalid
	}
	return &ConnectorRegistry{
		connectors:   make(map[string]*managedConnector),
		historyLimit: historyLimit,
	}, nil
}

// Register adds a connector in the Created state.
func (r *ConnectorRegistry) Register(id string, connector Connector) error {
	if id == "" {
		return ErrConnectorIDEmpty
	}
	if connector == nil {
		return ErrConnectorNil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return ErrConnectorRegistryClosed
	}
	if _, exists := r.connectors[id]; exists {
		return ErrConnectorAlreadyRegistered
	}
	r.connectors[id] = &managedConnector{
		id:        id,
		connector: connector,
		events:    make([]ConnectorEvent, 0, r.historyLimit),
		status: ConnectorStatus{
			ID:        id,
			State:     ConnectorCreated,
			UpdatedAt: time.Now().UTC(),
		},
	}
	return nil
}

// Unregister removes a connector that has not started or has already stopped.
// Active connectors must be stopped explicitly so a drop cannot abandon work.
func (r *ConnectorRegistry) Unregister(id string) error {
	if id == "" {
		return ErrConnectorIDEmpty
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return ErrConnectorRegistryClosed
	}
	entry := r.connectors[id]
	if entry == nil {
		return ErrConnectorNotFound
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if entry.removed {
		return ErrConnectorNotFound
	}
	if entry.status.State != ConnectorCreated && entry.status.State != ConnectorStopped {
		return ErrConnectorInvalidTransition
	}
	entry.removed = true
	delete(r.connectors, id)
	return nil
}

// Start starts a connector from Created or Failed.
func (r *ConnectorRegistry) Start(ctx context.Context, id string) error {
	return r.transition(ctx, id, connectorStart, false)
}

// Pause pauses a running connector.
func (r *ConnectorRegistry) Pause(ctx context.Context, id string) error {
	return r.transition(ctx, id, connectorPause, false)
}

// Resume resumes a paused connector.
func (r *ConnectorRegistry) Resume(ctx context.Context, id string) error {
	return r.transition(ctx, id, connectorResume, false)
}

// Stop stops a connector. Stopping an already stopped connector is idempotent.
func (r *ConnectorRegistry) Stop(ctx context.Context, id string) error {
	return r.transition(ctx, id, connectorStop, false)
}

// Close stops all registered connectors in ID order and rejects future
// lifecycle operations. It is idempotent.
func (r *ConnectorRegistry) Close(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	entries := make([]*managedConnector, 0, len(r.connectors))
	for _, entry := range r.connectors {
		entries = append(entries, entry)
	}
	r.mu.Unlock()
	sort.Slice(entries, func(i, j int) bool { return entries[i].id < entries[j].id })

	var firstErr error
	for _, entry := range entries {
		if err := r.transitionEntry(ctx, entry, connectorStop); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Status returns a copy of the current connector status.
func (r *ConnectorRegistry) Status(id string) (ConnectorStatus, bool) {
	if id == "" {
		return ConnectorStatus{}, false
	}
	r.mu.RLock()
	entry := r.connectors[id]
	r.mu.RUnlock()
	if entry == nil {
		return ConnectorStatus{}, false
	}
	entry.mu.Lock()
	status := entry.status
	entry.mu.Unlock()
	return status, true
}

// Snapshot returns all connector statuses sorted by ID.
func (r *ConnectorRegistry) Snapshot() []ConnectorStatus {
	r.mu.RLock()
	entries := make([]*managedConnector, 0, len(r.connectors))
	for _, entry := range r.connectors {
		entries = append(entries, entry)
	}
	r.mu.RUnlock()
	sort.Slice(entries, func(i, j int) bool { return entries[i].id < entries[j].id })

	statuses := make([]ConnectorStatus, 0, len(entries))
	for _, entry := range entries {
		entry.mu.Lock()
		statuses = append(statuses, entry.status)
		entry.mu.Unlock()
	}
	return statuses
}

// Events returns a copy of the retained transition history for a connector.
func (r *ConnectorRegistry) Events(id string) []ConnectorEvent {
	r.mu.RLock()
	entry := r.connectors[id]
	r.mu.RUnlock()
	if entry == nil {
		return []ConnectorEvent{}
	}
	entry.mu.Lock()
	events := make([]ConnectorEvent, len(entry.events))
	if len(entry.events) == r.historyLimit && len(entry.events) > 0 {
		copied := copy(events, entry.events[entry.eventNext:])
		copy(events[copied:], entry.events[:entry.eventNext])
	} else {
		copy(events, entry.events)
	}
	entry.mu.Unlock()
	return events
}

type connectorOperation uint8

const (
	connectorStart connectorOperation = iota
	connectorPause
	connectorResume
	connectorStop
)

func (r *ConnectorRegistry) transition(ctx context.Context, id string, operation connectorOperation, allowClosed bool) error {
	if id == "" {
		return ErrConnectorIDEmpty
	}
	if ctx == nil {
		ctx = context.Background()
	}
	r.mu.RLock()
	closed := r.closed
	entry := r.connectors[id]
	r.mu.RUnlock()
	if closed && !allowClosed {
		return ErrConnectorRegistryClosed
	}
	if entry == nil {
		return ErrConnectorNotFound
	}
	return r.transitionEntry(ctx, entry, operation)
}

func (r *ConnectorRegistry) transitionEntry(ctx context.Context, entry *managedConnector, operation connectorOperation) error {
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if entry.removed {
		return ErrConnectorNotFound
	}

	from := entry.status.State
	if operation == connectorStop && from == ConnectorStopped {
		return nil
	}
	if !validConnectorTransition(from, operation) {
		return ErrConnectorInvalidTransition
	}
	if err := ctx.Err(); err != nil {
		r.recordFailureLocked(entry, from, err)
		return err
	}

	var err error
	switch operation {
	case connectorStart:
		err = entry.connector.Start(ctx)
	case connectorPause:
		err = entry.connector.Pause(ctx)
	case connectorResume:
		err = entry.connector.Resume(ctx)
	case connectorStop:
		err = entry.connector.Stop(ctx)
	}
	if err != nil {
		r.recordFailureLocked(entry, from, err)
		return err
	}
	r.recordSuccessLocked(entry, from, connectorStateAfter(operation))
	return nil
}

func validConnectorTransition(state ConnectorState, operation connectorOperation) bool {
	switch operation {
	case connectorStart:
		return state == ConnectorCreated || state == ConnectorFailed
	case connectorPause:
		return state == ConnectorRunning
	case connectorResume:
		return state == ConnectorPaused
	case connectorStop:
		return state == ConnectorCreated || state == ConnectorRunning || state == ConnectorPaused || state == ConnectorFailed
	default:
		return false
	}
}

func connectorStateAfter(operation connectorOperation) ConnectorState {
	switch operation {
	case connectorStart, connectorResume:
		return ConnectorRunning
	case connectorPause:
		return ConnectorPaused
	case connectorStop:
		return ConnectorStopped
	default:
		return ConnectorFailed
	}
}

func (r *ConnectorRegistry) recordSuccessLocked(entry *managedConnector, from, to ConnectorState) {
	r.recordLocked(entry, from, to, "")
}

func (r *ConnectorRegistry) recordFailureLocked(entry *managedConnector, from ConnectorState, err error) {
	r.recordLocked(entry, from, ConnectorFailed, err.Error())
}

func (r *ConnectorRegistry) recordLocked(entry *managedConnector, from, to ConnectorState, message string) {
	entry.status.State = to
	entry.status.Generation++
	entry.status.UpdatedAt = time.Now().UTC()
	entry.status.LastError = message
	event := ConnectorEvent{
		Sequence: entry.status.Generation,
		ID:       entry.id,
		From:     from,
		To:       to,
		At:       entry.status.UpdatedAt,
		Error:    message,
	}
	if len(entry.events) < r.historyLimit {
		entry.events = append(entry.events, event)
		entry.eventNext = len(entry.events) % r.historyLimit
		return
	}
	entry.events[entry.eventNext] = event
	entry.eventNext = (entry.eventNext + 1) % r.historyLimit
}
