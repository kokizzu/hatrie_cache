package hatSql

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
)

var (
	// ErrSQLTriggerRegistryNil reports a nil trigger registry.
	ErrSQLTriggerRegistryNil = errors.New("SQL trigger registry is nil")
	// ErrSQLTriggerInvalid reports invalid trigger or event input.
	ErrSQLTriggerInvalid = errors.New("SQL trigger is invalid")
	// ErrSQLTriggerDuplicate reports a trigger name collision.
	ErrSQLTriggerDuplicate = errors.New("SQL trigger already exists")
	// ErrSQLTriggerTransactionClosed reports use after commit or rollback.
	ErrSQLTriggerTransactionClosed = errors.New("SQL trigger transaction is closed")
	// ErrSQLTriggerApplyRequired reports a missing primary mutation callback.
	ErrSQLTriggerApplyRequired = errors.New("SQL trigger transaction apply callback is required")
)

// SQLTriggerEvent describes one staged SQL mutation. Before and After are
// copied when the event enters a transaction and are safe for callers to
// retain after Add returns.
type SQLTriggerEvent struct {
	Source    string
	Operation string
	Key       string
	Before    Row
	After     Row
}

// SQLTriggerAction is a prepared transaction participant. Commit is called in
// deterministic order only after all trigger preparation and the primary
// apply callback succeed. Rollback is called in reverse order on failure and
// may also be called for an action whose Commit already ran, so implementations
// must make it idempotent.
type SQLTriggerAction struct {
	Commit   func(context.Context) error
	Rollback func(context.Context) error
}

// SQLTriggerPrepareFunc validates or prepares one trigger event. It must not
// publish externally visible state before returning its action.
type SQLTriggerPrepareFunc func(context.Context, SQLTriggerEvent) (SQLTriggerAction, error)

// SQLTriggerApplyFunc prepares the primary storage mutation for a transaction.
// The returned action participates in the same commit and rollback sequence as
// trigger actions.
type SQLTriggerApplyFunc func(context.Context, []SQLTriggerEvent) (SQLTriggerAction, error)

// SQLTrigger describes one ordered trigger. Empty Source or Operation matches
// every source or operation respectively. Lower Order values prepare and
// commit first; trigger names break ties deterministically.
type SQLTrigger struct {
	Name      string
	Source    string
	Operation string
	Order     int
	Prepare   SQLTriggerPrepareFunc
}

// SQLTriggerRegistry stores immutable trigger definitions and is safe for
// concurrent registration and transaction creation.
type SQLTriggerRegistry struct {
	mu       sync.RWMutex
	triggers []SQLTrigger
	names    map[string]struct{}
}

// NewSQLTriggerRegistry creates an empty trigger registry.
func NewSQLTriggerRegistry() *SQLTriggerRegistry {
	return &SQLTriggerRegistry{names: make(map[string]struct{})}
}

// Register adds one trigger. Names are unique within a registry.
func (registry *SQLTriggerRegistry) Register(trigger SQLTrigger) error {
	if registry == nil {
		return ErrSQLTriggerRegistryNil
	}
	trigger.Name = strings.TrimSpace(trigger.Name)
	trigger.Source = strings.TrimSpace(trigger.Source)
	trigger.Operation = strings.ToUpper(strings.TrimSpace(trigger.Operation))
	if trigger.Name == "" || trigger.Prepare == nil {
		return ErrSQLTriggerInvalid
	}

	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.names == nil {
		registry.names = make(map[string]struct{})
	}
	if _, exists := registry.names[trigger.Name]; exists {
		return ErrSQLTriggerDuplicate
	}
	registry.names[trigger.Name] = struct{}{}
	registry.triggers = append(registry.triggers, trigger)
	sort.Slice(registry.triggers, func(left, right int) bool {
		if registry.triggers[left].Order != registry.triggers[right].Order {
			return registry.triggers[left].Order < registry.triggers[right].Order
		}
		return registry.triggers[left].Name < registry.triggers[right].Name
	})
	return nil
}

// Unregister removes a trigger by name and reports whether it existed.
func (registry *SQLTriggerRegistry) Unregister(name string) bool {
	if registry == nil {
		return false
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return false
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if _, exists := registry.names[name]; !exists {
		return false
	}
	delete(registry.names, name)
	for index, trigger := range registry.triggers {
		if trigger.Name == name {
			copy(registry.triggers[index:], registry.triggers[index+1:])
			registry.triggers[len(registry.triggers)-1] = SQLTrigger{}
			registry.triggers = registry.triggers[:len(registry.triggers)-1]
			break
		}
	}
	return true
}

// BeginSQLTriggerTransaction creates a transaction using the registry's
// current trigger snapshot. A nil context uses context.Background.
func (registry *SQLTriggerRegistry) BeginSQLTriggerTransaction(ctx context.Context) (*SQLTriggerTransaction, error) {
	if registry == nil {
		return nil, ErrSQLTriggerRegistryNil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return &SQLTriggerTransaction{registry: registry, ctx: ctx}, nil
}

// BeginSQLTriggerTransaction creates a transaction from registry. It is the
// function form of SQLTriggerRegistry.BeginSQLTriggerTransaction.
func BeginSQLTriggerTransaction(registry *SQLTriggerRegistry, ctx context.Context) (*SQLTriggerTransaction, error) {
	return registry.BeginSQLTriggerTransaction(ctx)
}

func (registry *SQLTriggerRegistry) snapshot() []SQLTrigger {
	registry.mu.RLock()
	triggers := make([]SQLTrigger, len(registry.triggers))
	copy(triggers, registry.triggers)
	registry.mu.RUnlock()
	return triggers
}

// SQLTriggerTransaction stages events and commits them through the registry's
// ordered triggers and a caller-supplied primary mutation participant.
type SQLTriggerTransaction struct {
	registry *SQLTriggerRegistry
	ctx      context.Context

	mu         sync.Mutex
	events     []SQLTriggerEvent
	closed     bool
	committing bool
	resultErr  error
}

// Add stages one mutation in input order.
func (transaction *SQLTriggerTransaction) Add(event SQLTriggerEvent) error {
	if transaction == nil {
		return ErrSQLTriggerTransactionClosed
	}
	event.Source = strings.TrimSpace(event.Source)
	event.Operation = strings.ToUpper(strings.TrimSpace(event.Operation))
	if event.Source == "" || event.Operation == "" {
		return ErrSQLTriggerInvalid
	}

	transaction.mu.Lock()
	defer transaction.mu.Unlock()
	if transaction.closed || transaction.committing {
		return ErrSQLTriggerTransactionClosed
	}
	transaction.events = append(transaction.events, cloneSQLTriggerEvent(event))
	return nil
}

// Events returns an independently owned copy of staged events.
func (transaction *SQLTriggerTransaction) Events() []SQLTriggerEvent {
	if transaction == nil {
		return nil
	}
	transaction.mu.Lock()
	defer transaction.mu.Unlock()
	return cloneSQLTriggerEvents(transaction.events)
}

// Commit prepares and commits the primary mutation and matching triggers. All
// trigger preparation happens in event order, then trigger order. Commit
// actions run primary-first and trigger-order; any failure rolls every
// prepared action back in reverse order.
func (transaction *SQLTriggerTransaction) Commit(apply SQLTriggerApplyFunc) error {
	if transaction == nil {
		return ErrSQLTriggerTransactionClosed
	}
	transaction.mu.Lock()
	if transaction.closed || transaction.committing {
		transaction.mu.Unlock()
		return ErrSQLTriggerTransactionClosed
	}
	if apply == nil {
		transaction.mu.Unlock()
		return ErrSQLTriggerApplyRequired
	}
	transaction.committing = true
	events := cloneSQLTriggerEvents(transaction.events)
	transaction.mu.Unlock()

	prepared := make([]SQLTriggerAction, 0, len(events))
	triggers := transaction.registry.snapshot()
	if err := transaction.ctx.Err(); err != nil {
		return transaction.finishFailure(err, prepared)
	}
	for _, event := range events {
		for _, trigger := range triggers {
			if !sqlTriggerMatches(trigger, event) {
				continue
			}
			if err := transaction.ctx.Err(); err != nil {
				return transaction.finishFailure(err, prepared)
			}
			action, err := trigger.Prepare(transaction.ctx, cloneSQLTriggerEvent(event))
			if err != nil {
				return transaction.finishFailure(fmt.Errorf("SQL trigger %q prepare: %w", trigger.Name, err), prepared)
			}
			prepared = append(prepared, action)
		}
	}
	if err := transaction.ctx.Err(); err != nil {
		return transaction.finishFailure(err, prepared)
	}
	primary, err := apply(transaction.ctx, cloneSQLTriggerEvents(events))
	if err != nil {
		return transaction.finishFailure(fmt.Errorf("SQL trigger transaction apply: %w", err), prepared)
	}
	actions := make([]SQLTriggerAction, 1, len(prepared)+1)
	actions[0] = primary
	actions = append(actions, prepared...)
	for _, action := range actions {
		if err := transaction.ctx.Err(); err != nil {
			return transaction.finishFailure(err, actions)
		}
		if action.Commit == nil {
			continue
		}
		if err := action.Commit(transaction.ctx); err != nil {
			return transaction.finishFailure(err, actions)
		}
	}
	transaction.finish(nil)
	return nil
}

// Rollback discards uncommitted staged events. A transaction cannot be
// manually rolled back while Commit is executing; the commit path owns that
// lifecycle and performs action rollback on failure.
func (transaction *SQLTriggerTransaction) Rollback() {
	if transaction == nil {
		return
	}
	transaction.mu.Lock()
	if !transaction.closed && !transaction.committing {
		transaction.closed = true
		transaction.events = nil
	}
	transaction.mu.Unlock()
}

func (transaction *SQLTriggerTransaction) finishFailure(err error, actions []SQLTriggerAction) error {
	rollbackErr := rollbackSQLTriggerActions(transaction.ctx, actions)
	transaction.finish(err)
	if rollbackErr != nil {
		return fmt.Errorf("%w (rollback failed: %v)", err, rollbackErr)
	}
	return err
}

func (transaction *SQLTriggerTransaction) finish(err error) {
	transaction.mu.Lock()
	transaction.closed = true
	transaction.committing = false
	transaction.resultErr = err
	transaction.mu.Unlock()
}

func sqlTriggerMatches(trigger SQLTrigger, event SQLTriggerEvent) bool {
	return (trigger.Source == "" || trigger.Source == event.Source) &&
		(trigger.Operation == "" || trigger.Operation == event.Operation)
}

func rollbackSQLTriggerActions(ctx context.Context, actions []SQLTriggerAction) error {
	var firstErr error
	for index := len(actions) - 1; index >= 0; index-- {
		if actions[index].Rollback == nil {
			continue
		}
		if err := actions[index].Rollback(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func cloneSQLTriggerEvent(event SQLTriggerEvent) SQLTriggerEvent {
	event.Before = cloneSQLTriggerRow(event.Before)
	event.After = cloneSQLTriggerRow(event.After)
	return event
}

func cloneSQLTriggerEvents(events []SQLTriggerEvent) []SQLTriggerEvent {
	if len(events) == 0 {
		return nil
	}
	cloned := make([]SQLTriggerEvent, len(events))
	for index, event := range events {
		cloned[index] = cloneSQLTriggerEvent(event)
	}
	return cloned
}

func cloneSQLTriggerRow(row Row) Row {
	if row == nil {
		return nil
	}
	return CloneRows([]Row{row})[0]
}
