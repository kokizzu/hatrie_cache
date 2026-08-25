package hatSql

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"
)

// QuerySubscriptionDefinition declares one query-result subscription. Its
// dependencies are explicit cache or application source names used to decide
// which subscriptions need re-evaluation after a change.
type QuerySubscriptionDefinition struct {
	Query        string
	Parameters   []interface{}
	Dependencies []string
}

// QuerySubscriptionSnapshot is one immutable query-result version.
type QuerySubscriptionSnapshot struct {
	ID       uint64
	Revision uint64
	Result   QueryResult
}

// QuerySubscription receives coalesced query-result updates. Close is
// idempotent and closes Updates after removing the subscription from its
// registry.
type QuerySubscription struct {
	registry   *QuerySubscriptions
	id         uint64
	definition QuerySubscriptionDefinition

	mu       sync.RWMutex
	snapshot QuerySubscriptionSnapshot
	updates  chan QuerySubscriptionSnapshot
	closed   bool
}

// QuerySubscriptions manages query-result subscriptions. NotifyChanged is
// deliberately caller-driven so cache writes do not acquire query locks or
// start background work implicitly.
type QuerySubscriptions struct {
	mu     sync.RWMutex
	nextID uint64
	buffer int
	subs   map[uint64]*QuerySubscription
}

// NewQuerySubscriptions creates a registry. buffer is the number of queued
// snapshots per subscriber; a nonpositive value uses one latest-only slot.
func NewQuerySubscriptions(buffer int) *QuerySubscriptions {
	if buffer < 1 {
		buffer = 1
	}
	return &QuerySubscriptions{buffer: buffer, subs: make(map[uint64]*QuerySubscription)}
}

// Subscribe evaluates definition once and returns a handle initialized at
// revision one. No update is emitted for the initial result; use Snapshot.
func (registry *QuerySubscriptions) Subscribe(ctx context.Context, definition QuerySubscriptionDefinition, resolver SourceResolver, options QueryOptions) (*QuerySubscription, error) {
	if registry == nil {
		return nil, fmt.Errorf("query subscriptions are nil")
	}
	definition, err := normalizeQuerySubscriptionDefinition(definition)
	if err != nil {
		return nil, err
	}
	result, err := ExecuteQueryParameters(ctx, definition.Query, resolver, definition.Parameters, options)
	if err != nil {
		return nil, err
	}

	registry.mu.Lock()
	registry.nextID++
	subscription := &QuerySubscription{
		registry:   registry,
		id:         registry.nextID,
		definition: definition,
		snapshot: QuerySubscriptionSnapshot{
			ID:       registry.nextID,
			Revision: 1,
			Result:   cloneQueryResult(result),
		},
		updates: make(chan QuerySubscriptionSnapshot, registry.buffer),
	}
	registry.subs[subscription.id] = subscription
	registry.mu.Unlock()
	return subscription, nil
}

// NotifyChanged evaluates subscriptions whose declared dependencies intersect
// changed. It publishes only changed row snapshots and leaves all prior
// snapshots untouched if any affected query fails.
func (registry *QuerySubscriptions) NotifyChanged(ctx context.Context, changed []string, resolver SourceResolver, options QueryOptions) error {
	if registry == nil {
		return fmt.Errorf("query subscriptions are nil")
	}
	changedSet := make(map[string]struct{}, len(changed))
	for _, dependency := range changed {
		if dependency = strings.TrimSpace(dependency); dependency != "" {
			changedSet[dependency] = struct{}{}
		}
	}
	if len(changedSet) == 0 {
		return nil
	}

	registry.mu.RLock()
	subscriptions := make([]*QuerySubscription, 0, len(registry.subs))
	for _, subscription := range registry.subs {
		if querySubscriptionDependsOn(subscription.definition, changedSet) {
			subscriptions = append(subscriptions, subscription)
		}
	}
	registry.mu.RUnlock()
	results := make([]QueryResult, len(subscriptions))
	for index, subscription := range subscriptions {
		result, err := ExecuteQueryParameters(ctx, subscription.definition.Query, resolver, subscription.definition.Parameters, options)
		if err != nil {
			return fmt.Errorf("refresh query subscription %d: %w", subscription.id, err)
		}
		results[index] = cloneQueryResult(result)
	}
	for index, subscription := range subscriptions {
		subscription.publish(results[index])
	}
	return nil
}

// Updates receives coalesced latest snapshots. A subscription never blocks an
// update caller: when the channel is full, its stale queued snapshot is
// replaced by the newest one.
func (subscription *QuerySubscription) Updates() <-chan QuerySubscriptionSnapshot {
	if subscription == nil {
		return nil
	}
	return subscription.updates
}

// Snapshot returns an independent copy of the current snapshot while the
// subscription is active.
func (subscription *QuerySubscription) Snapshot() (QuerySubscriptionSnapshot, bool) {
	if subscription == nil {
		return QuerySubscriptionSnapshot{}, false
	}
	subscription.mu.RLock()
	defer subscription.mu.RUnlock()
	if subscription.closed {
		return QuerySubscriptionSnapshot{}, false
	}
	return cloneQuerySubscriptionSnapshot(subscription.snapshot), true
}

// Close removes the subscription and closes its update channel.
func (subscription *QuerySubscription) Close() {
	if subscription == nil || subscription.registry == nil {
		return
	}
	subscription.registry.remove(subscription)
}

func (registry *QuerySubscriptions) remove(subscription *QuerySubscription) {
	registry.mu.Lock()
	if registry.subs[subscription.id] == subscription {
		delete(registry.subs, subscription.id)
	}
	registry.mu.Unlock()
	subscription.mu.Lock()
	if !subscription.closed {
		subscription.closed = true
		close(subscription.updates)
	}
	subscription.mu.Unlock()
}

func (subscription *QuerySubscription) publish(result QueryResult) {
	subscription.mu.Lock()
	defer subscription.mu.Unlock()
	if subscription.closed || sameQuerySubscriptionResult(subscription.snapshot.Result, result) {
		return
	}
	subscription.snapshot.Revision++
	subscription.snapshot.Result = cloneQueryResult(result)
	update := cloneQuerySubscriptionSnapshot(subscription.snapshot)
	select {
	case subscription.updates <- update:
	default:
		select {
		case <-subscription.updates:
		default:
		}
		select {
		case subscription.updates <- update:
		default:
		}
	}
}

func normalizeQuerySubscriptionDefinition(definition QuerySubscriptionDefinition) (QuerySubscriptionDefinition, error) {
	definition.Query = strings.TrimSpace(definition.Query)
	if definition.Query == "" {
		return QuerySubscriptionDefinition{}, fmt.Errorf("query subscription query is required")
	}
	if len(definition.Dependencies) == 0 {
		return QuerySubscriptionDefinition{}, fmt.Errorf("query subscription dependencies are required")
	}
	dependencies := make([]string, 0, len(definition.Dependencies))
	seen := make(map[string]struct{}, len(definition.Dependencies))
	for _, dependency := range definition.Dependencies {
		dependency = strings.TrimSpace(dependency)
		if dependency == "" {
			return QuerySubscriptionDefinition{}, fmt.Errorf("query subscription has an empty dependency")
		}
		if _, exists := seen[dependency]; exists {
			return QuerySubscriptionDefinition{}, fmt.Errorf("query subscription has duplicate dependency %q", dependency)
		}
		seen[dependency] = struct{}{}
		dependencies = append(dependencies, dependency)
	}
	definition.Dependencies = dependencies
	definition.Parameters = append([]interface{}(nil), definition.Parameters...)
	return definition, nil
}

func querySubscriptionDependsOn(definition QuerySubscriptionDefinition, changed map[string]struct{}) bool {
	for _, dependency := range definition.Dependencies {
		if _, exists := changed[dependency]; exists {
			return true
		}
	}
	return false
}

func sameQuerySubscriptionResult(left, right QueryResult) bool {
	return left.HasMore == right.HasMore && left.NextCursor == right.NextCursor && reflect.DeepEqual(left.Columns, right.Columns) && reflect.DeepEqual(left.Rows, right.Rows)
}

func cloneQuerySubscriptionSnapshot(snapshot QuerySubscriptionSnapshot) QuerySubscriptionSnapshot {
	snapshot.Result = cloneQueryResult(snapshot.Result)
	return snapshot
}
