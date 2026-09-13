package hatSql

import (
	"context"
	"fmt"
	"sort"
)

// ExportSnapshotsAt evaluates every active subscription at one exact logical
// frontier and returns detached snapshots ordered by subscription ID. It does
// not advance revisions, publish updates, or close subscriptions.
//
// The resolver must implement HistoricalSourceResolver so the requested
// frontier can be evaluated. A subscription's AsOf and UpTo bounds are
// enforced; exporting beyond UpTo would not be an exact snapshot of that
// subscription's valid timeline.
func (registry *QuerySubscriptions) ExportSnapshotsAt(ctx context.Context, frontier uint64, resolver SourceResolver, options QueryOptions) ([]QuerySubscriptionSnapshot, error) {
	if registry == nil {
		return nil, fmt.Errorf("query subscriptions are nil")
	}
	if frontier == 0 {
		return nil, fmt.Errorf("query subscription export frontier must be positive")
	}

	registry.mu.RLock()
	subscriptions := make([]*QuerySubscription, 0, len(registry.subs))
	for _, subscription := range registry.subs {
		subscriptions = append(subscriptions, subscription)
	}
	registry.mu.RUnlock()
	sort.Slice(subscriptions, func(left, right int) bool {
		return subscriptions[left].id < subscriptions[right].id
	})

	exported := make([]QuerySubscriptionSnapshot, 0, len(subscriptions))
	for _, subscription := range subscriptions {
		if subscription == nil {
			continue
		}
		subscription.mu.RLock()
		definition := subscription.definition
		id := subscription.id
		revision := subscription.snapshot.Revision
		closed := subscription.closed
		subscription.mu.RUnlock()
		if closed {
			continue
		}
		if frontier < definition.AsOf {
			return nil, fmt.Errorf("subscription %d export frontier %d is before AsOf %d", id, frontier, definition.AsOf)
		}
		if definition.UpTo > 0 && frontier > definition.UpTo {
			return nil, fmt.Errorf("subscription %d export frontier %d is after UpTo %d", id, frontier, definition.UpTo)
		}
		queryResolver, err := querySubscriptionResolver(resolver, frontier)
		if err != nil {
			return nil, fmt.Errorf("export subscription %d: %w", id, err)
		}
		result, err := ExecuteQueryParameters(ctx, definition.Query, queryResolver, definition.Parameters, options)
		if err != nil {
			return nil, fmt.Errorf("export subscription %d at frontier %d: %w", id, frontier, err)
		}
		exported = append(exported, QuerySubscriptionSnapshot{
			ID:       id,
			Revision: revision,
			Frontier: frontier,
			Complete: definition.UpTo > 0 && frontier == definition.UpTo,
			Result:   cloneQueryResult(result),
		})
	}
	return exported, nil
}
