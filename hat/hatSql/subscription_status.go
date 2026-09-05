package hatSql

import "sort"

// QuerySubscriptionStatus is an on-demand progress snapshot for one active
// query subscription. Lag is the distance from observedFrontier to Frontier,
// clamped to zero when the observed frontier is older than the subscription.
type QuerySubscriptionStatus struct {
	ID            uint64
	Revision      uint64
	Frontier      uint64
	Lag           uint64
	Complete      bool
	QueuedUpdates int
}

// Status returns deterministic, sorted progress snapshots for active query
// subscriptions. It does not start workers or retain history, so callers can
// poll it from a metrics endpoint without changing subscription behavior.
func (registry *QuerySubscriptions) Status(observedFrontier uint64) []QuerySubscriptionStatus {
	if registry == nil {
		return nil
	}
	registry.mu.RLock()
	subscriptions := make([]*QuerySubscription, 0, len(registry.subs))
	for _, subscription := range registry.subs {
		subscriptions = append(subscriptions, subscription)
	}
	registry.mu.RUnlock()

	statuses := make([]QuerySubscriptionStatus, 0, len(subscriptions))
	for _, subscription := range subscriptions {
		status, ok := subscription.status(observedFrontier)
		if ok {
			statuses = append(statuses, status)
		}
	}
	if len(statuses) == 0 {
		return nil
	}
	sort.Slice(statuses, func(left, right int) bool {
		return statuses[left].ID < statuses[right].ID
	})
	return statuses
}

func (subscription *QuerySubscription) status(observedFrontier uint64) (QuerySubscriptionStatus, bool) {
	if subscription == nil {
		return QuerySubscriptionStatus{}, false
	}
	subscription.mu.RLock()
	defer subscription.mu.RUnlock()
	if subscription.closed {
		return QuerySubscriptionStatus{}, false
	}
	frontier := subscription.snapshot.Frontier
	lag := uint64(0)
	if observedFrontier > frontier {
		lag = observedFrontier - frontier
	}
	return QuerySubscriptionStatus{
		ID:            subscription.snapshot.ID,
		Revision:      subscription.snapshot.Revision,
		Frontier:      frontier,
		Lag:           lag,
		Complete:      subscription.snapshot.Complete,
		QueuedUpdates: len(subscription.updates),
	}, true
}
