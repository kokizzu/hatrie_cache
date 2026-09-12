package hatSql

import (
	"context"
	"sort"
	"strconv"
	"strings"
)

// QuerySubscriptionDelta is one signed multiplicity change for a row. A
// positive Diff inserts rows; a negative Diff retracts them.
type QuerySubscriptionDelta struct {
	Row  Row
	Diff int64
}

// QuerySubscriptionDeltaBatch is one atomic differential update at a logical
// frontier. Deltas in a batch are applied together before advancing the
// consumer's frontier. Progress batches have no Deltas.
//
// Reset is set when the bounded queue had to coalesce pending updates. A
// consumer must discard its current multiset and apply the batch's positive
// Deltas as the complete current result before continuing.
type QuerySubscriptionDeltaBatch struct {
	ID       uint64
	Revision uint64
	Frontier uint64
	Columns  []string
	Deltas   []QuerySubscriptionDelta
	Progress bool
	Complete bool
	Reset    bool
}

// QueryDifferentialSubscription exposes signed row changes for an opt-in
// query subscription. Its updates are bounded and retain the existing
// subscription registry's latest-update behavior, with Reset marking any
// coalescing that requires consumer resynchronization.
type QueryDifferentialSubscription struct {
	subscription *QuerySubscription
}

// SubscribeDifferential evaluates definition once and returns a subscription
// that publishes an initial positive batch followed by signed row changes.
// The query definition's EmitProgress option controls whether frontier-only
// progress batches are emitted.
func (registry *QuerySubscriptions) SubscribeDifferential(ctx context.Context, definition QuerySubscriptionDefinition, resolver SourceResolver, options QueryOptions) (*QueryDifferentialSubscription, error) {
	subscription, err := registry.subscribe(ctx, definition, resolver, options, true)
	if err != nil {
		return nil, err
	}
	return &QueryDifferentialSubscription{subscription: subscription}, nil
}

// Updates returns the bounded differential batch channel. A nil receiver
// returns nil.
func (subscription *QueryDifferentialSubscription) Updates() <-chan QuerySubscriptionDeltaBatch {
	if subscription == nil || subscription.subscription == nil {
		return nil
	}
	return subscription.subscription.differentialUpdates
}

// Close removes the differential subscription and closes Updates. Close is
// idempotent.
func (subscription *QueryDifferentialSubscription) Close() {
	if subscription == nil || subscription.subscription == nil {
		return
	}
	subscription.subscription.Close()
}

func querySubscriptionInitialDelta(snapshot QuerySubscriptionSnapshot) QuerySubscriptionDeltaBatch {
	return querySubscriptionDeltaBatch(snapshot, QueryResult{}, false)
}

func querySubscriptionDeltaBatch(snapshot QuerySubscriptionSnapshot, previous QueryResult, hasPrevious bool) QuerySubscriptionDeltaBatch {
	batch := QuerySubscriptionDeltaBatch{
		ID:       snapshot.ID,
		Revision: snapshot.Revision,
		Frontier: snapshot.Frontier,
		Columns:  append([]string(nil), snapshot.Result.Columns...),
		Progress: snapshot.Progress,
		Complete: snapshot.Complete,
	}
	if snapshot.Progress {
		return batch
	}
	if !hasPrevious {
		batch.Deltas = querySubscriptionPositiveRows(snapshot.Result.Rows)
		return batch
	}
	batch.Deltas = querySubscriptionRowDeltas(previous.Rows, snapshot.Result.Rows)
	return batch
}

func querySubscriptionResetDelta(snapshot QuerySubscriptionSnapshot, result QueryResult) QuerySubscriptionDeltaBatch {
	batch := querySubscriptionDeltaBatch(snapshot, QueryResult{}, false)
	batch.Reset = true
	batch.Deltas = querySubscriptionPositiveRows(result.Rows)
	batch.Columns = append([]string(nil), result.Columns...)
	return batch
}

func querySubscriptionPositiveRows(rows []Row) []QuerySubscriptionDelta {
	if len(rows) == 0 {
		return nil
	}
	groups, order := querySubscriptionRowGroups(rows)
	deltas := make([]QuerySubscriptionDelta, 0, len(order))
	for _, key := range order {
		group := groups[key]
		deltas = append(deltas, QuerySubscriptionDelta{Row: cloneDifferentialRow(group.row), Diff: group.count})
	}
	return deltas
}

func querySubscriptionRowDeltas(previous, current []Row) []QuerySubscriptionDelta {
	previousGroups, previousOrder := querySubscriptionRowGroups(previous)
	currentGroups, currentOrder := querySubscriptionRowGroups(current)
	deltas := make([]QuerySubscriptionDelta, 0, len(previousOrder)+len(currentOrder))
	for _, key := range previousOrder {
		before := previousGroups[key]
		after := currentGroups[key]
		if before.count > after.count {
			deltas = append(deltas, QuerySubscriptionDelta{Row: cloneDifferentialRow(before.row), Diff: after.count - before.count})
		}
	}
	for _, key := range currentOrder {
		before := previousGroups[key]
		after := currentGroups[key]
		if after.count > before.count {
			deltas = append(deltas, QuerySubscriptionDelta{Row: cloneDifferentialRow(after.row), Diff: after.count - before.count})
		}
	}
	return deltas
}

type querySubscriptionRowGroup struct {
	row   Row
	count int64
}

func querySubscriptionRowGroups(rows []Row) (map[string]querySubscriptionRowGroup, []string) {
	if len(rows) == 0 {
		return nil, nil
	}
	groups := make(map[string]querySubscriptionRowGroup, len(rows))
	order := make([]string, 0, len(rows))
	for _, row := range rows {
		key := querySubscriptionRowKey(row)
		group, exists := groups[key]
		if !exists {
			group.row = row
			order = append(order, key)
		}
		group.count++
		groups[key] = group
	}
	return groups, order
}

func querySubscriptionRowKey(row Row) string {
	keys := make([]string, 0, len(row))
	for key := range row {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var builder strings.Builder
	for _, key := range keys {
		value := sqlCollationValueKey(SQLCollationBinary, row[key])
		builder.WriteString(strconv.Itoa(len(key)))
		builder.WriteByte(':')
		builder.WriteString(key)
		builder.WriteByte('=')
		builder.WriteString(strconv.Itoa(len(value)))
		builder.WriteByte(':')
		builder.WriteString(value)
		builder.WriteByte(';')
	}
	return builder.String()
}

func enqueueQuerySubscriptionDifferential(subscription *QuerySubscription, batch QuerySubscriptionDeltaBatch, resetResult QueryResult) {
	if subscription == nil || subscription.differentialUpdates == nil {
		return
	}
	select {
	case subscription.differentialUpdates <- batch:
		return
	default:
	}
	for {
		select {
		case <-subscription.differentialUpdates:
		default:
			reset := querySubscriptionResetDelta(QuerySubscriptionSnapshot{
				ID:       batch.ID,
				Revision: batch.Revision,
				Frontier: batch.Frontier,
				Progress: batch.Progress,
				Complete: batch.Complete,
			}, resetResult)
			select {
			case subscription.differentialUpdates <- reset:
			default:
			}
			return
		}
	}
}
