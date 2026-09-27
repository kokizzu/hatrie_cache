package hatSql

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
)

const (
	// DefaultMaterializedViewRefreshQueueMaxPendingSources bounds the number of
	// distinct source keys retained while a refresh is running.
	DefaultMaterializedViewRefreshQueueMaxPendingSources = 4096
	// MaxMaterializedViewRefreshQueueMaxPendingSources prevents an accidental
	// unbounded source-notification backlog.
	MaxMaterializedViewRefreshQueueMaxPendingSources = 1 << 20
)

var (
	// ErrMaterializedViewRefreshQueueNil reports a method call on a nil queue.
	ErrMaterializedViewRefreshQueueNil = errors.New("materialized view refresh queue is nil")
	// ErrMaterializedViewRefreshQueueFull reports a notification that would
	// exceed the queue's bounded distinct-source admission limit.
	ErrMaterializedViewRefreshQueueFull = errors.New("materialized view refresh queue is full")
	// ErrMaterializedViewRefreshQueueOptionsInvalid reports an invalid queue
	// constructor configuration.
	ErrMaterializedViewRefreshQueueOptionsInvalid = errors.New("invalid materialized view refresh queue options")
	// ErrMaterializedViewRefreshQueueContext reports a nil context argument.
	ErrMaterializedViewRefreshQueueContext = errors.New("materialized view refresh queue context is nil")
)

// MaterializedViewRefreshQueueOptions bounds event-driven source
// notifications. A zero limit selects the package default. The bound counts
// distinct pending and in-flight source keys, so repeated changes to one
// source can still be coalesced without silently dropping a later update.
type MaterializedViewRefreshQueueOptions struct {
	MaxPendingSources int
}

// MaterializedViewRefreshQueueStats reports queue admission, coalescing, and
// refresh outcomes. PendingSources excludes the batch currently being
// refreshed; InFlightSources reports that batch separately.
type MaterializedViewRefreshQueueStats struct {
	Notifications          uint64
	CoalescedNotifications uint64
	RefreshBatches         uint64
	RefreshedViews         uint64
	Failures               uint64
	PendingSources         int
	InFlightSources        int
	MaxPendingSources      int
}

// MaterializedViewRefreshQueue turns explicit source-change notifications into
// dependency-aware materialized-view refreshes. It is opt-in and does not
// change the existing synchronous MaterializedViews API.
//
// Notify is non-blocking except for context cancellation. Run or RunOnce must
// be called by the owner to perform refresh work. Notifications arriving while
// a batch is being refreshed are retained for the next batch; failed or
// canceled batches are requeued unchanged.
type MaterializedViewRefreshQueue struct {
	runMu sync.Mutex
	mu    sync.Mutex

	views        *MaterializedViews
	resolver     SourceResolver
	queryOptions QueryOptions

	maxPendingSources int
	pendingSources    map[string]struct{}
	pendingKeys       map[string]struct{}
	inFlightSources   map[string]struct{}
	inFlightKeys      map[string]struct{}
	wake              chan struct{}

	notifications          uint64
	coalescedNotifications uint64
	refreshBatches         uint64
	refreshedViews         uint64
	failures               uint64
}

// NewMaterializedViewRefreshQueue creates an event-driven refresh queue for
// views. resolver and queryOptions are retained as the refresh contract and
// should therefore be treated as immutable after construction.
func NewMaterializedViewRefreshQueue(views *MaterializedViews, resolver SourceResolver, queryOptions QueryOptions, options MaterializedViewRefreshQueueOptions) (*MaterializedViewRefreshQueue, error) {
	if views == nil {
		return nil, fmt.Errorf("%w: materialized views are nil", ErrMaterializedViewRefreshQueueOptionsInvalid)
	}
	maxPendingSources := options.MaxPendingSources
	if maxPendingSources == 0 {
		maxPendingSources = DefaultMaterializedViewRefreshQueueMaxPendingSources
	}
	if maxPendingSources < 1 || maxPendingSources > MaxMaterializedViewRefreshQueueMaxPendingSources {
		return nil, fmt.Errorf("%w: max pending sources %d", ErrMaterializedViewRefreshQueueOptionsInvalid, maxPendingSources)
	}
	return &MaterializedViewRefreshQueue{
		views:             views,
		resolver:          resolver,
		queryOptions:      queryOptions,
		maxPendingSources: maxPendingSources,
		pendingSources:    make(map[string]struct{}),
		pendingKeys:       make(map[string]struct{}),
		inFlightSources:   make(map[string]struct{}),
		inFlightKeys:      make(map[string]struct{}),
		wake:              make(chan struct{}, 1),
	}, nil
}

// Notify admits one source-change batch. Source names and idempotency keys
// are trimmed, deduplicated, and sorted at processing time. An over-limit
// notification is rejected atomically, leaving the existing queue unchanged.
func (queue *MaterializedViewRefreshQueue) Notify(ctx context.Context, changed []string, metadata MaterializedViewRefreshMetadata) error {
	if queue == nil {
		return ErrMaterializedViewRefreshQueueNil
	}
	if ctx == nil {
		return ErrMaterializedViewRefreshQueueContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	sources := uniqueMaterializedViewRefreshSources(changed)
	if len(sources) == 0 {
		return nil
	}
	keys := uniqueMaterializedViewRefreshKeys(normalizeMaterializedViewIdempotencyKeys(metadata.IdempotencyKeys))

	queue.mu.Lock()
	occupied := make(map[string]struct{}, len(queue.pendingSources)+len(queue.inFlightSources))
	for source := range queue.pendingSources {
		occupied[source] = struct{}{}
	}
	for source := range queue.inFlightSources {
		occupied[source] = struct{}{}
	}
	coalesced := uint64(0)
	for _, source := range sources {
		if _, exists := occupied[source]; exists {
			coalesced++
			continue
		}
		occupied[source] = struct{}{}
	}
	if len(occupied) > queue.maxPendingSources {
		queue.mu.Unlock()
		return fmt.Errorf("%w: pending sources would reach %d, maximum is %d", ErrMaterializedViewRefreshQueueFull, len(occupied), queue.maxPendingSources)
	}
	for _, source := range sources {
		queue.pendingSources[source] = struct{}{}
	}
	for _, key := range keys {
		queue.pendingKeys[key] = struct{}{}
	}
	queue.notifications++
	queue.coalescedNotifications += coalesced
	queue.mu.Unlock()

	select {
	case queue.wake <- struct{}{}:
	default:
	}
	return nil
}

// RunOnce refreshes one coalesced notification batch without waiting for new
// work. processed is true when a batch was claimed, including failed or
// canceled batches that were requeued for retry.
func (queue *MaterializedViewRefreshQueue) RunOnce(ctx context.Context) ([]MaterializedViewStatus, bool, error) {
	if queue == nil {
		return nil, false, ErrMaterializedViewRefreshQueueNil
	}
	if ctx == nil {
		return nil, false, ErrMaterializedViewRefreshQueueContext
	}
	queue.runMu.Lock()
	defer queue.runMu.Unlock()

	sources, keys, ok := queue.claim()
	if !ok {
		return nil, false, nil
	}
	queue.mu.Lock()
	queue.refreshBatches++
	queue.mu.Unlock()
	if err := ctx.Err(); err != nil {
		queue.requeue(sources, keys, true)
		return nil, true, err
	}
	statuses, err := queue.views.RefreshChangedWithMetadata(ctx, sources, queue.resolver, queue.queryOptions, MaterializedViewRefreshMetadata{IdempotencyKeys: keys})
	if err != nil {
		queue.requeue(sources, keys, true)
		return nil, true, err
	}
	queue.complete(sources, keys, len(statuses))
	return statuses, true, nil
}

// Run waits for notifications and processes coalesced batches until ctx is
// canceled. A failed batch is requeued and returned to the caller; no source
// change is discarded on failure.
func (queue *MaterializedViewRefreshQueue) Run(ctx context.Context) error {
	if queue == nil {
		return ErrMaterializedViewRefreshQueueNil
	}
	if ctx == nil {
		return ErrMaterializedViewRefreshQueueContext
	}
	for {
		_, processed, err := queue.RunOnce(ctx)
		if err != nil {
			return err
		}
		if processed {
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-queue.wake:
		}
	}
}

// Stats returns a consistent snapshot of queue state and counters.
func (queue *MaterializedViewRefreshQueue) Stats() MaterializedViewRefreshQueueStats {
	if queue == nil {
		return MaterializedViewRefreshQueueStats{}
	}
	queue.mu.Lock()
	defer queue.mu.Unlock()
	return MaterializedViewRefreshQueueStats{
		Notifications:          queue.notifications,
		CoalescedNotifications: queue.coalescedNotifications,
		RefreshBatches:         queue.refreshBatches,
		RefreshedViews:         queue.refreshedViews,
		Failures:               queue.failures,
		PendingSources:         len(queue.pendingSources),
		InFlightSources:        len(queue.inFlightSources),
		MaxPendingSources:      queue.maxPendingSources,
	}
}

func (queue *MaterializedViewRefreshQueue) claim() ([]string, []string, bool) {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	if len(queue.pendingSources) == 0 {
		return nil, nil, false
	}
	sources := make([]string, 0, len(queue.pendingSources))
	for source := range queue.pendingSources {
		sources = append(sources, source)
		queue.inFlightSources[source] = struct{}{}
		delete(queue.pendingSources, source)
	}
	keys := make([]string, 0, len(queue.pendingKeys))
	for key := range queue.pendingKeys {
		keys = append(keys, key)
		queue.inFlightKeys[key] = struct{}{}
		delete(queue.pendingKeys, key)
	}
	sort.Strings(sources)
	sort.Strings(keys)
	return sources, keys, true
}

func (queue *MaterializedViewRefreshQueue) complete(sources, keys []string, refreshedViews int) {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	for _, source := range sources {
		delete(queue.inFlightSources, source)
	}
	for _, key := range keys {
		delete(queue.inFlightKeys, key)
	}
	queue.refreshedViews += uint64(refreshedViews)
}

func (queue *MaterializedViewRefreshQueue) requeue(sources, keys []string, failed bool) {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	for _, source := range sources {
		delete(queue.inFlightSources, source)
		queue.pendingSources[source] = struct{}{}
	}
	for _, key := range keys {
		delete(queue.inFlightKeys, key)
		queue.pendingKeys[key] = struct{}{}
	}
	if failed {
		queue.failures++
	}
}

func uniqueMaterializedViewRefreshSources(changed []string) []string {
	set := make(map[string]struct{}, len(changed))
	for _, source := range changed {
		if source = strings.TrimSpace(source); source != "" {
			set[source] = struct{}{}
		}
	}
	sources := make([]string, 0, len(set))
	for source := range set {
		sources = append(sources, source)
	}
	sort.Strings(sources)
	return sources
}

func uniqueMaterializedViewRefreshKeys(keys []string) []string {
	set := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		if key = strings.TrimSpace(key); key != "" {
			set[key] = struct{}{}
		}
	}
	unique := make([]string, 0, len(set))
	for key := range set {
		unique = append(unique, key)
	}
	sort.Strings(unique)
	return unique
}
