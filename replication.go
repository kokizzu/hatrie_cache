package hatriecache

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"hatrie_cache/internal/jsonwire"
)

const DefaultReplicationTimeout = 2 * time.Second
const DefaultReplicationRetryInterval = 250 * time.Millisecond
const DefaultReplicationDeadLetterLimit = 128
const DefaultReplicationCircuitBreakerFailures = 5
const DefaultReplicationCircuitBreakerCooldown = 30 * time.Second
const maxHTTPReplicationResponseBytes = 1 << 20
const maxHTTPResponseDrainBytes = 1 << 20
const minCompressedReplicationRequestBytes = 16 << 10
const defaultReplicationSyncKeyPageSize = 1024

var (
	errReplicationResponseTooLarge = errors.New("hatriecache: replication response is too large")
	errReplicationSyncKeyPageFull  = errors.New("hatriecache: replication sync key page full")
)

type HTTPReplicatorOptions struct {
	Context                context.Context
	Self                   string
	Topology               *TopologyStore
	Election               *ElectionStore
	Client                 *http.Client
	Timeout                time.Duration
	AsyncQueueSize         int
	AsyncRetryInterval     time.Duration
	AsyncMaxAttempts       uint
	AsyncDeadLetterLimit   int
	AsyncOutbox            *ReplicationOutboxStore
	CircuitBreakerFailures int
	CircuitBreakerCooldown time.Duration
	WireFormat             CommandWireFormat
}

type HTTPReplicator struct {
	mu              sync.RWMutex
	self            string
	topology        *TopologyStore
	election        *ElectionStore
	client          *http.Client
	timeout         time.Duration
	queue           chan replicationJob
	retry           time.Duration
	attempts        uint
	wireFormat      CommandWireFormat
	outbox          *ReplicationOutboxStore
	breakerFailures int
	breakerCooldown time.Duration
	breakers        map[string]replicationCircuitBreakerState
	done            chan struct{}
	stopped         chan struct{}
	asyncCtx        context.Context
	cancel          context.CancelFunc
	close           sync.Once
	closed          bool
	sequence        uint64
	queueSeq        uint64
	deadSeq         uint64
	deadLimit       int
	pending         []replicationQueueMeta
	deadLetters     []ReplicationDeadLetter
	queueStats      ReplicationQueueStats
	last            ReplicationResult
}

type ReplicationResult struct {
	Command         string                            `json:"command,omitempty"`
	Key             string                            `json:"key,omitempty"`
	Entries         int                               `json:"entries,omitempty"`
	Queued          bool                              `json:"queued,omitempty"`
	Skipped         bool                              `json:"skipped"`
	Reason          string                            `json:"reason,omitempty"`
	Health          string                            `json:"health"`
	HealthScore     int                               `json:"health_score"`
	HealthReason    string                            `json:"health_reason,omitempty"`
	DeadLetterCount int                               `json:"dead_letter_count,omitempty"`
	DeadLetters     []ReplicationDeadLetter           `json:"dead_letters,omitempty"`
	CircuitBreakers []ReplicationCircuitBreakerTarget `json:"circuit_breakers,omitempty"`
	StartedAt       *time.Time                        `json:"started_at,omitempty"`
	FinishedAt      *time.Time                        `json:"finished_at,omitempty"`
	DurationMillis  int64                             `json:"duration_millis,omitempty"`
	Queue           *ReplicationQueueStats            `json:"queue,omitempty"`
	Targets         []ReplicationTargetResult         `json:"targets,omitempty"`
}

type ReplicationDeadLetter struct {
	ID       uint64                    `json:"id"`
	Command  string                    `json:"command,omitempty"`
	Key      string                    `json:"key,omitempty"`
	FailedAt *time.Time                `json:"failed_at,omitempty"`
	Attempts uint                      `json:"attempts"`
	Reason   string                    `json:"reason,omitempty"`
	Targets  []ReplicationTargetResult `json:"targets,omitempty"`
}

type ReplicationCircuitBreakerTarget struct {
	Node              string     `json:"node"`
	State             string     `json:"state"`
	Failures          int        `json:"failures"`
	OpenedAt          *time.Time `json:"opened_at,omitempty"`
	OpenUntil         *time.Time `json:"open_until,omitempty"`
	LastFailureAt     *time.Time `json:"last_failure_at,omitempty"`
	LastSuccessAt     *time.Time `json:"last_success_at,omitempty"`
	LastFailureReason string     `json:"last_failure_reason,omitempty"`
}

// ReplicationQueueStats reports bounded async replication outbox health.
type ReplicationQueueStats struct {
	Enabled               bool              `json:"enabled"`
	Depth                 int               `json:"depth"`
	Capacity              int               `json:"capacity"`
	Enqueued              uint64            `json:"enqueued"`
	Dropped               uint64            `json:"dropped"`
	Attempts              uint64            `json:"attempts"`
	Successes             uint64            `json:"successes"`
	Failures              uint64            `json:"failures"`
	Retried               uint64            `json:"retried"`
	OldestQueuedAt        *time.Time        `json:"oldest_queued_at,omitempty"`
	OldestQueuedAgeMillis int64             `json:"oldest_queued_age_millis,omitempty"`
	OldestQueuedKey       string            `json:"oldest_queued_key,omitempty"`
	OldestQueuedTargets   []string          `json:"oldest_queued_targets,omitempty"`
	InFlightStartedAt     *time.Time        `json:"in_flight_started_at,omitempty"`
	InFlightAgeMillis     int64             `json:"in_flight_age_millis,omitempty"`
	InFlightKey           string            `json:"in_flight_key,omitempty"`
	LastRetryAt           *time.Time        `json:"last_retry_at,omitempty"`
	LastRetryAgeMillis    int64             `json:"last_retry_age_millis,omitempty"`
	LastRetryKey          string            `json:"last_retry_key,omitempty"`
	DroppedByTarget       map[string]uint64 `json:"dropped_by_target,omitempty"`
	FailuresByTarget      map[string]uint64 `json:"failures_by_target,omitempty"`
	Closed                bool              `json:"closed"`
}

type ReplicationTargetResult struct {
	Node             string     `json:"node"`
	Key              string     `json:"key,omitempty"`
	Address          string     `json:"address,omitempty"`
	OK               bool       `json:"ok"`
	Status           int        `json:"status,omitempty"`
	Error            string     `json:"error,omitempty"`
	CircuitOpen      bool       `json:"circuit_open,omitempty"`
	CircuitState     string     `json:"circuit_state,omitempty"`
	CircuitOpenUntil *time.Time `json:"circuit_open_until,omitempty"`
}

type replicationPayloadKind int

const (
	replicationPayloadNone replicationPayloadKind = iota
	replicationPayloadSet
	replicationPayloadDelete
)

const (
	replicationMetaSourceNode          = "_hatrie_replication_source"
	replicationMetaSequence            = "_hatrie_replication_sequence"
	replicationMetaTopologyFingerprint = "_hatrie_topology_fingerprint"
)

type replicationTask struct {
	target  TopologyNode
	payload CacheCommandRequest
}

type replicationJob struct {
	id         uint64
	result     ReplicationResult
	tasks      []replicationTask
	enqueuedAt time.Time
}

type replicationQueueMeta struct {
	id         uint64
	key        string
	targets    []string
	enqueuedAt time.Time
}

type replicationCircuitBreakerState struct {
	state             string
	failures          int
	openedAt          *time.Time
	openUntil         *time.Time
	lastFailureAt     *time.Time
	lastSuccessAt     *time.Time
	lastFailureReason string
}

const (
	replicationCircuitClosed   = "closed"
	replicationCircuitOpen     = "open"
	replicationCircuitHalfOpen = "half_open"
)

type ReplicationSafetyStore struct {
	mu   sync.Mutex
	last map[string]uint64
}

type replicationSafetyToken struct {
	source   string
	sequence uint64
}

func NewReplicationSafetyStore() *ReplicationSafetyStore {
	return &ReplicationSafetyStore{last: map[string]uint64{}}
}

func (store *ReplicationSafetyStore) Check(source string, sequence uint64) (replicationSafetyToken, bool) {
	if store == nil || source == "" || sequence == 0 {
		return replicationSafetyToken{}, false
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	token := replicationSafetyToken{source: source, sequence: sequence}
	return token, sequence <= store.last[source]
}

func (store *ReplicationSafetyStore) Commit(token replicationSafetyToken) {
	if store == nil || token.source == "" || token.sequence == 0 {
		return
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if token.sequence > store.last[token.source] {
		store.last[token.source] = token.sequence
	}
}

func NewHTTPReplicator(options HTTPReplicatorOptions) *HTTPReplicator {
	client := options.Client
	if client == nil {
		client = http.DefaultClient
	}
	timeout := options.Timeout
	if timeout == 0 {
		timeout = DefaultReplicationTimeout
	}
	wireFormat := options.WireFormat
	if wireFormat == "" {
		wireFormat = DefaultCommandWireFormat
	} else if parsed, err := ParseCommandWireFormat(string(wireFormat)); err == nil {
		wireFormat = parsed
	} else {
		wireFormat = DefaultCommandWireFormat
	}
	replicator := &HTTPReplicator{
		self:            strings.TrimSpace(options.Self),
		topology:        options.Topology,
		election:        options.Election,
		client:          client,
		timeout:         timeout,
		wireFormat:      wireFormat,
		outbox:          options.AsyncOutbox,
		breakerFailures: options.CircuitBreakerFailures,
		breakerCooldown: options.CircuitBreakerCooldown,
	}
	if replicator.breakerFailures < 0 {
		replicator.breakerFailures = 0
	}
	if replicator.breakerCooldown < 0 {
		replicator.breakerCooldown = 0
	}
	if replicator.breakerFailures > 0 && replicator.breakerCooldown > 0 {
		replicator.breakers = map[string]replicationCircuitBreakerState{}
	}
	if options.AsyncQueueSize > 0 {
		retry := options.AsyncRetryInterval
		if retry <= 0 {
			retry = DefaultReplicationRetryInterval
		}
		attempts := options.AsyncMaxAttempts
		if attempts == 0 {
			attempts = 1
		}
		deadLimit := options.AsyncDeadLetterLimit
		if deadLimit == 0 {
			deadLimit = DefaultReplicationDeadLetterLimit
		}
		if deadLimit < 0 {
			deadLimit = 0
		}
		parent := options.Context
		if parent == nil {
			parent = context.Background()
		}
		ctx, cancel := context.WithCancel(parent)
		restoredJobs := replicator.restoreAsyncOutbox()
		queueSize := options.AsyncQueueSize
		if len(restoredJobs) > queueSize {
			queueSize = len(restoredJobs)
		}
		replicator.queue = make(chan replicationJob, queueSize)
		replicator.retry = retry
		replicator.attempts = attempts
		replicator.deadLimit = deadLimit
		replicator.done = make(chan struct{})
		replicator.stopped = make(chan struct{})
		replicator.asyncCtx = ctx
		replicator.cancel = cancel
		replicator.queueStats = ReplicationQueueStats{
			Enabled:  true,
			Capacity: queueSize,
		}
		replicator.enqueueRestoredAsyncJobs(restoredJobs)
		go replicator.runAsync(ctx)
	}
	return replicator
}

func (replicator *HTTPReplicator) LastResult() ReplicationResult {
	if replicator == nil {
		return withReplicationHealth(ReplicationResult{Skipped: true, Reason: "replication is not configured"})
	}
	replicator.mu.RLock()
	defer replicator.mu.RUnlock()
	result := cloneReplicationResult(replicator.last)
	if replicator.queue != nil {
		stats := replicator.queueStatsLocked()
		result.Queue = &stats
	}
	result.DeadLetterCount = len(replicator.deadLetters)
	result.DeadLetters = cloneReplicationDeadLetters(replicator.deadLetters)
	result.CircuitBreakers = replicator.replicationCircuitBreakersLocked()
	return withReplicationHealth(result)
}

func (replicator *HTTPReplicator) restoreAsyncOutbox() []replicationJob {
	if replicator == nil || replicator.outbox == nil {
		return nil
	}
	deadSeq, deadLetters := replicator.outbox.deadLetters()
	replicator.deadSeq = deadSeq
	replicator.deadLetters = cloneReplicationDeadLetters(deadLetters)
	return replicator.outbox.jobs()
}

func (replicator *HTTPReplicator) enqueueRestoredAsyncJobs(jobs []replicationJob) {
	if replicator == nil || replicator.queue == nil {
		return
	}
	for _, job := range jobs {
		if job.id > replicator.queueSeq {
			replicator.queueSeq = job.id
		}
		if job.enqueuedAt.IsZero() {
			job.enqueuedAt = time.Now().UTC()
		}
		replicator.pending = append(replicator.pending, replicationQueueMeta{
			id:         job.id,
			key:        job.result.Key,
			targets:    replicationJobTargetIDs(job.tasks),
			enqueuedAt: job.enqueuedAt,
		})
		replicator.queue <- job
		replicator.queueStats.Enqueued++
	}
}

func (replicator *HTTPReplicator) Close() {
	if replicator == nil || replicator.queue == nil {
		return
	}
	replicator.close.Do(func() {
		replicator.mu.Lock()
		replicator.closed = true
		replicator.queueStats.Closed = true
		replicator.mu.Unlock()
		replicator.cancel()
		close(replicator.done)
		<-replicator.stopped
	})
}

func (replicator *HTTPReplicator) ReplicateCommand(ctx context.Context, trie *HatTrie, request CacheCommandRequest, response CacheCommandResponse) ReplicationResult {
	if replicator == nil {
		return withReplicationHealth(ReplicationResult{
			Command: normalizedCommand(request.Command),
			Key:     strings.TrimSpace(request.Key),
			Skipped: true,
			Reason:  "replication is not configured",
		})
	}

	startedAt := time.Now().UTC()
	var result ReplicationResult
	if replicator.queue != nil {
		result = replicator.enqueueReplication(ctx, trie, request, response)
	} else {
		result = replicator.replicateCommand(ctx, trie, request, response)
	}
	result = finishReplicationResult(result, startedAt)
	result = replicator.attachReplicationHealth(result)
	replicator.storeLastResult(result)
	return result
}

func (replicator *HTTPReplicator) SyncAll(ctx context.Context, trie *HatTrie, prefix string) ReplicationResult {
	if replicator == nil {
		return withReplicationHealth(ReplicationResult{
			Command: "SYNC",
			Key:     prefix,
			Skipped: true,
			Reason:  "replication is not configured",
		})
	}

	startedAt := time.Now().UTC()
	result := finishReplicationResult(replicator.syncAll(ctx, trie, prefix), startedAt)
	result = replicator.attachReplicationHealth(result)
	replicator.storeLastResult(result)
	return result
}

func (replicator *HTTPReplicator) replicateCommand(ctx context.Context, trie *HatTrie, request CacheCommandRequest, response CacheCommandResponse) ReplicationResult {
	result, tasks := replicator.planReplication(ctx, trie, request, response)
	if len(tasks) == 0 {
		return result
	}
	return replicator.executeReplicationTasks(ctx, result, tasks)
}

func (replicator *HTTPReplicator) enqueueReplication(ctx context.Context, trie *HatTrie, request CacheCommandRequest, response CacheCommandResponse) ReplicationResult {
	if replicator.asyncClosed() {
		replicator.recordAsyncDropped()
		return ReplicationResult{
			Command: normalizedCommand(request.Command),
			Key:     strings.TrimSpace(request.Key),
			Skipped: true,
			Reason:  "replication queue is closed",
		}
	}
	result, tasks := replicator.planReplication(ctx, trie, request, response)
	if len(tasks) == 0 {
		return result
	}
	result.Queued = true
	result.Targets = plannedReplicationTargets(tasks)
	job := replicationJob{
		result: cloneReplicationResult(result),
		tasks:  tasks,
	}
	if replicator.asyncClosed() {
		result.Queued = false
		result.Skipped = true
		result.Reason = "replication queue is closed"
		result.Targets = nil
		replicator.recordAsyncDroppedForTasks(tasks)
		return result
	}
	replicator.reserveAsyncJob(&job)
	if err := replicator.persistAsyncJob(job); err != nil {
		replicator.unreserveAsyncJob(job.id)
		result.Queued = false
		result.Skipped = true
		result.Reason = "replication outbox write failed: " + err.Error()
		result.Targets = nil
		replicator.recordAsyncDroppedForTasks(tasks)
		return result
	}
	select {
	case replicator.queue <- job:
		replicator.recordAsyncEnqueued()
		return result
	case <-replicator.done:
		replicator.unreserveAsyncJob(job.id)
		replicator.deleteAsyncJob(job.id)
		result.Queued = false
		result.Skipped = true
		result.Reason = "replication queue is closed"
		result.Targets = nil
		replicator.recordAsyncDroppedForTasks(tasks)
		return result
	default:
		replicator.unreserveAsyncJob(job.id)
		replicator.deleteAsyncJob(job.id)
		result.Queued = false
		result.Skipped = true
		result.Reason = "replication queue is full"
		result.Targets = nil
		replicator.recordAsyncDroppedForTasks(tasks)
		return result
	}
}

func (replicator *HTTPReplicator) asyncClosed() bool {
	if replicator == nil {
		return true
	}
	replicator.mu.RLock()
	defer replicator.mu.RUnlock()
	return replicator.closed || (replicator.asyncCtx != nil && replicator.asyncCtx.Err() != nil)
}

func (replicator *HTTPReplicator) markAsyncClosed() {
	if replicator == nil || replicator.queue == nil {
		return
	}
	replicator.mu.Lock()
	defer replicator.mu.Unlock()
	replicator.closed = true
	replicator.queueStats.Closed = true
}

func (replicator *HTTPReplicator) recordAsyncEnqueued() {
	if replicator == nil || replicator.queue == nil {
		return
	}
	replicator.mu.Lock()
	defer replicator.mu.Unlock()
	replicator.queueStats.Enqueued++
}

func (replicator *HTTPReplicator) reserveAsyncJob(job *replicationJob) {
	if replicator == nil || job == nil || replicator.queue == nil {
		return
	}
	targets := make([]string, 0, len(job.tasks))
	for _, task := range job.tasks {
		if task.target.ID != "" {
			targets = append(targets, task.target.ID)
		}
	}
	now := time.Now().UTC()
	replicator.mu.Lock()
	defer replicator.mu.Unlock()
	replicator.queueSeq++
	job.id = replicator.queueSeq
	job.enqueuedAt = now
	replicator.pending = append(replicator.pending, replicationQueueMeta{
		id:         job.id,
		key:        job.result.Key,
		targets:    targets,
		enqueuedAt: now,
	})
}

func replicationJobTargetIDs(tasks []replicationTask) []string {
	targets := make([]string, 0, len(tasks))
	for _, task := range tasks {
		if task.target.ID != "" {
			targets = append(targets, task.target.ID)
		}
	}
	return targets
}

func (replicator *HTTPReplicator) persistAsyncJob(job replicationJob) error {
	if replicator == nil || replicator.outbox == nil {
		return nil
	}
	return replicator.outbox.putJob(job)
}

func (replicator *HTTPReplicator) deleteAsyncJob(id uint64) {
	if replicator == nil || replicator.outbox == nil || id == 0 {
		return
	}
	_ = replicator.outbox.deleteJob(id)
}

func (replicator *HTTPReplicator) unreserveAsyncJob(id uint64) {
	if replicator == nil || id == 0 {
		return
	}
	replicator.mu.Lock()
	defer replicator.mu.Unlock()
	replicator.removePendingLocked(id)
}

func (replicator *HTTPReplicator) markAsyncDequeued(job replicationJob) {
	if replicator == nil || replicator.queue == nil {
		return
	}
	now := time.Now().UTC()
	replicator.mu.Lock()
	defer replicator.mu.Unlock()
	replicator.removePendingLocked(job.id)
	replicator.queueStats.InFlightStartedAt = cloneTimePtr(&now)
	replicator.queueStats.InFlightKey = job.result.Key
}

func (replicator *HTTPReplicator) clearAsyncInFlight(job replicationJob) {
	if replicator == nil || replicator.queue == nil {
		return
	}
	replicator.mu.Lock()
	defer replicator.mu.Unlock()
	if replicator.queueStats.InFlightKey == job.result.Key {
		replicator.queueStats.InFlightStartedAt = nil
		replicator.queueStats.InFlightAgeMillis = 0
		replicator.queueStats.InFlightKey = ""
	}
}

func (replicator *HTTPReplicator) removePendingLocked(id uint64) {
	for idx, meta := range replicator.pending {
		if meta.id != id {
			continue
		}
		copy(replicator.pending[idx:], replicator.pending[idx+1:])
		replicator.pending[len(replicator.pending)-1] = replicationQueueMeta{}
		replicator.pending = replicator.pending[:len(replicator.pending)-1]
		return
	}
}

func (replicator *HTTPReplicator) recordAsyncDropped() {
	if replicator == nil || replicator.queue == nil {
		return
	}
	replicator.mu.Lock()
	defer replicator.mu.Unlock()
	replicator.queueStats.Dropped++
}

func (replicator *HTTPReplicator) recordAsyncDroppedForTasks(tasks []replicationTask) {
	if replicator == nil || replicator.queue == nil {
		return
	}
	replicator.mu.Lock()
	defer replicator.mu.Unlock()
	replicator.queueStats.Dropped++
	if len(tasks) == 0 {
		return
	}
	if replicator.queueStats.DroppedByTarget == nil {
		replicator.queueStats.DroppedByTarget = map[string]uint64{}
	}
	for _, task := range tasks {
		if task.target.ID != "" {
			replicator.queueStats.DroppedByTarget[task.target.ID]++
		}
	}
}

func (replicator *HTTPReplicator) recordAsyncAttempt(result ReplicationResult, retried bool) {
	if replicator == nil || replicator.queue == nil {
		return
	}
	replicator.mu.Lock()
	defer replicator.mu.Unlock()
	replicator.queueStats.Attempts += uint64(len(result.Targets))
	for _, target := range result.Targets {
		if target.OK {
			replicator.queueStats.Successes++
		} else {
			replicator.queueStats.Failures++
			if target.Node != "" {
				if replicator.queueStats.FailuresByTarget == nil {
					replicator.queueStats.FailuresByTarget = map[string]uint64{}
				}
				replicator.queueStats.FailuresByTarget[target.Node]++
			}
		}
	}
	if retried {
		now := time.Now().UTC()
		replicator.queueStats.Retried++
		replicator.queueStats.LastRetryAt = cloneTimePtr(&now)
		replicator.queueStats.LastRetryKey = result.Key
	}
}

func (replicator *HTTPReplicator) recordDeadLetter(result ReplicationResult, attempts uint) {
	if replicator == nil || replicator.queue == nil || replicator.deadLimit <= 0 {
		return
	}
	failedTargets := make([]ReplicationTargetResult, 0, len(result.Targets))
	for _, target := range result.Targets {
		if !target.OK {
			failedTargets = append(failedTargets, target)
		}
	}
	if len(failedTargets) == 0 {
		return
	}
	failedAt := time.Now().UTC()
	replicator.mu.Lock()
	defer replicator.mu.Unlock()
	replicator.deadSeq++
	deadLetter := ReplicationDeadLetter{
		ID:       replicator.deadSeq,
		Command:  result.Command,
		Key:      result.Key,
		FailedAt: cloneTimePtr(&failedAt),
		Attempts: attempts,
		Reason:   replicationDeadLetterReason(failedTargets),
		Targets:  append([]ReplicationTargetResult(nil), failedTargets...),
	}
	replicator.deadLetters = append(replicator.deadLetters, deadLetter)
	if len(replicator.deadLetters) > replicator.deadLimit {
		copy(replicator.deadLetters, replicator.deadLetters[len(replicator.deadLetters)-replicator.deadLimit:])
		replicator.deadLetters = replicator.deadLetters[:replicator.deadLimit]
	}
	if replicator.outbox != nil {
		_ = replicator.outbox.setDeadLetters(replicator.deadSeq, replicator.deadLetters)
	}
}

func replicationDeadLetterReason(targets []ReplicationTargetResult) string {
	for _, target := range targets {
		if target.Error != "" {
			return target.Error
		}
		if target.Status != 0 {
			return fmt.Sprintf("target returned HTTP %d", target.Status)
		}
	}
	return "replication target delivery failed"
}

func (replicator *HTTPReplicator) queueStatsLocked() ReplicationQueueStats {
	stats := cloneReplicationQueueStats(replicator.queueStats)
	now := time.Now().UTC()
	if replicator.queue != nil {
		stats.Depth = len(replicator.queue)
		stats.Capacity = cap(replicator.queue)
	}
	if len(replicator.pending) > 0 {
		oldest := replicator.pending[0]
		stats.OldestQueuedAt = cloneTimePtr(&oldest.enqueuedAt)
		stats.OldestQueuedAgeMillis = now.Sub(oldest.enqueuedAt).Milliseconds()
		stats.OldestQueuedKey = oldest.key
		stats.OldestQueuedTargets = append([]string(nil), oldest.targets...)
	}
	if stats.InFlightStartedAt != nil {
		stats.InFlightAgeMillis = now.Sub(*stats.InFlightStartedAt).Milliseconds()
	}
	if stats.LastRetryAt != nil {
		stats.LastRetryAgeMillis = now.Sub(*stats.LastRetryAt).Milliseconds()
	}
	return stats
}

func (replicator *HTTPReplicator) planReplication(ctx context.Context, trie *HatTrie, request CacheCommandRequest, response CacheCommandResponse) (ReplicationResult, []replicationTask) {
	ctx = replicationContext(ctx)
	command := normalizedCommand(request.Command)
	key := strings.TrimSpace(request.Key)
	result := ReplicationResult{Command: command, Key: key}
	if replicator == nil {
		result.Skipped = true
		result.Reason = "replication is not configured"
		return result, nil
	}
	if trie == nil {
		result.Skipped = true
		result.Reason = "trie is not configured"
		return result, nil
	}
	if err := ctx.Err(); err != nil {
		result.Skipped = true
		result.Reason = err.Error()
		return result, nil
	}
	kind := replicationPayloadKindFor(request, response)
	if kind == replicationPayloadNone {
		result.Skipped = true
		result.Reason = "command is not replicated"
		return result, nil
	}

	route, ok := replicator.routeForKey(key)
	if !ok {
		result.Skipped = true
		result.Reason = "topology cannot route key"
		return result, nil
	}
	if replicator.self != "" && route.Leader.Leader != "" && route.Leader.Leader != replicator.self {
		result.Skipped = true
		result.Reason = "local node is not elected leader"
		return result, nil
	}

	targets := replicator.replicationTargets(route)
	if len(targets) == 0 {
		result.Skipped = true
		result.Reason = "no remote replication targets"
		return result, nil
	}

	payload, ok := replicationCommandPayload(trie, key, kind)
	if !ok {
		result.Skipped = true
		result.Reason = "no local value to replicate"
		return result, nil
	}
	payload = replicator.annotateReplicationPayload(payload)
	tasks := make([]replicationTask, 0, len(targets))
	for _, target := range targets {
		tasks = append(tasks, replicationTask{target: target, payload: payload})
	}
	return result, tasks
}

func (replicator *HTTPReplicator) executeReplicationTasks(ctx context.Context, result ReplicationResult, tasks []replicationTask) ReplicationResult {
	result.Queued = false
	result.Targets = make([]ReplicationTargetResult, 0, len(tasks))
	for _, task := range tasks {
		result.Targets = append(result.Targets, replicator.executeReplicationTarget(ctx, task.target, task.payload))
	}
	return result
}

func (replicator *HTTPReplicator) runAsync(ctx context.Context) {
	defer close(replicator.stopped)
	defer replicator.markAsyncClosed()
	for {
		select {
		case <-ctx.Done():
			return
		case <-replicator.done:
			return
		case job := <-replicator.queue:
			replicator.markAsyncDequeued(job)
			replicator.runAsyncJob(ctx, job)
			replicator.clearAsyncInFlight(job)
		}
	}
}

func (replicator *HTTPReplicator) runAsyncJob(ctx context.Context, job replicationJob) {
	attempts := replicator.attempts
	if attempts == 0 {
		attempts = 1
	}
	var result ReplicationResult
	for attempt := uint(1); attempt <= attempts; attempt++ {
		if err := ctx.Err(); err != nil {
			result = job.result
			result.Queued = false
			result.Skipped = true
			result.Reason = err.Error()
			result.Targets = nil
			result = replicator.attachReplicationHealth(result)
			replicator.storeLastResult(result)
			return
		}
		startedAt := time.Now().UTC()
		result = finishReplicationResult(replicator.executeReplicationTasks(ctx, job.result, job.tasks), startedAt)
		if err := ctx.Err(); err != nil {
			result = job.result
			result.Queued = false
			result.Skipped = true
			result.Reason = err.Error()
			result.Targets = nil
			result = replicator.attachReplicationHealth(result)
			replicator.storeLastResult(result)
			return
		}
		needsRetry := replicationNeedsRetry(result)
		willRetry := needsRetry && attempt < attempts
		replicator.recordAsyncAttempt(result, willRetry)
		if needsRetry && attempt == attempts {
			replicator.recordDeadLetter(result, attempt)
		}
		result = replicator.attachReplicationHealth(result)
		replicator.storeLastResult(result)
		if !needsRetry || attempt == attempts {
			replicator.deleteAsyncJob(job.id)
			return
		}
		if !replicator.waitForRetry(ctx) {
			return
		}
	}
}

func (replicator *HTTPReplicator) waitForRetry(ctx context.Context) bool {
	retry := replicator.retry
	if retry <= 0 {
		return true
	}
	timer := time.NewTimer(retry)
	defer stopReplicationRetryTimer(timer)
	select {
	case <-ctx.Done():
		return false
	case <-replicator.done:
		return false
	case <-timer.C:
		return true
	}
}

func stopReplicationRetryTimer(timer *time.Timer) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}

func replicationNeedsRetry(result ReplicationResult) bool {
	if result.Skipped {
		return false
	}
	for _, target := range result.Targets {
		if !target.OK {
			return true
		}
	}
	return false
}

func plannedReplicationTargets(tasks []replicationTask) []ReplicationTargetResult {
	out := make([]ReplicationTargetResult, len(tasks))
	for idx, task := range tasks {
		out[idx] = ReplicationTargetResult{
			Node:    task.target.ID,
			Address: task.target.Address,
		}
	}
	return out
}

func (replicator *HTTPReplicator) syncAll(ctx context.Context, trie *HatTrie, prefix string) ReplicationResult {
	return replicator.syncAllPaged(ctx, trie, prefix, defaultReplicationSyncKeyPageSize)
}

func (replicator *HTTPReplicator) annotateReplicationPayload(payload CacheCommandRequest) CacheCommandRequest {
	if replicator == nil {
		return payload
	}
	if payload.Pairs == nil {
		payload.Pairs = Map{}
	}
	if replicator.self != "" {
		payload.Pairs[replicationMetaSourceNode] = replicator.self
	}
	payload.Pairs[replicationMetaSequence] = strconv.FormatUint(replicator.nextReplicationSequence(), 10)
	if replicator.topology != nil {
		if fingerprint := replicator.topology.Fingerprint(); fingerprint != "" {
			payload.Pairs[replicationMetaTopologyFingerprint] = fingerprint
		}
	}
	return payload
}

func (replicator *HTTPReplicator) nextReplicationSequence() uint64 {
	replicator.mu.Lock()
	defer replicator.mu.Unlock()
	replicator.sequence++
	return replicator.sequence
}

func (replicator *HTTPReplicator) syncAllPaged(ctx context.Context, trie *HatTrie, prefix string, pageSize int) ReplicationResult {
	ctx = replicationContext(ctx)
	result := ReplicationResult{Command: "SYNC", Key: prefix}
	if replicator == nil {
		result.Skipped = true
		result.Reason = "replication is not configured"
		return result
	}
	if trie == nil {
		result.Skipped = true
		result.Reason = "trie is not configured"
		return result
	}
	if err := ctx.Err(); err != nil {
		result.Skipped = true
		result.Reason = err.Error()
		return result
	}
	if pageSize <= 0 {
		pageSize = defaultReplicationSyncKeyPageSize
	}

	afterKey := ""
	hasAfterKey := false
	seenKeys := false
	for {
		page, err := replicationSyncKeysPage(trie, prefix, afterKey, hasAfterKey, pageSize)
		if err != nil {
			if len(result.Targets) == 0 {
				result.Skipped = true
				result.Reason = "no entries to sync"
			}
			return result
		}
		if len(page.keys) == 0 {
			if !seenKeys {
				result.Skipped = true
				result.Reason = "no entries to sync"
				return result
			}
			break
		}
		seenKeys = true

		for _, key := range page.keys {
			if err := ctx.Err(); err != nil {
				if len(result.Targets) == 0 {
					result.Skipped = true
					result.Reason = err.Error()
				}
				return result
			}
			route, ok := replicator.routeForKey(key)
			if !ok {
				continue
			}
			if replicator.self != "" && route.Leader.Leader != "" && route.Leader.Leader != replicator.self {
				continue
			}
			targets := replicator.replicationTargets(route)
			if len(targets) == 0 {
				continue
			}
			payload, ok := replicationCommandPayload(trie, key, replicationPayloadSet)
			if !ok {
				continue
			}
			payload = replicator.annotateReplicationPayload(payload)
			result.Entries++
			for _, target := range targets {
				targetResult := replicator.executeReplicationTarget(ctx, target, payload)
				targetResult.Key = key
				result.Targets = append(result.Targets, targetResult)
			}
		}

		if !page.hasMore {
			break
		}
		afterKey = page.nextAfterKey
		hasAfterKey = true
	}
	if len(result.Targets) == 0 {
		result.Skipped = true
		result.Reason = "no sync targets"
	}
	return result
}

type replicationSyncKeyPage struct {
	keys         []string
	nextAfterKey string
	hasMore      bool
}

func replicationSyncKeysPage(trie *HatTrie, prefix string, afterKey string, hasAfterKey bool, limit int) (replicationSyncKeyPage, error) {
	if limit <= 0 {
		limit = 1
	}

	trie.mu.Lock()
	defer trie.mu.Unlock()

	now := time.Time{}
	if len(trie.expires) > 0 {
		now = trie.currentTime()
	}

	page := replicationSyncKeyPage{keys: make([]string, 0, limit)}
	err := trie.scanEntriesWithPrefixAtLockedChecked(prefix, true, now, func(entry Entry) error {
		if hasAfterKey && entry.Key <= afterKey {
			return nil
		}
		if len(page.keys) >= limit {
			page.hasMore = true
			return errReplicationSyncKeyPageFull
		}
		page.keys = append(page.keys, entry.Key)
		page.nextAfterKey = entry.Key
		return nil
	})
	if errors.Is(err, errReplicationSyncKeyPageFull) {
		return page, nil
	}
	if err != nil {
		return replicationSyncKeyPage{}, err
	}
	return page, nil
}

func (replicator *HTTPReplicator) storeLastResult(result ReplicationResult) {
	if replicator == nil {
		return
	}
	replicator.mu.Lock()
	defer replicator.mu.Unlock()
	replicator.last = cloneReplicationResult(result)
}

func finishReplicationResult(result ReplicationResult, startedAt time.Time) ReplicationResult {
	if startedAt.IsZero() {
		startedAt = time.Now().UTC()
	}
	finishedAt := time.Now().UTC()
	result.StartedAt = cloneTimePtr(&startedAt)
	result.FinishedAt = cloneTimePtr(&finishedAt)
	result.DurationMillis = finishedAt.Sub(startedAt).Milliseconds()
	if result.DurationMillis < 0 {
		result.DurationMillis = 0
	}
	return result
}

func (replicator *HTTPReplicator) attachReplicationHealth(result ReplicationResult) ReplicationResult {
	if replicator != nil && replicator.queue != nil && result.Queue == nil {
		replicator.mu.RLock()
		stats := replicator.queueStatsLocked()
		replicator.mu.RUnlock()
		result.Queue = &stats
	}
	if replicator != nil && result.CircuitBreakers == nil {
		result.CircuitBreakers = replicator.replicationCircuitBreakers()
	}
	return withReplicationHealth(result)
}

func withReplicationHealth(result ReplicationResult) ReplicationResult {
	result.Health, result.HealthScore, result.HealthReason = replicationHealth(result)
	return result
}

func replicationHealth(result ReplicationResult) (string, int, string) {
	if result.Skipped && result.Reason == "replication is not configured" {
		return "disabled", 0, "replication is not configured"
	}

	score := 100
	reason := "healthy"
	if result.Queue != nil {
		queue := result.Queue
		if queue.Closed {
			return "unhealthy", 0, "replication queue is closed"
		}
		if queue.Capacity > 0 && queue.Depth > 0 {
			fillPercent := (queue.Depth * 100) / queue.Capacity
			switch {
			case fillPercent >= 100:
				score -= 45
				reason = "replication queue is full"
			case fillPercent >= 75:
				score -= 25
				if reason == "healthy" {
					reason = "replication queue is above 75%"
				}
			case fillPercent >= 50:
				score -= 10
				if reason == "healthy" {
					reason = "replication queue is above 50%"
				}
			}
		}
		if queue.Dropped > 0 {
			score -= 30
			reason = "async replication drops recorded"
		}
		if queue.Failures > 0 {
			penalty := 20
			if queue.Attempts > 0 {
				penalty = int(math.Ceil((float64(queue.Failures) / float64(queue.Attempts)) * 40))
				if penalty < 10 {
					penalty = 10
				}
				if penalty > 40 {
					penalty = 40
				}
			}
			score -= penalty
			if reason == "healthy" {
				reason = "target failures recorded"
			}
		}
		switch {
		case queue.OldestQueuedAgeMillis >= int64((5 * time.Minute).Milliseconds()):
			score -= 30
			reason = "old queued replication work"
		case queue.OldestQueuedAgeMillis >= int64((30 * time.Second).Milliseconds()):
			score -= 10
			if reason == "healthy" {
				reason = "queued replication work is aging"
			}
		}
		switch {
		case queue.InFlightAgeMillis >= int64((5 * time.Minute).Milliseconds()):
			score -= 30
			reason = "replication delivery is stuck"
		case queue.InFlightAgeMillis >= int64((30 * time.Second).Milliseconds()):
			score -= 10
			if reason == "healthy" {
				reason = "replication delivery is slow"
			}
		}
	}
	if result.DeadLetterCount > 0 {
		score -= 25
		if reason == "healthy" {
			reason = "replication dead letters retained"
		}
	}
	openBreakers := 0
	halfOpenBreakers := 0
	for _, breaker := range result.CircuitBreakers {
		switch breaker.State {
		case replicationCircuitOpen:
			openBreakers++
		case replicationCircuitHalfOpen:
			halfOpenBreakers++
		}
	}
	if openBreakers > 0 {
		score -= 30
		reason = "replication circuit breaker open"
	} else if halfOpenBreakers > 0 {
		score -= 10
		if reason == "healthy" {
			reason = "replication circuit breaker probing"
		}
	}

	failedTargets := 0
	for _, target := range result.Targets {
		if !target.OK {
			failedTargets++
		}
	}
	if failedTargets > 0 {
		if failedTargets == len(result.Targets) {
			score -= 40
			reason = "all replication targets failed"
		} else {
			score -= 20
			if reason == "healthy" {
				reason = "some replication targets failed"
			}
		}
	}

	score = clampReplicationHealthScore(score)
	status := "unhealthy"
	if score >= 90 {
		status = "ok"
	} else if score >= 60 {
		status = "degraded"
	}
	return status, score, reason
}

func clampReplicationHealthScore(score int) int {
	if score < 0 {
		return 0
	}
	if score > 100 {
		return 100
	}
	return score
}

func (replicator *HTTPReplicator) routeForKey(key string) (ElectionKeyRoute, bool) {
	if replicator.election != nil {
		return replicator.election.LeaderForKey(key)
	}
	if replicator.topology == nil {
		return ElectionKeyRoute{}, false
	}
	route, ok := replicator.topology.Route(key)
	if !ok {
		return ElectionKeyRoute{}, false
	}
	return ElectionKeyRoute{
		Key:   key,
		Route: route,
		Leader: ElectionLeader{
			Shard:      route.Shard.ID,
			Leader:     route.Shard.Primary,
			Available:  true,
			Primary:    route.Shard.Primary,
			Candidates: routeOwners(route.Shard),
		},
	}, true
}

func (replicator *HTTPReplicator) replicationTargets(route ElectionKeyRoute) []TopologyNode {
	if replicator.topology == nil {
		return nil
	}
	topology := replicator.topology.Get()
	nodes := topologyNodesByID(topology)
	online := onlineElectionNodes(replicator.election)
	owners := route.Route.Owners
	if len(owners) == 0 {
		owners = routeOwners(route.Route.Shard)
	}

	targets := make([]TopologyNode, 0, len(owners))
	seen := map[string]bool{}
	for _, nodeID := range owners {
		nodeID = strings.TrimSpace(nodeID)
		if nodeID == "" || nodeID == replicator.self || seen[nodeID] {
			continue
		}
		if online != nil && !online[nodeID] {
			continue
		}
		node, ok := nodes[nodeID]
		if !ok {
			continue
		}
		seen[nodeID] = true
		targets = append(targets, node)
	}
	sort.Slice(targets, func(i, j int) bool {
		return targets[i].ID < targets[j].ID
	})
	return targets
}

func (replicator *HTTPReplicator) executeReplicationTarget(ctx context.Context, target TopologyNode, payload CacheCommandRequest) ReplicationTargetResult {
	if result, allowed, state := replicator.beforeReplicationTarget(target); !allowed {
		return result
	} else {
		return replicator.afterReplicationTarget(target, state, replicator.postReplicationCommand(ctx, target, payload))
	}
}

func (replicator *HTTPReplicator) beforeReplicationTarget(target TopologyNode) (ReplicationTargetResult, bool, string) {
	result := ReplicationTargetResult{
		Node:    target.ID,
		Address: target.Address,
	}
	if !replicator.replicationCircuitBreakerEnabled() {
		return result, true, replicationCircuitClosed
	}
	node := replicationCircuitBreakerNode(target)
	if node == "" {
		return result, true, replicationCircuitClosed
	}
	now := time.Now().UTC()
	replicator.mu.Lock()
	defer replicator.mu.Unlock()
	state := replicator.breakers[node]
	switch state.state {
	case replicationCircuitOpen:
		if state.openUntil == nil || now.Before(*state.openUntil) {
			result.CircuitOpen = true
			result.CircuitState = replicationCircuitOpen
			result.CircuitOpenUntil = cloneTimePtr(state.openUntil)
			result.Error = "replication circuit breaker open"
			return result, false, replicationCircuitOpen
		}
		state.state = replicationCircuitHalfOpen
		replicator.breakers[node] = state
		return result, true, replicationCircuitHalfOpen
	case replicationCircuitHalfOpen:
		result.CircuitOpen = true
		result.CircuitState = replicationCircuitHalfOpen
		result.CircuitOpenUntil = cloneTimePtr(state.openUntil)
		result.Error = "replication circuit breaker half-open probe in progress"
		return result, false, replicationCircuitHalfOpen
	default:
		return result, true, replicationCircuitClosed
	}
}

func (replicator *HTTPReplicator) afterReplicationTarget(target TopologyNode, attemptState string, result ReplicationTargetResult) ReplicationTargetResult {
	if !replicator.replicationCircuitBreakerEnabled() {
		return result
	}
	node := replicationCircuitBreakerNode(target)
	if node == "" {
		return result
	}
	now := time.Now().UTC()
	replicator.mu.Lock()
	defer replicator.mu.Unlock()
	if result.OK {
		result.CircuitState = replicationCircuitClosed
		if existing, ok := replicator.breakers[node]; ok {
			existing.state = replicationCircuitClosed
			existing.failures = 0
			existing.openedAt = nil
			existing.openUntil = nil
			existing.lastSuccessAt = cloneTimePtr(&now)
			existing.lastFailureReason = ""
			if existing.lastFailureAt == nil {
				delete(replicator.breakers, node)
			} else {
				replicator.breakers[node] = existing
			}
		}
		return result
	}

	state := replicator.breakers[node]
	state.failures++
	state.lastFailureAt = cloneTimePtr(&now)
	state.lastFailureReason = replicationTargetFailureReason(result)
	if attemptState == replicationCircuitHalfOpen || state.failures >= replicator.breakerFailures {
		openUntil := now.Add(replicator.breakerCooldown)
		state.state = replicationCircuitOpen
		state.openedAt = cloneTimePtr(&now)
		state.openUntil = cloneTimePtr(&openUntil)
		result.CircuitOpen = true
		result.CircuitState = replicationCircuitOpen
		result.CircuitOpenUntil = cloneTimePtr(&openUntil)
	} else {
		state.state = replicationCircuitClosed
		result.CircuitState = replicationCircuitClosed
	}
	replicator.breakers[node] = state
	return result
}

func (replicator *HTTPReplicator) replicationCircuitBreakerEnabled() bool {
	return replicator != nil && replicator.breakerFailures > 0 && replicator.breakerCooldown > 0
}

func replicationCircuitBreakerNode(target TopologyNode) string {
	if node := strings.TrimSpace(target.ID); node != "" {
		return node
	}
	return strings.TrimSpace(target.Address)
}

func replicationTargetFailureReason(result ReplicationTargetResult) string {
	if result.Error != "" {
		return result.Error
	}
	if result.Status != 0 {
		return fmt.Sprintf("target returned HTTP %d", result.Status)
	}
	return "replication target delivery failed"
}

func (replicator *HTTPReplicator) postReplicationCommand(ctx context.Context, target TopologyNode, payload CacheCommandRequest) ReplicationTargetResult {
	ctx = replicationContext(ctx)
	result := ReplicationTargetResult{
		Node:    target.ID,
		Address: target.Address,
	}
	if err := ctx.Err(); err != nil {
		result.Error = err.Error()
		return result
	}
	endpoint, err := replicationEndpoint(target.Address)
	if err != nil {
		result.Error = err.Error()
		return result
	}
	postCtx := ctx
	cancel := func() {}
	if replicator.timeout > 0 {
		postCtx, cancel = context.WithTimeout(ctx, replicator.timeout)
	}
	defer cancel()

	body, contentType, contentEncoding, err := replicationRequestBodyForFormat(payload, replicator.wireFormat)
	if err != nil {
		result.Error = err.Error()
		return result
	}
	req, err := http.NewRequestWithContext(postCtx, http.MethodPost, endpoint, body)
	if err != nil {
		result.Error = err.Error()
		return result
	}
	req.Header.Set("Accept", contentType)
	req.Header.Set("Content-Type", contentType)
	if contentEncoding != "" {
		req.Header.Set("Content-Encoding", contentEncoding)
	}
	resp, err := replicator.client.Do(req)
	if err != nil {
		result.Error = err.Error()
		return result
	}
	defer drainAndClose(resp.Body)

	result.Status = resp.StatusCode
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		result.Error = resp.Status
		return result
	}
	commandResponse, err := decodeReplicationCommandResponseWire(resp.Body, resp.Header.Get("Content-Type"))
	if err != nil {
		result.Error = err.Error()
		return result
	}
	if !commandResponse.OK {
		result.Error = commandResponse.Message
		return result
	}
	result.OK = true
	return result
}

func decodeReplicationCommandResponse(body io.Reader) (CacheCommandResponse, error) {
	return decodeReplicationCommandResponseWire(body, commandWireContentTypeJSON)
}

func decodeReplicationCommandResponseWire(body io.Reader, contentType string) (CacheCommandResponse, error) {
	return decodeCommandResponseWire(body, contentType, maxHTTPReplicationResponseBytes)
}

func replicationRequestBody(payload CacheCommandRequest) (io.Reader, string, error) {
	body, _, contentEncoding, err := replicationRequestBodyForFormat(payload, CommandWireFormatJSON)
	return body, contentEncoding, err
}

func replicationRequestBodyForFormat(payload CacheCommandRequest, format CommandWireFormat) (io.Reader, string, string, error) {
	estimatedBytes := estimatedReplicationRequestBytes(payload)
	body, contentType, contentEncoding, err := commandRequestBody(payload, format, estimatedBytes, minCompressedReplicationRequestBytes)
	if err == nil || format != CommandWireFormatProtobuf || !errors.Is(err, ErrUnsupportedCommandWireProtobufValue) {
		return body, contentType, contentEncoding, err
	}
	return commandRequestBody(payload, CommandWireFormatJSON, estimatedBytes, minCompressedReplicationRequestBytes)
}

func estimatedReplicationRequestBytes(payload CacheCommandRequest) int {
	estimate := 64 +
		jsonwire.EstimateJSONStringBytes(payload.Command) +
		jsonwire.EstimateJSONStringBytes(payload.Key) +
		jsonwire.EstimateJSONStringBytes(payload.Value) +
		jsonwire.EstimateJSONStringBytes(payload.Subkey)
	if estimate >= minCompressedReplicationRequestBytes {
		return minCompressedReplicationRequestBytes
	}
	for _, value := range payload.Values {
		estimate = jsonwire.AddEstimate(estimate, jsonwire.EstimateJSONValueBytes(value, minCompressedReplicationRequestBytes), minCompressedReplicationRequestBytes)
		if estimate >= minCompressedReplicationRequestBytes {
			return minCompressedReplicationRequestBytes
		}
	}
	for key, value := range payload.Pairs {
		estimate = jsonwire.AddEstimate(estimate, jsonwire.EstimateJSONStringBytes(key)+1, minCompressedReplicationRequestBytes)
		if estimate >= minCompressedReplicationRequestBytes {
			return minCompressedReplicationRequestBytes
		}
		estimate = jsonwire.AddEstimate(estimate, jsonwire.EstimateJSONValueBytes(value, minCompressedReplicationRequestBytes), minCompressedReplicationRequestBytes)
		if estimate >= minCompressedReplicationRequestBytes {
			return minCompressedReplicationRequestBytes
		}
	}
	if payload.Priority != nil {
		estimate = addEstimatedOptionalCommandInt64(estimate, payload.Priority, minCompressedReplicationRequestBytes)
	}
	if payload.TTLSeconds != nil {
		estimate = addEstimatedOptionalCommandInt64(estimate, payload.TTLSeconds, minCompressedReplicationRequestBytes)
	}
	if payload.UnixSeconds != nil {
		estimate = addEstimatedOptionalCommandInt64(estimate, payload.UnixSeconds, minCompressedReplicationRequestBytes)
	}
	return estimate
}

func addEstimatedOptionalCommandInt64(estimate int, value *int64, threshold int) int {
	if value == nil {
		return estimate
	}
	return jsonwire.AddEstimate(estimate, jsonwire.EstimateJSONValueBytes(*value, threshold), threshold)
}

func drainAndClose(body io.ReadCloser) {
	if body == nil {
		return
	}
	_, _ = io.CopyN(io.Discard, body, maxHTTPResponseDrainBytes)
	_ = body.Close()
}

func replicationContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func replicationCommandPayload(trie *HatTrie, key string, kind replicationPayloadKind) (CacheCommandRequest, bool) {
	if kind == replicationPayloadDelete {
		return CacheCommandRequest{Command: "INTERNALDEL", Key: key}, true
	}
	dump := trie.ExecuteCommand(CacheCommandRequest{Command: "DUMP", Key: key})
	if !dump.OK || strings.TrimSpace(dump.Value) == "" {
		return CacheCommandRequest{}, false
	}
	return CacheCommandRequest{Command: "INTERNALSET", Key: key, Value: dump.Value}, true
}

func replicationSafetyMetadata(request CacheCommandRequest) (string, uint64, string) {
	source := commandPairString(request.Pairs, replicationMetaSourceNode)
	sequence := uint64(0)
	if value, ok := request.Pairs[replicationMetaSequence]; ok {
		if parsed, err := commandUint64Value(value); err == nil {
			sequence = parsed
		}
	}
	return source, sequence, commandPairString(request.Pairs, replicationMetaTopologyFingerprint)
}

func commandPairString(pairs Map, key string) string {
	if len(pairs) == 0 {
		return ""
	}
	value, ok := pairs[key]
	if !ok {
		return ""
	}
	text, err := commandScalarString(value)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(text)
}

func replicationPayloadKindFor(request CacheCommandRequest, response CacheCommandResponse) replicationPayloadKind {
	if !response.OK {
		return replicationPayloadNone
	}
	command := normalizedCommand(request.Command)
	if command == "" || strings.TrimSpace(request.Key) == "" {
		return replicationPayloadNone
	}
	switch command {
	case "INTERNALSET", "INTERNALDEL":
		return replicationPayloadNone
	case "DEL":
		return replicationPayloadDelete
	}
	if !commandShouldJournal(request) {
		return replicationPayloadNone
	}
	return replicationPayloadSet
}

func replicationEndpoint(address string) (string, error) {
	address = strings.TrimSpace(address)
	if address == "" {
		return "", errors.New("replication target address is required")
	}
	if !strings.Contains(address, "://") {
		address = "http://" + address
	}
	parsed, err := url.Parse(address)
	if err != nil {
		return "", err
	}
	if parsed.Scheme == "" || parsed.Host == "" {
		return "", errors.New("replication target address is invalid")
	}
	parsed.Path = strings.TrimRight(parsed.Path, "/") + "/api/commands"
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return parsed.String(), nil
}

func normalizedCommand(command string) string {
	return strings.ToUpper(strings.TrimSpace(command))
}

func topologyNodesByID(topology ClusterTopology) map[string]TopologyNode {
	nodes := make(map[string]TopologyNode, len(topology.Nodes))
	for _, node := range topology.Nodes {
		nodes[node.ID] = node
	}
	return nodes
}

func onlineElectionNodes(election *ElectionStore) map[string]bool {
	if election == nil {
		return nil
	}
	status := election.Status()
	nodes := make(map[string]bool, len(status.Nodes))
	for _, node := range status.Nodes {
		nodes[node.ID] = node.Online
	}
	return nodes
}

func (replicator *HTTPReplicator) replicationCircuitBreakers() []ReplicationCircuitBreakerTarget {
	if replicator == nil {
		return nil
	}
	replicator.mu.RLock()
	defer replicator.mu.RUnlock()
	return replicator.replicationCircuitBreakersLocked()
}

func (replicator *HTTPReplicator) replicationCircuitBreakersLocked() []ReplicationCircuitBreakerTarget {
	if replicator == nil || len(replicator.breakers) == 0 {
		return nil
	}
	out := make([]ReplicationCircuitBreakerTarget, 0, len(replicator.breakers))
	for node, state := range replicator.breakers {
		if state.failures == 0 && state.state != replicationCircuitOpen && state.state != replicationCircuitHalfOpen {
			continue
		}
		publicState := state.state
		if publicState == "" {
			publicState = replicationCircuitClosed
		}
		out = append(out, ReplicationCircuitBreakerTarget{
			Node:              node,
			State:             publicState,
			Failures:          state.failures,
			OpenedAt:          cloneTimePtr(state.openedAt),
			OpenUntil:         cloneTimePtr(state.openUntil),
			LastFailureAt:     cloneTimePtr(state.lastFailureAt),
			LastSuccessAt:     cloneTimePtr(state.lastSuccessAt),
			LastFailureReason: state.lastFailureReason,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Node < out[j].Node
	})
	return out
}

func cloneReplicationResult(result ReplicationResult) ReplicationResult {
	out := result
	out.StartedAt = cloneTimePtr(result.StartedAt)
	out.FinishedAt = cloneTimePtr(result.FinishedAt)
	if result.Queue != nil {
		stats := cloneReplicationQueueStats(*result.Queue)
		out.Queue = &stats
	}
	if result.DeadLetters != nil {
		out.DeadLetters = cloneReplicationDeadLetters(result.DeadLetters)
	}
	if result.CircuitBreakers != nil {
		out.CircuitBreakers = cloneReplicationCircuitBreakers(result.CircuitBreakers)
	}
	if result.Targets != nil {
		out.Targets = cloneReplicationTargets(result.Targets)
	}
	return out
}

func cloneReplicationDeadLetters(deadLetters []ReplicationDeadLetter) []ReplicationDeadLetter {
	if deadLetters == nil {
		return nil
	}
	out := make([]ReplicationDeadLetter, len(deadLetters))
	for idx, deadLetter := range deadLetters {
		out[idx] = deadLetter
		out[idx].FailedAt = cloneTimePtr(deadLetter.FailedAt)
		if deadLetter.Targets != nil {
			out[idx].Targets = cloneReplicationTargets(deadLetter.Targets)
		}
	}
	return out
}

func cloneReplicationCircuitBreakers(breakers []ReplicationCircuitBreakerTarget) []ReplicationCircuitBreakerTarget {
	if breakers == nil {
		return nil
	}
	out := make([]ReplicationCircuitBreakerTarget, len(breakers))
	for idx, breaker := range breakers {
		out[idx] = breaker
		out[idx].OpenedAt = cloneTimePtr(breaker.OpenedAt)
		out[idx].OpenUntil = cloneTimePtr(breaker.OpenUntil)
		out[idx].LastFailureAt = cloneTimePtr(breaker.LastFailureAt)
		out[idx].LastSuccessAt = cloneTimePtr(breaker.LastSuccessAt)
	}
	return out
}

func cloneReplicationTargets(targets []ReplicationTargetResult) []ReplicationTargetResult {
	if targets == nil {
		return nil
	}
	out := make([]ReplicationTargetResult, len(targets))
	for idx, target := range targets {
		out[idx] = target
		out[idx].CircuitOpenUntil = cloneTimePtr(target.CircuitOpenUntil)
	}
	return out
}

func cloneReplicationQueueStats(stats ReplicationQueueStats) ReplicationQueueStats {
	out := stats
	out.OldestQueuedAt = cloneTimePtr(stats.OldestQueuedAt)
	out.InFlightStartedAt = cloneTimePtr(stats.InFlightStartedAt)
	out.LastRetryAt = cloneTimePtr(stats.LastRetryAt)
	if stats.OldestQueuedTargets != nil {
		out.OldestQueuedTargets = append([]string(nil), stats.OldestQueuedTargets...)
	}
	if stats.DroppedByTarget != nil {
		out.DroppedByTarget = make(map[string]uint64, len(stats.DroppedByTarget))
		for target, count := range stats.DroppedByTarget {
			out.DroppedByTarget[target] = count
		}
	}
	if stats.FailuresByTarget != nil {
		out.FailuresByTarget = make(map[string]uint64, len(stats.FailuresByTarget))
		for target, count := range stats.FailuresByTarget {
			out.FailuresByTarget[target] = count
		}
	}
	return out
}
