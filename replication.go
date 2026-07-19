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
	"sync/atomic"
	"time"

	"hatrie_cache/internal/jsonwire"

	"google.golang.org/grpc"
)

const DefaultReplicationTimeout = 2 * time.Second
const DefaultReplicationRetryInterval = 250 * time.Millisecond
const DefaultReplicationDeadLetterLimit = 128
const DefaultReplicationCircuitBreakerFailures = 5
const DefaultReplicationCircuitBreakerCooldown = 30 * time.Second
const DefaultReplicationBatchMaxBytes = 1 << 20
const DefaultReplicationMaxInFlightTargets = 4
const DefaultReplicationGRPCStreamWindow = 32
const MaxReplicationGRPCStreamWindow = 1024
const maxHTTPReplicationResponseBytes = 1 << 20
const maxHTTPResponseDrainBytes = 1 << 20
const minCompressedReplicationRequestBytes = 16 << 10
const defaultReplicationSyncKeyPageSize = 1024
const replicationLinearGroupTaskLimit = 16
const replicationLinearGroupTargetLimit = 4
const replicationBatchEnvelopeCommand = "INTERNALBATCHV2"
const replicationSetBinaryCommand = "INTERNALSETV2"
const replicationSetCompactCommand = "INTERNALSETV3"
const replicationDigestCommand = "INTERNALDIGESTV1"

type ReplicationTransport string

const (
	ReplicationTransportHTTP       ReplicationTransport = "http"
	ReplicationTransportGRPCStream ReplicationTransport = "grpc-stream"
)

func ParseReplicationTransport(value string) (ReplicationTransport, error) {
	switch ReplicationTransport(strings.ToLower(strings.TrimSpace(value))) {
	case "", ReplicationTransportHTTP:
		return ReplicationTransportHTTP, nil
	case ReplicationTransportGRPCStream:
		return ReplicationTransportGRPCStream, nil
	default:
		return "", errors.New("hatriecache: replication transport must be http or grpc-stream")
	}
}

var (
	errReplicationResponseTooLarge = errors.New("hatriecache: replication response is too large")
)

type HTTPReplicatorOptions struct {
	Context                  context.Context
	Self                     string
	Topology                 *TopologyStore
	Election                 *ElectionStore
	Client                   *http.Client
	Timeout                  time.Duration
	AsyncQueueSize           int
	AsyncRetryInterval       time.Duration
	AsyncMaxAttempts         uint
	AsyncDeadLetterLimit     int
	AsyncOutbox              *ReplicationOutboxStore
	CircuitBreakerFailures   int
	CircuitBreakerCooldown   time.Duration
	WireFormat               CommandWireFormat
	AuthToken                string
	ReplicationBatchMaxBytes int
	MaxInFlightTargets       int
	Transport                ReplicationTransport
	GRPCStreamWindow         int
	DisableHTTPFallback      bool
	GRPCDialOptions          []grpc.DialOption
}

type HTTPReplicator struct {
	mu                  sync.RWMutex
	self                string
	topology            *TopologyStore
	election            *ElectionStore
	client              *http.Client
	timeout             time.Duration
	queue               chan replicationJob
	retry               time.Duration
	attempts            uint
	wireFormat          CommandWireFormat
	authToken           string
	outbox              *ReplicationOutboxStore
	breakerFailures     int
	breakerCooldown     time.Duration
	batchMaxBytes       int
	maxInFlight         int
	transport           ReplicationTransport
	grpcStreamWindow    int
	disableHTTPFallback bool
	grpcDialOptions     []grpc.DialOption
	grpcLiveSession     *replicationGRPCSyncSession
	grpcLiveCancel      context.CancelFunc
	grpcStreamBatches   atomic.Uint64
	breakers            map[string]replicationCircuitBreakerState
	done                chan struct{}
	stopped             chan struct{}
	asyncCtx            context.Context
	cancel              context.CancelFunc
	close               sync.Once
	closed              bool
	sequence            uint64
	queueSeq            uint64
	deadSeq             uint64
	deadLimit           int
	pending             []replicationQueueMeta
	deadLetters         []ReplicationDeadLetter
	queueStats          ReplicationQueueStats
	last                ReplicationResult
	metrics             replicationMetrics
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
	Node                        string     `json:"node"`
	Key                         string     `json:"key,omitempty"`
	Address                     string     `json:"address,omitempty"`
	OK                          bool       `json:"ok"`
	Status                      int        `json:"status,omitempty"`
	Error                       string     `json:"error,omitempty"`
	CircuitOpen                 bool       `json:"circuit_open,omitempty"`
	CircuitState                string     `json:"circuit_state,omitempty"`
	CircuitOpenUntil            *time.Time `json:"circuit_open_until,omitempty"`
	unsupportedTypedReplication bool
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
	target       TopologyNode
	payload      CacheCommandRequest
	payloadBytes int
}

type plannedReplicationBatch struct {
	last    ReplicationResult
	tasks   []replicationTask
	entries int
	seen    bool
}

type replicationTaskGroup struct {
	target             TopologyNode
	payloads           []CacheCommandRequest
	syncPayloads       []replicationSyncPayload
	syncPayloadArena   *replicationSyncPayloadArena
	syncPayloadIndexes []uint32
	keys               []string
	payloadBytes       []int
	deferredMetadata   bool
	metadataSource     string
	metadataTopology   string
}

func (group replicationTaskGroup) replicationSyncPayloadBatch() replicationSyncPayloadBatch {
	return replicationSyncPayloadBatch{
		inline:  group.syncPayloads,
		arena:   group.syncPayloadArena,
		indexes: group.syncPayloadIndexes,
	}
}

type replicationRoutingSnapshot struct {
	topology    ClusterTopology
	shards      []TopologyShard
	nodes       map[string]TopologyNode
	online      map[string]bool
	leaders     map[uint32]ElectionLeader
	owners      map[uint32][]string
	targets     map[uint32][]TopologyNode
	self        string
	fingerprint string
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
	scope    string
	sequence uint64
}

func NewReplicationSafetyStore() *ReplicationSafetyStore {
	return &ReplicationSafetyStore{last: map[string]uint64{}}
}

func (store *ReplicationSafetyStore) Check(source string, sequence uint64) (replicationSafetyToken, bool) {
	return store.checkScope("source:"+source, sequence)
}

func (store *ReplicationSafetyStore) CheckKey(source string, key string, sequence uint64) (replicationSafetyToken, bool) {
	if source == "" || sequence == 0 {
		return replicationSafetyToken{}, false
	}
	if strings.TrimSpace(key) == "" {
		return store.Check(source, sequence)
	}
	scope := "key:" + strconv.Itoa(len(source)) + ":" + source + key
	return store.checkScope(scope, sequence)
}

func (store *ReplicationSafetyStore) checkScope(scope string, sequence uint64) (replicationSafetyToken, bool) {
	if store == nil || scope == "source:" || sequence == 0 {
		return replicationSafetyToken{}, false
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	token := replicationSafetyToken{scope: scope, sequence: sequence}
	return token, sequence <= store.last[scope]
}

func (store *ReplicationSafetyStore) Commit(token replicationSafetyToken) {
	if store == nil || token.scope == "" || token.sequence == 0 {
		return
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if token.sequence > store.last[token.scope] {
		store.last[token.scope] = token.sequence
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
	transport, err := ParseReplicationTransport(string(options.Transport))
	if err != nil {
		transport = ReplicationTransportHTTP
	}
	replicator := &HTTPReplicator{
		self:                strings.TrimSpace(options.Self),
		topology:            options.Topology,
		election:            options.Election,
		client:              client,
		timeout:             timeout,
		wireFormat:          wireFormat,
		authToken:           normalizeAuthToken(options.AuthToken),
		outbox:              options.AsyncOutbox,
		breakerFailures:     options.CircuitBreakerFailures,
		breakerCooldown:     options.CircuitBreakerCooldown,
		batchMaxBytes:       options.ReplicationBatchMaxBytes,
		maxInFlight:         options.MaxInFlightTargets,
		transport:           transport,
		grpcStreamWindow:    options.GRPCStreamWindow,
		disableHTTPFallback: options.DisableHTTPFallback,
		grpcDialOptions:     append([]grpc.DialOption(nil), options.GRPCDialOptions...),
	}
	if replicator.batchMaxBytes == 0 {
		replicator.batchMaxBytes = DefaultReplicationBatchMaxBytes
	} else if replicator.batchMaxBytes < 0 {
		replicator.batchMaxBytes = 0
	}
	if replicator.maxInFlight == 0 {
		replicator.maxInFlight = DefaultReplicationMaxInFlightTargets
	} else if replicator.maxInFlight < 0 {
		replicator.maxInFlight = 1
	}
	if replicator.grpcStreamWindow == 0 {
		replicator.grpcStreamWindow = DefaultReplicationGRPCStreamWindow
	} else if replicator.grpcStreamWindow < 0 {
		replicator.grpcStreamWindow = 1
	} else if replicator.grpcStreamWindow > MaxReplicationGRPCStreamWindow {
		replicator.grpcStreamWindow = MaxReplicationGRPCStreamWindow
	}
	if replicator.transport == ReplicationTransportGRPCStream {
		parent := options.Context
		if parent == nil {
			parent = context.Background()
		}
		liveCtx, cancel := context.WithCancel(parent)
		replicator.grpcLiveCancel = cancel
		replicator.grpcLiveSession = newReplicationGRPCLiveSession(liveCtx, replicator)
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
	if replicator == nil {
		return
	}
	replicator.close.Do(func() {
		replicator.mu.Lock()
		replicator.closed = true
		if replicator.queue != nil {
			replicator.queueStats.Closed = true
		}
		replicator.mu.Unlock()
		if replicator.grpcLiveCancel != nil {
			replicator.grpcLiveCancel()
		}
		if replicator.grpcLiveSession != nil {
			replicator.grpcLiveSession.close()
		}
		if replicator.queue != nil {
			replicator.cancel()
			close(replicator.done)
			<-replicator.stopped
		}
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

func (replicator *HTTPReplicator) replicatePlannedBatch(ctx context.Context, planned plannedReplicationBatch) ReplicationResult {
	if replicator == nil {
		return withReplicationHealth(ReplicationResult{
			Command: "BATCH",
			Skipped: true,
			Reason:  "replication is not configured",
		})
	}
	startedAt := time.Now().UTC()
	result := replicator.replicatePlannedBatchTasks(ctx, planned)
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

func (replicator *HTTPReplicator) replicatePlannedBatchTasks(ctx context.Context, planned plannedReplicationBatch) ReplicationResult {
	result, tasks := aggregatePlannedReplication(planned)
	if len(tasks) == 0 {
		return result
	}
	if replicator.queue != nil {
		return replicator.enqueueReplicationTasks(result, tasks)
	}
	return replicator.executeReplicationTasks(replicationContext(ctx), result, tasks)
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
	return replicator.enqueueReplicationTasks(result, tasks)
}

func (replicator *HTTPReplicator) enqueueReplicationTasks(result ReplicationResult, tasks []replicationTask) ReplicationResult {
	if len(tasks) == 0 {
		return result
	}
	if replicator.asyncClosed() {
		result.Queued = false
		result.Skipped = true
		result.Reason = "replication queue is closed"
		result.Targets = nil
		replicator.recordAsyncDroppedForTasks(tasks)
		return result
	}
	result.Queued = true
	result.Targets = replicator.plannedReplicationTargets(tasks)
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

func (planned *plannedReplicationBatch) add(result ReplicationResult, tasks []replicationTask) {
	if planned == nil {
		return
	}
	planned.seen = true
	planned.last = result
	if len(tasks) == 0 {
		if result.Reason != "command is not replicated" {
			planned.entries++
		}
		return
	}
	planned.entries++
	planned.tasks = append(planned.tasks, tasks...)
}

func aggregatePlannedReplication(planned plannedReplicationBatch) (ReplicationResult, []replicationTask) {
	result := ReplicationResult{
		Command: "BATCH",
		Entries: planned.entries,
	}
	if !planned.seen {
		result.Skipped = true
		result.Reason = "command is not replicated"
		return result, nil
	}
	if len(planned.tasks) == 0 {
		result.Skipped = true
		result.Reason = planned.last.Reason
		return result, nil
	}
	return result, planned.tasks
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

func (replicator *HTTPReplicator) completeAsyncJob(id uint64, deadSeq uint64, deadLetters []ReplicationDeadLetter, updateDeadLetters bool) {
	if replicator == nil || replicator.outbox == nil || id == 0 {
		return
	}
	_ = replicator.outbox.completeJob(id, deadSeq, deadLetters, updateDeadLetters)
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

func (replicator *HTTPReplicator) recordDeadLetter(result ReplicationResult, attempts uint) (uint64, []ReplicationDeadLetter, bool) {
	if replicator == nil || replicator.queue == nil || replicator.deadLimit <= 0 {
		return 0, nil, false
	}
	failedTargets := make([]ReplicationTargetResult, 0, len(result.Targets))
	for _, target := range result.Targets {
		if !target.OK {
			failedTargets = append(failedTargets, target)
		}
	}
	if len(failedTargets) == 0 {
		return 0, nil, false
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
	return replicator.deadSeq, cloneReplicationDeadLetters(replicator.deadLetters), true
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
	if trie == nil {
		result := ReplicationResult{
			Command: normalizedCommand(request.Command),
			Key:     strings.TrimSpace(request.Key),
			Skipped: true,
			Reason:  "trie is not configured",
		}
		return result, nil
	}
	result, kind, targets, ok := replicator.planReplicationTargets(ctx, request, response)
	if !ok {
		return result, nil
	}

	payload, ok := replicationCommandPayload(trie, result.Key, kind)
	if !ok {
		result.Skipped = true
		result.Reason = "no local value to replicate"
		return result, nil
	}
	return replicator.tasksForReplicationPayload(result, targets, payload)
}

func (replicator *HTTPReplicator) planReplicationLocked(ctx context.Context, trie *HatTrie, request CacheCommandRequest, response CacheCommandResponse) (ReplicationResult, []replicationTask) {
	if trie == nil {
		result := ReplicationResult{
			Command: normalizedCommand(request.Command),
			Key:     strings.TrimSpace(request.Key),
			Skipped: true,
			Reason:  "trie is not configured",
		}
		return result, nil
	}
	result, kind, targets, ok := replicator.planReplicationTargets(ctx, request, response)
	if !ok {
		return result, nil
	}

	payload, ok := replicationCommandPayloadLocked(trie, result.Key, kind)
	if !ok {
		result.Skipped = true
		result.Reason = "no local value to replicate"
		return result, nil
	}
	return replicator.tasksForReplicationPayload(result, targets, payload)
}

func (replicator *HTTPReplicator) planReplicationTargets(ctx context.Context, request CacheCommandRequest, response CacheCommandResponse) (ReplicationResult, replicationPayloadKind, []TopologyNode, bool) {
	ctx = replicationContext(ctx)
	command := normalizedCommand(request.Command)
	key := strings.TrimSpace(request.Key)
	result := ReplicationResult{Command: command, Key: key}
	if replicator == nil {
		result.Skipped = true
		result.Reason = "replication is not configured"
		return result, replicationPayloadNone, nil, false
	}
	if err := ctx.Err(); err != nil {
		result.Skipped = true
		result.Reason = err.Error()
		return result, replicationPayloadNone, nil, false
	}
	kind := replicationPayloadKindFor(request, response)
	if kind == replicationPayloadNone {
		result.Skipped = true
		result.Reason = "command is not replicated"
		return result, kind, nil, false
	}

	route, ok := replicator.routeForKey(key)
	if !ok {
		result.Skipped = true
		result.Reason = "topology cannot route key"
		return result, kind, nil, false
	}
	if replicator.self != "" && route.Leader.Leader != "" && route.Leader.Leader != replicator.self {
		result.Skipped = true
		result.Reason = "local node is not elected leader"
		return result, kind, nil, false
	}

	targets := replicator.replicationTargets(route)
	if len(targets) == 0 {
		result.Skipped = true
		result.Reason = "no remote replication targets"
		return result, kind, nil, false
	}
	return result, kind, targets, true
}

func (replicator *HTTPReplicator) tasksForReplicationPayload(result ReplicationResult, targets []TopologyNode, payload CacheCommandRequest) (ReplicationResult, []replicationTask) {
	tasks := make([]replicationTask, 0, len(targets))
	return result, replicator.appendReplicationTasksForTargets(tasks, targets, payload)
}

func (replicator *HTTPReplicator) appendReplicationTasksForTargets(tasks []replicationTask, targets []TopologyNode, payload CacheCommandRequest) []replicationTask {
	fingerprint := ""
	if replicator.topology != nil {
		fingerprint = replicator.topology.Fingerprint()
	}
	return replicator.appendReplicationTasksForTargetsWithFingerprint(tasks, targets, payload, fingerprint)
}

func (replicator *HTTPReplicator) appendReplicationTasksForTargetsWithFingerprint(tasks []replicationTask, targets []TopologyNode, payload CacheCommandRequest, fingerprint string) []replicationTask {
	payload = replicator.annotateReplicationPayloadWithFingerprint(payload, fingerprint)
	payloadBytes := 0
	if replicator.batchMaxBytes > 0 {
		payloadBytes = estimatedReplicationRequestBytesWithin(payload, replicationPayloadEstimateThreshold(replicator.batchMaxBytes))
	}
	for _, target := range targets {
		tasks = append(tasks, replicationTask{target: target, payload: payload, payloadBytes: payloadBytes})
	}
	return tasks
}

func (replicator *HTTPReplicator) appendReplicationPayloadToTargetGroups(groups []replicationTaskGroup, indexes map[TopologyNode]int, groupCapacity int, targets []TopologyNode, payload CacheCommandRequest, fingerprint string) []replicationTaskGroup {
	payloadBytes := 0
	if replicator.batchMaxBytes > 0 {
		payloadBytes = estimatedReplicationRequestBytesWithin(payload, replicationPayloadEstimateThreshold(replicator.batchMaxBytes))
	}
	key := strings.TrimSpace(payload.Key)
	for _, target := range targets {
		idx, ok := indexes[target]
		if !ok {
			idx = len(groups)
			indexes[target] = idx
			groups = append(groups, replicationTaskGroup{
				target:           target,
				payloads:         make([]CacheCommandRequest, 0, groupCapacity),
				keys:             make([]string, 0, groupCapacity),
				payloadBytes:     make([]int, 0, groupCapacity),
				deferredMetadata: true,
				metadataSource:   replicator.self,
				metadataTopology: fingerprint,
			})
		}
		groups[idx].payloads = append(groups[idx].payloads, payload)
		groups[idx].keys = append(groups[idx].keys, key)
		groups[idx].payloadBytes = append(groups[idx].payloadBytes, payloadBytes)
	}
	return groups
}

func (replicator *HTTPReplicator) appendReplicationSyncPayloadToTargetGroups(groups []replicationTaskGroup, indexes map[TopologyNode]int, groupCapacity int, targets []TopologyNode, key string, binaryValue []byte, fingerprint string) []replicationTaskGroup {
	payloadBytes := 0
	if replicator.batchMaxBytes > 0 {
		payloadBytes = estimatedReplicationRequestBytesWithin(CacheCommandRequest{Command: replicationSetCompactCommand, Key: key, BinaryValue: binaryValue}, replicationPayloadEstimateThreshold(replicator.batchMaxBytes))
	}
	payload := replicationSyncPayload{
		key:          key,
		binaryValue:  binaryValue,
		payloadBytes: payloadBytes,
	}
	for _, target := range targets {
		idx, ok := indexes[target]
		if !ok {
			idx = len(groups)
			indexes[target] = idx
			groups = append(groups, replicationTaskGroup{
				target:           target,
				syncPayloads:     make([]replicationSyncPayload, 0, groupCapacity),
				deferredMetadata: true,
				metadataSource:   replicator.self,
				metadataTopology: fingerprint,
			})
		}
		groups[idx].syncPayloads = append(groups[idx].syncPayloads, payload)
	}
	return groups
}

func (replicator *HTTPReplicator) appendReplicationSyncArenaPayloadToTargetGroups(groups []replicationTaskGroup, indexes map[TopologyNode]int, groupCapacity int, targets []TopologyNode, arena *replicationSyncPayloadArena, key string, binaryValue []byte, fingerprint string) ([]replicationTaskGroup, error) {
	payloadBytes := 0
	if replicator.batchMaxBytes > 0 {
		payloadBytes = estimatedReplicationRequestBytesWithin(CacheCommandRequest{Command: replicationSetCompactCommand, Key: key, BinaryValue: binaryValue}, replicationPayloadEstimateThreshold(replicator.batchMaxBytes))
	}
	recordIndex, err := arena.append(key, binaryValue, payloadBytes)
	if err != nil {
		return groups, err
	}
	return replicator.appendReplicationSyncArenaRecordToTargetGroups(groups, indexes, groupCapacity, targets, arena, recordIndex, fingerprint), nil
}

func (replicator *HTTPReplicator) appendReplicationSyncArenaRecordToTargetGroups(groups []replicationTaskGroup, indexes map[TopologyNode]int, groupCapacity int, targets []TopologyNode, arena *replicationSyncPayloadArena, recordIndex uint32, fingerprint string) []replicationTaskGroup {
	for _, target := range targets {
		idx, ok := indexes[target]
		if !ok {
			idx = len(groups)
			indexes[target] = idx
			groups = append(groups, replicationTaskGroup{
				target:             target,
				syncPayloadArena:   arena,
				syncPayloadIndexes: make([]uint32, 0, groupCapacity),
				deferredMetadata:   true,
				metadataSource:     replicator.self,
				metadataTopology:   fingerprint,
			})
		}
		groups[idx].syncPayloadIndexes = append(groups[idx].syncPayloadIndexes, recordIndex)
	}
	return groups
}

func (replicator *HTTPReplicator) executeReplicationTasks(ctx context.Context, result ReplicationResult, tasks []replicationTask) ReplicationResult {
	return replicator.executeReplicationTaskGroups(ctx, result, replicator.groupReplicationTasksByTarget(tasks))
}

func (replicator *HTTPReplicator) executeReplicationTaskGroups(ctx context.Context, result ReplicationResult, groups []replicationTaskGroup) ReplicationResult {
	result.Queued = false
	result.Targets = make([]ReplicationTargetResult, len(groups))
	maxInFlight := replicator.maxInFlight
	if maxInFlight <= 1 || len(groups) <= 1 {
		for idx, group := range groups {
			result.Targets[idx] = replicator.executeReplicationTaskGroup(ctx, group)
		}
		return result
	}
	if maxInFlight > len(groups) {
		maxInFlight = len(groups)
	}
	jobs := make(chan int)
	var workers sync.WaitGroup
	workers.Add(maxInFlight)
	for worker := 0; worker < maxInFlight; worker++ {
		go func() {
			defer workers.Done()
			for idx := range jobs {
				result.Targets[idx] = replicator.executeReplicationTaskGroup(ctx, groups[idx])
			}
		}()
	}
	for idx := range groups {
		jobs <- idx
	}
	close(jobs)
	workers.Wait()
	return result
}

func (replicator *HTTPReplicator) executeReplicationTaskGroup(ctx context.Context, group replicationTaskGroup) ReplicationTargetResult {
	if replicator.transport == ReplicationTransportGRPCStream && replicator.grpcLiveSession != nil {
		streamGroup, err := replicator.liveReplicationGRPCGroup(group)
		if err == nil {
			return replicator.grpcLiveSession.executeReplicationTaskGroup(ctx, streamGroup)
		}
		if replicator.disableHTTPFallback {
			return ReplicationTargetResult{Node: group.target.ID, Address: group.target.GRPCAddress, Error: err.Error()}
		}
	}
	return replicator.executeReplicationTaskGroupHTTP(ctx, group)
}

func (replicator *HTTPReplicator) executeReplicationTaskGroupHTTP(ctx context.Context, group replicationTaskGroup) ReplicationTargetResult {
	targetResult := replicator.executeReplicationTargetGroup(ctx, group)
	if payloads := group.replicationSyncPayloadBatch(); payloads.len() == 1 {
		targetResult.Key = payloads.payload(0).key
	} else if len(group.keys) == 1 {
		targetResult.Key = group.keys[0]
	}
	return targetResult
}

func (replicator *HTTPReplicator) liveReplicationGRPCGroup(group replicationTaskGroup) (replicationTaskGroup, error) {
	if group.replicationSyncPayloadBatch().len() > 0 {
		return group, nil
	}
	if len(group.payloads) == 0 {
		return replicationTaskGroup{}, errors.New("hatriecache: empty live replication group")
	}
	streamPayloads := make([]replicationSyncPayload, 0, len(group.payloads))
	for _, payload := range group.payloads {
		key := strings.TrimSpace(payload.Key)
		switch normalizedCommand(payload.Command) {
		case replicationSetCompactCommand:
			if key == "" || len(payload.BinaryValue) == 0 {
				return replicationTaskGroup{}, errors.New("hatriecache: compact live replication value is empty")
			}
			streamPayloads = append(streamPayloads, replicationSyncPayload{key: key, binaryValue: payload.BinaryValue})
		case "INTERNALDEL":
			if key == "" {
				return replicationTaskGroup{}, errors.New("hatriecache: live replication delete key is empty")
			}
			streamPayloads = append(streamPayloads, replicationSyncPayload{key: key})
		default:
			return replicationTaskGroup{}, fmt.Errorf("hatriecache: command %s is not supported by live gRPC replication", payload.Command)
		}
	}
	fingerprint := ""
	if replicator.topology != nil {
		fingerprint = replicator.topology.Fingerprint()
	}
	group.syncPayloads = streamPayloads
	group.payloads = nil
	group.keys = nil
	group.payloadBytes = nil
	group.deferredMetadata = true
	group.metadataSource = replicator.self
	group.metadataTopology = fingerprint
	return group, nil
}

func (replicator *HTTPReplicator) executeReplicationTargetGroup(ctx context.Context, group replicationTaskGroup) ReplicationTargetResult {
	if payloads := group.replicationSyncPayloadBatch(); payloads.len() > 0 {
		return replicator.executeDeferredReplicationSyncTargetBatchSource(ctx, group.target, payloads, group.metadataSource, group.metadataTopology)
	}
	if group.deferredMetadata {
		return replicator.executeDeferredReplicationTargetBatch(ctx, group.target, group.payloads, group.metadataSource, group.metadataTopology)
	}
	return replicator.executeReplicationTargetBatch(ctx, group.target, group.payloads)
}

func groupReplicationTasksByTarget(tasks []replicationTask) []replicationTaskGroup {
	if len(tasks) == 0 {
		return nil
	}
	if len(tasks) == 1 {
		task := tasks[0]
		return []replicationTaskGroup{{
			target:       task.target,
			payloads:     []CacheCommandRequest{task.payload},
			keys:         []string{strings.TrimSpace(task.payload.Key)},
			payloadBytes: []int{task.payloadBytes},
		}}
	}
	if len(tasks) <= replicationLinearGroupTaskLimit {
		if replicationTasksHaveAtMostTargets(tasks, replicationLinearGroupTargetLimit) {
			groups, _ := groupReplicationTasksByTargetLinear(tasks, 0)
			return groups
		}
	}
	return groupReplicationTasksByTargetMap(tasks)
}

func replicationTasksHaveAtMostTargets(tasks []replicationTask, maxTargets int) bool {
	if maxTargets <= 0 {
		return false
	}
	var targets [replicationLinearGroupTargetLimit]TopologyNode
	if maxTargets > len(targets) {
		maxTargets = len(targets)
	}
	count := 0
	for _, task := range tasks {
		found := false
		for idx := 0; idx < count; idx++ {
			if replicationTargetsEqual(targets[idx], task.target) {
				found = true
				break
			}
		}
		if found {
			continue
		}
		if count >= maxTargets {
			return false
		}
		targets[count] = task.target
		count++
	}
	return true
}

func groupReplicationTasksByTargetMap(tasks []replicationTask) []replicationTaskGroup {
	groups := make([]replicationTaskGroup, 0, len(tasks))
	indexes := make(map[string]int, len(tasks))
	for _, task := range tasks {
		key := replicationTaskTargetKey(task.target)
		if idx, ok := indexes[key]; ok {
			groups[idx].payloads = append(groups[idx].payloads, task.payload)
			groups[idx].keys = append(groups[idx].keys, strings.TrimSpace(task.payload.Key))
			groups[idx].payloadBytes = append(groups[idx].payloadBytes, task.payloadBytes)
			continue
		}
		indexes[key] = len(groups)
		groups = append(groups, replicationTaskGroup{
			target:       task.target,
			payloads:     []CacheCommandRequest{task.payload},
			keys:         []string{strings.TrimSpace(task.payload.Key)},
			payloadBytes: []int{task.payloadBytes},
		})
	}
	return groups
}

func groupReplicationTasksByTargetLinear(tasks []replicationTask, maxTargets int) ([]replicationTaskGroup, bool) {
	groups := make([]replicationTaskGroup, 0, len(tasks))
	for _, task := range tasks {
		found := -1
		for idx := range groups {
			if replicationTargetsEqual(groups[idx].target, task.target) {
				found = idx
				break
			}
		}
		if found >= 0 {
			groups[found].payloads = append(groups[found].payloads, task.payload)
			groups[found].keys = append(groups[found].keys, strings.TrimSpace(task.payload.Key))
			groups[found].payloadBytes = append(groups[found].payloadBytes, task.payloadBytes)
			continue
		}
		if maxTargets > 0 && len(groups) >= maxTargets {
			return nil, false
		}
		groups = append(groups, replicationTaskGroup{
			target:       task.target,
			payloads:     []CacheCommandRequest{task.payload},
			keys:         []string{strings.TrimSpace(task.payload.Key)},
			payloadBytes: []int{task.payloadBytes},
		})
	}
	return groups, true
}

func replicationTargetsEqual(left TopologyNode, right TopologyNode) bool {
	if left.ID != "" || right.ID != "" {
		return left.ID == right.ID
	}
	return left.Address == right.Address
}

func (replicator *HTTPReplicator) groupReplicationTasksByTarget(tasks []replicationTask) []replicationTaskGroup {
	groups := groupReplicationTasksByTarget(tasks)
	if replicator == nil || replicator.batchMaxBytes <= 0 {
		return groups
	}
	return splitReplicationTaskGroupsByMaxBytes(groups, replicator.batchMaxBytes)
}

func splitReplicationTaskGroupsByMaxBytes(groups []replicationTaskGroup, maxBytes int) []replicationTaskGroup {
	if maxBytes <= 0 || len(groups) == 0 {
		return groups
	}
	out := make([]replicationTaskGroup, 0, len(groups))
	for _, group := range groups {
		out = append(out, splitReplicationTaskGroupByMaxBytes(group, maxBytes)...)
	}
	return out
}

func splitReplicationTaskGroupByMaxBytes(group replicationTaskGroup, maxBytes int) []replicationTaskGroup {
	if group.replicationSyncPayloadBatch().len() > 0 {
		return splitReplicationSyncTaskGroupByMaxBytes(group, maxBytes)
	}
	if maxBytes <= 0 || len(group.payloads) <= 1 {
		return []replicationTaskGroup{group}
	}
	threshold := maxBytes + 1
	current := replicationTaskGroup{
		target:           group.target,
		deferredMetadata: group.deferredMetadata,
		metadataSource:   group.metadataSource,
		metadataTopology: group.metadataTopology,
	}
	currentBytes := estimatedReplicationBatchEnvelopeBytes(threshold)
	out := make([]replicationTaskGroup, 0, len(group.payloads))
	for idx, payload := range group.payloads {
		payloadBytes := replicationTaskPayloadBytes(group, idx, payload, threshold)
		if len(current.payloads) > 0 && currentBytes+payloadBytes+2 > maxBytes {
			out = append(out, current)
			current = replicationTaskGroup{
				target:           group.target,
				deferredMetadata: group.deferredMetadata,
				metadataSource:   group.metadataSource,
				metadataTopology: group.metadataTopology,
			}
			currentBytes = estimatedReplicationBatchEnvelopeBytes(threshold)
		}
		current.payloads = append(current.payloads, payload)
		key := strings.TrimSpace(payload.Key)
		if idx < len(group.keys) {
			key = group.keys[idx]
		}
		current.keys = append(current.keys, key)
		current.payloadBytes = append(current.payloadBytes, payloadBytes)
		currentBytes = jsonwire.AddEstimate(currentBytes, payloadBytes+2, threshold)
	}
	if len(current.payloads) > 0 {
		out = append(out, current)
	}
	return out
}

func splitReplicationSyncTaskGroupByMaxBytes(group replicationTaskGroup, maxBytes int) []replicationTaskGroup {
	payloads := group.replicationSyncPayloadBatch()
	if maxBytes <= 0 || payloads.len() <= 1 {
		return []replicationTaskGroup{group}
	}
	threshold := maxBytes + 1
	newGroup := func() replicationTaskGroup {
		return replicationTaskGroup{
			target:           group.target,
			syncPayloadArena: group.syncPayloadArena,
			deferredMetadata: group.deferredMetadata,
			metadataSource:   group.metadataSource,
			metadataTopology: group.metadataTopology,
		}
	}
	current := newGroup()
	currentBytes := estimatedReplicationBatchEnvelopeBytes(threshold)
	out := make([]replicationTaskGroup, 0, payloads.len())
	for idx := 0; idx < payloads.len(); idx++ {
		payload := payloads.payload(idx)
		payloadBytes := payload.payloadBytes
		if payloadBytes <= 0 {
			payloadBytes = estimatedReplicationRequestBytesWithin(CacheCommandRequest{
				Command: replicationSetCompactCommand, Key: payload.key, BinaryValue: payload.binaryValue,
			}, threshold)
		}
		if current.replicationSyncPayloadBatch().len() > 0 && currentBytes+payloadBytes+2 > maxBytes {
			out = append(out, current)
			current = newGroup()
			currentBytes = estimatedReplicationBatchEnvelopeBytes(threshold)
		}
		if group.syncPayloadArena != nil {
			current.syncPayloadIndexes = append(current.syncPayloadIndexes, group.syncPayloadIndexes[idx])
		} else {
			payload.payloadBytes = payloadBytes
			current.syncPayloads = append(current.syncPayloads, payload)
		}
		currentBytes = jsonwire.AddEstimate(currentBytes, payloadBytes+2, threshold)
	}
	if current.replicationSyncPayloadBatch().len() > 0 {
		out = append(out, current)
	}
	return out
}

func replicationPayloadEstimateThreshold(maxBytes int) int {
	if maxBytes > 0 {
		return maxBytes + 1
	}
	return minCompressedReplicationRequestBytes
}

func replicationTaskPayloadBytes(group replicationTaskGroup, idx int, payload CacheCommandRequest, threshold int) int {
	if idx < len(group.payloadBytes) && group.payloadBytes[idx] > 0 {
		return group.payloadBytes[idx]
	}
	return estimatedReplicationRequestBytesWithin(payload, threshold)
}

func estimatedReplicationBatchEnvelopeBytes(threshold int) int {
	return 64 + jsonwire.EstimateJSONStringBytes("INTERNALBATCH")
}

func replicationTaskTargetKey(target TopologyNode) string {
	if target.ID != "" {
		return "id:" + target.ID
	}
	return "addr:" + target.Address
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
		deadSeq := uint64(0)
		var deadLetters []ReplicationDeadLetter
		deadLettersUpdated := false
		if needsRetry && attempt == attempts {
			deadSeq, deadLetters, deadLettersUpdated = replicator.recordDeadLetter(result, attempt)
		}
		result = replicator.attachReplicationHealth(result)
		replicator.storeLastResult(result)
		if !needsRetry || attempt == attempts {
			replicator.completeAsyncJob(job.id, deadSeq, deadLetters, deadLettersUpdated)
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
	startedAt := time.Now()
	timer := time.NewTimer(retry)
	defer stopReplicationRetryTimer(timer)
	select {
	case <-ctx.Done():
		return false
	case <-replicator.done:
		return false
	case <-timer.C:
		replicator.recordReplicationRetryDelay(time.Since(startedAt))
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

func (replicator *HTTPReplicator) plannedReplicationTargets(tasks []replicationTask) []ReplicationTargetResult {
	groups := replicator.groupReplicationTasksByTarget(tasks)
	out := make([]ReplicationTargetResult, len(groups))
	for idx, group := range groups {
		out[idx] = ReplicationTargetResult{
			Node:    group.target.ID,
			Address: group.target.Address,
		}
		if len(group.keys) == 1 {
			out[idx].Key = group.keys[0]
		}
	}
	return out
}

func (replicator *HTTPReplicator) syncAll(ctx context.Context, trie *HatTrie, prefix string) ReplicationResult {
	return replicator.syncAllPaged(ctx, trie, prefix, defaultReplicationSyncKeyPageSize)
}

func (replicator *HTTPReplicator) annotateReplicationPayload(payload CacheCommandRequest) CacheCommandRequest {
	fingerprint := ""
	if replicator != nil && replicator.topology != nil {
		fingerprint = replicator.topology.Fingerprint()
	}
	return replicator.annotateReplicationPayloadWithFingerprint(payload, fingerprint)
}

func (replicator *HTTPReplicator) annotateReplicationPayloadWithFingerprint(payload CacheCommandRequest, fingerprint string) CacheCommandRequest {
	if replicator == nil {
		return payload
	}
	return replicator.annotateReplicationPayloadWithMetadata(payload, replicator.self, fingerprint)
}

func (replicator *HTTPReplicator) annotateReplicationPayloadWithMetadata(payload CacheCommandRequest, source string, fingerprint string) CacheCommandRequest {
	if replicator == nil {
		return payload
	}
	if payload.Pairs == nil {
		payload.Pairs = Map{}
	}
	if source != "" {
		payload.Pairs[replicationMetaSourceNode] = source
	}
	payload.Pairs[replicationMetaSequence] = strconv.FormatUint(replicator.nextReplicationSequence(), 10)
	if fingerprint != "" {
		payload.Pairs[replicationMetaTopologyFingerprint] = fingerprint
	}
	return payload
}

func (replicator *HTTPReplicator) nextReplicationSequence() uint64 {
	replicator.mu.Lock()
	defer replicator.mu.Unlock()
	replicator.sequence++
	return replicator.sequence
}

type replicationSyncPage struct {
	scanned      int
	nextAfterKey string
	hasMore      bool
}

type replicationSyncCursor struct {
	scan     *hatTrieScanCursor
	prefix   string
	visited  int
	restarts int
}

func (cursor *replicationSyncCursor) close(trie *HatTrie) {
	if cursor == nil || trie == nil {
		return
	}
	trie.mu.Lock()
	defer trie.mu.Unlock()
	cursor.closeLocked(trie)
}

func (cursor *replicationSyncCursor) closeLocked(trie *HatTrie) {
	if cursor == nil || cursor.scan == nil {
		return
	}
	cursor.scan.closeLocked(trie)
	cursor.scan = nil
}

func replicationSyncEntriesPage(trie *HatTrie, prefix string, afterKey string, hasAfterKey bool, limit int, visit func(Entry) error) (replicationSyncPage, error) {
	cursor := &replicationSyncCursor{}
	defer cursor.close(trie)
	return replicationSyncEntriesPageWithCursor(trie, prefix, afterKey, hasAfterKey, limit, cursor, visit)
}

func replicationSyncEntriesPageWithCursor(trie *HatTrie, prefix string, afterKey string, hasAfterKey bool, limit int, cursor *replicationSyncCursor, visit func(Entry) error) (replicationSyncPage, error) {
	if limit <= 0 {
		limit = 1
	}
	if cursor == nil {
		return replicationSyncPage{}, errors.New("hatriecache: replication sync cursor is nil")
	}

	trie.mu.Lock()
	defer trie.mu.Unlock()
	if cursor.scan == nil || cursor.prefix != prefix || cursor.scan.generation != trie.mutationEpoch {
		restarting := cursor.scan != nil
		cursor.closeLocked(trie)
		scan, err := trie.newScanCursorLocked(prefix, true)
		if err != nil {
			return replicationSyncPage{}, err
		}
		cursor.scan = scan
		cursor.prefix = prefix
		if restarting {
			cursor.restarts++
		}
	}

	now := time.Time{}
	if len(trie.expires) > 0 {
		now = trie.currentTime()
	}

	page := replicationSyncPage{}
	visitedBefore := cursor.scan.visited
	defer func() {
		if cursor.scan != nil {
			cursor.visited += cursor.scan.visited - visitedBefore
		}
	}()
	for {
		entry, ok := cursor.scan.currentLiveEntryLocked(trie, now)
		if !ok {
			cursor.visited += cursor.scan.visited - visitedBefore
			visitedBefore = cursor.scan.visited
			cursor.closeLocked(trie)
			return page, nil
		}
		if hasAfterKey && entry.Key <= afterKey {
			cursor.scan.consume()
			continue
		}
		if page.scanned >= limit {
			page.hasMore = true
			return page, nil
		}
		page.scanned++
		page.nextAfterKey = entry.Key
		if visit != nil {
			if err := visit(entry); err != nil {
				cursor.visited += cursor.scan.visited - visitedBefore
				visitedBefore = cursor.scan.visited
				cursor.closeLocked(trie)
				return replicationSyncPage{}, err
			}
		}
		cursor.scan.consume()
	}
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

func (replicator *HTTPReplicator) snapshotReplicationRouting() (replicationRoutingSnapshot, bool) {
	if replicator == nil || replicator.topology == nil {
		return replicationRoutingSnapshot{}, false
	}
	return newReplicationRoutingSnapshot(replicator.self, replicator.topology, replicator.election)
}

func newReplicationRoutingSnapshot(self string, topologyStore *TopologyStore, election *ElectionStore) (replicationRoutingSnapshot, bool) {
	if topologyStore == nil {
		return replicationRoutingSnapshot{}, false
	}
	topology := topologyStore.Get()
	snapshot := replicationRoutingSnapshot{
		topology:    topology,
		nodes:       topologyNodesByID(topology),
		leaders:     make(map[uint32]ElectionLeader, len(topology.Shards)),
		owners:      make(map[uint32][]string, len(topology.Shards)),
		targets:     make(map[uint32][]TopologyNode, len(topology.Shards)),
		self:        self,
		fingerprint: topology.Fingerprint(),
	}
	if election != nil {
		snapshot.online = election.activeNodesSnapshot(topology)
	}
	if topologyMode(topology.Mode) == TopologyModeFullReplica {
		shard, ok := topology.fullReplicaShard()
		if !ok {
			return replicationRoutingSnapshot{}, false
		}
		snapshot.shards = []TopologyShard{shard}
	} else {
		snapshot.shards = topology.Shards
	}
	if len(snapshot.shards) == 0 {
		return replicationRoutingSnapshot{}, false
	}
	for _, shard := range snapshot.shards {
		owners := routeOwners(shard)
		snapshot.owners[shard.ID] = owners
		snapshot.targets[shard.ID] = precomputedReplicationTargets(owners, snapshot.nodes, snapshot.online, snapshot.self)
		if election != nil {
			snapshot.leaders[shard.ID] = electShardLeader(shard, snapshot.online)
			continue
		}
		snapshot.leaders[shard.ID] = ElectionLeader{
			Shard:      shard.ID,
			Leader:     shard.Primary,
			Available:  true,
			Primary:    shard.Primary,
			Candidates: owners,
		}
	}
	return snapshot, true
}

func (snapshot replicationRoutingSnapshot) routeForKey(key string) (ElectionKeyRoute, bool) {
	if len(snapshot.shards) == 0 {
		return ElectionKeyRoute{}, false
	}
	mode := topologyMode(snapshot.topology.Mode)
	shard := snapshot.shards[0]
	var bucket *uint32
	if mode != TopologyModeFullReplica {
		if snapshot.topology.BucketCount > 0 {
			value := hashKeyToBucket(key, snapshot.topology.BucketCount)
			selected, ok := snapshot.topology.shardForBucket(value, snapshot.shards)
			if !ok {
				return ElectionKeyRoute{}, false
			}
			shard = selected
			bucket = &value
		} else {
			shard = snapshot.shards[hashKeyToShardIndex(key, len(snapshot.shards))]
		}
	}
	owners := snapshot.owners[shard.ID]
	route := TopologyRoute{Key: key, Mode: mode, Bucket: bucket, Shard: shard, Owners: owners}
	return ElectionKeyRoute{Key: key, Route: route, Leader: snapshot.leaders[shard.ID]}, true
}

func (snapshot replicationRoutingSnapshot) replicationTargets(route ElectionKeyRoute, self string) []TopologyNode {
	if self == snapshot.self {
		if targets, ok := snapshot.targets[route.Route.Shard.ID]; ok {
			return targets
		}
	}
	owners := route.Route.Owners
	if len(owners) == 0 {
		owners = snapshot.owners[route.Route.Shard.ID]
	}
	targets := make([]TopologyNode, 0, len(owners))
	for _, nodeID := range owners {
		if nodeID == "" || nodeID == self {
			continue
		}
		if snapshot.online != nil && !snapshot.online[nodeID] {
			continue
		}
		node, ok := snapshot.nodes[nodeID]
		if ok {
			targets = append(targets, node)
		}
	}
	sort.Slice(targets, func(i, j int) bool {
		return targets[i].ID < targets[j].ID
	})
	return targets
}

func precomputedReplicationTargets(owners []string, nodes map[string]TopologyNode, online map[string]bool, self string) []TopologyNode {
	targets := make([]TopologyNode, 0, len(owners))
	seen := make(map[string]struct{}, len(owners))
	for _, nodeID := range owners {
		nodeID = strings.TrimSpace(nodeID)
		if nodeID == "" || nodeID == self {
			continue
		}
		if _, ok := seen[nodeID]; ok {
			continue
		}
		if online != nil && !online[nodeID] {
			continue
		}
		node, ok := nodes[nodeID]
		if !ok {
			continue
		}
		seen[nodeID] = struct{}{}
		targets = append(targets, node)
	}
	sort.Slice(targets, func(i, j int) bool {
		return targets[i].ID < targets[j].ID
	})
	return targets
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
		startedAt := time.Now()
		result := replicator.postReplicationCommand(ctx, target, payload)
		replicator.recordReplicationTargetLatency(target, time.Since(startedAt))
		return replicator.afterReplicationTarget(target, state, result)
	}
}

func (replicator *HTTPReplicator) executeReplicationSyncTarget(ctx context.Context, target TopologyNode, payloads []replicationSyncPayload, command string, source string, sequence uint64, fingerprint string) ReplicationTargetResult {
	return replicator.executeReplicationSyncTargetBatch(ctx, target, replicationSyncPayloadBatch{inline: payloads}, command, source, sequence, fingerprint)
}

func (replicator *HTTPReplicator) executeReplicationSyncTargetBatch(ctx context.Context, target TopologyNode, payloads replicationSyncPayloadBatch, command string, source string, sequence uint64, fingerprint string) ReplicationTargetResult {
	if result, allowed, state := replicator.beforeReplicationTarget(target); !allowed {
		return result
	} else {
		startedAt := time.Now()
		result := replicator.postReplicationCommandWithBody(
			ctx,
			target,
			func(message string) bool {
				return replicationResponseRejectsTypedCommand(replicationBatchEnvelopeCommand, true, message)
			},
			func() (io.Reader, string, string, error) {
				return replicationSyncBatchRequestBodyBatch(payloads, command, source, sequence, fingerprint, minCompressedReplicationRequestBytes)
			},
		)
		replicator.recordReplicationTargetLatency(target, time.Since(startedAt))
		return replicator.afterReplicationTarget(target, state, result)
	}
}

func (replicator *HTTPReplicator) executeReplicationTargetBatch(ctx context.Context, target TopologyNode, payloads []CacheCommandRequest) ReplicationTargetResult {
	replicator.recordReplicationBatchSize(target, len(payloads))
	if len(payloads) == 1 {
		payload := payloads[0]
		if replicator.wireFormat == CommandWireFormatJSON {
			legacy, err := replicationLegacyPayload(payload)
			if err != nil {
				return ReplicationTargetResult{Node: target.ID, Address: target.Address, Error: err.Error()}
			}
			return replicator.executeReplicationTarget(ctx, target, legacy)
		}
		result := replicator.executeReplicationTarget(ctx, target, payload)
		if !result.unsupportedTypedReplication {
			return result
		}
		if normalizedCommand(payload.Command) == replicationSetCompactCommand {
			v2, err := replicationBinaryV2Payload(payload)
			if err != nil {
				return ReplicationTargetResult{Node: target.ID, Address: target.Address, Error: err.Error()}
			}
			result = replicator.executeReplicationTarget(ctx, target, v2)
			if !result.unsupportedTypedReplication {
				return result
			}
			payload = v2
		}
		legacy, err := replicationLegacyPayload(payload)
		if err != nil {
			return ReplicationTargetResult{Node: target.ID, Address: target.Address, Error: err.Error()}
		}
		return replicator.executeReplicationTarget(ctx, target, legacy)
	}
	if replicator.wireFormat == CommandWireFormatJSON {
		legacyPayload, err := replicationBatchPayload(payloads)
		if err != nil {
			return ReplicationTargetResult{Node: target.ID, Address: target.Address, Error: err.Error()}
		}
		return replicator.executeReplicationTarget(ctx, target, legacyPayload)
	}
	payload, err := replicationBatchEnvelopePayload(payloads)
	if err != nil {
		return ReplicationTargetResult{
			Node:    target.ID,
			Address: target.Address,
			Error:   err.Error(),
		}
	}
	result := replicator.executeReplicationTarget(ctx, target, payload)
	if !result.unsupportedTypedReplication {
		return result
	}
	if replicationPayloadsContainCompact(payloads) {
		payloads, err = replicationBinaryV2Payloads(payloads)
		if err != nil {
			return ReplicationTargetResult{Node: target.ID, Address: target.Address, Error: err.Error()}
		}
		payload, err = replicationBatchEnvelopePayload(payloads)
		if err != nil {
			return ReplicationTargetResult{Node: target.ID, Address: target.Address, Error: err.Error()}
		}
		result = replicator.executeReplicationTarget(ctx, target, payload)
		if !result.unsupportedTypedReplication {
			return result
		}
	}
	legacyPayload, err := replicationBatchPayload(payloads)
	if err != nil {
		return ReplicationTargetResult{
			Node:    target.ID,
			Address: target.Address,
			Error:   err.Error(),
		}
	}
	return replicator.executeReplicationTarget(ctx, target, legacyPayload)
}

func (replicator *HTTPReplicator) executeDeferredReplicationTargetBatch(ctx context.Context, target TopologyNode, payloads []CacheCommandRequest, source string, fingerprint string) ReplicationTargetResult {
	replicator.recordReplicationBatchSize(target, len(payloads))
	if len(payloads) == 0 {
		return ReplicationTargetResult{Node: target.ID, Address: target.Address, Error: "hatriecache: empty replication batch"}
	}
	if len(payloads) == 1 {
		payload := replicator.annotateReplicationPayloadWithMetadata(payloads[0], source, fingerprint)
		if replicator.wireFormat == CommandWireFormatJSON {
			legacy, err := replicationLegacyPayload(payload)
			if err != nil {
				return ReplicationTargetResult{Node: target.ID, Address: target.Address, Error: err.Error()}
			}
			return replicator.executeReplicationTarget(ctx, target, legacy)
		}
		result := replicator.executeReplicationTarget(ctx, target, payload)
		if !result.unsupportedTypedReplication {
			return result
		}
		if normalizedCommand(payload.Command) == replicationSetCompactCommand {
			v2, err := replicationBinaryV2Payload(payload)
			if err != nil {
				return ReplicationTargetResult{Node: target.ID, Address: target.Address, Error: err.Error()}
			}
			result = replicator.executeReplicationTarget(ctx, target, v2)
			if !result.unsupportedTypedReplication {
				return result
			}
			payload = v2
		}
		legacy, err := replicationLegacyPayload(payload)
		if err != nil {
			return ReplicationTargetResult{Node: target.ID, Address: target.Address, Error: err.Error()}
		}
		return replicator.executeReplicationTarget(ctx, target, legacy)
	}

	if replicator.wireFormat == CommandWireFormatJSON {
		legacyPayload, err := replicator.deferredReplicationLegacyBatchPayload(payloads, source, fingerprint)
		if err != nil {
			return ReplicationTargetResult{Node: target.ID, Address: target.Address, Error: err.Error()}
		}
		return replicator.executeReplicationTarget(ctx, target, legacyPayload)
	}

	sequence := replicator.nextReplicationSequence()
	payload := replicationBatchEnvelopePayloadWithMetadata(payloads, source, sequence, fingerprint)
	result := replicator.executeReplicationTarget(ctx, target, payload)
	if !result.unsupportedTypedReplication {
		return result
	}
	if replicationPayloadsContainCompact(payloads) {
		v2Payloads, err := replicationBinaryV2Payloads(payloads)
		if err != nil {
			return ReplicationTargetResult{Node: target.ID, Address: target.Address, Error: err.Error()}
		}
		result = replicator.executeReplicationTarget(ctx, target, replicationBatchEnvelopePayloadWithMetadata(v2Payloads, source, sequence, fingerprint))
		if !result.unsupportedTypedReplication {
			return result
		}
		payloads = v2Payloads
	}
	legacyPayload, err := replicator.deferredReplicationLegacyBatchPayload(payloads, source, fingerprint)
	if err != nil {
		return ReplicationTargetResult{Node: target.ID, Address: target.Address, Error: err.Error()}
	}
	return replicator.executeReplicationTarget(ctx, target, legacyPayload)
}

func (replicator *HTTPReplicator) executeDeferredReplicationSyncTargetBatch(ctx context.Context, target TopologyNode, payloads []replicationSyncPayload, source string, fingerprint string) ReplicationTargetResult {
	return replicator.executeDeferredReplicationSyncTargetBatchSource(ctx, target, replicationSyncPayloadBatch{inline: payloads}, source, fingerprint)
}

func (replicator *HTTPReplicator) executeDeferredReplicationSyncTargetBatchSource(ctx context.Context, target TopologyNode, payloads replicationSyncPayloadBatch, source string, fingerprint string) ReplicationTargetResult {
	if payloads.len() <= 1 {
		return replicator.executeDeferredReplicationTargetBatch(ctx, target, replicationSyncPayloadBatchToCommandRequests(payloads, replicationSetCompactCommand), source, fingerprint)
	}
	replicator.recordReplicationBatchSize(target, payloads.len())
	if replicator.wireFormat == CommandWireFormatJSON {
		legacyPayload, err := replicator.deferredReplicationLegacyBatchPayload(replicationSyncPayloadBatchToCommandRequests(payloads, replicationSetCompactCommand), source, fingerprint)
		if err != nil {
			return ReplicationTargetResult{Node: target.ID, Address: target.Address, Error: err.Error()}
		}
		return replicator.executeReplicationTarget(ctx, target, legacyPayload)
	}

	sequence := replicator.nextReplicationSequence()
	result := replicator.executeReplicationSyncTargetBatch(ctx, target, payloads, replicationSetCompactCommand, source, sequence, fingerprint)
	if !result.unsupportedTypedReplication {
		return result
	}
	v2Payloads, err := replicationSyncPayloadBatchV2(payloads)
	if err != nil {
		return ReplicationTargetResult{Node: target.ID, Address: target.Address, Error: err.Error()}
	}
	result = replicator.executeReplicationSyncTarget(ctx, target, v2Payloads, replicationSetBinaryCommand, source, sequence, fingerprint)
	if !result.unsupportedTypedReplication {
		return result
	}
	legacyPayload, err := replicator.deferredReplicationLegacyBatchPayload(replicationSyncPayloadsToCommandRequests(v2Payloads, replicationSetBinaryCommand), source, fingerprint)
	if err != nil {
		return ReplicationTargetResult{Node: target.ID, Address: target.Address, Error: err.Error()}
	}
	return replicator.executeReplicationTarget(ctx, target, legacyPayload)
}

func replicationSyncPayloadsToCommandRequests(payloads []replicationSyncPayload, command string) []CacheCommandRequest {
	return replicationSyncPayloadBatchToCommandRequests(replicationSyncPayloadBatch{inline: payloads}, command)
}

func replicationSyncPayloadBatchToCommandRequests(payloads replicationSyncPayloadBatch, command string) []CacheCommandRequest {
	requests := make([]CacheCommandRequest, payloads.len())
	for idx := 0; idx < payloads.len(); idx++ {
		payload := payloads.payload(idx)
		requests[idx] = CacheCommandRequest{Command: command, Key: payload.key, BinaryValue: payload.binaryValue}
	}
	return requests
}

func replicationSyncPayloadsV2(payloads []replicationSyncPayload) ([]replicationSyncPayload, error) {
	return replicationSyncPayloadBatchV2(replicationSyncPayloadBatch{inline: payloads})
}

func replicationSyncPayloadBatchV2(payloads replicationSyncPayloadBatch) ([]replicationSyncPayload, error) {
	v2 := make([]replicationSyncPayload, payloads.len())
	for idx := 0; idx < payloads.len(); idx++ {
		payload := payloads.payload(idx)
		converted, err := replicationBinaryV2Payload(CacheCommandRequest{
			Command: replicationSetCompactCommand, Key: payload.key, BinaryValue: payload.binaryValue,
		})
		if err != nil {
			return nil, fmt.Errorf("replication V2 payload %d: %w", idx, err)
		}
		v2[idx] = replicationSyncPayload{key: converted.Key, binaryValue: converted.BinaryValue}
	}
	return v2, nil
}

func (replicator *HTTPReplicator) deferredReplicationLegacyBatchPayload(payloads []CacheCommandRequest, source string, fingerprint string) (CacheCommandRequest, error) {
	annotated := make([]CacheCommandRequest, len(payloads))
	for idx, payload := range payloads {
		annotated[idx] = replicator.annotateReplicationPayloadWithMetadata(payload, source, fingerprint)
	}
	return replicationBatchPayload(annotated)
}

func replicationBatchPayload(payloads []CacheCommandRequest) (CacheCommandRequest, error) {
	batch := make([]CacheCommandRequest, len(payloads))
	for idx, payload := range payloads {
		legacy, err := replicationLegacyPayload(payload)
		if err != nil {
			return CacheCommandRequest{}, fmt.Errorf("replication batch payload %d: %w", idx, err)
		}
		batch[idx] = legacy
	}
	return CacheCommandRequest{
		Command: "INTERNALBATCH",
		Batch:   batch,
	}, nil
}

func replicationLegacyPayload(payload CacheCommandRequest) (CacheCommandRequest, error) {
	payload, err := replicationBinaryV2Payload(payload)
	if err != nil {
		return CacheCommandRequest{}, err
	}
	if normalizedCommand(payload.Command) != replicationSetBinaryCommand {
		return payload, nil
	}
	operation, err := commandSnapshotBinaryOperation(strings.TrimSpace(payload.Key), payload.BinaryValue)
	if err != nil {
		return CacheCommandRequest{}, err
	}
	data, err := marshalSnapshotEntryJSON(operation.entry)
	if err != nil {
		return CacheCommandRequest{}, err
	}
	payload.Command = "INTERNALSET"
	payload.Value = string(data)
	payload.BinaryValue = nil
	return payload, nil
}

func replicationBinaryV2Payload(payload CacheCommandRequest) (CacheCommandRequest, error) {
	if normalizedCommand(payload.Command) != replicationSetCompactCommand {
		return payload, nil
	}
	entry, err := unmarshalReplicationValueBinary(strings.TrimSpace(payload.Key), payload.BinaryValue)
	if err != nil {
		return CacheCommandRequest{}, err
	}
	data, err := marshalLevelDBEntry(entry, StorageFormatBinary)
	if err != nil {
		return CacheCommandRequest{}, err
	}
	payload.Command = replicationSetBinaryCommand
	payload.BinaryValue = data
	return payload, nil
}

func replicationPayloadsContainCompact(payloads []CacheCommandRequest) bool {
	for _, payload := range payloads {
		if normalizedCommand(payload.Command) == replicationSetCompactCommand {
			return true
		}
	}
	return false
}

func replicationBinaryV2Payloads(payloads []CacheCommandRequest) ([]CacheCommandRequest, error) {
	v2 := make([]CacheCommandRequest, len(payloads))
	for idx, payload := range payloads {
		converted, err := replicationBinaryV2Payload(payload)
		if err != nil {
			return nil, fmt.Errorf("replication V2 payload %d: %w", idx, err)
		}
		v2[idx] = converted
	}
	return v2, nil
}

func replicationBatchEnvelopePayload(payloads []CacheCommandRequest) (CacheCommandRequest, error) {
	batch := make([]CacheCommandRequest, len(payloads))
	var source string
	var fingerprint string
	var sequence uint64
	for idx, payload := range payloads {
		payloadSource, payloadSequence, payloadFingerprint := replicationSafetyMetadata(payload)
		if idx == 0 {
			source = payloadSource
			fingerprint = payloadFingerprint
		} else if payloadSource != source || payloadFingerprint != fingerprint {
			return CacheCommandRequest{}, errors.New("hatriecache: replication batch metadata mismatch")
		}
		if payloadSequence > sequence {
			sequence = payloadSequence
		}
		payload.Pairs = commandPairsWithoutReplicationMetadata(payload.Pairs)
		batch[idx] = payload
	}
	pairs := Map{}
	if source != "" {
		pairs[replicationMetaSourceNode] = source
	}
	if sequence > 0 {
		pairs[replicationMetaSequence] = strconv.FormatUint(sequence, 10)
	}
	if fingerprint != "" {
		pairs[replicationMetaTopologyFingerprint] = fingerprint
	}
	if len(pairs) == 0 {
		pairs = nil
	}
	return CacheCommandRequest{
		Command: replicationBatchEnvelopeCommand,
		Pairs:   pairs,
		Batch:   batch,
	}, nil
}

func replicationBatchEnvelopePayloadWithMetadata(payloads []CacheCommandRequest, source string, sequence uint64, fingerprint string) CacheCommandRequest {
	pairs := replicationMetadataPairs(source, sequence, fingerprint)
	return CacheCommandRequest{
		Command: replicationBatchEnvelopeCommand,
		Pairs:   pairs,
		Batch:   payloads,
	}
}

func replicationMetadataPairs(source string, sequence uint64, fingerprint string) Map {
	pairs := Map{}
	if source != "" {
		pairs[replicationMetaSourceNode] = source
	}
	if sequence > 0 {
		pairs[replicationMetaSequence] = strconv.FormatUint(sequence, 10)
	}
	if fingerprint != "" {
		pairs[replicationMetaTopologyFingerprint] = fingerprint
	}
	if len(pairs) == 0 {
		return nil
	}
	return pairs
}

func commandPairsWithoutReplicationMetadata(pairs Map) Map {
	remaining := len(pairs)
	for _, key := range []string{replicationMetaSourceNode, replicationMetaSequence, replicationMetaTopologyFingerprint} {
		if _, ok := pairs[key]; ok {
			remaining--
		}
	}
	if remaining <= 0 {
		return nil
	}
	out := make(Map, remaining)
	for key, value := range pairs {
		switch key {
		case replicationMetaSourceNode, replicationMetaSequence, replicationMetaTopologyFingerprint:
			continue
		default:
			out[key] = value
		}
	}
	return out
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
		replicator.recordReplicationCircuitTransition(target, replicationCircuitHalfOpen)
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
	if result.unsupportedTypedReplication {
		return result
	}
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
			if existing.state == replicationCircuitOpen || existing.state == replicationCircuitHalfOpen {
				replicator.recordReplicationCircuitTransition(target, replicationCircuitClosed)
			}
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
	previousState := state.state
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
		if previousState != replicationCircuitOpen {
			replicator.recordReplicationCircuitTransition(target, replicationCircuitOpen)
		}
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
	return replicator.postReplicationCommandWithBody(
		ctx,
		target,
		func(message string) bool { return replicationResponseRejectsTypedPayload(payload, message) },
		func() (io.Reader, string, string, error) {
			return replicationRequestBodyForFormat(payload, replicator.wireFormat)
		},
	)
}

func (replicator *HTTPReplicator) postReplicationCommandResponse(ctx context.Context, target TopologyNode, payload CacheCommandRequest, rejectsPayload func(string) bool) (ReplicationTargetResult, CacheCommandResponse) {
	return replicator.postReplicationCommandWithBodyResponse(
		ctx,
		target,
		rejectsPayload,
		func() (io.Reader, string, string, error) {
			return replicationRequestBodyForFormat(payload, replicator.wireFormat)
		},
	)
}

func (replicator *HTTPReplicator) postReplicationCommandWithBody(ctx context.Context, target TopologyNode, rejectsTypedPayload func(string) bool, requestBody func() (io.Reader, string, string, error)) ReplicationTargetResult {
	result, _ := replicator.postReplicationCommandWithBodyResponse(ctx, target, rejectsTypedPayload, requestBody)
	return result
}

func (replicator *HTTPReplicator) postReplicationCommandWithBodyResponse(ctx context.Context, target TopologyNode, rejectsTypedPayload func(string) bool, requestBody func() (io.Reader, string, string, error)) (ReplicationTargetResult, CacheCommandResponse) {
	ctx = replicationContext(ctx)
	result := ReplicationTargetResult{
		Node:    target.ID,
		Address: target.Address,
	}
	if err := ctx.Err(); err != nil {
		result.Error = err.Error()
		return result, CacheCommandResponse{}
	}
	endpoint, err := replicationEndpoint(target.Address)
	if err != nil {
		result.Error = err.Error()
		return result, CacheCommandResponse{}
	}
	postCtx := ctx
	cancel := func() {}
	if replicator.timeout > 0 {
		postCtx, cancel = context.WithTimeout(ctx, replicator.timeout)
	}
	defer cancel()

	body, contentType, contentEncoding, err := requestBody()
	if err != nil {
		result.Error = err.Error()
		return result, CacheCommandResponse{}
	}
	req, err := http.NewRequestWithContext(postCtx, http.MethodPost, endpoint, body)
	if err != nil {
		result.Error = err.Error()
		return result, CacheCommandResponse{}
	}
	req.Header.Set("Accept", contentType)
	req.Header.Set("Content-Type", contentType)
	if replicator.authToken != "" {
		req.Header.Set("Authorization", "Bearer "+replicator.authToken)
		req.Header.Set("X-Hatrie-Replication-Token", replicator.authToken)
	}
	if contentEncoding != "" {
		req.Header.Set("Content-Encoding", contentEncoding)
	}
	resp, err := replicator.client.Do(req)
	if err != nil {
		result.Error = err.Error()
		return result, CacheCommandResponse{}
	}
	defer drainAndClose(resp.Body)

	result.Status = resp.StatusCode
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		result.Error = resp.Status
		return result, CacheCommandResponse{}
	}
	commandResponse, err := decodeReplicationCommandResponseWire(resp.Body, resp.Header.Get("Content-Type"))
	if err != nil {
		result.Error = err.Error()
		return result, CacheCommandResponse{}
	}
	if !commandResponse.OK {
		result.Error = commandResponse.Message
		if rejectsTypedPayload != nil {
			result.unsupportedTypedReplication = rejectsTypedPayload(commandResponse.Message)
		}
		return result, commandResponse
	}
	result.OK = true
	return result, commandResponse
}

func replicationResponseRejectsTypedPayload(payload CacheCommandRequest, message string) bool {
	command := normalizedCommand(payload.Command)
	hasTypedChild := false
	for _, child := range payload.Batch {
		childCommand := normalizedCommand(child.Command)
		if childCommand == replicationSetBinaryCommand || childCommand == replicationSetCompactCommand {
			hasTypedChild = true
			break
		}
	}
	return replicationResponseRejectsTypedCommand(command, hasTypedChild, message)
}

func replicationResponseRejectsTypedCommand(command string, hasTypedChild bool, message string) bool {
	command = normalizedCommand(command)
	if command != replicationSetBinaryCommand && command != replicationSetCompactCommand && command != replicationBatchEnvelopeCommand {
		return false
	}
	if message == "unsupported command" {
		return true
	}
	if command != replicationBatchEnvelopeCommand || !strings.HasPrefix(message, "internal replication batch value ") || !strings.HasSuffix(message, " must be INTERNALSET or INTERNALDEL") {
		return false
	}
	return hasTypedChild
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
	return estimatedReplicationRequestBytesWithin(payload, minCompressedReplicationRequestBytes)
}

func estimatedReplicationRequestBytesWithin(payload CacheCommandRequest, threshold int) int {
	if threshold <= 0 {
		threshold = int(^uint(0) >> 1)
	}
	estimate := 64 +
		jsonwire.EstimateJSONStringBytes(payload.Command) +
		jsonwire.EstimateJSONStringBytes(payload.Key) +
		jsonwire.EstimateJSONStringBytes(payload.Value) +
		jsonwire.EstimateJSONStringBytes(payload.Subkey)
	estimate = jsonwire.AddEstimate(estimate, len(payload.BinaryValue), threshold)
	if estimate >= threshold {
		return threshold
	}
	for _, value := range payload.Values {
		estimate = jsonwire.AddEstimate(estimate, jsonwire.EstimateJSONValueBytes(value, threshold), threshold)
		if estimate >= threshold {
			return threshold
		}
	}
	for key, value := range payload.Pairs {
		estimate = jsonwire.AddEstimate(estimate, jsonwire.EstimateJSONStringBytes(key)+1, threshold)
		if estimate >= threshold {
			return threshold
		}
		estimate = jsonwire.AddEstimate(estimate, jsonwire.EstimateJSONValueBytes(value, threshold), threshold)
		if estimate >= threshold {
			return threshold
		}
	}
	for _, payload := range payload.Batch {
		estimate = jsonwire.AddEstimate(estimate, estimatedReplicationRequestBytesWithin(payload, threshold), threshold)
		if estimate >= threshold {
			return threshold
		}
	}
	if payload.Priority != nil {
		estimate = addEstimatedOptionalCommandInt64(estimate, payload.Priority, threshold)
	}
	if payload.TTLSeconds != nil {
		estimate = addEstimatedOptionalCommandInt64(estimate, payload.TTLSeconds, threshold)
	}
	if payload.UnixSeconds != nil {
		estimate = addEstimatedOptionalCommandInt64(estimate, payload.UnixSeconds, threshold)
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
	data, ok, err := trie.commandDumpEntryBinary(key)
	if err != nil || !ok || len(data) == 0 {
		return CacheCommandRequest{}, false
	}
	return CacheCommandRequest{Command: replicationSetCompactCommand, Key: key, BinaryValue: data}, true
}

func replicationCommandPayloadLocked(trie *HatTrie, key string, kind replicationPayloadKind) (CacheCommandRequest, bool) {
	if kind == replicationPayloadDelete {
		return CacheCommandRequest{Command: "INTERNALDEL", Key: key}, true
	}
	data, ok, err := trie.commandDumpEntryBinaryLocked(key)
	if err != nil || !ok || len(data) == 0 {
		return CacheCommandRequest{}, false
	}
	return CacheCommandRequest{Command: replicationSetCompactCommand, Key: key, BinaryValue: data}, true
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
