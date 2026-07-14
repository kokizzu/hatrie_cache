package hatriecache

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	json "github.com/goccy/go-json"
)

const DefaultReplicationTimeout = 2 * time.Second
const DefaultReplicationRetryInterval = 250 * time.Millisecond
const maxHTTPReplicationResponseBytes = 1 << 20
const maxHTTPResponseDrainBytes = 1 << 20

var errReplicationResponseTooLarge = errors.New("hatriecache: replication response is too large")

type HTTPReplicatorOptions struct {
	Self               string
	Topology           *TopologyStore
	Election           *ElectionStore
	Client             *http.Client
	Timeout            time.Duration
	AsyncQueueSize     int
	AsyncRetryInterval time.Duration
	AsyncMaxAttempts   uint
}

type HTTPReplicator struct {
	mu         sync.RWMutex
	self       string
	topology   *TopologyStore
	election   *ElectionStore
	client     *http.Client
	timeout    time.Duration
	queue      chan replicationJob
	retry      time.Duration
	attempts   uint
	done       chan struct{}
	stopped    chan struct{}
	cancel     context.CancelFunc
	close      sync.Once
	closed     bool
	queueStats ReplicationQueueStats
	last       ReplicationResult
}

type ReplicationResult struct {
	Command string                    `json:"command,omitempty"`
	Key     string                    `json:"key,omitempty"`
	Entries int                       `json:"entries,omitempty"`
	Queued  bool                      `json:"queued,omitempty"`
	Skipped bool                      `json:"skipped"`
	Reason  string                    `json:"reason,omitempty"`
	Queue   *ReplicationQueueStats    `json:"queue,omitempty"`
	Targets []ReplicationTargetResult `json:"targets,omitempty"`
}

// ReplicationQueueStats reports bounded async replication outbox health.
type ReplicationQueueStats struct {
	Enabled   bool   `json:"enabled"`
	Depth     int    `json:"depth"`
	Capacity  int    `json:"capacity"`
	Enqueued  uint64 `json:"enqueued"`
	Dropped   uint64 `json:"dropped"`
	Attempts  uint64 `json:"attempts"`
	Successes uint64 `json:"successes"`
	Failures  uint64 `json:"failures"`
	Retried   uint64 `json:"retried"`
	Closed    bool   `json:"closed"`
}

type ReplicationTargetResult struct {
	Node    string `json:"node"`
	Key     string `json:"key,omitempty"`
	Address string `json:"address,omitempty"`
	OK      bool   `json:"ok"`
	Status  int    `json:"status,omitempty"`
	Error   string `json:"error,omitempty"`
}

type replicationPayloadKind int

const (
	replicationPayloadNone replicationPayloadKind = iota
	replicationPayloadSet
	replicationPayloadDelete
)

type replicationTask struct {
	target  TopologyNode
	payload CacheCommandRequest
}

type replicationJob struct {
	result ReplicationResult
	tasks  []replicationTask
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
	replicator := &HTTPReplicator{
		self:     strings.TrimSpace(options.Self),
		topology: options.Topology,
		election: options.Election,
		client:   client,
		timeout:  timeout,
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
		ctx, cancel := context.WithCancel(context.Background())
		replicator.queue = make(chan replicationJob, options.AsyncQueueSize)
		replicator.retry = retry
		replicator.attempts = attempts
		replicator.done = make(chan struct{})
		replicator.stopped = make(chan struct{})
		replicator.cancel = cancel
		replicator.queueStats = ReplicationQueueStats{
			Enabled:  true,
			Capacity: options.AsyncQueueSize,
		}
		go replicator.runAsync(ctx)
	}
	return replicator
}

func (replicator *HTTPReplicator) LastResult() ReplicationResult {
	if replicator == nil {
		return ReplicationResult{Skipped: true, Reason: "replication is not configured"}
	}
	replicator.mu.RLock()
	defer replicator.mu.RUnlock()
	result := cloneReplicationResult(replicator.last)
	if replicator.queue != nil {
		stats := replicator.queueStatsLocked()
		result.Queue = &stats
	}
	return result
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
		return ReplicationResult{
			Command: normalizedCommand(request.Command),
			Key:     strings.TrimSpace(request.Key),
			Skipped: true,
			Reason:  "replication is not configured",
		}
	}

	var result ReplicationResult
	if replicator.queue != nil {
		result = replicator.enqueueReplication(ctx, trie, request, response)
	} else {
		result = replicator.replicateCommand(ctx, trie, request, response)
	}
	replicator.storeLastResult(result)
	return result
}

func (replicator *HTTPReplicator) SyncAll(ctx context.Context, trie *HatTrie, prefix string) ReplicationResult {
	if replicator == nil {
		return ReplicationResult{
			Command: "SYNC",
			Key:     prefix,
			Skipped: true,
			Reason:  "replication is not configured",
		}
	}

	result := replicator.syncAll(ctx, trie, prefix)
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
		replicator.recordAsyncDropped()
		return result
	}
	select {
	case replicator.queue <- job:
		replicator.recordAsyncEnqueued()
		return result
	case <-replicator.done:
		result.Queued = false
		result.Skipped = true
		result.Reason = "replication queue is closed"
		result.Targets = nil
		replicator.recordAsyncDropped()
		return result
	default:
		result.Queued = false
		result.Skipped = true
		result.Reason = "replication queue is full"
		result.Targets = nil
		replicator.recordAsyncDropped()
		return result
	}
}

func (replicator *HTTPReplicator) asyncClosed() bool {
	if replicator == nil {
		return true
	}
	replicator.mu.RLock()
	defer replicator.mu.RUnlock()
	return replicator.closed
}

func (replicator *HTTPReplicator) recordAsyncEnqueued() {
	if replicator == nil || replicator.queue == nil {
		return
	}
	replicator.mu.Lock()
	defer replicator.mu.Unlock()
	replicator.queueStats.Enqueued++
}

func (replicator *HTTPReplicator) recordAsyncDropped() {
	if replicator == nil || replicator.queue == nil {
		return
	}
	replicator.mu.Lock()
	defer replicator.mu.Unlock()
	replicator.queueStats.Dropped++
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
		}
	}
	if retried {
		replicator.queueStats.Retried++
	}
}

func (replicator *HTTPReplicator) queueStatsLocked() ReplicationQueueStats {
	stats := replicator.queueStats
	if replicator.queue != nil {
		stats.Depth = len(replicator.queue)
		stats.Capacity = cap(replicator.queue)
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
		result.Targets = append(result.Targets, replicator.postReplicationCommand(ctx, task.target, task.payload))
	}
	return result
}

func (replicator *HTTPReplicator) runAsync(ctx context.Context) {
	defer close(replicator.stopped)
	for {
		select {
		case <-ctx.Done():
			return
		case <-replicator.done:
			return
		case job := <-replicator.queue:
			replicator.runAsyncJob(ctx, job)
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
			replicator.storeLastResult(result)
			return
		}
		result = replicator.executeReplicationTasks(ctx, job.result, job.tasks)
		needsRetry := replicationNeedsRetry(result)
		willRetry := needsRetry && attempt < attempts
		replicator.recordAsyncAttempt(result, willRetry)
		replicator.storeLastResult(result)
		if !needsRetry || attempt == attempts {
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
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-replicator.done:
		return false
	case <-timer.C:
		return true
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

	keys := trie.KeysWithPrefix(prefix, true)
	if len(keys) == 0 {
		result.Skipped = true
		result.Reason = "no entries to sync"
		return result
	}

	for _, key := range keys {
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
		result.Entries++
		for _, target := range targets {
			targetResult := replicator.postReplicationCommand(ctx, target, payload)
			targetResult.Key = key
			result.Targets = append(result.Targets, targetResult)
		}
	}
	if len(result.Targets) == 0 {
		result.Skipped = true
		result.Reason = "no sync targets"
	}
	return result
}

func (replicator *HTTPReplicator) storeLastResult(result ReplicationResult) {
	if replicator == nil {
		return
	}
	replicator.mu.Lock()
	defer replicator.mu.Unlock()
	replicator.last = cloneReplicationResult(result)
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
	data, err := json.Marshal(payload)
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

	req, err := http.NewRequestWithContext(postCtx, http.MethodPost, endpoint, bytes.NewReader(data))
	if err != nil {
		result.Error = err.Error()
		return result
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
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
	commandResponse, err := decodeReplicationCommandResponse(resp.Body)
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
	limited := &io.LimitedReader{R: body, N: maxHTTPReplicationResponseBytes + 1}
	decoder := json.NewDecoder(limited)
	var response CacheCommandResponse
	if err := decoder.Decode(&response); err != nil {
		if limitedReaderExceeded(limited) {
			return CacheCommandResponse{}, errReplicationResponseTooLarge
		}
		return CacheCommandResponse{}, err
	}
	if limited.N <= 0 {
		return CacheCommandResponse{}, errReplicationResponseTooLarge
	}
	var extra struct{}
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if limitedReaderExceeded(limited) {
			return CacheCommandResponse{}, errReplicationResponseTooLarge
		}
		if err == nil {
			return CacheCommandResponse{}, errors.New("hatriecache: invalid replication response JSON")
		}
		return CacheCommandResponse{}, err
	}
	if limitedReaderExceeded(limited) {
		return CacheCommandResponse{}, errReplicationResponseTooLarge
	}
	return response, nil
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

func cloneReplicationResult(result ReplicationResult) ReplicationResult {
	out := result
	if result.Targets != nil {
		out.Targets = append([]ReplicationTargetResult(nil), result.Targets...)
	}
	return out
}
