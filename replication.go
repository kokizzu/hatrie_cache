package hatriecache

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"
)

const DefaultReplicationTimeout = 2 * time.Second

type HTTPReplicatorOptions struct {
	Self     string
	Topology *TopologyStore
	Election *ElectionStore
	Client   *http.Client
	Timeout  time.Duration
}

type HTTPReplicator struct {
	mu       sync.RWMutex
	self     string
	topology *TopologyStore
	election *ElectionStore
	client   *http.Client
	timeout  time.Duration
	last     ReplicationResult
}

type ReplicationResult struct {
	Command string                    `json:"command,omitempty"`
	Key     string                    `json:"key,omitempty"`
	Skipped bool                      `json:"skipped"`
	Reason  string                    `json:"reason,omitempty"`
	Targets []ReplicationTargetResult `json:"targets,omitempty"`
}

type ReplicationTargetResult struct {
	Node    string `json:"node"`
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

func NewHTTPReplicator(options HTTPReplicatorOptions) *HTTPReplicator {
	client := options.Client
	if client == nil {
		client = http.DefaultClient
	}
	timeout := options.Timeout
	if timeout == 0 {
		timeout = DefaultReplicationTimeout
	}
	return &HTTPReplicator{
		self:     strings.TrimSpace(options.Self),
		topology: options.Topology,
		election: options.Election,
		client:   client,
		timeout:  timeout,
	}
}

func (replicator *HTTPReplicator) LastResult() ReplicationResult {
	if replicator == nil {
		return ReplicationResult{Skipped: true, Reason: "replication is not configured"}
	}
	replicator.mu.RLock()
	defer replicator.mu.RUnlock()
	return cloneReplicationResult(replicator.last)
}

func (replicator *HTTPReplicator) ReplicateCommand(ctx context.Context, trie *HatTrie, request CacheCommandRequest, response CacheCommandResponse) ReplicationResult {
	result := replicator.replicateCommand(ctx, trie, request, response)
	replicator.storeLastResult(result)
	return result
}

func (replicator *HTTPReplicator) replicateCommand(ctx context.Context, trie *HatTrie, request CacheCommandRequest, response CacheCommandResponse) ReplicationResult {
	ctx = replicationContext(ctx)
	command := normalizedCommand(request.Command)
	key := strings.TrimSpace(request.Key)
	result := ReplicationResult{Command: command, Key: key}
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
	kind := replicationPayloadKindFor(request, response)
	if kind == replicationPayloadNone {
		result.Skipped = true
		result.Reason = "command is not replicated"
		return result
	}

	route, ok := replicator.routeForKey(key)
	if !ok {
		result.Skipped = true
		result.Reason = "topology cannot route key"
		return result
	}
	if replicator.self != "" && route.Leader.Leader != "" && route.Leader.Leader != replicator.self {
		result.Skipped = true
		result.Reason = "local node is not elected leader"
		return result
	}

	targets := replicator.replicationTargets(route)
	if len(targets) == 0 {
		result.Skipped = true
		result.Reason = "no remote replication targets"
		return result
	}

	payload, ok := replicationCommandPayload(trie, key, kind)
	if !ok {
		result.Skipped = true
		result.Reason = "no local value to replicate"
		return result
	}
	for _, target := range targets {
		result.Targets = append(result.Targets, replicator.postReplicationCommand(ctx, target, payload))
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
	defer resp.Body.Close()

	result.Status = resp.StatusCode
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		result.Error = resp.Status
		return result
	}
	var commandResponse CacheCommandResponse
	if err := json.NewDecoder(resp.Body).Decode(&commandResponse); err != nil {
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
	case "DUMP", "GET", "GETSTR", "EXISTS", "TTL", "PEEKMAP", "HEADSLICE", "TAILSLICE", "HASSET", "GETSET", "PEEKPQ", "PEEKPRIORITY", "GETPQ", "GETPRIORITY", "HASBF", "BFHAS", "BFEXISTS", "INFOBF", "BFINFO", "ESTCMS", "QUERYCMS", "CMSQUERY", "CMSCOUNT", "INFOCMS", "CMSINFO", "COUNTHLL", "ESTHLL", "HLLCOUNT", "HLLCARD", "INFOHLL", "HLLINFO", "INTERNALSET", "INTERNALDEL":
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
