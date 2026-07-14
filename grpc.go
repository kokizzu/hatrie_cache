package hatriecache

import (
	"context"
	"os"
	"runtime"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	hatriecachev1 "hatrie_cache/internal/gen/hatriecache/v1"
)

type CacheGRPCOptions struct {
	NodeName            string
	StartAt             time.Time
	Snapshot            func() error
	Journal             *CommandJournal
	Topology            *TopologyStore
	Election            *ElectionStore
	Replicator          *HTTPReplicator
	EnforceLeaderWrites bool
}

type CacheGRPCServer struct {
	hatriecachev1.UnimplementedCacheServiceServer
	trie    *HatTrie
	options CacheGRPCOptions
}

func NewCacheGRPCServer(trie *HatTrie, options CacheGRPCOptions) *CacheGRPCServer {
	if options.StartAt.IsZero() {
		options.StartAt = time.Now()
	}
	if options.NodeName == "" {
		if hostname, err := os.Hostname(); err == nil && hostname != "" {
			options.NodeName = hostname
		} else {
			options.NodeName = "local"
		}
	}
	return &CacheGRPCServer{trie: trie, options: options}
}

func RegisterCacheGRPCServer(registrar grpc.ServiceRegistrar, server *CacheGRPCServer) {
	hatriecachev1.RegisterCacheServiceServer(registrar, server)
}

func (server *CacheGRPCServer) Health(ctx context.Context, _ *hatriecachev1.HealthRequest) (*hatriecachev1.HealthResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	return &hatriecachev1.HealthResponse{
		Status:          "online",
		Node:            server.options.NodeName,
		UptimeSeconds:   int64(time.Since(server.options.StartAt).Seconds()),
		MemoryBytes:     mem.Alloc,
		DiskSpillBytes:  server.trie.diskSpillBytes(),
		CleanersRunning: 0,
	}, nil
}

func (server *CacheGRPCServer) Stats(ctx context.Context, _ *hatriecachev1.StatsRequest) (*hatriecachev1.StatsResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	stats := server.trie.Stats()
	return &hatriecachev1.StatsResponse{
		Reads:             stats.Reads,
		Hits:              stats.Hits,
		Misses:            stats.Misses,
		Writes:            stats.Writes,
		Deletes:           stats.Deletes,
		Expirations:       stats.Expirations,
		LastHitUnixNano:   unixNanoOrZero(stats.LastHit),
		LastMissUnixNano:  unixNanoOrZero(stats.LastMiss),
		LastWriteUnixNano: unixNanoOrZero(stats.LastWrite),
		HitRate:           stats.HitRate,
		CumulativeHitRate: stats.CumulativeHitRate,
	}, nil
}

func (server *CacheGRPCServer) Entries(ctx context.Context, request *hatriecachev1.EntriesRequest) (*hatriecachev1.EntriesResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	limit := request.GetLimit()
	if limit > maxMonitoringEntriesLimit {
		return nil, status.Errorf(codes.InvalidArgument, "limit must be <= %d", maxMonitoringEntriesLimit)
	}
	entries := server.trie.monitoringEntriesLimited(request.GetPrefix(), int(limit))
	out := make([]*hatriecachev1.Entry, 0, len(entries.Entries))
	for _, entry := range entries.Entries {
		out = append(out, grpcEntry(entry))
	}
	return &hatriecachev1.EntriesResponse{
		Entries: out,
		Limit:   entries.Limit,
		HasMore: entries.HasMore,
	}, nil
}

func (server *CacheGRPCServer) Command(ctx context.Context, request *hatriecachev1.CommandRequest) (*hatriecachev1.CommandResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	command := cacheCommandRequestFromProto(request)
	response, _ := executeCacheCommand(ctx, server.trie, command, commandExecutionOptions{
		NodeName:            server.options.NodeName,
		Journal:             server.options.Journal,
		Election:            server.options.Election,
		Replicator:          server.options.Replicator,
		EnforceLeaderWrites: server.options.EnforceLeaderWrites,
	})
	return grpcCommandResponse(response), nil
}

func (server *CacheGRPCServer) Snapshot(ctx context.Context, _ *hatriecachev1.SnapshotRequest) (*hatriecachev1.CommandResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if server.options.Snapshot == nil {
		return grpcCommandResponse(commandError("snapshot path is not configured")), nil
	}
	if err := server.options.Snapshot(); err != nil {
		return grpcCommandResponse(commandError(err.Error())), nil
	}
	return grpcCommandResponse(CacheCommandResponse{OK: true, Message: "snapshot saved"}), nil
}

func (server *CacheGRPCServer) Replication(ctx context.Context, request *hatriecachev1.ReplicationRequest) (*hatriecachev1.ReplicationResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if request == nil {
		request = &hatriecachev1.ReplicationRequest{}
	}
	if request.GetSync() {
		return grpcReplicationResponse(server.options.Replicator.SyncAll(ctx, server.trie, request.GetPrefix())), nil
	}
	return grpcReplicationResponse(server.options.Replicator.LastResult()), nil
}

func (server *CacheGRPCServer) Topology(ctx context.Context, request *hatriecachev1.TopologyRequest) (*hatriecachev1.TopologyResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if server.options.Topology == nil {
		return grpcTopologyError("topology store is not configured"), nil
	}
	key := request.GetKey()
	if key != "" {
		route, ok := server.options.Topology.Route(key)
		if !ok {
			return grpcTopologyError("topology has no shards"), nil
		}
		return &hatriecachev1.TopologyResponse{
			Ok:      true,
			Message: "ok",
			Route:   grpcTopologyRoute(route),
		}, nil
	}
	return &hatriecachev1.TopologyResponse{
		Ok:       true,
		Message:  "ok",
		Topology: grpcClusterTopology(server.options.Topology.Get()),
	}, nil
}

func (server *CacheGRPCServer) UpdateTopology(ctx context.Context, request *hatriecachev1.UpdateTopologyRequest) (*hatriecachev1.TopologyResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if server.options.Topology == nil {
		return grpcTopologyError("topology store is not configured"), nil
	}
	if err := server.options.Topology.Set(clusterTopologyFromProto(request.GetTopology())); err != nil {
		return grpcTopologyError(err.Error()), nil
	}
	return &hatriecachev1.TopologyResponse{
		Ok:       true,
		Message:  "ok",
		Topology: grpcClusterTopology(server.options.Topology.Get()),
	}, nil
}

func (server *CacheGRPCServer) Election(ctx context.Context, request *hatriecachev1.ElectionRequest) (*hatriecachev1.ElectionResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if server.options.Election == nil {
		return grpcElectionError("election store is not configured"), nil
	}
	key := request.GetKey()
	if key != "" {
		route, ok := server.options.Election.LeaderForKey(key)
		if !ok {
			return grpcElectionError("topology cannot route key"), nil
		}
		return &hatriecachev1.ElectionResponse{
			Ok:      true,
			Message: "ok",
			Route:   grpcElectionKeyRoute(route),
		}, nil
	}
	return &hatriecachev1.ElectionResponse{
		Ok:      true,
		Message: "ok",
		Status:  grpcElectionStatus(server.options.Election.Status()),
	}, nil
}

func (server *CacheGRPCServer) UpdateElection(ctx context.Context, request *hatriecachev1.UpdateElectionRequest) (*hatriecachev1.ElectionResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if server.options.Election == nil {
		return grpcElectionError("election store is not configured"), nil
	}
	if request == nil {
		request = &hatriecachev1.UpdateElectionRequest{}
	}
	var err error
	if request.Online == nil || request.GetOnline() {
		err = server.options.Election.Heartbeat(request.GetNode())
	} else {
		err = server.options.Election.MarkOffline(request.GetNode())
	}
	if err != nil {
		return grpcElectionError(err.Error()), nil
	}
	return &hatriecachev1.ElectionResponse{
		Ok:      true,
		Message: "ok",
		Status:  grpcElectionStatus(server.options.Election.Status()),
	}, nil
}

func grpcEntry(entry MonitoringEntry) *hatriecachev1.Entry {
	out := &hatriecachev1.Entry{
		Key:          entry.Key,
		Type:         entry.Type,
		OnDisk:       entry.OnDisk,
		SizeBytes:    entry.SizeBytes,
		ValuePreview: entry.ValuePreview,
	}
	if entry.TTLMillis != nil {
		out.TtlMillis = entry.TTLMillis
	}
	return out
}

func grpcTopologyError(message string) *hatriecachev1.TopologyResponse {
	return &hatriecachev1.TopologyResponse{
		Ok:      false,
		Message: message,
	}
}

func grpcElectionError(message string) *hatriecachev1.ElectionResponse {
	return &hatriecachev1.ElectionResponse{
		Ok:      false,
		Message: message,
	}
}

func grpcReplicationResponse(result ReplicationResult) *hatriecachev1.ReplicationResponse {
	out := &hatriecachev1.ReplicationResponse{
		Command: result.Command,
		Key:     result.Key,
		Entries: uint64(result.Entries),
		Queued:  result.Queued,
		Skipped: result.Skipped,
		Reason:  result.Reason,
		Targets: make([]*hatriecachev1.ReplicationTarget, 0, len(result.Targets)),
	}
	if result.Queue != nil {
		out.Queue = &hatriecachev1.ReplicationQueue{
			Enabled:   result.Queue.Enabled,
			Depth:     int64(result.Queue.Depth),
			Capacity:  int64(result.Queue.Capacity),
			Enqueued:  result.Queue.Enqueued,
			Dropped:   result.Queue.Dropped,
			Attempts:  result.Queue.Attempts,
			Successes: result.Queue.Successes,
			Failures:  result.Queue.Failures,
			Retried:   result.Queue.Retried,
			Closed:    result.Queue.Closed,
		}
	}
	for _, target := range result.Targets {
		out.Targets = append(out.Targets, &hatriecachev1.ReplicationTarget{
			Node:    target.Node,
			Key:     target.Key,
			Address: target.Address,
			Ok:      target.OK,
			Status:  int32(target.Status),
			Error:   target.Error,
		})
	}
	return out
}

func grpcClusterTopology(topology ClusterTopology) *hatriecachev1.ClusterTopology {
	out := &hatriecachev1.ClusterTopology{
		Version:      topology.Version,
		Mode:         topology.Mode,
		BucketCount:  topology.BucketCount,
		BucketRanges: make([]*hatriecachev1.TopologyBucketRange, 0, len(topology.BucketRanges)),
		Self:         topology.Self,
		Nodes:        make([]*hatriecachev1.TopologyNode, 0, len(topology.Nodes)),
		Shards:       make([]*hatriecachev1.TopologyShard, 0, len(topology.Shards)),
	}
	for _, bucketRange := range topology.BucketRanges {
		out.BucketRanges = append(out.BucketRanges, grpcTopologyBucketRange(bucketRange))
	}
	for _, node := range topology.Nodes {
		out.Nodes = append(out.Nodes, grpcTopologyNode(node))
	}
	for _, shard := range topology.Shards {
		out.Shards = append(out.Shards, grpcTopologyShard(shard))
	}
	return out
}

func grpcTopologyNode(node TopologyNode) *hatriecachev1.TopologyNode {
	return &hatriecachev1.TopologyNode{
		Id:      node.ID,
		Address: node.Address,
		Role:    node.Role,
	}
}

func grpcTopologyShard(shard TopologyShard) *hatriecachev1.TopologyShard {
	return &hatriecachev1.TopologyShard{
		Id:       shard.ID,
		Primary:  shard.Primary,
		Replicas: append([]string(nil), shard.Replicas...),
	}
}

func grpcTopologyBucketRange(bucketRange TopologyBucketRange) *hatriecachev1.TopologyBucketRange {
	return &hatriecachev1.TopologyBucketRange{
		Start: bucketRange.Start,
		End:   bucketRange.End,
		Shard: bucketRange.Shard,
	}
}

func grpcTopologyRoute(route TopologyRoute) *hatriecachev1.TopologyRoute {
	out := &hatriecachev1.TopologyRoute{
		Key:    route.Key,
		Mode:   route.Mode,
		Shard:  grpcTopologyShard(route.Shard),
		Owners: append([]string(nil), route.Owners...),
	}
	if route.Bucket != nil {
		bucket := *route.Bucket
		out.Bucket = &bucket
	}
	return out
}

func clusterTopologyFromProto(topology *hatriecachev1.ClusterTopology) ClusterTopology {
	if topology == nil {
		return ClusterTopology{}
	}
	out := ClusterTopology{
		Version:      topology.GetVersion(),
		Mode:         topology.GetMode(),
		BucketCount:  topology.GetBucketCount(),
		BucketRanges: make([]TopologyBucketRange, 0, len(topology.GetBucketRanges())),
		Self:         topology.GetSelf(),
		Nodes:        make([]TopologyNode, 0, len(topology.GetNodes())),
		Shards:       make([]TopologyShard, 0, len(topology.GetShards())),
	}
	for _, bucketRange := range topology.GetBucketRanges() {
		out.BucketRanges = append(out.BucketRanges, topologyBucketRangeFromProto(bucketRange))
	}
	for _, node := range topology.GetNodes() {
		out.Nodes = append(out.Nodes, topologyNodeFromProto(node))
	}
	for _, shard := range topology.GetShards() {
		out.Shards = append(out.Shards, topologyShardFromProto(shard))
	}
	return out
}

func topologyNodeFromProto(node *hatriecachev1.TopologyNode) TopologyNode {
	if node == nil {
		return TopologyNode{}
	}
	return TopologyNode{
		ID:      node.GetId(),
		Address: node.GetAddress(),
		Role:    node.GetRole(),
	}
}

func topologyShardFromProto(shard *hatriecachev1.TopologyShard) TopologyShard {
	if shard == nil {
		return TopologyShard{}
	}
	return TopologyShard{
		ID:       shard.GetId(),
		Primary:  shard.GetPrimary(),
		Replicas: append([]string(nil), shard.GetReplicas()...),
	}
}

func topologyBucketRangeFromProto(bucketRange *hatriecachev1.TopologyBucketRange) TopologyBucketRange {
	if bucketRange == nil {
		return TopologyBucketRange{}
	}
	return TopologyBucketRange{
		Start: bucketRange.GetStart(),
		End:   bucketRange.GetEnd(),
		Shard: bucketRange.GetShard(),
	}
}

func grpcElectionStatus(status ElectionStatus) *hatriecachev1.ElectionStatus {
	out := &hatriecachev1.ElectionStatus{
		TimeoutMillis: status.TimeoutMillis,
		Nodes:         make([]*hatriecachev1.ElectionNodeStatus, 0, len(status.Nodes)),
		Leaders:       make([]*hatriecachev1.ElectionLeader, 0, len(status.Leaders)),
	}
	for _, node := range status.Nodes {
		out.Nodes = append(out.Nodes, grpcElectionNodeStatus(node))
	}
	for _, leader := range status.Leaders {
		out.Leaders = append(out.Leaders, grpcElectionLeader(leader))
	}
	return out
}

func grpcElectionNodeStatus(node ElectionNodeStatus) *hatriecachev1.ElectionNodeStatus {
	return &hatriecachev1.ElectionNodeStatus{
		Id:               node.ID,
		Online:           node.Online,
		Reason:           node.Reason,
		LastSeenUnixNano: unixNanoPtrOrZero(node.LastSeen),
	}
}

func grpcElectionLeader(leader ElectionLeader) *hatriecachev1.ElectionLeader {
	return &hatriecachev1.ElectionLeader{
		Shard:      leader.Shard,
		Leader:     leader.Leader,
		Available:  leader.Available,
		Primary:    leader.Primary,
		Candidates: append([]string(nil), leader.Candidates...),
	}
}

func grpcElectionKeyRoute(route ElectionKeyRoute) *hatriecachev1.ElectionKeyRoute {
	return &hatriecachev1.ElectionKeyRoute{
		Key:    route.Key,
		Route:  grpcTopologyRoute(route.Route),
		Leader: grpcElectionLeader(route.Leader),
	}
}

func cacheCommandRequestFromProto(request *hatriecachev1.CommandRequest) CacheCommandRequest {
	out := CacheCommandRequest{
		Command: request.GetCommand(),
		Key:     request.GetKey(),
		Value:   request.GetValue(),
		Subkey:  request.GetSubkey(),
	}
	if request.TtlSeconds != nil {
		ttl := request.GetTtlSeconds()
		out.TTLSeconds = &ttl
	}
	if request.UnixSeconds != nil {
		unixSeconds := request.GetUnixSeconds()
		out.UnixSeconds = &unixSeconds
	}
	if request.Priority != nil {
		priority := request.GetPriority()
		out.Priority = &priority
	}
	if len(request.Values) > 0 {
		out.Values = make(Slice, len(request.Values))
		for i, value := range request.Values {
			out.Values[i] = value
		}
	}
	if len(request.Pairs) > 0 {
		out.Pairs = make(Map, len(request.Pairs))
		for key, value := range request.Pairs {
			out.Pairs[key] = value
		}
	}
	return out
}

func grpcCommandResponse(response CacheCommandResponse) *hatriecachev1.CommandResponse {
	return &hatriecachev1.CommandResponse{
		Ok:      response.OK,
		Message: response.Message,
		Value:   response.Value,
	}
}

func unixNanoOrZero(value time.Time) int64 {
	if value.IsZero() {
		return 0
	}
	return value.UnixNano()
}

func unixNanoPtrOrZero(value *time.Time) int64 {
	if value == nil {
		return 0
	}
	return unixNanoOrZero(*value)
}
