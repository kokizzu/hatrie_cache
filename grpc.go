package hatriecache

import (
	"context"
	"errors"
	"io"
	"os"
	"runtime"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	hatriecachev1 "hatrie_cache/internal/gen/hatriecache/v1"
)

type CacheGRPCOptions struct {
	NodeName                         string
	AuthToken                        string
	AuthPreviousToken                string
	AuthPreviousExpiresAt            time.Time
	ReplicationAuthToken             string
	ReplicationAuthPreviousToken     string
	ReplicationAuthPreviousExpiresAt time.Time
	AuditLog                         *AuditLogger
	WriteProtected                   bool
	RateLimiter                      *RateLimiter
	Metrics                          *APIMetrics
	StartAt                          time.Time
	Snapshot                         func() error
	Journal                          *CommandJournal
	DirtyTracker                     *LevelDBDirtyTracker
	Topology                         *TopologyStore
	Election                         *ElectionStore
	Replicator                       *HTTPReplicator
	ReplicationSafety                *ReplicationSafetyStore
	EnforceLeaderWrites              bool
}

type CacheGRPCServer struct {
	hatriecachev1.UnimplementedCacheServiceServer
	trie    *HatTrie
	options CacheGRPCOptions
}

func NewCacheGRPCServer(trie *HatTrie, options CacheGRPCOptions) *CacheGRPCServer {
	options.AuthToken = normalizeAuthToken(options.AuthToken)
	options.AuthPreviousToken = normalizeAuthToken(options.AuthPreviousToken)
	options.ReplicationAuthToken = normalizeAuthToken(options.ReplicationAuthToken)
	options.ReplicationAuthPreviousToken = normalizeAuthToken(options.ReplicationAuthPreviousToken)
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
	if options.Metrics == nil {
		options.Metrics = NewAPIMetrics()
	}
	if options.ReplicationSafety == nil {
		options.ReplicationSafety = NewReplicationSafetyStore()
	}
	return &CacheGRPCServer{trie: trie, options: options}
}

func (server *CacheGRPCServer) requireReplicationAuthorized(ctx context.Context) error {
	tokens := newAuthTokenSet(server.options.ReplicationAuthToken, server.options.ReplicationAuthPreviousToken, server.options.ReplicationAuthPreviousExpiresAt)
	if !tokens.configured() {
		return server.requireAuthorized(ctx)
	}
	now := time.Now()
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		for _, candidate := range md.Get("x-hatrie-replication-token") {
			if tokens.matches(candidate, now) {
				return nil
			}
		}
		for _, candidate := range md.Get("authorization") {
			if tokens.matches(authBearerToken(candidate), now) {
				return nil
			}
		}
	}
	if server.options.AuthToken != "" {
		return server.requireAuthorized(ctx)
	}
	return status.Error(codes.Unauthenticated, "unauthorized")
}

func RegisterCacheGRPCServer(registrar grpc.ServiceRegistrar, server *CacheGRPCServer) {
	hatriecachev1.RegisterCacheServiceServer(registrar, server)
}

func grpcContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func (server *CacheGRPCServer) requireTrie() error {
	if server.trie != nil {
		return nil
	}
	return status.Error(codes.Unavailable, "trie is not configured")
}

func (server *CacheGRPCServer) requestContext(ctx context.Context) (context.Context, error) {
	ctx = grpcContext(ctx)
	if err := ctx.Err(); err != nil {
		return ctx, err
	}
	if err := server.requireAuthorized(ctx); err != nil {
		return ctx, err
	}
	return ctx, nil
}

func (server *CacheGRPCServer) requireAuthorized(ctx context.Context) error {
	tokens := newAuthTokenSet(server.options.AuthToken, server.options.AuthPreviousToken, server.options.AuthPreviousExpiresAt)
	if !tokens.configured() {
		return nil
	}
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "unauthorized")
	}
	now := time.Now()
	for _, candidate := range md.Get("x-hatrie-auth-token") {
		if tokens.matches(candidate, now) {
			return nil
		}
	}
	for _, candidate := range md.Get("authorization") {
		if tokens.matches(authBearerToken(candidate), now) {
			return nil
		}
	}
	return status.Error(codes.Unauthenticated, "unauthorized")
}

func (server *CacheGRPCServer) auditGRPC(event AuditEvent) {
	if server.options.AuditLog == nil {
		return
	}
	event.Node = server.options.NodeName
	event.Protocol = "grpc"
	server.options.Metrics.RecordAuditResult(server.options.AuditLog.Log(event))
}

func (server *CacheGRPCServer) rejectDangerousGRPC(action string, event AuditEvent) error {
	if server.options.WriteProtected {
		server.options.Metrics.RecordWriteProtectionRejection()
		event.Action = action
		event.OK = false
		event.Message = "writes are disabled"
		server.auditGRPC(event)
		return status.Error(codes.PermissionDenied, "writes are disabled")
	}
	if server.options.RateLimiter != nil && !server.options.RateLimiter.Allow("grpc") {
		server.options.Metrics.RecordRateLimitRejection()
		event.Action = action
		event.OK = false
		event.Message = "rate limit exceeded"
		server.auditGRPC(event)
		return status.Error(codes.ResourceExhausted, "rate limit exceeded")
	}
	return nil
}

func (server *CacheGRPCServer) Health(ctx context.Context, _ *hatriecachev1.HealthRequest) (*hatriecachev1.HealthResponse, error) {
	ctx, err := server.requestContext(ctx)
	if err != nil {
		return nil, err
	}
	if err := server.requireTrie(); err != nil {
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
	ctx, err := server.requestContext(ctx)
	if err != nil {
		return nil, err
	}
	if err := server.requireTrie(); err != nil {
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
	ctx, err := server.requestContext(ctx)
	if err != nil {
		return nil, err
	}
	if err := server.requireTrie(); err != nil {
		return nil, err
	}
	limit := request.GetLimit()
	if limit > maxMonitoringEntriesLimit {
		return nil, status.Errorf(codes.InvalidArgument, "limit must be <= %d", maxMonitoringEntriesLimit)
	}
	afterKey, hasAfterKey, err := monitoringEntriesAfterKey(request.GetPrefix(), request.GetAfterKey(), request != nil && request.AfterKey != nil)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	entries := server.trie.monitoringEntriesPage(request.GetPrefix(), afterKey, hasAfterKey, int(limit))
	out := make([]*hatriecachev1.Entry, 0, len(entries.Entries))
	for _, entry := range entries.Entries {
		out = append(out, grpcEntry(entry))
	}
	return &hatriecachev1.EntriesResponse{
		Entries:      out,
		Limit:        entries.Limit,
		HasMore:      entries.HasMore,
		AfterKey:     entries.AfterKey,
		NextAfterKey: entries.NextAfterKey,
	}, nil
}

func (server *CacheGRPCServer) Command(ctx context.Context, request *hatriecachev1.CommandRequest) (*hatriecachev1.CommandResponse, error) {
	ctx, err := server.requestContext(ctx)
	if err != nil {
		return nil, err
	}
	return server.executeGRPCCommand(ctx, request, "/hatriecache.v1.CacheService/Command")
}

func (server *CacheGRPCServer) CommandStream(stream hatriecachev1.CacheService_CommandStreamServer) error {
	ctx, err := server.requestContext(stream.Context())
	if err != nil {
		return err
	}
	for {
		request, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		response, err := server.executeGRPCCommand(ctx, request, "/hatriecache.v1.CacheService/CommandStream")
		if err != nil {
			return err
		}
		if err := stream.Send(response); err != nil {
			return err
		}
	}
}

func (server *CacheGRPCServer) CommandBatchStream(stream hatriecachev1.CacheService_CommandBatchStreamServer) error {
	ctx, err := server.requestContext(stream.Context())
	if err != nil {
		return err
	}
	for {
		batch, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		requests := batch.GetRequests()
		request := &hatriecachev1.CommandRequest{Command: "BATCH", Batch: requests}
		response, err := server.executeGRPCCommand(ctx, request, "/hatriecache.v1.CacheService/CommandBatchStream")
		if err != nil {
			return err
		}
		if err := stream.Send(&hatriecachev1.CommandBatchResponse{
			BatchId:   batch.GetBatchId(),
			Responses: response.GetResponses(),
			Ok:        response.GetOk(),
			Message:   response.GetMessage(),
		}); err != nil {
			return err
		}
	}
}

func (server *CacheGRPCServer) executeGRPCCommand(ctx context.Context, request *hatriecachev1.CommandRequest, method string) (*hatriecachev1.CommandResponse, error) {
	command := cacheCommandRequestFromProto(request)
	if commandShouldJournal(command) {
		audit := AuditEvent{
			Command: normalizedCommand(command.Command),
			Key:     strings.TrimSpace(command.Key),
			Method:  method,
		}
		if err := server.rejectDangerousGRPC("command", audit); err != nil {
			return nil, err
		}
	}
	response, _ := executeCacheCommand(ctx, server.trie, command, commandExecutionOptions{
		NodeName:            server.options.NodeName,
		Journal:             server.options.Journal,
		DirtyTracker:        server.options.DirtyTracker,
		Topology:            server.options.Topology,
		Election:            server.options.Election,
		Replicator:          server.options.Replicator,
		ReplicationSafety:   server.options.ReplicationSafety,
		EnforceLeaderWrites: server.options.EnforceLeaderWrites,
	})
	if commandShouldJournal(command) {
		server.auditGRPC(AuditEvent{
			Action:  "command",
			Command: normalizedCommand(command.Command),
			Key:     strings.TrimSpace(command.Key),
			OK:      response.OK,
			Method:  method,
			Message: response.Message,
		})
	}
	return grpcCommandResponse(response), nil
}

func (server *CacheGRPCServer) Snapshot(ctx context.Context, _ *hatriecachev1.SnapshotRequest) (*hatriecachev1.CommandResponse, error) {
	ctx, err := server.requestContext(ctx)
	if err != nil {
		return nil, err
	}
	if err := server.rejectDangerousGRPC("snapshot", AuditEvent{Method: "/hatriecache.v1.CacheService/Snapshot"}); err != nil {
		return nil, err
	}
	if server.options.Snapshot == nil {
		server.auditGRPC(AuditEvent{Action: "snapshot", OK: false, Method: "/hatriecache.v1.CacheService/Snapshot", Message: "snapshot path is not configured"})
		return grpcCommandResponse(commandError("snapshot path is not configured")), nil
	}
	if err := server.options.Snapshot(); err != nil {
		server.auditGRPC(AuditEvent{Action: "snapshot", OK: false, Method: "/hatriecache.v1.CacheService/Snapshot", Message: err.Error()})
		return grpcCommandResponse(commandError(err.Error())), nil
	}
	server.auditGRPC(AuditEvent{Action: "snapshot", OK: true, Method: "/hatriecache.v1.CacheService/Snapshot", Message: "snapshot saved"})
	return grpcCommandResponse(CacheCommandResponse{OK: true, Message: "snapshot saved"}), nil
}

func (server *CacheGRPCServer) Replication(ctx context.Context, request *hatriecachev1.ReplicationRequest) (*hatriecachev1.ReplicationResponse, error) {
	ctx, err := server.requestContext(ctx)
	if err != nil {
		return nil, err
	}
	if request == nil {
		request = &hatriecachev1.ReplicationRequest{}
	}
	if request.GetSync() {
		if err := server.rejectDangerousGRPC("replication.sync", AuditEvent{Method: "/hatriecache.v1.CacheService/Replication", Details: map[string]interface{}{"prefix": request.GetPrefix()}}); err != nil {
			return nil, err
		}
		result := server.options.Replicator.SyncAll(ctx, server.trie, request.GetPrefix())
		server.auditGRPC(AuditEvent{Action: "replication.sync", OK: !result.Skipped, Method: "/hatriecache.v1.CacheService/Replication", Message: result.Reason, Details: map[string]interface{}{"prefix": request.GetPrefix(), "entries": result.Entries}})
		return grpcReplicationResponse(result), nil
	}
	return grpcReplicationResponse(server.options.Replicator.LastResult()), nil
}

func (server *CacheGRPCServer) ReplicationStream(stream hatriecachev1.CacheService_ReplicationStreamServer) error {
	ctx := grpcContext(stream.Context())
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := server.requireTrie(); err != nil {
		return err
	}
	if err := server.requireReplicationAuthorized(ctx); err != nil {
		return err
	}

	for {
		batch, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		ack := server.applyReplicationStreamBatch(ctx, batch)
		if err := stream.Send(ack); err != nil {
			return err
		}
	}
}

func (server *CacheGRPCServer) applyReplicationStreamBatch(ctx context.Context, batch *hatriecachev1.ReplicationStreamBatch) *hatriecachev1.ReplicationStreamAck {
	if batch == nil {
		return &hatriecachev1.ReplicationStreamAck{Message: "replication stream batch is required"}
	}
	ack := &hatriecachev1.ReplicationStreamAck{Sequence: batch.GetSequence()}
	keys := batch.GetKeys()
	values := batch.GetBinaryValues()
	if len(keys) == 0 || len(keys) != len(values) {
		ack.Message = "replication stream keys and values must have the same non-zero length"
		return ack
	}
	if err := server.rejectDangerousGRPC("replication.stream", AuditEvent{
		Method:  "/hatriecache.v1.CacheService/ReplicationStream",
		Details: map[string]interface{}{"source": batch.GetSource(), "sequence": batch.GetSequence(), "entries": len(keys)},
	}); err != nil {
		ack.Message = err.Error()
		return ack
	}

	payloads := make([]CacheCommandRequest, len(keys))
	for idx := range keys {
		if len(values[idx]) == 0 {
			payloads[idx] = CacheCommandRequest{Command: "INTERNALDEL", Key: keys[idx]}
		} else {
			payloads[idx] = CacheCommandRequest{
				Command:     replicationSetCompactCommand,
				Key:         keys[idx],
				BinaryValue: values[idx],
			}
		}
	}
	request := replicationBatchEnvelopePayloadWithMetadata(payloads, batch.GetSource(), batch.GetSequence(), batch.GetTopologyFingerprint())
	response, _ := executeCacheCommand(ctx, server.trie, request, commandExecutionOptions{
		NodeName:          server.options.NodeName,
		Journal:           server.options.Journal,
		DirtyTracker:      server.options.DirtyTracker,
		Topology:          server.options.Topology,
		Election:          server.options.Election,
		ReplicationSafety: server.options.ReplicationSafety,
	})
	ack.Ok = response.OK
	ack.Message = response.Message
	if response.OK {
		ack.Entries = uint64(len(keys))
	}
	server.auditGRPC(AuditEvent{
		Action: "replication.stream", OK: response.OK, Method: "/hatriecache.v1.CacheService/ReplicationStream", Message: response.Message,
		Details: map[string]interface{}{"source": batch.GetSource(), "sequence": batch.GetSequence(), "entries": len(keys)},
	})
	return ack
}

func (server *CacheGRPCServer) Topology(ctx context.Context, request *hatriecachev1.TopologyRequest) (*hatriecachev1.TopologyResponse, error) {
	ctx, err := server.requestContext(ctx)
	if err != nil {
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
	ctx, err := server.requestContext(ctx)
	if err != nil {
		return nil, err
	}
	if server.options.Topology == nil {
		return grpcTopologyError("topology store is not configured"), nil
	}
	if err := server.rejectDangerousGRPC("topology.update", AuditEvent{Method: "/hatriecache.v1.CacheService/UpdateTopology"}); err != nil {
		return nil, err
	}
	if err := server.options.Topology.Set(clusterTopologyFromProto(request.GetTopology())); err != nil {
		server.auditGRPC(AuditEvent{Action: "topology.update", OK: false, Method: "/hatriecache.v1.CacheService/UpdateTopology", Message: err.Error()})
		return grpcTopologyError(err.Error()), nil
	}
	topology := server.options.Topology.Get()
	server.auditGRPC(AuditEvent{Action: "topology.update", OK: true, Method: "/hatriecache.v1.CacheService/UpdateTopology", Details: map[string]interface{}{"mode": topology.Mode, "version": topology.Version, "nodes": len(topology.Nodes), "shards": len(topology.Shards)}})
	return &hatriecachev1.TopologyResponse{
		Ok:       true,
		Message:  "ok",
		Topology: grpcClusterTopology(topology),
	}, nil
}

func (server *CacheGRPCServer) Election(ctx context.Context, request *hatriecachev1.ElectionRequest) (*hatriecachev1.ElectionResponse, error) {
	ctx, err := server.requestContext(ctx)
	if err != nil {
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
	ctx, err := server.requestContext(ctx)
	if err != nil {
		return nil, err
	}
	if server.options.Election == nil {
		return grpcElectionError("election store is not configured"), nil
	}
	if request == nil {
		request = &hatriecachev1.UpdateElectionRequest{}
	}
	if err := server.rejectDangerousGRPC("election.update", AuditEvent{Method: "/hatriecache.v1.CacheService/UpdateElection", Details: map[string]interface{}{"node": request.GetNode(), "online": request.Online == nil || request.GetOnline()}}); err != nil {
		return nil, err
	}
	if request.Online == nil || request.GetOnline() {
		err = server.options.Election.Heartbeat(request.GetNode())
	} else {
		err = server.options.Election.MarkOffline(request.GetNode())
	}
	if err != nil {
		server.auditGRPC(AuditEvent{Action: "election.update", OK: false, Method: "/hatriecache.v1.CacheService/UpdateElection", Message: err.Error(), Details: map[string]interface{}{"node": request.GetNode(), "online": request.Online == nil || request.GetOnline()}})
		return grpcElectionError(err.Error()), nil
	}
	server.auditGRPC(AuditEvent{Action: "election.update", OK: true, Method: "/hatriecache.v1.CacheService/UpdateElection", Details: map[string]interface{}{"node": request.GetNode(), "online": request.Online == nil || request.GetOnline()}})
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
		Command:            result.Command,
		Key:                result.Key,
		Entries:            uint64(result.Entries),
		Queued:             result.Queued,
		Skipped:            result.Skipped,
		Reason:             result.Reason,
		StartedAtUnixNano:  unixNanoPtrOrZero(result.StartedAt),
		FinishedAtUnixNano: unixNanoPtrOrZero(result.FinishedAt),
		DurationMillis:     result.DurationMillis,
		Health:             result.Health,
		HealthScore:        int32(result.HealthScore),
		HealthReason:       result.HealthReason,
		Targets:            make([]*hatriecachev1.ReplicationTarget, 0, len(result.Targets)),
	}
	if result.Queue != nil {
		out.Queue = &hatriecachev1.ReplicationQueue{
			Enabled:        result.Queue.Enabled,
			Depth:          int64(result.Queue.Depth),
			Capacity:       int64(result.Queue.Capacity),
			Enqueued:       result.Queue.Enqueued,
			Dropped:        result.Queue.Dropped,
			Attempts:       result.Queue.Attempts,
			Successes:      result.Queue.Successes,
			Failures:       result.Queue.Failures,
			Retried:        result.Queue.Retried,
			Closed:         result.Queue.Closed,
			DurableBacklog: result.Queue.DurableBacklog,
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
		Id:                node.ID,
		Address:           node.Address,
		GrpcAddress:       node.GRPCAddress,
		Role:              node.Role,
		Maintenance:       node.Maintenance,
		MaintenanceReason: node.MaintenanceReason,
		MaintenanceSince:  node.MaintenanceSince,
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
		ID:                node.GetId(),
		Address:           node.GetAddress(),
		GRPCAddress:       node.GetGrpcAddress(),
		Role:              node.GetRole(),
		Maintenance:       node.GetMaintenance(),
		MaintenanceReason: node.GetMaintenanceReason(),
		MaintenanceSince:  node.GetMaintenanceSince(),
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
	if request == nil {
		return CacheCommandRequest{}
	}
	out := CacheCommandRequest{
		Command:     request.GetCommand(),
		Key:         request.GetKey(),
		Value:       request.GetValue(),
		Subkey:      request.GetSubkey(),
		BinaryValue: append([]byte(nil), request.GetBinaryValue()...),
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
	if len(request.Batch) > 0 {
		out.Batch = make([]CacheCommandRequest, len(request.Batch))
		for i, value := range request.Batch {
			out.Batch[i] = cacheCommandRequestFromProto(value)
		}
	}
	return out
}

func grpcCommandResponse(response CacheCommandResponse) *hatriecachev1.CommandResponse {
	out := &hatriecachev1.CommandResponse{
		Ok:      response.OK,
		Message: response.Message,
		Value:   response.Value,
	}
	if len(response.Responses) > 0 {
		out.Responses = make([]*hatriecachev1.CommandResponse, len(response.Responses))
		for i := range response.Responses {
			out.Responses[i] = grpcCommandResponse(response.Responses[i])
		}
	}
	return out
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
