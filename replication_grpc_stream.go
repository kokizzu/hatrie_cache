package hatriecache

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	hatriecachev1 "hatrie_cache/internal/gen/hatriecache/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	grpcgzip "google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/metadata"
)

type replicationGRPCSyncSession struct {
	replicator *HTTPReplicator
	ctx        context.Context
	mu         sync.Mutex
	targets    map[string]*replicationGRPCStreamTarget
	fallback   map[string]bool
}

type replicationGRPCStreamTarget struct {
	conn   *grpc.ClientConn
	stream hatriecachev1.CacheService_ReplicationStreamClient
	cancel context.CancelFunc
	mu     sync.Mutex
}

func newReplicationGRPCSyncSession(ctx context.Context, replicator *HTTPReplicator) *replicationGRPCSyncSession {
	if ctx == nil {
		ctx = context.Background()
	}
	return &replicationGRPCSyncSession{
		replicator: replicator,
		ctx:        ctx,
		targets:    make(map[string]*replicationGRPCStreamTarget),
		fallback:   make(map[string]bool),
	}
}

func (session *replicationGRPCSyncSession) close() {
	if session == nil {
		return
	}
	session.mu.Lock()
	targets := session.targets
	session.targets = make(map[string]*replicationGRPCStreamTarget)
	session.mu.Unlock()
	for _, target := range targets {
		target.close()
	}
}

func (target *replicationGRPCStreamTarget) close() {
	if target == nil {
		return
	}
	target.mu.Lock()
	defer target.mu.Unlock()
	if target.stream != nil {
		_ = target.stream.CloseSend()
		target.stream = nil
	}
	if target.cancel != nil {
		target.cancel()
		target.cancel = nil
	}
	if target.conn != nil {
		_ = target.conn.Close()
		target.conn = nil
	}
}

func (session *replicationGRPCSyncSession) executeReplicationTaskGroups(ctx context.Context, result ReplicationResult, groups []replicationTaskGroup) ReplicationResult {
	result.Queued = false
	result.Targets = make([]ReplicationTargetResult, len(groups))
	targetIndexes := make(map[string][]int, len(groups))
	targetOrder := make([]string, 0, len(groups))
	for idx := range groups {
		key := replicationTaskTargetKey(groups[idx].target)
		if _, ok := targetIndexes[key]; !ok {
			targetOrder = append(targetOrder, key)
		}
		targetIndexes[key] = append(targetIndexes[key], idx)
	}
	maxInFlight := session.replicator.maxInFlight
	if maxInFlight <= 1 || len(targetOrder) <= 1 {
		for _, key := range targetOrder {
			for _, idx := range targetIndexes[key] {
				result.Targets[idx] = session.executeReplicationTaskGroup(ctx, groups[idx])
			}
		}
		return result
	}
	if maxInFlight > len(targetOrder) {
		maxInFlight = len(targetOrder)
	}
	jobs := make(chan string)
	var workers sync.WaitGroup
	workers.Add(maxInFlight)
	for worker := 0; worker < maxInFlight; worker++ {
		go func() {
			defer workers.Done()
			for key := range jobs {
				for _, idx := range targetIndexes[key] {
					result.Targets[idx] = session.executeReplicationTaskGroup(ctx, groups[idx])
				}
			}
		}()
	}
	for _, key := range targetOrder {
		jobs <- key
	}
	close(jobs)
	workers.Wait()
	return result
}

func (session *replicationGRPCSyncSession) executeReplicationTaskGroup(ctx context.Context, group replicationTaskGroup) ReplicationTargetResult {
	key := replicationTaskTargetKey(group.target)
	session.mu.Lock()
	useFallback := session.fallback[key]
	session.mu.Unlock()
	if useFallback {
		return session.replicator.executeReplicationTaskGroup(ctx, group)
	}

	startedAt := time.Now()
	result, err := session.sendGroup(group)
	session.replicator.recordReplicationTargetLatency(group.target, time.Since(startedAt))
	if err == nil {
		return result
	}
	if session.replicator.disableHTTPFallback {
		return ReplicationTargetResult{Node: group.target.ID, Address: group.target.GRPCAddress, Error: err.Error()}
	}
	session.markFallback(key)
	return session.replicator.executeReplicationTaskGroup(ctx, group)
}

func (session *replicationGRPCSyncSession) markFallback(key string) {
	session.mu.Lock()
	session.fallback[key] = true
	target := session.targets[key]
	delete(session.targets, key)
	session.mu.Unlock()
	target.close()
}

func (session *replicationGRPCSyncSession) sendGroup(group replicationTaskGroup) (ReplicationTargetResult, error) {
	payloads := group.replicationSyncPayloadBatch()
	if payloads.len() == 0 {
		return ReplicationTargetResult{}, errors.New("hatriecache: gRPC replication stream requires sync payloads")
	}
	target, err := session.streamTarget(group.target)
	if err != nil {
		return ReplicationTargetResult{}, err
	}

	keys := make([]string, payloads.len())
	values := make([][]byte, payloads.len())
	for idx := 0; idx < payloads.len(); idx++ {
		payload := payloads.payload(idx)
		keys[idx] = payload.key
		values[idx] = payload.binaryValue
	}
	target.mu.Lock()
	defer target.mu.Unlock()
	sequence := session.replicator.nextReplicationSequence()
	request := &hatriecachev1.ReplicationStreamBatch{
		Source:              group.metadataSource,
		Sequence:            sequence,
		TopologyFingerprint: group.metadataTopology,
		Keys:                keys,
		BinaryValues:        values,
	}
	timer := time.AfterFunc(session.replicator.timeout, target.cancel)
	err = target.stream.Send(request)
	var ack *hatriecachev1.ReplicationStreamAck
	if err == nil {
		ack, err = target.stream.Recv()
	} else if errors.Is(err, io.EOF) {
		_, err = target.stream.Recv()
	}
	if !timer.Stop() && err == nil {
		err = context.DeadlineExceeded
	}
	if err != nil {
		return ReplicationTargetResult{}, fmt.Errorf("gRPC replication stream: %w", err)
	}
	if ack.GetSequence() != sequence {
		return ReplicationTargetResult{}, fmt.Errorf("gRPC replication stream acknowledged sequence %d, want %d", ack.GetSequence(), sequence)
	}
	if !ack.GetOk() {
		return ReplicationTargetResult{
			Node: group.target.ID, Address: group.target.GRPCAddress, Error: ack.GetMessage(),
		}, nil
	}
	session.replicator.grpcStreamBatches.Add(1)
	return ReplicationTargetResult{
		Node: group.target.ID, Address: group.target.GRPCAddress, OK: true, Status: 200,
	}, nil
}

func (session *replicationGRPCSyncSession) streamTarget(node TopologyNode) (*replicationGRPCStreamTarget, error) {
	key := replicationTaskTargetKey(node)
	session.mu.Lock()
	if target := session.targets[key]; target != nil {
		session.mu.Unlock()
		return target, nil
	}
	session.mu.Unlock()

	address, options, err := session.replicator.replicationGRPCDialOptions(node)
	if err != nil {
		return nil, err
	}
	dialCtx, cancelDial := context.WithTimeout(session.ctx, session.replicator.timeout)
	defer cancelDial()
	conn, err := grpc.DialContext(dialCtx, address, append(options, grpc.WithBlock())...)
	if err != nil {
		return nil, fmt.Errorf("dial gRPC replication target %s: %w", address, err)
	}
	streamCtx, cancelStream := context.WithCancel(session.ctx)
	if token := session.replicator.authToken; token != "" {
		streamCtx = metadata.AppendToOutgoingContext(streamCtx,
			"authorization", "Bearer "+token,
			"x-hatrie-replication-token", token,
		)
	}
	stream, err := hatriecachev1.NewCacheServiceClient(conn).ReplicationStream(streamCtx, grpc.UseCompressor(grpcgzip.Name))
	if err != nil {
		cancelStream()
		_ = conn.Close()
		return nil, fmt.Errorf("open gRPC replication stream: %w", err)
	}
	target := &replicationGRPCStreamTarget{conn: conn, stream: stream, cancel: cancelStream}

	session.mu.Lock()
	if existing := session.targets[key]; existing != nil {
		session.mu.Unlock()
		target.close()
		return existing, nil
	}
	session.targets[key] = target
	session.mu.Unlock()
	return target, nil
}

func (replicator *HTTPReplicator) replicationGRPCDialOptions(node TopologyNode) (string, []grpc.DialOption, error) {
	address := strings.TrimSpace(node.GRPCAddress)
	if address == "" {
		return "", nil, errors.New("hatriecache: replication target has no grpc_address")
	}
	if len(replicator.grpcDialOptions) > 0 {
		return address, append([]grpc.DialOption(nil), replicator.grpcDialOptions...), nil
	}
	if strings.HasPrefix(address, "grpcs://") {
		address = strings.TrimPrefix(address, "grpcs://")
		return address, []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12}))}, nil
	}
	address = strings.TrimPrefix(address, "grpc://")
	return address, []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}, nil
}
