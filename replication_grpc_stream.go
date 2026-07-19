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
	replicator     *HTTPReplicator
	ctx            context.Context
	mu             sync.Mutex
	targets        map[string]*replicationGRPCStreamTarget
	fallback       map[string]bool
	stickyFallback bool
	closed         bool
}

type replicationGRPCStreamTarget struct {
	conn       *grpc.ClientConn
	stream     hatriecachev1.CacheService_ReplicationStreamClient
	ctx        context.Context
	cancel     context.CancelFunc
	jobs       chan *replicationGRPCStreamJob
	done       chan struct{}
	window     int
	timeout    time.Duration
	replicator *HTTPReplicator
	closeOnce  sync.Once
}

type replicationGRPCStreamJob struct {
	ctx     context.Context
	request *hatriecachev1.ReplicationStreamBatch
	result  chan replicationGRPCStreamJobResult
}

type replicationGRPCStreamJobResult struct {
	ack *hatriecachev1.ReplicationStreamAck
	err error
}

func newReplicationGRPCSyncSession(ctx context.Context, replicator *HTTPReplicator) *replicationGRPCSyncSession {
	return newReplicationGRPCSession(ctx, replicator, true)
}

func newReplicationGRPCLiveSession(ctx context.Context, replicator *HTTPReplicator) *replicationGRPCSyncSession {
	return newReplicationGRPCSession(ctx, replicator, false)
}

func newReplicationGRPCSession(ctx context.Context, replicator *HTTPReplicator, stickyFallback bool) *replicationGRPCSyncSession {
	if ctx == nil {
		ctx = context.Background()
	}
	return &replicationGRPCSyncSession{
		replicator:     replicator,
		ctx:            ctx,
		targets:        make(map[string]*replicationGRPCStreamTarget),
		fallback:       make(map[string]bool),
		stickyFallback: stickyFallback,
	}
}

func (session *replicationGRPCSyncSession) close() {
	if session == nil {
		return
	}
	session.mu.Lock()
	session.closed = true
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
	target.closeOnce.Do(func() {
		target.cancel()
		<-target.done
	})
}

func (target *replicationGRPCStreamTarget) run() {
	defer close(target.done)
	defer target.conn.Close()
	defer target.stream.CloseSend()
	defer target.cancel()
	receives := make(chan replicationGRPCStreamJobResult, 1)
	go func() {
		for {
			ack, err := target.stream.Recv()
			select {
			case receives <- replicationGRPCStreamJobResult{ack: ack, err: err}:
			case <-target.ctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()

	pending := make(map[uint64]*replicationGRPCStreamJob, target.window)
	timer := time.NewTimer(time.Hour)
	if !timer.Stop() {
		<-timer.C
	}
	defer timer.Stop()
	var timeout <-chan time.Time
	terminalErr := context.Canceled
	defer func() {
		for _, job := range pending {
			target.completeJob(job, nil, terminalErr)
		}
		target.failQueued(terminalErr)
	}()

	for {
		var jobs <-chan *replicationGRPCStreamJob
		if len(pending) < target.window {
			jobs = target.jobs
		}
		select {
		case <-target.ctx.Done():
			terminalErr = target.ctx.Err()
			return
		case <-timeout:
			terminalErr = context.DeadlineExceeded
			return
		case job := <-jobs:
			if err := job.ctx.Err(); err != nil {
				target.completeJob(job, nil, err)
				continue
			}
			sequence := target.replicator.nextReplicationSequence()
			job.request.Sequence = sequence
			pending[sequence] = job
			if err := target.stream.Send(job.request); err != nil {
				terminalErr = target.resolveSendError(err, receives)
				return
			}
			if len(pending) == 1 {
				target.resetTimer(timer)
				timeout = timer.C
			}
		case received := <-receives:
			if received.err != nil {
				terminalErr = received.err
				return
			}
			sequence := received.ack.GetSequence()
			job := pending[sequence]
			if job == nil {
				terminalErr = fmt.Errorf("gRPC replication stream acknowledged unknown sequence %d", sequence)
				return
			}
			delete(pending, sequence)
			target.completeJob(job, received.ack, nil)
			if len(pending) == 0 {
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timeout = nil
			} else {
				target.resetTimer(timer)
				timeout = timer.C
			}
		}
	}
}

func (target *replicationGRPCStreamTarget) resetTimer(timer *time.Timer) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(target.timeout)
}

func (target *replicationGRPCStreamTarget) resolveSendError(err error, receives <-chan replicationGRPCStreamJobResult) error {
	if !errors.Is(err, io.EOF) {
		return err
	}
	timer := time.NewTimer(target.timeout)
	defer timer.Stop()
	select {
	case received := <-receives:
		if received.err != nil {
			return received.err
		}
	case <-timer.C:
	case <-target.ctx.Done():
		return target.ctx.Err()
	}
	return err
}

func (target *replicationGRPCStreamTarget) completeJob(job *replicationGRPCStreamJob, ack *hatriecachev1.ReplicationStreamAck, err error) {
	select {
	case job.result <- replicationGRPCStreamJobResult{ack: ack, err: err}:
	default:
	}
}

func (target *replicationGRPCStreamTarget) failQueued(err error) {
	for {
		select {
		case job := <-target.jobs:
			job.result <- replicationGRPCStreamJobResult{err: err}
		default:
			return
		}
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
		return session.replicator.executeReplicationTaskGroupHTTP(ctx, group)
	}

	startedAt := time.Now()
	result, err := session.sendGroup(ctx, group)
	session.replicator.recordReplicationTargetLatency(group.target, time.Since(startedAt))
	if err == nil {
		return result
	}
	if session.replicator.disableHTTPFallback {
		return ReplicationTargetResult{Node: group.target.ID, Address: group.target.GRPCAddress, Error: err.Error()}
	}
	session.markFallback(key)
	return session.replicator.executeReplicationTaskGroupHTTP(ctx, group)
}

func (session *replicationGRPCSyncSession) markFallback(key string) {
	session.mu.Lock()
	if session.stickyFallback {
		session.fallback[key] = true
	}
	target := session.targets[key]
	delete(session.targets, key)
	session.mu.Unlock()
	target.close()
}

func (session *replicationGRPCSyncSession) sendGroup(ctx context.Context, group replicationTaskGroup) (ReplicationTargetResult, error) {
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
	request := &hatriecachev1.ReplicationStreamBatch{
		Source:              group.metadataSource,
		TopologyFingerprint: group.metadataTopology,
		Keys:                keys,
		BinaryValues:        values,
	}
	jobCtx, cancelJob := context.WithTimeout(replicationContext(ctx), session.replicator.timeout)
	defer cancelJob()
	job := &replicationGRPCStreamJob{
		ctx:     jobCtx,
		request: request,
		result:  make(chan replicationGRPCStreamJobResult, 1),
	}
	select {
	case target.jobs <- job:
	case <-session.ctx.Done():
		return ReplicationTargetResult{}, session.ctx.Err()
	case <-target.done:
		return ReplicationTargetResult{}, errors.New("gRPC replication stream target is closed")
	case <-jobCtx.Done():
		return ReplicationTargetResult{}, jobCtx.Err()
	}
	var jobResult replicationGRPCStreamJobResult
	select {
	case jobResult = <-job.result:
	case <-session.ctx.Done():
		return ReplicationTargetResult{}, session.ctx.Err()
	case <-target.done:
		select {
		case jobResult = <-job.result:
		default:
			return ReplicationTargetResult{}, errors.New("gRPC replication stream target closed before acknowledgement")
		}
	case <-jobCtx.Done():
		return ReplicationTargetResult{}, jobCtx.Err()
	}
	ack, err := jobResult.ack, jobResult.err
	if err != nil {
		return ReplicationTargetResult{}, fmt.Errorf("gRPC replication stream: %w", err)
	}
	if ack.GetSequence() != request.GetSequence() {
		return ReplicationTargetResult{}, fmt.Errorf("gRPC replication stream acknowledged sequence %d, want %d", ack.GetSequence(), request.GetSequence())
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
	if session.closed {
		session.mu.Unlock()
		return nil, errors.New("hatriecache: gRPC replication session is closed")
	}
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
	window := session.replicator.grpcStreamWindow
	if window <= 0 {
		window = 1
	}
	target := &replicationGRPCStreamTarget{
		conn: conn, stream: stream, ctx: streamCtx, cancel: cancelStream,
		jobs: make(chan *replicationGRPCStreamJob, window*2), done: make(chan struct{}),
		window: window, timeout: session.replicator.timeout, replicator: session.replicator,
	}
	go target.run()

	session.mu.Lock()
	if session.closed {
		session.mu.Unlock()
		target.close()
		return nil, errors.New("hatriecache: gRPC replication session is closed")
	}
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
