package hatTopology

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"sort"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"

	"github.com/cespare/xxhash/v2"
)

const (
	VShardBucketMapVersion            uint16 = 1
	VShardMigrationCheckpointVersion  uint16 = 1
	DefaultVShardBucketCount          uint32 = 256
	MaxVShardBucketCount              uint32 = 1 << 20
	MaxVShardNodes                           = 1024
	MaxVShardReplicationFactor               = 16
	MaxVShardNodeIDBytes                     = 256
	MaxVShardMigrationBatchRecords           = 65536
	MaxVShardMigrationRecordBytes            = 16 << 20
	MaxVShardMigrationFailureBytes           = 1024
	MaxVShardMigrationCheckpointBytes        = 4096
)

var (
	ErrVShardBucketMapInvalid       = errors.New("hatriecache: invalid vshard bucket map")
	ErrVShardNodeInvalid            = errors.New("hatriecache: invalid vshard node")
	ErrVShardMigrationNil           = errors.New("hatriecache: nil vshard migration")
	ErrVShardMigrationInvalid       = errors.New("hatriecache: invalid vshard migration")
	ErrVShardMigrationPhase         = errors.New("hatriecache: invalid vshard migration phase")
	ErrVShardMigrationContext       = errors.New("hatriecache: vshard migration context is required")
	ErrVShardMigrationCallback      = errors.New("hatriecache: vshard migration callback is required")
	ErrVShardMigrationFenced        = errors.New("hatriecache: vshard migration fencing token is stale")
	ErrVShardSequenceGap            = errors.New("hatriecache: vshard migration WAL sequence gap")
	ErrVShardSequenceRange          = errors.New("hatriecache: vshard migration WAL sequence is out of range")
	ErrVShardMigrationFailed        = errors.New("hatriecache: vshard migration failed")
	ErrVShardMigrationCheckpoint    = errors.New("hatriecache: invalid vshard migration checkpoint")
	ErrVShardMigrationCheckpointCRC = errors.New("hatriecache: vshard migration checkpoint checksum mismatch")
)

// VShardNode is a stable node identity used by the opt-in virtual-bucket map.
// Node order does not affect the generated map.
type VShardNode struct {
	ID string
}

// VShardBucketMapOptions controls construction of an immutable virtual-bucket
// ownership map. Zero bucket count, replication factor, generation, and fence
// use conservative defaults.
type VShardBucketMapOptions struct {
	BucketCount       uint32
	ReplicationFactor int
	Generation        uint64
	FencingToken      uint64
	Nodes             []VShardNode
}

// VShardBucketMap is an immutable, compact ownership map. Each bucket stores
// node indexes rather than repeated node strings, so steady-state routing does
// not allocate and map memory is proportional to bucket count times replicas.
type VShardBucketMap struct {
	bucketCount       uint32
	replicationFactor uint8
	generation        uint64
	fencingToken      uint64
	nodes             []string
	owners            []uint16
}

// VShardRoute is the allocation-free result of routing one key.
type VShardRoute struct {
	Bucket       uint32
	Primary      string
	generation   uint64
	fencingToken uint64
	nodes        []string
	owners       []uint16
}

// Generation returns the map generation that produced the route.
func (route VShardRoute) Generation() uint64 { return route.generation }

// FencingToken returns the map fencing token that authorizes the route.
func (route VShardRoute) FencingToken() uint64 { return route.fencingToken }

// ReplicaCount returns the number of replicas after the primary.
func (route VShardRoute) ReplicaCount() int {
	if len(route.owners) == 0 {
		return 0
	}
	return len(route.owners) - 1
}

// Replica returns one replica by zero-based replica index.
func (route VShardRoute) Replica(index int) (string, bool) {
	if index < 0 || index+1 >= len(route.owners) {
		return "", false
	}
	return route.nodes[route.owners[index+1]], true
}

// Owners returns the primary followed by replicas as an independent slice.
func (route VShardRoute) Owners() []string {
	if len(route.owners) == 0 {
		return nil
	}
	owners := make([]string, len(route.owners))
	for index, nodeIndex := range route.owners {
		owners[index] = route.nodes[nodeIndex]
	}
	return owners
}

// VShardBucketOwnership is a detached ownership snapshot for one bucket.
type VShardBucketOwnership struct {
	Bucket       uint32
	Primary      string
	Replicas     []string
	Generation   uint64
	FencingToken uint64
}

// Owners returns the primary followed by replicas as an independent slice.
func (ownership VShardBucketOwnership) Owners() []string {
	owners := make([]string, 0, 1+len(ownership.Replicas))
	if ownership.Primary != "" {
		owners = append(owners, ownership.Primary)
	}
	return append(owners, ownership.Replicas...)
}

// ReplicasEqual compares only the replica order and values.
func (ownership VShardBucketOwnership) ReplicasEqual(other VShardBucketOwnership) bool {
	if len(ownership.Replicas) != len(other.Replicas) {
		return false
	}
	for index := range ownership.Replicas {
		if ownership.Replicas[index] != other.Replicas[index] {
			return false
		}
	}
	return true
}

// NewVShardBucketMap builds a deterministic immutable map using rendezvous
// ownership over a fixed virtual-bucket space. Adding or removing a node only
// changes buckets for which the node wins an owner slot.
func NewVShardBucketMap(options VShardBucketMapOptions) (VShardBucketMap, error) {
	bucketCount := options.BucketCount
	if bucketCount == 0 {
		bucketCount = DefaultVShardBucketCount
	}
	if bucketCount > MaxVShardBucketCount || bucketCount&(bucketCount-1) != 0 {
		return VShardBucketMap{}, fmt.Errorf("%w: bucket count %d must be a power of two up to %d", ErrVShardBucketMapInvalid, bucketCount, MaxVShardBucketCount)
	}
	replicationFactor := options.ReplicationFactor
	if replicationFactor == 0 {
		replicationFactor = 1
	}
	if replicationFactor < 1 || replicationFactor > MaxVShardReplicationFactor {
		return VShardBucketMap{}, fmt.Errorf("%w: replication factor %d is out of range", ErrVShardBucketMapInvalid, replicationFactor)
	}
	generation := options.Generation
	if generation == 0 {
		generation = 1
	}
	fencingToken := options.FencingToken
	if fencingToken == 0 {
		fencingToken = 1
	}
	nodes, err := normalizeVShardNodes(options.Nodes)
	if err != nil {
		return VShardBucketMap{}, err
	}
	if replicationFactor > len(nodes) {
		return VShardBucketMap{}, fmt.Errorf("%w: replication factor %d exceeds node count %d", ErrVShardBucketMapInvalid, replicationFactor, len(nodes))
	}
	owners := makeVShardOwners(bucketCount, replicationFactor, nodes)
	return VShardBucketMap{
		bucketCount:       bucketCount,
		replicationFactor: uint8(replicationFactor),
		generation:        generation,
		fencingToken:      fencingToken,
		nodes:             nodes,
		owners:            owners,
	}, nil
}

// BucketCount returns the number of virtual buckets.
func (mapping VShardBucketMap) BucketCount() uint32 { return mapping.bucketCount }

// ReplicationFactor returns the number of owners per bucket.
func (mapping VShardBucketMap) ReplicationFactor() int { return int(mapping.replicationFactor) }

// Generation returns the topology generation represented by the map.
func (mapping VShardBucketMap) Generation() uint64 { return mapping.generation }

// FencingToken returns the write fence represented by the map.
func (mapping VShardBucketMap) FencingToken() uint64 { return mapping.fencingToken }

// Nodes returns an independent node list in canonical order.
func (mapping VShardBucketMap) Nodes() []VShardNode {
	nodes := make([]VShardNode, len(mapping.nodes))
	for index, node := range mapping.nodes {
		nodes[index] = VShardNode{ID: node}
	}
	return nodes
}

// RouteKey returns the owner set for key without allocating.
func (mapping VShardBucketMap) RouteKey(key string) (VShardRoute, bool) {
	if !mapping.validFast() {
		return VShardRoute{}, false
	}
	bucket := uint32(xxhash.Sum64String(key) & uint64(mapping.bucketCount-1))
	return mapping.routeBucket(bucket), true
}

// RouteBucket returns the owner set for a virtual bucket.
func (mapping VShardBucketMap) RouteBucket(bucket uint32) (VShardRoute, bool) {
	if !mapping.validFast() || bucket >= mapping.bucketCount {
		return VShardRoute{}, false
	}
	return mapping.routeBucket(bucket), true
}

// BucketOwnership returns a detached snapshot suitable for migration plans.
func (mapping VShardBucketMap) BucketOwnership(bucket uint32) (VShardBucketOwnership, bool) {
	if !mapping.validFast() || bucket >= mapping.bucketCount {
		return VShardBucketOwnership{}, false
	}
	return mapping.bucketOwnershipUnchecked(bucket), true
}

func (mapping VShardBucketMap) bucketOwnershipUnchecked(bucket uint32) VShardBucketOwnership {
	start := int(bucket) * int(mapping.replicationFactor)
	owners := mapping.owners[start : start+int(mapping.replicationFactor)]
	replicas := make([]string, len(owners)-1)
	for index := 1; index < len(owners); index++ {
		replicas[index-1] = mapping.nodes[owners[index]]
	}
	return VShardBucketOwnership{
		Bucket:       bucket,
		Primary:      mapping.nodes[owners[0]],
		Replicas:     replicas,
		Generation:   mapping.generation,
		FencingToken: mapping.fencingToken,
	}
}

func (mapping VShardBucketMap) routeBucket(bucket uint32) VShardRoute {
	start := int(bucket) * int(mapping.replicationFactor)
	end := start + int(mapping.replicationFactor)
	owners := mapping.owners[start:end]
	return VShardRoute{
		Bucket:       bucket,
		Primary:      mapping.nodes[owners[0]],
		generation:   mapping.generation,
		fencingToken: mapping.fencingToken,
		nodes:        mapping.nodes,
		owners:       owners,
	}
}

// VShardRebalancePlan is an immutable transition from one bucket map to the
// next. Only buckets whose owner set changes are included in Moves.
type VShardRebalancePlan struct {
	source VShardBucketMap
	target VShardBucketMap
	moves  []VShardBucketMove
}

// VShardBucketMove describes one bucket's source and target ownership.
type VShardBucketMove struct {
	Bucket uint32
	Source VShardBucketOwnership
	Target VShardBucketOwnership
}

// PlanRebalance builds a deterministic ownership transition. Generation and
// fence must advance, preventing stale plans from becoming active.
func (mapping VShardBucketMap) PlanRebalance(nodes []VShardNode, generation, fencingToken uint64) (VShardRebalancePlan, error) {
	if err := mapping.validate(); err != nil {
		return VShardRebalancePlan{}, err
	}
	if generation <= mapping.generation {
		return VShardRebalancePlan{}, fmt.Errorf("%w: generation %d is not newer than %d", ErrVShardBucketMapInvalid, generation, mapping.generation)
	}
	if fencingToken <= mapping.fencingToken {
		return VShardRebalancePlan{}, fmt.Errorf("%w: fencing token %d is not newer than %d", ErrVShardBucketMapInvalid, fencingToken, mapping.fencingToken)
	}
	target, err := NewVShardBucketMap(VShardBucketMapOptions{
		BucketCount:       mapping.bucketCount,
		ReplicationFactor: int(mapping.replicationFactor),
		Generation:        generation,
		FencingToken:      fencingToken,
		Nodes:             nodes,
	})
	if err != nil {
		return VShardRebalancePlan{}, err
	}
	moves := make([]VShardBucketMove, 0, target.bucketCount/4)
	for bucket := uint32(0); bucket < mapping.bucketCount; bucket++ {
		if vShardBucketOwnersEqual(mapping, target, bucket) {
			continue
		}
		source := mapping.bucketOwnershipUnchecked(bucket)
		destination := target.bucketOwnershipUnchecked(bucket)
		moves = append(moves, VShardBucketMove{Bucket: bucket, Source: source, Target: destination})
	}
	return VShardRebalancePlan{source: mapping, target: target, moves: moves}, nil
}

// Source returns the old map.
func (plan VShardRebalancePlan) Source() VShardBucketMap { return plan.source }

// Target returns the new map to activate after migrations complete.
func (plan VShardRebalancePlan) Target() VShardBucketMap { return plan.target }

// MoveCount returns the number of buckets requiring migration.
func (plan VShardRebalancePlan) MoveCount() int { return len(plan.moves) }

// Moves returns detached migration descriptions.
func (plan VShardRebalancePlan) Moves() []VShardBucketMove {
	if len(plan.moves) == 0 {
		return nil
	}
	moves := make([]VShardBucketMove, len(plan.moves))
	for index, move := range plan.moves {
		moves[index] = VShardBucketMove{
			Bucket: move.Bucket,
			Source: cloneVShardOwnership(move.Source),
			Target: cloneVShardOwnership(move.Target),
		}
	}
	return moves
}

// VShardMigrationPhase is the fenced snapshot/WAL handoff state.
type VShardMigrationPhase string

const (
	VShardMigrationPending          VShardMigrationPhase = "pending"
	VShardMigrationSnapshotApplying VShardMigrationPhase = "snapshot-applying"
	VShardMigrationSnapshotApplied  VShardMigrationPhase = "snapshot-applied"
	VShardMigrationWALApplying      VShardMigrationPhase = "wal-applying"
	VShardMigrationReady            VShardMigrationPhase = "ready"
	VShardMigrationActivating       VShardMigrationPhase = "activating"
	VShardMigrationActive           VShardMigrationPhase = "active"
	VShardMigrationAborted          VShardMigrationPhase = "aborted"
	VShardMigrationFailed           VShardMigrationPhase = "failed"
)

// VShardMigrationOptions defines the backup snapshot boundary and final WAL
// sequence that the target must receive before activation.
type VShardMigrationOptions struct {
	SnapshotSequence uint64
	WALLastSequence  uint64
}

// VShardWALRecord is a bounded caller-owned journal record copied before a
// migration callback runs.
type VShardWALRecord struct {
	Sequence uint64
	Payload  []byte
}

// VShardMigrationStatus is a detached progress snapshot.
type VShardMigrationStatus struct {
	Phase            VShardMigrationPhase
	Bucket           uint32
	SourcePrimary    string
	TargetPrimary    string
	FencingToken     uint64
	SnapshotSequence uint64
	AppliedSequence  uint64
	WALLastSequence  uint64
	SnapshotChecksum [32]byte
	Failure          string
}

// VShardMigration coordinates caller-owned snapshot, WAL, and ownership
// activation callbacks. The callback runs without the state lock and a
// failed or ambiguous callback fences the migration permanently.
type VShardMigration struct {
	mu               sync.Mutex
	move             VShardBucketMove
	phase            VShardMigrationPhase
	snapshotSequence uint64
	appliedSequence  uint64
	walLastSequence  uint64
	snapshotChecksum [32]byte
	failure          string
}

// NewVShardMigration creates a migration for one planned bucket move.
func NewVShardMigration(move VShardBucketMove, options VShardMigrationOptions) (*VShardMigration, error) {
	if move.Source.Bucket != move.Bucket || move.Target.Bucket != move.Bucket || validateVShardOwnership(move.Source) != nil || validateVShardOwnership(move.Target) != nil || move.Target.FencingToken == 0 {
		return nil, fmt.Errorf("%w: source, target, bucket, and fencing token are invalid", ErrVShardMigrationInvalid)
	}
	if options.WALLastSequence < options.SnapshotSequence {
		return nil, fmt.Errorf("%w: WAL sequence %d precedes snapshot sequence %d", ErrVShardMigrationInvalid, options.WALLastSequence, options.SnapshotSequence)
	}
	return &VShardMigration{
		move:             VShardBucketMove{Bucket: move.Bucket, Source: cloneVShardOwnership(move.Source), Target: cloneVShardOwnership(move.Target)},
		phase:            VShardMigrationPending,
		snapshotSequence: options.SnapshotSequence,
		walLastSequence:  options.WALLastSequence,
	}, nil
}

// ApplySnapshot validates and installs the snapshot boundary.
func (migration *VShardMigration) ApplySnapshot(ctx context.Context, checksum [32]byte, apply func() error) error {
	if migration == nil {
		return ErrVShardMigrationNil
	}
	if ctx == nil {
		return ErrVShardMigrationContext
	}
	if apply == nil {
		return ErrVShardMigrationCallback
	}
	migration.mu.Lock()
	if migration.phase != VShardMigrationPending {
		err := fmt.Errorf("%w: snapshot cannot start from %s", ErrVShardMigrationPhase, migration.phase)
		migration.mu.Unlock()
		return err
	}
	migration.phase = VShardMigrationSnapshotApplying
	migration.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return migration.fail(err)
	}
	if err := apply(); err != nil {
		return migration.fail(err)
	}
	if err := ctx.Err(); err != nil {
		return migration.fail(err)
	}
	migration.mu.Lock()
	defer migration.mu.Unlock()
	if migration.phase != VShardMigrationSnapshotApplying {
		return fmt.Errorf("%w: snapshot callback completed after phase changed", ErrVShardMigrationPhase)
	}
	migration.snapshotChecksum = checksum
	migration.appliedSequence = migration.snapshotSequence
	if migration.snapshotSequence == migration.walLastSequence {
		migration.phase = VShardMigrationReady
	} else {
		migration.phase = VShardMigrationSnapshotApplied
	}
	return nil
}

// ApplyWAL applies one contiguous bounded batch after the snapshot.
func (migration *VShardMigration) ApplyWAL(ctx context.Context, records []VShardWALRecord, apply func([]VShardWALRecord) error) error {
	if migration == nil {
		return ErrVShardMigrationNil
	}
	if ctx == nil {
		return ErrVShardMigrationContext
	}
	migration.mu.Lock()
	if migration.phase != VShardMigrationSnapshotApplied {
		err := fmt.Errorf("%w: WAL cannot start from %s", ErrVShardMigrationPhase, migration.phase)
		migration.mu.Unlock()
		return err
	}
	if len(records) == 0 {
		if migration.appliedSequence != migration.walLastSequence {
			err := fmt.Errorf("%w: empty batch before final sequence", ErrVShardSequenceGap)
			migration.mu.Unlock()
			return err
		}
		migration.phase = VShardMigrationReady
		migration.mu.Unlock()
		return nil
	}
	if apply == nil {
		migration.mu.Unlock()
		return ErrVShardMigrationCallback
	}
	if len(records) > MaxVShardMigrationBatchRecords {
		err := fmt.Errorf("%w: batch contains %d records", ErrVShardMigrationInvalid, len(records))
		migration.mu.Unlock()
		return err
	}
	expected := migration.appliedSequence + 1
	if migration.appliedSequence == ^uint64(0) {
		err := ErrVShardSequenceRange
		migration.mu.Unlock()
		return err
	}
	copied := make([]VShardWALRecord, len(records))
	for index, record := range records {
		if record.Sequence != expected {
			err := fmt.Errorf("%w: expected %d, got %d", ErrVShardSequenceGap, expected, record.Sequence)
			migration.mu.Unlock()
			return err
		}
		if record.Sequence > migration.walLastSequence {
			err := fmt.Errorf("%w: sequence %d exceeds final %d", ErrVShardSequenceRange, record.Sequence, migration.walLastSequence)
			migration.mu.Unlock()
			return err
		}
		if len(record.Payload) > MaxVShardMigrationRecordBytes {
			err := fmt.Errorf("%w: record %d exceeds %d bytes", ErrVShardMigrationInvalid, record.Sequence, MaxVShardMigrationRecordBytes)
			migration.mu.Unlock()
			return err
		}
		copied[index] = VShardWALRecord{Sequence: record.Sequence, Payload: append([]byte(nil), record.Payload...)}
		expected++
	}
	migration.phase = VShardMigrationWALApplying
	migration.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return migration.fail(err)
	}
	if err := apply(copied); err != nil {
		return migration.fail(err)
	}
	if err := ctx.Err(); err != nil {
		return migration.fail(err)
	}
	migration.mu.Lock()
	defer migration.mu.Unlock()
	if migration.phase != VShardMigrationWALApplying {
		return fmt.Errorf("%w: WAL callback completed after phase changed", ErrVShardMigrationPhase)
	}
	migration.appliedSequence = copied[len(copied)-1].Sequence
	if migration.appliedSequence == migration.walLastSequence {
		migration.phase = VShardMigrationReady
	} else {
		migration.phase = VShardMigrationSnapshotApplied
	}
	return nil
}

// Activate atomically hands ownership to the target through the caller's
// callback after verifying the target fencing token.
func (migration *VShardMigration) Activate(ctx context.Context, fencingToken uint64, activate func() error) error {
	if migration == nil {
		return ErrVShardMigrationNil
	}
	if ctx == nil {
		return ErrVShardMigrationContext
	}
	if activate == nil {
		return ErrVShardMigrationCallback
	}
	migration.mu.Lock()
	if migration.phase != VShardMigrationReady {
		err := fmt.Errorf("%w: activation cannot start from %s", ErrVShardMigrationPhase, migration.phase)
		migration.mu.Unlock()
		return err
	}
	if fencingToken != migration.move.Target.FencingToken {
		migration.mu.Unlock()
		return ErrVShardMigrationFenced
	}
	migration.phase = VShardMigrationActivating
	migration.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return migration.fail(err)
	}
	if err := activate(); err != nil {
		return migration.fail(err)
	}
	if err := ctx.Err(); err != nil {
		return migration.fail(err)
	}
	migration.mu.Lock()
	defer migration.mu.Unlock()
	if migration.phase != VShardMigrationActivating {
		return fmt.Errorf("%w: activation callback completed after phase changed", ErrVShardMigrationPhase)
	}
	migration.phase = VShardMigrationActive
	return nil
}

// Abort prevents a pending or in-flight migration from becoming active.
func (migration *VShardMigration) Abort(reason error) {
	if migration == nil {
		return
	}
	migration.mu.Lock()
	defer migration.mu.Unlock()
	if migration.phase == VShardMigrationActive || migration.phase == VShardMigrationAborted || migration.phase == VShardMigrationFailed {
		return
	}
	migration.phase = VShardMigrationAborted
	if reason == nil {
		migration.failure = "aborted"
	} else {
		migration.failure = boundedVShardFailure(reason)
	}
}

// Status returns a detached migration status.
func (migration *VShardMigration) Status() VShardMigrationStatus {
	if migration == nil {
		return VShardMigrationStatus{Phase: VShardMigrationFailed, Failure: ErrVShardMigrationNil.Error()}
	}
	migration.mu.Lock()
	defer migration.mu.Unlock()
	return migration.statusLocked()
}

func (migration *VShardMigration) statusLocked() VShardMigrationStatus {
	return VShardMigrationStatus{
		Phase:            migration.phase,
		Bucket:           migration.move.Bucket,
		SourcePrimary:    migration.move.Source.Primary,
		TargetPrimary:    migration.move.Target.Primary,
		FencingToken:     migration.move.Target.FencingToken,
		SnapshotSequence: migration.snapshotSequence,
		AppliedSequence:  migration.appliedSequence,
		WALLastSequence:  migration.walLastSequence,
		SnapshotChecksum: migration.snapshotChecksum,
		Failure:          migration.failure,
	}
}

func (migration *VShardMigration) fail(err error) error {
	if err == nil {
		err = ErrVShardMigrationFailed
	}
	migration.mu.Lock()
	defer migration.mu.Unlock()
	if migration.phase != VShardMigrationActive && migration.phase != VShardMigrationAborted {
		migration.phase = VShardMigrationFailed
		migration.failure = boundedVShardFailure(err)
	}
	return err
}

// VShardMigrationCheckpoint is a bounded durable resume record.
type VShardMigrationCheckpoint struct {
	Version          uint16
	Bucket           uint32
	SourcePrimary    string
	SourceReplicas   []string
	TargetPrimary    string
	TargetReplicas   []string
	FencingToken     uint64
	SnapshotSequence uint64
	AppliedSequence  uint64
	WALLastSequence  uint64
	Phase            VShardMigrationPhase
	SnapshotChecksum [32]byte
	Failure          string
}

// Checkpoint returns a detached durable resume record.
func (migration *VShardMigration) Checkpoint() VShardMigrationCheckpoint {
	if migration == nil {
		return VShardMigrationCheckpoint{Version: VShardMigrationCheckpointVersion, Phase: VShardMigrationFailed, Failure: ErrVShardMigrationNil.Error()}
	}
	migration.mu.Lock()
	defer migration.mu.Unlock()
	return VShardMigrationCheckpoint{
		Version:          VShardMigrationCheckpointVersion,
		Bucket:           migration.move.Bucket,
		SourcePrimary:    migration.move.Source.Primary,
		SourceReplicas:   append([]string(nil), migration.move.Source.Replicas...),
		TargetPrimary:    migration.move.Target.Primary,
		TargetReplicas:   append([]string(nil), migration.move.Target.Replicas...),
		FencingToken:     migration.move.Target.FencingToken,
		SnapshotSequence: migration.snapshotSequence,
		AppliedSequence:  migration.appliedSequence,
		WALLastSequence:  migration.walLastSequence,
		Phase:            migration.phase,
		SnapshotChecksum: migration.snapshotChecksum,
		Failure:          migration.failure,
	}
}

// NewVShardMigrationFromCheckpoint resumes a non-callback phase from a
// checkpoint after verifying that the move identity and fence still match.
func NewVShardMigrationFromCheckpoint(move VShardBucketMove, checkpoint VShardMigrationCheckpoint) (*VShardMigration, error) {
	if err := validateVShardCheckpoint(checkpoint); err != nil {
		return nil, err
	}
	if checkpoint.Bucket != move.Bucket || checkpoint.SourcePrimary != move.Source.Primary || checkpoint.TargetPrimary != move.Target.Primary || checkpoint.FencingToken != move.Target.FencingToken || !stringSlicesEqual(checkpoint.SourceReplicas, move.Source.Replicas) || !stringSlicesEqual(checkpoint.TargetReplicas, move.Target.Replicas) {
		return nil, fmt.Errorf("%w: checkpoint does not match migration move", ErrVShardMigrationCheckpoint)
	}
	if checkpoint.Phase == VShardMigrationSnapshotApplying || checkpoint.Phase == VShardMigrationWALApplying || checkpoint.Phase == VShardMigrationActivating {
		return nil, fmt.Errorf("%w: in-flight callback phase requires operator resolution", ErrVShardMigrationCheckpoint)
	}
	migration, err := NewVShardMigration(move, VShardMigrationOptions{SnapshotSequence: checkpoint.SnapshotSequence, WALLastSequence: checkpoint.WALLastSequence})
	if err != nil {
		return nil, err
	}
	migration.appliedSequence = checkpoint.AppliedSequence
	migration.phase = checkpoint.Phase
	migration.snapshotChecksum = checkpoint.SnapshotChecksum
	migration.failure = checkpoint.Failure
	return migration, nil
}

var vShardCheckpointCRC = crc32.MakeTable(crc32.Castagnoli)

var vShardCheckpointMagic = [4]byte{'V', 'S', 'M', '1'}

// MarshalBinary encodes a bounded CRC32C-protected checkpoint.
func (checkpoint VShardMigrationCheckpoint) MarshalBinary() ([]byte, error) {
	if checkpoint.Version == 0 {
		checkpoint.Version = VShardMigrationCheckpointVersion
	}
	if err := validateVShardCheckpoint(checkpoint); err != nil {
		return nil, err
	}
	encoded := make([]byte, 0, 256)
	encoded = append(encoded, vShardCheckpointMagic[:]...)
	encoded = appendVShardU16(encoded, checkpoint.Version)
	phase, _ := vShardPhaseCode(checkpoint.Phase)
	encoded = append(encoded, phase)
	encoded = appendVShardU32(encoded, checkpoint.Bucket)
	encoded = appendVShardU64(encoded, checkpoint.FencingToken)
	encoded = appendVShardU64(encoded, checkpoint.SnapshotSequence)
	encoded = appendVShardU64(encoded, checkpoint.AppliedSequence)
	encoded = appendVShardU64(encoded, checkpoint.WALLastSequence)
	encoded = appendVShardString(encoded, checkpoint.SourcePrimary)
	encoded = appendVShardString(encoded, checkpoint.TargetPrimary)
	encoded = binary.AppendUvarint(encoded, uint64(len(checkpoint.SourceReplicas)))
	for _, replica := range checkpoint.SourceReplicas {
		encoded = appendVShardString(encoded, replica)
	}
	encoded = binary.AppendUvarint(encoded, uint64(len(checkpoint.TargetReplicas)))
	for _, replica := range checkpoint.TargetReplicas {
		encoded = appendVShardString(encoded, replica)
	}
	encoded = append(encoded, checkpoint.SnapshotChecksum[:]...)
	encoded = appendVShardString(encoded, checkpoint.Failure)
	if len(encoded)+4 > MaxVShardMigrationCheckpointBytes {
		return nil, fmt.Errorf("%w: encoded size exceeds %d bytes", ErrVShardMigrationCheckpoint, MaxVShardMigrationCheckpointBytes)
	}
	return appendVShardU32(encoded, crc32.Checksum(encoded, vShardCheckpointCRC)), nil
}

// UnmarshalBinary decodes and validates a checkpoint.
func (checkpoint *VShardMigrationCheckpoint) UnmarshalBinary(payload []byte) error {
	if checkpoint == nil {
		return ErrVShardMigrationCheckpoint
	}
	if len(payload) < 4+2+1+4+8+8+8+8+32+4 || len(payload) > MaxVShardMigrationCheckpointBytes || !equalVShardBytes(payload[:4], vShardCheckpointMagic[:]) {
		return ErrVShardMigrationCheckpoint
	}
	provided := binary.LittleEndian.Uint32(payload[len(payload)-4:])
	if crc32.Checksum(payload[:len(payload)-4], vShardCheckpointCRC) != provided {
		return ErrVShardMigrationCheckpointCRC
	}
	limit := len(payload) - 4
	offset := 4
	version, ok := readVShardU16(payload, &offset, limit)
	if !ok {
		return ErrVShardMigrationCheckpoint
	}
	if offset >= limit {
		return ErrVShardMigrationCheckpoint
	}
	phase, ok := vShardPhaseFromCode(payload[offset])
	if !ok {
		return ErrVShardMigrationCheckpoint
	}
	offset++
	bucket, ok := readVShardU32(payload, &offset, limit)
	if !ok {
		return ErrVShardMigrationCheckpoint
	}
	fence, ok := readVShardU64(payload, &offset, limit)
	if !ok {
		return ErrVShardMigrationCheckpoint
	}
	snapshot, ok := readVShardU64(payload, &offset, limit)
	if !ok {
		return ErrVShardMigrationCheckpoint
	}
	applied, ok := readVShardU64(payload, &offset, limit)
	if !ok {
		return ErrVShardMigrationCheckpoint
	}
	walLast, ok := readVShardU64(payload, &offset, limit)
	if !ok {
		return ErrVShardMigrationCheckpoint
	}
	source, ok := readVShardString(payload, &offset, limit, MaxVShardNodeIDBytes)
	if !ok {
		return ErrVShardMigrationCheckpoint
	}
	target, ok := readVShardString(payload, &offset, limit, MaxVShardNodeIDBytes)
	if !ok {
		return ErrVShardMigrationCheckpoint
	}
	sourceReplicas, ok := readVShardStringList(payload, &offset, limit)
	if !ok {
		return ErrVShardMigrationCheckpoint
	}
	targetReplicas, ok := readVShardStringList(payload, &offset, limit)
	if !ok {
		return ErrVShardMigrationCheckpoint
	}
	if offset+len(vShardMigrationChecksum{}) > limit {
		return ErrVShardMigrationCheckpoint
	}
	var checksum [32]byte
	copy(checksum[:], payload[offset:offset+len(checksum)])
	offset += len(checksum)
	failure, ok := readVShardString(payload, &offset, limit, MaxVShardMigrationFailureBytes)
	if !ok || offset != limit {
		return ErrVShardMigrationCheckpoint
	}
	decoded := VShardMigrationCheckpoint{
		Version:          version,
		Bucket:           bucket,
		SourcePrimary:    source,
		SourceReplicas:   sourceReplicas,
		TargetPrimary:    target,
		TargetReplicas:   targetReplicas,
		FencingToken:     fence,
		SnapshotSequence: snapshot,
		AppliedSequence:  applied,
		WALLastSequence:  walLast,
		Phase:            phase,
		SnapshotChecksum: checksum,
		Failure:          failure,
	}
	if err := validateVShardCheckpoint(decoded); err != nil {
		return err
	}
	*checkpoint = decoded
	return nil
}

type vShardMigrationChecksum [32]byte

func validateVShardCheckpoint(checkpoint VShardMigrationCheckpoint) error {
	if checkpoint.Version != VShardMigrationCheckpointVersion || checkpoint.FencingToken == 0 || checkpoint.WALLastSequence < checkpoint.SnapshotSequence || checkpoint.AppliedSequence > checkpoint.WALLastSequence {
		return ErrVShardMigrationCheckpoint
	}
	if _, ok := vShardPhaseCode(checkpoint.Phase); !ok {
		return ErrVShardMigrationCheckpoint
	}
	if err := validateVShardID(checkpoint.SourcePrimary); err != nil {
		return ErrVShardMigrationCheckpoint
	}
	if err := validateVShardID(checkpoint.TargetPrimary); err != nil {
		return ErrVShardMigrationCheckpoint
	}
	if err := validateVShardReplicaList(checkpoint.SourcePrimary, checkpoint.SourceReplicas); err != nil {
		return ErrVShardMigrationCheckpoint
	}
	if err := validateVShardReplicaList(checkpoint.TargetPrimary, checkpoint.TargetReplicas); err != nil {
		return ErrVShardMigrationCheckpoint
	}
	if len(checkpoint.Failure) > MaxVShardMigrationFailureBytes || !utf8.ValidString(checkpoint.Failure) {
		return ErrVShardMigrationCheckpoint
	}
	switch checkpoint.Phase {
	case VShardMigrationReady, VShardMigrationActive:
		if checkpoint.AppliedSequence != checkpoint.WALLastSequence {
			return ErrVShardMigrationCheckpoint
		}
	case VShardMigrationSnapshotApplied:
		if checkpoint.AppliedSequence < checkpoint.SnapshotSequence {
			return ErrVShardMigrationCheckpoint
		}
	}
	return nil
}

func (mapping VShardBucketMap) validate() error {
	if mapping.bucketCount == 0 || mapping.bucketCount > MaxVShardBucketCount || mapping.bucketCount&(mapping.bucketCount-1) != 0 || mapping.replicationFactor == 0 || int(mapping.replicationFactor) > MaxVShardReplicationFactor || len(mapping.nodes) == 0 || len(mapping.nodes) > MaxVShardNodes || len(mapping.owners) != int(mapping.bucketCount)*int(mapping.replicationFactor) || mapping.generation == 0 || mapping.fencingToken == 0 {
		return ErrVShardBucketMapInvalid
	}
	for index, node := range mapping.nodes {
		if index > 0 && mapping.nodes[index-1] >= node {
			return ErrVShardBucketMapInvalid
		}
		if err := validateVShardID(node); err != nil {
			return ErrVShardBucketMapInvalid
		}
	}
	for _, owner := range mapping.owners {
		if int(owner) >= len(mapping.nodes) {
			return ErrVShardBucketMapInvalid
		}
	}
	return nil
}

func (mapping VShardBucketMap) validFast() bool {
	return mapping.bucketCount != 0 && mapping.bucketCount <= MaxVShardBucketCount && mapping.bucketCount&(mapping.bucketCount-1) == 0 && mapping.replicationFactor != 0 && int(mapping.replicationFactor) <= MaxVShardReplicationFactor && len(mapping.nodes) != 0 && len(mapping.nodes) <= MaxVShardNodes && len(mapping.owners) == int(mapping.bucketCount)*int(mapping.replicationFactor) && mapping.generation != 0 && mapping.fencingToken != 0
}

func vShardBucketOwnersEqual(left, right VShardBucketMap, bucket uint32) bool {
	leftStart := int(bucket) * int(left.replicationFactor)
	rightStart := int(bucket) * int(right.replicationFactor)
	for index := 0; index < int(left.replicationFactor); index++ {
		if left.nodes[left.owners[leftStart+index]] != right.nodes[right.owners[rightStart+index]] {
			return false
		}
	}
	return true
}

type vShardCandidate struct {
	index uint16
	score uint64
}

func makeVShardOwners(bucketCount uint32, replicationFactor int, nodes []string) []uint16 {
	owners := make([]uint16, int(bucketCount)*replicationFactor)
	nodeHashes := make([]uint64, len(nodes))
	for index, node := range nodes {
		nodeHashes[index] = xxhash.Sum64String(node)
	}
	candidates := make([]vShardCandidate, len(nodes))
	for bucket := uint32(0); bucket < bucketCount; bucket++ {
		for index, nodeHash := range nodeHashes {
			candidates[index] = vShardCandidate{index: uint16(index), score: vShardScore(nodeHash, bucket)}
		}
		for selected := 0; selected < replicationFactor; selected++ {
			best := selected
			for candidate := selected + 1; candidate < len(candidates); candidate++ {
				if candidates[candidate].score > candidates[best].score || candidates[candidate].score == candidates[best].score && candidates[candidate].index < candidates[best].index {
					best = candidate
				}
			}
			candidates[selected], candidates[best] = candidates[best], candidates[selected]
			owners[int(bucket)*replicationFactor+selected] = candidates[selected].index
		}
	}
	return owners
}

func vShardScore(nodeHash uint64, bucket uint32) uint64 {
	value := nodeHash ^ (uint64(bucket) + 0x9e3779b97f4a7c15)
	value ^= value >> 30
	value *= 0xbf58476d1ce4e5b9
	value ^= value >> 27
	value *= 0x94d049bb133111eb
	return value ^ value>>31
}

func normalizeVShardNodes(input []VShardNode) ([]string, error) {
	if len(input) == 0 || len(input) > MaxVShardNodes {
		return nil, fmt.Errorf("%w: node count %d is out of range", ErrVShardNodeInvalid, len(input))
	}
	nodes := make([]string, len(input))
	seen := make(map[string]struct{}, len(input))
	for index, node := range input {
		if err := validateVShardID(node.ID); err != nil {
			return nil, fmt.Errorf("%w at index %d: %v", ErrVShardNodeInvalid, index, err)
		}
		if _, exists := seen[node.ID]; exists {
			return nil, fmt.Errorf("%w: duplicate node %q", ErrVShardNodeInvalid, node.ID)
		}
		seen[node.ID] = struct{}{}
		nodes[index] = node.ID
	}
	sort.Strings(nodes)
	return nodes, nil
}

func validateVShardID(value string) error {
	if value == "" || value != strings.TrimSpace(value) || len(value) > MaxVShardNodeIDBytes || !utf8.ValidString(value) {
		return errors.New("node ID is empty, oversized, surrounding-space padded, or invalid UTF-8")
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return errors.New("node ID contains a control character")
		}
	}
	return nil
}

func validateVShardReplicaList(primary string, replicas []string) error {
	if len(replicas) > MaxVShardReplicationFactor-1 {
		return ErrVShardMigrationCheckpoint
	}
	seen := make(map[string]struct{}, len(replicas)+1)
	seen[primary] = struct{}{}
	for _, replica := range replicas {
		if err := validateVShardID(replica); err != nil {
			return err
		}
		if _, exists := seen[replica]; exists {
			return errors.New("duplicate vshard owner")
		}
		seen[replica] = struct{}{}
	}
	return nil
}

func validateVShardOwnership(ownership VShardBucketOwnership) error {
	if err := validateVShardID(ownership.Primary); err != nil {
		return err
	}
	return validateVShardReplicaList(ownership.Primary, ownership.Replicas)
}

func cloneVShardOwnership(ownership VShardBucketOwnership) VShardBucketOwnership {
	ownership.Replicas = append([]string(nil), ownership.Replicas...)
	return ownership
}

func stringSlicesEqual(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func boundedVShardFailure(err error) string {
	if err == nil {
		return ""
	}
	value := err.Error()
	if len(value) <= MaxVShardMigrationFailureBytes {
		return value
	}
	value = value[:MaxVShardMigrationFailureBytes]
	for len(value) > 0 && !utf8.ValidString(value) {
		value = value[:len(value)-1]
	}
	return value
}

func vShardPhaseCode(phase VShardMigrationPhase) (byte, bool) {
	switch phase {
	case VShardMigrationPending:
		return 1, true
	case VShardMigrationSnapshotApplying:
		return 2, true
	case VShardMigrationSnapshotApplied:
		return 3, true
	case VShardMigrationWALApplying:
		return 4, true
	case VShardMigrationReady:
		return 5, true
	case VShardMigrationActivating:
		return 6, true
	case VShardMigrationActive:
		return 7, true
	case VShardMigrationAborted:
		return 8, true
	case VShardMigrationFailed:
		return 9, true
	default:
		return 0, false
	}
}

func vShardPhaseFromCode(code byte) (VShardMigrationPhase, bool) {
	phases := [...]VShardMigrationPhase{"", VShardMigrationPending, VShardMigrationSnapshotApplying, VShardMigrationSnapshotApplied, VShardMigrationWALApplying, VShardMigrationReady, VShardMigrationActivating, VShardMigrationActive, VShardMigrationAborted, VShardMigrationFailed}
	if int(code) >= len(phases) || code == 0 {
		return "", false
	}
	return phases[code], true
}

func appendVShardU16(payload []byte, value uint16) []byte {
	var encoded [2]byte
	binary.LittleEndian.PutUint16(encoded[:], value)
	return append(payload, encoded[:]...)
}

func appendVShardU32(payload []byte, value uint32) []byte {
	var encoded [4]byte
	binary.LittleEndian.PutUint32(encoded[:], value)
	return append(payload, encoded[:]...)
}

func appendVShardU64(payload []byte, value uint64) []byte {
	var encoded [8]byte
	binary.LittleEndian.PutUint64(encoded[:], value)
	return append(payload, encoded[:]...)
}

func appendVShardString(payload []byte, value string) []byte {
	payload = binary.AppendUvarint(payload, uint64(len(value)))
	return append(payload, value...)
}

func readVShardU16(payload []byte, offset *int, limit int) (uint16, bool) {
	if *offset+2 > limit {
		return 0, false
	}
	value := binary.LittleEndian.Uint16(payload[*offset : *offset+2])
	*offset += 2
	return value, true
}

func readVShardU32(payload []byte, offset *int, limit int) (uint32, bool) {
	if *offset+4 > limit {
		return 0, false
	}
	value := binary.LittleEndian.Uint32(payload[*offset : *offset+4])
	*offset += 4
	return value, true
}

func readVShardU64(payload []byte, offset *int, limit int) (uint64, bool) {
	if *offset+8 > limit {
		return 0, false
	}
	value := binary.LittleEndian.Uint64(payload[*offset : *offset+8])
	*offset += 8
	return value, true
}

func readVShardString(payload []byte, offset *int, limit, max int) (string, bool) {
	length, size := binary.Uvarint(payload[*offset:limit])
	if size <= 0 || length > uint64(max) || length > uint64(limit-*offset-size) {
		return "", false
	}
	*offset += size
	value := string(payload[*offset : *offset+int(length)])
	*offset += int(length)
	return value, utf8.ValidString(value)
}

func readVShardStringList(payload []byte, offset *int, limit int) ([]string, bool) {
	count, size := binary.Uvarint(payload[*offset:limit])
	if size <= 0 || count > MaxVShardReplicationFactor-1 {
		return nil, false
	}
	*offset += size
	if count == 0 {
		return nil, true
	}
	values := make([]string, int(count))
	for index := range values {
		value, ok := readVShardString(payload, offset, limit, MaxVShardNodeIDBytes)
		if !ok {
			return nil, false
		}
		values[index] = value
	}
	return values, true
}

func equalVShardBytes(left, right []byte) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
