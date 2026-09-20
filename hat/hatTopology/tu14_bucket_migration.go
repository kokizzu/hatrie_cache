package hatTopology

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"sync"
)

const (
	// DefaultBucketMigrationMaxMigrations bounds one coordinator's in-flight
	// plans. Zero-value options use this limit.
	DefaultBucketMigrationMaxMigrations = 256
	maxBucketMigrationMaxMigrations     = 4096
	maxBucketMigrationIDBytes           = 256
)

var (
	ErrBucketMigrationCoordinatorNil   = errors.New("hatriecache: bucket migration coordinator is nil")
	ErrBucketMigrationPlanInvalid      = errors.New("hatriecache: bucket migration plan is invalid")
	ErrBucketMigrationExists           = errors.New("hatriecache: bucket migration already exists")
	ErrBucketMigrationNotFound         = errors.New("hatriecache: bucket migration was not found")
	ErrBucketMigrationCapacity         = errors.New("hatriecache: bucket migration capacity exceeded")
	ErrBucketMigrationPhase            = errors.New("hatriecache: bucket migration phase does not allow this operation")
	ErrBucketMigrationProgressInvalid  = errors.New("hatriecache: bucket migration progress is invalid")
	ErrBucketMigrationNotCaughtUp      = errors.New("hatriecache: bucket migration target is not caught up")
	ErrBucketMigrationTopologyMismatch = errors.New("hatriecache: bucket migration target topology does not match")
)

// BucketMigrationKind describes whether a bucket changes primary shard or
// only changes its backup owner set.
type BucketMigrationKind string

const (
	BucketMigrationMove   BucketMigrationKind = "move"
	BucketMigrationBackup BucketMigrationKind = "backup"
)

// BucketMigrationPhase is the bounded lifecycle of one planned migration.
type BucketMigrationPhase uint8

const (
	BucketMigrationPlanned BucketMigrationPhase = iota + 1
	BucketMigrationCopying
	BucketMigrationReady
	BucketMigrationCutover
	BucketMigrationCompleted
	BucketMigrationAborted
)

func (phase BucketMigrationPhase) String() string {
	switch phase {
	case BucketMigrationPlanned:
		return "planned"
	case BucketMigrationCopying:
		return "copying"
	case BucketMigrationReady:
		return "ready"
	case BucketMigrationCutover:
		return "cutover"
	case BucketMigrationCompleted:
		return "completed"
	case BucketMigrationAborted:
		return "aborted"
	default:
		return "unknown"
	}
}

// BucketMigrationPlan is a deterministic, caller-executable transfer plan.
// The ownership snapshots include the target primary and backups so a caller
// can copy before cutover without losing the target replica contract.
type BucketMigrationPlan struct {
	ID              string              `json:"id"`
	Kind            BucketMigrationKind `json:"kind"`
	Bucket          uint32              `json:"bucket"`
	SourceOwnership PartitionOwnership  `json:"source_ownership"`
	TargetOwnership PartitionOwnership  `json:"target_ownership"`
}

// PlanBucketMigrations compares two normalized sharded topologies and emits
// deterministic plans for changed bucket ownership. It does not move data or
// publish the target topology.
func PlanBucketMigrations(current, target ClusterTopology) ([]BucketMigrationPlan, error) {
	current, err := Normalize(current)
	if err != nil {
		return nil, fmt.Errorf("%w: current topology: %v", ErrBucketMigrationPlanInvalid, err)
	}
	target, err = Normalize(target)
	if err != nil {
		return nil, fmt.Errorf("%w: target topology: %v", ErrBucketMigrationPlanInvalid, err)
	}
	if current.Mode != TopologyModeSharded || target.Mode != TopologyModeSharded || current.BucketCount == 0 || target.BucketCount == 0 {
		return nil, fmt.Errorf("%w: both topologies must be sharded with buckets", ErrBucketMigrationPlanInvalid)
	}
	if current.BucketCount != target.BucketCount {
		return nil, fmt.Errorf("%w: bucket count changed from %d to %d", ErrBucketMigrationPlanInvalid, current.BucketCount, target.BucketCount)
	}
	currentFingerprint, targetFingerprint := fingerprintNormalized(current), fingerprintNormalized(target)
	plans := make([]BucketMigrationPlan, 0)
	for bucket := uint32(0); bucket < current.BucketCount; bucket++ {
		currentShard, ok := current.shardForBucket(bucket, current.Shards)
		if !ok {
			return nil, fmt.Errorf("%w: current bucket %d has no shard", ErrBucketMigrationPlanInvalid, bucket)
		}
		targetShard, ok := target.shardForBucket(bucket, target.Shards)
		if !ok {
			return nil, fmt.Errorf("%w: target bucket %d has no shard", ErrBucketMigrationPlanInvalid, bucket)
		}
		if sameBucketOwners(currentShard, targetShard) {
			continue
		}
		sourceOwnership := bucketMigrationOwnership(current, currentShard, currentFingerprint)
		targetOwnership := bucketMigrationOwnership(target, targetShard, targetFingerprint)
		kind := BucketMigrationBackup
		if currentShard.ID != targetShard.ID {
			kind = BucketMigrationMove
		}
		plans = append(plans, BucketMigrationPlan{
			ID:              fmt.Sprintf("bucket-%d-%s-%s", bucket, currentFingerprint, targetFingerprint),
			Kind:            kind,
			Bucket:          bucket,
			SourceOwnership: sourceOwnership,
			TargetOwnership: targetOwnership,
		})
	}
	return plans, nil
}

func sameBucketOwners(left, right TopologyShard) bool {
	if left.ID != right.ID || left.Primary != right.Primary || len(left.Replicas) != len(right.Replicas) {
		return false
	}
	for index := range left.Replicas {
		if left.Replicas[index] != right.Replicas[index] {
			return false
		}
	}
	return true
}

func bucketMigrationOwnership(topology ClusterTopology, shard TopologyShard, fingerprint string) PartitionOwnership {
	return PartitionOwnership{
		ShardID:             shard.ID,
		Primary:             shard.Primary,
		Replicas:            append([]string(nil), shard.Replicas...),
		TopologyFingerprint: fingerprint,
		FencingToken:        topology.FencingToken,
	}
}

// BucketMigrationCoordinatorOptions bounds in-flight migration state.
type BucketMigrationCoordinatorOptions struct {
	MaxMigrations int
}

// BucketMigrationStatus is an independent snapshot of one migration.
type BucketMigrationStatus struct {
	Plan           BucketMigrationPlan  `json:"plan"`
	Phase          BucketMigrationPhase `json:"phase"`
	CopiedRecords  uint64               `json:"copied_records"`
	CopiedBytes    uint64               `json:"copied_bytes"`
	SourceSequence uint64               `json:"source_sequence"`
	TargetSequence uint64               `json:"target_sequence"`
}

type bucketMigrationRecord struct {
	status BucketMigrationStatus
}

// BucketMigrationCoordinator tracks bounded migration state. It coordinates
// ownership and progress but deliberately leaves data transfer, WAL replay,
// and topology publication to the embedding service.
type BucketMigrationCoordinator struct {
	mu            sync.Mutex
	maxMigrations int
	migrations    map[string]*bucketMigrationRecord
}

// NewBucketMigrationCoordinator creates a bounded migration coordinator.
func NewBucketMigrationCoordinator(options BucketMigrationCoordinatorOptions) (*BucketMigrationCoordinator, error) {
	maxMigrations := options.MaxMigrations
	if maxMigrations == 0 {
		maxMigrations = DefaultBucketMigrationMaxMigrations
	}
	if maxMigrations < 0 || maxMigrations > maxBucketMigrationMaxMigrations {
		return nil, fmt.Errorf("%w: max migrations", ErrBucketMigrationPlanInvalid)
	}
	return &BucketMigrationCoordinator{
		maxMigrations: maxMigrations,
		migrations:    make(map[string]*bucketMigrationRecord),
	}, nil
}

// Start publishes one planned migration. Repeating the exact plan is
// idempotent; reusing an ID for a different plan is rejected.
func (coordinator *BucketMigrationCoordinator) Start(plan BucketMigrationPlan) (BucketMigrationStatus, error) {
	if coordinator == nil {
		return BucketMigrationStatus{}, ErrBucketMigrationCoordinatorNil
	}
	if err := validateBucketMigrationPlan(plan); err != nil {
		return BucketMigrationStatus{}, err
	}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	if existing, ok := coordinator.migrations[plan.ID]; ok {
		if !reflect.DeepEqual(existing.status.Plan, plan) {
			return BucketMigrationStatus{}, fmt.Errorf("%w: %s", ErrBucketMigrationExists, plan.ID)
		}
		return cloneBucketMigrationStatus(existing.status), nil
	}
	if len(coordinator.migrations) >= coordinator.maxMigrations {
		return BucketMigrationStatus{}, ErrBucketMigrationCapacity
	}
	status := BucketMigrationStatus{Plan: cloneBucketMigrationPlan(plan), Phase: BucketMigrationPlanned}
	coordinator.migrations[plan.ID] = &bucketMigrationRecord{status: status}
	return cloneBucketMigrationStatus(status), nil
}

// BeginCopy starts or repeats the copy phase.
func (coordinator *BucketMigrationCoordinator) BeginCopy(id string) error {
	return coordinator.update(id, func(status *BucketMigrationStatus) error {
		switch status.Phase {
		case BucketMigrationPlanned, BucketMigrationCopying:
			status.Phase = BucketMigrationCopying
			return nil
		default:
			return ErrBucketMigrationPhase
		}
	})
}

// RecordCopy advances copied counters monotonically without allowing uint64
// wraparound.
func (coordinator *BucketMigrationCoordinator) RecordCopy(id string, records, bytes uint64) error {
	return coordinator.update(id, func(status *BucketMigrationStatus) error {
		if status.Phase != BucketMigrationCopying {
			return ErrBucketMigrationPhase
		}
		if records > math.MaxUint64-status.CopiedRecords || bytes > math.MaxUint64-status.CopiedBytes {
			return ErrBucketMigrationProgressInvalid
		}
		status.CopiedRecords += records
		status.CopiedBytes += bytes
		return nil
	})
}

// MarkCaughtUp records source and target journal positions. A target is ready
// for cutover only when both positions are equal.
func (coordinator *BucketMigrationCoordinator) MarkCaughtUp(id string, sourceSequence, targetSequence uint64) error {
	return coordinator.update(id, func(status *BucketMigrationStatus) error {
		if status.Phase != BucketMigrationCopying {
			return ErrBucketMigrationPhase
		}
		if targetSequence != sourceSequence {
			return ErrBucketMigrationNotCaughtUp
		}
		status.SourceSequence, status.TargetSequence = sourceSequence, targetSequence
		status.Phase = BucketMigrationReady
		return nil
	})
}

// MarkCutoverReady fences the copy phase before a caller publishes the target
// ownership. The explicit phase prevents partial transfers from being treated
// as committed ownership.
func (coordinator *BucketMigrationCoordinator) MarkCutoverReady(id string) error {
	return coordinator.update(id, func(status *BucketMigrationStatus) error {
		if status.Phase != BucketMigrationReady {
			return ErrBucketMigrationPhase
		}
		status.Phase = BucketMigrationCutover
		return nil
	})
}

// CompleteCutover commits the migration only against the exact target
// topology and ownership snapshot recorded by the plan.
func (coordinator *BucketMigrationCoordinator) CompleteCutover(id string, target ClusterTopology) error {
	return coordinator.update(id, func(status *BucketMigrationStatus) error {
		if status.Phase != BucketMigrationCutover {
			return ErrBucketMigrationPhase
		}
		normalized, err := Normalize(target)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrBucketMigrationTopologyMismatch, err)
		}
		if normalized.Mode != TopologyModeSharded || normalized.BucketCount <= status.Plan.Bucket {
			return ErrBucketMigrationTopologyMismatch
		}
		shard, ok := normalized.shardForBucket(status.Plan.Bucket, normalized.Shards)
		if !ok || shard.ID != status.Plan.TargetOwnership.ShardID {
			return ErrBucketMigrationTopologyMismatch
		}
		ownership, ok := partitionOwnershipForNormalizedShard(normalized, shard.ID)
		if !ok || !partitionOwnershipConsensusMetadataEqual(ownership, status.Plan.TargetOwnership) {
			return ErrBucketMigrationTopologyMismatch
		}
		status.Phase = BucketMigrationCompleted
		return nil
	})
}

// Abort stops a migration while retaining progress for an explicit Resume.
func (coordinator *BucketMigrationCoordinator) Abort(id string) error {
	return coordinator.update(id, func(status *BucketMigrationStatus) error {
		if status.Phase == BucketMigrationCompleted {
			return ErrBucketMigrationPhase
		}
		if status.Phase == BucketMigrationAborted {
			return nil
		}
		status.Phase = BucketMigrationAborted
		return nil
	})
}

// Resume restarts an aborted migration from its retained copy counters.
func (coordinator *BucketMigrationCoordinator) Resume(id string) error {
	return coordinator.update(id, func(status *BucketMigrationStatus) error {
		if status.Phase != BucketMigrationAborted {
			return ErrBucketMigrationPhase
		}
		status.Phase = BucketMigrationCopying
		return nil
	})
}

// Status returns an isolated status snapshot.
func (coordinator *BucketMigrationCoordinator) Status(id string) (BucketMigrationStatus, error) {
	if coordinator == nil {
		return BucketMigrationStatus{}, ErrBucketMigrationCoordinatorNil
	}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	record, ok := coordinator.migrations[strings.TrimSpace(id)]
	if !ok {
		return BucketMigrationStatus{}, ErrBucketMigrationNotFound
	}
	return cloneBucketMigrationStatus(record.status), nil
}

func (coordinator *BucketMigrationCoordinator) update(id string, update func(*BucketMigrationStatus) error) error {
	if coordinator == nil {
		return ErrBucketMigrationCoordinatorNil
	}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	record, ok := coordinator.migrations[strings.TrimSpace(id)]
	if !ok {
		return ErrBucketMigrationNotFound
	}
	return update(&record.status)
}

func validateBucketMigrationPlan(plan BucketMigrationPlan) error {
	originalID := plan.ID
	plan.ID = strings.TrimSpace(plan.ID)
	if plan.ID == "" || len(plan.ID) > maxBucketMigrationIDBytes || originalID != plan.ID {
		return fmt.Errorf("%w: id", ErrBucketMigrationPlanInvalid)
	}
	if plan.Kind != BucketMigrationMove && plan.Kind != BucketMigrationBackup {
		return fmt.Errorf("%w: kind", ErrBucketMigrationPlanInvalid)
	}
	if err := validatePartitionOwnershipConsensusMetadata(plan.SourceOwnership); err != nil {
		return fmt.Errorf("%w: source ownership: %v", ErrBucketMigrationPlanInvalid, err)
	}
	if err := validatePartitionOwnershipConsensusMetadata(plan.TargetOwnership); err != nil {
		return fmt.Errorf("%w: target ownership: %v", ErrBucketMigrationPlanInvalid, err)
	}
	if plan.Kind == BucketMigrationMove && plan.SourceOwnership.ShardID == plan.TargetOwnership.ShardID {
		return fmt.Errorf("%w: move must change shard", ErrBucketMigrationPlanInvalid)
	}
	if plan.Kind == BucketMigrationBackup && plan.SourceOwnership.ShardID != plan.TargetOwnership.ShardID {
		return fmt.Errorf("%w: backup must retain shard", ErrBucketMigrationPlanInvalid)
	}
	return nil
}

func cloneBucketMigrationPlan(plan BucketMigrationPlan) BucketMigrationPlan {
	plan.SourceOwnership = clonePartitionOwnershipConsensusMetadata(plan.SourceOwnership)
	plan.TargetOwnership = clonePartitionOwnershipConsensusMetadata(plan.TargetOwnership)
	return plan
}

func cloneBucketMigrationStatus(status BucketMigrationStatus) BucketMigrationStatus {
	status.Plan = cloneBucketMigrationPlan(status.Plan)
	return status
}
