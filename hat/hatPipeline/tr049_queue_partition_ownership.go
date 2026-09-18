package hatPipeline

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
)

const (
	// MaxQueuePartitionOwnershipPartitions bounds one ownership registry.
	MaxQueuePartitionOwnershipPartitions = 1 << 20
)

var (
	// ErrQueuePartitionOwnershipNil indicates a method call on a nil registry.
	ErrQueuePartitionOwnershipNil = errors.New("hatPipeline: queue partition ownership is nil")
	// ErrQueuePartitionOwnershipOptionsInvalid indicates invalid constructor
	// options or an initial owner list with the wrong size.
	ErrQueuePartitionOwnershipOptionsInvalid = errors.New("hatPipeline: queue partition ownership options are invalid")
	// ErrQueuePartitionInvalid indicates a partition outside the registry.
	ErrQueuePartitionInvalid = errors.New("hatPipeline: queue partition is invalid")
	// ErrQueuePartitionUnassigned indicates that no node currently owns a
	// partition.
	ErrQueuePartitionUnassigned = errors.New("hatPipeline: queue partition is unassigned")
	// ErrQueuePartitionMigrationTargetRequired indicates an empty target node.
	ErrQueuePartitionMigrationTargetRequired = errors.New("hatPipeline: queue partition migration target is required")
	// ErrQueuePartitionMigrationAlreadyActive indicates a second migration was
	// requested before the current one was completed or aborted.
	ErrQueuePartitionMigrationAlreadyActive = errors.New("hatPipeline: queue partition migration is already active")
	// ErrQueuePartitionMigrationSameOwner indicates a migration to the current
	// owner, which cannot make progress.
	ErrQueuePartitionMigrationSameOwner = errors.New("hatPipeline: queue partition migration target is already the owner")
	// ErrQueuePartitionMigrationNotActive indicates that a token no longer
	// refers to an active migration.
	ErrQueuePartitionMigrationNotActive = errors.New("hatPipeline: queue partition migration is not active")
	// ErrQueuePartitionMigrationStale indicates that a token does not match the
	// current source, target, generation, or fence.
	ErrQueuePartitionMigrationStale = errors.New("hatPipeline: queue partition migration token is stale")
	// ErrQueuePartitionMigrationNotCaughtUp indicates that the target has not
	// reached the source fence.
	ErrQueuePartitionMigrationNotCaughtUp = errors.New("hatPipeline: queue partition migration target is not caught up")
	// ErrQueuePartitionMigrationNotReady indicates that cutover was requested
	// before the target acknowledged the source fence.
	ErrQueuePartitionMigrationNotReady = errors.New("hatPipeline: queue partition migration is not ready")
)

// QueuePartitionOwnershipState identifies the routing phase of one queue
// partition. During migration, Owner remains the write and read owner until a
// target has caught up and CutoverMigration atomically publishes the target.
type QueuePartitionOwnershipState uint8

const (
	QueuePartitionOwnershipStable QueuePartitionOwnershipState = iota + 1
	QueuePartitionOwnershipMigrating
)

// String returns a stable diagnostic name for an ownership state.
func (state QueuePartitionOwnershipState) String() string {
	switch state {
	case QueuePartitionOwnershipStable:
		return "stable"
	case QueuePartitionOwnershipMigrating:
		return "migrating"
	default:
		return "unknown"
	}
}

// QueuePartitionOwnershipOptions configures one explicit partition ownership
// registry. InitialOwners may be nil to create unassigned partitions, or it
// must contain exactly PartitionCount entries.
type QueuePartitionOwnershipOptions struct {
	PartitionCount int
	InitialOwners  []string
}

// QueuePartitionAssignment is an immutable point-in-time routing record.
// Target, Fence, and MigrationReady are populated only while State is
// QueuePartitionOwnershipMigrating.
type QueuePartitionAssignment struct {
	Partition      int
	Owner          string
	Target         string
	Fence          uint64
	Generation     uint64
	State          QueuePartitionOwnershipState
	MigrationReady bool
}

// QueuePartitionMigration is a capability-like handoff token returned by
// BeginMigration. It is safe to serialize and pass through a control plane;
// every mutating method checks all fields against the current assignment.
type QueuePartitionMigration struct {
	Partition  int
	Source     string
	Target     string
	Fence      uint64
	Generation uint64
}

type queuePartitionOwnershipSnapshot struct {
	assignments []QueuePartitionAssignment
	owners      []string
}

// QueuePartitionOwnership is a transport-neutral control-plane registry for
// explicit queue partitions. Reads use an immutable atomic snapshot and do not
// allocate or take the control mutex. Ownership changes copy only the bounded
// assignment slice and publish it atomically.
type QueuePartitionOwnership struct {
	controlMu sync.Mutex
	snapshot  atomic.Pointer[queuePartitionOwnershipSnapshot]
}

// QueuePartitionOwnershipSnapshot is an immutable routing view. Acquire one
// at a queue batch boundary with Snapshot, then reuse it for every item in the
// batch to avoid an atomic load per item. A view intentionally remains stable
// across later migrations; refresh it before the next batch when cutover
// visibility is required.
type QueuePartitionOwnershipSnapshot struct {
	snapshot *queuePartitionOwnershipSnapshot
	owners   []string
}

// NewQueuePartitionOwnership creates a registry with generation one for every
// partition. An empty initial owner leaves that partition intentionally
// unassigned until an external control plane supplies an owner.
func NewQueuePartitionOwnership(options QueuePartitionOwnershipOptions) (*QueuePartitionOwnership, error) {
	if options.PartitionCount <= 0 || options.PartitionCount > MaxQueuePartitionOwnershipPartitions {
		return nil, fmt.Errorf("%w: partition count %d", ErrQueuePartitionOwnershipOptionsInvalid, options.PartitionCount)
	}
	if len(options.InitialOwners) != 0 && len(options.InitialOwners) != options.PartitionCount {
		return nil, fmt.Errorf("%w: got %d initial owners for %d partitions", ErrQueuePartitionOwnershipOptionsInvalid, len(options.InitialOwners), options.PartitionCount)
	}
	assignments := make([]QueuePartitionAssignment, options.PartitionCount)
	owners := make([]string, options.PartitionCount)
	for partition := range assignments {
		owner := ""
		if len(options.InitialOwners) != 0 {
			owner = strings.TrimSpace(options.InitialOwners[partition])
		}
		assignments[partition] = QueuePartitionAssignment{
			Partition:  partition,
			Owner:      owner,
			Generation: 1,
			State:      QueuePartitionOwnershipStable,
		}
		owners[partition] = owner
	}
	ownership := &QueuePartitionOwnership{}
	ownership.snapshot.Store(&queuePartitionOwnershipSnapshot{assignments: assignments, owners: owners})
	return ownership, nil
}

// Assignment returns one immutable point-in-time assignment. An unassigned
// partition is a valid assignment; use Owner when routing requires a node.
func (ownership *QueuePartitionOwnership) Assignment(partition int) (QueuePartitionAssignment, error) {
	if ownership == nil {
		return QueuePartitionAssignment{}, ErrQueuePartitionOwnershipNil
	}
	return ownership.Snapshot().Assignment(partition)
}

// Owner returns the current owner used for routing. It continues to return
// the source owner during a migration until cutover completes.
func (ownership *QueuePartitionOwnership) Owner(partition int) (string, error) {
	if ownership == nil {
		return "", ErrQueuePartitionOwnershipNil
	}
	return ownership.Snapshot().Owner(partition)
}

// Snapshot acquires an immutable routing view without allocating.
func (ownership *QueuePartitionOwnership) Snapshot() QueuePartitionOwnershipSnapshot {
	if ownership == nil {
		return QueuePartitionOwnershipSnapshot{}
	}
	snapshot := ownership.snapshot.Load()
	if snapshot == nil {
		return QueuePartitionOwnershipSnapshot{}
	}
	return QueuePartitionOwnershipSnapshot{snapshot: snapshot, owners: snapshot.owners}
}

// Assignment returns one assignment from the immutable routing view.
func (snapshot QueuePartitionOwnershipSnapshot) Assignment(partition int) (QueuePartitionAssignment, error) {
	if snapshot.snapshot == nil {
		return QueuePartitionAssignment{}, ErrQueuePartitionOwnershipNil
	}
	if partition < 0 || partition >= len(snapshot.snapshot.assignments) {
		return QueuePartitionAssignment{}, fmt.Errorf("%w: got %d, count %d", ErrQueuePartitionInvalid, partition, len(snapshot.snapshot.assignments))
	}
	return snapshot.snapshot.assignments[partition], nil
}

// Owner returns the owner from the immutable routing view. It is the
// allocation-free hot-loop variant of QueuePartitionOwnership.Owner.
func (snapshot QueuePartitionOwnershipSnapshot) Owner(partition int) (string, error) {
	if snapshot.snapshot == nil {
		return "", ErrQueuePartitionOwnershipNil
	}
	if partition < 0 || partition >= len(snapshot.owners) {
		return "", fmt.Errorf("%w: got %d, count %d", ErrQueuePartitionInvalid, partition, len(snapshot.owners))
	}
	owner := snapshot.owners[partition]
	if owner == "" {
		return "", fmt.Errorf("%w: partition %d", ErrQueuePartitionUnassigned, partition)
	}
	return owner, nil
}

// OwnerUnchecked returns the owner without bounds, nil-view, or unassigned
// checks. Callers must validate the view and partition once before entering a
// hot loop; invalid input will panic just like direct slice indexing.
func (snapshot QueuePartitionOwnershipSnapshot) OwnerUnchecked(partition int) string {
	return snapshot.owners[partition]
}

// Assignments returns an independent copy suitable for diagnostics, backup,
// or control-plane publication. The registry remains immutable to the caller.
func (ownership *QueuePartitionOwnership) Assignments() []QueuePartitionAssignment {
	snapshot := ownership.Snapshot()
	if snapshot.snapshot == nil {
		return nil
	}
	return append([]QueuePartitionAssignment(nil), snapshot.snapshot.assignments...)
}

// BeginMigration marks a partition as moving while retaining the source as
// the active owner. fence is the source sequence that the target must reach.
func (ownership *QueuePartitionOwnership) BeginMigration(partition int, target string, fence uint64) (QueuePartitionMigration, error) {
	if ownership == nil {
		return QueuePartitionMigration{}, ErrQueuePartitionOwnershipNil
	}
	target = strings.TrimSpace(target)
	if target == "" {
		return QueuePartitionMigration{}, ErrQueuePartitionMigrationTargetRequired
	}
	ownership.controlMu.Lock()
	defer ownership.controlMu.Unlock()
	snapshot, current, err := ownership.currentAssignmentLocked(partition)
	if err != nil {
		return QueuePartitionMigration{}, err
	}
	if current.State != QueuePartitionOwnershipStable {
		return QueuePartitionMigration{}, ErrQueuePartitionMigrationAlreadyActive
	}
	if current.Owner == "" {
		return QueuePartitionMigration{}, fmt.Errorf("%w: partition %d", ErrQueuePartitionUnassigned, partition)
	}
	if current.Owner == target {
		return QueuePartitionMigration{}, ErrQueuePartitionMigrationSameOwner
	}
	next := cloneQueuePartitionAssignments(snapshot.assignments)
	next[partition].Target = target
	next[partition].Fence = fence
	next[partition].State = QueuePartitionOwnershipMigrating
	next[partition].MigrationReady = false
	ownership.publishLocked(next, snapshot.owners)
	return QueuePartitionMigration{
		Partition:  partition,
		Source:     current.Owner,
		Target:     target,
		Fence:      fence,
		Generation: current.Generation,
	}, nil
}

// AcknowledgeCatchUp marks a migration ready once targetSequence reaches the
// captured source fence. Repeated acknowledgements after readiness are
// idempotent and do not publish another snapshot.
func (ownership *QueuePartitionOwnership) AcknowledgeCatchUp(migration QueuePartitionMigration, targetSequence uint64) (QueuePartitionAssignment, error) {
	if ownership == nil {
		return QueuePartitionAssignment{}, ErrQueuePartitionOwnershipNil
	}
	ownership.controlMu.Lock()
	defer ownership.controlMu.Unlock()
	snapshot, current, err := ownership.validateMigrationLocked(migration)
	if err != nil {
		return QueuePartitionAssignment{}, err
	}
	if targetSequence < migration.Fence {
		return current, fmt.Errorf("%w: target %d, fence %d", ErrQueuePartitionMigrationNotCaughtUp, targetSequence, migration.Fence)
	}
	if current.MigrationReady {
		return current, nil
	}
	next := cloneQueuePartitionAssignments(snapshot.assignments)
	next[migration.Partition].MigrationReady = true
	ownership.publishLocked(next, snapshot.owners)
	return next[migration.Partition], nil
}

// Cutover atomically makes the caught-up target the active owner and
// advances the partition generation. Data movement and replication remain the
// caller's responsibility; this method only publishes the ownership fence.
func (ownership *QueuePartitionOwnership) Cutover(migration QueuePartitionMigration) (QueuePartitionAssignment, error) {
	if ownership == nil {
		return QueuePartitionAssignment{}, ErrQueuePartitionOwnershipNil
	}
	ownership.controlMu.Lock()
	defer ownership.controlMu.Unlock()
	snapshot, current, err := ownership.validateMigrationLocked(migration)
	if err != nil {
		return QueuePartitionAssignment{}, err
	}
	if !current.MigrationReady {
		return current, ErrQueuePartitionMigrationNotReady
	}
	next := cloneQueuePartitionAssignments(snapshot.assignments)
	next[migration.Partition].Owner = migration.Target
	next[migration.Partition].Target = ""
	next[migration.Partition].Fence = 0
	next[migration.Partition].Generation++
	next[migration.Partition].State = QueuePartitionOwnershipStable
	next[migration.Partition].MigrationReady = false
	owners := append([]string(nil), snapshot.owners...)
	owners[migration.Partition] = migration.Target
	ownership.publishLocked(next, owners)
	return next[migration.Partition], nil
}

// AbortMigration returns a moving partition to its source and advances its
// generation so the aborted token cannot be reused.
func (ownership *QueuePartitionOwnership) AbortMigration(migration QueuePartitionMigration) (QueuePartitionAssignment, error) {
	if ownership == nil {
		return QueuePartitionAssignment{}, ErrQueuePartitionOwnershipNil
	}
	ownership.controlMu.Lock()
	defer ownership.controlMu.Unlock()
	snapshot, _, err := ownership.validateMigrationLocked(migration)
	if err != nil {
		return QueuePartitionAssignment{}, err
	}
	next := cloneQueuePartitionAssignments(snapshot.assignments)
	next[migration.Partition].Target = ""
	next[migration.Partition].Fence = 0
	next[migration.Partition].Generation++
	next[migration.Partition].State = QueuePartitionOwnershipStable
	next[migration.Partition].MigrationReady = false
	ownership.publishLocked(next, snapshot.owners)
	return next[migration.Partition], nil
}

func (ownership *QueuePartitionOwnership) currentAssignmentLocked(partition int) (*queuePartitionOwnershipSnapshot, QueuePartitionAssignment, error) {
	snapshot := ownership.snapshot.Load()
	if snapshot == nil {
		return nil, QueuePartitionAssignment{}, ErrQueuePartitionOwnershipNil
	}
	if partition < 0 || partition >= len(snapshot.assignments) {
		return nil, QueuePartitionAssignment{}, fmt.Errorf("%w: got %d, count %d", ErrQueuePartitionInvalid, partition, len(snapshot.assignments))
	}
	return snapshot, snapshot.assignments[partition], nil
}

func (ownership *QueuePartitionOwnership) validateMigrationLocked(migration QueuePartitionMigration) (*queuePartitionOwnershipSnapshot, QueuePartitionAssignment, error) {
	snapshot, current, err := ownership.currentAssignmentLocked(migration.Partition)
	if err != nil {
		return nil, QueuePartitionAssignment{}, err
	}
	if current.State != QueuePartitionOwnershipMigrating {
		return nil, QueuePartitionAssignment{}, ErrQueuePartitionMigrationNotActive
	}
	if current.Generation != migration.Generation || current.Owner != migration.Source || current.Target != migration.Target || current.Fence != migration.Fence {
		return nil, QueuePartitionAssignment{}, ErrQueuePartitionMigrationStale
	}
	return snapshot, current, nil
}

func (ownership *QueuePartitionOwnership) publishLocked(assignments []QueuePartitionAssignment, owners []string) {
	ownership.snapshot.Store(&queuePartitionOwnershipSnapshot{assignments: assignments, owners: owners})
}

func cloneQueuePartitionAssignments(assignments []QueuePartitionAssignment) []QueuePartitionAssignment {
	return append([]QueuePartitionAssignment(nil), assignments...)
}
