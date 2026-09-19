package hatSql

import "sort"

// TypedTableArrangementAdvisorAction is the next safe action for a requested
// arrangement. The advisor never mutates a registry or silently treats stale
// state as immediately reusable.
type TypedTableArrangementAdvisorAction string

const (
	TypedTableArrangementAdvisorCreate           TypedTableArrangementAdvisorAction = "create"
	TypedTableArrangementAdvisorReuse            TypedTableArrangementAdvisorAction = "reuse"
	TypedTableArrangementAdvisorHydrateThenReuse TypedTableArrangementAdvisorAction = "hydrate_then_reuse"
)

// TypedTableArrangementAdvisorOptions controls the coarse memory model used
// for planning. Estimates are incremental: reusing a fresh arrangement costs
// zero additional bytes, while creation and stale hydration report their
// expected new or transient memory.
type TypedTableArrangementAdvisorOptions struct {
	AggregateStateBytesPerRow uint64
	JoinStateBytesPerRow      uint64
	FixedArrangementBytes     uint64
	ReplayBytesPerChange      uint64
}

const (
	defaultTypedTableAggregateStateBytesPerRow = uint64(160)
	defaultTypedTableJoinStateBytesPerRow      = uint64(128)
	defaultTypedTableFixedArrangementBytes     = uint64(256)
	defaultTypedTableReplayBytesPerChange      = uint64(64)
)

// DefaultTypedTableArrangementAdvisorOptions returns the conservative default
// cost model. These are planning estimates, not allocator measurements.
func DefaultTypedTableArrangementAdvisorOptions() TypedTableArrangementAdvisorOptions {
	return TypedTableArrangementAdvisorOptions{
		AggregateStateBytesPerRow: defaultTypedTableAggregateStateBytesPerRow,
		JoinStateBytesPerRow:      defaultTypedTableJoinStateBytesPerRow,
		FixedArrangementBytes:     defaultTypedTableFixedArrangementBytes,
		ReplayBytesPerChange:      defaultTypedTableReplayBytesPerChange,
	}
}

func (options TypedTableArrangementAdvisorOptions) normalized() TypedTableArrangementAdvisorOptions {
	defaults := DefaultTypedTableArrangementAdvisorOptions()
	if options.AggregateStateBytesPerRow == 0 {
		options.AggregateStateBytesPerRow = defaults.AggregateStateBytesPerRow
	}
	if options.JoinStateBytesPerRow == 0 {
		options.JoinStateBytesPerRow = defaults.JoinStateBytesPerRow
	}
	if options.FixedArrangementBytes == 0 {
		options.FixedArrangementBytes = defaults.FixedArrangementBytes
	}
	if options.ReplayBytesPerChange == 0 {
		options.ReplayBytesPerChange = defaults.ReplayBytesPerChange
	}
	return options
}

// TypedTableArrangementMemoryEstimate reports incremental persistent and
// transient memory expected for the recommended action.
type TypedTableArrangementMemoryEstimate struct {
	PersistentBytes uint64 `json:"persistent_bytes"`
	TransientBytes  uint64 `json:"transient_bytes"`
	TotalBytes      uint64 `json:"total_bytes"`
}

// TypedTableAggregateArrangementRequest describes a prospective aggregate
// dataflow. EstimatedStateRows is the expected number of retained groups.
type TypedTableAggregateArrangementRequest struct {
	TableName          string
	Definition         TypedTableAggregateDefinition
	EstimatedStateRows uint64
}

// TypedTableJoinArrangementRequest describes a prospective join dataflow.
// EstimatedStateRows is the expected number of retained matching pairs.
type TypedTableJoinArrangementRequest struct {
	LeftTableName      string
	RightTableName     string
	Definition         TypedTableJoinDefinition
	EstimatedStateRows uint64
}

// TypedTableAggregateArrangementRecommendation describes one exact matching
// aggregate arrangement and the action required before using it.
type TypedTableAggregateArrangementRecommendation struct {
	TableName      string
	Definition     TypedTableAggregateDefinition
	Action         TypedTableArrangementAdvisorAction
	References     int
	Shared         bool
	Checkpoint     uint64
	SourceSequence uint64
	Stale          bool
	Memory         TypedTableArrangementMemoryEstimate
}

// TypedTableAggregateArrangementAdvice is a deterministic aggregate reuse
// decision. Candidates are ordered from safest/cheapest to least preferred.
type TypedTableAggregateArrangementAdvice struct {
	Action     TypedTableArrangementAdvisorAction
	Memory     TypedTableArrangementMemoryEstimate
	Candidates []TypedTableAggregateArrangementRecommendation
}

// TypedTableJoinArrangementRecommendation describes one exact matching join
// arrangement and the action required before using it.
type TypedTableJoinArrangementRecommendation struct {
	LeftTableName       string
	RightTableName      string
	Definition          TypedTableJoinDefinition
	Action              TypedTableArrangementAdvisorAction
	References          int
	Shared              bool
	LeftCheckpoint      uint64
	LeftSourceSequence  uint64
	RightCheckpoint     uint64
	RightSourceSequence uint64
	Stale               bool
	Memory              TypedTableArrangementMemoryEstimate
}

// TypedTableJoinArrangementAdvice is a deterministic join reuse decision.
type TypedTableJoinArrangementAdvice struct {
	Action     TypedTableArrangementAdvisorAction
	Memory     TypedTableArrangementMemoryEstimate
	Candidates []TypedTableJoinArrangementRecommendation
}

// AdviseTypedTableAggregateArrangement finds exact compatible aggregate
// arrangements. A stale match is never reported as immediately reusable; the
// caller must hydrate it and handle a possible compacted-changefeed error.
func AdviseTypedTableAggregateArrangement(
	catalog []TypedTableAggregateArrangementInfo,
	request TypedTableAggregateArrangementRequest,
	options TypedTableArrangementAdvisorOptions,
) TypedTableAggregateArrangementAdvice {
	options = options.normalized()
	requestedKey := typedTableAggregateArrangementKey(request.Definition)
	candidates := make([]TypedTableAggregateArrangementRecommendation, 0)
	for _, existing := range catalog {
		if existing.TableName != request.TableName || typedTableAggregateArrangementKey(existing.Definition) != requestedKey {
			continue
		}
		stale := existing.Stale || existing.Checkpoint < existing.SourceSequence
		action := TypedTableArrangementAdvisorReuse
		memory := TypedTableArrangementMemoryEstimate{}
		if stale {
			action = TypedTableArrangementAdvisorHydrateThenReuse
			memory = typedTableArrangementHydrationMemory(
				typedTableArrangementPending(existing.Checkpoint, existing.SourceSequence),
				options.ReplayBytesPerChange,
			)
		}
		candidates = append(candidates, TypedTableAggregateArrangementRecommendation{
			TableName:      existing.TableName,
			Definition:     cloneTypedTableAggregateDefinition(existing.Definition),
			Action:         action,
			References:     existing.References,
			Shared:         existing.Shared || existing.References > 1,
			Checkpoint:     existing.Checkpoint,
			SourceSequence: existing.SourceSequence,
			Stale:          stale,
			Memory:         memory,
		})
	}
	sort.Slice(candidates, func(left, right int) bool {
		return lessTypedTableAggregateArrangementRecommendation(candidates[left], candidates[right])
	})
	if len(candidates) > 0 {
		return TypedTableAggregateArrangementAdvice{
			Action:     candidates[0].Action,
			Memory:     candidates[0].Memory,
			Candidates: candidates,
		}
	}
	memory := typedTableArrangementCreationMemory(
		request.EstimatedStateRows,
		options.AggregateStateBytesPerRow,
		options.FixedArrangementBytes,
	)
	return TypedTableAggregateArrangementAdvice{
		Action: TypedTableArrangementAdvisorCreate,
		Memory: memory,
	}
}

// AdviseTypedTableJoinArrangement finds exact compatible join arrangements.
// Left/right orientation is significant because the maintained state and
// update paths are directional.
func AdviseTypedTableJoinArrangement(
	catalog []TypedTableJoinArrangementInfo,
	request TypedTableJoinArrangementRequest,
	options TypedTableArrangementAdvisorOptions,
) TypedTableJoinArrangementAdvice {
	options = options.normalized()
	requestedKey := typedTableArrangementAdvisorJoinDefinitionKey(request.Definition)
	candidates := make([]TypedTableJoinArrangementRecommendation, 0)
	for _, existing := range catalog {
		if existing.LeftTableName != request.LeftTableName || existing.RightTableName != request.RightTableName || typedTableArrangementAdvisorJoinDefinitionKey(existing.Definition) != requestedKey {
			continue
		}
		stale := existing.Stale || existing.LeftCheckpoint < existing.LeftSourceSequence || existing.RightCheckpoint < existing.RightSourceSequence
		action := TypedTableArrangementAdvisorReuse
		memory := TypedTableArrangementMemoryEstimate{}
		if stale {
			action = TypedTableArrangementAdvisorHydrateThenReuse
			memory = typedTableArrangementHydrationMemory(
				addTypedTableArrangementBytes(
					typedTableArrangementPending(existing.LeftCheckpoint, existing.LeftSourceSequence),
					typedTableArrangementPending(existing.RightCheckpoint, existing.RightSourceSequence),
				),
				options.ReplayBytesPerChange,
			)
		}
		candidates = append(candidates, TypedTableJoinArrangementRecommendation{
			LeftTableName:       existing.LeftTableName,
			RightTableName:      existing.RightTableName,
			Definition:          existing.Definition,
			Action:              action,
			References:          existing.References,
			Shared:              existing.Shared || existing.References > 1,
			LeftCheckpoint:      existing.LeftCheckpoint,
			LeftSourceSequence:  existing.LeftSourceSequence,
			RightCheckpoint:     existing.RightCheckpoint,
			RightSourceSequence: existing.RightSourceSequence,
			Stale:               stale,
			Memory:              memory,
		})
	}
	sort.Slice(candidates, func(left, right int) bool {
		return lessTypedTableJoinArrangementRecommendation(candidates[left], candidates[right])
	})
	if len(candidates) > 0 {
		return TypedTableJoinArrangementAdvice{
			Action:     candidates[0].Action,
			Memory:     candidates[0].Memory,
			Candidates: candidates,
		}
	}
	memory := typedTableArrangementCreationMemory(
		request.EstimatedStateRows,
		options.JoinStateBytesPerRow,
		options.FixedArrangementBytes,
	)
	return TypedTableJoinArrangementAdvice{
		Action: TypedTableArrangementAdvisorCreate,
		Memory: memory,
	}
}

func cloneTypedTableAggregateDefinition(definition TypedTableAggregateDefinition) TypedTableAggregateDefinition {
	definition.GroupBy = append([]string(nil), definition.GroupBy...)
	return definition
}

func typedTableArrangementHydrationMemory(changes, bytesPerChange uint64) TypedTableArrangementMemoryEstimate {
	transient := multiplyTypedTableArrangementBytes(changes, bytesPerChange)
	return TypedTableArrangementMemoryEstimate{TransientBytes: transient, TotalBytes: transient}
}

func typedTableArrangementCreationMemory(rows, bytesPerRow, fixedBytes uint64) TypedTableArrangementMemoryEstimate {
	persistent := addTypedTableArrangementBytes(fixedBytes, multiplyTypedTableArrangementBytes(rows, bytesPerRow))
	return TypedTableArrangementMemoryEstimate{PersistentBytes: persistent, TotalBytes: persistent}
}

func typedTableArrangementPending(checkpoint, sourceSequence uint64) uint64 {
	if sourceSequence <= checkpoint {
		return 0
	}
	return sourceSequence - checkpoint
}

func addTypedTableArrangementBytes(left, right uint64) uint64 {
	if ^uint64(0)-left < right {
		return ^uint64(0)
	}
	return left + right
}

func multiplyTypedTableArrangementBytes(left, right uint64) uint64 {
	if left == 0 || right == 0 {
		return 0
	}
	if left > ^uint64(0)/right {
		return ^uint64(0)
	}
	return left * right
}

func typedTableArrangementAdvisorActionRank(action TypedTableArrangementAdvisorAction) int {
	switch action {
	case TypedTableArrangementAdvisorReuse:
		return 0
	case TypedTableArrangementAdvisorHydrateThenReuse:
		return 1
	default:
		return 2
	}
}

func lessTypedTableAggregateArrangementRecommendation(
	left, right TypedTableAggregateArrangementRecommendation,
) bool {
	if leftRank, rightRank := typedTableArrangementAdvisorActionRank(left.Action), typedTableArrangementAdvisorActionRank(right.Action); leftRank != rightRank {
		return leftRank < rightRank
	}
	if left.References != right.References {
		return left.References > right.References
	}
	if left.Checkpoint != right.Checkpoint {
		return left.Checkpoint > right.Checkpoint
	}
	if left.SourceSequence != right.SourceSequence {
		return left.SourceSequence < right.SourceSequence
	}
	return typedTableAggregateArrangementKey(left.Definition) < typedTableAggregateArrangementKey(right.Definition)
}

func lessTypedTableJoinArrangementRecommendation(
	left, right TypedTableJoinArrangementRecommendation,
) bool {
	if leftRank, rightRank := typedTableArrangementAdvisorActionRank(left.Action), typedTableArrangementAdvisorActionRank(right.Action); leftRank != rightRank {
		return leftRank < rightRank
	}
	if left.References != right.References {
		return left.References > right.References
	}
	if left.LeftCheckpoint != right.LeftCheckpoint {
		return left.LeftCheckpoint > right.LeftCheckpoint
	}
	if left.RightCheckpoint != right.RightCheckpoint {
		return left.RightCheckpoint > right.RightCheckpoint
	}
	if left.LeftSourceSequence != right.LeftSourceSequence {
		return left.LeftSourceSequence < right.LeftSourceSequence
	}
	if left.RightSourceSequence != right.RightSourceSequence {
		return left.RightSourceSequence < right.RightSourceSequence
	}
	return typedTableArrangementAdvisorJoinDefinitionKey(left.Definition) < typedTableArrangementAdvisorJoinDefinitionKey(right.Definition)
}

func typedTableArrangementAdvisorJoinDefinitionKey(definition TypedTableJoinDefinition) string {
	return definition.LeftField + "\x00" + definition.RightField
}
