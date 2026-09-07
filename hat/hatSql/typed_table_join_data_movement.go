package hatSql

// TypedTableJoinDataMovement is a cumulative accounting snapshot for change
// records offered to both sides of a typed table join. LogicalBytes is a
// deterministic payload estimate, not the size of a particular wire codec.
type TypedTableJoinDataMovement struct {
	LeftChanges  uint64 `json:"left_changes"`
	RightChanges uint64 `json:"right_changes"`
	LeftRows     uint64 `json:"left_rows"`
	RightRows    uint64 `json:"right_rows"`
	LeftBytes    uint64 `json:"left_bytes"`
	RightBytes   uint64 `json:"right_bytes"`
}

// DataMovement returns cumulative input accounting for changes offered to the
// join. It includes stale and rejected batches because those records still
// crossed the caller's transport boundary.
func (join *TypedTableJoin) DataMovement() TypedTableJoinDataMovement {
	if join == nil {
		return TypedTableJoinDataMovement{}
	}
	join.mu.RLock()
	defer join.mu.RUnlock()
	return join.dataMovement
}

func (join *TypedTableJoin) recordLeftDataMovement(changes []TypedTableChange) {
	if !join.trackDataMovement {
		return
	}
	join.dataMovement.LeftChanges = typedTableJoinSaturatingAdd(join.dataMovement.LeftChanges, uint64(len(changes)))
	for _, change := range changes {
		if change.Before != nil {
			join.dataMovement.LeftRows = typedTableJoinSaturatingAdd(join.dataMovement.LeftRows, 1)
		}
		if change.After != nil {
			join.dataMovement.LeftRows = typedTableJoinSaturatingAdd(join.dataMovement.LeftRows, 1)
		}
		join.dataMovement.LeftBytes = typedTableJoinSaturatingAdd(join.dataMovement.LeftBytes, typedTableJoinLogicalChangeBytes(change))
	}
}

func (join *TypedTableJoin) recordRightDataMovement(changes []TypedTableChange) {
	if !join.trackDataMovement {
		return
	}
	join.dataMovement.RightChanges = typedTableJoinSaturatingAdd(join.dataMovement.RightChanges, uint64(len(changes)))
	for _, change := range changes {
		if change.Before != nil {
			join.dataMovement.RightRows = typedTableJoinSaturatingAdd(join.dataMovement.RightRows, 1)
		}
		if change.After != nil {
			join.dataMovement.RightRows = typedTableJoinSaturatingAdd(join.dataMovement.RightRows, 1)
		}
		join.dataMovement.RightBytes = typedTableJoinSaturatingAdd(join.dataMovement.RightBytes, typedTableJoinLogicalChangeBytes(change))
	}
}

func typedTableJoinLogicalChangeBytes(change TypedTableChange) uint64 {
	bytes := uint64(8 + len(change.Operation) + len(change.Key))
	if change.Before != nil {
		bytes += typedTableJoinLogicalRowBytes(change.Before)
	}
	if change.After != nil {
		bytes += typedTableJoinLogicalRowBytes(change.After)
	}
	return bytes
}

func typedTableJoinLogicalRowBytes(values []TypedTableValue) uint64 {
	bytes := uint64(4)
	for _, value := range values {
		bytes += 2
		if !value.Valid {
			continue
		}
		switch value.Kind {
		case TypedTableString:
			bytes += uint64(4 + len(value.String))
		case TypedTableInt64, TypedTableFloat64:
			bytes += 8
		case TypedTableBool:
			bytes++
		}
	}
	return bytes
}

func typedTableJoinSaturatingAdd(left, right uint64) uint64 {
	if ^uint64(0)-left < right {
		return ^uint64(0)
	}
	return left + right
}
