package hatSql

// TypedTableJoinOptions controls optional maintenance behavior for a typed
// table join. The zero value preserves the original full-row arrangement.
type TypedTableJoinOptions struct {
	SemijoinReduction bool
	TrackDataMovement bool
}

// TypedTableJoinStats describes the rows currently retained by a join. With
// semijoin reduction enabled, pending rows retain only their source key and
// join-key membership until a counterpart exists.
type TypedTableJoinStats struct {
	SemijoinReduction bool
	LeftRows          int
	RightRows         int
	LeftPending       int
	RightPending      int
}

// Stats returns a point-in-time retention summary for the join.
func (join *TypedTableJoin) Stats() TypedTableJoinStats {
	if join == nil {
		return TypedTableJoinStats{}
	}
	join.mu.RLock()
	defer join.mu.RUnlock()
	return TypedTableJoinStats{
		SemijoinReduction: join.semijoinReduction,
		LeftRows:          len(join.leftRows),
		RightRows:         len(join.rightRows),
		LeftPending:       len(join.leftPendingKeys),
		RightPending:      len(join.rightPendingKeys),
	}
}

func (join *TypedTableJoin) advanceLeftCheckpoint(change TypedTableChange) {
	join.leftCheckpoint = change.Sequence
	if !join.semijoinReduction || change.Operation == "DELETE" {
		return
	}
	if valueKey, joined := typedTableJoinValue(change.After, join.leftField); joined {
		join.activateLeftPending(valueKey)
	}
}

func (join *TypedTableJoin) advanceRightCheckpoint(change TypedTableChange) {
	join.rightCheckpoint = change.Sequence
	if !join.semijoinReduction || change.Operation == "DELETE" {
		return
	}
	if valueKey, joined := typedTableJoinValue(change.After, join.rightField); joined {
		join.activateRightPending(valueKey)
	}
}

func (join *TypedTableJoin) addLeftPending(valueKey, key string) {
	addTypedTableJoinPending(join.leftPending, join.leftPendingKeys, valueKey, key)
}

func (join *TypedTableJoin) addRightPending(valueKey, key string) {
	addTypedTableJoinPending(join.rightPending, join.rightPendingKeys, valueKey, key)
}

func (join *TypedTableJoin) removeLeftPending(key string) {
	removeTypedTableJoinPending(join.leftPending, join.leftPendingKeys, key)
}

func (join *TypedTableJoin) removeRightPending(key string) {
	removeTypedTableJoinPending(join.rightPending, join.rightPendingKeys, key)
}

func addTypedTableJoinPending(groups map[string]map[string]struct{}, keys map[string]string, valueKey, key string) {
	if previous, found := keys[key]; found {
		if previous == valueKey {
			return
		}
		removeTypedTableJoinPending(groups, keys, key)
	}
	if groups[valueKey] == nil {
		groups[valueKey] = map[string]struct{}{}
	}
	groups[valueKey][key] = struct{}{}
	keys[key] = valueKey
}

func removeTypedTableJoinPending(groups map[string]map[string]struct{}, keys map[string]string, key string) {
	valueKey, found := keys[key]
	if !found {
		return
	}
	removeTypedTableJoinIndex(groups, valueKey, key)
	delete(keys, key)
}

func (join *TypedTableJoin) demoteLeft(valueKey string) {
	keys := make([]string, 0, len(join.leftIndex[valueKey]))
	for key := range join.leftIndex[valueKey] {
		keys = append(keys, key)
	}
	for _, key := range keys {
		if _, found := join.leftRows[key]; !found {
			removeTypedTableJoinIndex(join.leftIndex, valueKey, key)
			continue
		}
		delete(join.leftRows, key)
		removeTypedTableJoinIndex(join.leftIndex, valueKey, key)
		join.addLeftPending(valueKey, key)
	}
}

func (join *TypedTableJoin) demoteRight(valueKey string) {
	keys := make([]string, 0, len(join.rightIndex[valueKey]))
	for key := range join.rightIndex[valueKey] {
		keys = append(keys, key)
	}
	for _, key := range keys {
		if _, found := join.rightRows[key]; !found {
			removeTypedTableJoinIndex(join.rightIndex, valueKey, key)
			continue
		}
		delete(join.rightRows, key)
		removeTypedTableJoinIndex(join.rightIndex, valueKey, key)
		join.addRightPending(valueKey, key)
	}
}

func (join *TypedTableJoin) activateLeftPending(valueKey string) {
	if len(join.leftPending[valueKey]) == 0 {
		return
	}
	values := make([]struct {
		key    string
		values []TypedTableValue
	}, 0, len(join.leftPending[valueKey]))
	for key := range join.leftPending[valueKey] {
		row, found, ready := typedTableJoinSourceRowAtCheckpoint(join.left, key, join.leftCheckpoint)
		if !ready {
			return
		}
		if found {
			values = append(values, struct {
				key    string
				values []TypedTableValue
			}{key: key, values: row})
		} else {
			join.removeLeftPending(key)
		}
	}
	for _, pending := range values {
		join.removeLeftPending(pending.key)
		join.addLeft(pending.key, pending.values)
	}
}

func (join *TypedTableJoin) activateRightPending(valueKey string) {
	if len(join.rightPending[valueKey]) == 0 {
		return
	}
	values := make([]struct {
		key    string
		values []TypedTableValue
	}, 0, len(join.rightPending[valueKey]))
	for key := range join.rightPending[valueKey] {
		row, found, ready := typedTableJoinSourceRowAtCheckpoint(join.right, key, join.rightCheckpoint)
		if !ready {
			return
		}
		if found {
			values = append(values, struct {
				key    string
				values []TypedTableValue
			}{key: key, values: row})
		} else {
			join.removeRightPending(key)
		}
	}
	for _, pending := range values {
		join.removeRightPending(pending.key)
		join.addRight(pending.key, pending.values)
	}
}

func typedTableJoinSourceRowAtCheckpoint(table *TypedTable, key string, checkpoint uint64) ([]TypedTableValue, bool, bool) {
	if table == nil {
		return nil, false, false
	}
	table.mu.RLock()
	defer table.mu.RUnlock()
	if table.sequence != checkpoint {
		return nil, false, false
	}
	index, found := table.positions[key]
	if !found || table.typedTableRowDeletedLocked(index) {
		return nil, false, true
	}
	return table.rowLocked(index), true, true
}
