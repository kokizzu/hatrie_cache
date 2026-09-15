package hatSql

const typedTableTTLNoExpiryPosition = -1

type typedTableTTLExpiryEntry struct {
	deadline int64
	row      int
}

func (state *typedTableTTLState) ensureExpiryPosition(index int) {
	for len(state.expiryPositions) <= index {
		state.expiryPositions = append(state.expiryPositions, typedTableTTLNoExpiryPosition)
	}
}

func (state *typedTableTTLState) expiryLess(left, right int) bool {
	a := state.expiryHeap[left]
	b := state.expiryHeap[right]
	if a.deadline != b.deadline {
		return a.deadline < b.deadline
	}
	return a.row < b.row
}

func (state *typedTableTTLState) expirySwap(left, right int) {
	state.expiryHeap[left], state.expiryHeap[right] = state.expiryHeap[right], state.expiryHeap[left]
	state.expiryPositions[state.expiryHeap[left].row] = left
	state.expiryPositions[state.expiryHeap[right].row] = right
}

func (state *typedTableTTLState) expirySiftUp(position int) {
	for position > 0 {
		parent := (position - 1) / 2
		if !state.expiryLess(position, parent) {
			return
		}
		state.expirySwap(position, parent)
		position = parent
	}
}

func (state *typedTableTTLState) expirySiftDown(position int) {
	for {
		left := position*2 + 1
		if left >= len(state.expiryHeap) {
			return
		}
		smallest := left
		right := left + 1
		if right < len(state.expiryHeap) && state.expiryLess(right, left) {
			smallest = right
		}
		if !state.expiryLess(smallest, position) {
			return
		}
		state.expirySwap(position, smallest)
		position = smallest
	}
}

func (state *typedTableTTLState) expiryFix(position int) {
	if position > 0 && state.expiryLess(position, (position-1)/2) {
		state.expirySiftUp(position)
		return
	}
	state.expirySiftDown(position)
}

func (state *typedTableTTLState) expiryUpsert(row int, deadline int64) {
	state.ensureExpiryPosition(row)
	position := state.expiryPositions[row]
	if position == typedTableTTLNoExpiryPosition {
		state.expiryPositions[row] = len(state.expiryHeap)
		state.expiryHeap = append(state.expiryHeap, typedTableTTLExpiryEntry{deadline: deadline, row: row})
		state.expirySiftUp(len(state.expiryHeap) - 1)
		return
	}
	state.expiryHeap[position].deadline = deadline
	state.expiryFix(position)
}

func (state *typedTableTTLState) expiryRemove(row int) {
	if row < 0 || row >= len(state.expiryPositions) {
		return
	}
	position := state.expiryPositions[row]
	if position == typedTableTTLNoExpiryPosition {
		return
	}
	last := len(state.expiryHeap) - 1
	state.expiryPositions[row] = typedTableTTLNoExpiryPosition
	if position == last {
		state.expiryHeap = state.expiryHeap[:last]
		return
	}
	state.expiryHeap[position] = state.expiryHeap[last]
	state.expiryHeap = state.expiryHeap[:last]
	state.expiryPositions[state.expiryHeap[position].row] = position
	state.expiryFix(position)
}

func (state *typedTableTTLState) expiryPop() (typedTableTTLExpiryEntry, bool) {
	if len(state.expiryHeap) == 0 {
		return typedTableTTLExpiryEntry{}, false
	}
	entry := state.expiryHeap[0]
	state.expiryPositions[entry.row] = typedTableTTLNoExpiryPosition
	last := len(state.expiryHeap) - 1
	if last == 0 {
		state.expiryHeap = state.expiryHeap[:0]
		return entry, true
	}
	state.expiryHeap[0] = state.expiryHeap[last]
	state.expiryHeap = state.expiryHeap[:last]
	state.expiryPositions[state.expiryHeap[0].row] = 0
	state.expirySiftDown(0)
	return entry, true
}

func (state *typedTableTTLState) expiryMove(from, to int) {
	if from == to || from < 0 || from >= len(state.expiryPositions) {
		return
	}
	state.ensureExpiryPosition(to)
	position := state.expiryPositions[from]
	state.expiryPositions[from] = typedTableTTLNoExpiryPosition
	state.expiryPositions[to] = position
	if position != typedTableTTLNoExpiryPosition {
		state.expiryHeap[position].row = to
	}
}

func (state *typedTableTTLState) expiryTruncate(length int) {
	if length < len(state.expiryPositions) {
		state.expiryPositions = state.expiryPositions[:length]
	}
}

func typedTableTTLEventDeadline(value int64, lifetime int64) (int64, bool) {
	if value > int64(^uint64(0)>>1)-lifetime {
		return 0, false
	}
	return value + lifetime, true
}

func (table *TypedTable) typedTableTTLDeadlineAtLocked(index int) (int64, bool) {
	if table == nil || table.ttl == nil || index < 0 || index >= len(table.keys) {
		return 0, false
	}
	switch table.ttl.options.Mode {
	case TypedTableTTLProcessingTime:
		if index >= len(table.ttl.deadlines) {
			return 0, false
		}
		return table.ttl.deadlines[index], true
	case TypedTableTTLEventTime:
		value := table.columns[table.ttl.field].value(index)
		if !value.Valid {
			return 0, false
		}
		return typedTableTTLEventDeadline(value.Int64, int64(table.ttl.options.Lifetime))
	default:
		return 0, false
	}
}

func (table *TypedTable) rebuildTypedTableTTLExpiryIndexLocked(rowCount int) {
	if table == nil || table.ttl == nil {
		return
	}
	state := table.ttl
	state.expiryHeap = state.expiryHeap[:0]
	state.expiryPositions = make([]int, rowCount)
	for index := range state.expiryPositions {
		state.expiryPositions[index] = typedTableTTLNoExpiryPosition
	}
	for index := 0; index < rowCount; index++ {
		if deadline, ok := table.typedTableTTLDeadlineAtLocked(index); ok {
			state.expiryUpsert(index, deadline)
		}
	}
}
