package hatSql

const sqlHashGroupAggregateSmallIndexCapacity = 4

type sqlHashGroupAggregateIndexEntry struct {
	key   string
	index int
}

type sqlHashGroupAggregateIndexes struct {
	small    [sqlHashGroupAggregateSmallIndexCapacity]sqlHashGroupAggregateIndexEntry
	smallLen uint8
	large    map[string]int
}

func newSQLHashGroupAggregateIndexes() sqlHashGroupAggregateIndexes {
	return sqlHashGroupAggregateIndexes{}
}

func (indexes *sqlHashGroupAggregateIndexes) find(key string) (int, bool) {
	if indexes.large != nil {
		index, ok := indexes.large[key]
		return index, ok
	}
	for index := 0; index < int(indexes.smallLen); index++ {
		entry := indexes.small[index]
		if entry.key == key {
			return entry.index, true
		}
	}
	return 0, false
}

func (indexes *sqlHashGroupAggregateIndexes) add(key string, index int) {
	if indexes.large == nil {
		if int(indexes.smallLen) < len(indexes.small) {
			indexes.small[indexes.smallLen] = sqlHashGroupAggregateIndexEntry{key: key, index: index}
			indexes.smallLen++
			return
		}
		indexes.large = make(map[string]int)
		for index := 0; index < int(indexes.smallLen); index++ {
			entry := indexes.small[index]
			indexes.large[entry.key] = entry.index
		}
		for index := range indexes.small {
			indexes.small[index] = sqlHashGroupAggregateIndexEntry{}
		}
		indexes.smallLen = 0
	}
	indexes.large[key] = index
}
