package hatSql

import (
	"crypto/sha256"
	"encoding/json"
	"sort"
	"strconv"
	"sync"
)

// ImportDeduplicator remembers content hashes scoped by source offset.
type ImportDeduplicator struct {
	mu   sync.Mutex
	seen map[string]struct{}
}

func NewImportDeduplicator() *ImportDeduplicator {
	return &ImportDeduplicator{seen: make(map[string]struct{})}
}
func (dedup *ImportDeduplicator) Seen(source string, offset int64, data []byte) bool {
	if dedup == nil {
		return false
	}
	hash := sha256.Sum256(data)
	key := source + ":" + strconv.FormatInt(offset, 10) + ":" + string(hash[:])
	dedup.mu.Lock()
	defer dedup.mu.Unlock()
	if _, ok := dedup.seen[key]; ok {
		return true
	}
	dedup.seen[key] = struct{}{}
	return false
}

type ExternalTableDiff struct {
	AddedColumns, RemovedColumns []string
	AddedRows, RemovedRows       []Row
}

// DiffExternalTables reports deterministic schema and data differences. Rows
// are compared by canonical JSON encoding, suitable for backup comparisons.
func DiffExternalTables(left, right ExternalTable) ExternalTableDiff {
	result := ExternalTableDiff{}
	leftColumns := make(map[string]struct{}, len(left.Columns))
	rightColumns := make(map[string]struct{}, len(right.Columns))
	for _, column := range left.Columns {
		leftColumns[column] = struct{}{}
	}
	for _, column := range right.Columns {
		rightColumns[column] = struct{}{}
	}
	for column := range rightColumns {
		if _, ok := leftColumns[column]; !ok {
			result.AddedColumns = append(result.AddedColumns, column)
		}
	}
	for column := range leftColumns {
		if _, ok := rightColumns[column]; !ok {
			result.RemovedColumns = append(result.RemovedColumns, column)
		}
	}
	sort.Strings(result.AddedColumns)
	sort.Strings(result.RemovedColumns)
	leftRows := externalRowSet(left.Rows)
	rightRows := externalRowSet(right.Rows)
	for key, row := range rightRows {
		if _, ok := leftRows[key]; !ok {
			result.AddedRows = append(result.AddedRows, row)
		}
	}
	for key, row := range leftRows {
		if _, ok := rightRows[key]; !ok {
			result.RemovedRows = append(result.RemovedRows, row)
		}
	}
	sortRows(result.AddedRows)
	sortRows(result.RemovedRows)
	return result
}
func externalRowSet(rows []Row) map[string]Row {
	result := make(map[string]Row, len(rows))
	for _, row := range rows {
		data, _ := json.Marshal(row)
		result[string(data)] = CloneRows([]Row{row})[0]
	}
	return result
}
func sortRows(rows []Row) {
	sort.Slice(rows, func(i, j int) bool {
		left, _ := json.Marshal(rows[i])
		right, _ := json.Marshal(rows[j])
		return string(left) < string(right)
	})
}
