package hatSql

import (
	"errors"
	"sort"
	"strings"
)

const (
	// DefaultSQLTemporalIntervalArrangementCapacity bounds retained intervals
	// when the caller does not select a capacity.
	DefaultSQLTemporalIntervalArrangementCapacity = 65_536
	// MaxSQLTemporalIntervalArrangementCapacity prevents accidental unbounded
	// map and tree growth from an invalid configuration.
	MaxSQLTemporalIntervalArrangementCapacity    = 1_000_000
	maxSQLTemporalIntervalArrangementStringBytes = 4096
)

var (
	ErrSQLTemporalIntervalArrangementNil     = errors.New("SQL temporal interval arrangement is nil")
	ErrSQLTemporalIntervalArrangementInvalid = errors.New("SQL temporal interval arrangement input is invalid")
	ErrSQLTemporalIntervalArrangementFull    = errors.New("SQL temporal interval arrangement is full")
)

// SQLTemporalIntervalArrangementOptions configures a bounded temporal
// arrangement. The zero value uses DefaultSQLTemporalIntervalArrangementCapacity.
type SQLTemporalIntervalArrangementOptions struct {
	MaxIntervals int
}

// SQLTemporalInterval is a uniquely identified half-open [Start, End)
// interval. Key groups intervals belonging to one temporal dimension or join
// key; ID identifies one replaceable source record.
type SQLTemporalInterval struct {
	ID    string
	Key   string
	Start int64
	End   int64
	Row   Row
}

type sqlTemporalIntervalEntry struct {
	interval SQLTemporalInterval
	priority uint64
	maxEnd   int64
	left     *sqlTemporalIntervalEntry
	right    *sqlTemporalIntervalEntry
}

type sqlTemporalIntervalBucket struct {
	root *sqlTemporalIntervalEntry
}

// SQLTemporalIntervalArrangement maintains per-key valid-time intervals in an
// augmented treap. Upsert and Delete are expected O(log n); At and Overlap are
// O(log n + k), where k is the number of returned intervals. The arrangement
// is intentionally not synchronized; callers should serialize mutations and
// queries, matching IncrementalIntervalJoin and TemporalTable.
type SQLTemporalIntervalArrangement struct {
	maxIntervals int
	intervals    map[string]*sqlTemporalIntervalEntry
	buckets      map[string]*sqlTemporalIntervalBucket
	length       int
}

// NewSQLTemporalIntervalArrangement creates an empty bounded arrangement.
func NewSQLTemporalIntervalArrangement(options SQLTemporalIntervalArrangementOptions) (*SQLTemporalIntervalArrangement, error) {
	if options.MaxIntervals < 0 || options.MaxIntervals > MaxSQLTemporalIntervalArrangementCapacity {
		return nil, ErrSQLTemporalIntervalArrangementInvalid
	}
	if options.MaxIntervals == 0 {
		options.MaxIntervals = DefaultSQLTemporalIntervalArrangementCapacity
	}
	return &SQLTemporalIntervalArrangement{
		maxIntervals: options.MaxIntervals,
		intervals:    make(map[string]*sqlTemporalIntervalEntry),
		buckets:      make(map[string]*sqlTemporalIntervalBucket),
	}, nil
}

// Upsert inserts or replaces one interval. Replacement is atomic with
// respect to validation and does not consume another capacity slot.
func (arrangement *SQLTemporalIntervalArrangement) Upsert(interval SQLTemporalInterval) error {
	if arrangement == nil {
		return ErrSQLTemporalIntervalArrangementNil
	}
	normalized, err := normalizeSQLTemporalInterval(interval)
	if err != nil {
		return err
	}
	existing, found := arrangement.intervals[normalized.ID]
	if !found && arrangement.length >= arrangement.maxIntervals {
		return ErrSQLTemporalIntervalArrangementFull
	}
	if found {
		arrangement.removeEntry(existing)
	} else {
		arrangement.length++
	}
	entry := &sqlTemporalIntervalEntry{
		interval: normalized,
		priority: sqlTemporalIntervalPriority(normalized),
		maxEnd:   normalized.End,
	}
	arrangement.intervals[normalized.ID] = entry
	arrangement.insertEntry(entry)
	return nil
}

// Delete removes an interval by ID. It returns false when ID was not present.
func (arrangement *SQLTemporalIntervalArrangement) Delete(id string) (bool, error) {
	if arrangement == nil {
		return false, ErrSQLTemporalIntervalArrangementNil
	}
	id = strings.TrimSpace(id)
	if id == "" || len(id) > maxSQLTemporalIntervalArrangementStringBytes {
		return false, ErrSQLTemporalIntervalArrangementInvalid
	}
	entry, found := arrangement.intervals[id]
	if !found {
		return false, nil
	}
	arrangement.removeEntry(entry)
	delete(arrangement.intervals, id)
	arrangement.length--
	return true, nil
}

// At returns detached intervals containing at for one key. The interval
// boundary is half-open: Start is included and End is excluded.
func (arrangement *SQLTemporalIntervalArrangement) At(key string, at int64) ([]SQLTemporalInterval, error) {
	if arrangement == nil {
		return nil, ErrSQLTemporalIntervalArrangementNil
	}
	key, err := normalizeSQLTemporalIntervalString(key)
	if err != nil {
		return nil, err
	}
	bucket := arrangement.buckets[key]
	if bucket == nil {
		return nil, nil
	}
	result := make([]SQLTemporalInterval, 0, 1)
	querySQLTemporalIntervalAt(bucket.root, at, &result)
	return result, nil
}

// Overlap returns detached intervals intersecting the half-open [start, end)
// query range for one key. Results are ordered by start, end, and ID.
func (arrangement *SQLTemporalIntervalArrangement) Overlap(key string, start, end int64) ([]SQLTemporalInterval, error) {
	if arrangement == nil {
		return nil, ErrSQLTemporalIntervalArrangementNil
	}
	key, err := normalizeSQLTemporalIntervalString(key)
	if err != nil {
		return nil, err
	}
	if start >= end {
		return nil, ErrSQLTemporalIntervalArrangementInvalid
	}
	bucket := arrangement.buckets[key]
	if bucket == nil {
		return nil, nil
	}
	result := make([]SQLTemporalInterval, 0, 1)
	querySQLTemporalIntervalOverlap(bucket.root, start, end, &result)
	return result, nil
}

// Snapshot returns all detached intervals in deterministic key/time/ID order.
func (arrangement *SQLTemporalIntervalArrangement) Snapshot() []SQLTemporalInterval {
	if arrangement == nil || arrangement.length == 0 {
		return nil
	}
	entries := make([]*sqlTemporalIntervalEntry, 0, arrangement.length)
	for _, entry := range arrangement.intervals {
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(left, right int) bool {
		return compareSQLTemporalIntervals(entries[left].interval, entries[right].interval) < 0
	})
	return cloneSQLTemporalIntervals(entries)
}

// Len returns the number of retained intervals.
func (arrangement *SQLTemporalIntervalArrangement) Len() int {
	if arrangement == nil {
		return 0
	}
	return arrangement.length
}

func normalizeSQLTemporalInterval(interval SQLTemporalInterval) (SQLTemporalInterval, error) {
	var err error
	interval.ID, err = normalizeSQLTemporalIntervalString(interval.ID)
	if err != nil {
		return SQLTemporalInterval{}, err
	}
	interval.Key, err = normalizeSQLTemporalIntervalString(interval.Key)
	if err != nil {
		return SQLTemporalInterval{}, err
	}
	if interval.Start >= interval.End {
		return SQLTemporalInterval{}, ErrSQLTemporalIntervalArrangementInvalid
	}
	interval.Row = cloneSQLTemporalIntervalRow(interval.Row)
	return interval, nil
}

func normalizeSQLTemporalIntervalString(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > maxSQLTemporalIntervalArrangementStringBytes {
		return "", ErrSQLTemporalIntervalArrangementInvalid
	}
	return value, nil
}

func cloneSQLTemporalIntervalRow(row Row) Row {
	if row == nil {
		return nil
	}
	return CloneRows([]Row{row})[0]
}

func cloneSQLTemporalIntervals(entries []*sqlTemporalIntervalEntry) []SQLTemporalInterval {
	if len(entries) == 0 {
		return nil
	}
	result := make([]SQLTemporalInterval, len(entries))
	for index, entry := range entries {
		result[index] = entry.interval
		result[index].Row = cloneSQLTemporalIntervalRow(entry.interval.Row)
	}
	return result
}

func (arrangement *SQLTemporalIntervalArrangement) insertEntry(entry *sqlTemporalIntervalEntry) {
	bucket := arrangement.buckets[entry.interval.Key]
	if bucket == nil {
		bucket = &sqlTemporalIntervalBucket{}
		arrangement.buckets[entry.interval.Key] = bucket
	}
	bucket.root = insertSQLTemporalIntervalEntry(bucket.root, entry)
}

func (arrangement *SQLTemporalIntervalArrangement) removeEntry(entry *sqlTemporalIntervalEntry) {
	bucket := arrangement.buckets[entry.interval.Key]
	if bucket == nil {
		return
	}
	bucket.root = deleteSQLTemporalIntervalEntry(bucket.root, entry)
	if bucket.root == nil {
		delete(arrangement.buckets, entry.interval.Key)
	}
}

func insertSQLTemporalIntervalEntry(root, entry *sqlTemporalIntervalEntry) *sqlTemporalIntervalEntry {
	if root == nil {
		return entry
	}
	if compareSQLTemporalIntervals(entry.interval, root.interval) < 0 {
		root.left = insertSQLTemporalIntervalEntry(root.left, entry)
		if root.left.priority > root.priority {
			root = rotateSQLTemporalIntervalRight(root)
		}
	} else {
		root.right = insertSQLTemporalIntervalEntry(root.right, entry)
		if root.right.priority > root.priority {
			root = rotateSQLTemporalIntervalLeft(root)
		}
	}
	return updateSQLTemporalIntervalEntry(root)
}

func deleteSQLTemporalIntervalEntry(root, target *sqlTemporalIntervalEntry) *sqlTemporalIntervalEntry {
	if root == nil {
		return nil
	}
	comparison := compareSQLTemporalIntervals(target.interval, root.interval)
	if comparison < 0 {
		root.left = deleteSQLTemporalIntervalEntry(root.left, target)
		return updateSQLTemporalIntervalEntry(root)
	}
	if comparison > 0 {
		root.right = deleteSQLTemporalIntervalEntry(root.right, target)
		return updateSQLTemporalIntervalEntry(root)
	}
	if root.left == nil {
		return root.right
	}
	if root.right == nil {
		return root.left
	}
	if root.left.priority > root.right.priority {
		root = rotateSQLTemporalIntervalRight(root)
		root.right = deleteSQLTemporalIntervalEntry(root.right, target)
	} else {
		root = rotateSQLTemporalIntervalLeft(root)
		root.left = deleteSQLTemporalIntervalEntry(root.left, target)
	}
	return updateSQLTemporalIntervalEntry(root)
}

func rotateSQLTemporalIntervalRight(root *sqlTemporalIntervalEntry) *sqlTemporalIntervalEntry {
	child := root.left
	root.left = child.right
	child.right = updateSQLTemporalIntervalEntry(root)
	return updateSQLTemporalIntervalEntry(child)
}

func rotateSQLTemporalIntervalLeft(root *sqlTemporalIntervalEntry) *sqlTemporalIntervalEntry {
	child := root.right
	root.right = child.left
	child.left = updateSQLTemporalIntervalEntry(root)
	return updateSQLTemporalIntervalEntry(child)
}

func updateSQLTemporalIntervalEntry(entry *sqlTemporalIntervalEntry) *sqlTemporalIntervalEntry {
	if entry == nil {
		return nil
	}
	entry.maxEnd = entry.interval.End
	if entry.left != nil && entry.left.maxEnd > entry.maxEnd {
		entry.maxEnd = entry.left.maxEnd
	}
	if entry.right != nil && entry.right.maxEnd > entry.maxEnd {
		entry.maxEnd = entry.right.maxEnd
	}
	return entry
}

func querySQLTemporalIntervalAt(root *sqlTemporalIntervalEntry, at int64, result *[]SQLTemporalInterval) {
	if root == nil || root.maxEnd <= at {
		return
	}
	if root.left != nil {
		querySQLTemporalIntervalAt(root.left, at, result)
	}
	if root.interval.Start <= at && at < root.interval.End {
		appendSQLTemporalIntervalResult(result, root)
	}
	if root.interval.Start <= at {
		querySQLTemporalIntervalAt(root.right, at, result)
	}
}

func querySQLTemporalIntervalOverlap(root *sqlTemporalIntervalEntry, start, end int64, result *[]SQLTemporalInterval) {
	if root == nil || root.maxEnd <= start {
		return
	}
	if root.left != nil {
		querySQLTemporalIntervalOverlap(root.left, start, end, result)
	}
	if root.interval.Start < end && root.interval.End > start {
		appendSQLTemporalIntervalResult(result, root)
	}
	if root.interval.Start < end {
		querySQLTemporalIntervalOverlap(root.right, start, end, result)
	}
}

func appendSQLTemporalIntervalResult(result *[]SQLTemporalInterval, entry *sqlTemporalIntervalEntry) {
	interval := entry.interval
	interval.Row = cloneSQLTemporalIntervalRow(interval.Row)
	*result = append(*result, interval)
}

func compareSQLTemporalIntervals(left, right SQLTemporalInterval) int {
	if left.Start < right.Start {
		return -1
	}
	if left.Start > right.Start {
		return 1
	}
	if left.End < right.End {
		return -1
	}
	if left.End > right.End {
		return 1
	}
	if left.ID < right.ID {
		return -1
	}
	if left.ID > right.ID {
		return 1
	}
	return 0
}

func sqlTemporalIntervalPriority(interval SQLTemporalInterval) uint64 {
	value := uint64(1469598103934665603)
	for _, part := range []string{interval.ID, interval.Key} {
		for index := 0; index < len(part); index++ {
			value ^= uint64(part[index])
			value *= 1099511628211
		}
		value ^= 0xff
		value *= 1099511628211
	}
	value ^= uint64(interval.Start)
	value *= 1099511628211
	value ^= uint64(interval.End)
	value *= 1099511628211
	return value
}
