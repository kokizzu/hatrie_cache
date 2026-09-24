package hatCache

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"hatrie_cache/hat/hatSql"
)

const (
	sqlJSONValidityArrangementKey = "__all__"
	sqlJSONValidityMinNanos       = int64(-1 << 63)
	sqlJSONValidityMaxNanos       = int64(1<<63 - 1)
)

var errSQLJSONValidityIndexUnavailable = fmt.Errorf("SQL JSON validity index is unavailable for source")

type sqlJSONValidityIndex struct {
	sqlJSONIndexState
	validFromField string
	validToField   string
	arrangement    *hatSql.SQLTemporalIntervalArrangement
}

func sqlJSONValidityIndexIdentifier(validFromField, validToField string) string {
	return validFromField + "\x00" + validToField
}

// CreateSQLJSONValidityIndex configures an opt-in half-open validity index for
// JSON timestamp fields. NULL or missing bounds are treated as unbounded.
// The source remains unchanged and the index rebuilds lazily on first use.
func (ht *HatTrie) CreateSQLJSONValidityIndex(key, validFromField, validToField string) error {
	if ht == nil || strings.TrimSpace(key) == "" || strings.TrimSpace(validFromField) == "" || strings.TrimSpace(validToField) == "" {
		return fmt.Errorf("SQL JSON validity index requires a cache key and two fields")
	}
	if validFromField == validToField {
		return fmt.Errorf("SQL JSON validity index fields must be different")
	}
	ht.registerSQLJSONIndexSource(key)
	ht.sqlIndexMu.Lock()
	defer ht.sqlIndexMu.Unlock()
	if ht.sqlJSONValidityIndexes == nil {
		ht.sqlJSONValidityIndexes = make(map[string]map[string]*sqlJSONValidityIndex)
	}
	if ht.sqlJSONValidityIndexes[key] == nil {
		ht.sqlJSONValidityIndexes[key] = make(map[string]*sqlJSONValidityIndex)
	}
	identifier := sqlJSONValidityIndexIdentifier(validFromField, validToField)
	ht.sqlJSONValidityIndexes[key][identifier] = &sqlJSONValidityIndex{
		validFromField: validFromField,
		validToField:   validToField,
	}
	return nil
}

// ResolveSQLTemporalValiditySource returns candidate rows whose configured
// validity interval contains at. The SQL executor rechecks VALID_AT, so rows
// returned here may safely be a superset of the final result.
func (ht *HatTrie) ResolveSQLTemporalValiditySource(name, key string, at time.Time, validFromField, validToField string) ([]SQLRow, bool, error) {
	if ht == nil || name != "CACHE" || strings.TrimSpace(key) == "" || validFromField == "" || validToField == "" {
		return nil, false, nil
	}
	atNanos, ok := sqlJSONValidityNanos(at)
	if !ok {
		return nil, false, nil
	}
	source, err := ht.sqlJSONSource(key)
	if err != nil {
		return nil, false, err
	}
	ht.sqlIndexMu.Lock()
	defer ht.sqlIndexMu.Unlock()
	index := ht.sqlJSONValidityIndexes[key][sqlJSONValidityIndexIdentifier(validFromField, validToField)]
	if index == nil {
		return nil, false, nil
	}
	if !ht.sqlJSONIndexSourceAdmittedLocked(source) {
		return nil, false, nil
	}
	if !source.current(index.sqlJSONIndexState) || index.arrangement == nil {
		snapshot, snapshotErr := ht.sqlJSONIndexSnapshotForSourceLocked(key, source)
		if snapshotErr != nil {
			if snapshotErr == errSQLJSONIndexAdmissionDenied {
				return nil, false, nil
			}
			return nil, false, snapshotErr
		}
		if refreshErr := refreshSQLJSONValidityIndexSource(index, source, snapshot.rows); refreshErr != nil {
			if refreshErr == errSQLJSONValidityIndexUnavailable {
				return nil, false, nil
			}
			return nil, false, refreshErr
		}
	}
	intervals, err := index.arrangement.At(sqlJSONValidityArrangementKey, atNanos)
	if err != nil {
		return nil, false, err
	}
	rows := make([]SQLRow, 0, len(intervals))
	for _, interval := range intervals {
		rows = append(rows, interval.Row)
	}
	return rows, true, nil
}

func refreshSQLJSONValidityIndexSource(index *sqlJSONValidityIndex, source sqlJSONSource, rows []SQLRow) error {
	if index == nil || len(rows) > hatSql.MaxSQLTemporalIntervalArrangementCapacity {
		return errSQLJSONValidityIndexUnavailable
	}
	arrangement, err := hatSql.NewSQLTemporalIntervalArrangement(hatSql.SQLTemporalIntervalArrangementOptions{MaxIntervals: len(rows)})
	if err != nil {
		return err
	}
	for ordinal, row := range rows {
		start, err := sqlJSONValidityBound(row[index.validFromField], sqlJSONValidityMinNanos)
		if err != nil {
			return errSQLJSONValidityIndexUnavailable
		}
		end, err := sqlJSONValidityBound(row[index.validToField], sqlJSONValidityMaxNanos)
		if err != nil {
			return errSQLJSONValidityIndexUnavailable
		}
		if start >= end {
			continue
		}
		if err := arrangement.Upsert(hatSql.SQLTemporalInterval{
			ID:    strconv.Itoa(ordinal),
			Key:   sqlJSONValidityArrangementKey,
			Start: start,
			End:   end,
			Row:   row,
		}); err != nil {
			return errSQLJSONValidityIndexUnavailable
		}
	}
	index.arrangement = arrangement
	index.sqlJSONIndexState = sqlJSONIndexState{raw: source.raw, generation: source.generation, ready: true}
	return nil
}

func sqlJSONValidityBound(value interface{}, unbounded int64) (int64, error) {
	if value == nil {
		return unbounded, nil
	}
	timestamp, err := hatSql.ParseSQLTimestamp(value)
	if err != nil {
		return 0, err
	}
	nanos, ok := sqlJSONValidityNanos(timestamp)
	if !ok {
		return 0, errSQLJSONValidityIndexUnavailable
	}
	return nanos, nil
}

func sqlJSONValidityNanos(timestamp time.Time) (int64, bool) {
	if timestamp.Year() < 1678 || timestamp.Year() > 2262 {
		return 0, false
	}
	return timestamp.UTC().UnixNano(), true
}
