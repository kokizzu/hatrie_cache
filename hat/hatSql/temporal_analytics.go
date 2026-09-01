package hatSql

import (
	"sort"
	"time"
)

type TemporalVersion struct {
	At  time.Time
	Row Row
}
type TemporalTable struct{ versions map[string][]TemporalVersion }

func NewTemporalTable() *TemporalTable {
	return &TemporalTable{versions: make(map[string][]TemporalVersion)}
}
func (table *TemporalTable) Upsert(key string, at time.Time, row Row) {
	versions := table.versions[key]
	version := TemporalVersion{At: at, Row: CloneRows([]Row{row})[0]}
	if len(versions) == 0 || !at.Before(versions[len(versions)-1].At) {
		table.versions[key] = append(versions, version)
		return
	}
	// Insert after equal timestamps so AS OF deterministically returns the
	// latest write at that timestamp.
	index := sort.Search(len(versions), func(index int) bool { return versions[index].At.After(at) })
	versions = append(versions, TemporalVersion{})
	copy(versions[index+1:], versions[index:])
	versions[index] = version
	table.versions[key] = versions
}
func (table *TemporalTable) AsOf(key string, at time.Time) (Row, bool) {
	versions := table.versions[key]
	index := sort.Search(len(versions), func(index int) bool { return versions[index].At.After(at) })
	if index == 0 {
		return nil, false
	}
	return CloneRows([]Row{versions[index-1].Row})[0], true
}
func (table *TemporalTable) RetainAfter(at time.Time, verified bool) int {
	if !verified {
		return 0
	}
	removed := 0
	for key, versions := range table.versions {
		keep := versions[:0]
		for _, version := range versions {
			if version.At.Before(at) {
				removed++
			} else {
				keep = append(keep, version)
			}
		}
		table.versions[key] = keep
	}
	return removed
}

type Watermark struct {
	allowed time.Duration
	latest  time.Time
}

func NewWatermark(allowed time.Duration) *Watermark { return &Watermark{allowed: allowed} }
func (watermark *Watermark) Observe(at time.Time) bool {
	if at.After(watermark.latest) {
		watermark.latest = at
		return false
	}
	return !at.After(watermark.latest.Add(-watermark.allowed))
}

type TimedRow struct {
	At  time.Time
	Row Row
}
type Session []TimedRow

func Sessionize(rows []TimedRow, gap time.Duration) []Session {
	if len(rows) == 0 {
		return nil
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].At.Before(rows[j].At) })
	result := []Session{{rows[0]}}
	for _, row := range rows[1:] {
		last := &result[len(result)-1]
		if row.At.Sub((*last)[len(*last)-1].At) > gap {
			result = append(result, Session{row})
		} else {
			*last = append(*last, row)
		}
	}
	return result
}

type TimeInterval struct{ Start, End time.Time }

func IntervalsOverlap(left, right TimeInterval) bool {
	return left.Start.Before(right.End) && right.Start.Before(left.End)
}
