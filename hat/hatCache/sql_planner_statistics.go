package hatCache

import (
	"fmt"
	"sort"
	"strings"
	"sync/atomic"

	json "github.com/goccy/go-json"
)

type sqlPlannerStatisticsEntry struct {
	epoch uint64
	value SQLWhatIfSourceStatistics
}

const maxSQLPlannerStatisticsEntries = 128

// AnalyzeSQLSource computes exact planner statistics for a CACHE JSON source.
// The scan is explicit and its result is reused by SQL what-if and planner
// consumers until the cache is mutated.
func (ht *HatTrie) AnalyzeSQLSource(name, key string, fields ...string) (SQLWhatIfSourceStatistics, error) {
	if ht == nil {
		return SQLWhatIfSourceStatistics{}, ErrNilHatTrie
	}
	name = strings.ToUpper(strings.TrimSpace(name))
	if name != "CACHE" {
		return SQLWhatIfSourceStatistics{}, fmt.Errorf("SQL planner statistics do not support source %q", name)
	}
	if key == "" {
		return SQLWhatIfSourceStatistics{}, fmt.Errorf("SQL planner statistics require a source key")
	}
	requestedFields := normalizeSQLPlannerStatisticsFields(fields)
	for attempt := 0; attempt < 3; attempt++ {
		startEpoch := atomic.LoadUint64(&ht.mutationEpoch)
		data, err := ht.GetBytesChecked(key)
		if err != nil {
			return SQLWhatIfSourceStatistics{}, err
		}
		rows, err := ht.ResolveSQLSource(name, key)
		if err != nil {
			return SQLWhatIfSourceStatistics{}, err
		}
		analysisFields := requestedFields
		if len(analysisFields) == 0 {
			analysisFields = sqlPlannerStatisticsRowFields(rows)
		}
		statistics := buildSQLPlannerStatistics(name, key, len(data), rows, analysisFields)
		endEpoch := atomic.LoadUint64(&ht.mutationEpoch)
		if startEpoch != endEpoch {
			continue
		}
		ht.sqlPlannerStatisticsMu.Lock()
		if atomic.LoadUint64(&ht.mutationEpoch) != endEpoch {
			ht.sqlPlannerStatisticsMu.Unlock()
			continue
		}
		if ht.sqlPlannerStatistics == nil {
			ht.sqlPlannerStatistics = make(map[string]sqlPlannerStatisticsEntry, maxSQLPlannerStatisticsEntries)
		}
		for cacheKey, entry := range ht.sqlPlannerStatistics {
			if entry.epoch != endEpoch {
				delete(ht.sqlPlannerStatistics, cacheKey)
			}
		}
		if len(ht.sqlPlannerStatistics) >= maxSQLPlannerStatisticsEntries {
			for cacheKey := range ht.sqlPlannerStatistics {
				delete(ht.sqlPlannerStatistics, cacheKey)
				break
			}
		}
		ht.sqlPlannerStatistics[sqlPlannerStatisticsKey(name, key)] = sqlPlannerStatisticsEntry{epoch: endEpoch, value: statistics}
		ht.sqlPlannerStatisticsMu.Unlock()
		return cloneSQLWhatIfSourceStatistics(statistics), nil
	}
	return SQLWhatIfSourceStatistics{}, fmt.Errorf("SQL planner statistics source changed during analysis")
}

// SQLWhatIfSourceStatistics returns a current explicit ANALYZE result. It
// never scans a source implicitly; unavailable tells the advisor to use its
// ordinary bounded read path.
func (ht *HatTrie) SQLWhatIfSourceStatistics(name, key string, fields []string) (SQLWhatIfSourceStatistics, bool, error) {
	if ht == nil {
		return SQLWhatIfSourceStatistics{}, false, ErrNilHatTrie
	}
	name = strings.ToUpper(strings.TrimSpace(name))
	if name != "CACHE" || key == "" {
		return SQLWhatIfSourceStatistics{}, false, nil
	}
	currentEpoch := atomic.LoadUint64(&ht.mutationEpoch)
	ht.sqlPlannerStatisticsMu.RLock()
	entry, ok := ht.sqlPlannerStatistics[sqlPlannerStatisticsKey(name, key)]
	ht.sqlPlannerStatisticsMu.RUnlock()
	if !ok || entry.epoch != currentEpoch {
		return SQLWhatIfSourceStatistics{}, false, nil
	}
	for _, field := range normalizeSQLPlannerStatisticsFields(fields) {
		if _, ok := entry.value.Fields[field]; !ok {
			return SQLWhatIfSourceStatistics{}, false, nil
		}
	}
	return cloneSQLWhatIfSourceStatistics(entry.value), true, nil
}

func sqlPlannerStatisticsKey(name, key string) string {
	return name + "\x00" + key
}

func normalizeSQLPlannerStatisticsFields(fields []string) []string {
	if len(fields) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(fields))
	result := make([]string, 0, len(fields))
	for _, field := range fields {
		field = strings.TrimSpace(field)
		if field == "" {
			continue
		}
		if _, ok := seen[field]; ok {
			continue
		}
		seen[field] = struct{}{}
		result = append(result, field)
	}
	sort.Strings(result)
	return result
}

func sqlPlannerStatisticsRowFields(rows []SQLRow) []string {
	seen := make(map[string]struct{})
	for _, row := range rows {
		for field := range row {
			seen[field] = struct{}{}
		}
	}
	fields := make([]string, 0, len(seen))
	for field := range seen {
		fields = append(fields, field)
	}
	sort.Strings(fields)
	return fields
}

type sqlPlannerFieldAccumulator struct {
	rows       int
	nullRows   int
	values     map[string]int
	minimum    float64
	maximum    float64
	numeric    bool
	nonNumeric bool
	valueBytes int
}

func buildSQLPlannerStatistics(name, key string, sourceBytes int, rows []SQLRow, fields []string) SQLWhatIfSourceStatistics {
	statistics := SQLWhatIfSourceStatistics{
		Source: name + "(" + key + ")",
		Rows:   len(rows),
		Bytes:  sourceBytes,
		Fields: make(map[string]SQLWhatIfFieldStatistics, len(fields)),
	}
	for _, field := range fields {
		accumulator := sqlPlannerFieldAccumulator{values: make(map[string]int)}
		for _, row := range rows {
			value, ok := row[field]
			if !ok || value == nil {
				accumulator.nullRows++
				continue
			}
			accumulator.rows++
			if valueKey, ok := sqlIndexValueKey(value); ok {
				accumulator.values[valueKey]++
			}
			if number, ok := sqlNumber(value); ok {
				if !accumulator.numeric || number < accumulator.minimum {
					accumulator.minimum = number
				}
				if !accumulator.numeric || number > accumulator.maximum {
					accumulator.maximum = number
				}
				accumulator.numeric = true
			} else {
				accumulator.nonNumeric = true
			}
			if encoded, err := json.Marshal(value); err == nil {
				accumulator.valueBytes += len(encoded)
			}
		}
		fieldStatistics := SQLWhatIfFieldStatistics{
			Rows:           accumulator.rows,
			NullRows:       accumulator.nullRows,
			DistinctValues: len(accumulator.values),
		}
		if accumulator.numeric && !accumulator.nonNumeric {
			fieldStatistics.Minimum = accumulator.minimum
			fieldStatistics.Maximum = accumulator.maximum
		}
		if accumulator.rows > 0 {
			fieldStatistics.AverageValueBytes = accumulator.valueBytes / accumulator.rows
		}
		fieldStatistics.FrequencyHistogram = sqlPlannerFrequencyHistogram(accumulator.values)
		statistics.Fields[field] = fieldStatistics
	}
	return statistics
}

func sqlPlannerFrequencyHistogram(values map[string]int) []SQLWhatIfFrequencyBucket {
	if len(values) == 0 {
		return nil
	}
	frequencies := make(map[int]int)
	for _, rows := range values {
		frequencies[rows]++
	}
	counts := make([]int, 0, len(frequencies))
	for rows := range frequencies {
		counts = append(counts, rows)
	}
	sort.Ints(counts)
	histogram := make([]SQLWhatIfFrequencyBucket, 0, len(counts))
	for _, rows := range counts {
		histogram = append(histogram, SQLWhatIfFrequencyBucket{RowsPerValue: rows, DistinctValues: frequencies[rows]})
	}
	return histogram
}

func cloneSQLWhatIfSourceStatistics(statistics SQLWhatIfSourceStatistics) SQLWhatIfSourceStatistics {
	clone := statistics
	clone.Fields = make(map[string]SQLWhatIfFieldStatistics, len(statistics.Fields))
	for field, value := range statistics.Fields {
		value.FrequencyHistogram = append([]SQLWhatIfFrequencyBucket(nil), value.FrequencyHistogram...)
		clone.Fields[field] = value
	}
	return clone
}

func (ht *HatTrie) clearSQLPlannerStatistics() {
	if ht == nil {
		return
	}
	ht.sqlPlannerStatisticsMu.Lock()
	ht.sqlPlannerStatistics = nil
	ht.sqlPlannerStatisticsMu.Unlock()
}
