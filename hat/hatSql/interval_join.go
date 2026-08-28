package hatSql

import (
	"fmt"
	"sort"
	"time"
)

// IntervalRecord is a keyed half-open interval [Start, End) used in range joins.
type IntervalRecord struct {
	Key      string
	ID       string
	Interval TimeInterval
	Row      Row
}

// IntervalMatch is one pair of keyed records whose intervals overlap.
type IntervalMatch struct {
	Left  IntervalRecord
	Right IntervalRecord
}

type intervalIndexNode struct {
	record      IntervalRecord
	maximumEnd  time.Time
	left, right *intervalIndexNode
}

// JoinOverlappingIntervals joins records with the same key when their half-open
// time ranges overlap. The right side is indexed by start and maximum end time,
// which avoids a full pair scan for sparse range joins.
func JoinOverlappingIntervals(left, right []IntervalRecord) ([]IntervalMatch, error) {
	if err := validateIntervalRecords(left); err != nil {
		return nil, err
	}
	if err := validateIntervalRecords(right); err != nil {
		return nil, err
	}
	indexes := make(map[string]*intervalIndexNode)
	byKey := make(map[string][]IntervalRecord)
	for _, record := range right {
		byKey[record.Key] = append(byKey[record.Key], record)
	}
	for key, records := range byKey {
		sort.Slice(records, func(first, second int) bool { return intervalRecordLess(records[first], records[second]) })
		indexes[key] = buildIntervalIndex(records)
	}
	orderedLeft := append([]IntervalRecord(nil), left...)
	sort.Slice(orderedLeft, func(first, second int) bool { return intervalRecordLess(orderedLeft[first], orderedLeft[second]) })
	result := make([]IntervalMatch, 0)
	for _, leftRecord := range orderedLeft {
		overlaps := make([]IntervalRecord, 0)
		queryIntervalIndex(indexes[leftRecord.Key], leftRecord.Interval, &overlaps)
		sort.Slice(overlaps, func(first, second int) bool { return intervalRecordLess(overlaps[first], overlaps[second]) })
		for _, rightRecord := range overlaps {
			result = append(result, IntervalMatch{Left: cloneIntervalRecord(leftRecord), Right: cloneIntervalRecord(rightRecord)})
		}
	}
	return result, nil
}

func validateIntervalRecords(records []IntervalRecord) error {
	for _, record := range records {
		if record.ID == "" {
			return fmt.Errorf("interval record ID cannot be empty")
		}
		if !record.Interval.Start.Before(record.Interval.End) {
			return fmt.Errorf("interval record %q must have start before end", record.ID)
		}
	}
	return nil
}

func intervalRecordLess(left, right IntervalRecord) bool {
	if left.Key != right.Key {
		return left.Key < right.Key
	}
	if !left.Interval.Start.Equal(right.Interval.Start) {
		return left.Interval.Start.Before(right.Interval.Start)
	}
	if !left.Interval.End.Equal(right.Interval.End) {
		return left.Interval.End.Before(right.Interval.End)
	}
	return left.ID < right.ID
}

func buildIntervalIndex(records []IntervalRecord) *intervalIndexNode {
	if len(records) == 0 {
		return nil
	}
	middle := len(records) / 2
	node := &intervalIndexNode{record: records[middle], maximumEnd: records[middle].Interval.End}
	node.left = buildIntervalIndex(records[:middle])
	node.right = buildIntervalIndex(records[middle+1:])
	if node.left != nil && node.maximumEnd.Before(node.left.maximumEnd) {
		node.maximumEnd = node.left.maximumEnd
	}
	if node.right != nil && node.maximumEnd.Before(node.right.maximumEnd) {
		node.maximumEnd = node.right.maximumEnd
	}
	return node
}

func queryIntervalIndex(node *intervalIndexNode, interval TimeInterval, result *[]IntervalRecord) {
	if node == nil {
		return
	}
	if node.left != nil && node.left.maximumEnd.After(interval.Start) {
		queryIntervalIndex(node.left, interval, result)
	}
	if IntervalsOverlap(node.record.Interval, interval) {
		*result = append(*result, node.record)
	}
	if node.record.Interval.Start.Before(interval.End) {
		queryIntervalIndex(node.right, interval, result)
	}
}

func cloneIntervalRecord(record IntervalRecord) IntervalRecord {
	if record.Row != nil {
		record.Row = CloneRows([]Row{record.Row})[0]
	}
	return record
}
