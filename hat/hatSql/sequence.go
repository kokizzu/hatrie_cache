package hatSql

import (
	"fmt"
	"sort"
	"time"
)

// OrderedEvent is a keyed event used by ordered sequence-pattern matching.
type OrderedEvent struct {
	Key  string
	Kind string
	At   time.Time
	Row  Row
}

// SequenceMatch is one non-overlapping, contiguous sequence-pattern match.
type SequenceMatch struct {
	Key    string
	Events []OrderedEvent
}

// MatchOrderedEventSequence finds non-overlapping contiguous event-kind
// patterns within each key. Events are ordered by key and time without changing
// caller input. A zero maximumGap disables the time-gap limit.
func MatchOrderedEventSequence(events []OrderedEvent, pattern []string, maximumGap time.Duration) ([]SequenceMatch, error) {
	if len(pattern) == 0 {
		return nil, fmt.Errorf("event sequence pattern cannot be empty")
	}
	if maximumGap < 0 {
		return nil, fmt.Errorf("event sequence maximum gap cannot be negative")
	}
	for _, kind := range pattern {
		if kind == "" {
			return nil, fmt.Errorf("event sequence pattern kinds cannot be empty")
		}
	}
	ordered := make([]OrderedEvent, len(events))
	copy(ordered, events)
	for _, event := range ordered {
		if event.Key == "" || event.Kind == "" {
			return nil, fmt.Errorf("ordered event key and kind cannot be empty")
		}
	}
	sort.SliceStable(ordered, func(left, right int) bool {
		if ordered[left].Key != ordered[right].Key {
			return ordered[left].Key < ordered[right].Key
		}
		return ordered[left].At.Before(ordered[right].At)
	})

	result := make([]SequenceMatch, 0)
	for offset := 0; offset+len(pattern) <= len(ordered); {
		if ordered[offset].Kind != pattern[0] || !sequenceMatchesAt(ordered, offset, pattern, maximumGap) {
			offset++
			continue
		}
		result = append(result, SequenceMatch{Key: ordered[offset].Key, Events: cloneOrderedEvents(ordered[offset : offset+len(pattern)])})
		offset += len(pattern)
	}
	return result, nil
}

func sequenceMatchesAt(events []OrderedEvent, offset int, pattern []string, maximumGap time.Duration) bool {
	key := events[offset].Key
	for step, kind := range pattern {
		event := events[offset+step]
		if event.Key != key || event.Kind != kind {
			return false
		}
		if step > 0 && maximumGap > 0 && event.At.Sub(events[offset+step-1].At) > maximumGap {
			return false
		}
	}
	return true
}

func cloneOrderedEvents(events []OrderedEvent) []OrderedEvent {
	cloned := make([]OrderedEvent, len(events))
	copy(cloned, events)
	for index, event := range cloned {
		if event.Row != nil {
			cloned[index].Row = CloneRows([]Row{event.Row})[0]
		}
	}
	return cloned
}
