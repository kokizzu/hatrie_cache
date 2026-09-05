package hatSql

import (
	"errors"
	"io"
	"sync"

	json "github.com/goccy/go-json"
)

// QueryTraceRecorder retains privacy-safe SQL query events for operator
// inspection and JSONL export. A positive limit keeps only the newest events;
// a non-positive limit retains all events for compatibility with the existing
// unbounded recorder convention.
type QueryTraceRecorder struct {
	mu     sync.Mutex
	limit  int
	events []QueryEvent
}

// NewQueryTraceRecorder creates an opt-in SQL trace recorder. It can be passed
// directly as SQLQueryOptions.Observer.
func NewQueryTraceRecorder(limit int) *QueryTraceRecorder {
	return &QueryTraceRecorder{limit: limit}
}

// ObserveSQLQuery records an independent copy of one privacy-safe query event.
func (recorder *QueryTraceRecorder) ObserveSQLQuery(event QueryEvent) {
	if recorder == nil {
		return
	}
	event.Operators = append([]QueryOperator(nil), event.Operators...)
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	if recorder.limit > 0 && len(recorder.events) >= recorder.limit {
		drop := len(recorder.events) - recorder.limit + 1
		copy(recorder.events, recorder.events[drop:])
		recorder.events = recorder.events[:len(recorder.events)-drop]
	}
	recorder.events = append(recorder.events, event)
}

// Events returns an independent snapshot in chronological order.
func (recorder *QueryTraceRecorder) Events() []QueryEvent {
	if recorder == nil {
		return nil
	}
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	result := make([]QueryEvent, len(recorder.events))
	for index, event := range recorder.events {
		result[index] = event
		result[index].Operators = append([]QueryOperator(nil), event.Operators...)
	}
	return result
}

// WriteJSONL writes the current trace snapshot as one JSON object per line.
// The snapshot is taken before writing so concurrent observations do not
// change the output being exported.
func (recorder *QueryTraceRecorder) WriteJSONL(writer io.Writer) error {
	if recorder == nil {
		return errors.New("hatSql: query trace recorder is nil")
	}
	if writer == nil {
		return errors.New("hatSql: query trace writer is nil")
	}
	encoder := json.NewEncoder(writer)
	for _, event := range recorder.Events() {
		if err := encoder.Encode(event); err != nil {
			return err
		}
	}
	return nil
}
