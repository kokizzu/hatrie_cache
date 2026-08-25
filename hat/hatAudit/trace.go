package hatAudit

import (
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"
	"time"

	json "github.com/goccy/go-json"

	"hatrie_cache/hat/hatCommand"
)

// Query filters the retained audit window. Empty fields do not filter; zero
// Limit returns every matching retained event.
type Query struct {
	Action    string
	Command   string
	KeyPrefix string
	OK        *bool
	Limit     int
}

// Trace is one replayable command execution. BinaryValue is retained outside
// Request because the normal command JSON contract intentionally omits it.
type Trace struct {
	Sequence    uint64              `json:"sequence"`
	Time        string              `json:"time"`
	Request     hatCommand.Request  `json:"request"`
	BinaryValue []byte              `json:"binary_value,omitempty"`
	Response    hatCommand.Response `json:"response"`
}

// TraceRecorder captures a bounded, ordered replay workload and can
// optionally stream it as JSONL to a caller-owned writer.
type TraceRecorder struct {
	mu     sync.Mutex
	writer io.Writer
	now    func() time.Time
	limit  int
	next   uint64
	traces []Trace
}

// ReplayReport describes one deterministic trace replay.
type ReplayReport struct {
	Applied      int    `json:"applied"`
	Mismatches   int    `json:"mismatches"`
	LastSequence uint64 `json:"last_sequence,omitempty"`
}

// NewTraceRecorder creates a recorder. A non-positive limit keeps all traces
// recorded by this recorder; writer may be nil for in-memory use.
func NewTraceRecorder(writer io.Writer, limit int) *TraceRecorder {
	return &TraceRecorder{writer: writer, now: time.Now, limit: limit}
}

// Record stores one command and its response in execution order.
func (recorder *TraceRecorder) Record(request hatCommand.Request, response hatCommand.Response) error {
	if recorder == nil {
		return nil
	}
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	recorder.next++
	trace := Trace{
		Sequence: recorder.next,
		Time:     recorder.now().UTC().Format(time.RFC3339Nano),
		Request:  cloneRequest(request),
		Response: cloneResponse(response),
	}
	trace.BinaryValue = append([]byte(nil), request.BinaryValue...)
	trace.Request.BinaryValue = nil
	if recorder.writer != nil {
		data, err := json.Marshal(trace)
		if err != nil {
			return err
		}
		if _, err := recorder.writer.Write(append(data, '\n')); err != nil {
			return err
		}
	}
	recorder.traces = append(recorder.traces, trace)
	if recorder.limit > 0 && len(recorder.traces) > recorder.limit {
		copy(recorder.traces, recorder.traces[len(recorder.traces)-recorder.limit:])
		recorder.traces = recorder.traces[:recorder.limit]
	}
	return nil
}

// Traces returns an independent ordered trace snapshot.
func (recorder *TraceRecorder) Traces() []Trace {
	if recorder == nil {
		return nil
	}
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	out := make([]Trace, len(recorder.traces))
	for index := range recorder.traces {
		out[index] = cloneTrace(recorder.traces[index])
	}
	return out
}

// Replay executes traces in recorded order and rejects the first response
// that differs from the recorded response.
func Replay(traces []Trace, execute func(hatCommand.Request) hatCommand.Response) (ReplayReport, error) {
	if execute == nil {
		return ReplayReport{}, errors.New("hatriecache: trace replay executor is required")
	}
	var report ReplayReport
	for _, trace := range traces {
		request := cloneRequest(trace.Request)
		request.BinaryValue = append([]byte(nil), trace.BinaryValue...)
		actual := execute(request)
		report.Applied++
		report.LastSequence = trace.Sequence
		if !reflect.DeepEqual(actual, trace.Response) {
			report.Mismatches++
			return report, fmt.Errorf("hatriecache: trace sequence %d response mismatch", trace.Sequence)
		}
	}
	return report, nil
}

func matchesQuery(event AuditEvent, query Query) bool {
	if query.Action != "" && event.Action != query.Action {
		return false
	}
	if query.Command != "" && event.Command != query.Command {
		return false
	}
	if query.KeyPrefix != "" && !strings.HasPrefix(event.Key, query.KeyPrefix) {
		return false
	}
	return query.OK == nil || event.OK == *query.OK
}

func cloneTrace(trace Trace) Trace {
	trace.Request = cloneRequest(trace.Request)
	trace.BinaryValue = append([]byte(nil), trace.BinaryValue...)
	trace.Response = cloneResponse(trace.Response)
	return trace
}

func cloneRequest(request hatCommand.Request) hatCommand.Request {
	out := request
	if request.Values != nil {
		out.Values = append([]any(nil), request.Values...)
	}
	if request.Batch != nil {
		out.Batch = make([]hatCommand.Request, len(request.Batch))
		for index := range request.Batch {
			out.Batch[index] = cloneRequest(request.Batch[index])
		}
	}
	if request.Pairs != nil {
		out.Pairs = make(map[string]any, len(request.Pairs))
		for key, value := range request.Pairs {
			out.Pairs[key] = value
		}
	}
	out.BinaryValue = append([]byte(nil), request.BinaryValue...)
	return out
}

func cloneResponse(response hatCommand.Response) hatCommand.Response {
	out := response
	if response.Responses != nil {
		out.Responses = make([]hatCommand.Response, len(response.Responses))
		for index := range response.Responses {
			out.Responses[index] = cloneResponse(response.Responses[index])
		}
	}
	return out
}
