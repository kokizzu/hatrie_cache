package hatSql

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"time"
)

// QueryTraceSpan is an SDK-neutral OpenTelemetry span projection. TraceID and
// SpanID use the hexadecimal widths required by OTLP; applications can map
// this value to their selected OpenTelemetry SDK without a dependency here.
type QueryTraceSpan struct {
	TraceID       string            `json:"trace_id"`
	SpanID        string            `json:"span_id"`
	ParentSpanID  string            `json:"parent_span_id,omitempty"`
	Name          string            `json:"name"`
	StartUnixNano int64             `json:"start_unix_nano"`
	EndUnixNano   int64             `json:"end_unix_nano"`
	Status        string            `json:"status"`
	Attributes    map[string]string `json:"attributes,omitempty"`
}

// OpenTelemetrySpans returns query and operator spans from the retained trace
// snapshot. Operator end times are anchored at query completion because the
// existing privacy-safe observer contract exposes phase durations, not phase
// start offsets. The result and all attribute maps are independent copies.
func (recorder *QueryTraceRecorder) OpenTelemetrySpans() []QueryTraceSpan {
	if recorder == nil {
		return nil
	}
	recorder.mu.Lock()
	entries := append([]queryTraceEvent(nil), recorder.events...)
	recorder.mu.Unlock()
	spanCount := 0
	for _, entry := range entries {
		spanCount += 1 + len(entry.event.Operators)
	}
	spans := make([]QueryTraceSpan, 0, spanCount)
	for _, entry := range entries {
		end := entry.observedAt.UnixNano()
		if end <= 0 {
			end = time.Now().UnixNano()
		}
		queryStart := queryTraceSpanStart(end, entry.event.ElapsedNanos)
		traceID := queryTraceTraceID(entry.event.QueryID, entry.sequence)
		querySpanID := queryTraceSpanID(entry.sequence, -1)
		status := queryTraceStatus(entry.event)
		spans = append(spans, QueryTraceSpan{
			TraceID:       traceID,
			SpanID:        querySpanID,
			Name:          "hatrie.sql.query",
			StartUnixNano: queryStart,
			EndUnixNano:   end,
			Status:        status,
			Attributes: map[string]string{
				"hatrie.sql.query_id":       entry.event.QueryID,
				"hatrie.sql.output_rows":    strconv.Itoa(entry.event.OutputRows),
				"hatrie.sql.output_columns": strconv.Itoa(entry.event.OutputColumns),
			},
		})
		for index, operator := range entry.event.Operators {
			operatorEnd := end
			operatorStart := queryTraceSpanStart(operatorEnd, operator.ElapsedNanos)
			spans = append(spans, QueryTraceSpan{
				TraceID:       traceID,
				SpanID:        queryTraceSpanID(entry.sequence, index),
				ParentSpanID:  querySpanID,
				Name:          "hatrie.sql.operator",
				StartUnixNano: operatorStart,
				EndUnixNano:   operatorEnd,
				Status:        status,
				Attributes:    queryTraceOperatorAttributes(operator, entry.event.QueryID),
			})
		}
	}
	return spans
}

func queryTraceTraceID(queryID string, sequence uint64) string {
	digest := sha256.Sum256([]byte("hatrie/sql/trace/" + queryID + "/" + strconv.FormatUint(sequence, 10)))
	return hex.EncodeToString(digest[:16])
}

func queryTraceSpanID(sequence uint64, operator int) string {
	digest := sha256.Sum256([]byte("hatrie/sql/span/" + strconv.FormatUint(sequence, 10) + "/" + strconv.Itoa(operator)))
	return hex.EncodeToString(digest[:8])
}

func queryTraceStatus(event QueryEvent) string {
	if event.OK {
		return "OK"
	}
	return "ERROR"
}

func queryTraceSpanStart(end, elapsed int64) int64 {
	if elapsed <= 0 || end <= 0 || elapsed >= end {
		if elapsed >= end && end > 0 {
			return 1
		}
		return end
	}
	return end - elapsed
}

func queryTraceOperatorAttributes(operator QueryOperator, queryID string) map[string]string {
	attributes := map[string]string{
		"hatrie.sql.query_id":    queryID,
		"hatrie.sql.operator":    operator.Node,
		"hatrie.sql.input_rows":  strconv.Itoa(operator.InputRows),
		"hatrie.sql.output_rows": strconv.Itoa(operator.OutputRows),
	}
	if operator.InputBytes != nil {
		attributes["hatrie.sql.input_bytes"] = strconv.Itoa(*operator.InputBytes)
	}
	if operator.OutputBytes != nil {
		attributes["hatrie.sql.output_bytes"] = strconv.Itoa(*operator.OutputBytes)
	}
	return attributes
}
