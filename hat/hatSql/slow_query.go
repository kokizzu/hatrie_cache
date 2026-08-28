package hatSql

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"sync"
)

// SQLSlowQuerySample is a retained, privacy-safe diagnostic record for one
// slow SQL query. It excludes SQL text, source names, predicates, row values,
// and bound parameter values. StatementFingerprint lets operators correlate a
// repeated query shape without retaining its input.
type SQLSlowQuerySample struct {
	QueryID              string             `json:"query_id"`
	StatementFingerprint string             `json:"statement_fingerprint"`
	Parameters           []string           `json:"parameters,omitempty"`
	ElapsedNanos         int64              `json:"elapsed_ns"`
	OutputRows           int                `json:"output_rows"`
	OutputColumns        int                `json:"output_columns"`
	ResultBytes          int                `json:"result_bytes"`
	OK                   bool               `json:"ok"`
	Slow                 bool               `json:"slow"`
	Canceled             bool               `json:"canceled,omitempty"`
	Plan                 []SQLQueryOperator `json:"plan,omitempty"`
}

// SlowQuerySample is the package-native name for SQLSlowQuerySample.
type SlowQuerySample = SQLSlowQuerySample

// SQLSlowQueryRecorder retains a bounded oldest-first history of privacy-safe
// slow-query samples. It is safe for concurrent query execution.
type SQLSlowQueryRecorder struct {
	mu       sync.RWMutex
	capacity int
	key      [32]byte
	samples  []SQLSlowQuerySample
}

// SlowQueryRecorder is the package-native name for SQLSlowQueryRecorder.
type SlowQueryRecorder = SQLSlowQueryRecorder

// NewSQLSlowQueryRecorder creates a recorder that retains at most capacity
// samples. A nonpositive capacity disables retention.
func NewSQLSlowQueryRecorder(capacity int) *SQLSlowQueryRecorder {
	recorder := &SQLSlowQueryRecorder{capacity: capacity}
	if _, err := rand.Read(recorder.key[:]); err != nil {
		// Recording is optional. Do not retain samples with a predictable key
		// when the operating system cannot provide secure randomness.
		recorder.capacity = 0
	}
	return recorder
}

// NewSlowQueryRecorder creates a bounded privacy-safe slow-query recorder.
func NewSlowQueryRecorder(capacity int) *SlowQueryRecorder {
	return NewSQLSlowQueryRecorder(capacity)
}

// Samples returns a stable oldest-first snapshot. Mutating the returned data
// does not affect the recorder.
func (recorder *SQLSlowQueryRecorder) Samples() []SQLSlowQuerySample {
	if recorder == nil {
		return nil
	}
	recorder.mu.RLock()
	defer recorder.mu.RUnlock()
	samples := make([]SQLSlowQuerySample, len(recorder.samples))
	for index := range recorder.samples {
		samples[index] = cloneSQLSlowQuerySample(recorder.samples[index])
	}
	return samples
}

func (recorder *SQLSlowQueryRecorder) record(event SQLQueryEvent, source string, parameters []interface{}) {
	if recorder == nil || !event.Slow {
		return
	}
	sample := SQLSlowQuerySample{
		QueryID:              event.QueryID,
		StatementFingerprint: recorder.statementFingerprint(source),
		Parameters:           redactedSQLParameters(parameters),
		ElapsedNanos:         event.ElapsedNanos,
		OutputRows:           event.OutputRows,
		OutputColumns:        event.OutputColumns,
		ResultBytes:          event.ResultBytes,
		OK:                   event.OK,
		Slow:                 event.Slow,
		Canceled:             event.Canceled,
		Plan:                 cloneSQLQueryOperators(event.Operators),
	}
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	if recorder.capacity <= 0 {
		return
	}
	if len(recorder.samples) == recorder.capacity {
		copy(recorder.samples, recorder.samples[1:])
		recorder.samples[len(recorder.samples)-1] = sample
		return
	}
	recorder.samples = append(recorder.samples, sample)
}

func (recorder *SQLSlowQueryRecorder) statementFingerprint(source string) string {
	digest := hmac.New(sha256.New, recorder.key[:])
	_, _ = digest.Write([]byte(source))
	return hex.EncodeToString(digest.Sum(nil))
}

func redactedSQLParameters(parameters []interface{}) []string {
	if len(parameters) == 0 {
		return nil
	}
	redacted := make([]string, len(parameters))
	for index := range redacted {
		redacted[index] = "[redacted]"
	}
	return redacted
}

func cloneSQLSlowQuerySample(sample SQLSlowQuerySample) SQLSlowQuerySample {
	sample.Parameters = append([]string(nil), sample.Parameters...)
	sample.Plan = cloneSQLQueryOperators(sample.Plan)
	return sample
}

func cloneSQLQueryOperators(operators []SQLQueryOperator) []SQLQueryOperator {
	if len(operators) == 0 {
		return nil
	}
	cloned := make([]SQLQueryOperator, len(operators))
	for index, operator := range operators {
		cloned[index] = operator
		if operator.InputBytes != nil {
			value := *operator.InputBytes
			cloned[index].InputBytes = &value
		}
		if operator.OutputBytes != nil {
			value := *operator.OutputBytes
			cloned[index].OutputBytes = &value
		}
		if operator.EstimatedRows != nil {
			value := *operator.EstimatedRows
			cloned[index].EstimatedRows = &value
		}
		if operator.EstimateErrorPercent != nil {
			value := *operator.EstimateErrorPercent
			cloned[index].EstimateErrorPercent = &value
		}
	}
	return cloned
}
