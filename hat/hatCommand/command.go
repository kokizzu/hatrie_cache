// Package hatCommand defines the stable cache command request and response
// contracts shared by HTTP, gRPC, replication, and embedded callers.
package hatCommand

// Request describes one cache command. BinaryValue is deliberately excluded
// from JSON because binary command transports encode it separately.
type Request struct {
	Command        string         `json:"command"`
	Atomic         bool           `json:"atomic,omitempty"` // BATCH only: apply all subcommands or roll back.
	Key            string         `json:"key"`
	Value          string         `json:"value,omitempty"`
	Values         []any          `json:"values,omitempty"`
	Batch          []Request      `json:"batch,omitempty"`
	Subkey         string         `json:"subkey,omitempty"`
	Pairs          map[string]any `json:"pairs,omitempty"`
	Priority       *int64         `json:"priority,omitempty"`
	TTLSeconds     *int64         `json:"ttl_seconds,omitempty"`
	UnixSeconds    *int64         `json:"unix_seconds,omitempty"`
	IdempotencyKey string         `json:"idempotency_key,omitempty"`
	BinaryValue    []byte         `json:"-"`
}

// Response is the result of one cache command, optionally including the
// results of a batch command in request order. Code is populated only for
// errors and is omitted from JSON when empty.
type Response struct {
	OK        bool       `json:"ok"`
	Message   string     `json:"message"`
	Value     string     `json:"value,omitempty"`
	Code      string     `json:"code,omitempty"`
	Responses []Response `json:"responses,omitempty"`
}

const (
	ErrorCodeInvalidArgument    = "invalid_argument"
	ErrorCodeUnsupportedCommand = "unsupported_command"
	ErrorCodeCounterOverflow    = "counter_overflow"
	ErrorCodeInternal           = "internal_error"
)
