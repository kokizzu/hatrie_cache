package hatSql

import (
	"context"
	"errors"
	"strings"
)

// ErrorCode is a stable class for programmatic SQL error handling.
type ErrorCode string

const (
	ErrorUnknown  ErrorCode = "UNKNOWN"
	ErrorSyntax   ErrorCode = "SYNTAX"
	ErrorType     ErrorCode = "TYPE"
	ErrorCapacity ErrorCode = "CAPACITY"
	ErrorConflict ErrorCode = "CONFLICT"
	ErrorCanceled ErrorCode = "CANCELED"
)

// CodedError adds a stable code while preserving the underlying error chain.
type CodedError struct {
	Code ErrorCode
	Err  error
}

func (err *CodedError) Error() string {
	if err == nil || err.Err == nil {
		return ""
	}
	return err.Err.Error()
}

func (err *CodedError) Unwrap() error {
	if err == nil {
		return nil
	}
	return err.Err
}

// WithErrorCode wraps err with a stable classification. Nil remains nil.
func WithErrorCode(code ErrorCode, err error) error {
	if err == nil {
		return nil
	}
	return &CodedError{Code: code, Err: err}
}

// ErrorCodeOf classifies an error without parsing display text.
func ErrorCodeOf(err error) ErrorCode {
	if err == nil {
		return ErrorUnknown
	}
	var coded *CodedError
	if errors.As(err, &coded) && coded != nil && coded.Code != "" {
		return coded.Code
	}
	var diagnostic *Diagnostic
	if errors.As(err, &diagnostic) && diagnostic != nil && diagnostic.Code != "" {
		return diagnostic.Code
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return ErrorCanceled
	}
	return ErrorUnknown
}

// sqlClassifyError applies a stable code at a public execution boundary. It
// leaves already classified errors and cancellation chains untouched, so
// errors.Is and errors.As keep their normal behavior for callers.
func sqlClassifyError(err error) error {
	if err == nil || ErrorCodeOf(err) != ErrorUnknown {
		return err
	}
	message := strings.ToLower(err.Error())
	switch {
	case strings.Contains(message, "conflict"):
		return WithErrorCode(ErrorConflict, err)
	case strings.Contains(message, "budget"),
		strings.Contains(message, "row limit"),
		strings.Contains(message, "maximum"),
		strings.Contains(message, "page_size"):
		return WithErrorCode(ErrorCapacity, err)
	case strings.Contains(message, "cannot convert"),
		strings.Contains(message, "expects "),
		strings.Contains(message, "cannot compare"),
		strings.Contains(message, "must evaluate to"):
		return WithErrorCode(ErrorType, err)
	default:
		return err
	}
}
