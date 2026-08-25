package hatSql

import (
	"context"
	"errors"
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
