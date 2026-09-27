package hatCache

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// ErrAsyncInsertSQLUnsupported identifies SQL that cannot be represented by a
// single bounded async-insert queue item. Callers can fall back to
// ExecuteSQLMutation for richer mutation semantics.
var ErrAsyncInsertSQLUnsupported = errors.New("hatriecache: async SQL insert is unsupported")

// SubmitSQL compiles one literal SQL INSERT and submits its journaled command
// to the bounded async-insert queue. It intentionally excludes result-bearing,
// conditional, query-backed, and multi-statement SQL because a submission
// receipt represents one durable cache command, not a general SQL mutation.
func (buffer *AsyncInsertBuffer) SubmitSQL(ctx context.Context, source string) (*AsyncInsertSubmission, error) {
	request, err := compileAsyncInsertSQL(source)
	if err != nil {
		return nil, err
	}
	return buffer.Submit(ctx, request)
}

func compileAsyncInsertSQL(source string) (CacheCommandRequest, error) {
	tokens, err := lexSQL(source)
	if err != nil {
		return CacheCommandRequest{}, err
	}
	if len(tokens) == 0 || tokens[0].kind != sqlTokenIdentifier || !strings.EqualFold(tokens[0].text, "INSERT") {
		return CacheCommandRequest{}, fmt.Errorf("%w: only INSERT statements are accepted", ErrAsyncInsertSQLUnsupported)
	}
	request, err := compileSQLTokens(source, tokens)
	if err != nil {
		return CacheCommandRequest{}, fmt.Errorf("%w: %v", ErrAsyncInsertSQLUnsupported, err)
	}
	if normalizedCommand(request.Command) == "BATCH" {
		return CacheCommandRequest{}, fmt.Errorf("%w: multiple statements are not supported", ErrAsyncInsertSQLUnsupported)
	}
	if !commandShouldJournal(request) {
		return CacheCommandRequest{}, fmt.Errorf("%w: statement is not a journaled write", ErrAsyncInsertSQLUnsupported)
	}
	return request, nil
}
