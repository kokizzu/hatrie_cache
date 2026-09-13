package hatSql

import (
	"context"
	"errors"
	"fmt"
	"time"
)

var (
	// ErrSQLSourceFrontierUnavailable indicates that a required source does
	// not expose a verifiable frontier.
	ErrSQLSourceFrontierUnavailable = errors.New("hatSql: SQL source frontier is unavailable")
	// ErrSQLSourceFrontierNotReady indicates that a required source has not
	// established a valid frontier yet.
	ErrSQLSourceFrontierNotReady = errors.New("hatSql: SQL source frontier is not ready")
	// ErrSQLSourceFrontierBehind indicates that a required source is behind the
	// query's minimum frontier.
	ErrSQLSourceFrontierBehind = errors.New("hatSql: SQL source frontier is behind requirement")
	// ErrSQLSourceFrontierWaitTimeout indicates that a required frontier did not
	// become ready before the configured wait timeout.
	ErrSQLSourceFrontierWaitTimeout = errors.New("hatSql: SQL source frontier wait timed out")
)

const defaultSQLSourceFrontierWaitInterval = 10 * time.Millisecond

func validateSQLSourceFrontierRequirement(ctx context.Context, query *sqlQuery, resolver SQLSourceResolver, options SQLQueryOptions) error {
	if !options.RequireSourceFrontier || query == nil {
		return nil
	}
	frontiers, ok := resolver.(SQLSourceFrontierResolver)
	if !ok {
		if sqlQueryHasRemoteSource(query) {
			return fmt.Errorf("%w: resolver does not implement SQLSourceFrontierResolver", ErrSQLSourceFrontierUnavailable)
		}
		return nil
	}
	validate := func() error {
		visited := make(map[sqlRequiredSourceFrontierKey]struct{})
		return validateSQLQueryFrontierSources(query, frontiers, options.RequiredSourceFrontier, visited)
	}
	if options.SourceFrontierWaitTimeout <= 0 {
		return validate()
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	deadline := time.Now().Add(options.SourceFrontierWaitTimeout)
	for {
		err := validate()
		if err == nil {
			return nil
		}
		if !sqlSourceFrontierWaitable(err) {
			return err
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return fmt.Errorf("%w: %v", ErrSQLSourceFrontierWaitTimeout, err)
		}
		interval := options.SourceFrontierWaitInterval
		if interval <= 0 {
			interval = defaultSQLSourceFrontierWaitInterval
		}
		if interval > remaining {
			interval = remaining
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func sqlSourceFrontierWaitable(err error) bool {
	return errors.Is(err, ErrSQLSourceFrontierNotReady) || errors.Is(err, ErrSQLSourceFrontierBehind)
}

type sqlRequiredSourceFrontierKey struct {
	kind string
	key  string
}

func validateSQLQueryFrontierSources(query *sqlQuery, resolver SQLSourceFrontierResolver, required uint64, visited map[sqlRequiredSourceFrontierKey]struct{}) error {
	if query == nil {
		return nil
	}
	if err := validateSQLSourceFrontierSource(query.from, resolver, required, visited); err != nil {
		return err
	}
	for index := range query.joins {
		if err := validateSQLSourceFrontierSource(&query.joins[index].source, resolver, required, visited); err != nil {
			return err
		}
	}
	for index := range query.ctes {
		if err := validateSQLQueryFrontierSources(query.ctes[index].query, resolver, required, visited); err != nil {
			return err
		}
	}
	for index := range query.unions {
		if err := validateSQLQueryFrontierSources(query.unions[index].query, resolver, required, visited); err != nil {
			return err
		}
	}
	return nil
}

func validateSQLSourceFrontierSource(source *sqlSource, resolver SQLSourceFrontierResolver, required uint64, visited map[sqlRequiredSourceFrontierKey]struct{}) error {
	if source == nil {
		return nil
	}
	if source.query != nil {
		if err := validateSQLQueryFrontierSources(source.query, resolver, required, visited); err != nil {
			return err
		}
		return nil
	}
	switch source.kind {
	case "", "VALUES", "CTE":
		return nil
	}
	key := sqlRequiredSourceFrontierKey{kind: source.kind, key: source.key}
	if _, exists := visited[key]; exists {
		return nil
	}
	visited[key] = struct{}{}
	frontier, ready, available, err := resolver.SQLSourceFrontier(source.kind, source.key)
	if err != nil {
		return fmt.Errorf("source %s(%s) frontier: %w", source.kind, source.key, err)
	}
	if !available {
		return fmt.Errorf("source %s(%s): %w", source.kind, source.key, ErrSQLSourceFrontierUnavailable)
	}
	if !ready {
		return fmt.Errorf("source %s(%s): %w", source.kind, source.key, ErrSQLSourceFrontierNotReady)
	}
	if frontier < required {
		return fmt.Errorf("source %s(%s) frontier %d is behind required %d: %w", source.kind, source.key, frontier, required, ErrSQLSourceFrontierBehind)
	}
	return nil
}

func sqlQueryHasRemoteSource(query *sqlQuery) bool {
	if query == nil {
		return false
	}
	if sqlSourceHasRemoteSource(query.from) {
		return true
	}
	for index := range query.joins {
		if sqlSourceHasRemoteSource(&query.joins[index].source) {
			return true
		}
	}
	for index := range query.ctes {
		if sqlQueryHasRemoteSource(query.ctes[index].query) {
			return true
		}
	}
	for index := range query.unions {
		if sqlQueryHasRemoteSource(query.unions[index].query) {
			return true
		}
	}
	return false
}

func sqlSourceHasRemoteSource(source *sqlSource) bool {
	if source == nil {
		return false
	}
	if source.query != nil {
		return sqlQueryHasRemoteSource(source.query)
	}
	return source.kind != "" && source.kind != "VALUES" && source.kind != "CTE"
}
