package hatSql

import (
	"context"
	"fmt"
	"strings"
	"sync"
)

// SQLSession owns temporary SQL sources and named result snapshots for one
// caller. It is safe for concurrent use and never mutates its base resolver.
type SQLSession struct {
	mu      sync.RWMutex
	source  SourceResolver
	tables  map[string][]Row
	results map[string][]Row
}

func NewSQLSession(source SourceResolver) *SQLSession {
	return &SQLSession{source: source, tables: map[string][]Row{}, results: map[string][]Row{}}
}

func (session *SQLSession) CreateTemporaryTable(name string, rows []Row) error {
	key, err := sessionObjectName(name)
	if err != nil {
		return err
	}
	session.mu.Lock()
	session.tables[key] = cloneSQLRows(rows)
	session.mu.Unlock()
	return nil
}

func (session *SQLSession) DropTemporaryTable(name string) {
	session.mu.Lock()
	delete(session.tables, strings.ToLower(name))
	session.mu.Unlock()
}

func (session *SQLSession) StoreNamedResult(name string, result SQLQueryResult) error {
	key, err := sessionObjectName(name)
	if err != nil {
		return err
	}
	session.mu.Lock()
	session.results[key] = cloneSQLRows(result.Rows)
	session.mu.Unlock()
	return nil
}

func (session *SQLSession) ResolveSQLSource(name, key string) ([]Row, error) {
	if strings.EqualFold(name, "CACHE") {
		session.mu.RLock()
		if rows, exists := session.tables[strings.ToLower(key)]; exists {
			session.mu.RUnlock()
			return cloneSQLRows(rows), nil
		}
		if rows, exists := session.results[strings.ToLower(key)]; exists {
			session.mu.RUnlock()
			return cloneSQLRows(rows), nil
		}
		session.mu.RUnlock()
	}
	if session.source == nil {
		return nil, nil
	}
	return session.source.ResolveSQLSource(name, key)
}

func (session *SQLSession) Execute(ctx context.Context, source string, parameters []interface{}, options SQLQueryOptions) (SQLQueryResult, error) {
	return ExecuteSQLQueryParameters(ctx, source, session, parameters, options)
}

func sessionObjectName(name string) (string, error) {
	name = strings.TrimSpace(name)
	if !catalogIdentifier(name) {
		return "", fmt.Errorf("SQL session object name must be a simple identifier")
	}
	return strings.ToLower(name), nil
}
