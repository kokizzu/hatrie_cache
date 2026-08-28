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
	views   map[string]sqlSessionView
}

type sqlSessionView struct {
	source       string
	dependencies []string
}

func NewSQLSession(source SourceResolver) *SQLSession {
	return &SQLSession{source: source, tables: map[string][]Row{}, results: map[string][]Row{}, views: map[string]sqlSessionView{}}
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

// CreateView stores a session-local query definition after rejecting direct
// and indirect dependencies on itself.
func (session *SQLSession) CreateView(name, source string) error {
	key, err := sessionObjectName(name)
	if err != nil {
		return err
	}
	query, err := parseSQLQuery(source)
	if err != nil {
		return err
	}
	view := sqlSessionView{source: source, dependencies: sqlQueryCacheDependencies(query)}
	session.mu.Lock()
	defer session.mu.Unlock()
	previous, existed := session.views[key]
	session.views[key] = view
	if session.viewCycleLocked(key) {
		if existed {
			session.views[key] = previous
		} else {
			delete(session.views, key)
		}
		return fmt.Errorf("SQL view %q introduces a dependency cycle", name)
	}
	return nil
}

func (session *SQLSession) viewCycleLocked(root string) bool {
	visiting, visited := map[string]bool{}, map[string]bool{}
	var visit func(string) bool
	visit = func(name string) bool {
		if visiting[name] {
			return true
		}
		if visited[name] {
			return false
		}
		view, exists := session.views[name]
		if !exists {
			return false
		}
		visiting[name] = true
		for _, dependency := range view.dependencies {
			if visit(dependency) {
				return true
			}
		}
		delete(visiting, name)
		visited[name] = true
		return false
	}
	return visit(root)
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
		view, exists := session.views[strings.ToLower(key)]
		session.mu.RUnlock()
		if exists {
			result, err := session.Execute(context.Background(), view.source, nil, SQLQueryOptions{})
			if err != nil {
				return nil, err
			}
			return cloneSQLRows(result.Rows), nil
		}
	}
	if session.source == nil {
		return nil, nil
	}
	return session.source.ResolveSQLSource(name, key)
}

func sqlQueryCacheDependencies(query *sqlQuery) []string {
	seen := map[string]struct{}{}
	var collect func(*sqlQuery)
	add := func(source sqlSource) {
		if strings.EqualFold(source.kind, "CACHE") {
			seen[strings.ToLower(source.key)] = struct{}{}
		}
		if source.query != nil {
			collect(source.query)
		}
	}
	collect = func(query *sqlQuery) {
		if query == nil {
			return
		}
		if query.from != nil {
			add(*query.from)
		}
		for _, join := range query.joins {
			add(join.source)
		}
		for _, cte := range query.ctes {
			collect(cte.query)
		}
		for _, union := range query.unions {
			collect(union.query)
		}
	}
	collect(query)
	dependencies := make([]string, 0, len(seen))
	for dependency := range seen {
		dependencies = append(dependencies, dependency)
	}
	return dependencies
}

func (session *SQLSession) Execute(ctx context.Context, source string, parameters []interface{}, options SQLQueryOptions) (SQLQueryResult, error) {
	if name, query, matched, err := sqlSessionCreateStatement(source, "CREATE VIEW"); matched {
		if err != nil {
			return SQLQueryResult{}, err
		}
		return SQLQueryResult{}, session.CreateView(name, query)
	}
	if name, query, matched, err := sqlSessionCreateStatement(source, "CREATE TEMP TABLE"); matched {
		if err != nil {
			return SQLQueryResult{}, err
		}
		result, err := ExecuteSQLQueryParameters(ctx, query, session, parameters, options)
		if err != nil {
			return SQLQueryResult{}, err
		}
		return SQLQueryResult{}, session.CreateTemporaryTable(name, result.Rows)
	}
	return ExecuteSQLQueryParameters(ctx, source, session, parameters, options)
}

func sqlSessionCreateStatement(source, prefix string) (string, string, bool, error) {
	trimmed := strings.TrimSpace(source)
	upper := strings.ToUpper(trimmed)
	prefixUpper := prefix + " "
	if !strings.HasPrefix(upper, prefixUpper) {
		return "", "", false, nil
	}
	rest := strings.TrimSpace(trimmed[len(prefix):])
	separator := strings.Index(strings.ToUpper(rest), " AS ")
	if separator < 1 {
		return "", "", true, fmt.Errorf("%s requires a name and AS query", prefix)
	}
	name := strings.TrimSpace(rest[:separator])
	query := strings.TrimSpace(rest[separator+4:])
	if _, err := sessionObjectName(name); err != nil || query == "" {
		if err != nil {
			return "", "", true, err
		}
		return "", "", true, fmt.Errorf("%s requires a query after AS", prefix)
	}
	return name, query, true, nil
}

func sessionObjectName(name string) (string, error) {
	name = strings.TrimSpace(name)
	if !catalogIdentifier(name) {
		return "", fmt.Errorf("SQL session object name must be a simple identifier")
	}
	return strings.ToLower(name), nil
}
