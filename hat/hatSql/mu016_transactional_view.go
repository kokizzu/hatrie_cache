package hatSql

import (
	"fmt"
	"sort"
	"strings"
)

// SQLSessionViewChange describes one view definition to create or replace in
// an atomic session catalog update.
type SQLSessionViewChange struct {
	Name  string
	Query string
}

// SQLSessionViewDefinition is the immutable public form of a session-local
// view definition returned by ViewCatalogSnapshot.
type SQLSessionViewDefinition struct {
	Name         string
	Query        string
	Dependencies []string
}

// SQLSessionViewCatalog is a consistent, value-copy catalog snapshot. Version
// advances once for each successfully published view batch.
type SQLSessionViewCatalog struct {
	Version uint64
	Views   []SQLSessionViewDefinition
}

// ApplyViewChanges validates and publishes all view changes as one catalog
// boundary. Parsing, duplicate detection, and cycle validation happen before
// the session's live view map is replaced, so a rejected batch has no effect.
func (session *SQLSession) ApplyViewChanges(changes []SQLSessionViewChange) (SQLSessionViewCatalog, error) {
	if session == nil {
		return SQLSessionViewCatalog{}, fmt.Errorf("SQL session is nil")
	}
	normalized, err := normalizeSQLSessionViewChanges(changes)
	if err != nil {
		return SQLSessionViewCatalog{}, err
	}

	session.mu.Lock()
	defer session.mu.Unlock()
	nextViews := make(map[string]sqlSessionView, len(session.views)+len(normalized))
	for name, view := range session.views {
		nextViews[name] = sqlSessionView{
			source:       view.source,
			dependencies: append([]string(nil), view.dependencies...),
		}
	}
	for _, change := range normalized {
		nextViews[change.name] = cloneSQLSessionView(change.view)
	}
	if sqlSessionViewGraphHasCycle(nextViews) {
		return SQLSessionViewCatalog{}, fmt.Errorf("SQL view batch introduces a dependency cycle")
	}
	session.views = nextViews
	session.catalogVersion++
	return session.viewCatalogSnapshotLocked(), nil
}

// ViewCatalogSnapshot returns a sorted, independently owned view catalog and
// the version at which all returned definitions were published.
func (session *SQLSession) ViewCatalogSnapshot() SQLSessionViewCatalog {
	if session == nil {
		return SQLSessionViewCatalog{}
	}
	session.mu.RLock()
	snapshot := session.viewCatalogSnapshotLocked()
	session.mu.RUnlock()
	return snapshot
}

// CatalogVersion returns the current session catalog version. It is useful for
// callers that need a cheap publication marker without copying definitions.
func (session *SQLSession) CatalogVersion() uint64 {
	if session == nil {
		return 0
	}
	session.mu.RLock()
	version := session.catalogVersion
	session.mu.RUnlock()
	return version
}

func (session *SQLSession) viewCatalogSnapshotLocked() SQLSessionViewCatalog {
	names := make([]string, 0, len(session.views))
	for name := range session.views {
		names = append(names, name)
	}
	sort.Strings(names)
	views := make([]SQLSessionViewDefinition, 0, len(names))
	for _, name := range names {
		view := session.views[name]
		dependencies := append([]string(nil), view.dependencies...)
		sort.Strings(dependencies)
		views = append(views, SQLSessionViewDefinition{
			Name:         name,
			Query:        view.source,
			Dependencies: dependencies,
		})
	}
	return SQLSessionViewCatalog{Version: session.catalogVersion, Views: views}
}

type sqlSessionViewChange struct {
	name string
	view sqlSessionView
}

func normalizeSQLSessionViewChanges(changes []SQLSessionViewChange) ([]sqlSessionViewChange, error) {
	if len(changes) == 0 {
		return nil, fmt.Errorf("at least one SQL view change is required")
	}
	normalized := make([]sqlSessionViewChange, 0, len(changes))
	seen := make(map[string]struct{}, len(changes))
	for _, change := range changes {
		normalizedChange, err := normalizeSQLSessionViewChange(change)
		if err != nil {
			return nil, err
		}
		name := normalizedChange.name
		if _, exists := seen[name]; exists {
			return nil, fmt.Errorf("SQL view %q appears more than once in one batch", change.Name)
		}
		seen[name] = struct{}{}
		normalized = append(normalized, normalizedChange)
	}
	return normalized, nil
}

func normalizeSQLSessionViewChange(change SQLSessionViewChange) (sqlSessionViewChange, error) {
	name, err := sessionObjectName(change.Name)
	if err != nil {
		return sqlSessionViewChange{}, err
	}
	query := strings.TrimSpace(change.Query)
	if query == "" {
		return sqlSessionViewChange{}, fmt.Errorf("SQL view %q requires a query", change.Name)
	}
	parsed, err := parseSQLQuery(query)
	if err != nil {
		return sqlSessionViewChange{}, err
	}
	dependencies := sqlQueryCacheDependencies(parsed)
	sort.Strings(dependencies)
	return sqlSessionViewChange{
		name: name,
		view: sqlSessionView{source: query, dependencies: dependencies},
	}, nil
}

func cloneSQLSessionView(view sqlSessionView) sqlSessionView {
	view.dependencies = append([]string(nil), view.dependencies...)
	return view
}

func sqlSessionViewGraphHasCycle(views map[string]sqlSessionView) bool {
	state := make(map[string]uint8, len(views))
	var visit func(string) bool
	visit = func(name string) bool {
		switch state[name] {
		case 1:
			return true
		case 2:
			return false
		}
		view, exists := views[name]
		if !exists {
			state[name] = 2
			return false
		}
		state[name] = 1
		for _, dependency := range view.dependencies {
			if visit(dependency) {
				return true
			}
		}
		state[name] = 2
		return false
	}
	for name := range views {
		if visit(name) {
			return true
		}
	}
	return false
}

func sqlSessionViewGraphHasCycleWithReplacement(views map[string]sqlSessionView, replacementName string, replacement sqlSessionView) bool {
	state := make(map[string]uint8, len(views))
	var visit func(string) bool
	visit = func(name string) bool {
		switch state[name] {
		case 1:
			return true
		case 2:
			return false
		}
		view, exists := views[name]
		if name == replacementName {
			view = replacement
			exists = true
		}
		if !exists {
			state[name] = 2
			return false
		}
		state[name] = 1
		for _, dependency := range view.dependencies {
			if visit(dependency) {
				return true
			}
		}
		state[name] = 2
		return false
	}
	for name := range views {
		if visit(name) {
			return true
		}
	}
	return visit(replacementName)
}
