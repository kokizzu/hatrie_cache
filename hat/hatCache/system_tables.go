package hatCache

import (
	"fmt"
	"strings"

	"hatrie_cache/hat/hatSql"
)

const (
	DefaultSQLSystemMutationLimit = 1000
	MaxSQLSystemMutationLimit     = 10000

	SQLSystemPartsTable        = "system.parts"
	SQLSystemMutationsTable    = "system.mutations"
	SQLSystemQueriesTable      = "system.queries"
	SQLSystemQueryHistoryTable = "system.query_history"
)

// SQLSystemTablesResolverOptions supplies optional operator data sources for
// SQLSystemTablesResolver. Missing sources produce empty system tables.
type SQLSystemTablesResolverOptions struct {
	// Trie is used to populate system.parts. When omitted, a *HatTrie source is
	// detected automatically.
	Trie *HatTrie
	// QueryManager supplies privacy-safe active and completed query status.
	QueryManager *hatSql.SQLQueryManager
	// Journal supplies durable mutation records without exposing values.
	Journal *CommandJournal
	// MutationLimit bounds the number of journal rows returned per read.
	MutationLimit int
}

// SQLSystemTablesResolver adds read-only operational tables to an existing SQL
// source resolver. It recognizes CACHE('system.*') while delegating every
// other source unchanged, so existing query behavior is preserved.
type SQLSystemTablesResolver struct {
	source        SQLSourceResolver
	trie          *HatTrie
	queryManager  *hatSql.SQLQueryManager
	journal       *CommandJournal
	mutationLimit int
}

// NewSQLSystemTablesResolver wraps source with read-only system tables.
func NewSQLSystemTablesResolver(source SQLSourceResolver, options SQLSystemTablesResolverOptions) *SQLSystemTablesResolver {
	trie := options.Trie
	if trie == nil {
		trie, _ = source.(*HatTrie)
	}
	limit := options.MutationLimit
	if limit <= 0 {
		limit = DefaultSQLSystemMutationLimit
	}
	if limit > MaxSQLSystemMutationLimit {
		limit = MaxSQLSystemMutationLimit
	}
	return &SQLSystemTablesResolver{
		source:        source,
		trie:          trie,
		queryManager:  options.QueryManager,
		journal:       options.Journal,
		mutationLimit: limit,
	}
}

// ResolveSQLSource implements SQLSourceResolver for CACHE('system.*') and
// delegates ordinary sources to the wrapped resolver.
func (resolver *SQLSystemTablesResolver) ResolveSQLSource(name, key string) ([]SQLRow, error) {
	if resolver == nil {
		return nil, nil
	}
	if strings.EqualFold(strings.TrimSpace(name), "CACHE") {
		switch strings.ToLower(strings.TrimSpace(key)) {
		case SQLSystemPartsTable:
			return resolver.parts(), nil
		case SQLSystemMutationsTable:
			return resolver.mutations()
		case SQLSystemQueriesTable:
			return resolver.queries(), nil
		case SQLSystemQueryHistoryTable:
			return resolver.queryHistory(), nil
		}
	}
	if resolver.source == nil {
		return nil, nil
	}
	return resolver.source.ResolveSQLSource(name, key)
}

func (resolver *SQLSystemTablesResolver) parts() []SQLRow {
	if resolver.trie == nil {
		return nil
	}
	partitioning := resolver.trie.LocalPartitioningStats()
	if !partitioning.Enabled || partitioning.Partitions == 0 {
		return []SQLRow{{
			"name":      "root",
			"partition": int64(0),
			"rows":      int64(resolver.trie.Size()),
			"active":    true,
		}}
	}
	rows := make([]SQLRow, len(partitioning.Sizes))
	for index, size := range partitioning.Sizes {
		rows[index] = SQLRow{
			"name":      fmt.Sprintf("local-%03d", index),
			"partition": int64(index),
			"rows":      int64(size),
			"active":    true,
		}
	}
	return rows
}

func (resolver *SQLSystemTablesResolver) mutations() ([]SQLRow, error) {
	if resolver.journal == nil {
		return nil, nil
	}
	tail, err := resolver.journal.Tail(0, resolver.mutationLimit)
	if err != nil {
		return nil, err
	}
	rows := make([]SQLRow, 0, len(tail.Entries))
	for _, entry := range tail.Entries {
		rows = append(rows, SQLRow{
			"sequence": entry.Sequence,
			"command":  entry.Request.Command,
			"key":      entry.Request.Key,
			"state":    "committed",
		})
	}
	return rows, nil
}

func (resolver *SQLSystemTablesResolver) queries() []SQLRow {
	if resolver.queryManager == nil {
		return nil
	}
	statuses := resolver.queryManager.Active()
	rows := make([]SQLRow, 0, len(statuses))
	for _, status := range statuses {
		rows = append(rows, sqlSystemQueryRow(status, true))
	}
	return rows
}

func (resolver *SQLSystemTablesResolver) queryHistory() []SQLRow {
	if resolver.queryManager == nil {
		return nil
	}
	statuses := resolver.queryManager.History()
	rows := make([]SQLRow, 0, len(statuses))
	for _, status := range statuses {
		rows = append(rows, sqlSystemQueryRow(status, false))
	}
	return rows
}

func sqlSystemQueryRow(status hatSql.SQLQueryStatus, active bool) SQLRow {
	row := SQLRow{
		"query_id":   status.QueryID,
		"state":      string(status.State),
		"started_at": status.StartedAt,
		"active":     active,
	}
	if !status.FinishedAt.IsZero() {
		row["finished_at"] = status.FinishedAt
	}
	if status.CancelReason != "" {
		row["cancel_reason"] = status.CancelReason
	}
	if status.ErrorCode != "" {
		row["error_code"] = string(status.ErrorCode)
	}
	return row
}
