package hatCache

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"hatrie_cache/hat/hatSql"
)

const (
	DefaultSQLSystemMutationLimit = 1000
	MaxSQLSystemMutationLimit     = 10000
	DefaultSQLSystemPartLimit     = 1024
	MaxSQLSystemPartLimit         = 10000
	MaxSQLSystemPartFieldBytes    = 4096

	SQLSystemPartsTable        = "system.parts"
	SQLSystemMutationsTable    = "system.mutations"
	SQLSystemQueriesTable      = "system.queries"
	SQLSystemQueryHistoryTable = "system.query_history"
)

var (
	ErrSQLSystemPartInvalid        = errors.New("hatriecache: invalid SQL system part")
	ErrSQLSystemPartsLimitExceeded = errors.New("hatriecache: SQL system parts limit exceeded")
)

// SQLSystemPart is the stable, privacy-conscious metadata contract for one
// physical part. It contains layout, checksum, and retention information but
// never requires a filesystem path or row values.
type SQLSystemPart struct {
	Name           string
	Partition      int64
	Rows           int64
	BytesOnDisk    int64
	Active         bool
	State          string
	Level          int64
	DataVersion    int64
	MinKey         string
	MaxKey         string
	Checksum       string
	CreatedAt      time.Time
	RetentionUntil time.Time
}

// SQLSystemPartProvider supplies a bounded point-in-time catalog snapshot.
// The provider owns synchronization and must return metadata only; resolver
// output is copied into fresh SQL rows.
type SQLSystemPartProvider interface {
	SnapshotSQLSystemParts() ([]SQLSystemPart, error)
}

// SQLSystemPartProviderFunc adapts a function to SQLSystemPartProvider.
type SQLSystemPartProviderFunc func() ([]SQLSystemPart, error)

// SnapshotSQLSystemParts implements SQLSystemPartProvider.
func (provider SQLSystemPartProviderFunc) SnapshotSQLSystemParts() ([]SQLSystemPart, error) {
	if provider == nil {
		return nil, nil
	}
	return provider()
}

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
	// PartProvider supplies complete immutable-part metadata. When omitted,
	// system.parts retains the legacy trie partition count view.
	PartProvider SQLSystemPartProvider
	// PartLimit bounds provider rows returned by one system.parts read.
	PartLimit int
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
	partProvider  SQLSystemPartProvider
	partLimit     int
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
	partLimit := options.PartLimit
	if partLimit <= 0 {
		partLimit = DefaultSQLSystemPartLimit
	}
	if partLimit > MaxSQLSystemPartLimit {
		partLimit = MaxSQLSystemPartLimit
	}
	return &SQLSystemTablesResolver{
		source:        source,
		trie:          trie,
		queryManager:  options.QueryManager,
		journal:       options.Journal,
		mutationLimit: limit,
		partProvider:  options.PartProvider,
		partLimit:     partLimit,
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
			if resolver.partProvider == nil {
				return resolver.legacyParts(), nil
			}
			return resolver.providerParts()
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

func (resolver *SQLSystemTablesResolver) legacyParts() []SQLRow {
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

func (resolver *SQLSystemTablesResolver) providerParts() ([]SQLRow, error) {
	parts, err := resolver.partProvider.SnapshotSQLSystemParts()
	if err != nil {
		return nil, err
	}
	if len(parts) > resolver.partLimit {
		return nil, fmt.Errorf("%w: got %d rows, limit %d", ErrSQLSystemPartsLimitExceeded, len(parts), resolver.partLimit)
	}
	parts = append([]SQLSystemPart(nil), parts...)
	for index := range parts {
		if err := validateSQLSystemPart(parts[index]); err != nil {
			return nil, err
		}
	}
	sort.SliceStable(parts, func(left, right int) bool {
		if parts[left].Partition != parts[right].Partition {
			return parts[left].Partition < parts[right].Partition
		}
		if parts[left].Name != parts[right].Name {
			return parts[left].Name < parts[right].Name
		}
		if parts[left].DataVersion != parts[right].DataVersion {
			return parts[left].DataVersion < parts[right].DataVersion
		}
		return parts[left].Level < parts[right].Level
	})
	rows := make([]SQLRow, len(parts))
	for index, part := range parts {
		rows[index] = sqlSystemPartRow(part)
	}
	return rows, nil
}

func validateSQLSystemPart(part SQLSystemPart) error {
	if strings.TrimSpace(part.Name) == "" || len(part.Name) > MaxSQLSystemPartFieldBytes {
		return fmt.Errorf("%w: name is required and must be at most %d bytes", ErrSQLSystemPartInvalid, MaxSQLSystemPartFieldBytes)
	}
	if part.Partition < 0 || part.Rows < 0 || part.BytesOnDisk < 0 || part.Level < 0 || part.DataVersion < 0 {
		return fmt.Errorf("%w: numeric metadata must be non-negative", ErrSQLSystemPartInvalid)
	}
	if len(part.State) > MaxSQLSystemPartFieldBytes || len(part.MinKey) > MaxSQLSystemPartFieldBytes || len(part.MaxKey) > MaxSQLSystemPartFieldBytes || len(part.Checksum) > MaxSQLSystemPartFieldBytes {
		return fmt.Errorf("%w: metadata field exceeds %d bytes", ErrSQLSystemPartInvalid, MaxSQLSystemPartFieldBytes)
	}
	return nil
}

func sqlSystemPartRow(part SQLSystemPart) SQLRow {
	state := strings.TrimSpace(part.State)
	if state == "" {
		if part.Active {
			state = "active"
		} else {
			state = "inactive"
		}
	}
	row := SQLRow{
		"name":            strings.TrimSpace(part.Name),
		"partition":       part.Partition,
		"rows":            part.Rows,
		"bytes_on_disk":   part.BytesOnDisk,
		"active":          part.Active,
		"state":           state,
		"level":           part.Level,
		"data_version":    part.DataVersion,
		"min_key":         part.MinKey,
		"max_key":         part.MaxKey,
		"checksum":        part.Checksum,
		"created_at":      nil,
		"retention_until": nil,
	}
	if !part.CreatedAt.IsZero() {
		row["created_at"] = part.CreatedAt.UTC()
	}
	if !part.RetentionUntil.IsZero() {
		row["retention_until"] = part.RetentionUntil.UTC()
	}
	return row
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
		status := commandJournalMutationStatusFromRecord(entry)
		rows = append(rows, SQLRow{
			"sequence":    status.Sequence,
			"mutation_id": status.Sequence,
			"command":     status.Command,
			"key":         status.Key,
			"state":       string(status.State),
			"progress":    int64(status.Progress),
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
