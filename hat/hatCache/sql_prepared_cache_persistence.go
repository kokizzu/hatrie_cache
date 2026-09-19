package hatCache

import hatSql "hatrie_cache/hat/hatSql"

type SQLPreparedQueryCachePersistenceOptions = hatSql.SQLPreparedQueryCachePersistenceOptions
type SQLPreparedQueryCacheLoadReport = hatSql.SQLPreparedQueryCacheLoadReport

const (
	SQLPreparedQueryCachePersistenceFormat                = hatSql.SQLPreparedQueryCachePersistenceFormat
	DefaultSQLPreparedQueryCachePersistenceMaxEntries     = hatSql.DefaultSQLPreparedQueryCachePersistenceMaxEntries
	MaxSQLPreparedQueryCachePersistenceMaxEntries         = hatSql.MaxSQLPreparedQueryCachePersistenceMaxEntries
	DefaultSQLPreparedQueryCachePersistenceMaxBytes       = hatSql.DefaultSQLPreparedQueryCachePersistenceMaxBytes
	MaxSQLPreparedQueryCachePersistenceMaxBytes           = hatSql.MaxSQLPreparedQueryCachePersistenceMaxBytes
	MaxSQLPreparedQueryCachePersistenceSourceBytes        = hatSql.MaxSQLPreparedQueryCachePersistenceSourceBytes
	MaxSQLPreparedQueryCachePersistenceSchemaVersionBytes = hatSql.MaxSQLPreparedQueryCachePersistenceSchemaVersionBytes
)

var (
	ErrSQLPreparedQueryCachePersistenceInvalid       = hatSql.ErrSQLPreparedQueryCachePersistenceInvalid
	ErrSQLPreparedQueryCachePersistenceLimitExceeded = hatSql.ErrSQLPreparedQueryCachePersistenceLimitExceeded
)
