package hatriecache

import "hatrie_cache/hat/hatSql"

// ErrSQLAsOfUnsupported reports that a historical SQL frontier was requested
// from a resolver without an SQLFrontierSnapshotProvider implementation.
var ErrSQLAsOfUnsupported = hatSql.ErrSQLAsOfUnsupported
