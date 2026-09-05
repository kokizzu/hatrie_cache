package hatriecache

import core "hatrie_cache/hat/hatCache"

type SQLQueryCanceledError = core.SQLQueryCanceledError
type SQLQueryManager = core.SQLQueryManager
type SQLQueryManagerOptions = core.SQLQueryManagerOptions
type SQLQueryState = core.SQLQueryState
type SQLQueryStatus = core.SQLQueryStatus

const (
	DefaultSQLQueryManagerHistoryCapacity = core.DefaultSQLQueryManagerHistoryCapacity
	SQLQueryStateRunning                  = core.SQLQueryStateRunning
	SQLQueryStateCancelRequested          = core.SQLQueryStateCancelRequested
	SQLQueryStateSucceeded                = core.SQLQueryStateSucceeded
	SQLQueryStateFailed                   = core.SQLQueryStateFailed
	SQLQueryStateCanceled                 = core.SQLQueryStateCanceled
)

var NewSQLQueryManager = core.NewSQLQueryManager
var NewSQLQueryManagerWithOptions = core.NewSQLQueryManagerWithOptions
