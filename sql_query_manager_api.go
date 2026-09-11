package hatriecache

import core "hatrie_cache/hat/hatCache"

type SQLQueryCanceledError = core.SQLQueryCanceledError
type SQLQueryManager = core.SQLQueryManager
type SQLQueryManagerOptions = core.SQLQueryManagerOptions
type SQLQueryLog = core.SQLQueryLog
type SQLQueryLogEntry = core.SQLQueryLogEntry
type SQLQueryLogOptions = core.SQLQueryLogOptions
type SQLQueryState = core.SQLQueryState
type SQLQueryStatus = core.SQLQueryStatus

const (
	DefaultSQLQueryManagerHistoryCapacity = core.DefaultSQLQueryManagerHistoryCapacity
	DefaultSQLQueryLogMaxRecordBytes      = core.DefaultSQLQueryLogMaxRecordBytes
	SQLQueryStateRunning                  = core.SQLQueryStateRunning
	SQLQueryStateCancelRequested          = core.SQLQueryStateCancelRequested
	SQLQueryStateSucceeded                = core.SQLQueryStateSucceeded
	SQLQueryStateFailed                   = core.SQLQueryStateFailed
	SQLQueryStateCanceled                 = core.SQLQueryStateCanceled
)

var (
	ErrSQLQueryLogClosed        = core.ErrSQLQueryLogClosed
	ErrSQLQueryLogRecordInvalid = core.ErrSQLQueryLogRecordInvalid
)

var NewSQLQueryManager = core.NewSQLQueryManager
var NewSQLQueryManagerWithOptions = core.NewSQLQueryManagerWithOptions
var OpenSQLQueryLog = core.OpenSQLQueryLog
var OpenSQLQueryLogWithOptions = core.OpenSQLQueryLogWithOptions
