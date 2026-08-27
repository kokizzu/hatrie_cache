package hatSql

// TableFunctionResolver supplies rows for user-defined TABLE(name(args...))
// sources. It embeds SQLSourceResolver so one resolver can serve ordinary SQL
// sources and table functions in the same query.
type TableFunctionResolver interface {
	SQLSourceResolver
	ResolveSQLTableFunction(name string, arguments []interface{}) ([]SQLRow, error)
}
