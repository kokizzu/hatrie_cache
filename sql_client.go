package hatriecache

import (
	"context"

	"hatrie_cache/hat/hatSql"
)

// SQLConn is a compatibility alias for the portable SQL HTTP client.
type SQLConn = hatSql.Conn

// NewSQLConn creates a connection to a hatrie-cache monitoring endpoint.
func NewSQLConn(baseURL string, token string) *SQLConn {
	return hatSql.NewConn(baseURL, token)
}

// QueryRows invokes visit once for each streamed SQL row.
func QueryRows[T any](ctx context.Context, conn *SQLConn, query string, visit func(T) error) (int, error) {
	return hatSql.QueryRows(ctx, conn, query, visit)
}
