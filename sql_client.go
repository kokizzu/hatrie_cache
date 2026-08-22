package hatriecache

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// SQLConn is a small HTTP client for the read-only SQL endpoint.
type SQLConn struct {
	BaseURL string
	Token   string
	Client  *http.Client
}

// NewSQLConn creates a connection to a hatrie-cache monitoring endpoint.
func NewSQLConn(baseURL string, token string) *SQLConn {
	return &SQLConn{BaseURL: strings.TrimRight(baseURL, "/"), Token: token, Client: http.DefaultClient}
}

// Query executes one relational SQL query and returns its materialized rows.
func (conn *SQLConn) Query(ctx context.Context, query string) (SQLQueryResult, error) {
	return conn.QueryParameters(ctx, query, nil)
}

// QueryParameters executes query with positional $1, $2, ... values encoded
// separately from the SQL source.
func (conn *SQLConn) QueryParameters(ctx context.Context, query string, parameters []interface{}) (SQLQueryResult, error) {
	return conn.queryRequest(ctx, SQLQueryRequest{Query: query, Parameters: parameters})
}

// QueryPage obtains one bounded result page. Pass NextCursor to retrieve the
// following page; a cursor is bound to query and parameters.
func (conn *SQLConn) QueryPage(ctx context.Context, query string, parameters []interface{}, pageSize int, cursor string) (SQLQueryResult, error) {
	return conn.queryRequest(ctx, SQLQueryRequest{Query: query, Parameters: parameters, PageSize: pageSize, Cursor: cursor})
}

func (conn *SQLConn) queryRequest(ctx context.Context, payload SQLQueryRequest) (SQLQueryResult, error) {
	if conn == nil || strings.TrimSpace(conn.BaseURL) == "" {
		return SQLQueryResult{}, fmt.Errorf("SQL connection URL is required")
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return SQLQueryResult{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, conn.BaseURL+"/api/sql", bytes.NewReader(body))
	if err != nil {
		return SQLQueryResult{}, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	if strings.TrimSpace(conn.Token) != "" {
		req.Header.Set("Authorization", "Bearer "+conn.Token)
	}
	client := conn.Client
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return SQLQueryResult{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		data, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		if readErr != nil {
			return SQLQueryResult{}, readErr
		}
		return SQLQueryResult{}, fmt.Errorf("SQL server returned %s: %s", resp.Status, strings.TrimSpace(string(data)))
	}
	var result SQLQueryResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return SQLQueryResult{}, err
	}
	return result, nil
}

// QueryRows invokes visit once for each decoded row. It returns the number of
// rows delivered to visit; returning an error from visit stops iteration.
// Go does not support generic methods, so this is intentionally a generic
// package function instead of a method on SQLConn.
func QueryRows[T any](ctx context.Context, conn *SQLConn, query string, visit func(T) error) (int, error) {
	if visit == nil {
		return 0, fmt.Errorf("SQL row callback is required")
	}
	result, err := conn.Query(ctx, query)
	if err != nil {
		return 0, err
	}
	for index, row := range result.Rows {
		data, err := json.Marshal(row)
		if err != nil {
			return index, err
		}
		var value T
		if err := json.Unmarshal(data, &value); err != nil {
			return index, err
		}
		if err := visit(value); err != nil {
			return index + 1, err
		}
	}
	return len(result.Rows), nil
}
