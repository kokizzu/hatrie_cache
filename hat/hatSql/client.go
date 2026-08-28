package hatSql

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// Conn is an HTTP client for the read-only hatrie-cache SQL endpoint.
type Conn struct {
	BaseURL string
	Token   string
	Client  *http.Client
}

// NewConn creates a connection to a hatrie-cache monitoring endpoint.
func NewConn(baseURL, token string) *Conn {
	return &Conn{BaseURL: strings.TrimRight(baseURL, "/"), Token: token, Client: http.DefaultClient}
}

// Query executes one relational SQL query and returns materialized rows.
func (conn *Conn) Query(ctx context.Context, query string) (QueryResult, error) {
	return conn.QueryParameters(ctx, query, nil)
}

// QueryParameters executes a query with positional $1, $2, ... values.
func (conn *Conn) QueryParameters(ctx context.Context, query string, parameters []interface{}) (QueryResult, error) {
	return conn.queryRequest(ctx, QueryRequest{Query: query, Parameters: parameters})
}

// QueryPage obtains one bounded result page using an opaque cursor.
func (conn *Conn) QueryPage(ctx context.Context, query string, parameters []interface{}, pageSize int, cursor string) (QueryResult, error) {
	return conn.queryRequest(ctx, QueryRequest{Query: query, Parameters: parameters, PageSize: pageSize, Cursor: cursor})
}

func (conn *Conn) queryRequest(ctx context.Context, payload QueryRequest) (QueryResult, error) {
	if conn == nil || strings.TrimSpace(conn.BaseURL) == "" {
		return QueryResult{}, fmt.Errorf("SQL connection URL is required")
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return QueryResult{}, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, conn.BaseURL+"/api/sql", bytes.NewReader(body))
	if err != nil {
		return QueryResult{}, err
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Content-Type", "application/json")
	if strings.TrimSpace(conn.Token) != "" {
		request.Header.Set("Authorization", "Bearer "+conn.Token)
	}
	response, err := conn.client().Do(request)
	if err != nil {
		return QueryResult{}, err
	}
	defer response.Body.Close()
	if err := validateResponse(response); err != nil {
		return QueryResult{}, err
	}
	var result QueryResult
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return QueryResult{}, err
	}
	return result, nil
}

func (conn *Conn) streamRequest(ctx context.Context, payload QueryRequest) (*http.Response, error) {
	if conn == nil || strings.TrimSpace(conn.BaseURL) == "" {
		return nil, fmt.Errorf("SQL connection URL is required")
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, conn.BaseURL+"/api/sql", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Accept", "application/x-ndjson")
	request.Header.Set("Content-Type", "application/json")
	if strings.TrimSpace(conn.Token) != "" {
		request.Header.Set("Authorization", "Bearer "+conn.Token)
	}
	response, err := conn.client().Do(request)
	if err != nil {
		return nil, err
	}
	if err := validateResponse(response); err != nil {
		response.Body.Close()
		return nil, err
	}
	return response, nil
}

func (conn *Conn) client() *http.Client {
	if conn.Client != nil {
		return conn.Client
	}
	return http.DefaultClient
}

func validateResponse(response *http.Response) error {
	if response.StatusCode >= http.StatusOK && response.StatusCode < http.StatusMultipleChoices {
		return nil
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return err
	}
	return fmt.Errorf("SQL server returned %s: %s", response.Status, strings.TrimSpace(string(data)))
}

// RowIterator incrementally decodes typed rows from one NDJSON SQL response.
// Call Close when stopping before the stream completes to release its HTTP
// connection. Next performs no read-ahead beyond the next protocol message.
type RowIterator[T any] struct {
	response *http.Response
	decoder  *json.Decoder
	columns  []string
	row      T
	err      error
	done     bool
}

// QueryIterator opens a pull-based typed row iterator. Positional parameters
// are encoded separately from query text and the supplied context cancels the
// HTTP request and any blocked Next call.
func QueryIterator[T any](ctx context.Context, conn *Conn, query string, parameters []interface{}) (*RowIterator[T], error) {
	response, err := conn.streamRequest(ctx, QueryRequest{Query: query, Parameters: parameters, Stream: true})
	if err != nil {
		return nil, err
	}
	return &RowIterator[T]{response: response, decoder: json.NewDecoder(response.Body)}, nil
}

// Next advances to the next row. It returns false at completion or when Err
// becomes non-nil.
func (iterator *RowIterator[T]) Next() bool {
	if iterator == nil || iterator.done || iterator.err != nil {
		return false
	}
	for {
		var message struct {
			Type    string          `json:"type"`
			Columns []string        `json:"columns"`
			Row     json.RawMessage `json:"row"`
			Error   string          `json:"error"`
		}
		if err := iterator.decoder.Decode(&message); err != nil {
			if err != io.EOF {
				iterator.err = err
			}
			iterator.done = true
			_ = iterator.Close()
			return false
		}
		switch message.Type {
		case "columns":
			iterator.columns = append(iterator.columns[:0], message.Columns...)
		case "done":
			iterator.done = true
			_ = iterator.Close()
			return false
		case "error":
			iterator.err = fmt.Errorf("SQL stream error: %s", message.Error)
			iterator.done = true
			_ = iterator.Close()
			return false
		case "row":
			var value T
			if err := json.Unmarshal(message.Row, &value); err != nil {
				iterator.err = err
				iterator.done = true
				_ = iterator.Close()
				return false
			}
			iterator.row = value
			return true
		default:
			iterator.err = fmt.Errorf("SQL stream returned unknown message type %q", message.Type)
			iterator.done = true
			_ = iterator.Close()
			return false
		}
	}
}

// Row returns the row from the most recent successful Next call.
func (iterator *RowIterator[T]) Row() T {
	if iterator == nil {
		var zero T
		return zero
	}
	return iterator.row
}

// Columns returns a copy of the stream's declared result columns.
func (iterator *RowIterator[T]) Columns() []string {
	if iterator == nil {
		return nil
	}
	return append([]string(nil), iterator.columns...)
}

// Err reports a stream protocol or typed-row decoding failure.
func (iterator *RowIterator[T]) Err() error {
	if iterator == nil {
		return nil
	}
	return iterator.err
}

// Close releases the HTTP response body. It is safe to call multiple times.
func (iterator *RowIterator[T]) Close() error {
	if iterator == nil || iterator.response == nil {
		return nil
	}
	response := iterator.response
	iterator.response = nil
	return response.Body.Close()
}

// QueryRows invokes visit for every NDJSON row. Returning an error stops it.
func QueryRows[T any](ctx context.Context, conn *Conn, query string, visit func(T) error) (int, error) {
	if visit == nil {
		return 0, fmt.Errorf("SQL row callback is required")
	}
	iterator, err := QueryIterator[T](ctx, conn, query, nil)
	if err != nil {
		return 0, err
	}
	defer iterator.Close()
	count := 0
	for iterator.Next() {
		count++
		if err := visit(iterator.Row()); err != nil {
			return count, err
		}
	}
	return count, iterator.Err()
}
