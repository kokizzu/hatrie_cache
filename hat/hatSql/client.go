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

// QueryRows invokes visit for every NDJSON row. Returning an error stops it.
func QueryRows[T any](ctx context.Context, conn *Conn, query string, visit func(T) error) (int, error) {
	if visit == nil {
		return 0, fmt.Errorf("SQL row callback is required")
	}
	response, err := conn.streamRequest(ctx, QueryRequest{Query: query, Stream: true})
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()
	count := 0
	decoder := json.NewDecoder(response.Body)
	for {
		var message struct {
			Type  string          `json:"type"`
			Row   json.RawMessage `json:"row"`
			Error string          `json:"error"`
		}
		if err := decoder.Decode(&message); err != nil {
			if err == io.EOF {
				return count, nil
			}
			return count, err
		}
		switch message.Type {
		case "columns", "done":
			continue
		case "error":
			return count, fmt.Errorf("SQL stream error: %s", message.Error)
		case "row":
			var value T
			if err := json.Unmarshal(message.Row, &value); err != nil {
				return count, err
			}
			count++
			if err := visit(value); err != nil {
				return count, err
			}
		default:
			return count, fmt.Errorf("SQL stream returned unknown message type %q", message.Type)
		}
	}
}
