package hatMonitoring

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"hatrie_cache/hat/hatCommand"
)

const maxErrorBytes = 1 << 20

// Client calls authenticated monitoring HTTP endpoints.
type Client struct {
	BaseURL string
	Token   string
	HTTP    *http.Client
}

// NewClient creates a client using the default HTTP transport.
func NewClient(baseURL, token string) *Client {
	return &Client{BaseURL: strings.TrimRight(strings.TrimSpace(baseURL), "/"), Token: token, HTTP: http.DefaultClient}
}

// Health fetches /api/health.
func (client *Client) Health(ctx context.Context) (Health, error) {
	var health Health
	return health, client.get(ctx, "/api/health", nil, &health)
}

// Entries fetches one /api/entries page.
func (client *Client) Entries(ctx context.Context, request EntriesRequest) (EntriesResponse, error) {
	request, err := request.Validate()
	if err != nil {
		return EntriesResponse{}, err
	}
	query := url.Values{}
	if request.Prefix != "" {
		query.Set("prefix", request.Prefix)
	}
	if request.AfterKey != "" {
		query.Set("after_key", request.AfterKey)
	}
	if request.Limit != 0 {
		query.Set("limit", strconv.Itoa(request.Limit))
	}
	var entries EntriesResponse
	return entries, client.get(ctx, "/api/entries", query, &entries)
}

// Command executes one public cache command over the JSON HTTP contract.
func (client *Client) Command(ctx context.Context, command hatCommand.Request) (hatCommand.Response, error) {
	body, err := json.Marshal(command)
	if err != nil {
		return hatCommand.Response{}, err
	}
	var response hatCommand.Response
	err = client.do(ctx, http.MethodPost, "/api/commands", nil, bytes.NewReader(body), "application/json", &response)
	return response, err
}

func (client *Client) get(ctx context.Context, path string, query url.Values, target interface{}) error {
	return client.do(ctx, http.MethodGet, path, query, nil, "", target)
}

func (client *Client) do(ctx context.Context, method string, path string, query url.Values, body io.Reader, contentType string, target interface{}) error {
	if client == nil || strings.TrimSpace(client.BaseURL) == "" {
		return fmt.Errorf("monitoring base URL is required")
	}
	base, err := url.Parse(client.BaseURL)
	if err != nil || base.Scheme == "" || base.Host == "" {
		return fmt.Errorf("invalid monitoring base URL")
	}
	base.Path = strings.TrimRight(base.Path, "/") + path
	base.RawQuery = query.Encode()
	request, err := http.NewRequestWithContext(ctx, method, base.String(), body)
	if err != nil {
		return err
	}
	if contentType != "" {
		request.Header.Set("Content-Type", contentType)
	}
	if client.Token != "" {
		request.Header.Set("Authorization", "Bearer "+client.Token)
	}
	httpClient := client.HTTP
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	response, err := httpClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		body, _ := io.ReadAll(io.LimitReader(response.Body, maxErrorBytes))
		return fmt.Errorf("monitoring request failed: %s: %s", response.Status, strings.TrimSpace(string(body)))
	}
	return json.NewDecoder(response.Body).Decode(target)
}
