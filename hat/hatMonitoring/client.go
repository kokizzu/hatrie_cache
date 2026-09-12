package hatMonitoring

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"hatrie_cache/hat/hatCommand"
	"hatrie_cache/internal/jsonwire"
)

const maxErrorBytes = 1 << 20

const maxCommandResponseBytes int64 = 64 << 20

// Client calls authenticated monitoring HTTP endpoints.
type Client struct {
	BaseURL string
	Token   string
	HTTP    *http.Client
	// CommandWireFormat selects the default command request/response format.
	// The zero value preserves the language-neutral JSON contract.
	CommandWireFormat hatCommand.CommandWireFormat
	// CommandCompressionThreshold enables gzip for command request bodies whose
	// encoded size reaches the threshold. Zero or a negative value disables it.
	CommandCompressionThreshold int
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

// Command executes one public cache command using the configured wire format.
func (client *Client) Command(ctx context.Context, command hatCommand.Request) (hatCommand.Response, error) {
	return client.CommandWithFormat(ctx, command, client.commandWireFormat())
}

func (client *Client) commandWireFormat() hatCommand.CommandWireFormat {
	if client != nil && client.CommandWireFormat == hatCommand.CommandWireFormatProtobuf {
		return hatCommand.CommandWireFormatProtobuf
	}
	return hatCommand.CommandWireFormatJSON
}

func (client *Client) commandCompressionThreshold() int {
	if client == nil || client.CommandCompressionThreshold <= 0 {
		return 0
	}
	return client.CommandCompressionThreshold
}

func (client *Client) commandJSON(ctx context.Context, command hatCommand.Request) (hatCommand.Response, error) {
	body, err := json.Marshal(command)
	if err != nil {
		return hatCommand.Response{}, err
	}
	bodyReader, contentEncoding, err := jsonwire.EncodedRequestBody(body, client.commandCompressionThreshold())
	if err != nil {
		return hatCommand.Response{}, err
	}
	if closer, ok := bodyReader.(io.Closer); ok {
		defer closer.Close()
	}
	var response hatCommand.Response
	err = client.doWithContentEncoding(ctx, http.MethodPost, "/api/commands", nil, bodyReader, "application/json", contentEncoding, &response)
	return response, err
}

// CommandWithFormat executes one public cache command using the requested
// command wire format. JSON remains the default used by Command.
func (client *Client) CommandWithFormat(ctx context.Context, command hatCommand.Request, format hatCommand.CommandWireFormat) (hatCommand.Response, error) {
	if format == hatCommand.CommandWireFormatJSON {
		return client.commandJSON(ctx, command)
	}
	body, contentType, contentEncoding, err := hatCommand.CommandRequestBody(command, format, 0, client.commandCompressionThreshold())
	if err != nil {
		return hatCommand.Response{}, err
	}
	if closer, ok := body.(io.Closer); ok {
		defer closer.Close()
	}
	var response hatCommand.Response
	err = client.doCommand(ctx, http.MethodPost, "/api/commands", body, contentType, contentEncoding, &response)
	return response, err
}

// Batch executes multiple public cache commands in one HTTP request.
func (client *Client) Batch(ctx context.Context, commands []hatCommand.Request, atomic bool) (hatCommand.Response, error) {
	return client.BatchWithFormat(ctx, commands, atomic, client.commandWireFormat())
}

// BatchWithFormat executes multiple public cache commands in one request using
// the requested command wire format.
func (client *Client) BatchWithFormat(ctx context.Context, commands []hatCommand.Request, atomic bool, format hatCommand.CommandWireFormat) (hatCommand.Response, error) {
	return client.CommandWithFormat(ctx, hatCommand.Request{Command: "BATCH", Batch: commands, Atomic: atomic}, format)
}

func (client *Client) get(ctx context.Context, path string, query url.Values, target interface{}) error {
	return client.do(ctx, http.MethodGet, path, query, nil, "", target)
}

func (client *Client) do(ctx context.Context, method string, path string, query url.Values, body io.Reader, contentType string, target interface{}) error {
	return client.doWithContentEncoding(ctx, method, path, query, body, contentType, "", target)
}

func (client *Client) doWithContentEncoding(ctx context.Context, method string, path string, query url.Values, body io.Reader, contentType string, contentEncoding string, target interface{}) error {
	httpClient, request, err := client.newRequest(ctx, method, path, query, body, contentType, contentEncoding)
	if err != nil {
		return err
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

func (client *Client) doCommand(ctx context.Context, method string, path string, body io.Reader, contentType string, contentEncoding string, target *hatCommand.Response) error {
	httpClient, request, err := client.newRequest(ctx, method, path, nil, body, contentType, contentEncoding)
	if err != nil {
		return err
	}
	if contentType != "" {
		request.Header.Set("Accept", contentType)
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
	decoded, err := hatCommand.DecodeCommandResponseWire(response.Body, response.Header.Get("Content-Type"), maxCommandResponseBytes)
	if err != nil {
		return err
	}
	*target = decoded
	return nil
}

func (client *Client) newRequest(ctx context.Context, method string, path string, query url.Values, body io.Reader, contentType string, contentEncoding string) (*http.Client, *http.Request, error) {
	if client == nil || strings.TrimSpace(client.BaseURL) == "" {
		return nil, nil, fmt.Errorf("monitoring base URL is required")
	}
	base, err := url.Parse(client.BaseURL)
	if err != nil || base.Scheme == "" || base.Host == "" {
		return nil, nil, fmt.Errorf("invalid monitoring base URL")
	}
	base.Path = strings.TrimRight(base.Path, "/") + path
	base.RawQuery = query.Encode()
	request, err := http.NewRequestWithContext(ctx, method, base.String(), body)
	if err != nil {
		return nil, nil, err
	}
	if contentType != "" {
		request.Header.Set("Content-Type", contentType)
	}
	if contentEncoding != "" {
		request.Header.Set("Content-Encoding", contentEncoding)
	}
	if client.Token != "" {
		request.Header.Set("Authorization", "Bearer "+client.Token)
	}
	httpClient := client.HTTP
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	return httpClient, request, nil
}
