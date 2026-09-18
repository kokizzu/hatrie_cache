package hatSql

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	// DefaultRemoteTableFunctionResponseBytes bounds one remote response.
	DefaultRemoteTableFunctionResponseBytes int64 = 16 << 20
	// DefaultRemoteTableFunctionRows bounds decoded rows from one response.
	DefaultRemoteTableFunctionRows = 1_000_000
)

var (
	ErrRemoteTableFunctionNil              = errors.New("remote table function resolver is nil")
	ErrRemoteTableFunctionBaseRequired     = errors.New("remote table function base resolver is required")
	ErrRemoteTableFunctionArguments        = errors.New("remote table function arguments are invalid")
	ErrRemoteTableFunctionURLScheme        = errors.New("remote table function URL must use HTTP or HTTPS")
	ErrRemoteTableFunctionS3Endpoint       = errors.New("remote table function S3 endpoint is invalid")
	ErrRemoteTableFunctionRangeInvalid     = errors.New("remote table function byte range is invalid")
	ErrRemoteTableFunctionResponseTooLarge = errors.New("remote table function response is too large")
	ErrRemoteTableFunctionRowsTooMany      = errors.New("remote table function returned too many rows")
	ErrRemoteTableFunctionFormat           = errors.New("remote table function format is unsupported")
)

// RemoteTableFunctionResolverOptions configures the opt-in URL and S3 table
// functions. The resolver reads public HTTP(S) objects only; authentication,
// custom headers, and network policy belong in the supplied HTTP client.
type RemoteTableFunctionResolverOptions struct {
	HTTPClient       *http.Client
	S3Endpoint       string
	MaxResponseBytes int64
	MaxRows          int
}

// RemoteTableFunctionResolver adds URL() and S3() table functions while
// delegating ordinary sources and unknown table functions to Base.
//
// URL arguments are (url, format[, start, length]), where format is csv, json,
// or ndjson. S3 arguments are (bucket, key, format[, start, length]). Ranges
// use the HTTP Range header and are bounded by MaxResponseBytes.
type RemoteTableFunctionResolver struct {
	TableFunctionResolver
	client          *http.Client
	s3Endpoint      *url.URL
	maxResponseSize int64
	maxRows         int
}

// NewRemoteTableFunctionResolver creates an opt-in remote table-function
// resolver. Base remains responsible for ordinary SQL sources and other table
// functions.
func NewRemoteTableFunctionResolver(base TableFunctionResolver, options RemoteTableFunctionResolverOptions) (*RemoteTableFunctionResolver, error) {
	if base == nil {
		return nil, ErrRemoteTableFunctionBaseRequired
	}
	maxResponseSize := options.MaxResponseBytes
	if maxResponseSize == 0 {
		maxResponseSize = DefaultRemoteTableFunctionResponseBytes
	}
	if maxResponseSize < 1 || maxResponseSize > 1<<30 {
		return nil, fmt.Errorf("%w: response byte limit", ErrRemoteTableFunctionArguments)
	}
	maxRows := options.MaxRows
	if maxRows == 0 {
		maxRows = DefaultRemoteTableFunctionRows
	}
	if maxRows < 1 || maxRows > 100_000_000 {
		return nil, fmt.Errorf("%w: row limit", ErrRemoteTableFunctionArguments)
	}
	client := options.HTTPClient
	if client == nil {
		client = &http.Client{
			Timeout: 30 * time.Second,
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
				return http.ErrUseLastResponse
			},
		}
	}
	endpointText := strings.TrimSpace(options.S3Endpoint)
	if endpointText == "" {
		endpointText = "https://s3.amazonaws.com"
	}
	endpoint, err := validateRemoteTableFunctionURL(endpointText)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrRemoteTableFunctionS3Endpoint, err)
	}
	return &RemoteTableFunctionResolver{
		TableFunctionResolver: base,
		client:                client,
		s3Endpoint:            endpoint,
		maxResponseSize:       maxResponseSize,
		maxRows:               maxRows,
	}, nil
}

// ResolveSQLTableFunction resolves URL and S3 functions or delegates to Base.
func (resolver *RemoteTableFunctionResolver) ResolveSQLTableFunction(name string, arguments []interface{}) ([]SQLRow, error) {
	if resolver == nil {
		return nil, ErrRemoteTableFunctionNil
	}
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "url":
		request, err := parseRemoteURLTableFunction(arguments)
		if err != nil {
			return nil, err
		}
		return resolver.fetchRemoteTableRows(request)
	case "s3":
		request, err := resolver.parseRemoteS3TableFunction(arguments)
		if err != nil {
			return nil, err
		}
		return resolver.fetchRemoteTableRows(request)
	default:
		return resolver.TableFunctionResolver.ResolveSQLTableFunction(name, arguments)
	}
}

type remoteTableFunctionRequest struct {
	target       *url.URL
	format       string
	rangeStart   int64
	rangeLength  int64
	hasByteRange bool
}

func parseRemoteURLTableFunction(arguments []interface{}) (remoteTableFunctionRequest, error) {
	if len(arguments) != 2 && len(arguments) != 4 {
		return remoteTableFunctionRequest{}, fmt.Errorf("%w: URL expects url, format, and optional start, length", ErrRemoteTableFunctionArguments)
	}
	rawURL, err := remoteTableFunctionStringArgument(arguments[0])
	if err != nil {
		return remoteTableFunctionRequest{}, err
	}
	target, err := validateRemoteTableFunctionURL(rawURL)
	if err != nil {
		return remoteTableFunctionRequest{}, err
	}
	format, err := remoteTableFunctionFormatArgument(arguments[1])
	if err != nil {
		return remoteTableFunctionRequest{}, err
	}
	request := remoteTableFunctionRequest{target: target, format: format}
	if len(arguments) == 4 {
		request.rangeStart, request.rangeLength, err = remoteTableFunctionRangeArguments(arguments[2], arguments[3])
		if err != nil {
			return remoteTableFunctionRequest{}, err
		}
		request.hasByteRange = true
	}
	return request, nil
}

func (resolver *RemoteTableFunctionResolver) parseRemoteS3TableFunction(arguments []interface{}) (remoteTableFunctionRequest, error) {
	if len(arguments) != 3 && len(arguments) != 5 {
		return remoteTableFunctionRequest{}, fmt.Errorf("%w: S3 expects bucket, key, format, and optional start, length", ErrRemoteTableFunctionArguments)
	}
	bucket, err := remoteTableFunctionStringArgument(arguments[0])
	if err != nil || strings.Contains(bucket, "/") {
		return remoteTableFunctionRequest{}, fmt.Errorf("%w: S3 bucket", ErrRemoteTableFunctionArguments)
	}
	key, err := remoteTableFunctionStringArgument(arguments[1])
	if err != nil || key == "" || remoteTableFunctionHasParentPath(key) {
		return remoteTableFunctionRequest{}, fmt.Errorf("%w: S3 key", ErrRemoteTableFunctionArguments)
	}
	if hasRemoteTableFunctionControl(bucket) || hasRemoteTableFunctionControl(key) {
		return remoteTableFunctionRequest{}, fmt.Errorf("%w: S3 path", ErrRemoteTableFunctionArguments)
	}
	format, err := remoteTableFunctionFormatArgument(arguments[2])
	if err != nil {
		return remoteTableFunctionRequest{}, err
	}
	target := *resolver.s3Endpoint
	target.Path = strings.TrimRight(target.Path, "/") + "/" + bucket + "/" + strings.TrimLeft(key, "/")
	request := remoteTableFunctionRequest{target: &target, format: format}
	if len(arguments) == 5 {
		request.rangeStart, request.rangeLength, err = remoteTableFunctionRangeArguments(arguments[3], arguments[4])
		if err != nil {
			return remoteTableFunctionRequest{}, err
		}
		request.hasByteRange = true
	}
	return request, nil
}

func (resolver *RemoteTableFunctionResolver) fetchRemoteTableRows(request remoteTableFunctionRequest) ([]SQLRow, error) {
	if resolver == nil || resolver.client == nil {
		return nil, ErrRemoteTableFunctionNil
	}
	httpRequest, err := http.NewRequest(http.MethodGet, request.target.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("%w: request: %v", ErrRemoteTableFunctionArguments, err)
	}
	if request.hasByteRange {
		end := request.rangeStart + request.rangeLength - 1
		httpRequest.Header.Set("Range", "bytes="+strconv.FormatInt(request.rangeStart, 10)+"-"+strconv.FormatInt(end, 10))
	}
	response, err := resolver.client.Do(httpRequest)
	if err != nil {
		return nil, fmt.Errorf("remote table function request: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusPartialContent {
		return nil, fmt.Errorf("remote table function HTTP status %s", response.Status)
	}
	if request.hasByteRange && request.rangeStart > 0 && response.StatusCode != http.StatusPartialContent {
		return nil, fmt.Errorf("%w: server ignored non-zero Range request", ErrRemoteTableFunctionRangeInvalid)
	}
	if response.ContentLength > resolver.maxResponseSize {
		return nil, ErrRemoteTableFunctionResponseTooLarge
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, resolver.maxResponseSize+1))
	if err != nil {
		return nil, fmt.Errorf("read remote table function response: %w", err)
	}
	if int64(len(body)) > resolver.maxResponseSize {
		return nil, ErrRemoteTableFunctionResponseTooLarge
	}
	return decodeRemoteTableFunctionRows(request.format, body, resolver.maxRows)
}

func decodeRemoteTableFunctionRows(format string, data []byte, maxRows int) ([]SQLRow, error) {
	switch format {
	case "csv":
		return decodeRemoteCSVRows(data, maxRows)
	case "json":
		return decodeRemoteJSONRows(data, maxRows)
	case "ndjson", "jsonl":
		return decodeRemoteNDJSONRows(data, maxRows)
	default:
		return nil, fmt.Errorf("%w: %s", ErrRemoteTableFunctionFormat, format)
	}
}

func decodeRemoteCSVRows(data []byte, maxRows int) ([]SQLRow, error) {
	reader := csv.NewReader(bytes.NewReader(data))
	reader.FieldsPerRecord = -1
	headers, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("decode remote CSV header: %w", err)
	}
	if len(headers) == 0 {
		return nil, fmt.Errorf("%w: empty CSV header", ErrRemoteTableFunctionArguments)
	}
	seen := make(map[string]struct{}, len(headers))
	for index := range headers {
		headers[index] = strings.TrimSpace(headers[index])
		if headers[index] == "" {
			return nil, fmt.Errorf("%w: empty CSV column", ErrRemoteTableFunctionArguments)
		}
		if _, exists := seen[headers[index]]; exists {
			return nil, fmt.Errorf("%w: duplicate CSV column %q", ErrRemoteTableFunctionArguments, headers[index])
		}
		seen[headers[index]] = struct{}{}
	}
	rows := make([]SQLRow, 0)
	for {
		record, readErr := reader.Read()
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return nil, fmt.Errorf("decode remote CSV row: %w", readErr)
		}
		if len(rows) == maxRows {
			return nil, ErrRemoteTableFunctionRowsTooMany
		}
		if len(record) != len(headers) {
			return nil, fmt.Errorf("%w: CSV row has %d fields, want %d", ErrRemoteTableFunctionArguments, len(record), len(headers))
		}
		row := make(SQLRow, len(headers))
		for index, header := range headers {
			row[header] = record[index]
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func decodeRemoteJSONRows(data []byte, maxRows int) ([]SQLRow, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	var value interface{}
	if err := decoder.Decode(&value); err != nil {
		return nil, fmt.Errorf("decode remote JSON: %w", err)
	}
	var values []interface{}
	switch typed := value.(type) {
	case []interface{}:
		values = typed
	case map[string]interface{}:
		values = []interface{}{typed}
	default:
		return nil, fmt.Errorf("%w: JSON must be an object or array", ErrRemoteTableFunctionArguments)
	}
	if len(values) > maxRows {
		return nil, ErrRemoteTableFunctionRowsTooMany
	}
	rows := make([]SQLRow, 0, len(values))
	for _, value := range values {
		row, ok := value.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("%w: JSON array contains a non-object", ErrRemoteTableFunctionArguments)
		}
		rows = append(rows, SQLRow(row))
	}
	var extra interface{}
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, fmt.Errorf("%w: multiple JSON documents", ErrRemoteTableFunctionArguments)
		}
		return nil, fmt.Errorf("decode remote JSON tail: %w", err)
	}
	return rows, nil
}

func decodeRemoteNDJSONRows(data []byte, maxRows int) ([]SQLRow, error) {
	decoder := json.NewDecoder(bufio.NewReader(bytes.NewReader(data)))
	rows := make([]SQLRow, 0)
	for {
		var row map[string]interface{}
		err := decoder.Decode(&row)
		if errors.Is(err, io.EOF) {
			return rows, nil
		}
		if err != nil {
			return nil, fmt.Errorf("decode remote NDJSON row: %w", err)
		}
		if row == nil {
			return nil, fmt.Errorf("%w: NDJSON row is not an object", ErrRemoteTableFunctionArguments)
		}
		if len(rows) == maxRows {
			return nil, ErrRemoteTableFunctionRowsTooMany
		}
		rows = append(rows, SQLRow(row))
	}
}

func validateRemoteTableFunctionURL(raw string) (*url.URL, error) {
	target, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || target.Host == "" || (target.Scheme != "http" && target.Scheme != "https") {
		return nil, ErrRemoteTableFunctionURLScheme
	}
	if target.User != nil || hasRemoteTableFunctionControl(target.String()) {
		return nil, fmt.Errorf("%w: credentials or control characters are not allowed", ErrRemoteTableFunctionArguments)
	}
	return target, nil
}

func remoteTableFunctionRangeArguments(startValue, lengthValue interface{}) (int64, int64, error) {
	start, err := remoteTableFunctionInt64Argument(startValue)
	if err != nil {
		return 0, 0, err
	}
	length, err := remoteTableFunctionInt64Argument(lengthValue)
	if err != nil || start < 0 || length < 1 || start > math.MaxInt64-length {
		return 0, 0, ErrRemoteTableFunctionRangeInvalid
	}
	return start, length, nil
}

func remoteTableFunctionStringArgument(value interface{}) (string, error) {
	text, ok := value.(string)
	if !ok || strings.TrimSpace(text) == "" {
		return "", fmt.Errorf("%w: string argument", ErrRemoteTableFunctionArguments)
	}
	return strings.TrimSpace(text), nil
}

func remoteTableFunctionFormatArgument(value interface{}) (string, error) {
	format, err := remoteTableFunctionStringArgument(value)
	if err != nil {
		return "", err
	}
	format = strings.ToLower(format)
	if format != "csv" && format != "json" && format != "ndjson" && format != "jsonl" {
		return "", fmt.Errorf("%w: %s", ErrRemoteTableFunctionFormat, format)
	}
	return format, nil
}

func remoteTableFunctionInt64Argument(value interface{}) (int64, error) {
	switch typed := value.(type) {
	case int:
		return int64(typed), nil
	case int64:
		return typed, nil
	case uint64:
		if typed > math.MaxInt64 {
			return 0, ErrRemoteTableFunctionRangeInvalid
		}
		return int64(typed), nil
	case float64:
		if typed < math.MinInt64 || typed > math.MaxInt64 || typed != math.Trunc(typed) {
			return 0, ErrRemoteTableFunctionRangeInvalid
		}
		return int64(typed), nil
	default:
		return 0, ErrRemoteTableFunctionRangeInvalid
	}
}

func hasRemoteTableFunctionControl(value string) bool {
	for index := 0; index < len(value); index++ {
		if value[index] < 0x20 || value[index] == 0x7f {
			return true
		}
	}
	return false
}

func remoteTableFunctionHasParentPath(value string) bool {
	for _, segment := range strings.Split(value, "/") {
		if segment == "." || segment == ".." {
			return true
		}
	}
	return false
}
