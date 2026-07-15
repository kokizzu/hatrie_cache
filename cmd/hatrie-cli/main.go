package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"hatrie_cache/internal/jsonwire"

	hatriecache "hatrie_cache"
)

type clientConfig struct {
	addr    string
	timeout time.Duration
}

const maxErrorBodyBytes = 1 << 20
const maxResponseDrainBytes = 1 << 20
const truncatedErrorBodySuffix = "\n... response body truncated"
const minCompressedJSONRequestBytes = 16 << 10
const defaultCommandWireFormat = "auto"
const defaultRequestTimeout = 30 * time.Second

func main() {
	if err := run(context.Background(), os.Args[1:], os.Stdout, os.Stderr, http.DefaultClient); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func cliContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func cliHTTPClient(client *http.Client) *http.Client {
	if client == nil {
		return http.DefaultClient
	}
	return client
}

func run(ctx context.Context, args []string, stdout io.Writer, stderr io.Writer, client *http.Client) error {
	ctx = cliContext(ctx)
	cfg, remaining, err := parseGlobalFlags(args, stderr)
	if err != nil {
		return err
	}
	if cfg.timeout < 0 {
		return errors.New("timeout must be non-negative")
	}
	if cfg.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cfg.timeout)
		defer cancel()
	}
	if len(remaining) == 0 {
		return errors.New("subcommand is required: health, stats, entries, topology, election, replication, journal, command, snapshot")
	}

	switch remaining[0] {
	case "health":
		return getJSON(ctx, client, cfg.addr, "/api/health", stdout)
	case "stats":
		return getJSON(ctx, client, cfg.addr, "/api/stats", stdout)
	case "entries":
		return runEntries(ctx, client, cfg.addr, remaining[1:], stdout, stderr)
	case "topology":
		return runTopology(ctx, client, cfg.addr, remaining[1:], stdout, stderr)
	case "election":
		return runElection(ctx, client, cfg.addr, remaining[1:], stdout, stderr)
	case "replication":
		return runReplication(ctx, client, cfg.addr, remaining[1:], stdout, stderr)
	case "journal":
		return runJournal(ctx, client, cfg.addr, remaining[1:], stdout, stderr)
	case "command":
		return runCommand(ctx, client, cfg.addr, remaining[1:], stdout, stderr)
	case "snapshot":
		return postJSON(ctx, client, cfg.addr, "/api/snapshot", []byte("{}"), stdout)
	default:
		return fmt.Errorf("unknown subcommand %q", remaining[0])
	}
}

func parseGlobalFlags(args []string, output io.Writer) (clientConfig, []string, error) {
	cfg := clientConfig{
		addr:    "http://127.0.0.1:8080",
		timeout: defaultRequestTimeout,
	}
	flags := flag.NewFlagSet("hatrie-cli", flag.ContinueOnError)
	flags.SetOutput(output)
	flags.StringVar(&cfg.addr, "addr", cfg.addr, "monitoring server base URL")
	flags.DurationVar(&cfg.timeout, "timeout", cfg.timeout, "request timeout; use 0 to disable")
	if err := flags.Parse(args); err != nil {
		return clientConfig{}, nil, err
	}
	return cfg, flags.Args(), nil
}

func runEntries(ctx context.Context, client *http.Client, addr string, args []string, stdout io.Writer, stderr io.Writer) error {
	flags := flag.NewFlagSet("entries", flag.ContinueOnError)
	flags.SetOutput(stderr)
	prefix := flags.String("prefix", "", "key prefix filter")
	limit := flags.Uint64("limit", 0, "maximum entries to fetch")
	afterKey := flags.String("after-key", "", "only return entries after this key")
	if err := flags.Parse(args); err != nil {
		return err
	}

	path := "/api/entries"
	query := url.Values{}
	if *prefix != "" {
		query.Set("prefix", *prefix)
	}
	if *limit > 0 {
		query.Set("limit", strconv.FormatUint(*limit, 10))
	}
	if *afterKey != "" {
		query.Set("after_key", *afterKey)
	}
	if encoded := query.Encode(); encoded != "" {
		path += "?" + encoded
	}
	return getJSON(ctx, client, addr, path, stdout)
}

func runTopology(ctx context.Context, client *http.Client, addr string, args []string, stdout io.Writer, stderr io.Writer) error {
	flags := flag.NewFlagSet("topology", flag.ContinueOnError)
	flags.SetOutput(stderr)
	filePath := flags.String("file", "", "topology JSON file to upload")
	key := flags.String("key", "", "cache key to route through the current topology")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *filePath != "" && *key != "" {
		return errors.New("topology -file and -key are mutually exclusive")
	}
	if *filePath != "" {
		file, err := os.Open(*filePath)
		if err != nil {
			return err
		}
		defer file.Close()
		return putJSONReader(ctx, client, addr, "/api/topology", file, stdout)
	}
	path := "/api/topology"
	if *key != "" {
		path += "?key=" + url.QueryEscape(*key)
	}
	return getJSON(ctx, client, addr, path, stdout)
}

func runElection(ctx context.Context, client *http.Client, addr string, args []string, stdout io.Writer, stderr io.Writer) error {
	flags := flag.NewFlagSet("election", flag.ContinueOnError)
	flags.SetOutput(stderr)
	key := flags.String("key", "", "cache key to route through election")
	heartbeat := flags.String("heartbeat", "", "node id to mark online")
	offline := flags.String("offline", "", "node id to mark offline")
	if err := flags.Parse(args); err != nil {
		return err
	}
	mutating := 0
	for _, value := range []string{*heartbeat, *offline} {
		if value != "" {
			mutating++
		}
	}
	if mutating > 1 || (mutating > 0 && *key != "") {
		return errors.New("election -key, -heartbeat, and -offline are mutually exclusive")
	}
	if *heartbeat != "" {
		return postElectionUpdate(ctx, client, addr, *heartbeat, true, stdout)
	}
	if *offline != "" {
		return postElectionUpdate(ctx, client, addr, *offline, false, stdout)
	}
	path := "/api/election"
	if *key != "" {
		path += "?key=" + url.QueryEscape(*key)
	}
	return getJSON(ctx, client, addr, path, stdout)
}

func postElectionUpdate(ctx context.Context, client *http.Client, addr string, node string, online bool, stdout io.Writer) error {
	body, err := jsonwire.Marshal(struct {
		Node   string `json:"node"`
		Online bool   `json:"online"`
	}{
		Node:   node,
		Online: online,
	})
	if err != nil {
		return err
	}
	return postJSON(ctx, client, addr, "/api/election", body, stdout)
}

func runReplication(ctx context.Context, client *http.Client, addr string, args []string, stdout io.Writer, stderr io.Writer) error {
	flags := flag.NewFlagSet("replication", flag.ContinueOnError)
	flags.SetOutput(stderr)
	sync := flags.Bool("sync", false, "push local entries to topology replicas")
	prefix := flags.String("prefix", "", "key prefix to sync")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *prefix != "" && !*sync {
		return errors.New("replication -prefix requires -sync")
	}
	if !*sync {
		return getJSON(ctx, client, addr, "/api/replication", stdout)
	}
	body, err := jsonwire.Marshal(struct {
		Prefix string `json:"prefix,omitempty"`
	}{
		Prefix: *prefix,
	})
	if err != nil {
		return err
	}
	return postJSON(ctx, client, addr, "/api/replication", body, stdout)
}

func runJournal(ctx context.Context, client *http.Client, addr string, args []string, stdout io.Writer, stderr io.Writer) error {
	flags := flag.NewFlagSet("journal", flag.ContinueOnError)
	flags.SetOutput(stderr)
	afterSequence := flags.Uint64("after-sequence", 0, "only return journal entries after this sequence")
	limit := flags.Uint64("limit", 0, "maximum journal entries to fetch or pull")
	pullFrom := flags.String("pull-from", "", "source monitoring server base URL to pull and apply journal entries from")
	untilCurrent := flags.Bool("until-current", false, "keep pulling batches until the source has no more entries")
	maxBatches := flags.Uint64("max-batches", 0, "maximum journal batches to pull with -until-current")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *maxBatches > 0 && !*untilCurrent {
		return errors.New("journal -max-batches requires -until-current")
	}
	if *untilCurrent && strings.TrimSpace(*pullFrom) == "" {
		return errors.New("journal -until-current requires -pull-from")
	}

	if strings.TrimSpace(*pullFrom) != "" {
		body, err := jsonwire.Marshal(struct {
			Source        string `json:"source"`
			AfterSequence uint64 `json:"after_sequence,omitempty"`
			Limit         uint64 `json:"limit,omitempty"`
			UntilCurrent  bool   `json:"until_current,omitempty"`
			MaxBatches    uint64 `json:"max_batches,omitempty"`
		}{
			Source:        strings.TrimSpace(*pullFrom),
			AfterSequence: *afterSequence,
			Limit:         *limit,
			UntilCurrent:  *untilCurrent,
			MaxBatches:    *maxBatches,
		})
		if err != nil {
			return err
		}
		return postJSON(ctx, client, addr, "/api/journal", body, stdout)
	}

	path := "/api/journal"
	query := url.Values{}
	if *afterSequence > 0 {
		query.Set("after_sequence", strconv.FormatUint(*afterSequence, 10))
	}
	if *limit > 0 {
		query.Set("limit", strconv.FormatUint(*limit, 10))
	}
	if encoded := query.Encode(); encoded != "" {
		path += "?" + encoded
	}
	return getJSON(ctx, client, addr, path, stdout)
}

func runCommand(ctx context.Context, client *http.Client, addr string, args []string, stdout io.Writer, stderr io.Writer) error {
	flags := flag.NewFlagSet("command", flag.ContinueOnError)
	flags.SetOutput(stderr)
	command := flags.String("cmd", "", "cache command")
	key := flags.String("key", "", "cache key")
	value := flags.String("value", "", "cache value")
	valuesJSON := flags.String("values", "", "JSON array for commands that accept multiple values")
	subkey := flags.String("subkey", "", "secondary key or command argument")
	pairsJSON := flags.String("pairs", "", "JSON object for map or radix tree fields")
	priority := flags.String("priority", "", "priority for priority queue push")
	ttlSeconds := flags.Int64("ttl-seconds", -1, "optional ttl in seconds")
	unixSeconds := flags.Int64("unix-seconds", -1, "optional absolute expiration as Unix seconds")
	wireFormat := flags.String("wire-format", defaultCommandWireFormat, "command request wire format: auto, protobuf, or json")
	if err := flags.Parse(args); err != nil {
		return err
	}

	request := hatriecache.CacheCommandRequest{
		Command: *command,
		Key:     *key,
		Value:   *value,
		Subkey:  *subkey,
	}
	if *ttlSeconds >= 0 {
		request.TTLSeconds = ttlSeconds
	}
	if *unixSeconds >= 0 {
		request.UnixSeconds = unixSeconds
	}
	if *priority != "" {
		parsed, err := strconv.ParseInt(strings.TrimSpace(*priority), 10, 64)
		if err != nil {
			return fmt.Errorf("priority: %w", err)
		}
		request.Priority = &parsed
	}
	if *valuesJSON != "" {
		values, err := decodeJSONFlag[hatriecache.Slice](*valuesJSON)
		if err != nil {
			return fmt.Errorf("values: %w", err)
		}
		request.Values = values
	}
	if *pairsJSON != "" {
		pairs, err := decodeJSONFlag[hatriecache.Map](*pairsJSON)
		if err != nil {
			return fmt.Errorf("pairs: %w", err)
		}
		request.Pairs = pairs
	}
	return postCommandValue(ctx, client, addr, request, *wireFormat, stdout)
}

func decodeJSONFlag[T any](value string) (T, error) {
	var out T
	decoder := jsonwire.NewDecoder(strings.NewReader(value))
	decoder.UseNumber()
	if err := decoder.Decode(&out); err != nil {
		return out, err
	}
	var extra struct{}
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return out, errors.New("invalid trailing JSON")
	}
	return out, nil
}

func getJSON(ctx context.Context, client *http.Client, addr string, path string, stdout io.Writer) error {
	req, err := http.NewRequestWithContext(cliContext(ctx), http.MethodGet, endpoint(addr, path), nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	return doAndCopy(client, req, stdout)
}

func postJSON(ctx context.Context, client *http.Client, addr string, path string, body []byte, stdout io.Writer) error {
	reader, contentEncoding, err := jsonRequestBody(body)
	if err != nil {
		return err
	}
	return postJSONReaderWithEncoding(ctx, client, addr, path, reader, contentEncoding, stdout)
}

func postJSONValue(ctx context.Context, client *http.Client, addr string, path string, value interface{}, estimatedSize int, stdout io.Writer) error {
	reader, contentEncoding, err := jsonValueRequestBody(value, estimatedSize)
	if err != nil {
		return err
	}
	return postJSONReaderWithEncoding(ctx, client, addr, path, reader, contentEncoding, stdout)
}

func postCommandValue(ctx context.Context, client *http.Client, addr string, request hatriecache.CacheCommandRequest, wireFormat string, stdout io.Writer) error {
	reader, contentType, contentEncoding, err := commandRequestBody(request, wireFormat)
	if err != nil {
		return err
	}
	err = postCommandReader(ctx, client, addr, reader, contentType, contentEncoding, stdout)
	if shouldRetryCommandAsJSON(wireFormat, contentType, err) {
		reader, contentType, contentEncoding, err = hatriecache.CommandRequestBody(request, hatriecache.CommandWireFormatJSON, estimatedCommandRequestBytes(request), minCompressedJSONRequestBytes)
		if err != nil {
			return err
		}
		return postCommandReader(ctx, client, addr, reader, contentType, contentEncoding, stdout)
	}
	return err
}

func postCommandReader(ctx context.Context, client *http.Client, addr string, body io.Reader, contentType string, contentEncoding string, stdout io.Writer) error {
	req, err := http.NewRequestWithContext(cliContext(ctx), http.MethodPost, endpoint(addr, "/api/commands"), body)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", contentType)
	req.Header.Set("Content-Type", contentType)
	if contentEncoding != "" {
		req.Header.Set("Content-Encoding", contentEncoding)
	}
	return doCommandAndCopy(client, req, stdout)
}

func shouldRetryCommandAsJSON(wireFormat string, contentType string, err error) bool {
	if !commandWireFormatAuto(wireFormat) || contentType != "application/x-protobuf" {
		return false
	}
	var httpErr *commandHTTPError
	return errors.As(err, &httpErr) && httpErr.statusCode == http.StatusUnsupportedMediaType
}

func commandWireFormatAuto(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", defaultCommandWireFormat:
		return true
	default:
		return false
	}
}

func postJSONReaderWithEncoding(ctx context.Context, client *http.Client, addr string, path string, body io.Reader, contentEncoding string, stdout io.Writer) error {
	req, err := http.NewRequestWithContext(cliContext(ctx), http.MethodPost, endpoint(addr, path), body)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	if contentEncoding != "" {
		req.Header.Set("Content-Encoding", contentEncoding)
	}
	return doAndCopy(client, req, stdout)
}

func jsonValueRequestBody(value interface{}, estimatedSize int) (io.Reader, string, error) {
	return jsonwire.RequestBody(value, estimatedSize, minCompressedJSONRequestBytes)
}

func jsonRequestBody(data []byte) (io.Reader, string, error) {
	return jsonwire.EncodedRequestBody(data, minCompressedJSONRequestBytes)
}

func commandRequestBody(request hatriecache.CacheCommandRequest, wireFormat string) (io.Reader, string, string, error) {
	estimatedSize := estimatedCommandRequestBytes(request)
	switch strings.ToLower(strings.TrimSpace(wireFormat)) {
	case "", defaultCommandWireFormat:
		body, contentType, contentEncoding, err := hatriecache.CommandRequestBody(request, hatriecache.CommandWireFormatProtobuf, estimatedSize, minCompressedJSONRequestBytes)
		if err == nil {
			return body, contentType, contentEncoding, nil
		}
		return hatriecache.CommandRequestBody(request, hatriecache.CommandWireFormatJSON, estimatedSize, minCompressedJSONRequestBytes)
	case string(hatriecache.CommandWireFormatJSON):
		return hatriecache.CommandRequestBody(request, hatriecache.CommandWireFormatJSON, estimatedSize, minCompressedJSONRequestBytes)
	case string(hatriecache.CommandWireFormatProtobuf), "proto", "pb":
		return hatriecache.CommandRequestBody(request, hatriecache.CommandWireFormatProtobuf, estimatedSize, minCompressedJSONRequestBytes)
	default:
		return nil, "", "", fmt.Errorf("unsupported command wire format %q", wireFormat)
	}
}

func estimatedCommandRequestBytes(request hatriecache.CacheCommandRequest) int {
	estimate := 64 +
		jsonwire.EstimateJSONStringBytes(request.Command) +
		jsonwire.EstimateJSONStringBytes(request.Key) +
		jsonwire.EstimateJSONStringBytes(request.Value) +
		jsonwire.EstimateJSONStringBytes(request.Subkey)
	if estimate >= minCompressedJSONRequestBytes {
		return minCompressedJSONRequestBytes
	}
	for _, value := range request.Values {
		estimate = jsonwire.AddEstimate(estimate, jsonwire.EstimateJSONValueBytes(value, minCompressedJSONRequestBytes), minCompressedJSONRequestBytes)
		if estimate >= minCompressedJSONRequestBytes {
			return minCompressedJSONRequestBytes
		}
	}
	for key, value := range request.Pairs {
		estimate = jsonwire.AddEstimate(estimate, jsonwire.EstimateJSONStringBytes(key)+1, minCompressedJSONRequestBytes)
		if estimate >= minCompressedJSONRequestBytes {
			return minCompressedJSONRequestBytes
		}
		estimate = jsonwire.AddEstimate(estimate, jsonwire.EstimateJSONValueBytes(value, minCompressedJSONRequestBytes), minCompressedJSONRequestBytes)
		if estimate >= minCompressedJSONRequestBytes {
			return minCompressedJSONRequestBytes
		}
	}
	if request.Priority != nil {
		estimate = jsonwire.AddEstimate(estimate, 20, minCompressedJSONRequestBytes)
	}
	if request.TTLSeconds != nil {
		estimate = jsonwire.AddEstimate(estimate, 20, minCompressedJSONRequestBytes)
	}
	if request.UnixSeconds != nil {
		estimate = jsonwire.AddEstimate(estimate, 20, minCompressedJSONRequestBytes)
	}
	return estimate
}

func putJSONReader(ctx context.Context, client *http.Client, addr string, path string, body io.Reader, stdout io.Writer) error {
	req, err := http.NewRequestWithContext(cliContext(ctx), http.MethodPut, endpoint(addr, path), body)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	return doAndCopy(client, req, stdout)
}

func doAndCopy(client *http.Client, req *http.Request, stdout io.Writer) error {
	client = cliHTTPClient(client)
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer drainAndCloseResponse(resp.Body)

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, err := readErrorBody(resp.Body)
		if err != nil {
			return err
		}
		return fmt.Errorf("server returned %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}
	return copyAndEnsureTrailingNewline(stdout, resp.Body)
}

func doCommandAndCopy(client *http.Client, req *http.Request, stdout io.Writer) error {
	client = cliHTTPClient(client)
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer drainAndCloseResponse(resp.Body)

	contentType := resp.Header.Get("Content-Type")
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, err := readErrorBody(resp.Body)
		if err != nil {
			return err
		}
		message := strings.TrimSpace(string(body))
		if decoded, decodeErr := hatriecache.DecodeCommandResponseWire(bytes.NewReader(body), contentType, maxErrorBodyBytes); decodeErr == nil {
			if data, marshalErr := jsonwire.Marshal(decoded); marshalErr == nil {
				message = string(data)
			}
		}
		return &commandHTTPError{status: resp.Status, statusCode: resp.StatusCode, message: message}
	}
	response, err := hatriecache.DecodeCommandResponseWire(resp.Body, contentType, maxErrorBodyBytes)
	if err != nil {
		if errors.Is(err, hatriecache.ErrUnsupportedCommandResponseContentType) {
			return copyAndEnsureTrailingNewline(stdout, resp.Body)
		}
		return err
	}
	data, err := jsonwire.Marshal(response)
	if err != nil {
		return err
	}
	if _, err := stdout.Write(data); err != nil {
		return err
	}
	_, err = stdout.Write([]byte{'\n'})
	return err
}

type commandHTTPError struct {
	status     string
	statusCode int
	message    string
}

func (err *commandHTTPError) Error() string {
	return fmt.Sprintf("server returned %s: %s", err.status, err.message)
}

func endpoint(addr string, path string) string {
	return strings.TrimRight(addr, "/") + path
}

func readErrorBody(body io.Reader) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(body, maxErrorBodyBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) <= maxErrorBodyBytes {
		return data, nil
	}
	data = data[:maxErrorBodyBytes]
	data = append(data, truncatedErrorBodySuffix...)
	return data, nil
}

func drainAndCloseResponse(body io.ReadCloser) {
	if body == nil {
		return
	}
	_, _ = io.CopyN(io.Discard, body, maxResponseDrainBytes)
	_ = body.Close()
}

type trailingNewlineWriter struct {
	writer io.Writer
	wrote  bool
	last   byte
}

func (writer *trailingNewlineWriter) Write(data []byte) (int, error) {
	n, err := writer.writer.Write(data)
	if n > 0 {
		writer.wrote = true
		writer.last = data[n-1]
	}
	return n, err
}

func copyAndEnsureTrailingNewline(stdout io.Writer, body io.Reader) error {
	writer := &trailingNewlineWriter{writer: stdout}
	if _, err := io.Copy(writer, body); err != nil {
		return err
	}
	if writer.wrote && writer.last != '\n' {
		_, err := stdout.Write([]byte{'\n'})
		return err
	}
	return nil
}
