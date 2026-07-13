package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"

	hatriecache "hatrie_cache"
)

type clientConfig struct {
	addr string
}

func main() {
	if err := run(context.Background(), os.Args[1:], os.Stdout, os.Stderr, http.DefaultClient); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, stdout io.Writer, stderr io.Writer, client *http.Client) error {
	cfg, remaining, err := parseGlobalFlags(args, stderr)
	if err != nil {
		return err
	}
	if len(remaining) == 0 {
		return errors.New("subcommand is required: health, stats, entries, topology, command, snapshot")
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
	case "command":
		return runCommand(ctx, client, cfg.addr, remaining[1:], stdout, stderr)
	case "snapshot":
		return postJSON(ctx, client, cfg.addr, "/api/snapshot", []byte("{}"), stdout)
	default:
		return fmt.Errorf("unknown subcommand %q", remaining[0])
	}
}

func parseGlobalFlags(args []string, output io.Writer) (clientConfig, []string, error) {
	cfg := clientConfig{addr: "http://127.0.0.1:8080"}
	flags := flag.NewFlagSet("hatrie-cli", flag.ContinueOnError)
	flags.SetOutput(output)
	flags.StringVar(&cfg.addr, "addr", cfg.addr, "monitoring server base URL")
	if err := flags.Parse(args); err != nil {
		return clientConfig{}, nil, err
	}
	return cfg, flags.Args(), nil
}

func runEntries(ctx context.Context, client *http.Client, addr string, args []string, stdout io.Writer, stderr io.Writer) error {
	flags := flag.NewFlagSet("entries", flag.ContinueOnError)
	flags.SetOutput(stderr)
	prefix := flags.String("prefix", "", "key prefix filter")
	if err := flags.Parse(args); err != nil {
		return err
	}

	path := "/api/entries"
	if *prefix != "" {
		path += "?prefix=" + url.QueryEscape(*prefix)
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
		data, err := os.ReadFile(*filePath)
		if err != nil {
			return err
		}
		return putJSON(ctx, client, addr, "/api/topology", data, stdout)
	}
	path := "/api/topology"
	if *key != "" {
		path += "?key=" + url.QueryEscape(*key)
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
	subkey := flags.String("subkey", "", "map subkey")
	pairsJSON := flags.String("pairs", "", "JSON object for map fields")
	ttlSeconds := flags.Int64("ttl-seconds", -1, "optional ttl in seconds")
	unixSeconds := flags.Int64("unix-seconds", -1, "optional absolute expiration as Unix seconds")
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
	body, err := json.Marshal(request)
	if err != nil {
		return err
	}
	return postJSON(ctx, client, addr, "/api/commands", body, stdout)
}

func decodeJSONFlag[T any](value string) (T, error) {
	var out T
	decoder := json.NewDecoder(strings.NewReader(value))
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
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint(addr, path), nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	return doAndCopy(client, req, stdout)
}

func postJSON(ctx context.Context, client *http.Client, addr string, path string, body []byte, stdout io.Writer) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint(addr, path), bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	return doAndCopy(client, req, stdout)
}

func putJSON(ctx context.Context, client *http.Client, addr string, path string, body []byte, stdout io.Writer) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, endpoint(addr, path), bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	return doAndCopy(client, req, stdout)
}

func doAndCopy(client *http.Client, req *http.Request, stdout io.Writer) error {
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("server returned %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}
	_, err = stdout.Write(ensureTrailingNewline(body))
	return err
}

func endpoint(addr string, path string) string {
	return strings.TrimRight(addr, "/") + path
}

func ensureTrailingNewline(value []byte) []byte {
	if len(value) == 0 || value[len(value)-1] == '\n' {
		return value
	}
	out := make([]byte, 0, len(value)+1)
	out = append(out, value...)
	out = append(out, '\n')
	return out
}
