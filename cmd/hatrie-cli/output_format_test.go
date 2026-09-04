package main

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCLIOutputFlagAcceptsPretty(t *testing.T) {
	cfg, remaining, err := parseGlobalFlags([]string{"-output", "pretty", "health"}, io.Discard)
	if err != nil {
		t.Fatalf("parseGlobalFlags() error = %v", err)
	}
	if cfg.output != cliOutputPretty {
		t.Fatalf("output = %q, want %q", cfg.output, cliOutputPretty)
	}
	if len(remaining) != 1 || remaining[0] != "health" {
		t.Fatalf("remaining = %#v, want health", remaining)
	}
}

func TestCLIOutputFlagRejectsUnknownFormat(t *testing.T) {
	if _, _, err := parseGlobalFlags([]string{"-output", "yaml"}, io.Discard); err == nil {
		t.Fatal("parseGlobalFlags() accepted unsupported output format")
	}
}

func TestCLIPrettyWriterFormatsEachJSONLine(t *testing.T) {
	var output bytes.Buffer
	writer := newCLIOutputWriter(&output, cliOutputPretty)
	if _, err := writer.Write([]byte("{\"b\":2,\"a\":1}\n")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	want := "{\n  \"b\": 2,\n  \"a\": 1\n}\n"
	if got := output.String(); got != want {
		t.Fatalf("formatted output = %q, want %q", got, want)
	}
}

func TestRunPrettyFormatsHTTPJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte("{\"b\":2,\"a\":1}"))
	}))
	defer server.Close()

	var output bytes.Buffer
	if err := run(context.Background(), []string{"-addr", server.URL, "-output", "pretty", "health"}, &output, io.Discard, server.Client()); err != nil {
		t.Fatalf("run() error = %v", err)
	}
	want := "{\n  \"b\": 2,\n  \"a\": 1\n}\n"
	if got := output.String(); got != want {
		t.Fatalf("pretty output = %q, want %q", got, want)
	}
}

func TestRunJSONPreservesResponseBytes(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		_, _ = writer.Write([]byte("{\"b\":2,\"a\":1}"))
	}))
	defer server.Close()

	var output bytes.Buffer
	if err := run(context.Background(), []string{"-addr", server.URL, "health"}, &output, io.Discard, server.Client()); err != nil {
		t.Fatalf("run() error = %v", err)
	}
	if got, want := output.String(), "{\"b\":2,\"a\":1}\n"; got != want {
		t.Fatalf("json output = %q, want %q", got, want)
	}
}

func BenchmarkCLIOutputWriter(b *testing.B) {
	payload := []byte("{\"health\":\"online\",\"nodes\":128,\"queue\":{\"depth\":7}}\n")
	b.SetBytes(int64(len(payload)))
	b.Run("json", func(b *testing.B) {
		b.ReportAllocs()
		for index := 0; index < b.N; index++ {
			if _, err := io.Discard.Write(payload); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("pretty", func(b *testing.B) {
		b.ReportAllocs()
		writer := newCLIOutputWriter(io.Discard, cliOutputPretty)
		for index := 0; index < b.N; index++ {
			if _, err := writer.Write(payload); err != nil {
				b.Fatal(err)
			}
		}
	})
}
