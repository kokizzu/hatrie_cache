package hatMonitoring_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"hatrie_cache/hat/hatCommand"
	"hatrie_cache/hat/hatMonitoring"
)

func newCommandBenchmarkServer() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		_, _ = io.Copy(io.Discard, request.Body)
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"ok":true,"message":"stored"}`))
	}))
}

func BenchmarkClientCommandJSON(b *testing.B) {
	server := newCommandBenchmarkServer()
	defer server.Close()
	client := hatMonitoring.NewClient(server.URL, "secret")
	client.HTTP = server.Client()
	command := hatCommand.Request{Command: "SETSTR", Key: "benchmark", Value: "value"}

	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := client.Command(context.Background(), command); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkManualCommandJSON(b *testing.B) {
	server := newCommandBenchmarkServer()
	defer server.Close()
	httpClient := server.Client()
	endpoint := server.URL + "/api/commands"
	command := hatCommand.Request{Command: "SETSTR", Key: "benchmark", Value: "value"}

	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		body, err := json.Marshal(command)
		if err != nil {
			b.Fatal(err)
		}
		request, err := http.NewRequestWithContext(context.Background(), http.MethodPost, endpoint, bytes.NewReader(body))
		if err != nil {
			b.Fatal(err)
		}
		request.Header.Set("Authorization", "Bearer secret")
		request.Header.Set("Content-Type", "application/json")
		response, err := httpClient.Do(request)
		if err != nil {
			b.Fatal(err)
		}
		if err := json.NewDecoder(response.Body).Decode(new(hatCommand.Response)); err != nil {
			b.Fatal(err)
		}
		_ = response.Body.Close()
	}
}
