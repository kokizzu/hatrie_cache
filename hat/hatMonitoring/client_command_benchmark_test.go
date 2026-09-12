package hatMonitoring_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"google.golang.org/protobuf/proto"
	"hatrie_cache/hat/hatCommand"
	"hatrie_cache/hat/hatGrpc"
	"hatrie_cache/hat/hatMonitoring"
)

func newCommandBenchmarkServer() *httptest.Server {
	protobufResponse, err := proto.Marshal(&hatGrpc.CommandResponse{Ok: true, Message: "stored"})
	if err != nil {
		panic(err)
	}
	return httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		_, _ = io.Copy(io.Discard, request.Body)
		if request.Header.Get("Accept") == hatCommand.ContentTypeProtobuf {
			writer.Header().Set("Content-Type", hatCommand.ContentTypeProtobuf)
			_, _ = writer.Write(protobufResponse)
			return
		}
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
	wireBody, err := json.Marshal(command)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	b.ReportMetric(float64(len(wireBody)), "wire-B/op")
	for index := 0; index < b.N; index++ {
		if _, err := client.Command(context.Background(), command); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkClientCommandProtobuf(b *testing.B) {
	server := newCommandBenchmarkServer()
	defer server.Close()
	client := hatMonitoring.NewClient(server.URL, "secret")
	client.HTTP = server.Client()
	command := hatCommand.Request{Command: "SETSTR", Key: "benchmark", Value: "value"}
	wireBytes := benchmarkCommandWireBytesFormat(command, hatCommand.CommandWireFormatProtobuf)

	b.ReportAllocs()
	b.ResetTimer()
	b.ReportMetric(float64(wireBytes), "wire-B/op")
	for index := 0; index < b.N; index++ {
		if _, err := client.CommandWithFormat(context.Background(), command, hatCommand.CommandWireFormatProtobuf); err != nil {
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

func benchmarkCommands(count int) []hatCommand.Request {
	commands := make([]hatCommand.Request, count)
	for index := range commands {
		commands[index] = hatCommand.Request{
			Command: "SETSTR",
			Key:     "batch-" + string(rune('a'+index)),
			Value:   "value",
		}
	}
	return commands
}

func benchmarkCommandWireBytes(commands []hatCommand.Request) int {
	total := 0
	for _, command := range commands {
		body, _ := json.Marshal(command)
		total += len(body)
	}
	return total
}

func benchmarkCommandWireBytesFormat(request hatCommand.Request, format hatCommand.CommandWireFormat) int {
	body, _, _, err := hatCommand.CommandRequestBody(request, format, 0, 0)
	if err != nil {
		panic(err)
	}
	if closer, ok := body.(io.Closer); ok {
		defer closer.Close()
	}
	payload, err := io.ReadAll(body)
	if err != nil {
		panic(err)
	}
	return len(payload)
}

func BenchmarkClientBatchJSON10(b *testing.B) {
	server := newCommandBenchmarkServer()
	defer server.Close()
	client := hatMonitoring.NewClient(server.URL, "secret")
	client.HTTP = server.Client()
	commands := benchmarkCommands(10)
	batchBody, err := json.Marshal(hatCommand.Request{Command: "BATCH", Batch: commands})
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	b.ReportMetric(float64(len(batchBody)), "wire-B/op")
	for index := 0; index < b.N; index++ {
		if _, err := client.Batch(context.Background(), commands, false); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkClientBatchProtobuf10(b *testing.B) {
	server := newCommandBenchmarkServer()
	defer server.Close()
	client := hatMonitoring.NewClient(server.URL, "secret")
	client.HTTP = server.Client()
	commands := benchmarkCommands(10)
	wireBytes := benchmarkCommandWireBytesFormat(
		hatCommand.Request{Command: "BATCH", Batch: commands},
		hatCommand.CommandWireFormatProtobuf,
	)

	b.ReportAllocs()
	b.ResetTimer()
	b.ReportMetric(float64(wireBytes), "wire-B/op")
	for index := 0; index < b.N; index++ {
		if _, err := client.BatchWithFormat(context.Background(), commands, false, hatCommand.CommandWireFormatProtobuf); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkClientCommandJSON10(b *testing.B) {
	server := newCommandBenchmarkServer()
	defer server.Close()
	client := hatMonitoring.NewClient(server.URL, "secret")
	client.HTTP = server.Client()
	commands := benchmarkCommands(10)

	b.ReportAllocs()
	b.ResetTimer()
	b.ReportMetric(float64(benchmarkCommandWireBytes(commands)), "wire-B/op")
	for index := 0; index < b.N; index++ {
		for _, command := range commands {
			if _, err := client.Command(context.Background(), command); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkClientCommandProtobuf10(b *testing.B) {
	server := newCommandBenchmarkServer()
	defer server.Close()
	commands := benchmarkCommands(10)
	client := hatMonitoring.NewClient(server.URL, "secret")
	client.HTTP = server.Client()

	b.ReportAllocs()
	b.ResetTimer()
	b.ReportMetric(float64(benchmarkCommandWireBytesFormat(commands[0], hatCommand.CommandWireFormatProtobuf)*len(commands)), "wire-B/op")
	for index := 0; index < b.N; index++ {
		for _, command := range commands {
			if _, err := client.CommandWithFormat(context.Background(), command, hatCommand.CommandWireFormatProtobuf); err != nil {
				b.Fatal(err)
			}
		}
	}
}
