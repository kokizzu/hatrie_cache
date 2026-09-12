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

func TestClientCommandUsesAuthenticatedJSONContract(t *testing.T) {
	var received hatCommand.Request
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.URL.Path != "/api/commands" {
			t.Fatalf("request = %s %s, want POST /api/commands", request.Method, request.URL.Path)
		}
		if request.Header.Get("Authorization") != "Bearer secret" {
			t.Fatalf("authorization = %q, want bearer token", request.Header.Get("Authorization"))
		}
		if request.Header.Get("Content-Type") != "application/json" {
			t.Fatalf("content type = %q, want application/json", request.Header.Get("Content-Type"))
		}
		if err := json.NewDecoder(request.Body).Decode(&received); err != nil {
			t.Fatalf("decode command request: %v", err)
		}
		writer.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(writer).Encode(hatCommand.Response{OK: true, Message: "stored"}); err != nil {
			t.Fatalf("encode command response: %v", err)
		}
	}))
	defer server.Close()

	client := hatMonitoring.NewClient(server.URL, "secret")
	client.HTTP = server.Client()
	response, err := client.Command(context.Background(), hatCommand.Request{
		Command: "SETSTR",
		Key:     "name",
		Value:   "ivi",
	})
	if err != nil {
		t.Fatalf("Command() error = %v", err)
	}
	if !response.OK || response.Message != "stored" {
		t.Fatalf("Command() response = %#v, want successful stored response", response)
	}
	if received.Command != "SETSTR" || received.Key != "name" || received.Value != "ivi" {
		t.Fatalf("received request = %#v, want SETSTR name ivi", received)
	}
}

func TestClientCommandReportsHTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		http.Error(writer, "command rejected", http.StatusConflict)
	}))
	defer server.Close()

	client := hatMonitoring.NewClient(server.URL, "")
	client.HTTP = server.Client()
	if _, err := client.Command(context.Background(), hatCommand.Request{Command: "SETSTR", Key: "name", Value: "ivi"}); err == nil {
		t.Fatal("Command() error = nil, want HTTP error")
	}
}

func TestClientBatchUsesExistingBatchCommandContract(t *testing.T) {
	var received hatCommand.Request
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.URL.Path != "/api/commands" {
			t.Fatalf("request = %s %s, want POST /api/commands", request.Method, request.URL.Path)
		}
		if err := json.NewDecoder(request.Body).Decode(&received); err != nil {
			t.Fatalf("decode batch request: %v", err)
		}
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(hatCommand.Response{OK: true, Message: "batch stored"})
	}))
	defer server.Close()

	client := hatMonitoring.NewClient(server.URL, "secret")
	client.HTTP = server.Client()
	response, err := client.Batch(context.Background(), []hatCommand.Request{
		{Command: "SETSTR", Key: "one", Value: "1"},
		{Command: "SETSTR", Key: "two", Value: "2"},
	}, true)
	if err != nil {
		t.Fatalf("Batch() error = %v", err)
	}
	if !response.OK || response.Message != "batch stored" {
		t.Fatalf("Batch() response = %#v, want successful batch response", response)
	}
	if received.Command != "BATCH" || !received.Atomic || len(received.Batch) != 2 {
		t.Fatalf("received batch request = %#v, want atomic batch of two commands", received)
	}
	if received.Batch[0].Key != "one" || received.Batch[1].Key != "two" {
		t.Fatalf("received batch entries = %#v, want one and two", received.Batch)
	}
}

func TestClientCommandWithFormatUsesProtobufWireContract(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer secret" {
			t.Fatalf("authorization = %q, want bearer token", request.Header.Get("Authorization"))
		}
		if request.Header.Get("Content-Type") != hatCommand.ContentTypeProtobuf || request.Header.Get("Accept") != hatCommand.ContentTypeProtobuf {
			t.Fatalf("wire headers = %q/%q, want protobuf", request.Header.Get("Content-Type"), request.Header.Get("Accept"))
		}
		payload, err := io.ReadAll(request.Body)
		if err != nil {
			t.Fatalf("read protobuf request: %v", err)
		}
		decoded, err := hatCommand.DecodeRequestProtobuf(bytes.NewReader(payload), 1<<20)
		if err != nil {
			t.Fatalf("DecodeRequestProtobuf() error = %v", err)
		}
		if decoded.Command != "SETSTR" || decoded.Key != "name" || decoded.Value != "ivi" {
			t.Fatalf("decoded request = %#v, want SETSTR name ivi", decoded)
		}
		responsePayload, err := proto.Marshal(&hatGrpc.CommandResponse{Ok: true, Message: "binary"})
		if err != nil {
			t.Fatalf("marshal protobuf response: %v", err)
		}
		writer.Header().Set("Content-Type", hatCommand.ContentTypeProtobuf)
		_, _ = writer.Write(responsePayload)
	}))
	defer server.Close()

	client := hatMonitoring.NewClient(server.URL, "secret")
	client.HTTP = server.Client()
	response, err := client.CommandWithFormat(context.Background(), hatCommand.Request{
		Command: "SETSTR",
		Key:     "name",
		Value:   "ivi",
	}, hatCommand.CommandWireFormatProtobuf)
	if err != nil {
		t.Fatalf("CommandWithFormat() error = %v", err)
	}
	if !response.OK || response.Message != "binary" {
		t.Fatalf("CommandWithFormat() response = %#v, want binary response", response)
	}
}

func TestClientBatchWithFormatUsesProtobufWireContract(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if got := request.Header.Get("Authorization"); got != "Bearer secret" {
			t.Fatalf("authorization = %q, want Bearer secret", got)
		}
		if got := request.Header.Get("Content-Type"); got != hatCommand.ContentTypeProtobuf {
			t.Fatalf("content type = %q, want %q", got, hatCommand.ContentTypeProtobuf)
		}
		if got := request.Header.Get("Accept"); got != hatCommand.ContentTypeProtobuf {
			t.Fatalf("accept = %q, want %q", got, hatCommand.ContentTypeProtobuf)
		}
		decoded, err := hatCommand.DecodeRequestProtobuf(request.Body, 1<<20)
		if err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if decoded.Command != "BATCH" {
			t.Fatalf("command = %q, want BATCH", decoded.Command)
		}
		if !decoded.Atomic {
			t.Fatal("atomic = false, want true")
		}
		if len(decoded.Batch) != 2 || decoded.Batch[0].Key != "first" || decoded.Batch[1].Value != "two" {
			t.Fatalf("batch = %#v, want two decoded commands", decoded.Batch)
		}
		payload, err := proto.Marshal(&hatGrpc.CommandResponse{Ok: true, Message: "batch-binary"})
		if err != nil {
			t.Fatalf("marshal response: %v", err)
		}
		writer.Header().Set("Content-Type", hatCommand.ContentTypeProtobuf)
		_, _ = writer.Write(payload)
	}))
	defer server.Close()

	client := hatMonitoring.NewClient(server.URL, "secret")
	client.HTTP = server.Client()
	response, err := client.BatchWithFormat(
		context.Background(),
		[]hatCommand.Request{
			{Command: "SETSTR", Key: "first", Value: "one"},
			{Command: "SETSTR", Key: "second", Value: "two"},
		},
		true,
		hatCommand.CommandWireFormatProtobuf,
	)
	if err != nil {
		t.Fatalf("BatchWithFormat() error = %v", err)
	}
	if !response.OK || response.Message != "batch-binary" {
		t.Fatalf("BatchWithFormat() response = %#v, want batch-binary response", response)
	}
}

func TestClientCommandUsesConfiguredProtobufWireFormat(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Content-Type") != hatCommand.ContentTypeProtobuf {
			t.Fatalf("content type = %q, want %q", request.Header.Get("Content-Type"), hatCommand.ContentTypeProtobuf)
		}
		if request.Header.Get("Accept") != hatCommand.ContentTypeProtobuf {
			t.Fatalf("accept = %q, want %q", request.Header.Get("Accept"), hatCommand.ContentTypeProtobuf)
		}
		if _, err := hatCommand.DecodeRequestProtobuf(request.Body, 1<<20); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		payload, err := proto.Marshal(&hatGrpc.CommandResponse{Ok: true, Message: "configured"})
		if err != nil {
			t.Fatalf("marshal response: %v", err)
		}
		writer.Header().Set("Content-Type", hatCommand.ContentTypeProtobuf)
		_, _ = writer.Write(payload)
	}))
	defer server.Close()

	client := hatMonitoring.NewClient(server.URL, "secret")
	client.HTTP = server.Client()
	client.CommandWireFormat = hatCommand.CommandWireFormatProtobuf
	response, err := client.Command(context.Background(), hatCommand.Request{Command: "SETSTR", Key: "name", Value: "ivi"})
	if err != nil {
		t.Fatalf("Command() error = %v", err)
	}
	if !response.OK || response.Message != "configured" {
		t.Fatalf("Command() response = %#v, want configured response", response)
	}
}
