package hatCommand

import (
	"bytes"
	"io"
	"testing"

	"google.golang.org/protobuf/proto"
)

func TestWirePublicAPIProtobufPreservesAtomicBatch(t *testing.T) {
	want := Request{
		Command: "BATCH",
		Atomic:  true,
		Batch: []Request{
			{Command: "SETSTR", Key: "first", Value: "one"},
			{Command: "SETSTR", Key: "second", Value: "two"},
		},
	}
	message, err := RequestToProto(want)
	if err != nil {
		t.Fatalf("RequestToProto() error = %v", err)
	}
	wireData, err := proto.Marshal(message)
	if err != nil {
		t.Fatalf("proto.Marshal() error = %v", err)
	}
	got, err := DecodeRequestProtobuf(bytes.NewReader(wireData), int64(len(wireData)))
	if err != nil {
		t.Fatalf("DecodeRequestProtobuf() error = %v", err)
	}
	if !got.Atomic || got.Command != want.Command || len(got.Batch) != len(want.Batch) {
		t.Fatalf("decoded request = %#v, want atomic batch %#v", got, want)
	}
}

func TestWirePublicAPIJSONRoundTrip(t *testing.T) {
	body, contentType, contentEncoding, err := CommandRequestBody(Request{
		Command: "SETSTR",
		Key:     "session:1",
		Value:   "value",
	}, CommandWireFormatJSON, 0, 0)
	if err != nil {
		t.Fatalf("CommandRequestBody() error = %v", err)
	}
	if closer, ok := body.(io.Closer); ok {
		defer closer.Close()
	}
	data, err := io.ReadAll(body)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if contentType != ContentTypeJSON || contentEncoding != "" || !bytes.Contains(data, []byte(`"command":"SETSTR"`)) {
		t.Fatalf("JSON wire body = %q/%q/%q, want JSON command payload", contentType, contentEncoding, data)
	}

	response, err := DecodeCommandResponseWire(bytes.NewReader([]byte(`{"ok":true,"value":"value"}`)), ContentTypeJSON, 1024)
	if err != nil {
		t.Fatalf("DecodeCommandResponseWire() error = %v", err)
	}
	if !response.OK || response.Value != "value" {
		t.Fatalf("DecodeCommandResponseWire() = %#v, want successful response", response)
	}
}
