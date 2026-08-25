package hatCommand

import (
	"bytes"
	"io"
	"testing"
)

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
