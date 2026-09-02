package hatCommand_test

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"

	"google.golang.org/protobuf/proto"

	"hatrie_cache/hat/hatCommand"
)

func TestRequestIdempotencyKeyRoundTripsAcrossJSONAndProtobuf(t *testing.T) {
	want := hatCommand.Request{
		Command:        "INC",
		Key:            "counter",
		Value:          "1",
		IdempotencyKey: "retry-1",
		BinaryValue:    []byte("payload"),
	}

	jsonData, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	var jsonGot hatCommand.Request
	if err := json.Unmarshal(jsonData, &jsonGot); err != nil {
		t.Fatal(err)
	}
	if jsonGot.IdempotencyKey != want.IdempotencyKey {
		t.Fatalf("JSON idempotency key = %q, want %q", jsonGot.IdempotencyKey, want.IdempotencyKey)
	}
	if len(jsonGot.BinaryValue) != 0 {
		t.Fatal("JSON unexpectedly carried the binary value")
	}

	message, err := hatCommand.RequestToProto(want)
	if err != nil {
		t.Fatal(err)
	}
	wireData, err := proto.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	protobufGot, err := hatCommand.DecodeRequestProtobuf(bytes.NewReader(wireData), int64(len(wireData)))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(protobufGot, want) {
		t.Fatalf("protobuf request = %#v, want %#v", protobufGot, want)
	}
}
