package hatCommand

import (
	"testing"

	"google.golang.org/protobuf/proto"

	hatriecachev1 "hatrie_cache/internal/gen/hatriecache/v1"
)

func TestCASExpectedValueRoundTripsThroughProtobuf(t *testing.T) {
	want := Request{
		Command:       "CAS",
		Key:           "cas:key",
		ExpectedValue: "old",
		Value:         "new",
	}

	message, err := RequestToProto(want)
	if err != nil {
		t.Fatalf("RequestToProto() error = %v", err)
	}
	if message.GetExpectedValue() != want.ExpectedValue {
		t.Fatalf("protobuf expected value = %q, want %q", message.GetExpectedValue(), want.ExpectedValue)
	}
	wire, err := proto.Marshal(message)
	if err != nil {
		t.Fatalf("marshal CAS request: %v", err)
	}
	decoded := &hatriecachev1.CommandRequest{}
	if err := proto.Unmarshal(wire, decoded); err != nil {
		t.Fatalf("unmarshal CAS request: %v", err)
	}
	got := cacheCommandRequestFromProto(decoded)
	if got.ExpectedValue != want.ExpectedValue {
		t.Fatalf("decoded expected value = %q, want %q", got.ExpectedValue, want.ExpectedValue)
	}
}
