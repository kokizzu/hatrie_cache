package hatCommand

import (
	"testing"

	json "github.com/goccy/go-json"
	"google.golang.org/protobuf/proto"

	hatriecachev1 "hatrie_cache/internal/gen/hatriecache/v1"
)

func TestRequestInsertQuorumRoundTripThroughProtobuf(t *testing.T) {
	want := Request{
		Command:      "SETSTR",
		Key:          "quorum:wire",
		Value:        "value",
		InsertQuorum: 3,
	}
	message, err := RequestToProto(want)
	if err != nil {
		t.Fatalf("RequestToProto() error = %v", err)
	}
	if got := message.GetInsertQuorum(); got != 3 {
		t.Fatalf("protobuf insert quorum = %d, want 3", got)
	}
	data, err := proto.Marshal(message)
	if err != nil {
		t.Fatalf("proto.Marshal() error = %v", err)
	}
	var decoded hatriecachev1.CommandRequest
	if err := proto.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("proto.Unmarshal() error = %v", err)
	}
	got := cacheCommandRequestFromProto(&decoded)
	if got.InsertQuorum != want.InsertQuorum {
		t.Fatalf("decoded insert quorum = %d, want %d", got.InsertQuorum, want.InsertQuorum)
	}
}

func TestRequestInsertQuorumJSONRoundTrip(t *testing.T) {
	data := []byte(`{"command":"SETSTR","key":"quorum:json","value":"value","insert_quorum":4}`)
	var got Request
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if got.InsertQuorum != 4 {
		t.Fatalf("decoded JSON insert quorum = %d, want 4", got.InsertQuorum)
	}
	encoded, err := json.Marshal(got)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	if string(encoded) != string(data) {
		t.Fatalf("encoded JSON = %s, want %s", encoded, data)
	}
}
