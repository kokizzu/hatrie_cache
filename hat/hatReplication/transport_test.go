package hatReplication_test

import (
	"testing"

	"hatrie_cache/hat/hatReplication"
)

func TestParseTransportCanonicalizesDefaultAndStream(t *testing.T) {
	transport, err := hatReplication.ParseTransport("")
	if err != nil || transport != hatReplication.TransportHTTP {
		t.Fatalf("ParseTransport(empty) = %q, %v", transport, err)
	}
	transport, err = hatReplication.ParseTransport("grpc-stream")
	if err != nil || transport != hatReplication.TransportGRPCStream {
		t.Fatalf("ParseTransport(grpc-stream) = %q, %v", transport, err)
	}
}
