package hatReplication

import (
	"fmt"
	"strings"
)

// Transport selects the replication transport implementation.
type Transport string

const (
	TransportHTTP       Transport = "http"
	TransportGRPCStream Transport = "grpc-stream"
)

// ParseTransport validates one supported replication transport. Empty input
// retains the HTTP default.
func ParseTransport(value string) (Transport, error) {
	switch Transport(strings.ToLower(strings.TrimSpace(value))) {
	case "", TransportHTTP:
		return TransportHTTP, nil
	case TransportGRPCStream:
		return TransportGRPCStream, nil
	default:
		return "", fmt.Errorf("hatriecache: replication transport must be http or grpc-stream")
	}
}
