package hatGrpc

import (
	stdgzip "compress/gzip"

	grpcgzip "google.golang.org/grpc/encoding/gzip"
)

// ConfigureBestSpeedCompression configures gRPC's registered gzip codec for
// low CPU overhead. It is safe to call during process initialization.
func ConfigureBestSpeedCompression() error {
	return grpcgzip.SetLevel(stdgzip.BestSpeed)
}
