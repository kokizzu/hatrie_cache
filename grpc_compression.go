package hatriecache

import (
	stdgzip "compress/gzip"

	grpcgzip "google.golang.org/grpc/encoding/gzip"
)

func init() {
	if err := grpcgzip.SetLevel(stdgzip.BestSpeed); err != nil {
		panic(err)
	}
}
