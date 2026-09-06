#!/bin/sh
set -eu

go test ./hat/hatGrpc \
	-run '^$' \
	-bench '^BenchmarkPublicGRPCClientProtoMarshal$' \
	-benchmem \
	-count=5
