#!/bin/sh
set -eu

gofmt -w \
	hat/hatCache/grpc.go \
	hat/hatCache/grpc_command_stream.go \
	hat/hatCache/grpc_command_stream_multiplex_test.go \
	hat/hatCache/grpc_command_stream_multiplex_benchmark_test.go \
	hat/hatCache/command_transport_benchmark_test.go
