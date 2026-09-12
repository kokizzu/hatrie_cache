#!/bin/sh
set -eu

git add \
	BENCHMARK.md \
	ENGINE_IDEAS.md \
	Makefile \
	README.md \
	TT032_IPROTO_MULTIPLEXING.md \
	hat/hatCache/command_transport_benchmark_test.go \
	hat/hatCache/grpc.go \
	hat/hatCache/grpc_command_stream.go \
	hat/hatCache/grpc_command_stream_multiplex_benchmark_test.go \
	hat/hatCache/grpc_command_stream_multiplex_test.go \
	internal/gen/hatriecache/v1/cache.pb.go \
	proto/hatriecache/v1/cache.proto \
	scripts/benchmark-tt032-multiplexing.sh \
	scripts/commit-tt032-multiplexing.sh \
	scripts/format-tt032-multiplexing.sh \
	scripts/push-tt032-multiplexing.sh \
	scripts/review-tt032-multiplexing.sh \
	scripts/test-race-tt032-multiplexing.sh \
	scripts/test-tt032-multiplexing.sh \
	scripts/test-tt032-package.sh \
	scripts/verify-tt032-docs.sh \
	scripts/vet-tt032-multiplexing.sh
git diff --cached --check
git commit -m "feat(grpc): add opt-in command stream multiplexing"
