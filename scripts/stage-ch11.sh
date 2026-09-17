#!/bin/sh
set -eu

git add -- \
	BENCHMARK.md \
	CH011_INSERT_QUORUM.md \
	INSPIRATION_BACKLOG.md \
	Makefile \
	README.md \
	hat/hatCache/command.go \
	hat/hatCache/grpc.go \
	hat/hatCache/insert_quorum_benchmark_test.go \
	hat/hatCache/insert_quorum_test.go \
	hat/hatCache/monitoring.go \
	hat/hatCommand/command.go \
	hat/hatCommand/insert_quorum_wire_test.go \
	hat/hatCommand/wire.go \
	internal/gen/hatriecache/v1/cache.pb.go \
	proto/hatriecache/v1/cache.proto \
	scripts/benchmark-ch11.sh \
	scripts/commit-ch11.sh \
	scripts/format-ch11.sh \
	scripts/push-ch11.sh \
	scripts/review-ch11-staged.sh \
	scripts/review-ch11.sh \
	scripts/stage-ch11.sh \
	scripts/test-ch11.sh \
	scripts/verify-ch11.sh
