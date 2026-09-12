#!/bin/sh
set -eu

git add -- BENCHMARK.md CLIENT_SDK.md INSPIRATION.md README.md Makefile \
	hat/hatCommand/wire.go hat/hatCommand/wire_test.go \
	hat/hatMonitoring/client.go hat/hatMonitoring/client_command_benchmark_test.go \
	hat/hatMonitoring/client_command_test.go internal/gen/hatriecache/v1/cache.pb.go \
	proto/hatriecache/v1/cache.proto scripts/benchmark-monitoring-command-client.sh \
	scripts/review-monitoring-command-wire.sh scripts/commit-monitoring-command-wire.sh \
	scripts/push-monitoring-command-wire.sh
git diff --cached --check
git commit -m 'monitoring: add configurable protobuf command wire'
