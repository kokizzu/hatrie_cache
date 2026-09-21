#!/usr/bin/env bash
set -euo pipefail

git add -- \
	BENCHMARK.md \
	ENGINE_IDEAS.md \
	Makefile \
	README.md \
	TT035_REQUEST_DEADLINES.md \
	cmd/hatrie-cache/main.go \
	cmd/hatrie-cache/tt035_request_deadline_config_test.go \
	hat/hatCache/grpc.go \
	hat/hatCache/grpc_command_stream.go \
	hat/hatCache/monitoring.go \
	hat/hatCache/tt035_request_deadline_test.go \
	hat/hatCommand/request_deadline.go \
	hat/hatCommand/tt035_request_deadline_test.go \
	scripts/benchmark-tt035-request-deadlines.sh \
	scripts/commit-tt035-request-deadlines.sh \
	scripts/format-tt035-request-deadlines.sh \
	scripts/race-tt035-request-deadlines.sh \
	scripts/review-tt035-request-deadlines.sh \
	scripts/stage-tt035-request-deadlines.sh \
	scripts/test-tt035-request-deadlines.sh \
	scripts/push-tt035-request-deadlines.sh
