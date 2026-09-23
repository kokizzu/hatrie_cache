#!/usr/bin/env bash
set -euo pipefail

git add \
	Makefile \
	README.md \
	BENCHMARK.md \
	INSPIRATION_ROUND2.md \
	T241_ROLE_BASED_AUTHORIZATION.md \
	hat/hatPeer/request_authorization.go \
	hat/hatPeer/compact_session.go \
	hat/hatPeer/t241_authorization_baseline_benchmark_test.go \
	hat/hatPeer/t241_request_authorization_test.go \
	scripts/benchmark-t241-before.sh \
	scripts/benchmark-t241.sh \
	scripts/format-t241.sh \
	scripts/race-t241.sh \
	scripts/test-t241.sh \
	scripts/vet-t241.sh \
	scripts/stage-t241.sh \
	scripts/commit-t241.sh \
	scripts/push-t241.sh

git diff --cached --check
