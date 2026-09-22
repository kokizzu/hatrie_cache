#!/usr/bin/env bash
set -euo pipefail

git add \
	BENCHMARK.md \
	INSPIRATION_ROUND2.md \
	Makefile \
	T238_BATCHED_BINARY_PROTOCOL.md \
	hat/hatPeer/compact_session.go \
	hat/hatPeer/t238_batch_protocol_benchmark_test.go \
	hat/hatPeer/t238_batch_protocol_test.go \
	scripts/benchmark-t238-batch-protocol.sh \
	scripts/commit-t238-batch-protocol.sh \
	scripts/format-t238-batch-protocol.sh \
	scripts/push-t238-batch-protocol.sh \
	scripts/race-t238-batch-protocol.sh \
	scripts/stage-t238-batch-protocol.sh \
	scripts/test-t238-batch-protocol.sh \
	scripts/vet-t238-batch-protocol.sh
git diff --cached --check
git diff --cached --name-only
