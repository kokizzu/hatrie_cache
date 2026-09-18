#!/usr/bin/env bash
set -euo pipefail

git add \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	BENCHMARK.md \
	INSPIRATION_BACKLOG.md \
	Makefile \
	README.md \
	TR045_COMPACT_PEER_CIRCUIT_BREAKER.md \
	hat/hatPeer/circuit_breaker.go \
	hat/hatPeer/circuit_breaker_benchmark_test.go \
	hat/hatPeer/circuit_breaker_test.go \
	scripts/benchmark-tr045-peer.sh \
	scripts/format-tr045-peer.sh \
	scripts/race-tr045-peer.sh \
	scripts/commit-tr045-peer.sh \
	scripts/push-tr045-peer.sh \
	scripts/stage-tr045-peer.sh \
	scripts/test-tr045-package.sh \
	scripts/test-tr045-peer.sh \
	scripts/verify-tr045-docs.sh \
	scripts/vet-tr045-peer.sh
git diff --cached --check
git diff --cached --stat
