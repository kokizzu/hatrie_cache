#!/usr/bin/env bash
set -euo pipefail

git add -- \
	Makefile \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	BENCHMARK.md \
	PRODUCT_IDEA_GAPS.md \
	README.md \
	TU38_CONFLICT_EVENT_LOG.md \
	hat/hatReplication/conflict_event_log.go \
	hat/hatReplication/conflict_event_log_test.go \
	hat/hatReplication/conflict_event_log_benchmark_test.go \
	scripts/format-chg15-conflict-events.sh \
	scripts/test-chg15-conflict-events.sh \
	scripts/benchmark-chg15-conflict-events.sh \
	scripts/test-chg15-conflict-events-package.sh \
	scripts/race-chg15-conflict-events.sh \
	scripts/vet-chg15-conflict-events.sh \
	scripts/status-chg15-conflict-events.sh \
	scripts/stage-chg15-conflict-events.sh \
	scripts/commit-chg15-conflict-events.sh \
	scripts/push-chg15-conflict-events.sh
