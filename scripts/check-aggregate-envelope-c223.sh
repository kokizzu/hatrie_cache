#!/usr/bin/env bash
set -euo pipefail

git diff --check -- \
	Makefile \
	INSPIRATION_ROUND2.md \
	PRODUCT_IDEA_GAPS.md \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	BENCHMARK.md \
	hat/hatDataStructure/partial_aggregate_envelope.go \
	hat/hatDataStructure/partial_aggregate_state.go \
	hat/hatDataStructure/partial_aggregate_envelope_test.go \
	hat/hatDataStructure/partial_aggregate_envelope_benchmark_test.go \
	hat/hatCache/partial_aggregate_state.go \
	hat/hatCache/partial_aggregate_envelope_test.go \
	hat/hatCache/partial_aggregate_envelope_benchmark_test.go \
	scripts/run-aggregate-envelope-c223.sh \
	scripts/format-aggregate-envelope-c223.sh \
	scripts/check-aggregate-envelope-c223.sh \
	scripts/stage-aggregate-envelope-c223.sh \
	scripts/commit-aggregate-envelope-c223.sh \
	scripts/push-aggregate-envelope-c223.sh
git diff --cached --check -- \
	Makefile \
	INSPIRATION_ROUND2.md \
	PRODUCT_IDEA_GAPS.md \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	BENCHMARK.md \
	hat/hatDataStructure/partial_aggregate_envelope.go \
	hat/hatDataStructure/partial_aggregate_state.go \
	hat/hatDataStructure/partial_aggregate_envelope_test.go \
	hat/hatDataStructure/partial_aggregate_envelope_benchmark_test.go \
	hat/hatCache/partial_aggregate_state.go \
	hat/hatCache/partial_aggregate_envelope_test.go \
	hat/hatCache/partial_aggregate_envelope_benchmark_test.go \
	scripts/run-aggregate-envelope-c223.sh \
	scripts/format-aggregate-envelope-c223.sh \
	scripts/check-aggregate-envelope-c223.sh \
	scripts/stage-aggregate-envelope-c223.sh \
	scripts/commit-aggregate-envelope-c223.sh \
	scripts/push-aggregate-envelope-c223.sh
printf '%s\n' 'check-aggregate-envelope-c223: ok'
