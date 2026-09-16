#!/usr/bin/env bash
set -euo pipefail

git diff --check
git status --short -- \
	hat/hatDataStructure/priority_visibility_queue.go \
	hat/hatDataStructure/priority_visibility_queue_test.go \
	PRIORITY_VISIBILITY_QUEUE.md \
	VISIBILITY_QUEUE.md \
	DATA_STRUCTURE.md \
	ENGINE_IDEAS.md \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	README.md \
	BENCHMARK.md \
	scripts/benchmark-tt048-c296.sh \
	scripts/format-tt048-c296.sh \
	scripts/test-race-tt048-c296.sh \
	scripts/vet-tt048-c296.sh \
	scripts/review-tt048-c300.sh \
	scripts/stage-tt048-c300.sh \
	scripts/inspect-staged-tt048-c300.sh \
	scripts/commit-tt048-c300.sh \
	scripts/push-tt048-c300.sh \
	Makefile
git diff --stat -- \
	hat/hatDataStructure/priority_visibility_queue.go \
	hat/hatDataStructure/priority_visibility_queue_test.go \
	PRIORITY_VISIBILITY_QUEUE.md \
	VISIBILITY_QUEUE.md \
	DATA_STRUCTURE.md \
	ENGINE_IDEAS.md \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	README.md \
	BENCHMARK.md \
	scripts/benchmark-tt048-c296.sh \
	scripts/format-tt048-c296.sh \
	scripts/test-race-tt048-c296.sh \
	scripts/vet-tt048-c296.sh \
	scripts/review-tt048-c300.sh \
	scripts/stage-tt048-c300.sh \
	scripts/inspect-staged-tt048-c300.sh \
	scripts/commit-tt048-c300.sh \
	scripts/push-tt048-c300.sh \
	Makefile
