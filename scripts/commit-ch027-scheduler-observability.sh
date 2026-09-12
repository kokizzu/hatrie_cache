#!/bin/sh
set -eu

git add \
	BENCHMARK.md \
	CH027_COMPACTION_SCHEDULER_OBSERVABILITY.md \
	ENGINE_IDEAS.md \
	Makefile \
	README.md \
	hat/hatStorage/ch027_scheduler_observability_baseline_test.go \
	hat/hatStorage/ch027_scheduler_observability_test.go \
	hat/hatStorage/compaction_scheduler.go \
	hat/hatStorage/compaction_scheduler_stats.go \
	scripts/benchmark-ch027-scheduler-observability.sh \
	scripts/commit-ch027-scheduler-observability.sh \
	scripts/format-ch027-scheduler-observability.sh \
	scripts/push-ch027-scheduler-observability.sh \
	scripts/race-ch027-scheduler-observability.sh \
	scripts/review-ch027-scheduler-observability.sh \
	scripts/test-ch027-scheduler-observability.sh \
	scripts/verify-ch027-docs.sh \
	scripts/vet-ch027-scheduler-observability.sh
git diff --cached --check
git commit -m "feat: add compaction scheduler age telemetry"
