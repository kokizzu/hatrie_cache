#!/bin/sh
set -eu

gofmt -w \
	hat/hatStorage/compaction_scheduler.go \
	hat/hatStorage/compaction_scheduler_stats.go \
	hat/hatStorage/ch027_scheduler_observability_baseline_test.go \
	hat/hatStorage/ch027_scheduler_observability_test.go
