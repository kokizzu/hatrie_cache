#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatStorage/compaction_scheduler.go \
	hat/hatStorage/compaction_scheduler_fastpath_test.go \
	hat/hatStorage/compaction_scheduler_fastpath_benchmark_test.go
