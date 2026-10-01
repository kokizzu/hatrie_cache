#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatStorage/compaction_scheduler.go ./hat/hatStorage/compaction_scheduler_io.go ./hat/hatStorage/compaction_scheduler_stats.go ./hat/hatStorage/compaction_scheduler_fastpath_test.go ./hat/hatStorage/ch_u28_legacy_fastpath_test.go -run '^TestCompactionScheduler|^TestCHU28' -count=1 "$@"
