#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatStorage/compaction_scheduler.go ./hat/hatStorage/compaction_scheduler_fastpath_test.go -run '^TestCompactionSchedulerSingleTaskCanQueueFollowup$' -count=1
