#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' 'Pipeline files:'
rg --files hat/hatPipeline | rg 'frontier|compaction|retention|scheduler'
printf '%s\n' '' 'scheduler.go:'
sed -n '1,280p' hat/hatPipeline/scheduler.go
printf '%s\n' '' 'frontier_registry.go:'
sed -n '1,280p' hat/hatPipeline/frontier_registry.go
printf '%s\n' '' 'frontier_retention.go:'
sed -n '1,380p' hat/hatPipeline/frontier_retention.go
printf '%s\n' '' 'scheduler_test.go:'
sed -n '1,280p' hat/hatPipeline/scheduler_test.go
printf '%s\n' '' 'retention test names/usages:'
rg -n 'NewFrontierRetentionRegistry|SafeCompactionBefore|CanCompactBefore|NewScheduler|\.Submit\(' hat/hatPipeline --glob '*_test.go'
printf '%s\n' '' 'inspiration/docs references:'
rg -n -C 4 'MZ-03|MZ-003|frontier-aware|compaction scheduler' INSPIRATION_BACKLOG.md ENGINE_IDEAS.md ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md
