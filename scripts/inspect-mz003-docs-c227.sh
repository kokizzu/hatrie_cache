#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' 'Exact MZ-003/MZ-004 entries:'
rg -n '^### .*MZ-00[234]|^## MZ-00[234]|^<a id="mz-00[234]|^\| MZ-00[234]|^\| MZ-0[234]' INSPIRATION_BACKLOG.md ENGINE_IDEAS.md ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md
printf '%s\n' '' 'Relevant frontier entries:'
rg -n -C 2 'frontier-aware logical compaction|FrontierCompactionScheduler|WaitUntilSafe|FrontierRetentionRegistry|MZ-002' INSPIRATION_BACKLOG.md ENGINE_IDEAS.md ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md
printf '%s\n' '' 'Document tails:'
tail -n 35 BENCHMARK.md
tail -n 12 ADOPTED_QUERY_ENGINE_IDEAS.md
printf '%s\n' '' 'Adopted matrix header:'
sed -n '1,65p' ADOPTED_QUERY_ENGINE_IDEAS.md
