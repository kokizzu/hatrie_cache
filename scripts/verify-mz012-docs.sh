#!/usr/bin/env bash
set -euo pipefail

test -f MZ012_EXACTLY_ONCE_SINK.md
rg -F 'MZ012_EXACTLY_ONCE_SINK.md' README.md ADOPTED_QUERY_ENGINE_IDEAS.md
rg -F 'MZ-012' ENGINE_IDEAS.md BENCHMARK.md MZ012_EXACTLY_ONCE_SINK.md
rg -F 'BENCHMARK.md#mz-012-exactly-once-sink-checkpoints' MZ012_EXACTLY_ONCE_SINK.md ADOPTED_QUERY_ENGINE_IDEAS.md
printf '%s\n' 'MZ-012 documentation links and references verified.'
