#!/usr/bin/env bash
set -euo pipefail

test -f MZ014_UPSERT_BATCH.md
rg -F 'MZ014_UPSERT_BATCH.md' README.md ADOPTED_QUERY_ENGINE_IDEAS.md
rg -F 'MZ-014' ENGINE_IDEAS.md BENCHMARK.md MZ014_UPSERT_BATCH.md
rg -F 'BENCHMARK.md#mz-014-upsert-batch-consolidation' MZ014_UPSERT_BATCH.md ADOPTED_QUERY_ENGINE_IDEAS.md
rg -F 'make test-mz014-upsert' MZ014_UPSERT_BATCH.md
rg -F 'make benchmark-mz014-upsert' MZ014_UPSERT_BATCH.md BENCHMARK.md
printf '%s\n' 'MZ-014 documentation links, benchmark references, and commands verified.'
