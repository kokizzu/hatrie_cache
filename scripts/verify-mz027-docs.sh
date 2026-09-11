#!/usr/bin/env bash
set -euo pipefail

printf 'telemetry doc\n'
test -f TYPED_TABLE_ARRANGEMENT_TELEMETRY.md
printf 'benchmark heading\n'
rg -q '^## MZ-027 Arrangement Memory Telemetry$' BENCHMARK.md
printf 'readme link\n'
rg -q 'TYPED_TABLE_ARRANGEMENT_TELEMETRY.md' README.md
printf 'catalog row\n'
rg -q 'MZ-027.*Arrangement memory telemetry' ENGINE_IDEAS.md
printf 'adoption row\n'
rg -q 'Arrangement memory telemetry' ADOPTED_QUERY_ENGINE_IDEAS.md
printf 'benchmark reference\n'
rg -q 'BenchmarkMZ027ArrangementStats' TYPED_TABLE_ARRANGEMENT_TELEMETRY.md
printf 'compaction field\n'
rg -q 'CompactionCount' TYPED_TABLE_ARRANGEMENT_TELEMETRY.md
