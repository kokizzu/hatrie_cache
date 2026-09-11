#!/usr/bin/env bash
set -euo pipefail

test -f SQL_TEMPORAL_VALIDITY.md
rg -F -q 'SQL_TEMPORAL_VALIDITY.md' README.md
rg -F -q 'MZ-009' ENGINE_IDEAS.md
rg -F -q 'Temporal validity filters' ADOPTED_QUERY_ENGINE_IDEAS.md
rg -F -q '## MZ-009 temporal validity filters' BENCHMARK.md
rg -F -q 'make benchmark-mz009-temporal-validity' SQL_TEMPORAL_VALIDITY.md BENCHMARK.md
