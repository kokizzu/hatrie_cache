#!/usr/bin/env bash
set -euo pipefail

test -s MU031_RETRACTABLE_AGGREGATES.md
rg -n -F 'MU031_RETRACTABLE_AGGREGATES.md' README.md PRODUCT_IDEA_GAPS.md ADOPTED_QUERY_ENGINE_IDEAS.md
rg -n -F 'mu-031-retractable-aggregate-capabilities' README.md ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md
go test ./hat/hatSql -run '^TestMU031' -count=1
