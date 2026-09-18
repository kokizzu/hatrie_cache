#!/usr/bin/env bash
set -euo pipefail

test -s MU032_UDF_CAPABILITIES.md
rg -n -F 'MU032_UDF_CAPABILITIES.md' README.md PRODUCT_IDEA_GAPS.md ADOPTED_QUERY_ENGINE_IDEAS.md
rg -n -F 'mu-032-udf-capability-classification' README.md ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md
go test ./hat/hatSql ./hat/hatCache -run '^TestMU032' -count=1
