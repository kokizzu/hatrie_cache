#!/usr/bin/env bash
set -euo pipefail
test -s MU028_MONOTONICITY.md
rg -F 'MU028_MONOTONICITY.md' README.md PRODUCT_IDEA_GAPS.md ADOPTED_QUERY_ENGINE_IDEAS.md
rg -F 'mu-028-sql-monotonicity-inference' BENCHMARK.md
go test ./hat/hatSql -run 'TestMU028' -count=1
