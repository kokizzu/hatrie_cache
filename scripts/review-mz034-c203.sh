#!/usr/bin/env bash
set -euo pipefail

test -f hat/hatSql/differential_rows.go
test -f hat/hatSql/differential_operators.go
test -f hat/hatSql/differential_difference.go
test -f hat/hatSql/differential_intersect.go
test -f MZ034_GENERIC_NEGATIVE_DIFF.md
rg -n 'DifferentialRow|FilterDifferentialRows|MapDifferentialRows|FlatMapDifferentialRows|UnionDifferentialRows|JoinDifferentialRows|ExceptDifferentialRows|DifferentialIntersect|MZ-034' hat/hatSql MZ034_GENERIC_NEGATIVE_DIFF.md ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md
git diff --check
