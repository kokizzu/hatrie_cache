#!/usr/bin/env bash
set -euo pipefail

git add -- BENCHMARK.md ENGINE_IDEAS.md CH045_NESTED_MAP_SUBCOLUMNS.md Makefile hat/hatSql/ch030_map_subcolumn_benchmark_test.go hat/hatSql/ch030_map_subcolumn_test.go hat/hatSql/columnar_map_scan.go scripts/benchmark-ch045-map-subcolumns-nested.sh scripts/benchmark-ch045-map-subcolumns-top-level.sh scripts/benchmark-ch045-map-subcolumns.sh scripts/commit-ch045-map-subcolumns.sh scripts/format-ch045-map-subcolumns.sh scripts/push-ch045-map-subcolumns.sh scripts/race-ch045-map-subcolumns.sh scripts/review-ch045-map-subcolumns.sh scripts/stage-ch045-map-subcolumns.sh scripts/test-ch045-map-subcolumns-package.sh scripts/test-ch045-map-subcolumns.sh scripts/verify-ch045-map-subcolumns.sh
