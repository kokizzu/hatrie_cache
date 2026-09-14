#!/usr/bin/env bash
set -euo pipefail

printf 'TTL symbols:\n'
rg -n 'TypedTableTTL|PurgeExpired|deadlines|typedTableRowHidden' \
  hat/hatSql/typed_table.go \
  hat/hatSql/typed_table_ttl.go \
  hat/hatSql/typed_table_patch_parts.go \
  hat/hatSql/typed_table_stats.go \
  hat/hatSql/typed_table_histogram.go
printf 'Makefile targets:\n'
rg -n -A 3 'test-ch007-row-ttl-c203|format-ch007-row-ttl-c203|benchmark-ch007-row-ttl-c203|verify-ch007-row-ttl-c203' Makefile
printf 'Makefile diff:\n'
git diff --unified=0 -- Makefile
printf 'Working tree:\n'
git status --short
printf 'Feature diffstat:\n'
git diff --stat -- \
  Makefile README.md ENGINE_IDEAS.md ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md CH007_ROW_TTL.md \
  hat/hatSql/typed_table.go hat/hatSql/typed_table_patch_parts.go hat/hatSql/typed_table_stats.go \
  hat/hatSql/typed_table_histogram.go hat/hatSql/typed_table_ttl.go \
  hat/hatSql/ch007_row_ttl_test.go hat/hatSql/ch007_row_ttl_benchmark_test.go \
  scripts/test-ch007-row-ttl-c203.sh scripts/format-ch007-row-ttl-c203.sh \
  scripts/benchmark-ch007-row-ttl-c203.sh scripts/verify-ch007-row-ttl-c203.sh \
  scripts/inspect-ch007-ttl-c203.sh
printf 'Changed production files:\n'
git diff --stat -- \
  hat/hatSql/typed_table.go hat/hatSql/typed_table_patch_parts.go \
  hat/hatSql/typed_table_stats.go hat/hatSql/typed_table_histogram.go
printf 'Stats diff:\n'
git diff --cached --unified=1 -- hat/hatSql/typed_table_stats.go
printf 'TTL document ending:\n'
tail -n 8 CH007_ROW_TTL.md
