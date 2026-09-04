#!/bin/sh
set -eu

gofmt -w \
  hat/hatSql/rewrite.go \
  hat/hatSql/constant_folding_test.go \
  hat/hatSql/constant_folding_benchmark_test.go

for file in \
  Makefile \
  CONSTANT_FOLDING.md \
  scripts/audit-next-inspiration.sh \
  scripts/benchmark-sql-constant-folding.sh \
  scripts/format-sql-constant-folding.sh \
  scripts/test-race-sql-constant-folding.sh \
  scripts/test-sql-constant-folding.sh \
  scripts/vet-sql-constant-folding.sh \
  scripts/commit-sql-constant-folding.sh
do
  while [ "$(tail -n 1 "$file")" = "" ]; do
    sed -i '${/^$/d;}' "$file"
  done
done
