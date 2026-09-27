#!/usr/bin/env bash
set -euo pipefail

allowed=(
  MZ037_SQL_INCREMENTAL_TOPK.md
  hat/hatSql/mz037_sql_topk.go
  hat/hatSql/mz037_sql_topk_test.go
  hat/hatSql/mz037_sql_topk_benchmark_test.go
  scripts/test-mz037-sql-topk.sh
  scripts/benchmark-mz037-sql-topk.sh
  scripts/format-mz037-sql-topk.sh
  scripts/race-mz037-sql-topk.sh
  scripts/vet-mz037-sql-topk.sh
  scripts/test-mz037-package.sh
  scripts/deliver-mz037-sql-topk.sh
)

if ! git diff --cached --quiet; then
  printf '%s\n' 'refusing delivery: the index already contains staged changes' >&2
  exit 1
fi

for path in "${allowed[@]}"; do
  test -f "$path"
done

git add -- \
  MZ037_SQL_INCREMENTAL_TOPK.md \
  hat/hatSql/mz037_sql_topk.go \
  hat/hatSql/mz037_sql_topk_test.go \
  hat/hatSql/mz037_sql_topk_benchmark_test.go \
  scripts/test-mz037-sql-topk.sh \
  scripts/benchmark-mz037-sql-topk.sh \
  scripts/format-mz037-sql-topk.sh \
  scripts/race-mz037-sql-topk.sh \
  scripts/vet-mz037-sql-topk.sh \
  scripts/test-mz037-package.sh \
  scripts/deliver-mz037-sql-topk.sh

git diff --cached --check

mapfile -t staged < <(git diff --cached --name-only)
if ((${#staged[@]} != ${#allowed[@]})); then
  printf '%s\n' 'refusing delivery: staged path count differs from the allowlist' >&2
  exit 1
fi
for path in "${staged[@]}"; do
  found=false
  for expected in "${allowed[@]}"; do
    if [[ "$path" == "$expected" ]]; then
      found=true
      break
    fi
  done
  if [[ "$found" != true ]]; then
    printf 'refusing delivery: unexpected staged path %s\n' "$path" >&2
    exit 1
  fi
done

git commit -m 'feat: add MZ-037 SQL incremental Top-K adapter [skip ci]'
git push origin HEAD
