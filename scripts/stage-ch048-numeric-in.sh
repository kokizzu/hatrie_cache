#!/usr/bin/env bash
set -euo pipefail

stage_paths=(
  BENCHMARK.md
  CH048_NUMERIC_IN.md
  CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md
  ENGINE_IDEAS.md
  hat/hatSql/columnar_numeric_in_predicate.go
  hat/hatSql/ch048_numeric_in_test.go
  hat/hatSql/query.go
  scripts/benchmark-ch048-numeric-in.sh
  scripts/commit-ch048-numeric-in.sh
  scripts/format-ch048-numeric-in.sh
  scripts/push-ch048-numeric-in.sh
  scripts/race-ch048-numeric-in.sh
  scripts/review-ch048-numeric-in.sh
  scripts/stage-ch048-numeric-in.sh
  scripts/test-ch048-numeric-in-package.sh
  scripts/test-ch048-numeric-in.sh
  scripts/verify-ch048-numeric-in-docs.sh
  scripts/vet-ch048-numeric-in.sh
)
allowed_paths=(Makefile "${stage_paths[@]}")

if ! git diff --cached --quiet; then
  while IFS= read -r path; do
    [[ -z "$path" ]] && continue
    allowed=0
    for allowed_path in "${allowed_paths[@]}"; do
      if [[ "$path" == "$allowed_path" ]]; then
        allowed=1
        break
      fi
    done
    if [[ "$allowed" -eq 0 ]]; then
      printf 'refusing to refresh stage: unrelated path is staged: %s\n' "$path" >&2
      exit 1
    fi
  done < <(git diff --cached --name-only)
  git add -- "${stage_paths[@]}"
  git diff --cached --check
  git status --short
  exit 0
fi

base=$(mktemp)
candidate=$(mktemp)
block=$(mktemp)
patch=$(mktemp)
trap 'rm -f "$base" "$candidate" "$block" "$patch"' EXIT

git show HEAD:Makefile > "$base"
awk '
  /# CH048_NUMERIC_IN_FEATURE_TARGETS_BEGIN/ { found=1 }
  found { print }
  /# CH048_NUMERIC_IN_FEATURE_TARGETS_END/ && found { exit }
' Makefile > "$block"

if ! rg -q '^# CH048_NUMERIC_IN_FEATURE_TARGETS_BEGIN$' "$block" || \
   ! rg -q '^# CH048_NUMERIC_IN_FEATURE_TARGETS_END$' "$block"; then
  printf 'numeric target marker block is missing\n' >&2
  exit 1
fi

cat "$base" "$block" > "$candidate"
if diff -u --label a/Makefile --label b/Makefile "$base" "$candidate" > "$patch"; then
  printf 'numeric Makefile target block is already in HEAD\n' >&2
  exit 1
else
  diff_status=$?
  if [[ "$diff_status" -ne 1 ]]; then
    cat "$patch" >&2
    exit "$diff_status"
  fi
fi
git apply --cached "$patch"

git add -- "${stage_paths[@]}"

git diff --cached --check
git status --short
