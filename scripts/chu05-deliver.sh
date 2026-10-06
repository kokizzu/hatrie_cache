#!/usr/bin/env bash
set -euo pipefail

branch='codex/inspiration-chu05-partitioned-window-20261006'
commit_message='feat(sql): stream partitioned external windows [skip ci]'
files=(
  Makefile
  README.md
  BENCHMARK.md
  CHU05_EXTERNAL_WINDOW_STREAM.md
  PRODUCT_IDEA_GAPS.md
  ADOPTED_QUERY_ENGINE_IDEAS.md
  hat/hatSql/query.go
  hat/hatSql/chu05_external_partitioned_window_test.go
  hat/hatSql/chu05_external_partitioned_window_benchmark_test.go
  scripts/chu05-partitioned-window.sh
  scripts/chu05-deliver.sh
)

mode=${1:-status}

require_branch() {
  actual=$(git branch --show-current)
  if [[ "$actual" != "$branch" ]]; then
    printf 'expected branch %s, got %s\n' "$branch" "$actual" >&2
    exit 1
  fi
}

verify_staged_scope() {
  while read -r path; do
    [[ -z "$path" ]] && continue
    allowed=false
    for expected in "${files[@]}"; do
      if [[ "$path" == "$expected" ]]; then
        allowed=true
        break
      fi
    done
    if [[ "$allowed" != true ]]; then
      printf 'unexpected staged path: %s\n' "$path" >&2
      exit 1
    fi
  done < <(git diff --cached --name-only)
}

case "$mode" in
  status)
    require_branch
    git status --short --branch
    git diff --check -- "${files[@]}"
    ;;
  verify)
    require_branch
    git diff --check -- "${files[@]}"
    git diff --cached --check -- "${files[@]}"
    verify_staged_scope
    ;;
  stage)
    require_branch
    git add -- "${files[@]}"
    verify_staged_scope
    ;;
  commit)
    require_branch
    verify_staged_scope
    if [[ -z "$(git diff --cached --name-only)" ]]; then
      printf 'no staged feature changes\n' >&2
      exit 1
    fi
    git commit -m "$commit_message"
    ;;
  push)
    require_branch
    git push origin "$branch"
    ;;
  *)
    printf 'usage: %s {status|verify|stage|commit|push}\n' "$0" >&2
    exit 2
    ;;
esac
