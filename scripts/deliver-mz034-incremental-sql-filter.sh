#!/usr/bin/env bash
set -euo pipefail

mode=${1:-deliver}
repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_root"

commit_message='feat(sql): add incremental SQL filter lowering [skip ci]'
feature_files=(
  MZ034_GENERIC_NEGATIVE_DIFF.md
  MZ034_INCREMENTAL_SQL_FILTER.md
  ADOPTED_QUERY_ENGINE_IDEAS.md
  ENGINE_IDEAS.md
  README.md
  hat/hatSql/mz034_sql_incremental_filter.go
  hat/hatSql/mz034_sql_incremental_filter_test.go
  scripts/test-mz034-incremental-sql-filter.sh
  scripts/deliver-mz034-incremental-sql-filter.sh
)

tmp_files=()
cleanup() {
  if ((${#tmp_files[@]} > 0)); then
    rm -f -- "${tmp_files[@]}"
  fi
}
trap cleanup EXIT

require_clean_index() {
  if ! git diff --cached --quiet; then
    printf 'Refusing to stage: the index already contains changes.\n' >&2
    exit 1
  fi
}

stage_makefile_targets() {
  local snapshot block blob
  snapshot=$(mktemp /tmp/hatrie-mz034-makefile.XXXXXX)
  block=$(mktemp /tmp/hatrie-mz034-targets.XXXXXX)
  tmp_files+=("$snapshot" "$block")

  git show HEAD:Makefile > "$snapshot"
  cat > "$block" <<'EOF'

baseline-mz034-incremental-sql-filter:
	bash ./scripts/test-mz034-incremental-sql-filter.sh baseline

format-mz034-incremental-sql-filter:
	bash ./scripts/test-mz034-incremental-sql-filter.sh format

test-mz034-incremental-sql-filter:
	bash ./scripts/test-mz034-incremental-sql-filter.sh test

benchmark-mz034-incremental-sql-filter:
	bash ./scripts/test-mz034-incremental-sql-filter.sh benchmark

test-mz034-incremental-sql-filter-package:
	bash ./scripts/test-mz034-incremental-sql-filter.sh package

race-mz034-incremental-sql-filter:
	bash ./scripts/test-mz034-incremental-sql-filter.sh race

vet-mz034-incremental-sql-filter:
	bash ./scripts/test-mz034-incremental-sql-filter.sh vet

stage-mz034-incremental-sql-filter:
	bash ./scripts/deliver-mz034-incremental-sql-filter.sh stage

commit-mz034-incremental-sql-filter:
	bash ./scripts/deliver-mz034-incremental-sql-filter.sh commit

push-mz034-incremental-sql-filter:
	bash ./scripts/deliver-mz034-incremental-sql-filter.sh push

deliver-mz034-incremental-sql-filter:
	bash ./scripts/deliver-mz034-incremental-sql-filter.sh deliver
EOF
  printf '\n' >> "$snapshot"
  cat "$block" >> "$snapshot"
  blob=$(git hash-object -w "$snapshot")
  git update-index --add --cacheinfo "100644,$blob,Makefile"
}

stage_feature() {
  require_clean_index
  git add -- "${feature_files[@]}"
  stage_makefile_targets
  git diff --cached --check
  git diff --cached --name-status
}

commit_feature() {
  if git diff --cached --quiet; then
    stage_feature
  fi
  git commit -m "$commit_message"
}

case "$mode" in
  stage)
    stage_feature
    ;;
  commit)
    commit_feature
    ;;
  push)
    git push origin HEAD
    ;;
  deliver)
    stage_feature
    git commit -m "$commit_message"
    git push origin HEAD
    ;;
  *)
    printf 'usage: %s {stage|commit|push|deliver}\n' "$0" >&2
    exit 2
    ;;
esac
