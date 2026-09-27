#!/usr/bin/env bash
set -euo pipefail

mode=${1:-deliver}
repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_root"

commit_message='feat(sql): add incremental grouped count distinct [skip ci]'
feature_files=(
  M039_INCREMENTAL_GROUP_COUNT_DISTINCT.md
  hat/hatSql/m039_incremental_group_count_distinct.go
  hat/hatSql/m039_incremental_group_count_distinct_test.go
  scripts/test-m039-incremental-group-count-distinct.sh
  scripts/deliver-m039-incremental-group-count-distinct.sh
)

require_clean_index() {
  if ! git diff --cached --quiet; then
    printf 'Refusing to stage: the index already contains changes.\n' >&2
    exit 1
  fi
}

stage_makefile_targets() {
  local snapshot block blob
  snapshot=$(mktemp /tmp/hatrie-m039-makefile.XXXXXX)
  block=$(mktemp /tmp/hatrie-m039-targets.XXXXXX)
  trap 'rm -f "${snapshot:-}" "${block:-}"' RETURN

  git show HEAD:Makefile > "$snapshot"
  cat > "$block" <<'EOF'

baseline-m039-incremental-group-count-distinct:
	bash ./scripts/test-m039-incremental-group-count-distinct.sh baseline

format-m039-incremental-group-count-distinct:
	bash ./scripts/test-m039-incremental-group-count-distinct.sh format

test-m039-incremental-group-count-distinct:
	bash ./scripts/test-m039-incremental-group-count-distinct.sh test

benchmark-m039-incremental-group-count-distinct:
	bash ./scripts/test-m039-incremental-group-count-distinct.sh benchmark

test-m039-incremental-group-count-distinct-package:
	bash ./scripts/test-m039-incremental-group-count-distinct.sh package

race-m039-incremental-group-count-distinct:
	bash ./scripts/test-m039-incremental-group-count-distinct.sh race

vet-m039-incremental-group-count-distinct:
	bash ./scripts/test-m039-incremental-group-count-distinct.sh vet

restage-m039-incremental-group-count-distinct:
	bash ./scripts/deliver-m039-incremental-group-count-distinct.sh restage

stage-m039-incremental-group-count-distinct:
	bash ./scripts/deliver-m039-incremental-group-count-distinct.sh stage

commit-m039-incremental-group-count-distinct:
	bash ./scripts/deliver-m039-incremental-group-count-distinct.sh commit

push-m039-incremental-group-count-distinct:
	bash ./scripts/deliver-m039-incremental-group-count-distinct.sh push

deliver-m039-incremental-group-count-distinct:
	bash ./scripts/deliver-m039-incremental-group-count-distinct.sh deliver
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

restage_feature() {
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
  restage)
    restage_feature
    ;;
  commit)
    commit_feature
    ;;
  push)
    commit_feature
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
