#!/usr/bin/env bash
set -euo pipefail

mode="${1:-deliver}"
commit_message="feat(sql): add incremental group count sum [skip ci]"
makefile_block=''
read -r -d '' makefile_block <<'EOF' || true
.PHONY: baseline-m038-incremental-group-count-sum
baseline-m038-incremental-group-count-sum:
	@bash scripts/test-m038-incremental-group-count-sum.sh baseline

.PHONY: format-m038-incremental-group-count-sum
format-m038-incremental-group-count-sum:
	@bash scripts/test-m038-incremental-group-count-sum.sh format

.PHONY: test-m038-incremental-group-count-sum
test-m038-incremental-group-count-sum:
	@bash scripts/test-m038-incremental-group-count-sum.sh test

.PHONY: benchmark-m038-incremental-group-count-sum
benchmark-m038-incremental-group-count-sum:
	@bash scripts/test-m038-incremental-group-count-sum.sh benchmark

.PHONY: test-m038-incremental-group-count-sum-package
test-m038-incremental-group-count-sum-package:
	@bash scripts/test-m038-incremental-group-count-sum.sh package

.PHONY: race-m038-incremental-group-count-sum
race-m038-incremental-group-count-sum:
	@bash scripts/test-m038-incremental-group-count-sum.sh race

.PHONY: vet-m038-incremental-group-count-sum
vet-m038-incremental-group-count-sum:
	@bash scripts/test-m038-incremental-group-count-sum.sh vet

.PHONY: stage-m038-incremental-group-count-sum
stage-m038-incremental-group-count-sum:
	@bash scripts/deliver-m038-incremental-group-count-sum.sh stage

.PHONY: commit-m038-incremental-group-count-sum
commit-m038-incremental-group-count-sum:
	@bash scripts/deliver-m038-incremental-group-count-sum.sh commit

.PHONY: push-m038-incremental-group-count-sum
push-m038-incremental-group-count-sum:
	@bash scripts/deliver-m038-incremental-group-count-sum.sh push

.PHONY: deliver-m038-incremental-group-count-sum
deliver-m038-incremental-group-count-sum:
	@bash scripts/deliver-m038-incremental-group-count-sum.sh deliver
EOF

feature_files=(
  "M038_INCREMENTAL_GROUP_COUNT_SUM.md"
  "hat/hatSql/m038_incremental_group_count_sum.go"
  "hat/hatSql/m038_incremental_group_count_sum_test.go"
  "scripts/test-m038-incremental-group-count-sum.sh"
  "scripts/deliver-m038-incremental-group-count-sum.sh"
)

require_clean_index() {
  if ! git diff --cached --quiet; then
    printf 'refusing to stage: the index already contains changes\n' >&2
    exit 1
  fi
}

stage_feature() {
  require_clean_index
  git add -- "${feature_files[@]}"
  temporary_makefile="$(mktemp /tmp/hatrie-m038-sum-makefile.XXXXXX)"
  trap 'rm -f "$temporary_makefile"' EXIT
  git show HEAD:Makefile > "$temporary_makefile"
  printf '\n%s\n' "$makefile_block" >> "$temporary_makefile"
  makefile_blob="$(git hash-object -w "$temporary_makefile")"
  git update-index --add --cacheinfo "100644,$makefile_blob,Makefile"
  git diff --cached --check
  git diff --cached --name-only
}

case "$mode" in
  stage)
    stage_feature
    ;;
  commit)
    if git diff --cached --quiet; then
      stage_feature
    fi
    git commit -m "$commit_message"
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
