#!/usr/bin/env bash
set -euo pipefail

mode="${1:-deliver}"
commit_message="feat(sql): add incremental group count [skip ci]"
makefile_block=''
read -r -d '' makefile_block <<'EOF' || true
.PHONY: baseline-m038-incremental-group-count
baseline-m038-incremental-group-count:
	@bash scripts/test-m038-incremental-group-count.sh baseline

.PHONY: format-m038-incremental-group-count
format-m038-incremental-group-count:
	@bash scripts/test-m038-incremental-group-count.sh format

.PHONY: test-m038-incremental-group-count
test-m038-incremental-group-count:
	@bash scripts/test-m038-incremental-group-count.sh test

.PHONY: benchmark-m038-incremental-group-count
benchmark-m038-incremental-group-count:
	@bash scripts/test-m038-incremental-group-count.sh benchmark

.PHONY: test-m038-incremental-group-count-package
test-m038-incremental-group-count-package:
	@bash scripts/test-m038-incremental-group-count.sh package

.PHONY: race-m038-incremental-group-count
race-m038-incremental-group-count:
	@bash scripts/test-m038-incremental-group-count.sh race

.PHONY: vet-m038-incremental-group-count
vet-m038-incremental-group-count:
	@bash scripts/test-m038-incremental-group-count.sh vet

.PHONY: stage-m038-incremental-group-count
stage-m038-incremental-group-count:
	@bash scripts/deliver-m038-incremental-group-count.sh stage

.PHONY: commit-m038-incremental-group-count
commit-m038-incremental-group-count:
	@bash scripts/deliver-m038-incremental-group-count.sh commit

.PHONY: push-m038-incremental-group-count
push-m038-incremental-group-count:
	@bash scripts/deliver-m038-incremental-group-count.sh push

.PHONY: deliver-m038-incremental-group-count
deliver-m038-incremental-group-count:
	@bash scripts/deliver-m038-incremental-group-count.sh deliver
EOF

feature_files=(
  "M038_INCREMENTAL_GROUP_COUNT.md"
  "hat/hatSql/m038_incremental_group_count.go"
  "hat/hatSql/m038_incremental_group_count_test.go"
  "scripts/test-m038-incremental-group-count.sh"
  "scripts/deliver-m038-incremental-group-count.sh"
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
  temporary_makefile="$(mktemp /tmp/hatrie-m038-makefile.XXXXXX)"
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
