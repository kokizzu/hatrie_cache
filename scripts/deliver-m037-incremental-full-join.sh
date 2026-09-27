#!/usr/bin/env bash
set -euo pipefail

readonly COMMIT_MESSAGE="feat(sql): add incremental full outer join [skip ci]"
readonly FEATURE_FILES=(
  M037_INCREMENTAL_FULL_JOIN.md
  hat/hatSql/m037_incremental_full_join.go
  hat/hatSql/m037_incremental_full_join_test.go
  scripts/test-m037-incremental-full-join.sh
  scripts/deliver-m037-incremental-full-join.sh
)

makefile_block() {
  cat <<'EOF'

.PHONY: format-m037-incremental-full-join test-m037-incremental-full-join benchmark-m037-incremental-full-join test-m037-full-join-package race-m037-incremental-full-join vet-m037-incremental-full-join
format-m037-incremental-full-join:
	@bash scripts/test-m037-incremental-full-join.sh format
test-m037-incremental-full-join:
	@bash scripts/test-m037-incremental-full-join.sh test
benchmark-m037-incremental-full-join:
	@bash scripts/test-m037-incremental-full-join.sh benchmark
test-m037-full-join-package:
	@bash scripts/test-m037-incremental-full-join.sh package
race-m037-incremental-full-join:
	@bash scripts/test-m037-incremental-full-join.sh race
vet-m037-incremental-full-join:
	@bash scripts/test-m037-incremental-full-join.sh vet
stage-m037-incremental-full-join:
	@bash scripts/deliver-m037-incremental-full-join.sh stage
commit-m037-incremental-full-join:
	@bash scripts/deliver-m037-incremental-full-join.sh commit
push-m037-incremental-full-join:
	@bash scripts/deliver-m037-incremental-full-join.sh push
deliver-m037-incremental-full-join:
	@bash scripts/deliver-m037-incremental-full-join.sh deliver
EOF
}

ensure_empty_index() {
  if ! git diff --cached --quiet; then
    printf '%s\n' 'Refusing to stage M037 full join with pre-existing staged changes.' >&2
    git diff --cached --name-status >&2
    exit 1
  fi
}

ensure_feature_files() {
  local file
  for file in "${FEATURE_FILES[@]}"; do
    if [[ ! -f "$file" ]]; then
      printf 'Missing feature file: %s\n' "$file" >&2
      exit 1
    fi
  done
}

stage_makefile_block() {
  local temp_dir base candidate patch diff_status
  temp_dir=$(mktemp -d "${TMPDIR:-/tmp}/m037-full-deliver.XXXXXX")
  base="$temp_dir/Makefile.base"
  candidate="$temp_dir/Makefile.candidate"
  patch="$temp_dir/Makefile.patch"

  git show HEAD:Makefile > "$base"
  cp "$base" "$candidate"
  makefile_block >> "$candidate"
  diff_status=0
  diff -u --label a/Makefile --label b/Makefile "$base" "$candidate" > "$patch" || diff_status=$?
  if (( diff_status > 1 )); then
    printf '%s\n' 'Could not build the full-join Makefile staging patch.' >&2
    rm -rf "$temp_dir"
    exit 1
  fi
  git apply --cached "$patch"
  rm -rf "$temp_dir"
}

verify_staged_files() {
  local expected actual
  expected=$(mktemp "${TMPDIR:-/tmp}/m037-full-expected.XXXXXX")
  actual=$(mktemp "${TMPDIR:-/tmp}/m037-full-actual.XXXXXX")
  printf '%s\n' Makefile "${FEATURE_FILES[@]}" | LC_ALL=C sort > "$expected"
  git diff --cached --name-only | LC_ALL=C sort > "$actual"
  if ! diff -u "$expected" "$actual"; then
    printf '%s\n' 'Unexpected staged paths; refusing to continue.' >&2
    rm -f "$expected" "$actual"
    exit 1
  fi
  git diff --cached --check
  rm -f "$expected" "$actual"
}

stage() {
  ensure_empty_index
  ensure_feature_files
  git add -- "${FEATURE_FILES[@]}"
  stage_makefile_block
  verify_staged_files
  git diff --cached --stat
}

commit() {
  verify_staged_files
  git commit -m "$COMMIT_MESSAGE"
}

push() {
  git push origin HEAD
}

case "${1:-}" in
  stage)
    stage
    ;;
  commit)
    commit
    ;;
  push)
    push
    ;;
  deliver)
    stage
    commit
    push
    ;;
  *)
    printf 'Usage: %s {stage|commit|push|deliver}\n' "$0" >&2
    exit 2
    ;;
esac
