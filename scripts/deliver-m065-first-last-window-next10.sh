#!/usr/bin/env bash
set -euo pipefail

action=${1:-status}
feature_paths=(
  "hat/hatSql/query.go"
  "hat/hatSql/m065_first_last_window_test.go"
  "M065_FIRST_LAST_WINDOW_STREAM.md"
  "ADOPTED_QUERY_ENGINE_IDEAS.md"
  "BENCHMARK.md"
  "INSPIRATION.md"
  "Makefile"
  "scripts/test-m065-first-last-window-next10.sh"
  "scripts/benchmark-m065-first-last-window-next10.sh"
  "scripts/format-m065-first-last-window-next10.sh"
  "scripts/test-m065-first-last-package-next10.sh"
  "scripts/race-m065-first-last-window-next10.sh"
  "scripts/vet-m065-first-last-window-next10.sh"
  "scripts/deliver-m065-first-last-window-next10.sh"
)

tmp_dir=
cleanup() {
  if [[ -n "$tmp_dir" ]]; then
    rm -rf "$tmp_dir"
  fi
}
trap cleanup EXIT

die() {
  printf 'error: %s\n' "$*" >&2
  exit 1
}

assert_clean_index() {
  if ! git diff --cached --quiet; then
    die "the index is not empty; review or commit the existing staged changes before staging M065"
  fi
}

is_feature_path() {
  local candidate
  for candidate in "${feature_paths[@]}"; do
    if [[ "$candidate" == "$1" ]]; then
      return 0
    fi
  done
  return 1
}

assert_cached_scope() {
  local path
  while IFS= read -r path || [[ -n "$path" ]]; do
    if ! is_feature_path "$path"; then
      die "staged path is outside M065 scope: $path"
    fi
  done < <(git diff --cached --name-only)
}

make_tmp_dir() {
  if [[ -z "$tmp_dir" ]]; then
    tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-m065-deliver.XXXXXX")
  fi
}

append_section_patch() {
  local path=$1
  local start=$2
  local label=$3
  local base section target patch diff_status

  make_tmp_dir
  base="$tmp_dir/${label}.base"
  section="$tmp_dir/${label}.section"
  target="$tmp_dir/${label}.target"
  patch="$tmp_dir/${label}.patch"

  git show "HEAD:$path" > "$base"
  awk -v start="$start" '
    index($0, start) == 1 { found = 1 }
    found { print }
    END { if (!found) exit 1 }
  ' "$path" > "$section" || die "could not find M065 section in $path"

  {
    cat "$base"
    printf '\n'
    cat "$section"
  } > "$target"

  diff_status=0
  git diff --no-index --src-prefix=a/ --dst-prefix=b/ "$base" "$target" > "$patch" || diff_status=$?
  if [[ "$diff_status" -ne 1 ]]; then
    die "unexpected diff result while preparing $path (status $diff_status)"
  fi
  sed -i \
    -e "s|a${tmp_dir}/${label}.base|a/$path|g" \
    -e "s|b${tmp_dir}/${label}.target|b/$path|g" \
    -e "s|${tmp_dir}/${label}.base|$path|g" \
    -e "s|${tmp_dir}/${label}.target|$path|g" \
    "$patch"
  git apply --cached "$patch"
}

marker_section_patch() {
  local path=$1
  local start=$2
  local end=$3
  local label=$4
  local base section target patch diff_status

  make_tmp_dir
  base="$tmp_dir/${label}.base"
  section="$tmp_dir/${label}.section"
  target="$tmp_dir/${label}.target"
  patch="$tmp_dir/${label}.patch"

  git show "HEAD:$path" > "$base"
  awk -v start="$start" -v end="$end" '
    index($0, start) == 1 { capturing = 1 }
    capturing { print }
    capturing && index($0, end) == 1 { found = 1; exit }
    END { if (!found) exit 1 }
  ' "$path" > "$section" || die "could not find M065 marker block in $path"

  {
    cat "$base"
    printf '\n'
    cat "$section"
  } > "$target"

  diff_status=0
  git diff --no-index --src-prefix=a/ --dst-prefix=b/ "$base" "$target" > "$patch" || diff_status=$?
  if [[ "$diff_status" -ne 1 ]]; then
    die "unexpected diff result while preparing $path (status $diff_status)"
  fi
  sed -i \
    -e "s|a${tmp_dir}/${label}.base|a/$path|g" \
    -e "s|b${tmp_dir}/${label}.target|b/$path|g" \
    -e "s|${tmp_dir}/${label}.base|$path|g" \
    -e "s|${tmp_dir}/${label}.target|$path|g" \
    "$patch"
  git apply --cached "$patch"
}

stage_feature() {
  assert_clean_index
  git diff --check -- "${feature_paths[@]}"

  git add -- \
    "hat/hatSql/query.go" \
    "hat/hatSql/m065_first_last_window_test.go" \
    "M065_FIRST_LAST_WINDOW_STREAM.md" \
    "scripts/test-m065-first-last-window-next10.sh" \
    "scripts/benchmark-m065-first-last-window-next10.sh" \
    "scripts/format-m065-first-last-window-next10.sh" \
    "scripts/test-m065-first-last-package-next10.sh" \
    "scripts/race-m065-first-last-window-next10.sh" \
    "scripts/vet-m065-first-last-window-next10.sh" \
    "scripts/deliver-m065-first-last-window-next10.sh"

  append_section_patch "ADOPTED_QUERY_ENGINE_IDEAS.md" "## M065ag" adopted
  append_section_patch "BENCHMARK.md" '<a id="m065ag-sql-first_value-last_value-streaming">' benchmark
  append_section_patch "INSPIRATION.md" "- [x] M065ag SQL" inspiration
  marker_section_patch "Makefile" "# M065_FIRST_LAST_WINDOW_NEXT10_BEGIN" "# M065_FIRST_LAST_DELIVERY_NEXT10_END" makefile

  assert_cached_scope
  git diff --cached --check
  printf '%s\n' 'Staged M065 paths:'
  git diff --cached --name-status
  git diff --cached --stat
}

unstage_feature() {
  git restore --staged -- "${feature_paths[@]}"
  printf '%s\n' 'Remaining staged paths:'
  git diff --cached --name-status
}

commit_feature() {
  if git diff --cached --quiet; then
    stage_feature
  fi
  assert_cached_scope
  git diff --cached --check
  git commit -m 'feat: stream first and last value windows [skip ci]'
}

push_feature() {
  git push origin HEAD
}

case "$action" in
status)
  git status --short -- "${feature_paths[@]}"
  git diff --check -- "${feature_paths[@]}"
  git diff --stat -- "${feature_paths[@]}"
  printf '%s\n' 'Already staged paths:'
  git diff --cached --name-status
  ;;
stage)
  stage_feature
  ;;
unstage)
  unstage_feature
  ;;
commit)
  commit_feature
  ;;
push)
  push_feature
  ;;
deliver)
  stage_feature
  commit_feature
  push_feature
  ;;
*)
  printf 'unsupported action: %s\n' "$action" >&2
  exit 2
  ;;
esac
