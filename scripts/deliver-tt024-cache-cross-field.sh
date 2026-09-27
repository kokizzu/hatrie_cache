#!/usr/bin/env bash
set -euo pipefail

action=${1:-status}
begin_makefile='# TT024_CACHE_CROSS_FIELD_BEGIN'
end_makefile='# TT024_CACHE_CROSS_FIELD_END'
source_path='hat/hatCache/sql_text_phrase.go'
source_begin='// ResolveSQLTextProximityMultiFieldUnionSource'
source_end='// ResolveSQLTextProximityMultiFieldIntersectionSource'

feature_files=(
  "$source_path"
  'hat/hatCache/tt024_cross_field_test.go'
  'hat/hatCache/tt024_cross_field_benchmark_test.go'
  'scripts/format-tt024-cross-field.sh'
  'scripts/test-tt024-cross-field.sh'
  'scripts/test-tt024-sql-cross-field.sh'
  'scripts/benchmark-tt024-cache-cross-field.sh'
  'scripts/race-tt024-cache-cross-field.sh'
  'scripts/vet-tt024-cache-cross-field.sh'
  'scripts/deliver-tt024-cache-cross-field.sh'
  'TT024_MULTI_FIELD_TEXT_UNION.md'
  'ENGINE_IDEAS.md'
  'Makefile'
)

die() {
  printf 'delivery error: %s\n' "$1" >&2
  exit 1
}

status() {
  git status --short
}

assert_no_staged_changes() {
  if ! git diff --cached --quiet; then
    die 'pre-existing staged changes found; refusing to mix feature files'
  fi
}

make_patch() {
  local old_file=$1
  local new_file=$2
  local patch_file=$3
  local diff_status
  set +e
  diff -u -L "a/$4" -L "b/$4" "$old_file" "$new_file" >"$patch_file"
  diff_status=$?
  set -e
  case "$diff_status" in
    0) return 1 ;;
    1) return 0 ;;
    *) die "could not create isolated patch for $4" ;;
  esac
}

stage_synthetic_source() {
  local temp_dir=$1
  git show "HEAD:$source_path" >"$temp_dir/source.old"
  awk -v begin="$source_begin" -v end="$source_end" -v block="$temp_dir/source.block" '
    index($0, begin) == 1 {capturing = 1}
    capturing && index($0, end) == 1 {exit}
    capturing {print}
  ' "$source_path" >"$temp_dir/source.block"
  [[ -s "$temp_dir/source.block" ]] || die 'TT-024 source block was not found'
  awk -v end="$source_end" -v block="$temp_dir/source.block" '
    index($0, end) == 1 {
      while ((getline line < block) > 0) print line
      close(block)
    }
    {print}
  ' "$temp_dir/source.old" >"$temp_dir/source.new"
  if make_patch "$temp_dir/source.old" "$temp_dir/source.new" "$temp_dir/source.patch" "$source_path"; then
    git apply --cached "$temp_dir/source.patch"
  fi
}

stage_synthetic_engine_ideas() {
  local temp_dir=$1
  local row_count
  git show HEAD:ENGINE_IDEAS.md >"$temp_dir/ideas.old"
  awk '/^\| TT-024 \|/ {print; count++} END {if (count != 1) exit 1}' ENGINE_IDEAS.md >"$temp_dir/ideas.row" || die 'expected exactly one current TT-024 idea row'
  row_count=$(wc -l <"$temp_dir/ideas.row")
  [[ "$row_count" -eq 1 ]] || die 'TT-024 idea row extraction failed'
  awk -v row_file="$temp_dir/ideas.row" '
    /^\| TT-024 \|/ {
      getline replacement < row_file
      close(row_file)
      print replacement
      replaced = 1
      next
    }
    {print}
    END {if (!replaced) exit 1}
  ' "$temp_dir/ideas.old" >"$temp_dir/ideas.new" || die 'TT-024 idea row was not present in HEAD'
  if make_patch "$temp_dir/ideas.old" "$temp_dir/ideas.new" "$temp_dir/ideas.patch" ENGINE_IDEAS.md; then
    git apply --cached "$temp_dir/ideas.patch"
  fi
}

stage_synthetic_makefile() {
  local temp_dir=$1
  git show HEAD:Makefile >"$temp_dir/make.old"
  awk -v begin="$begin_makefile" -v end="$end_makefile" '
    $0 == begin {capturing = 1}
    capturing {print}
    capturing && $0 == end {exit}
  ' Makefile >"$temp_dir/make.block"
  [[ -s "$temp_dir/make.block" ]] || die 'TT-024 Makefile block was not found'
  cp "$temp_dir/make.old" "$temp_dir/make.new"
  printf '\n' >>"$temp_dir/make.new"
  cat "$temp_dir/make.block" >>"$temp_dir/make.new"
  if make_patch "$temp_dir/make.old" "$temp_dir/make.new" "$temp_dir/make.patch" Makefile; then
    git apply --cached "$temp_dir/make.patch"
  fi
}

verify_staged_files() {
  local path
  local staged_count=0
  while IFS= read -r path; do
    staged_count=$((staged_count + 1))
    case " ${feature_files[*]} " in
      *" $path "*) ;;
      *) die "unexpected staged path: $path" ;;
    esac
  done < <(git diff --cached --name-only)
  [[ "$staged_count" -eq "${#feature_files[@]}" ]] || die 'staged feature file set is incomplete'
}

stage() {
  assert_no_staged_changes
  local temp_dir
  temp_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-tt024-stage.XXXXXX")
  trap 'rm -rf "$temp_dir"' RETURN
  git add -- \
    'hat/hatCache/tt024_cross_field_test.go' \
    'hat/hatCache/tt024_cross_field_benchmark_test.go' \
    'scripts/format-tt024-cross-field.sh' \
    'scripts/test-tt024-cross-field.sh' \
    'scripts/test-tt024-sql-cross-field.sh' \
    'scripts/benchmark-tt024-cache-cross-field.sh' \
    'scripts/race-tt024-cache-cross-field.sh' \
    'scripts/vet-tt024-cache-cross-field.sh' \
    'scripts/deliver-tt024-cache-cross-field.sh' \
    'TT024_MULTI_FIELD_TEXT_UNION.md'
  stage_synthetic_source "$temp_dir"
  stage_synthetic_engine_ideas "$temp_dir"
  stage_synthetic_makefile "$temp_dir"
  git diff --cached --check
  verify_staged_files
  trap - RETURN
  rm -rf "$temp_dir"
}

unstage() {
  local path
  for path in "${feature_files[@]}"; do
    git restore --staged -- "$path" 2>/dev/null || true
  done
}

normalize_makefile() {
  local temp_dir=$1
  awk -v begin="$begin_makefile" -v end="$end_makefile" '
    $0 == begin {capturing = 1; next}
    capturing && $0 == end {capturing = 0; next}
    !capturing {print}
  ' Makefile >"$temp_dir/make.without"
  awk -v begin="$begin_makefile" -v end="$end_makefile" '
    $0 == begin {capturing = 1}
    capturing {print}
    capturing && $0 == end {exit}
  ' Makefile >"$temp_dir/make.block"
  printf '\n' >>"$temp_dir/make.without"
  cat "$temp_dir/make.block" >>"$temp_dir/make.without"
  mv "$temp_dir/make.without" Makefile
}

commit() {
  verify_staged_files
  git commit -m 'feat: add TT024 multi-field text unions [skip ci]'
  local temp_dir
  temp_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-tt024-normalize.XXXXXX")
  normalize_makefile "$temp_dir"
  rm -rf "$temp_dir"
}

push() {
  git push origin HEAD
}

case "$action" in
  status) status ;;
  unstage) unstage ;;
  stage) stage ;;
  commit) commit ;;
  push) push ;;
  deliver) stage; commit; push ;;
  *) die "unknown action: $action" ;;
esac
