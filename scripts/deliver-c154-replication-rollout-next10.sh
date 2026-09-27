#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

feature_paths=(
  "hat/hatCache/c154_replication_schema_rollout.go"
  "hat/hatCache/c154_replication_schema_rollout_test.go"
  "C154_REPLICATION_SCHEMA_ROLLOUT.md"
  "CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md"
  "ADOPTED_QUERY_ENGINE_IDEAS.md"
  "BENCHMARK.md"
  "Makefile"
  "scripts/test-c154-replication-rollout-next10.sh"
  "scripts/format-c154-replication-rollout-next10.sh"
  "scripts/benchmark-c154-replication-rollout-next10.sh"
  "scripts/test-c154-replication-rollout-package-next10.sh"
  "scripts/race-c154-replication-rollout-next10.sh"
  "scripts/vet-c154-replication-rollout-next10.sh"
  "scripts/deliver-c154-replication-rollout-next10.sh"
)

new_paths=(
  "hat/hatCache/c154_replication_schema_rollout.go"
  "hat/hatCache/c154_replication_schema_rollout_test.go"
  "C154_REPLICATION_SCHEMA_ROLLOUT.md"
  "scripts/test-c154-replication-rollout-next10.sh"
  "scripts/format-c154-replication-rollout-next10.sh"
  "scripts/benchmark-c154-replication-rollout-next10.sh"
  "scripts/test-c154-replication-rollout-package-next10.sh"
  "scripts/race-c154-replication-rollout-next10.sh"
  "scripts/vet-c154-replication-rollout-next10.sh"
  "scripts/deliver-c154-replication-rollout-next10.sh"
)

shared_paths=(
  "CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md"
  "ADOPTED_QUERY_ENGINE_IDEAS.md"
  "BENCHMARK.md"
  "Makefile"
)

readonly audit_marker="- [ ] C154 Rolling schema changes across replicas."
readonly audit_addition="- [x] C154g Opt-in replication-schema rollout bridge composes validated replica phases with contract acceptance and retires the previous contract only after the last replica activates; transport, checkpoint persistence, and topology publication remain caller-owned. See [C154_REPLICATION_SCHEMA_ROLLOUT.md](C154_REPLICATION_SCHEMA_ROLLOUT.md)."
readonly adopted_section_start="## C154g: Replication Schema Rollout Contract Bridge"
readonly adopted_section_end="## C205: Subquery Result Cache Controls"
readonly benchmark_section_start="<a id=\"c154g-replication-schema-rollout\"></a>"
readonly benchmark_section_end="<a id=\"m065ag-sql-first_value-last_value-streaming\"></a>"
readonly make_section_start="# C154_REPLICATION_SCHEMA_ROLLOUT_NEXT10_BEGIN"
readonly make_section_end="# C154_REPLICATION_SCHEMA_ROLLOUT_DELIVERY_NEXT10_END"

die() {
  printf 'delivery error: %s\n' "$*" >&2
  exit 1
}

assert_no_feature_paths_staged() {
  if ! git diff --cached --quiet -- "${feature_paths[@]}"; then
    die "one or more feature paths already have staged changes; inspect with make status-c154-replication-rollout-next10"
  fi
}

assert_current_line() {
  local path="$1"
  local line="$2"
  grep -Fqx -- "$line" "$path" || die "expected line is missing from $path"
}

make_no_index_patch() {
  local base="$1"
  local target="$2"
  local patch_path="$3"
  local rc

  set +e
  git diff --no-index --binary --src-prefix=a/ --dst-prefix=b/ "$base" "$target" >"$patch_path"
  rc=$?
  set -e
  case "$rc" in
    0) : >"$patch_path" ;;
    1) ;;
    *) die "could not create patch for $target" ;;
  esac
}

apply_cached_patch() {
  local rel_path="$1"
  local base="$2"
  local target="$3"
  local tmp_dir="$4"
  local patch_path="$tmp_dir/$(printf '%s' "$rel_path" | tr '/ ' '__').patch"

  make_no_index_patch "$base" "$target" "$patch_path"
  if [[ ! -s "$patch_path" ]]; then
    return 0
  fi
  sed -i \
    -e "s|a/$base|a/$rel_path|g" \
    -e "s|b/$target|b/$rel_path|g" \
    -e "s|a/${base#/}|a/$rel_path|g" \
    -e "s|b/${target#/}|b/$rel_path|g" \
    "$patch_path"
  git apply --cached --check "$patch_path"
  git apply --cached "$patch_path"
}

stage_shared_line() {
  local path="$1"
  local marker="$2"
  local addition="$3"
  local tmp_dir="$4"
  local base="$tmp_dir/$(printf '%s' "$path" | tr '/ ' '__').base"
  local target="$tmp_dir/$(printf '%s' "$path" | tr '/ ' '__').target"

  assert_current_line "$path" "$addition"
  git show "HEAD:$path" >"$base"
  awk -v marker="$marker" -v addition="$addition" '
    { print }
    !inserted && index($0, marker) == 1 { print addition; inserted = 1 }
    END { if (!inserted) exit 2 }
  ' "$base" >"$target" || die "could not build the $path patch"
  apply_cached_patch "$path" "$base" "$target" "$tmp_dir"
}

extract_section() {
  local path="$1"
  local start="$2"
  local end="$3"
  local output="$4"

  awk -v start="$start" -v end="$end" '
    index($0, start) == 1 { capture = 1 }
    capture && index($0, end) == 1 { exit }
    capture { print }
    END { if (!capture) exit 2 }
  ' "$path" >"$output" || die "could not extract the expected section from $path"
  [[ -s "$output" ]] || die "the extracted section from $path is empty"
}

stage_inserted_section() {
  local path="$1"
  local start="$2"
  local end="$3"
  local tmp_dir="$4"
  local safe_name
  safe_name="$(printf '%s' "$path" | tr '/ ' '__')"
  local base="$tmp_dir/$safe_name.base"
  local target="$tmp_dir/$safe_name.target"
  local section="$tmp_dir/$safe_name.section"

  extract_section "$path" "$start" "$end" "$section"
  git show "HEAD:$path" >"$base"
  awk -v end="$end" -v section="$section" '
    index($0, end) == 1 && !inserted {
      while ((getline line < section) > 0) print line
      close(section)
      inserted = 1
    }
    { print }
    END { if (!inserted) exit 2 }
  ' "$base" >"$target" || die "could not build the $path section patch"
  apply_cached_patch "$path" "$base" "$target" "$tmp_dir"
}

stage_makefile_section() {
  local tmp_dir="$1"
  local base="$tmp_dir/Makefile.base"
  local target="$tmp_dir/Makefile.target"
  local section="$tmp_dir/Makefile.section"

  awk -v start="$make_section_start" -v end="$make_section_end" '
    index($0, start) == 1 { capture = 1 }
    capture { print }
    capture && index($0, end) == 1 { found = 1; exit }
    END { if (!found) exit 2 }
  ' Makefile >"$section" || die "could not extract the Makefile feature block"
  [[ -s "$section" ]] || die "the extracted Makefile feature block is empty"
  git show HEAD:Makefile >"$base"
  {
    cat "$base"
    cat "$section"
  } >"$target"
  apply_cached_patch Makefile "$base" "$target" "$tmp_dir"
}

stage_feature() {
  assert_no_feature_paths_staged

  local tmp_dir
  tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/hatrie-c154-delivery.XXXXXX")"
  trap 'rm -rf "$tmp_dir"' RETURN

  git add -- "${new_paths[@]}"
  stage_shared_line CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md "$audit_marker" "$audit_addition" "$tmp_dir"
  stage_inserted_section ADOPTED_QUERY_ENGINE_IDEAS.md "$adopted_section_start" "$adopted_section_end" "$tmp_dir"
  stage_inserted_section BENCHMARK.md "$benchmark_section_start" "$benchmark_section_end" "$tmp_dir"
  stage_makefile_section "$tmp_dir"

  git diff --cached --check
  git diff --cached --stat -- "${feature_paths[@]}"
  git diff --cached --name-status -- "${feature_paths[@]}"
}

status_feature() {
  git status --short --branch
  printf '\nFeature paths:\n'
  git status --short -- "${feature_paths[@]}"
  printf '\nFeature paths staged:\n'
  git diff --cached --name-status -- "${feature_paths[@]}"
}

unstage_feature() {
  git restore --staged -- "${feature_paths[@]}"
}

commit_feature() {
  git diff --cached --check
  git commit -m "feat: bind rolling schema to replication contracts [skip ci]"
}

push_feature() {
  git push origin HEAD
}

case "${1:-status}" in
  status)
    status_feature
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
    die "usage: $0 {status|stage|commit|push|deliver}"
    ;;
esac
