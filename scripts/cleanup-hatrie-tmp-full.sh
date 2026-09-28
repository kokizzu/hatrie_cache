#!/usr/bin/env bash
set -euo pipefail

readonly tmp_root="${TMPDIR:-/tmp}"
readonly repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly plan_file="${HATRIE_TMP_PLAN_FILE:-${repo_root}/.hatrie-tmp-full-cleanup.plan}"
readonly min_age_seconds="${HATRIE_TMP_MIN_AGE_SECONDS:-86400}"

usage() {
  printf 'usage: %s plan|apply|audit\n' "$0"
}

is_hatrie_name() {
  local path="$1"
  local name
  name="$(basename "$path")"
  [[ "$name" =~ [Hh][Aa][Tt][Rr][Ii][Ee] ]] ||
    [[ "$name" =~ [Hh][Aa][Tt][Rr][Ii] ]] ||
    [[ "$name" =~ [Hh][Aa][Tt]-[Tt][Rr][Ii][Ee] ]]
}

is_protected() {
  local path="$1"
  [[ -e "$path/.git" ]] || [[ -e "$path/.hatrie-keep" ]]
}

age_seconds() {
  local path="$1"
  local modified
  modified="$(stat -c '%Y' -- "$path")"
  printf '%s\n' "$(( $(date +%s) - modified ))"
}

size_bytes() {
  du -sx --bytes -- "$1" | cut -f1
}

describe_path() {
  local path="$1"
  local age size kind
  age="$(age_seconds "$path")"
  size="$(size_bytes "$path")"
  if [[ -d "$path" ]]; then
    kind=dir
  else
    kind=file
  fi
  printf '%s\t%s\t%s\t%s\n' "$kind" "$age" "$size" "$path"
}

list_candidates() {
  local path
  while IFS= read -r -d '' path; do
    is_hatrie_name "$path" || continue
    is_protected "$path" && continue
    [[ "$(age_seconds "$path")" -ge "$min_age_seconds" ]] || continue
    describe_path "$path"
  done < <(find -P "$tmp_root" -mindepth 1 -maxdepth 1 -print0)
}

list_inventory() {
  local path
  printf 'Hatrie /tmp inventory: %s\n' "$tmp_root"
  printf 'Protected rule: any candidate containing .git or .hatrie-keep is never deleted.\n'
  printf 'Candidate rule: top-level Hatrie-named entries older than %ss.\n' "$min_age_seconds"
  printf 'Review-only rule: generic go-build* entries are listed but never deleted.\n'
  while IFS= read -r -d '' path; do
    is_hatrie_name "$path" || continue
    if is_protected "$path"; then
      printf 'PROTECTED\t%s\n' "$path"
    else
      describe_path "$path"
    fi
  done < <(find -P "$tmp_root" -mindepth 1 -maxdepth 1 -print0)
  while IFS= read -r -d '' path; do
    [[ "$(basename "$path")" == go-build* ]] || continue
    describe_path "$path"
  done < <(find -P "$tmp_root" -mindepth 1 -maxdepth 1 -print0)
}

write_plan() {
  local candidate_count=0
  : > "$plan_file"
  while IFS= read -r line; do
    printf '%s\n' "$line" >> "$plan_file"
    candidate_count=$((candidate_count + 1))
  done < <(list_candidates)
  printf 'Plan: %s\n' "$plan_file"
  printf 'Stale candidate count: %s\n' "$candidate_count"
  if [[ "$candidate_count" -gt 0 ]]; then
    printf 'kind\tage_seconds\tsize_bytes\tpath\n'
    printf '%s\n' "$(<"$plan_file")"
  else
    rm -f -- "$plan_file"
    printf 'Plan: none\n'
  fi
}

apply_plan() {
  [[ -f "$plan_file" ]] || {
    printf 'No cleanup plan found at %s; run the plan target first.\n' "$plan_file" >&2
    exit 1
  }
  local kind age size path current_age
  while IFS=$'\t' read -r kind age size path; do
    [[ -n "$path" ]] || continue
    [[ "$path" == "$tmp_root"/* ]] || {
      printf 'Refusing path outside TMPDIR: %s\n' "$path" >&2
      exit 1
    }
    [[ "$(dirname "$path")" == "$tmp_root" ]] || {
      printf 'Refusing non-top-level path: %s\n' "$path" >&2
      exit 1
    }
    is_hatrie_name "$path" || {
      printf 'Refusing path that no longer matches Hatrie rule: %s\n' "$path" >&2
      exit 1
    }
    [[ -e "$path" ]] || continue
    is_protected "$path" && {
      printf 'Refusing protected path: %s\n' "$path" >&2
      exit 1
    }
    current_age="$(age_seconds "$path")"
    [[ "$current_age" -ge "$min_age_seconds" ]] || {
      printf 'Refusing path that became recent: %s\n' "$path" >&2
      exit 1
    }
    printf 'DELETE\t%s\n' "$path"
    rm -rf --one-file-system -- "$path"
  done < "$plan_file"
  rm -f -- "$plan_file"
  printf 'Cleanup complete.\n'
}

self_test() {
  local fixture stale recent protected generic output plan_content
  fixture="$(mktemp -d "${tmp_root%/}/hatrie-cache-cleanup-self-test.XXXXXX")"
  trap 'rm -rf -- "${fixture:-}"' EXIT
  stale="${fixture}/hatrie-cache-cleanup-stale"
  recent="${fixture}/hatrie-cache-cleanup-recent"
  protected="${fixture}/hatrie-cache-cleanup-protected"
  generic="${fixture}/go-build-cleanup-review"
  mkdir -p -- "$stale" "$recent" "$protected/.git" "$generic"
  touch -d '2 days ago' -- "$stale"
  output="$(TMPDIR="$fixture" HATRIE_TMP_MIN_AGE_SECONDS=86400 HATRIE_TMP_PLAN_FILE="${fixture}/plan" bash "$0" plan)"
  [[ "$output" == *"$stale"* ]] || {
    printf 'Self-test expected stale candidate: %s\n' "$stale" >&2
    exit 1
  }
  plan_content="$(<"${fixture}/plan")"
  [[ "$plan_content" == *"$stale"* ]] || {
    printf 'Self-test did not write stale path to plan: %s\n' "$stale" >&2
    exit 1
  }
  [[ "$plan_content" != *"$recent"* ]] || {
    printf 'Self-test unexpectedly planned recent path: %s\n' "$recent" >&2
    exit 1
  }
  [[ "$plan_content" != *"$protected"* ]] || {
    printf 'Self-test unexpectedly planned protected path: %s\n' "$protected" >&2
    exit 1
  }
  [[ -d "$generic" ]] || {
    printf 'Self-test lost generic review path: %s\n' "$generic" >&2
    exit 1
  }
  TMPDIR="$fixture" HATRIE_TMP_MIN_AGE_SECONDS=86400 HATRIE_TMP_PLAN_FILE="${fixture}/plan" bash "$0" apply >/dev/null
  [[ ! -e "$stale" ]] || {
    printf 'Self-test failed to remove stale path: %s\n' "$stale" >&2
    exit 1
  }
  [[ -d "$recent" && -d "$protected" && -d "$generic" ]] || {
    printf 'Self-test removed a path that should have been preserved.\n' >&2
    exit 1
  }
  printf 'Hatrie /tmp cleanup self-test passed.\n'
}

main() {
  [[ "$#" -eq 1 ]] || {
    usage >&2
    exit 2
  }
  case "$1" in
    plan)
      list_inventory
      write_plan
      ;;
    audit)
      list_inventory
      printf 'Stale cleanup candidates: %s\n' "$(list_candidates | wc -l)"
      ;;
    apply)
      apply_plan
      ;;
    self-test)
      self_test
      ;;
    *)
      usage >&2
      exit 2
      ;;
  esac
}

main "$@"
