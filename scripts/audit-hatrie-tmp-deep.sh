#!/usr/bin/env bash
set -euo pipefail

mode="${1:-preview}"
tmp_root="${TMPDIR:-/tmp}"
plan_file="$tmp_root/hatrie-cache-deep-cleanup.plan"

case "$mode" in
  preview|apply)
    ;;
  *)
    printf 'usage: %s [preview|apply]\n' "$0" >&2
    exit 2
    ;;
esac

declare -a protected_roots=()
shopt -s nullglob
for candidate in "$tmp_root"/hatrie-cache-*; do
  if [[ -e "$candidate/.git" ]]; then
    protected_roots+=("$candidate")
  fi
done

is_protected() {
  local candidate="$1"
  local root
  for root in "${protected_roots[@]}"; do
    if [[ "$candidate" == "$root" || "$candidate" == "$root"/* ]]; then
      return 0
    fi
  done
  return 1
}

is_hatrie_candidate() {
  local name="$(basename "$1")"
  case "$name" in
    *hatrie*|*Hatrie*|hatri*|hatrie-build*)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

is_review_only_candidate() {
  local name="$(basename "$1")"
  case "$name" in
    go-build*|Test*|test-*)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

format_size() {
  du -sh -- "$1" 2>/dev/null | cut -f1 || printf 'unknown'
}

if [[ "$mode" == "preview" ]]; then
  : > "$plan_file"
fi

printf 'Deep /tmp Hatrie/build inventory: %s\n' "$tmp_root"
printf 'Protected roots: %s\n' "${#protected_roots[@]}"
for root in "${protected_roots[@]}"; do
  printf '  KEEP protected worktree: %s (%s)\n' "$root" "$(format_size "$root")"
done

candidate_count=0
review_only_count=0
plan_count=0
while IFS= read -r -d '' candidate; do
  if [[ "$candidate" == "$plan_file" ]]; then
    continue
  fi
  if is_protected "$candidate"; then
    continue
  fi
  if is_hatrie_candidate "$candidate"; then
    candidate_count=$((candidate_count + 1))
    printf 'CANDIDATE removable Hatrie path: %s (%s)\n' "$candidate" "$(format_size "$candidate")"
    if [[ "$mode" == "preview" ]]; then
      printf '%s\n' "$candidate" >> "$plan_file"
      plan_count=$((plan_count + 1))
    fi
    continue
  fi
  if is_review_only_candidate "$candidate"; then
    review_only_count=$((review_only_count + 1))
    printf 'REVIEW-ONLY generic/test path: %s (%s)\n' "$candidate" "$(format_size "$candidate")"
  fi
done < <(find "$tmp_root" -mindepth 1 -maxdepth 1 \( -type d -o -type f \) -print0 | sort -z)

if [[ "$mode" == "preview" ]]; then
  if [[ "$plan_count" == 0 ]]; then
    rm -f -- "$plan_file"
    printf 'Plan: none\n'
  else
    printf 'Plan: %s (%s path(s))\n' "$plan_file" "$plan_count"
  fi
  printf 'Summary: %s removable Hatrie path(s), %s review-only generic/test path(s).\n' "$candidate_count" "$review_only_count"
  exit 0
fi

if [[ ! -s "$plan_file" ]]; then
  printf 'Apply: no reviewed plan found at %s\n' "$plan_file"
  exit 0
fi

applied=0
while IFS= read -r candidate; do
  [[ -n "$candidate" ]] || continue
  case "$candidate" in
    "$tmp_root"/*)
      ;;
    *)
      printf 'refusing unsafe cleanup path: %s\n' "$candidate" >&2
      exit 1
      ;;
  esac
  if is_protected "$candidate" || ! is_hatrie_candidate "$candidate"; then
    printf 'refusing changed or protected cleanup path: %s\n' "$candidate" >&2
    exit 1
  fi
  rm -rf -- "$candidate"
  applied=$((applied + 1))
  printf 'REMOVED %s\n' "$candidate"
done < "$plan_file"
rm -f -- "$plan_file"
printf 'Apply complete: %s path(s).\n' "$applied"
