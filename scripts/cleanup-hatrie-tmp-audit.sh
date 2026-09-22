#!/usr/bin/env bash
set -euo pipefail

mode="${1:-plan}"
plan_file="${HATRIE_TMP_AUDIT_PLAN:-/tmp/.hatrie-cache-tmp-audit.plan}"

if [[ "$mode" != "plan" && "$mode" != "apply" ]]; then
  printf 'usage: %s [plan|apply]\n' "$0" >&2
  exit 2
fi

if [[ ! -f "$plan_file" ]]; then
  printf 'No Hatrie audit plan exists: %s\n' "$plan_file"
  exit 0
fi

count=0
while IFS= read -r candidate; do
  [[ -z "$candidate" ]] && continue
  [[ "$candidate" == \#* ]] && continue

  case "$candidate" in
    /tmp/hatrie*) ;;
    *)
      printf 'Refusing unexpected audit path: %s\n' "$candidate" >&2
      exit 1
      ;;
  esac

  if [[ "$candidate" == "$plan_file" || "$candidate" == "/tmp" || "$candidate" == "/" ]]; then
    printf 'Refusing protected audit path: %s\n' "$candidate" >&2
    exit 1
  fi

  if [[ -e "$candidate/.git" ]]; then
    printf 'PROTECTED .git %s\n' "$candidate"
    continue
  fi

  if [[ ! -e "$candidate" && ! -L "$candidate" ]]; then
    printf 'MISSING %s\n' "$candidate"
    continue
  fi

  count=$((count + 1))
  if [[ "$mode" == "plan" ]]; then
    printf 'REMOVE %s\n' "$candidate"
    continue
  fi

  if [[ -d "$candidate" && ! -L "$candidate" ]]; then
    rm -rf -- "$candidate"
  else
    rm -f -- "$candidate"
  fi
  printf 'REMOVED %s\n' "$candidate"
done < "$plan_file"

printf '%s %d candidate(s).\n' "$mode" "$count"
