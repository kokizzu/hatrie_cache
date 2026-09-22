#!/usr/bin/env bash
set -euo pipefail

plan=".hatrie-tmp-cleanup.plan"
test_plan="/tmp/hatrie-cache-test-tmp.plan"
mode="${1:-preview}"

plan_has_actions() {
  local line
  while IFS= read -r line; do
    case "$line" in
      ""|\#*)
        ;;
      *)
        return 0
        ;;
    esac
  done < "$plan"
  return 1
}

show_plan() {
  local path="$1"
  if [ -f "$path" ]; then
    while IFS= read -r line; do
      printf '%s\n' "$line"
    done < "$path"
  else
    printf 'no cleanup plan present: %s\n' "$path"
  fi
}

case "$mode" in
  show)
    show_plan "$plan"
    ;;
  show-test)
    show_plan "$test_plan"
    ;;
  preview)
    if [ -f "$plan" ]; then
      if plan_has_actions; then
        printf 'non-empty Hatrie cleanup plan requires review: %s\n' "$plan"
      else
        printf 'empty Hatrie cleanup plan may be removed: %s\n' "$plan"
      fi
    else
      printf 'no Hatrie cleanup plan present\n'
    fi
    ;;
  apply)
    if [ -f "$plan" ]; then
      if plan_has_actions; then
        printf 'refusing to remove non-empty Hatrie cleanup plan: %s\n' "$plan" >&2
        exit 1
      fi
      rm -f "$plan"
      printf 'removed empty Hatrie cleanup plan: %s\n' "$plan"
    else
      printf 'no Hatrie cleanup plan to remove\n'
    fi
    ;;
  *)
    printf 'unknown cleanup mode: %s\n' "$mode" >&2
    exit 2
    ;;
esac
