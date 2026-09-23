#!/usr/bin/env bash
set -euo pipefail

allowed='^(BENCHMARK\.md|INSPIRATION_ROUND2\.md|Makefile|README\.md|T225_COVERING_INDEX\.md|scripts/(benchmark-t225|commit-t225|format-t225|push-t225|race-t225|stage-t225|test-t225|verify-t225-scope|vet-t225)\.sh|scripts/(commit-c237-projection|inspect-c237-row-mapping|push-c237-projection|stage-c237-projection)\.sh)$'
paths="$(git status --short | awk '{print substr($0, 4)}')"
bad=0
while IFS= read -r path; do
  [ -z "$path" ] && continue
  if ! printf '%s\n' "$path" | grep -Eq "$allowed"; then
    printf 'Unexpected T225 path: %s\n' "$path" >&2
    bad=1
  fi
done <<< "$paths"
if [ "$bad" -ne 0 ]; then
  exit 1
fi
printf '%s\n' 'T225 scope verified.'
