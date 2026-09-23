#!/usr/bin/env bash
set -euo pipefail

allowed='^(BENCHMARK\.md|INSPIRATION_ROUND2\.md|Makefile|README\.md|T223_FUNCTIONAL_INDEX\.md|scripts/(benchmark-t223|commit-t223|format-t223|push-t223|race-t223|stage-t223|test-t223|verify-t223-scope|vet-t223)\.sh|scripts/(commit-c237-projection|inspect-c237-row-mapping|push-c237-projection|stage-c237-projection)\.sh)$'
paths="$(git status --short | awk '{print substr($0, 4)}')"
bad=0
while IFS= read -r path; do
  [ -z "$path" ] && continue
  if ! printf '%s\n' "$path" | grep -Eq "$allowed"; then
    printf 'Unexpected T223 path: %s\n' "$path" >&2
    bad=1
  fi
done <<< "$paths"
if [ "$bad" -ne 0 ]; then
  exit 1
fi
printf '%s\n' 'T223 scope verified.'
