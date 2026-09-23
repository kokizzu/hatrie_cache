#!/usr/bin/env bash
set -euo pipefail

allowed='^(BENCHMARK\.md|INSPIRATION_ROUND2\.md|Makefile|README\.md|T224_PARTIAL_INDEX\.md|scripts/(benchmark-t224|commit-t224|format-t224|push-t224|race-t224|stage-t224|test-t224|verify-t224-scope|vet-t224)\.sh|scripts/(commit-c237-projection|inspect-c237-row-mapping|push-c237-projection|stage-c237-projection)\.sh)$'
paths="$(git status --short | awk '{print substr($0, 4)}')"
bad=0
while IFS= read -r path; do
  [ -z "$path" ] && continue
  if ! printf '%s\n' "$path" | grep -Eq "$allowed"; then
    printf 'Unexpected T224 path: %s\n' "$path" >&2
    bad=1
  fi
done <<< "$paths"
if [ "$bad" -ne 0 ]; then
  exit 1
fi
printf '%s\n' 'T224 scope verified.'
