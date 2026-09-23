#!/usr/bin/env bash
set -euo pipefail

allowed='^(BENCHMARK\.md|INSPIRATION_ROUND2\.md|Makefile|README\.md|T222_MULTIKEY_INDEX\.md|scripts/(benchmark-t222|commit-t222|format-t222|push-t222|race-t222|stage-t222|test-t222|verify-t222-scope|vet-t222)\.sh|scripts/(commit-c237-projection|inspect-c237-row-mapping|push-c237-projection|stage-c237-projection)\.sh)$'
paths="$(git status --short | awk '{print substr($0, 4)}')"
bad=0
while IFS= read -r path; do
  [ -z "$path" ] && continue
  if ! printf '%s\n' "$path" | grep -Eq "$allowed"; then
    printf 'Unexpected T222 path: %s\n' "$path" >&2
    bad=1
  fi
done <<< "$paths"
if [ "$bad" -ne 0 ]; then
  exit 1
fi
printf '%s\n' 'T222 scope verified.'
