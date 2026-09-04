#!/usr/bin/env bash
set -euo pipefail

if git diff --cached --name-only | awk 'END { exit !(NR > 0) }'; then
  true
else
  printf '%s\n' 'refusing to commit T046: no staged changes' >&2
  exit 1
fi

expected='^((INSPIRATION|README)\.md|Makefile|hat/hatCache/(backup_bundle\.go|backup_context(_test)?\.go|backup_repository\.go)|scripts/(format|review|stage|commit|push|test|test-race|vet)-t046\.sh)$'
unexpected=$(git diff --cached --name-only | awk -v pattern="$expected" '$0 !~ pattern {print}')
if [ -n "$unexpected" ]; then
  printf '%s\n' "$unexpected" >&2
  exit 1
fi
git commit -m 'feat(backup): add cancellable backup creation'
