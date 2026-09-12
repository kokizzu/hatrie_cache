#!/usr/bin/env bash
set -euo pipefail

expected_remote="52395e3e8655ebaf743138bba0d100527bc061d1"
remote_head="$(git ls-remote origin refs/heads/master | cut -f1)"
if [[ "$remote_head" != "$expected_remote" ]]; then
  printf 'refusing to push: expected origin/master %s, got %s\n' "$expected_remote" "$remote_head" >&2
  exit 1
fi
git push origin HEAD:master
