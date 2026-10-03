#!/usr/bin/env bash
set -euo pipefail

branch="codex/next-inspiration-round98-chg14-20261003"
[[ "$(git branch --show-current)" == "$branch" ]] || {
  printf '%s\n' "unexpected branch: $(git branch --show-current)" >&2
  exit 1
}
git push -u origin "$branch"
