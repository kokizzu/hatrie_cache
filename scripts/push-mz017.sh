#!/usr/bin/env bash
set -euo pipefail

remote_head="$(git symbolic-ref --quiet --short refs/remotes/origin/HEAD 2>/dev/null || printf '%s\n' origin/master)"
remote_branch="${remote_head#origin/}"
git push origin "HEAD:${remote_branch}"
