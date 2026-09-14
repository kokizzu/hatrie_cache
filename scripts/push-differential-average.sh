#!/usr/bin/env bash
set -euo pipefail

remote_head=$(git symbolic-ref --short refs/remotes/origin/HEAD 2>/dev/null || true)
if [[ "$remote_head" == origin/* ]]; then
  branch=${remote_head#origin/}
else
  branch=master
fi

git push origin "HEAD:refs/heads/$branch"
