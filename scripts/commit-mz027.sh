#!/usr/bin/env bash
set -eu

git add scripts/commit-mz027.sh scripts/push-mz027.sh
git commit -m "chore: fix MZ027 push target"
