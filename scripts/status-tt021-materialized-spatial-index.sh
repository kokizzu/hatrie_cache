#!/usr/bin/env bash
set -euo pipefail

git status --short --untracked-files=all
git diff --stat
