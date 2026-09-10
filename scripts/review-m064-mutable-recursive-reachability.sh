#!/usr/bin/env bash
set -eu

git diff --check
printf '%s\n' '--- changed tracked paths ---'
git diff --name-only
printf '%s\n' '--- changed untracked paths ---'
git ls-files --others --exclude-standard
printf '%s\n' '--- diff stat ---'
git diff --stat
printf '%s\n' '--- adoption documentation diff ---'
git diff -- ADOPTED_QUERY_ENGINE_IDEAS.md
