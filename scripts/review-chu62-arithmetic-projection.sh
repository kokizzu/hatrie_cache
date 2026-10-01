#!/usr/bin/env bash
set -euo pipefail

if test -e hat/hatSql/round21_build_compat.go; then
	printf '%s\n' 'temporary round21 compatibility shim is still present' >&2
	exit 1
fi
if test -e scripts/inspect-round21.sh || test -e scripts/inspect-round21-context.sh || test -e scripts/debug-round21-recovery.sh; then
	printf '%s\n' 'temporary round21 diagnostic artifact is still present' >&2
	exit 1
fi
git diff --check
git status --short
git diff --stat
