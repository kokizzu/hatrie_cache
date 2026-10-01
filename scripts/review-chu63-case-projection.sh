#!/usr/bin/env bash
set -euo pipefail

if test -e hat/hatSql/round22_build_compat.go; then
	printf '%s\n' 'temporary round22 compatibility shim is still present' >&2
	exit 1
fi
if test -e scripts/inspect-round22-case.sh || test -e scripts/inspect-round22-dispatch.sh; then
	printf '%s\n' 'temporary round22 diagnostic artifact is still present' >&2
	exit 1
fi
git diff --check
git status --short
git diff --stat
