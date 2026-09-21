#!/usr/bin/env bash
set -euo pipefail

if [[ "${1:-}" == open ]]; then
	printf '%s\n' '--- OPEN ENGINE IDEAS ---'
	awk '/^\| (CH|MZ|TT)-/ && $0 !~ /\[x\]|Implemented|Adopted:/{print}' ENGINE_IDEAS.md
	exit 0
fi
printf '%s\n' '--- INSPIRATION_BACKLOG.md ---'
awk '/^\|/{print}' INSPIRATION_BACKLOG.md
printf '%s\n' '--- ENGINE_IDEAS.md ---'
awk '/^\|/{print}' ENGINE_IDEAS.md
printf '%s\n' '--- OPEN ENGINE IDEAS ---'
awk '/^\| (CH|MZ|TT)-/ && $0 !~ /\[x\]|Implemented|Adopted:/{print}' ENGINE_IDEAS.md
