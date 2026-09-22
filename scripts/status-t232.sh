#!/usr/bin/env bash
set -euo pipefail
git status --short
git diff --stat
printf '%s\n' 'Makefile diff:'
git diff -- Makefile
if test -f .hatrie-tmp-cleanup.plan; then
	printf '%s\n' 'Cleanup plan:'
	sed -n '1,120p' .hatrie-tmp-cleanup.plan
fi
