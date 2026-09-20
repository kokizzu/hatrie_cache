#!/usr/bin/env bash
set -euo pipefail

if ! git symbolic-ref --quiet --short HEAD >/dev/null; then
	git switch -c codex/next-inspiration-tu30
fi
git push -u origin HEAD
