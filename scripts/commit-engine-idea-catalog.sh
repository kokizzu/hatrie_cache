#!/usr/bin/env bash
set -euo pipefail

git add ENGINE_IDEAS.md Makefile scripts/inspect-engine-ideas.sh scripts/commit-engine-idea-catalog.sh scripts/push-engine-idea-catalog.sh
git commit -m "docs: catalog engine improvement ideas"
