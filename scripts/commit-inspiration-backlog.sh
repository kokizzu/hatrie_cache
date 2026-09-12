#!/bin/sh
set -eu

git add Makefile INSPIRATION.md INSPIRATION_BACKLOG.md scripts/audit-inspiration-inventory.sh scripts/commit-inspiration-backlog.sh scripts/push-inspiration-backlog.sh scripts/review-inspiration-backlog.sh
git commit -m "docs: add product inspiration backlog"
