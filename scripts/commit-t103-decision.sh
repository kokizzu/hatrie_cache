#!/bin/sh
set -eu

git add INSPIRATION.md scripts/review-t103-decision.sh scripts/commit-t103-decision.sh scripts/push-t103-decision.sh
git diff --cached --check
git commit -m "docs: defer native FFI boundary"
