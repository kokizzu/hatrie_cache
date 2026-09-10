#!/usr/bin/env bash
set -euo pipefail

git add \
  Makefile \
  hat/hatPipeline/pipeline.go \
  scripts/commit-pipeline-cancellation.sh \
  scripts/push-pipeline-cancellation.sh
git diff --cached --check
git commit -m "fix(pipeline): report pre-completion context cancellation"
