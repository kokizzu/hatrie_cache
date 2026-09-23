#!/usr/bin/env bash
set -euo pipefail

git add -- \
  BENCHMARK.md \
  CH007_GROUPING_SETS.md \
  IDEA_GAP_CATALOG.md \
  Makefile \
  scripts/inspect-ch-g07-context.sh \
  scripts/inspect-idea-gap-catalog.sh \
  scripts/m270-ch-g07-test.sh \
  scripts/m271-ch-g07-benchmark.sh \
  scripts/m272-ch-g07-race.sh \
  scripts/stage-ch-g07-audit.sh \
  scripts/commit-ch-g07-audit.sh \
  scripts/push-ch-g07-audit.sh
git diff --cached --check
git status --short
