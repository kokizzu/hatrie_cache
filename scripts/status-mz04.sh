#!/usr/bin/env bash
set -eu

git diff --check
git status --short
rg -n 'MZ-04|MZ04|CompactionDebt|BlockedByLease' INSPIRATION_BACKLOG.md README.md MZ04_FRONTIER_COMPACTION_METRICS.md hat/hatPipeline/frontier_retention.go
