#!/usr/bin/env bash
set -euo pipefail

git add \
  README.md \
  ENGINE_IDEAS.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  CH007_ROW_TTL.md \
  hat/hatSql/typed_table.go \
  hat/hatSql/typed_table_patch_parts.go \
  hat/hatSql/typed_table_stats.go \
  hat/hatSql/typed_table_histogram.go \
  hat/hatSql/typed_table_ttl.go \
  hat/hatSql/ch007_row_ttl_test.go \
  hat/hatSql/ch007_row_ttl_benchmark_test.go \
  scripts/test-ch007-row-ttl-c203.sh \
  scripts/format-ch007-row-ttl-c203.sh \
  scripts/benchmark-ch007-row-ttl-c203.sh \
  scripts/verify-ch007-row-ttl-c203.sh \
  scripts/inspect-ch007-ttl-c203.sh \
  scripts/stage-ch007-row-ttl-c203.sh \
  scripts/inspect-staged-ch007-row-ttl-c203.sh \
  scripts/commit-ch007-row-ttl-c203.sh \
  scripts/push-ch007-row-ttl-c203.sh

# The worktree contains unrelated Makefile additions from a concurrent session.
# Build the staged Makefile from HEAD and append only this feature's targets.
staged_makefile=$(mktemp)
trap 'rm -f "$staged_makefile"' EXIT
git show HEAD:Makefile > "$staged_makefile"
printf '\n\n.PHONY: test-ch007-row-ttl-c203\ntest-ch007-row-ttl-c203:\n\t@bash scripts/test-ch007-row-ttl-c203.sh\n\n.PHONY: format-ch007-row-ttl-c203\nformat-ch007-row-ttl-c203:\n\t@bash scripts/format-ch007-row-ttl-c203.sh\n\n.PHONY: inspect-ch007-ttl-c203\ninspect-ch007-ttl-c203:\n\t@bash scripts/inspect-ch007-ttl-c203.sh\n\n.PHONY: benchmark-ch007-row-ttl-c203\nbenchmark-ch007-row-ttl-c203:\n\t@bash scripts/benchmark-ch007-row-ttl-c203.sh\n\n.PHONY: verify-ch007-row-ttl-c203\nverify-ch007-row-ttl-c203:\n\t@bash scripts/verify-ch007-row-ttl-c203.sh\n\n.PHONY: stage-ch007-row-ttl-c203\nstage-ch007-row-ttl-c203:\n\t@bash scripts/stage-ch007-row-ttl-c203.sh\n\n.PHONY: inspect-staged-ch007-row-ttl-c203\ninspect-staged-ch007-row-ttl-c203:\n\t@bash scripts/inspect-staged-ch007-row-ttl-c203.sh\n\n.PHONY: commit-ch007-row-ttl-c203\ncommit-ch007-row-ttl-c203:\n\t@bash scripts/commit-ch007-row-ttl-c203.sh\n\n.PHONY: push-ch007-row-ttl-c203\npush-ch007-row-ttl-c203:\n\t@bash scripts/push-ch007-row-ttl-c203.sh\n' >> "$staged_makefile"
staged_makefile_blob=$(git hash-object -w "$staged_makefile")
git update-index --cacheinfo 100644,"$staged_makefile_blob",Makefile
git diff --cached --check
