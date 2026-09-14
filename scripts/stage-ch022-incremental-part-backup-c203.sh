#!/usr/bin/env bash
set -euo pipefail

git add \
  README.md \
  ENGINE_IDEAS.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  CH022_INCREMENTAL_PART_BACKUP.md \
  hat/hatBackup/model.go \
  hat/hatBackup/chain.go \
  hat/hatBackup/encryption.go \
  hat/hatBackup/object_store.go \
  hat/hatBackup/ch022_incremental_part_backup_test.go \
  hat/hatBackup/ch022_incremental_part_backup_benchmark_test.go \
  scripts/inspect-ch022-incremental-backup-c203.sh \
  scripts/test-ch022-incremental-part-backup-c203.sh \
  scripts/format-ch022-incremental-part-backup-c203.sh \
  scripts/benchmark-ch022-incremental-part-backup-c203.sh \
  scripts/verify-ch022-incremental-part-backup-c203.sh \
  scripts/stage-ch022-incremental-part-backup-c203.sh \
  scripts/inspect-staged-ch022-incremental-part-backup-c203.sh \
  scripts/commit-ch022-incremental-part-backup-c203.sh \
  scripts/push-ch022-incremental-part-backup-c203.sh

staged_makefile=$(mktemp)
trap 'rm -f "$staged_makefile"' EXIT
git show HEAD:Makefile > "$staged_makefile"
printf '\n\n.PHONY: inspect-ch022-incremental-backup-c203\ninspect-ch022-incremental-backup-c203:\n\t@bash scripts/inspect-ch022-incremental-backup-c203.sh\n\n.PHONY: test-ch022-incremental-part-backup-c203\ntest-ch022-incremental-part-backup-c203:\n\t@bash scripts/test-ch022-incremental-part-backup-c203.sh\n\n.PHONY: format-ch022-incremental-part-backup-c203\nformat-ch022-incremental-part-backup-c203:\n\t@bash scripts/format-ch022-incremental-part-backup-c203.sh\n\n.PHONY: benchmark-ch022-incremental-part-backup-c203\nbenchmark-ch022-incremental-part-backup-c203:\n\t@bash scripts/benchmark-ch022-incremental-part-backup-c203.sh\n\n.PHONY: verify-ch022-incremental-part-backup-c203\nverify-ch022-incremental-part-backup-c203:\n\t@bash scripts/verify-ch022-incremental-part-backup-c203.sh\n\n.PHONY: stage-ch022-incremental-part-backup-c203\nstage-ch022-incremental-part-backup-c203:\n\t@bash scripts/stage-ch022-incremental-part-backup-c203.sh\n\n.PHONY: inspect-staged-ch022-incremental-part-backup-c203\ninspect-staged-ch022-incremental-part-backup-c203:\n\t@bash scripts/inspect-staged-ch022-incremental-part-backup-c203.sh\n\n.PHONY: commit-ch022-incremental-part-backup-c203\ncommit-ch022-incremental-part-backup-c203:\n\t@bash scripts/commit-ch022-incremental-part-backup-c203.sh\n\n.PHONY: push-ch022-incremental-part-backup-c203\npush-ch022-incremental-part-backup-c203:\n\t@bash scripts/push-ch022-incremental-part-backup-c203.sh\n' >> "$staged_makefile"
staged_makefile_blob=$(git hash-object -w "$staged_makefile")
git update-index --cacheinfo 100644,"$staged_makefile_blob",Makefile
git diff --cached --check
