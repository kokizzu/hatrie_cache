#!/usr/bin/env bash
set -euo pipefail

paths=(
  hat/hatBackup/chain.go
  hat/hatBackup/chain_test.go
  T22_INCREMENTAL_BACKUP_CHAIN.md
  scripts/format-t-g22-backup-chain.sh
  scripts/test-t-g22-backup-chain.sh
  scripts/test-t-g22-backup-chain-package.sh
  scripts/race-t-g22-backup-chain.sh
  scripts/vet-t-g22-backup-chain.sh
  scripts/benchmark-t-g22-backup-chain.sh
  scripts/commit-t-g22-backup-chain.sh
  scripts/publish-t-g22-backup-chain.sh
)

git add -- "${paths[@]}"
git commit --only -m "feat(backup): validate incremental chains" -- "${paths[@]}"
