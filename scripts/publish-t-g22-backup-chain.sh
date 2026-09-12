#!/usr/bin/env bash
set -euo pipefail

feature_commit="$(git rev-parse HEAD^{commit})"
remote_dir="$(mktemp -d /tmp/hatrie-cache-t-g22-publish.XXXXXX)"

cleanup() {
  git worktree remove --force "$remote_dir" >/dev/null 2>&1 || true
}
trap cleanup EXIT

git fetch origin master
git worktree add --detach "$remote_dir" origin/master
git -C "$remote_dir" checkout "$feature_commit" -- \
  hat/hatBackup/chain.go \
  hat/hatBackup/chain_test.go \
  T22_INCREMENTAL_BACKUP_CHAIN.md \
  scripts/format-t-g22-backup-chain.sh \
  scripts/test-t-g22-backup-chain.sh \
  scripts/test-t-g22-backup-chain-package.sh \
  scripts/race-t-g22-backup-chain.sh \
  scripts/vet-t-g22-backup-chain.sh \
  scripts/benchmark-t-g22-backup-chain.sh \
  scripts/commit-t-g22-backup-chain.sh \
  scripts/publish-t-g22-backup-chain.sh
# origin/master has one pre-existing CLI runtime-build failure; it is verified
# separately by test-origin-cli-restore-rehearsal.sh.
go -C "$remote_dir" test -p 1 ./... -skip '^TestRunRestoreRehearsalVerifiesBackupPath$' -count=1
git -C "$remote_dir" add -- \
  hat/hatBackup/chain.go \
  hat/hatBackup/chain_test.go \
  T22_INCREMENTAL_BACKUP_CHAIN.md \
  scripts/format-t-g22-backup-chain.sh \
  scripts/test-t-g22-backup-chain.sh \
  scripts/test-t-g22-backup-chain-package.sh \
  scripts/race-t-g22-backup-chain.sh \
  scripts/vet-t-g22-backup-chain.sh \
  scripts/benchmark-t-g22-backup-chain.sh \
  scripts/commit-t-g22-backup-chain.sh \
  scripts/publish-t-g22-backup-chain.sh
git -C "$remote_dir" commit -m "feat(backup): validate incremental chains"
git -C "$remote_dir" push origin HEAD:master
