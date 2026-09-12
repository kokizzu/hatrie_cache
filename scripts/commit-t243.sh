#!/usr/bin/env bash
set -euo pipefail

git fetch origin master
if ! git merge-base --is-ancestor origin/master HEAD; then
  printf '%s\n' 'publisher worktree is behind or diverged from origin/master' >&2
  exit 1
fi
git add INSPIRATION_ROUND2.md Makefile README.md T243_MTLS_CERTIFICATE_ROTATION.md hat/hatPeer/tls_rotation.go hat/hatPeer/tls_rotation_test.go scripts/benchmark-t243.sh scripts/commit-t243.sh scripts/format-t243.sh scripts/race-t243.sh scripts/test-t243-full.sh scripts/test-t243-package.sh scripts/test-t243-tls-rotation.sh scripts/verify-t243.sh scripts/vet-t243.sh
git diff --cached --check
git diff --cached --stat
git commit -m 'feat(hatPeer): add mutual TLS certificate rotation'
git push origin HEAD:master
git rev-parse HEAD
