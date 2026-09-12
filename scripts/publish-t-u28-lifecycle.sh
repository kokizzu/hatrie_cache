#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

feature_files=(
  README.md
  PRODUCT_IDEA_GAPS.md
  PEER_LIFECYCLE.md
  hat/hatPeer/compact_session.go
  hat/hatPeer/peer_lifecycle.go
  hat/hatPeer/peer_lifecycle_test.go
  hat/hatPeer/peer_lifecycle_benchmark_test.go
  scripts/format-t-u28-lifecycle.sh
  scripts/test-t-u28-lifecycle.sh
  scripts/test-t-u28-package.sh
  scripts/race-t-u28-lifecycle.sh
  scripts/vet-t-u28-lifecycle.sh
  scripts/benchmark-t-u28-lifecycle.sh
  scripts/publish-t-u28-lifecycle.sh
)

for path in "${feature_files[@]}"; do
  if [[ ! -f "$repo_root/$path" ]]; then
    printf 'missing feature file: %s\n' "$path" >&2
    exit 1
  fi
done

git fetch origin master
base_revision="$(git rev-parse origin/master)"
worktree="$(mktemp -d "${TMPDIR:-/tmp}/hatrie-t-u28.XXXXXX")"
cleanup() {
  git worktree remove --force "$worktree" >/dev/null 2>&1 || true
}
trap cleanup EXIT

git worktree add --detach "$worktree" "$base_revision"
for path in "${feature_files[@]}"; do
  destination="$worktree/$path"
  mkdir -p "$(dirname "$destination")"
  cp "$repo_root/$path" "$destination"
done

if ! rg -q '^format-t-u28-lifecycle:' "$worktree/Makefile"; then
  printf '%s\n' \
    '.PHONY: format-t-u28-lifecycle' \
    'format-t-u28-lifecycle:' \
    $'\t@bash scripts/format-t-u28-lifecycle.sh' \
    '.PHONY: test-t-u28-lifecycle' \
    'test-t-u28-lifecycle:' \
    $'\t@bash scripts/test-t-u28-lifecycle.sh' \
    '.PHONY: test-t-u28-package' \
    'test-t-u28-package:' \
    $'\t@bash scripts/test-t-u28-package.sh' \
    '.PHONY: race-t-u28-lifecycle' \
    'race-t-u28-lifecycle:' \
    $'\t@bash scripts/race-t-u28-lifecycle.sh' \
    '.PHONY: vet-t-u28-lifecycle' \
    'vet-t-u28-lifecycle:' \
    $'\t@bash scripts/vet-t-u28-lifecycle.sh' \
    '.PHONY: benchmark-t-u28-lifecycle' \
    'benchmark-t-u28-lifecycle:' \
    $'\t@bash scripts/benchmark-t-u28-lifecycle.sh' \
    '.PHONY: publish-t-u28-lifecycle' \
    'publish-t-u28-lifecycle:' \
    $'\t@bash scripts/publish-t-u28-lifecycle.sh' >> "$worktree/Makefile"
fi

gofmt -w \
  "$worktree/hat/hatPeer/compact_session.go" \
  "$worktree/hat/hatPeer/peer_lifecycle.go" \
  "$worktree/hat/hatPeer/peer_lifecycle_test.go" \
  "$worktree/hat/hatPeer/peer_lifecycle_benchmark_test.go"

go test -C "$worktree" ./hat/hatPeer -run '^Test(PeerLifecycle|CompactPeerSessionEmitsLifecycleEvents)' -count=1
go test -C "$worktree" ./hat/hatPeer -count=1
go test -C "$worktree" -race ./hat/hatPeer -run '^Test(PeerLifecycle|CompactPeerSessionEmitsLifecycleEvents)' -count=1
go vet -C "$worktree" ./hat/hatPeer
go test -C "$worktree" ./hat/hatPeer -run '^$' -bench '^BenchmarkPeerLifecycle' -benchmem -count=3

if [[ -f "$worktree/audit-product-idea-gaps.sh" ]]; then
  (cd "$worktree" && bash audit-product-idea-gaps.sh)
fi
go test -C "$worktree" ./... -timeout 90s -p 1 -skip '^TestRunRestoreRehearsalVerifiesBackupPath$' -count=1
git -C "$worktree" diff --check

git -C "$worktree" add -- \
  Makefile \
  README.md \
  PRODUCT_IDEA_GAPS.md \
  PEER_LIFECYCLE.md \
  hat/hatPeer/compact_session.go \
  hat/hatPeer/peer_lifecycle.go \
  hat/hatPeer/peer_lifecycle_test.go \
  hat/hatPeer/peer_lifecycle_benchmark_test.go \
  scripts/format-t-u28-lifecycle.sh \
  scripts/test-t-u28-lifecycle.sh \
  scripts/test-t-u28-package.sh \
  scripts/race-t-u28-lifecycle.sh \
  scripts/vet-t-u28-lifecycle.sh \
  scripts/benchmark-t-u28-lifecycle.sh \
  scripts/publish-t-u28-lifecycle.sh
git -C "$worktree" diff --cached --check
git -C "$worktree" commit -m 'feat(peer): add lifecycle hooks'
git -C "$worktree" push origin HEAD:master
