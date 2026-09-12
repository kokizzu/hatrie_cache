#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

feature_files=(
  README.md
  PRODUCT_IDEA_GAPS.md
  PEER_STREAM.md
  hat/hatPeer/peer_stream.go
  hat/hatPeer/peer_stream_test.go
  hat/hatPeer/peer_stream_benchmark_test.go
  scripts/format-t-u29-stream.sh
  scripts/test-t-u29-stream.sh
  scripts/test-t-u29-package.sh
  scripts/race-t-u29-stream.sh
  scripts/vet-t-u29-stream.sh
  scripts/benchmark-t-u29-stream.sh
  scripts/publish-t-u29-stream.sh
)

for path in "${feature_files[@]}"; do
  if [[ ! -f "$repo_root/$path" ]]; then
    printf 'missing feature file: %s\n' "$path" >&2
    exit 1
  fi
done

git fetch origin master
base_revision="$(git rev-parse origin/master)"
worktree="$(mktemp -d "${TMPDIR:-/tmp}/hatrie-t-u29.XXXXXX")"
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

if ! rg -q '^format-t-u29-stream:' "$worktree/Makefile"; then
  printf '%s\n' \
    '.PHONY: format-t-u29-stream' \
    'format-t-u29-stream:' \
    $'\t@bash scripts/format-t-u29-stream.sh' \
    '.PHONY: test-t-u29-stream' \
    'test-t-u29-stream:' \
    $'\t@bash scripts/test-t-u29-stream.sh' \
    '.PHONY: test-t-u29-package' \
    'test-t-u29-package:' \
    $'\t@bash scripts/test-t-u29-package.sh' \
    '.PHONY: race-t-u29-stream' \
    'race-t-u29-stream:' \
    $'\t@bash scripts/race-t-u29-stream.sh' \
    '.PHONY: vet-t-u29-stream' \
    'vet-t-u29-stream:' \
    $'\t@bash scripts/vet-t-u29-stream.sh' \
    '.PHONY: benchmark-t-u29-stream' \
    'benchmark-t-u29-stream:' \
    $'\t@bash scripts/benchmark-t-u29-stream.sh' \
    '.PHONY: publish-t-u29-stream' \
    'publish-t-u29-stream:' \
    $'\t@bash scripts/publish-t-u29-stream.sh' >> "$worktree/Makefile"
fi

gofmt -w \
  "$worktree/hat/hatPeer/peer_stream.go" \
  "$worktree/hat/hatPeer/peer_stream_test.go" \
  "$worktree/hat/hatPeer/peer_stream_benchmark_test.go"

go test -C "$worktree" ./hat/hatPeer -run '^TestCompactPeerStream' -count=1
go test -C "$worktree" ./hat/hatPeer -count=1
go test -C "$worktree" -race ./hat/hatPeer -run '^TestCompactPeerStream' -count=1
go vet -C "$worktree" ./hat/hatPeer
go test -C "$worktree" ./hat/hatPeer -run '^$' -bench '^Benchmark(CompactPeerSessionCall|CompactPeerStreamCall)$' -benchmem -count=3

if [[ -f "$worktree/audit-product-idea-gaps.sh" ]]; then
  (cd "$worktree" && bash audit-product-idea-gaps.sh)
fi
go test -C "$worktree" ./... -timeout 90s -p 1 -skip '^TestRunRestoreRehearsalVerifiesBackupPath$' -count=1
git -C "$worktree" diff --check

git -C "$worktree" add -- \
  Makefile \
  README.md \
  PRODUCT_IDEA_GAPS.md \
  PEER_STREAM.md \
  hat/hatPeer/peer_stream.go \
  hat/hatPeer/peer_stream_test.go \
  hat/hatPeer/peer_stream_benchmark_test.go \
  scripts/format-t-u29-stream.sh \
  scripts/test-t-u29-stream.sh \
  scripts/test-t-u29-package.sh \
  scripts/race-t-u29-stream.sh \
  scripts/vet-t-u29-stream.sh \
  scripts/benchmark-t-u29-stream.sh \
  scripts/publish-t-u29-stream.sh
git -C "$worktree" diff --cached --check
git -C "$worktree" commit -m 'feat(peer): add transaction streams'
git -C "$worktree" push origin HEAD:master
