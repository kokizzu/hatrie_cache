#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

feature_files=(
  README.md
  PRODUCT_IDEA_GAPS.md
  FRONTIER_REGISTRY.md
  FRONTIER_SNAPSHOTS.md
  hat/hatPipeline/frontier_snapshot.go
  hat/hatPipeline/frontier_snapshot_test.go
  hat/hatPipeline/frontier_snapshot_benchmark_test.go
  scripts/format-m-u09-snapshot.sh
  scripts/test-m-u09-snapshot.sh
  scripts/race-m-u09-snapshot.sh
  scripts/vet-m-u09-snapshot.sh
  scripts/benchmark-m-u09-snapshot.sh
  scripts/publish-m-u09-snapshot.sh
)

for path in "${feature_files[@]}"; do
  if [[ ! -f "$repo_root/$path" ]]; then
    printf 'missing feature file: %s\n' "$path" >&2
    exit 1
  fi
done

git fetch origin master
base_revision="$(git rev-parse origin/master)"
worktree="$(mktemp -d "${TMPDIR:-/tmp}/hatrie-m-u09.XXXXXX")"
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

if ! rg -q '^format-m-u09-snapshot:' "$worktree/Makefile"; then
  printf '%s\n' \
    '.PHONY: format-m-u09-snapshot' \
    'format-m-u09-snapshot:' \
    $'\t@bash scripts/format-m-u09-snapshot.sh' \
    '.PHONY: test-m-u09-snapshot' \
    'test-m-u09-snapshot:' \
    $'\t@bash scripts/test-m-u09-snapshot.sh' \
    '.PHONY: race-m-u09-snapshot' \
    'race-m-u09-snapshot:' \
    $'\t@bash scripts/race-m-u09-snapshot.sh' \
    '.PHONY: vet-m-u09-snapshot' \
    'vet-m-u09-snapshot:' \
    $'\t@bash scripts/vet-m-u09-snapshot.sh' \
    '.PHONY: benchmark-m-u09-snapshot' \
    'benchmark-m-u09-snapshot:' \
    $'\t@bash scripts/benchmark-m-u09-snapshot.sh' \
    '.PHONY: publish-m-u09-snapshot' \
    'publish-m-u09-snapshot:' \
    $'\t@bash scripts/publish-m-u09-snapshot.sh' >> "$worktree/Makefile"
fi

gofmt -w \
  "$worktree/hat/hatPipeline/frontier_snapshot.go" \
  "$worktree/hat/hatPipeline/frontier_snapshot_test.go" \
  "$worktree/hat/hatPipeline/frontier_snapshot_benchmark_test.go"

go test -C "$worktree" ./hat/hatPipeline -run '^TestFrontierRegistry(Snapshot|Restore)' -count=1
go test -C "$worktree" ./hat/hatPipeline -count=1
go test -C "$worktree" -race ./hat/hatPipeline -run '^TestFrontierRegistry(Snapshot|Restore)' -count=1
go vet -C "$worktree" ./hat/hatPipeline
go test -C "$worktree" ./hat/hatPipeline -run '^$' -bench '^BenchmarkFrontierRegistry((Marshal|Decode)(JSON)?Snapshot)$' -benchmem -count=3

if [[ -f "$worktree/audit-product-idea-gaps.sh" ]]; then
  (cd "$worktree" && bash audit-product-idea-gaps.sh)
fi
go test -C "$worktree" ./... -timeout 90s -p 1 -skip '^TestRunRestoreRehearsalVerifiesBackupPath$' -count=1
git -C "$worktree" diff --check

git -C "$worktree" add -- \
  Makefile \
  README.md \
  PRODUCT_IDEA_GAPS.md \
  FRONTIER_REGISTRY.md \
  FRONTIER_SNAPSHOTS.md \
  hat/hatPipeline/frontier_snapshot.go \
  hat/hatPipeline/frontier_snapshot_test.go \
  hat/hatPipeline/frontier_snapshot_benchmark_test.go \
  scripts/format-m-u09-snapshot.sh \
  scripts/test-m-u09-snapshot.sh \
  scripts/race-m-u09-snapshot.sh \
  scripts/vet-m-u09-snapshot.sh \
  scripts/benchmark-m-u09-snapshot.sh \
  scripts/publish-m-u09-snapshot.sh
git -C "$worktree" diff --cached --check
git -C "$worktree" commit -m 'feat(pipeline): add durable frontier snapshots'
git -C "$worktree" push origin HEAD:master
