#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

feature_files=(
  README.md
  PRODUCT_IDEA_GAPS.md
  FRONTIER_RETENTION.md
  hat/hatPipeline/frontier_retention.go
  hat/hatPipeline/frontier_retention_test.go
  hat/hatPipeline/frontier_retention_benchmark_test.go
  scripts/format-m-u33-retention.sh
  scripts/test-m-u33-retention.sh
  scripts/test-m-u33-package.sh
  scripts/race-m-u33-retention.sh
  scripts/vet-m-u33-retention.sh
  scripts/benchmark-m-u33-retention.sh
  scripts/publish-m-u33-retention.sh
)

for path in "${feature_files[@]}"; do
  if [[ ! -f "$repo_root/$path" ]]; then
    printf 'missing feature file: %s\n' "$path" >&2
    exit 1
  fi
done

git fetch origin master
base_revision="$(git rev-parse origin/master)"
worktree="$(mktemp -d "${TMPDIR:-/tmp}/hatrie-m-u33.XXXXXX")"
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

if ! rg -q '^format-m-u33-retention:' "$worktree/Makefile"; then
  printf '%s\n' \
    '.PHONY: format-m-u33-retention' \
    'format-m-u33-retention:' \
    $'\t@bash scripts/format-m-u33-retention.sh' \
    '.PHONY: test-m-u33-retention' \
    'test-m-u33-retention:' \
    $'\t@bash scripts/test-m-u33-retention.sh' \
    '.PHONY: test-m-u33-package' \
    'test-m-u33-package:' \
    $'\t@bash scripts/test-m-u33-package.sh' \
    '.PHONY: race-m-u33-retention' \
    'race-m-u33-retention:' \
    $'\t@bash scripts/race-m-u33-retention.sh' \
    '.PHONY: vet-m-u33-retention' \
    'vet-m-u33-retention:' \
    $'\t@bash scripts/vet-m-u33-retention.sh' \
    '.PHONY: benchmark-m-u33-retention' \
    'benchmark-m-u33-retention:' \
    $'\t@bash scripts/benchmark-m-u33-retention.sh' \
    '.PHONY: publish-m-u33-retention' \
    'publish-m-u33-retention:' \
    $'\t@bash scripts/publish-m-u33-retention.sh' >> "$worktree/Makefile"
fi

gofmt -w \
  "$worktree/hat/hatPipeline/frontier_retention.go" \
  "$worktree/hat/hatPipeline/frontier_retention_test.go" \
  "$worktree/hat/hatPipeline/frontier_retention_benchmark_test.go"

go test -C "$worktree" ./hat/hatPipeline -run '^TestFrontierRetention' -count=1
go test -C "$worktree" ./hat/hatPipeline -count=1
go test -C "$worktree" -race ./hat/hatPipeline -run '^TestFrontierRetention' -count=1
go vet -C "$worktree" ./hat/hatPipeline
go test -C "$worktree" ./hat/hatPipeline -run '^$' -bench '^BenchmarkFrontierRetention' -benchmem -count=3

if [[ -f "$worktree/audit-product-idea-gaps.sh" ]]; then
  (cd "$worktree" && bash audit-product-idea-gaps.sh)
fi
go test -C "$worktree" ./... -timeout 90s -p 1 -skip '^TestRunRestoreRehearsalVerifiesBackupPath$' -count=1
git -C "$worktree" diff --check

git -C "$worktree" add -- \
  Makefile \
  README.md \
  PRODUCT_IDEA_GAPS.md \
  FRONTIER_RETENTION.md \
  hat/hatPipeline/frontier_retention.go \
  hat/hatPipeline/frontier_retention_test.go \
  hat/hatPipeline/frontier_retention_benchmark_test.go \
  scripts/format-m-u33-retention.sh \
  scripts/test-m-u33-retention.sh \
  scripts/test-m-u33-package.sh \
  scripts/race-m-u33-retention.sh \
  scripts/vet-m-u33-retention.sh \
  scripts/benchmark-m-u33-retention.sh \
  scripts/publish-m-u33-retention.sh
git -C "$worktree" diff --cached --check
git -C "$worktree" commit -m 'feat(pipeline): add frontier retention leases'
git -C "$worktree" push origin HEAD:master
