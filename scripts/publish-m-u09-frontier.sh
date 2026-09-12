#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
feature_files=(
	README.md
	PRODUCT_IDEA_GAPS.md
	FRONTIER_REGISTRY.md
	hat/hatPipeline/frontier_registry.go
	hat/hatPipeline/frontier_registry_test.go
	hat/hatPipeline/frontier_registry_benchmark_test.go
	scripts/format-m-u09-frontier.sh
	scripts/test-m-u09-frontier.sh
	scripts/test-m-u09-package.sh
	scripts/race-m-u09-frontier.sh
	scripts/vet-m-u09-frontier.sh
	scripts/benchmark-m-u09-frontier.sh
	scripts/publish-m-u09-frontier.sh
)

git -C "$root" fetch origin master
base=$(git -C "$root" rev-parse origin/master)
worktree=$(mktemp -d /tmp/hatrie-mu09-publish.XXXXXX)

cleanup() {
	git -C "$root" worktree remove --force "$worktree" >/dev/null 2>&1 || true
}
trap cleanup EXIT

git -C "$root" worktree add --detach "$worktree" "$base"
for file in "${feature_files[@]}"; do
	mkdir -p "$worktree/$(dirname "$file")"
	cp "$root/$file" "$worktree/$file"
done

if ! grep -q '^test-m-u09-frontier:' "$worktree/Makefile"; then
	printf '\n.PHONY: test-m-u09-frontier\ntest-m-u09-frontier:\n\tbash ./scripts/test-m-u09-frontier.sh\n\n.PHONY: format-m-u09-frontier\nformat-m-u09-frontier:\n\tbash ./scripts/format-m-u09-frontier.sh\n\n.PHONY: benchmark-m-u09-frontier\nbenchmark-m-u09-frontier:\n\tbash ./scripts/benchmark-m-u09-frontier.sh\n\n.PHONY: test-m-u09-package\ntest-m-u09-package:\n\tbash ./scripts/test-m-u09-package.sh\n\n.PHONY: race-m-u09-frontier\nrace-m-u09-frontier:\n\tbash ./scripts/race-m-u09-frontier.sh\n\n.PHONY: vet-m-u09-frontier\nvet-m-u09-frontier:\n\tbash ./scripts/vet-m-u09-frontier.sh\n' >> "$worktree/Makefile"
fi

gofmt -w \
	"$worktree/hat/hatPipeline/frontier_registry.go" \
	"$worktree/hat/hatPipeline/frontier_registry_test.go" \
	"$worktree/hat/hatPipeline/frontier_registry_benchmark_test.go"

go test -C "$worktree" ./hat/hatPipeline -count=1
go test -C "$worktree" -race ./hat/hatPipeline -run '^TestFrontierRegistry' -count=1
go vet -C "$worktree" ./hat/hatPipeline
go test -C "$worktree" ./hat/hatPipeline -run '^$' -bench '^BenchmarkFrontierRegistry' -benchmem -count=3
bash "$worktree/scripts/audit-product-idea-gaps.sh"
go test -C "$worktree" ./... -timeout 90s -p 1 -skip '^TestRunRestoreRehearsalVerifiesBackupPath$' -count=1

git -C "$worktree" diff --check
git -C "$worktree" add \
	Makefile \
	README.md \
	PRODUCT_IDEA_GAPS.md \
	FRONTIER_REGISTRY.md \
	hat/hatPipeline/frontier_registry.go \
	hat/hatPipeline/frontier_registry_test.go \
	hat/hatPipeline/frontier_registry_benchmark_test.go \
	scripts/format-m-u09-frontier.sh \
	scripts/test-m-u09-frontier.sh \
	scripts/test-m-u09-package.sh \
	scripts/race-m-u09-frontier.sh \
	scripts/vet-m-u09-frontier.sh \
	scripts/benchmark-m-u09-frontier.sh \
	scripts/publish-m-u09-frontier.sh
git -C "$worktree" diff --cached --check
git -C "$worktree" commit -m "feat(pipeline): add named frontier registry"
git -C "$worktree" push origin HEAD:master
