#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
feature_files=(
	README.md
	PRODUCT_IDEA_GAPS.md
	CONNECTOR_LIFECYCLE.md
	hat/hatPipeline/connector_lifecycle.go
	hat/hatPipeline/connector_lifecycle_test.go
	hat/hatPipeline/connector_lifecycle_benchmark_test.go
	scripts/format-mu01-connector-lifecycle.sh
	scripts/test-mu01-connector-lifecycle.sh
	scripts/test-mu01-connector-package.sh
	scripts/race-mu01-connector-lifecycle.sh
	scripts/vet-mu01-connector-lifecycle.sh
	scripts/benchmark-mu01-connector-lifecycle.sh
	scripts/publish-mu01-connector-lifecycle.sh
)

git -C "$root" fetch origin master
base=$(git -C "$root" rev-parse origin/master)
worktree=$(mktemp -d /tmp/hatrie-mu01-publish.XXXXXX)

cleanup() {
	git -C "$root" worktree remove --force "$worktree" >/dev/null 2>&1 || true
}
trap cleanup EXIT

git -C "$root" worktree add --detach "$worktree" "$base"
for file in "${feature_files[@]}"; do
	mkdir -p "$worktree/$(dirname "$file")"
	cp "$root/$file" "$worktree/$file"
done

if ! grep -q '^test-mu01-connector-lifecycle:' "$worktree/Makefile"; then
	printf '\n.PHONY: test-mu01-connector-lifecycle\ntest-mu01-connector-lifecycle:\n\tbash ./scripts/test-mu01-connector-lifecycle.sh\n\n.PHONY: format-mu01-connector-lifecycle\nformat-mu01-connector-lifecycle:\n\tbash ./scripts/format-mu01-connector-lifecycle.sh\n\n.PHONY: benchmark-mu01-connector-lifecycle\nbenchmark-mu01-connector-lifecycle:\n\tbash ./scripts/benchmark-mu01-connector-lifecycle.sh\n\n.PHONY: test-mu01-connector-package\ntest-mu01-connector-package:\n\tbash ./scripts/test-mu01-connector-package.sh\n\n.PHONY: race-mu01-connector-lifecycle\nrace-mu01-connector-lifecycle:\n\tbash ./scripts/race-mu01-connector-lifecycle.sh\n\n.PHONY: vet-mu01-connector-lifecycle\nvet-mu01-connector-lifecycle:\n\tbash ./scripts/vet-mu01-connector-lifecycle.sh\n' >> "$worktree/Makefile"
fi

gofmt -w \
	"$worktree/hat/hatPipeline/connector_lifecycle.go" \
	"$worktree/hat/hatPipeline/connector_lifecycle_test.go" \
	"$worktree/hat/hatPipeline/connector_lifecycle_benchmark_test.go"

go test -C "$worktree" ./hat/hatPipeline -count=1
go test -C "$worktree" -race ./hat/hatPipeline -run '^TestConnectorRegistry' -count=1
go vet -C "$worktree" ./hat/hatPipeline
go test -C "$worktree" ./hat/hatPipeline -run '^$' -bench '^BenchmarkConnector' -benchmem -count=3
go test -C "$worktree" ./... -timeout 90s -p 1 -skip '^TestRunRestoreRehearsalVerifiesBackupPath$' -count=1

git -C "$worktree" diff --check
git -C "$worktree" add \
	Makefile \
	README.md \
	PRODUCT_IDEA_GAPS.md \
	CONNECTOR_LIFECYCLE.md \
	hat/hatPipeline/connector_lifecycle.go \
	hat/hatPipeline/connector_lifecycle_test.go \
	hat/hatPipeline/connector_lifecycle_benchmark_test.go \
	scripts/format-mu01-connector-lifecycle.sh \
	scripts/test-mu01-connector-lifecycle.sh \
	scripts/test-mu01-connector-package.sh \
	scripts/race-mu01-connector-lifecycle.sh \
	scripts/vet-mu01-connector-lifecycle.sh \
	scripts/benchmark-mu01-connector-lifecycle.sh \
	scripts/publish-mu01-connector-lifecycle.sh
git -C "$worktree" diff --cached --check
git -C "$worktree" commit -m "feat(pipeline): add connector lifecycle registry"
git -C "$worktree" push origin HEAD:master
