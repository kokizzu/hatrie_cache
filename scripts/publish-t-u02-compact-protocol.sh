#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
feature_files=(
	README.md
	PRODUCT_IDEA_GAPS.md
	COMPACT_PEER_PROTOCOL.md
	hat/hatPeer/compact_protocol.go
	hat/hatPeer/compact_protocol_test.go
	hat/hatPeer/compact_protocol_benchmark_test.go
	scripts/format-t-u02-compact-protocol.sh
	scripts/test-t-u02-compact-protocol.sh
	scripts/test-t-u02-compact-package.sh
	scripts/race-t-u02-compact-protocol.sh
	scripts/vet-t-u02-compact-protocol.sh
	scripts/benchmark-t-u02-compact-protocol.sh
	scripts/measure-t-u02-wire-size.sh
	scripts/publish-t-u02-compact-protocol.sh
)

git -C "$root" fetch origin master
base=$(git -C "$root" rev-parse origin/master)
worktree=$(mktemp -d /tmp/hatrie-tu02-publish.XXXXXX)

cleanup() {
	git -C "$root" worktree remove --force "$worktree" >/dev/null 2>&1 || true
}
trap cleanup EXIT

git -C "$root" worktree add --detach "$worktree" "$base"
for file in "${feature_files[@]}"; do
	mkdir -p "$worktree/$(dirname "$file")"
	cp "$root/$file" "$worktree/$file"
done

if ! grep -q '^test-t-u02-compact-protocol:' "$worktree/Makefile"; then
	printf '\n.PHONY: test-t-u02-compact-protocol\ntest-t-u02-compact-protocol:\n\tbash ./scripts/test-t-u02-compact-protocol.sh\n\n.PHONY: format-t-u02-compact-protocol\nformat-t-u02-compact-protocol:\n\tbash ./scripts/format-t-u02-compact-protocol.sh\n\n.PHONY: benchmark-t-u02-compact-protocol\nbenchmark-t-u02-compact-protocol:\n\tbash ./scripts/benchmark-t-u02-compact-protocol.sh\n\n.PHONY: measure-t-u02-wire-size\nmeasure-t-u02-wire-size:\n\tbash ./scripts/measure-t-u02-wire-size.sh\n\n.PHONY: test-t-u02-compact-package\ntest-t-u02-compact-package:\n\tbash ./scripts/test-t-u02-compact-package.sh\n\n.PHONY: race-t-u02-compact-protocol\nrace-t-u02-compact-protocol:\n\tbash ./scripts/race-t-u02-compact-protocol.sh\n\n.PHONY: vet-t-u02-compact-protocol\nvet-t-u02-compact-protocol:\n\tbash ./scripts/vet-t-u02-compact-protocol.sh\n' >> "$worktree/Makefile"
fi

gofmt -w \
	"$worktree/hat/hatPeer/compact_protocol.go" \
	"$worktree/hat/hatPeer/compact_protocol_test.go" \
	"$worktree/hat/hatPeer/compact_protocol_benchmark_test.go"

go test -C "$worktree" ./hat/hatPeer -count=1
go test -C "$worktree" -race ./hat/hatPeer -run '^TestCompact' -count=1
go vet -C "$worktree" ./hat/hatPeer
go test -C "$worktree" ./hat/hatPeer -run '^$' -bench '^(BenchmarkCompactProtocol|BenchmarkJSONFrame)' -benchmem -count=3
bash "$worktree/scripts/audit-product-idea-gaps.sh"
go test -C "$worktree" ./... -timeout 90s -p 1 -skip '^TestRunRestoreRehearsalVerifiesBackupPath$' -count=1

git -C "$worktree" diff --check
git -C "$worktree" add \
	Makefile \
	README.md \
	PRODUCT_IDEA_GAPS.md \
	COMPACT_PEER_PROTOCOL.md \
	hat/hatPeer/compact_protocol.go \
	hat/hatPeer/compact_protocol_test.go \
	hat/hatPeer/compact_protocol_benchmark_test.go \
	scripts/format-t-u02-compact-protocol.sh \
	scripts/test-t-u02-compact-protocol.sh \
	scripts/test-t-u02-compact-package.sh \
	scripts/race-t-u02-compact-protocol.sh \
	scripts/vet-t-u02-compact-protocol.sh \
	scripts/benchmark-t-u02-compact-protocol.sh \
	scripts/measure-t-u02-wire-size.sh \
	scripts/publish-t-u02-compact-protocol.sh
git -C "$worktree" diff --cached --check
git -C "$worktree" commit -m "feat(peer): add compact multiplexed protocol"
git -C "$worktree" push origin HEAD:master
