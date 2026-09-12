#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
feature_files=(
	README.md
	PRODUCT_IDEA_GAPS.md
	COMPACT_PEER_SESSION.md
	hat/hatPeer/compact_session.go
	hat/hatPeer/compact_session_test.go
	hat/hatPeer/compact_session_benchmark_test.go
	scripts/format-t-u02-session.sh
	scripts/test-t-u02-session.sh
	scripts/test-t-u02-session-package.sh
	scripts/race-t-u02-session.sh
	scripts/vet-t-u02-session.sh
	scripts/benchmark-t-u02-session.sh
	scripts/publish-t-u02-session.sh
)

git -C "$root" fetch origin master
base=$(git -C "$root" rev-parse origin/master)
worktree=$(mktemp -d /tmp/hatrie-tu02-session-publish.XXXXXX)
cleanup() {
	git -C "$root" worktree remove --force "$worktree" >/dev/null 2>&1 || true
}
trap cleanup EXIT

git -C "$root" worktree add --detach "$worktree" "$base"
for file in "${feature_files[@]}"; do
	mkdir -p "$worktree/$(dirname "$file")"
	cp "$root/$file" "$worktree/$file"
done

if ! grep -q '^format-t-u02-session:' "$worktree/Makefile"; then
	printf '\n.PHONY: format-t-u02-session\nformat-t-u02-session:\n\tbash ./scripts/format-t-u02-session.sh\n\n.PHONY: test-t-u02-session\ntest-t-u02-session:\n\tbash ./scripts/test-t-u02-session.sh\n\n.PHONY: test-t-u02-session-package\ntest-t-u02-session-package:\n\tbash ./scripts/test-t-u02-session-package.sh\n\n.PHONY: race-t-u02-session\nrace-t-u02-session:\n\tbash ./scripts/race-t-u02-session.sh\n\n.PHONY: vet-t-u02-session\nvet-t-u02-session:\n\tbash ./scripts/vet-t-u02-session.sh\n\n.PHONY: benchmark-t-u02-session\nbenchmark-t-u02-session:\n\tbash ./scripts/benchmark-t-u02-session.sh\n\n.PHONY: publish-t-u02-session\npublish-t-u02-session:\n\tbash ./scripts/publish-t-u02-session.sh\n' >> "$worktree/Makefile"
fi

gofmt -w \
	"$worktree/hat/hatPeer/compact_session.go" \
	"$worktree/hat/hatPeer/compact_session_test.go" \
	"$worktree/hat/hatPeer/compact_session_benchmark_test.go"
go test -C "$worktree" ./hat/hatPeer -run '^TestCompactPeerSession' -count=1
go test -C "$worktree" -race ./hat/hatPeer -run '^TestCompactPeerSession' -count=1
go vet -C "$worktree" ./hat/hatPeer
go test -C "$worktree" ./hat/hatPeer -run '^$' -bench '^BenchmarkCompactPeerSessionCall$' -benchmem -count=3
bash "$worktree/scripts/audit-product-idea-gaps.sh"
go test -C "$worktree" ./... -timeout 90s -p 1 -skip '^TestRunRestoreRehearsalVerifiesBackupPath$' -count=1
git -C "$worktree" diff --check
git -C "$worktree" add \
	Makefile \
	README.md \
	PRODUCT_IDEA_GAPS.md \
	COMPACT_PEER_SESSION.md \
	hat/hatPeer/compact_session.go \
	hat/hatPeer/compact_session_test.go \
	hat/hatPeer/compact_session_benchmark_test.go \
	scripts/format-t-u02-session.sh \
	scripts/test-t-u02-session.sh \
	scripts/test-t-u02-session-package.sh \
	scripts/race-t-u02-session.sh \
	scripts/vet-t-u02-session.sh \
	scripts/benchmark-t-u02-session.sh \
	scripts/publish-t-u02-session.sh
git -C "$worktree" diff --cached --check
git -C "$worktree" commit -m "feat(peer): add compact peer session"
git -C "$worktree" push origin HEAD:master
