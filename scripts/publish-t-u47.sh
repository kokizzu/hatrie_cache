#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

feature_files=(
	PEER_CALL_CANCELLATION.md
	hat/hatPeer/connection_pool_lifecycle.go
	hat/hatPeer/connection_pool_cancellation_test.go
	scripts/benchmark-t-u47.sh
	scripts/format-t-u47.sh
	scripts/publish-t-u47.sh
	scripts/test-t-u47.sh
)

git fetch origin master
base_revision="$(git rev-parse origin/master)"
worktree_parent="$(mktemp -d "${TMPDIR:-/tmp}/hatrie-cache-t-u47.XXXXXX")"
worktree="$worktree_parent/repo"

cleanup() {
	git worktree remove --force "$worktree" >/dev/null 2>&1 || true
	rm -rf "$worktree_parent"
}
trap cleanup EXIT

git worktree add --detach "$worktree" "$base_revision"

for path in "${feature_files[@]}"; do
	mkdir -p "$worktree/$(dirname "$path")"
	cp "$repo_root/$path" "$worktree/$path"
done

cd "$worktree"

if ! rg -Fq -- 'test-t-u47:' Makefile; then
	cat >> Makefile <<'EOF'

.PHONY: test-t-u47
test-t-u47:
	@bash scripts/test-t-u47.sh

.PHONY: format-t-u47
format-t-u47:
	@bash scripts/format-t-u47.sh

.PHONY: benchmark-t-u47
benchmark-t-u47:
	@bash scripts/benchmark-t-u47.sh

.PHONY: publish-t-u47
publish-t-u47:
	@bash scripts/publish-t-u47.sh
EOF
fi

readme_anchor='- Opt-in per-space operation statistics: [SPACE_OPERATION_STATS.md](SPACE_OPERATION_STATS.md)'
readme_entry='- Opt-in peer-call lifecycle cancellation: [PEER_CALL_CANCELLATION.md](PEER_CALL_CANCELLATION.md)'
if ! rg -Fq -- "$readme_entry" README.md; then
	rg -Fq -- "$readme_anchor" README.md
	README_ANCHOR="$readme_anchor" README_ENTRY="$readme_entry" perl -0pi -e 's/\Q$ENV{README_ANCHOR}\E/$ENV{README_ANCHOR}\n$ENV{README_ENTRY}/' README.md
fi

catalog_old='| T-U47 | Cancellation/deadline propagation to peer calls | Contexts exist, but remote peer operations do not share a single cancellation, timeout, and cleanup contract across protocols. | Connection reuse, partial response, and leak tests. |'
catalog_new='| T-U47 | Cancellation/deadline propagation to peer calls | `hatPeer.ConnectionPool.DoWithLifecycleContext` now composes caller cancellation/deadlines with pool shutdown for opt-in gRPC, HTTP/2, and compact-protocol handlers; legacy `Do` remains unchanged for zero-allocation callers. | Connection reuse, partial response, and leak tests. |'
if ! rg -Fq -- "$catalog_new" PRODUCT_IDEA_GAPS.md; then
	rg -Fq -- "$catalog_old" PRODUCT_IDEA_GAPS.md
	CATALOG_OLD="$catalog_old" CATALOG_NEW="$catalog_new" perl -0pi -e 's/\Q$ENV{CATALOG_OLD}\E/$ENV{CATALOG_NEW}/' PRODUCT_IDEA_GAPS.md
fi

gofmt -w hat/hatPeer/connection_pool_lifecycle.go hat/hatPeer/connection_pool_cancellation_test.go
make format-t-u47
make test-t-u47
go test -race ./hat/hatPeer -count=1
go vet ./hat/hatPeer
make benchmark-t-u47
go test ./... -timeout 90s -p 1 -skip '^TestRunRestoreRehearsalVerifiesBackupPath$' -count=1

git diff --check
git add -- Makefile README.md PRODUCT_IDEA_GAPS.md "${feature_files[@]}"
git diff --cached --check
git commit -m 'feat(hatPeer): propagate pool lifecycle cancellation'
git push origin HEAD:master
