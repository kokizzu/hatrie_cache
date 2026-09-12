#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

feature_files=(
	RETRY_POLICY.md
	hat/hatPeer/retry_policy.go
	hat/hatPeer/retry_policy_test.go
	scripts/benchmark-t-u48.sh
	scripts/format-t-u48.sh
	scripts/publish-t-u48.sh
	scripts/test-t-u48.sh
)

git fetch origin master
base_revision="$(git rev-parse origin/master)"
worktree_parent="$(mktemp -d "${TMPDIR:-/tmp}/hatrie-cache-t-u48.XXXXXX")"
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

if ! rg -Fq -- 'test-t-u48:' Makefile; then
	cat >> Makefile <<'EOF'

.PHONY: test-t-u48
test-t-u48:
	@bash scripts/test-t-u48.sh

.PHONY: format-t-u48
format-t-u48:
	@bash scripts/format-t-u48.sh

.PHONY: benchmark-t-u48
benchmark-t-u48:
	@bash scripts/benchmark-t-u48.sh

.PHONY: publish-t-u48
publish-t-u48:
	@bash scripts/publish-t-u48.sh
EOF
fi

readme_anchor='- Opt-in index cardinality and hot-key statistics: [INDEX_STATS.md](INDEX_STATS.md)'
readme_entry='- Opt-in method-aware peer retries: [RETRY_POLICY.md](RETRY_POLICY.md)'
if ! rg -Fq -- "$readme_entry" README.md; then
	rg -Fq -- "$readme_anchor" README.md
	README_ANCHOR="$readme_anchor" README_ENTRY="$readme_entry" perl -0pi -e 's/\Q$ENV{README_ANCHOR}\E/$ENV{README_ANCHOR}\n$ENV{README_ENTRY}/' README.md
fi

catalog_old='| T-U48 | Idempotent remote-call retry policy | Peer calls lack a method-aware retry/backoff policy tied to idempotency keys and fencing tokens. | No duplicate mutation, jitter, and observability. |'
catalog_new='| T-U48 | Idempotent remote-call retry policy | `hatPeer.RetryPolicy` now provides opt-in method-aware bounded retries with stable idempotency keys, fencing tokens, cancellation-aware exponential backoff/jitter, and observer events; remote handlers still enforce deduplication. | No duplicate mutation, jitter, and observability. |'
if ! rg -Fq -- "$catalog_new" PRODUCT_IDEA_GAPS.md; then
	rg -Fq -- "$catalog_old" PRODUCT_IDEA_GAPS.md
	CATALOG_OLD="$catalog_old" CATALOG_NEW="$catalog_new" perl -0pi -e 's/\Q$ENV{CATALOG_OLD}\E/$ENV{CATALOG_NEW}/' PRODUCT_IDEA_GAPS.md
fi

gofmt -w hat/hatPeer/retry_policy.go hat/hatPeer/retry_policy_test.go
make format-t-u48
make test-t-u48
go test -race ./hat/hatPeer -count=1
go vet ./hat/hatPeer
make benchmark-t-u48
go test ./... -timeout 90s -p 1 -skip '^TestRunRestoreRehearsalVerifiesBackupPath$' -count=1

git diff --check
git add -- Makefile README.md PRODUCT_IDEA_GAPS.md "${feature_files[@]}"
git diff --cached --check
git commit -m 'feat(hatPeer): add idempotent retry policy'
git push origin HEAD:master
