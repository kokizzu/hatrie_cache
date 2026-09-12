#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

feature_files=(
	INDEX_STATS.md
	hat/hatDataStructure/index_stats.go
	hat/hatDataStructure/index_stats_test.go
	scripts/benchmark-t-u46.sh
	scripts/format-t-u46.sh
	scripts/publish-t-u46.sh
	scripts/test-t-u46.sh
)

git fetch origin master
base_revision="$(git rev-parse origin/master)"
worktree_parent="$(mktemp -d "${TMPDIR:-/tmp}/hatrie-cache-t-u46.XXXXXX")"
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

if ! rg -Fq -- 'test-t-u46:' Makefile; then
	cat >> Makefile <<'EOF'

.PHONY: test-t-u46
test-t-u46:
	@bash scripts/test-t-u46.sh

.PHONY: format-t-u46
format-t-u46:
	@bash scripts/format-t-u46.sh

.PHONY: benchmark-t-u46
benchmark-t-u46:
	@bash scripts/benchmark-t-u46.sh

.PHONY: publish-t-u46
publish-t-u46:
	@bash scripts/publish-t-u46.sh
EOF
fi

readme_anchor='- Opt-in peer-call lifecycle cancellation: [PEER_CALL_CANCELLATION.md](PEER_CALL_CANCELLATION.md)'
readme_entry='- Opt-in index cardinality and hot-key statistics: [INDEX_STATS.md](INDEX_STATS.md)'
if ! rg -Fq -- "$readme_entry" README.md; then
	rg -Fq -- "$readme_anchor" README.md
	README_ANCHOR="$readme_anchor" README_ENTRY="$readme_entry" perl -0pi -e 's/\Q$ENV{README_ANCHOR}\E/$ENV{README_ANCHOR}\n$ENV{README_ENTRY}/' README.md
fi

catalog_old='| T-U46 | Index cardinality/hot-key statistics | Index APIs do not expose bounded cardinality, posting-length, and hot-key diagnostics for planning. | Privacy, sampling, and update overhead. |'
catalog_new='| T-U46 | Index cardinality/hot-key statistics | `hatDataStructure.IndexStats` now provides opt-in fixed-memory HyperLogLog cardinality, exact posting-length counters, and Space-Saving hot-key hashes; index structures remain uninstrumented until callers observe keys/lookups. | Privacy, sampling, and update overhead. |'
if ! rg -Fq -- "$catalog_new" PRODUCT_IDEA_GAPS.md; then
	rg -Fq -- "$catalog_old" PRODUCT_IDEA_GAPS.md
	CATALOG_OLD="$catalog_old" CATALOG_NEW="$catalog_new" perl -0pi -e 's/\Q$ENV{CATALOG_OLD}\E/$ENV{CATALOG_NEW}/' PRODUCT_IDEA_GAPS.md
fi

gofmt -w hat/hatDataStructure/index_stats.go hat/hatDataStructure/index_stats_test.go
make format-t-u46
make test-t-u46
go test -race ./hat/hatDataStructure -count=1
go vet ./hat/hatDataStructure
make benchmark-t-u46
go test ./... -timeout 90s -p 1 -skip '^TestRunRestoreRehearsalVerifiesBackupPath$' -count=1

git diff --check
git add -- Makefile README.md PRODUCT_IDEA_GAPS.md "${feature_files[@]}"
git diff --cached --check
git commit -m 'feat(hatDataStructure): add index statistics'
git push origin HEAD:master
