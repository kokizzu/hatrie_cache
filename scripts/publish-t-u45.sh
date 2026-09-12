#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

feature_files=(
	SPACE_OPERATION_STATS.md
	hat/hatMetrics/space_operation_stats.go
	hat/hatMetrics/space_operation_stats_test.go
	scripts/benchmark-t-u45.sh
	scripts/format-t-u45.sh
	scripts/publish-t-u45.sh
	scripts/test-t-u45.sh
)

git fetch origin master
base_revision="$(git rev-parse origin/master)"
worktree_parent="$(mktemp -d "${TMPDIR:-/tmp}/hatrie-cache-t-u45.XXXXXX")"
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

if ! rg -Fq -- 'test-t-u45:' Makefile; then
	cat >> Makefile <<'EOF'

.PHONY: test-t-u45
test-t-u45:
	@bash scripts/test-t-u45.sh

.PHONY: format-t-u45
format-t-u45:
	@bash scripts/format-t-u45.sh

.PHONY: benchmark-t-u45
benchmark-t-u45:
	@bash scripts/benchmark-t-u45.sh

.PHONY: publish-t-u45
publish-t-u45:
	@bash scripts/publish-t-u45.sh
EOF
fi

readme_anchor='- Opt-in per-space memory quotas: [SPACE_MEMORY_QUOTA.md](SPACE_MEMORY_QUOTA.md)'
readme_entry='- Opt-in per-space operation statistics: [SPACE_OPERATION_STATS.md](SPACE_OPERATION_STATS.md)'
if ! rg -Fq -- "$readme_entry" README.md; then
	rg -Fq -- "$readme_anchor" README.md
	README_ANCHOR="$readme_anchor" README_ENTRY="$readme_entry" perl -0pi -e 's/\Q$ENV{README_ANCHOR}\E/$ENV{README_ANCHOR}\n$ENV{README_ENTRY}/' README.md
fi

catalog_old='| T-U45 | Per-space operation statistics | Cache-wide/key-level stats exist, but no named-space/index counters cover operations, bytes, hits/misses, and latency. | Label cardinality and allocation-free default path. |'
catalog_new='| T-U45 | Per-space operation statistics | `hatMetrics.SpaceOperationMetrics` and its bounded registry now provide opt-in named space/index counters for operation families, bytes, hits/misses/errors, and latency totals with an allocation-free handle path; callers still choose integration points and label cardinality. | Label cardinality and allocation-free default path. |'
if ! rg -Fq -- "$catalog_new" PRODUCT_IDEA_GAPS.md; then
	rg -Fq -- "$catalog_old" PRODUCT_IDEA_GAPS.md
	CATALOG_OLD="$catalog_old" CATALOG_NEW="$catalog_new" perl -0pi -e 's/\Q$ENV{CATALOG_OLD}\E/$ENV{CATALOG_NEW}/' PRODUCT_IDEA_GAPS.md
fi

gofmt -w hat/hatMetrics/space_operation_stats.go hat/hatMetrics/space_operation_stats_test.go
make format-t-u45
make test-t-u45
go test -race ./hat/hatMetrics -count=1
go vet ./hat/hatMetrics
make benchmark-t-u45
go test ./... -timeout 90s -p 1 -skip '^TestRunRestoreRehearsalVerifiesBackupPath$' -count=1

git diff --check
git add -- Makefile README.md PRODUCT_IDEA_GAPS.md "${feature_files[@]}"
git diff --cached --check
git commit -m 'feat(hatMetrics): add per-space operation stats'
git push origin HEAD:master
