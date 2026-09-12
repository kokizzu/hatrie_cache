#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

feature_files=(
	hat/hatStorage/space_memory_quota.go
	hat/hatStorage/space_memory_quota_test.go
	scripts/benchmark-t-u43.sh
	scripts/format-t-u43.sh
	scripts/publish-t-u43.sh
	scripts/test-t-u43.sh
	SPACE_MEMORY_QUOTA.md
)

for path in "${feature_files[@]}"; do
	if [[ ! -f "$path" ]]; then
		printf 'missing feature file: %s\n' "$path" >&2
		exit 1
	fi
done

git fetch origin master
base_revision="$(git rev-parse origin/master)"
worktree="$(mktemp -d "${TMPDIR:-/tmp}/hatrie-cache-t-u43.XXXXXX")"
cleanup() {
	git worktree remove --force "$worktree" >/dev/null 2>&1 || true
}
trap cleanup EXIT

git worktree add --detach "$worktree" "$base_revision"
for path in "${feature_files[@]}"; do
	destination="$worktree/$path"
	mkdir -p "$(dirname "$destination")"
	cp "$path" "$destination"
done

if ! rg -q '^test-t-u43:' "$worktree/Makefile"; then
	printf '%s\n' \
		'' \
		'.PHONY: format-t-u43' \
		'format-t-u43:' \
		$'\t@bash scripts/format-t-u43.sh' \
		'' \
		'.PHONY: test-t-u43' \
		'test-t-u43:' \
		$'\t@bash scripts/test-t-u43.sh' \
		'' \
		'.PHONY: benchmark-t-u43' \
		'benchmark-t-u43:' \
		$'\t@bash scripts/benchmark-t-u43.sh' \
		'' \
		'.PHONY: publish-t-u43' \
		'publish-t-u43:' \
		$'\t@bash scripts/publish-t-u43.sh' \
		>> "$worktree/Makefile"
fi

old_readme='- Snapshot-consistent ordered index cursors: [ORDERED_SNAPSHOT_CURSOR.md](ORDERED_SNAPSHOT_CURSOR.md)'
new_readme="$old_readme"$'\n''- Opt-in per-space memory quotas: [SPACE_MEMORY_QUOTA.md](SPACE_MEMORY_QUOTA.md)'
if ! rg -Fq 'SPACE_MEMORY_QUOTA.md' "$worktree/README.md"; then
	rg -Fq -- "$old_readme" "$worktree/README.md"
	OLD_README="$old_readme" NEW_README="$new_readme" perl -0pi -e 's/\Q$ENV{OLD_README}\E/$ENV{NEW_README}/' "$worktree/README.md"
fi

old_catalog='| T-U43 | Per-space memory quotas | Runtime memory reports exist, but allocations cannot be budgeted and rejected per named space/data structure. | Accurate attribution, no deadlock, and default zero overhead. |'
new_catalog='| T-U43 | Per-space memory quotas | `hatStorage.SpaceMemoryQuota` and its registry now provide opt-in named-space byte admission with atomic reserve/release, deterministic snapshots, and default zero integration overhead; callers still declare logical bytes and release them on free. | Accurate attribution, no deadlock, and default zero overhead. |'
if ! rg -Fq 'SpaceMemoryQuota` and its registry' "$worktree/PRODUCT_IDEA_GAPS.md"; then
	rg -Fq -- "$old_catalog" "$worktree/PRODUCT_IDEA_GAPS.md"
	OLD_CATALOG="$old_catalog" NEW_CATALOG="$new_catalog" perl -0pi -e 's/\Q$ENV{OLD_CATALOG}\E/$ENV{NEW_CATALOG}/' "$worktree/PRODUCT_IDEA_GAPS.md"
fi

(cd "$worktree" && gofmt -w hat/hatStorage/space_memory_quota.go hat/hatStorage/space_memory_quota_test.go)
(cd "$worktree" && make test-t-u43)
(cd "$worktree" && go test -race ./hat/hatStorage -count=1)
(cd "$worktree" && go vet ./hat/hatStorage)
(cd "$worktree" && make benchmark-t-u43)
(cd "$worktree" && go test ./... -timeout 90s -p 1 -skip '^TestRunRestoreRehearsalVerifiesBackupPath$' -count=1)

(cd "$worktree" && git diff --check)
(cd "$worktree" && git add -- Makefile README.md PRODUCT_IDEA_GAPS.md "${feature_files[@]}")
(cd "$worktree" && git diff --cached --check)
(cd "$worktree" && git commit -m 'feat(hatStorage): add per-space memory quotas')
(cd "$worktree" && git push origin HEAD:master)
