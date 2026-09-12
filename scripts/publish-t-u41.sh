#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

feature_files=(
	hat/hatDataStructure/ordered_snapshot_cursor.go
	hat/hatDataStructure/ordered_snapshot_cursor_test.go
	scripts/benchmark-t-u41.sh
	scripts/format-t-u41.sh
	scripts/publish-t-u41.sh
	scripts/test-t-u41.sh
	ORDERED_SNAPSHOT_CURSOR.md
)

for path in "${feature_files[@]}"; do
	if [[ ! -f "$path" ]]; then
		printf 'missing feature file: %s\n' "$path" >&2
		exit 1
	fi
done

git fetch origin master
base_revision="$(git rev-parse origin/master)"
worktree="$(mktemp -d "${TMPDIR:-/tmp}/hatrie-cache-t-u41.XXXXXX")"
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

if ! rg -q '^test-t-u41:' "$worktree/Makefile"; then
	printf '%s\n' \
		'' \
		'.PHONY: format-t-u41' \
		'format-t-u41:' \
		$'\t@bash scripts/format-t-u41.sh' \
		'' \
		'.PHONY: test-t-u41' \
		'test-t-u41:' \
		$'\t@bash scripts/test-t-u41.sh' \
		'' \
		'.PHONY: benchmark-t-u41' \
		'benchmark-t-u41:' \
		$'\t@bash scripts/benchmark-t-u41.sh' \
		'' \
		'.PHONY: publish-t-u41' \
		'publish-t-u41:' \
		$'\t@bash scripts/publish-t-u41.sh' \
		>> "$worktree/Makefile"
fi

old_readme='- Tarantool-style tuple format version negotiation: [TUPLE_FORMAT_NEGOTIATION.md](TUPLE_FORMAT_NEGOTIATION.md)'
new_readme="$old_readme"$'\n''- Snapshot-consistent ordered index cursors: [ORDERED_SNAPSHOT_CURSOR.md](ORDERED_SNAPSHOT_CURSOR.md)'
if ! rg -Fq 'ORDERED_SNAPSHOT_CURSOR.md' "$worktree/README.md"; then
	rg -Fq -- "$old_readme" "$worktree/README.md"
	OLD_README="$old_readme" NEW_README="$new_readme" perl -0pi -e 's/\Q$ENV{OLD_README}\E/$ENV{NEW_README}/' "$worktree/README.md"
fi

old_catalog='| T-U41 | Snapshot-consistent iterator cursors | Ordered iterators have invalidation semantics, but no generic named-space cursor provides a stable snapshot across concurrent mutations. | Memory bound, invalidation, and repeatable ordering. |'
new_catalog='| T-U41 | Snapshot-consistent iterator cursors | `hatDataStructure.OrderedIndex.SnapshotCursor` now provides an opt-in zero-copy stable view across concurrent mutations, with explicit seek/close/EOF semantics and existing copy-on-write retention; named-space registry wiring remains caller-owned. | Memory bound, invalidation, and repeatable ordering. |'
if ! rg -Fq 'OrderedIndex.SnapshotCursor' "$worktree/PRODUCT_IDEA_GAPS.md"; then
	rg -Fq -- "$old_catalog" "$worktree/PRODUCT_IDEA_GAPS.md"
	OLD_CATALOG="$old_catalog" NEW_CATALOG="$new_catalog" perl -0pi -e 's/\Q$ENV{OLD_CATALOG}\E/$ENV{NEW_CATALOG}/' "$worktree/PRODUCT_IDEA_GAPS.md"
fi

(cd "$worktree" && gofmt -w hat/hatDataStructure/ordered_snapshot_cursor.go hat/hatDataStructure/ordered_snapshot_cursor_test.go)
(cd "$worktree" && make test-t-u41)
(cd "$worktree" && go test -race ./hat/hatDataStructure -count=1)
(cd "$worktree" && go vet ./hat/hatDataStructure)
(cd "$worktree" && make benchmark-t-u41)
(cd "$worktree" && go test ./... -timeout 90s -p 1 -skip '^TestRunRestoreRehearsalVerifiesBackupPath$' -count=1)

(cd "$worktree" && git diff --check)
(cd "$worktree" && git add -- Makefile README.md PRODUCT_IDEA_GAPS.md "${feature_files[@]}")
(cd "$worktree" && git diff --cached --check)
(cd "$worktree" && git commit -m 'feat(hatDataStructure): add snapshot cursors')
(cd "$worktree" && git push origin HEAD:master)
