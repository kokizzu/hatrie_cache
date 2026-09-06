#!/usr/bin/env bash
set -euo pipefail

mode="${1:-preview}"
repo_root="$(cd "$(dirname "$0")/.." && pwd)"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT

index_file="$tmp_dir/index"

git -C "$repo_root" show HEAD:Makefile > "$tmp_dir/Makefile"
if ! grep -Fq 'test-sql-collapsing-merge:' "$tmp_dir/Makefile"; then
	printf '\n.PHONY: test-sql-collapsing-merge\ntest-sql-collapsing-merge:\n\tbash ./scripts/test-sql-collapsing-merge.sh\n\n.PHONY: test-race-sql-collapsing-merge\ntest-race-sql-collapsing-merge:\n\tbash ./scripts/test-race-sql-collapsing-merge.sh\n\n.PHONY: format-sql-collapsing-merge\nformat-sql-collapsing-merge:\n\tbash ./scripts/format-sql-collapsing-merge.sh\n\n.PHONY: benchmark-sql-collapsing-merge\nbenchmark-sql-collapsing-merge:\n\tbash ./scripts/benchmark-sql-collapsing-merge.sh\n\n.PHONY: deliver-sql-collapsing-merge\ndeliver-sql-collapsing-merge:\n\tbash ./scripts/deliver-sql-collapsing-merge.sh preview\n\n.PHONY: commit-sql-collapsing-merge\ncommit-sql-collapsing-merge:\n\tbash ./scripts/deliver-sql-collapsing-merge.sh commit\n\n.PHONY: push-sql-collapsing-merge\npush-sql-collapsing-merge:\n\tbash ./scripts/deliver-sql-collapsing-merge.sh push\n' >> "$tmp_dir/Makefile"
fi

git -C "$repo_root" show HEAD:INSPIRATION.md > "$tmp_dir/INSPIRATION.md"
awk '
/^- \[x\] C032a Explicit deterministic signed-row cancellation merge\.$/ {
	if (seen++) {
		next
	}
}
{ print }
' "$tmp_dir/INSPIRATION.md" > "$tmp_dir/INSPIRATION.md.new"
mv "$tmp_dir/INSPIRATION.md.new" "$tmp_dir/INSPIRATION.md"
if ! grep -Fq 'C032a Explicit deterministic signed-row cancellation merge.' "$tmp_dir/INSPIRATION.md"; then
	awk '/^- \[ \] C032 CollapsingMergeTree-style sign-based row cancellation\.$/ { print; print "- [x] C032a Explicit deterministic signed-row cancellation merge."; next } { print }' "$tmp_dir/INSPIRATION.md" > "$tmp_dir/INSPIRATION.md.new"
	mv "$tmp_dir/INSPIRATION.md.new" "$tmp_dir/INSPIRATION.md"
fi

GIT_INDEX_FILE="$index_file" git -C "$repo_root" read-tree HEAD
for path in \
	INSPIRATION.md \
	Makefile \
	COLLAPSING_MERGE.md \
	hat/hatSql/collapsing_merge.go \
	hat/hatSql/collapsing_merge_test.go \
	scripts/benchmark-sql-collapsing-merge.sh \
	scripts/deliver-sql-collapsing-merge.sh \
	scripts/format-sql-collapsing-merge.sh \
	scripts/test-race-sql-collapsing-merge.sh \
	scripts/test-sql-collapsing-merge.sh; do
	if [[ "$path" == "Makefile" ]]; then
		object_id="$(git -C "$repo_root" hash-object -w "$tmp_dir/Makefile")"
		GIT_INDEX_FILE="$index_file" git -C "$repo_root" update-index --add --cacheinfo "100644,$object_id,$path"
	elif [[ "$path" == "INSPIRATION.md" ]]; then
		object_id="$(git -C "$repo_root" hash-object -w "$tmp_dir/INSPIRATION.md")"
		GIT_INDEX_FILE="$index_file" git -C "$repo_root" update-index --add --cacheinfo "100644,$object_id,$path"
	else
		GIT_INDEX_FILE="$index_file" git -C "$repo_root" add -- "$path"
	fi
done

echo "Isolated delivery diff:"
GIT_INDEX_FILE="$index_file" git -C "$repo_root" diff --cached --name-status
GIT_INDEX_FILE="$index_file" git -C "$repo_root" diff --cached --stat

case "$mode" in
	preview)
		;;
	commit)
		GIT_INDEX_FILE="$index_file" git -C "$repo_root" commit -m "feat(sql): add collapsing merge"
		;;
	push)
		git -C "$repo_root" push origin HEAD:master
		;;
	*)
		echo "unknown delivery mode: $mode" >&2
		exit 2
		;;
esac
