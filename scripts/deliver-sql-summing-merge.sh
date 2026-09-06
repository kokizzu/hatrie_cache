#!/usr/bin/env bash
set -euo pipefail

mode="${1:-preview}"
repo_root="$(cd "$(dirname "$0")/.." && pwd)"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT

index_file="$tmp_dir/index"

git -C "$repo_root" show HEAD:Makefile > "$tmp_dir/Makefile"
if ! grep -Fq 'test-sql-summing-merge:' "$tmp_dir/Makefile"; then
	printf '\n.PHONY: test-sql-summing-merge\ntest-sql-summing-merge:\n\tbash ./scripts/test-sql-summing-merge.sh\n\n.PHONY: test-race-sql-summing-merge\ntest-race-sql-summing-merge:\n\tbash ./scripts/test-race-sql-summing-merge.sh\n\n.PHONY: format-sql-summing-merge\nformat-sql-summing-merge:\n\tbash ./scripts/format-sql-summing-merge.sh\n\n.PHONY: benchmark-sql-summing-merge\nbenchmark-sql-summing-merge:\n\tbash ./scripts/benchmark-sql-summing-merge.sh\n\n.PHONY: deliver-sql-summing-merge\ndeliver-sql-summing-merge:\n\tbash ./scripts/deliver-sql-summing-merge.sh preview\n\n.PHONY: commit-sql-summing-merge\ncommit-sql-summing-merge:\n\tbash ./scripts/deliver-sql-summing-merge.sh commit\n\n.PHONY: push-sql-summing-merge\npush-sql-summing-merge:\n\tbash ./scripts/deliver-sql-summing-merge.sh push\n' >> "$tmp_dir/Makefile"
fi

git -C "$repo_root" show HEAD:INSPIRATION.md > "$tmp_dir/INSPIRATION.md"
awk '
/^- \[x\] C033a Explicit overflow-checked summing merge for selected numeric columns\.$/ {
	if (seen++) {
		next
	}
}
{ print }
' "$tmp_dir/INSPIRATION.md" > "$tmp_dir/INSPIRATION.md.new"
mv "$tmp_dir/INSPIRATION.md.new" "$tmp_dir/INSPIRATION.md"
if ! grep -Fq 'C033a Explicit overflow-checked summing merge for selected numeric columns.' "$tmp_dir/INSPIRATION.md"; then
	awk '/^- \[ \] C033 SummingMergeTree-style merge-time summation\.$/ { print; print "- [x] C033a Explicit overflow-checked summing merge for selected numeric columns."; next } { print }' "$tmp_dir/INSPIRATION.md" > "$tmp_dir/INSPIRATION.md.new"
	mv "$tmp_dir/INSPIRATION.md.new" "$tmp_dir/INSPIRATION.md"
fi

GIT_INDEX_FILE="$index_file" git -C "$repo_root" read-tree HEAD
for path in \
	INSPIRATION.md \
	Makefile \
	SUMMING_MERGE.md \
	hat/hatSql/summing_merge.go \
	hat/hatSql/summing_merge_test.go \
	scripts/benchmark-sql-summing-merge.sh \
	scripts/deliver-sql-summing-merge.sh \
	scripts/format-sql-summing-merge.sh \
	scripts/test-race-sql-summing-merge.sh \
	scripts/test-sql-summing-merge.sh; do
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
		GIT_INDEX_FILE="$index_file" git -C "$repo_root" commit -m "feat(sql): add summing merge"
		;;
	push)
		git -C "$repo_root" push origin HEAD:master
		;;
	*)
		echo "unknown delivery mode: $mode" >&2
		exit 2
		;;
esac
