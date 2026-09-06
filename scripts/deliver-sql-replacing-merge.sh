#!/usr/bin/env bash
set -euo pipefail

mode="${1:-preview}"
repo_root="$(cd "$(dirname "$0")/.." && pwd)"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT

index_file="$tmp_dir/index"

git -C "$repo_root" show HEAD:Makefile > "$tmp_dir/Makefile"
if ! grep -Fq 'test-sql-replacing-merge:' "$tmp_dir/Makefile"; then
	printf '\n.PHONY: test-sql-replacing-merge\ntest-sql-replacing-merge:\n\tbash ./scripts/test-sql-replacing-merge.sh\n\n.PHONY: test-race-sql-replacing-merge\ntest-race-sql-replacing-merge:\n\tbash ./scripts/test-race-sql-replacing-merge.sh\n\n.PHONY: format-sql-replacing-merge\nformat-sql-replacing-merge:\n\tbash ./scripts/format-sql-replacing-merge.sh\n\n.PHONY: benchmark-sql-replacing-merge\nbenchmark-sql-replacing-merge:\n\tbash ./scripts/benchmark-sql-replacing-merge.sh\n\n.PHONY: deliver-sql-replacing-merge\ndeliver-sql-replacing-merge:\n\tbash ./scripts/deliver-sql-replacing-merge.sh preview\n\n.PHONY: commit-sql-replacing-merge\ncommit-sql-replacing-merge:\n\tbash ./scripts/deliver-sql-replacing-merge.sh commit\n\n.PHONY: push-sql-replacing-merge\npush-sql-replacing-merge:\n\tbash ./scripts/deliver-sql-replacing-merge.sh push\n' >> "$tmp_dir/Makefile"
fi

git -C "$repo_root" show HEAD:INSPIRATION.md > "$tmp_dir/INSPIRATION.md"
awk '
/^- \[x\] C031a Explicit stable-order replacing merge for versioned rows\.$/ {
	if (seen++) {
		next
	}
}
{ print }
' "$tmp_dir/INSPIRATION.md" > "$tmp_dir/INSPIRATION.md.new"
mv "$tmp_dir/INSPIRATION.md.new" "$tmp_dir/INSPIRATION.md"
if ! grep -Fq 'C031a Explicit stable-order replacing merge for versioned rows.' "$tmp_dir/INSPIRATION.md"; then
	awk '/^- \[ \] C031 ReplacingMergeTree-style latest-row replacement\.$/ { print; print "- [x] C031a Explicit stable-order replacing merge for versioned rows."; next } { print }' "$tmp_dir/INSPIRATION.md" > "$tmp_dir/INSPIRATION.md.new"
	mv "$tmp_dir/INSPIRATION.md.new" "$tmp_dir/INSPIRATION.md"
fi

GIT_INDEX_FILE="$index_file" git -C "$repo_root" read-tree HEAD
for path in \
	INSPIRATION.md \
	Makefile \
	REPLACING_MERGE.md \
	hat/hatSql/replacing_merge.go \
	hat/hatSql/replacing_merge_test.go \
	scripts/benchmark-sql-replacing-merge.sh \
	scripts/deliver-sql-replacing-merge.sh \
	scripts/format-sql-replacing-merge.sh \
	scripts/test-race-sql-replacing-merge.sh \
	scripts/test-sql-replacing-merge.sh; do
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
		GIT_INDEX_FILE="$index_file" git -C "$repo_root" commit -m "feat(sql): add replacing merge"
		;;
	push)
		git -C "$repo_root" push origin HEAD:master
		;;
	*)
		echo "unknown delivery mode: $mode" >&2
		exit 2
		;;
esac
