#!/usr/bin/env bash
set -euo pipefail

mode="${1:-preview}"
repo_root="$(cd "$(dirname "$0")/.." && pwd)"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT

index_file="$tmp_dir/index"

git -C "$repo_root" show HEAD:Makefile > "$tmp_dir/Makefile"
if ! grep -Fq 'inspect-row-binary-adaptive:' "$tmp_dir/Makefile"; then
	printf '\n.PHONY: inspect-row-binary-adaptive\ninspect-row-binary-adaptive:\n\tbash ./scripts/inspect-row-binary-adaptive.sh\n\n.PHONY: test-sql-row-binary-adaptive-sampled\ntest-sql-row-binary-adaptive-sampled:\n\tbash ./scripts/test-sql-row-binary-adaptive-sampled.sh\n\n.PHONY: test-race-sql-row-binary-adaptive-sampled\ntest-race-sql-row-binary-adaptive-sampled:\n\tbash ./scripts/test-race-sql-row-binary-adaptive-sampled.sh\n\n.PHONY: format-sql-row-binary-adaptive-sampled\nformat-sql-row-binary-adaptive-sampled:\n\tbash ./scripts/format-sql-row-binary-adaptive-sampled.sh\n\n.PHONY: benchmark-sql-row-binary-adaptive-sampled\nbenchmark-sql-row-binary-adaptive-sampled:\n\tbash ./scripts/benchmark-sql-row-binary-adaptive-sampled.sh\n\n.PHONY: deliver-sql-row-binary-adaptive-sampled\ndeliver-sql-row-binary-adaptive-sampled:\n\tbash ./scripts/deliver-sql-row-binary-adaptive-sampled.sh preview\n\n.PHONY: commit-sql-row-binary-adaptive-sampled\ncommit-sql-row-binary-adaptive-sampled:\n\tbash ./scripts/deliver-sql-row-binary-adaptive-sampled.sh commit\n\n.PHONY: push-sql-row-binary-adaptive-sampled\npush-sql-row-binary-adaptive-sampled:\n\tbash ./scripts/deliver-sql-row-binary-adaptive-sampled.sh push\n' >> "$tmp_dir/Makefile"
fi

git -C "$repo_root" show HEAD:INSPIRATION.md > "$tmp_dir/INSPIRATION.md"
awk '
/^- \[x\] C059a Sampled adaptive codec selection from a bounded prefix\.$/ {
	if (seen++) {
		next
	}
}
{ print }
' "$tmp_dir/INSPIRATION.md" > "$tmp_dir/INSPIRATION.md.new"
mv "$tmp_dir/INSPIRATION.md.new" "$tmp_dir/INSPIRATION.md"
if ! grep -Fq 'C059a Sampled adaptive codec selection from a bounded prefix.' "$tmp_dir/INSPIRATION.md"; then
	awk '/^- \[ \] C059 Codec selection from sampled column entropy\.$/ { print; print "- [x] C059a Sampled adaptive codec selection from a bounded prefix."; next } { print }' "$tmp_dir/INSPIRATION.md" > "$tmp_dir/INSPIRATION.md.new"
	mv "$tmp_dir/INSPIRATION.md.new" "$tmp_dir/INSPIRATION.md"
fi

GIT_INDEX_FILE="$index_file" git -C "$repo_root" read-tree HEAD
for path in \
	INSPIRATION.md \
	Makefile \
	ROW_BINARY_ADAPTIVE_SAMPLED.md \
	hat/hatSql/row_binary_adaptive_sampled.go \
	hat/hatSql/row_binary_adaptive_sampled_test.go \
	scripts/benchmark-sql-row-binary-adaptive-sampled.sh \
	scripts/deliver-sql-row-binary-adaptive-sampled.sh \
	scripts/format-sql-row-binary-adaptive-sampled.sh \
	scripts/inspect-row-binary-adaptive.sh \
	scripts/test-race-sql-row-binary-adaptive-sampled.sh \
	scripts/test-sql-row-binary-adaptive-sampled.sh; do
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
		GIT_INDEX_FILE="$index_file" git -C "$repo_root" commit -m "feat(sql): add sampled adaptive RowBinary codec"
		;;
	push)
		git -C "$repo_root" push origin HEAD:master
		;;
	*)
		echo "unknown delivery mode: $mode" >&2
		exit 2
		;;
esac
