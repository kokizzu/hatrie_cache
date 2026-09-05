#!/usr/bin/env bash
set -euo pipefail

mode="${1:-preview}"
repo_root="$(cd "$(dirname "$0")/.." && pwd)"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT

index_file="$tmp_dir/index"

git -C "$repo_root" show HEAD:Makefile > "$tmp_dir/Makefile"
if ! grep -Fq 'benchmark-sql-row-binary-codec-accounting:' "$tmp_dir/Makefile"; then
	printf '\n.PHONY: benchmark-sql-row-binary-codec-accounting\nbenchmark-sql-row-binary-codec-accounting:\n\tbash ./scripts/benchmark-sql-row-binary-codec-accounting.sh\n\n.PHONY: deliver-sql-row-binary-codec-accounting\ndeliver-sql-row-binary-codec-accounting:\n\tbash ./scripts/deliver-sql-row-binary-codec-accounting.sh preview\n\n.PHONY: commit-sql-row-binary-codec-accounting\ncommit-sql-row-binary-codec-accounting:\n\tbash ./scripts/deliver-sql-row-binary-codec-accounting.sh commit\n\n.PHONY: push-sql-row-binary-codec-accounting\npush-sql-row-binary-codec-accounting:\n\tbash ./scripts/deliver-sql-row-binary-codec-accounting.sh push\n' >> "$tmp_dir/Makefile"
fi

git -C "$repo_root" show HEAD:INSPIRATION.md > "$tmp_dir/INSPIRATION.md"

awk '
/^- \[x\] C060a Opt-in codec size and synchronous decode-time accounting\.$/ {
	if (seen++) {
		next
	}
}
{ print }
' "$tmp_dir/INSPIRATION.md" > "$tmp_dir/INSPIRATION.md.new"
mv "$tmp_dir/INSPIRATION.md.new" "$tmp_dir/INSPIRATION.md"

if ! grep -Fq 'C060a Opt-in codec size and synchronous decode-time accounting.' "$tmp_dir/INSPIRATION.md"; then
	awk '/^- \[ \] C060 Compression ratio and decompression CPU accounting\.$/ { print; print "- [x] C060a Opt-in codec size and synchronous decode-time accounting."; next } { print }' "$tmp_dir/INSPIRATION.md" > "$tmp_dir/INSPIRATION.md.new"
	mv "$tmp_dir/INSPIRATION.md.new" "$tmp_dir/INSPIRATION.md"
fi

GIT_INDEX_FILE="$index_file" git -C "$repo_root" read-tree HEAD
for path in \
	INSPIRATION.md \
	Makefile \
	ROW_BINARY_CODEC_ACCOUNTING.md \
	hat/hatSql/row_binary_codec_accounting.go \
	hat/hatSql/row_binary_codec_accounting_test.go \
	scripts/benchmark-sql-row-binary-codec-accounting.sh \
	scripts/deliver-sql-row-binary-codec-accounting.sh \
	scripts/format-sql-row-binary-codec-accounting.sh \
	scripts/test-race-sql-row-binary-codec-accounting.sh \
	scripts/test-sql-row-binary-codec-accounting.sh; do
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
		GIT_INDEX_FILE="$index_file" git -C "$repo_root" commit -m "feat(sql): add RowBinary codec accounting"
		;;
	push)
		git -C "$repo_root" push origin HEAD:master
		;;
	*)
		echo "unknown delivery mode: $mode" >&2
		exit 2
		;;
esac
