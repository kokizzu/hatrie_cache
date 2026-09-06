#!/usr/bin/env bash
set -euo pipefail

mode="${1:-preview}"
repo_root="$(cd "$(dirname "$0")/.." && pwd)"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT

index_file="$tmp_dir/index"

git -C "$repo_root" show HEAD:Makefile > "$tmp_dir/Makefile"
if ! grep -Fq 'test-sql-deterministic-sample:' "$tmp_dir/Makefile"; then
	printf '\n.PHONY: test-sql-deterministic-sample\ntest-sql-deterministic-sample:\n\tbash ./scripts/test-sql-deterministic-sample.sh\n\n.PHONY: test-race-sql-deterministic-sample\ntest-race-sql-deterministic-sample:\n\tbash ./scripts/test-race-sql-deterministic-sample.sh\n\n.PHONY: format-sql-deterministic-sample\nformat-sql-deterministic-sample:\n\tbash ./scripts/format-sql-deterministic-sample.sh\n\n.PHONY: benchmark-sql-deterministic-sample\nbenchmark-sql-deterministic-sample:\n\tbash ./scripts/benchmark-sql-deterministic-sample.sh\n\n.PHONY: deliver-sql-deterministic-sample\ndeliver-sql-deterministic-sample:\n\tbash ./scripts/deliver-sql-deterministic-sample.sh preview\n\n.PHONY: commit-sql-deterministic-sample\ncommit-sql-deterministic-sample:\n\tbash ./scripts/deliver-sql-deterministic-sample.sh commit\n\n.PHONY: push-sql-deterministic-sample\npush-sql-deterministic-sample:\n\tbash ./scripts/deliver-sql-deterministic-sample.sh push\n' >> "$tmp_dir/Makefile"
fi

git -C "$repo_root" show HEAD:INSPIRATION.md > "$tmp_dir/INSPIRATION.md"
awk '
/^- \[x\] C040a Deterministic key-hash sampling across partition boundaries\.$/ {
	if (seen++) {
		next
	}
}
{ print }
' "$tmp_dir/INSPIRATION.md" > "$tmp_dir/INSPIRATION.md.new"
mv "$tmp_dir/INSPIRATION.md.new" "$tmp_dir/INSPIRATION.md"
if ! grep -Fq 'C040a Deterministic key-hash sampling across partition boundaries.' "$tmp_dir/INSPIRATION.md"; then
	awk '/^- \[ \] C040 Sampling key with deterministic SAMPLE semantics across partitions\.$/ { print; print "- [x] C040a Deterministic key-hash sampling across partition boundaries."; next } { print }' "$tmp_dir/INSPIRATION.md" > "$tmp_dir/INSPIRATION.md.new"
	mv "$tmp_dir/INSPIRATION.md.new" "$tmp_dir/INSPIRATION.md"
fi

GIT_INDEX_FILE="$index_file" git -C "$repo_root" read-tree HEAD
for path in \
	INSPIRATION.md \
	Makefile \
	DETERMINISTIC_SAMPLE.md \
	hat/hatSql/deterministic_sample.go \
	hat/hatSql/deterministic_sample_test.go \
	scripts/benchmark-sql-deterministic-sample.sh \
	scripts/deliver-sql-deterministic-sample.sh \
	scripts/format-sql-deterministic-sample.sh \
	scripts/test-race-sql-deterministic-sample.sh \
	scripts/test-sql-deterministic-sample.sh; do
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
		GIT_INDEX_FILE="$index_file" git -C "$repo_root" commit -m "feat(sql): add deterministic sampling"
		;;
	push)
		git -C "$repo_root" push origin HEAD:master
		;;
	*)
		echo "unknown delivery mode: $mode" >&2
		exit 2
		;;
esac
