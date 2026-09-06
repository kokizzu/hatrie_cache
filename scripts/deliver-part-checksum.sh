#!/usr/bin/env bash
set -euo pipefail

mode="${1:-preview}"
repo_root="$(cd "$(dirname "$0")/.." && pwd)"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT

index_file="$tmp_dir/index"

git -C "$repo_root" show HEAD:Makefile > "$tmp_dir/Makefile"
if ! grep -Fq 'inspect-replication-checksum:' "$tmp_dir/Makefile"; then
	printf '\n.PHONY: inspect-replication-checksum\ninspect-replication-checksum:\n\tbash ./scripts/inspect-replication-checksum.sh\n\n.PHONY: test-part-checksum\ntest-part-checksum:\n\tbash ./scripts/test-part-checksum.sh\n\n.PHONY: test-race-part-checksum\ntest-race-part-checksum:\n\tbash ./scripts/test-race-part-checksum.sh\n\n.PHONY: format-part-checksum\nformat-part-checksum:\n\tbash ./scripts/format-part-checksum.sh\n\n.PHONY: benchmark-part-checksum\nbenchmark-part-checksum:\n\tbash ./scripts/benchmark-part-checksum.sh\n\n.PHONY: deliver-part-checksum\ndeliver-part-checksum:\n\tbash ./scripts/deliver-part-checksum.sh preview\n\n.PHONY: commit-part-checksum\ncommit-part-checksum:\n\tbash ./scripts/deliver-part-checksum.sh commit\n\n.PHONY: push-part-checksum\npush-part-checksum:\n\tbash ./scripts/deliver-part-checksum.sh push\n' >> "$tmp_dir/Makefile"
fi

git -C "$repo_root" show HEAD:INSPIRATION.md > "$tmp_dir/INSPIRATION.md"
awk '
/^- \[x\] C148a Immutable-part length and SHA-256 checksums\.$/ {
	if (seen++) {
		next
	}
}
{ print }
' "$tmp_dir/INSPIRATION.md" > "$tmp_dir/INSPIRATION.md.new"
mv "$tmp_dir/INSPIRATION.md.new" "$tmp_dir/INSPIRATION.md"
if ! grep -Fq 'C148a Immutable-part length and SHA-256 checksums.' "$tmp_dir/INSPIRATION.md"; then
	awk '/^- \[ \] C148 Replicated part exchange with checksums\.$/ { print; print "- [x] C148a Immutable-part length and SHA-256 checksums."; next } { print }' "$tmp_dir/INSPIRATION.md" > "$tmp_dir/INSPIRATION.md.new"
	mv "$tmp_dir/INSPIRATION.md.new" "$tmp_dir/INSPIRATION.md"
fi

GIT_INDEX_FILE="$index_file" git -C "$repo_root" read-tree HEAD
for path in \
	INSPIRATION.md \
	Makefile \
	PART_CHECKSUMS.md \
	hat/hatMerkle/part_checksum.go \
	hat/hatMerkle/part_checksum_test.go \
	scripts/benchmark-part-checksum.sh \
	scripts/deliver-part-checksum.sh \
	scripts/format-part-checksum.sh \
	scripts/inspect-replication-checksum.sh \
	scripts/test-part-checksum.sh \
	scripts/test-race-part-checksum.sh; do
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
		GIT_INDEX_FILE="$index_file" git -C "$repo_root" commit -m "feat(merkle): add immutable part checksums"
		;;
	push)
		git -C "$repo_root" push origin HEAD:master
		;;
	*)
		echo "unknown delivery mode: $mode" >&2
		exit 2
		;;
esac
