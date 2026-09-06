#!/usr/bin/env bash
set -euo pipefail

mode="${1:-preview}"
root_dir="$(git rev-parse --show-toplevel)"
branch="$(git symbolic-ref --short HEAD)"

if [[ "$mode" == "push" ]]; then
	git push origin "$branch"
	exit 0
fi
if [[ "$mode" != "preview" && "$mode" != "commit" ]]; then
	echo "usage: deliver-c057-checklist.sh [preview|commit|push]" >&2
	exit 2
fi

base_commit="${BASE_COMMIT:-HEAD}"
tmp_dir="$(mktemp -d)"
index_file="$tmp_dir/index"
export GIT_INDEX_FILE="$index_file"

cleanup() {
	unset GIT_INDEX_FILE
	rm -rf "$tmp_dir"
}
trap cleanup EXIT

git read-tree "$base_commit"

git show "$base_commit:INSPIRATION.md" > "$tmp_dir/INSPIRATION.md"
awk '
BEGIN { updated = 0 }
{
	if (!updated && $0 == "- [ ] C057 Gorilla-style floating-point encoding.") {
		print "- [x] C057 Gorilla-style floating-point encoding. Bit-preserving XOR window encoding and exact-input validation are provided by hatCodec (see GORILLA_FLOAT.md)."
		updated = 1
	} else {
		print
	}
}
END {
	if (!updated) exit 1
}' "$tmp_dir/INSPIRATION.md" > "$tmp_dir/INSPIRATION.new"
blob="$(git hash-object -w "$tmp_dir/INSPIRATION.new")"
git update-index --add --cacheinfo 100644 "$blob" INSPIRATION.md

git show "$base_commit:README.md" > "$tmp_dir/README.md"
awk '
BEGIN { added = 0 }
{
	print
	if (!added && $0 == "- Importable Cartesian R-tree for selective rectangle and point queries: [R-tree spatial index](SPATIAL_RTREE.md)") {
		print "- Bit-preserving XOR-window float encoding: [Gorilla float codec](GORILLA_FLOAT.md)"
		added = 1
	}
}
END {
	if (!added) exit 1
}' "$tmp_dir/README.md" > "$tmp_dir/README.new"
blob="$(git hash-object -w "$tmp_dir/README.new")"
git update-index --add --cacheinfo 100644 "$blob" README.md

for path in scripts/verify-c057.sh scripts/deliver-c057-checklist.sh; do
	if [[ ! -f "$root_dir/$path" ]]; then
		echo "missing delivery file: $path" >&2
		exit 1
	fi
done
chmod +x "$root_dir/scripts/verify-c057.sh" "$root_dir/scripts/deliver-c057-checklist.sh"
git add -- "$root_dir/scripts/verify-c057.sh" "$root_dir/scripts/deliver-c057-checklist.sh"

git show "$base_commit:Makefile" > "$tmp_dir/Makefile"
if awk '/^verify-c057:$/ { found = 1 } END { exit found }' "$tmp_dir/Makefile"; then
	awk '
	{ print }
	END {
		print ""
		print "verify-c057:"
		print "\tbash ./scripts/verify-c057.sh"
		print ""
		print "deliver-c057-checklist:"
		print "\tbash ./scripts/deliver-c057-checklist.sh preview"
		print ""
		print "commit-c057-checklist:"
		print "\tbash ./scripts/deliver-c057-checklist.sh commit"
		print ""
		print "push-c057-checklist:"
		print "\tbash ./scripts/deliver-c057-checklist.sh push"
	}' "$tmp_dir/Makefile" > "$tmp_dir/Makefile.new"
	blob="$(git hash-object -w "$tmp_dir/Makefile.new")"
	git update-index --add --cacheinfo 100644 "$blob" Makefile
else
	echo "C057 verification targets already exist in $base_commit" >&2
	exit 1
fi

git diff --cached --check
git diff --cached --stat
git diff --cached --name-status

if [[ "$mode" == "commit" ]]; then
	git commit -m "docs(inspiration): mark Gorilla float codec verified"
	git show -s --format='%h %s' HEAD
fi
