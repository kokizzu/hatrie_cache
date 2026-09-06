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
	echo "usage: deliver-rtree.sh [preview|commit|push]" >&2
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

for path in \
	hat/hatDataStructure/rtree.go \
	hat/hatDataStructure/rtree_test.go \
	hat/hatDataStructure/rtree_public_test.go \
	hat/hatDataStructure/rtree_benchmark_test.go \
	SPATIAL_RTREE.md \
	scripts/format-rtree.sh \
	scripts/test-rtree.sh \
	scripts/race-rtree.sh \
	scripts/benchmark-rtree.sh \
	scripts/deliver-rtree.sh; do
	if [[ ! -f "$root_dir/$path" ]]; then
		echo "missing delivery file: $path" >&2
		exit 1
	fi
done

chmod +x "$root_dir"/scripts/format-rtree.sh "$root_dir"/scripts/test-rtree.sh "$root_dir"/scripts/race-rtree.sh "$root_dir"/scripts/benchmark-rtree.sh "$root_dir"/scripts/deliver-rtree.sh
git add -- \
	"$root_dir/hat/hatDataStructure/rtree.go" \
	"$root_dir/hat/hatDataStructure/rtree_test.go" \
	"$root_dir/hat/hatDataStructure/rtree_public_test.go" \
	"$root_dir/hat/hatDataStructure/rtree_benchmark_test.go" \
	"$root_dir/SPATIAL_RTREE.md" \
	"$root_dir/scripts/format-rtree.sh" \
	"$root_dir/scripts/test-rtree.sh" \
	"$root_dir/scripts/race-rtree.sh" \
	"$root_dir/scripts/benchmark-rtree.sh" \
	"$root_dir/scripts/deliver-rtree.sh"

git show "$base_commit:README.md" > "$tmp_dir/README.md"
awk '
BEGIN { added = 0 }
{
	print
	if (!added && $0 == "- New to the SQL interface: [SQL.md](SQL.md)") {
		print "- Importable Cartesian R-tree for selective rectangle and point queries: [R-tree spatial index](SPATIAL_RTREE.md)"
		added = 1
	}
}
END {
	if (!added) exit 1
}' "$tmp_dir/README.md" > "$tmp_dir/README.new"
blob="$(git hash-object -w "$tmp_dir/README.new")"
git update-index --add --cacheinfo 100644 "$blob" README.md

git show "$base_commit:INSPIRATION.md" > "$tmp_dir/INSPIRATION.md"
awk '
BEGIN { updated = 0 }
{
	if (!updated && $0 == "- [ ] T008 RTREE spatial index.") {
		print "- [x] T008 RTREE spatial index. Importable uint64-ID rectangle index with deterministic overlap and point search (see SPATIAL_RTREE.md)."
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

git show "$base_commit:Makefile" > "$tmp_dir/Makefile"
if awk '/^format-rtree:$/ { found = 1 } END { exit found }' "$tmp_dir/Makefile"; then
	awk '
	{ print }
	END {
		print ""
		print "format-rtree:"
		print "\tbash ./scripts/format-rtree.sh"
		print ""
		print "test-rtree:"
		print "\tbash ./scripts/test-rtree.sh"
		print ""
		print "race-rtree:"
		print "\tbash ./scripts/race-rtree.sh"
		print ""
		print "benchmark-rtree:"
		print "\tbash ./scripts/benchmark-rtree.sh"
		print ""
		print "deliver-rtree:"
		print "\tbash ./scripts/deliver-rtree.sh preview"
		print ""
		print "commit-rtree:"
		print "\tbash ./scripts/deliver-rtree.sh commit"
		print ""
		print "push-rtree:"
		print "\tbash ./scripts/deliver-rtree.sh push"
	}' "$tmp_dir/Makefile" > "$tmp_dir/Makefile.new"
	blob="$(git hash-object -w "$tmp_dir/Makefile.new")"
	git update-index --add --cacheinfo 100644 "$blob" Makefile
else
	echo "R-tree Makefile targets already exist in $base_commit" >&2
	exit 1
fi

git diff --cached --check
git diff --cached --stat
git diff --cached --name-status

if [[ "$mode" == "commit" ]]; then
	git commit -m "feat(data-structure): add R-tree spatial index"
	git show -s --format='%h %s' HEAD
fi
