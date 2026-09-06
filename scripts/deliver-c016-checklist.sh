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
	echo "usage: deliver-c016-checklist.sh [preview|commit|push]" >&2
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
	if (!updated && $0 == "- [ ] C016 Pipeline stages with independently scheduled work.") {
		print "- [x] C016 Pipeline stages with independently scheduled work. Bounded queues, independent worker pools, backpressure, cancellation, and stage-scoped errors are provided by hatPipeline (see PIPELINE_STAGES.md)."
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
		print "- Bounded independently scheduled processing stages: [Pipeline stages](PIPELINE_STAGES.md)"
		added = 1
	}
}
END {
	if (!added) exit 1
}' "$tmp_dir/README.md" > "$tmp_dir/README.new"
blob="$(git hash-object -w "$tmp_dir/README.new")"
git update-index --add --cacheinfo 100644 "$blob" README.md

chmod +x "$root_dir/scripts/deliver-c016-checklist.sh"
git add -- "$root_dir/scripts/deliver-c016-checklist.sh"

git show "$base_commit:Makefile" > "$tmp_dir/Makefile"
if awk '/^deliver-c016-checklist:$/ { found = 1 } END { exit found }' "$tmp_dir/Makefile"; then
	awk '
	{ print }
	END {
		print ""
		print "deliver-c016-checklist:"
		print "\tbash ./scripts/deliver-c016-checklist.sh preview"
		print ""
		print "commit-c016-checklist:"
		print "\tbash ./scripts/deliver-c016-checklist.sh commit"
		print ""
		print "push-c016-checklist:"
		print "\tbash ./scripts/deliver-c016-checklist.sh push"
	}' "$tmp_dir/Makefile" > "$tmp_dir/Makefile.new"
	blob="$(git hash-object -w "$tmp_dir/Makefile.new")"
	git update-index --add --cacheinfo 100644 "$blob" Makefile
else
	echo "C016 checklist targets already exist in $base_commit" >&2
	exit 1
fi

git diff --cached --check
git diff --cached --stat
git diff --cached --name-status

if [[ "$mode" == "commit" ]]; then
	git commit -m "docs(inspiration): mark pipeline stages verified"
	git show -s --format='%h %s' HEAD
fi
