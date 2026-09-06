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
	echo "usage: deliver-c016-fix.sh [preview|commit|push]" >&2
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
git show "$base_commit:hat/hatPipeline/pipeline_test.go" > "$tmp_dir/pipeline_test.go"
awk '
BEGIN { replaced = 0 }
{
	if ($0 == "\tstarted := make(chan struct{}, 4)") {
		print "\tstarted := make(chan struct{}, 8)"
		replaced++
	} else {
		print
	}
}
END {
	if (replaced != 1) exit 1
}' "$tmp_dir/pipeline_test.go" > "$tmp_dir/pipeline_test.new"
blob="$(git hash-object -w "$tmp_dir/pipeline_test.new")"
git update-index --add --cacheinfo 100644 "$blob" hat/hatPipeline/pipeline_test.go

for path in scripts/verify-c016.sh scripts/deliver-c016-fix.sh; do
	if [[ ! -f "$root_dir/$path" ]]; then
		echo "missing delivery file: $path" >&2
		exit 1
	fi
done
chmod +x "$root_dir/scripts/verify-c016.sh" "$root_dir/scripts/deliver-c016-fix.sh"
git add -- "$root_dir/scripts/verify-c016.sh" "$root_dir/scripts/deliver-c016-fix.sh"

git show "$base_commit:Makefile" > "$tmp_dir/Makefile"
if awk '/^verify-c016:$/ { found = 1 } END { exit found }' "$tmp_dir/Makefile"; then
	awk '
	{ print }
	END {
		print ""
		print "verify-c016:"
		print "\tbash ./scripts/verify-c016.sh"
		print ""
		print "deliver-c016-fix:"
		print "\tbash ./scripts/deliver-c016-fix.sh preview"
		print ""
		print "commit-c016-fix:"
		print "\tbash ./scripts/deliver-c016-fix.sh commit"
		print ""
		print "push-c016-fix:"
		print "\tbash ./scripts/deliver-c016-fix.sh push"
	}' "$tmp_dir/Makefile" > "$tmp_dir/Makefile.new"
	blob="$(git hash-object -w "$tmp_dir/Makefile.new")"
	git update-index --add --cacheinfo 100644 "$blob" Makefile
else
	echo "C016 verification targets already exist in $base_commit" >&2
	exit 1
fi

git diff --cached --check
git diff --cached --stat
git diff --cached --name-status

if [[ "$mode" == "commit" ]]; then
	git commit -m "test(pipeline): prevent worker test deadlock"
	git show -s --format='%h %s' HEAD
fi
