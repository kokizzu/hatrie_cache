#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$root"

mode=${1:-preview}
commit_message='feat(grpc): expose public protobuf client surface'
child='- [x] T150a Public importable gRPC client aliases over the language-neutral protobuf contract.'

work=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-public-grpc-client.XXXXXX")
index="$work/index"
trap 'rm -rf -- "$work"' EXIT

base=$(git rev-parse HEAD)
git show "$base:Makefile" > "$work/Makefile"
git show "$base:INSPIRATION.md" > "$work/INSPIRATION.md"

if awk '/^- \[x\] T150a / { found=1 } END { exit !found }' "$work/INSPIRATION.md"; then
	printf '%s\n' 'T150a is already present in HEAD; refusing a duplicate delivery.' >&2
	exit 1
fi

awk -v child="$child" '
BEGIN { added=0 }
{
	print
	if (!added && $0 ~ /^- \[ \] T150 /) {
		print child
		added=1
	}
}
END {
	if (!added) {
		print "T150 parent checklist row was not found" > "/dev/stderr"
		exit 1
	}
}' "$work/INSPIRATION.md" > "$work/INSPIRATION.next"
mv "$work/INSPIRATION.next" "$work/INSPIRATION.md"

if ! awk '/^# public-grpc-client-targets$/ { found=1 } END { exit found }' "$work/Makefile"; then
	awk '
	{ print }
	END {
		print ""
		print "# public-grpc-client-targets"
		print "format-public-grpc-client:"
		print "\tbash ./scripts/format-public-grpc-client.sh"
		print ""
		print "test-public-grpc-client:"
		print "\tbash ./scripts/test-public-grpc-client.sh"
		print ""
		print "test-race-public-grpc-client:"
		print "\tbash ./scripts/test-race-public-grpc-client.sh"
		print ""
		print "benchmark-public-grpc-client:"
		print "\tbash ./scripts/benchmark-public-grpc-client.sh"
		print ""
		print "deliver-public-grpc-client:"
		print "\tbash ./scripts/deliver-public-grpc-client.sh preview"
		print ""
		print "commit-public-grpc-client:"
		print "\tbash ./scripts/deliver-public-grpc-client.sh commit"
		print ""
		print "push-public-grpc-client:"
		print "\tbash ./scripts/deliver-public-grpc-client.sh push"
	}
	' "$work/Makefile" > "$work/Makefile.next"
	mv "$work/Makefile.next" "$work/Makefile"
fi

rm -f "$index"
GIT_INDEX_FILE="$index" git read-tree "$base"

make_blob=$(git hash-object -w "$work/Makefile")
inspiration_blob=$(git hash-object -w "$work/INSPIRATION.md")
GIT_INDEX_FILE="$index" git update-index --add --cacheinfo "100644,$make_blob,Makefile"
GIT_INDEX_FILE="$index" git update-index --add --cacheinfo "100644,$inspiration_blob,INSPIRATION.md"

for path in \
	CLIENT_SDK.md \
	hat/hatGrpc/public_client.go \
	hat/hatGrpc/public_client_test.go \
	scripts/benchmark-public-grpc-client.sh \
	scripts/deliver-public-grpc-client.sh \
	scripts/format-public-grpc-client.sh \
	scripts/test-public-grpc-client.sh \
	scripts/test-race-public-grpc-client.sh; do
	if [ ! -f "$path" ]; then
		printf 'missing delivery file: %s\n' "$path" >&2
		exit 1
	fi
	GIT_INDEX_FILE="$index" git add -- "$path"
done

case "$mode" in
preview)
	printf '%s\n' "base: $base"
	GIT_INDEX_FILE="$index" git diff --cached --name-status
	GIT_INDEX_FILE="$index" git diff --cached --stat
	;;
commit)
	GIT_INDEX_FILE="$index" git diff --cached --check
	GIT_INDEX_FILE="$index" git commit -m "$commit_message"
	git rev-parse HEAD
	;;
push)
	git push
	git rev-parse HEAD
	;;
*)
	printf 'usage: %s [preview|commit|push]\n' "$0" >&2
	exit 2
	;;
esac
