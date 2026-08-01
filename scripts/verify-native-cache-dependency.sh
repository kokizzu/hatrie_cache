#!/bin/sh
set -eu

project_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
fixture=$(mktemp -d /tmp/hatrie-native-cache-check.XXXXXX)
archive="$fixture/source.tar"
work="$fixture/work"
cache="$fixture/gocache"
log="$fixture/mutated-test.log"

cleanup() {
	rm -rf -- "$fixture"
}
trap cleanup EXIT HUP INT TERM

mkdir -p "$work" "$cache"
(
	cd "$project_root"
	git ls-files --cached --others --exclude-standard -z |
		tar --null -T - -cf "$archive"
)
tar -xf "$archive" -C "$work"

(
	cd "$work"
	env GOCACHE="$cache" go test -c -o "$fixture/before" .
	"$fixture/before" -test.run=TestEmptyKeyIsCountedIterableAndDeletable -test.count=1 >/dev/null
	sed -i 's/return T->m;/return T->m + 1;/' luikore__hat-trie/src/hat-trie.c
	grep -q 'return T->m + 1;' luikore__hat-trie/src/hat-trie.c
	env GOCACHE="$cache" go test -c -o "$fixture/after" .
)

if "$fixture/after" -test.run=TestEmptyKeyIsCountedIterableAndDeletable -test.count=1 >"$log" 2>&1; then
	echo "verify-native-cache-dependency: stale native object reused" >&2
	exit 1
fi
if ! grep -q 'Size() after empty key insert = 2, want 1' "$log"; then
	cat "$log" >&2
	echo "verify-native-cache-dependency: mutated fixture failed unexpectedly" >&2
	exit 1
fi

echo "verify-native-cache-dependency: ok"
