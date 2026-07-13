#!/usr/bin/env sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)

gcc -std=c99 -Wall -Wextra \
	-I"$ROOT/luikore__hat-trie/src" \
	-o /tmp/hatrie_cache_check_hattrie \
	"$ROOT/luikore__hat-trie/test/check_hattrie.c" \
	"$ROOT/luikore__hat-trie/test/str_map.c" \
	"$ROOT/luikore__hat-trie/src/hat-trie.c" \
	"$ROOT/luikore__hat-trie/src/ahtable.c" \
	"$ROOT/luikore__hat-trie/src/misc.c" \
	"$ROOT/luikore__hat-trie/src/murmurhash3.c"

gcc -std=c99 -Wall -Wextra \
	-I"$ROOT/luikore__hat-trie/src" \
	-o /tmp/hatrie_cache_check_ahtable \
	"$ROOT/luikore__hat-trie/test/check_ahtable.c" \
	"$ROOT/luikore__hat-trie/test/str_map.c" \
	"$ROOT/luikore__hat-trie/src/ahtable.c" \
	"$ROOT/luikore__hat-trie/src/misc.c" \
	"$ROOT/luikore__hat-trie/src/murmurhash3.c"

/tmp/hatrie_cache_check_hattrie
/tmp/hatrie_cache_check_ahtable
